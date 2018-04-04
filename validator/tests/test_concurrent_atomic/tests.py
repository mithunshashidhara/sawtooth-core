# Copyright 2018 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------------------------

import logging
from threading import RLock
import unittest
from unittest.mock import patch

from sawtooth_validator.database.dict_database import DictDatabase
from sawtooth_validator.concurrent.atomic import ConcurrentMultiMap
from sawtooth_validator.journal.block_cache import BlockCache
from sawtooth_validator.journal.block_wrapper import BlockStatus
from sawtooth_validator.journal.block_wrapper import BlockWrapper
from sawtooth_validator.journal.block_wrapper import NULL_BLOCK_IDENTIFIER
from sawtooth_validator.journal.block_store import BlockStore
from sawtooth_validator.journal.block_validator import BlockValidator
from sawtooth_validator.journal.block_validator import BlockValidationFailure
from sawtooth_validator.journal.chain import ChainController
from sawtooth_validator.journal.chain_commit_state import ChainCommitState
from sawtooth_validator.journal.chain_commit_state import DuplicateTransaction
from sawtooth_validator.journal.chain_commit_state import DuplicateBatch
from sawtooth_validator.journal.chain_commit_state import MissingDependency
from sawtooth_validator.journal.publisher import BlockPublisher
from sawtooth_validator.journal.timed_cache import TimedCache
from sawtooth_validator.journal.event_extractors \
    import BlockEventExtractor
from sawtooth_validator.journal.event_extractors \
    import ReceiptEventExtractor
from sawtooth_validator.journal.batch_injector import \
    DefaultBatchInjectorFactory
from sawtooth_validator.concurrent.atomic import Counter
from sawtooth_validator.server.events.subscription import EventSubscription
from sawtooth_validator.server.events.subscription import EventFilterFactory

from sawtooth_validator.protobuf.transaction_pb2 import Transaction
from sawtooth_validator.protobuf.transaction_pb2 import TransactionHeader
from sawtooth_validator.protobuf.batch_pb2 import Batch
from sawtooth_validator.protobuf.block_pb2 import Block
from sawtooth_validator.protobuf.block_pb2 import BlockHeader
from sawtooth_validator.protobuf.transaction_receipt_pb2 import \
    TransactionReceipt
from sawtooth_validator.protobuf.transaction_receipt_pb2 import StateChange
from sawtooth_validator.protobuf.transaction_receipt_pb2 import StateChangeList
from sawtooth_validator.protobuf.events_pb2 import Event
from sawtooth_validator.protobuf.events_pb2 import EventFilter

from sawtooth_validator.state.settings_view import SettingsViewFactory
from sawtooth_validator.state.settings_cache import SettingsCache

from test_journal.block_tree_manager import BlockTreeManager

from test_journal.mock import MockChainIdManager
from test_journal.mock import MockBlockSender
from test_journal.mock import MockBatchSender
from test_journal.mock import MockNetwork
from test_journal.mock import MockStateViewFactory, CreateSetting
from test_journal.mock import MockTransactionExecutor
from test_journal.mock import MockPermissionVerifier
from test_journal.mock import SynchronousExecutor
from test_journal.mock import MockBatchInjectorFactory
from test_journal.utils import wait_until

from test_journal import mock_consensus


LOGGER = logging.getLogger(__name__)


class Test_ConcurrentMultiMap(unittest.TestCase):
    def setUp(self):
        self.gossip = MockNetwork()
        self.txn_executor = MockTransactionExecutor()
        self.block_sender = MockBlockSender()
        self.batch_sender = MockBatchSender()
        self.permission_verifier = MockPermissionVerifier()
        self.blocks_pending = ConcurrentMultiMap()

    def test_generate_and_publish_block(self):
        """
        Test that the Journal will produce blocks and consume those blocks
        to extend the chain.
        :return:
        """
        # construction and wire the journal to the
        # gossip layer.

        btm = BlockTreeManager()
        block_publisher = None
        chain_controller = None
        try:
            block_publisher = BlockPublisher(
                transaction_executor=self.txn_executor,
                block_cache=btm.block_cache,
                state_view_factory=MockStateViewFactory(btm.state_db),
                settings_cache=SettingsCache(
                    SettingsViewFactory(
                        btm.state_view_factory),
                ),
                block_sender=self.block_sender,
                batch_sender=self.batch_sender,
                squash_handler=None,
                chain_head=btm.block_store.chain_head,
                identity_signer=btm.identity_signer,
                data_dir=None,
                config_dir=None,
                permission_verifier=self.permission_verifier,
                check_publish_block_frequency=0.1,
                batch_observers=[],
                batch_injector_factory=DefaultBatchInjectorFactory(
                    block_store=btm.block_store,
                    state_view_factory=MockStateViewFactory(btm.state_db),
                    signer=btm.identity_signer))

            block_validator = BlockValidator(
                state_view_factory=MockStateViewFactory(btm.state_db),
                block_cache=btm.block_cache,
                transaction_executor=self.txn_executor,
                squash_handler=None,
                identity_signer=btm.identity_signer,
                data_dir=None,
                config_dir=None,
                permission_verifier=self.permission_verifier)

            chain_controller = ChainController(
                block_cache=btm.block_cache,
                block_validator=block_validator,
                state_view_factory=MockStateViewFactory(btm.state_db),
                chain_head_lock=block_publisher.chain_head_lock,
                on_chain_updated=block_publisher.on_chain_updated,
                chain_id_manager=None,
                data_dir=None,
                config_dir=None,
                chain_observers=[])

            self.gossip.on_batch_received = block_publisher.queue_batch
            self.gossip.on_block_received = chain_controller.queue_block

            block_publisher.start()
            chain_controller.start()

            # feed it a batch
            batch = Batch()
            block_publisher.queue_batch(batch)

            wait_until(lambda: self.block_sender.new_block is not None, 2)
            self.assertTrue(self.block_sender.new_block is not None)

            block = BlockWrapper(self.block_sender.new_block)
            chain_controller.queue_block(block)

            # wait for the chain_head to be updated.
            block_list = []
            wait_until(lambda: btm.chain_head.identifier ==
                       block.identifier, 2)
            self.assertTrue(btm.chain_head.identifier == block.identifier)
            self.blocks_pending.set(block.identifier, block_list)
            self.blocks_pending.append(block.identifier, block_list)
            self.assertTrue(self.blocks_pending.swap(
                block.identifier, block_list) != [])
            self.assertTrue(self.blocks_pending.pop(
                block.identifier, block_list) == [])
            self.assertTrue(self.blocks_pending.get(
                block.identifier, block_list) == [])
            try:
                self.blocks_pending.swap(block.identifier, block)
            except Exception:
                LOGGER.debug("swap raised an exception")
            try:
                self.blocks_pending.set(block.identifier, block)
            except Exception:
                LOGGER.debug("set raised an exception")
        finally:
            if block_publisher is not None:
                block_publisher.stop()
            if chain_controller is not None:
                chain_controller.stop()
            if block_validator is not None:
                block_validator.stop()


class Test_Counter(unittest.TestCase):
    def test_counter(self):
        self.test_counter = Counter(0)
        self.test_counter.inc(1)
        self.assertTrue(self.test_counter.get() == 1)
        self.test_counter.dec(1)
        self.assertTrue(self.test_counter.get_and_inc(1) == 0)
        self.assertTrue(self.test_counter.get_and_dec(1) == 1)
