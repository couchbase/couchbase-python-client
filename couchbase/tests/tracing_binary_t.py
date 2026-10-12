#  Copyright 2016-2026. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

from collections.abc import Generator
from datetime import timedelta
from typing import Any

import pytest

from couchbase.durability import DurabilityLevel, ServerDurability
from couchbase.exceptions import DocumentNotFoundException, InvalidArgumentException
from couchbase.logic.observability import OpName
from couchbase.options import (AppendOptions,
                               DecrementOptions,
                               IncrementOptions,
                               PrependOptions,
                               SignedInt64)
from tests.environments.managers import TestEnvironmentBucketManager
from tests.environments.test_environment import (ConnectionStrategy,
                                                 DocType,
                                                 SyncTestEnvironment,
                                                 TestEnvironment,
                                                 TestProtocol,
                                                 build_environment,
                                                 teardown_environment)
from tests.environments.tracing_environment import TracingEnvironment, TracingType
from tests.manifest import validate_test_manifest
from tests.suite_fixtures import TopologySuite
from tests.tracing_validators import KeyValueSpanValidator

# Deliberately the wrong type, and annotated Any so that saying so is the test rather than
# something the type checker has to be told to ignore.  append and prepend take str or bytes; a
# bare int for `initial` is the mistake SignedInt64 exists to catch.
NON_BINARY_VALUE: Any = 123
UNWRAPPED_INITIAL: Any = 123
# Without a negative initial the server creates the counter document and the operation
# succeeds, which is a different test.
NEGATIVE_INITIAL = SignedInt64(-1)
# MAJORITY rather than PERSIST_TO_MAJORITY: the span carries whichever level was asked for, and
# a persisted write waits on disk on more than one node, which a loaded cluster has been measured
# taking past 9s.  The persisted levels are covered by the durability suites.
MAJORITY = ServerDurability(level=DurabilityLevel.MAJORITY)
MAJORITY_LABEL = DurabilityLevel.to_server_str(DurabilityLevel.MAJORITY)
# The server gives up on a durable write within the request's own timeout and reports it
# ambiguous, and a loaded cluster has been measured taking past 9s over a MAJORITY write alone.
SERVER_DURABLE_TIMEOUT = timedelta(seconds=30)
# The server reports a durable write ambiguous when it cannot get a majority to acknowledge it within
# its own limit, and a single write can stall for that whole limit while the writes around it finish
# in seconds, so a longer timeout only waits longer.  These tests assert the span rather than the
# durability, so a stalled write is retried; any other failure still fails the test.
SYNC_WRITE_AMBIGUOUS_RERUN = pytest.mark.flaky(reruns=2,
                                               reruns_delay=1,
                                               only_rerun=['DurabilitySyncWriteAmbiguous'])


class BinaryTracingTestSuite(TopologySuite):
    # The binary half of `tracing_kv_t`, with the same three steps per test and the same four
    # tracing types.  Four operations, each in five situations: on its own, with a durability
    # requirement, refused by the server, refused by the client, and handed a parent span.
    #
    # Each operation is named literally rather than reached through `getattr`, which has no type
    # for the generator to read: the generated suites would call it without awaiting it, and an
    # unawaited operation produces no spans at all.

    # Observability support in CNG is pending.
    UNSUPPORTED_PROTOCOLS = [TestProtocol.COUCHBASE2]
    # A tracer is a cluster option, so the suite needs a cluster of its own.
    CONNECTION_STRATEGY = ConnectionStrategy.NEW_CONNECTION
    # Read by the topology fixtures the durability tests are gated on.
    MANAGERS = [TestEnvironmentBucketManager]
    # append and prepend need a document that is already empty bytes; increment and decrement
    # need one holding an int.  Nothing here reads a populated BYTES document.
    DATA_SET = [DocType.BYTES_EMPTY, DocType.COUNTER]
    NUM_DOCS = 25

    TEST_MANIFEST = [
        'test_append',
        'test_append_invalid_value',
        'test_append_missing_key',
        'test_append_with_durability',
        'test_append_with_parent',
        'test_decrement',
        'test_decrement_invalid_initial',
        'test_decrement_missing_key',
        'test_decrement_with_durability',
        'test_decrement_with_parent',
        'test_increment',
        'test_increment_invalid_initial',
        'test_increment_missing_key',
        'test_increment_with_durability',
        'test_increment_with_parent',
        'test_prepend',
        'test_prepend_invalid_value',
        'test_prepend_missing_key',
        'test_prepend_with_durability',
        'test_prepend_with_parent',
    ]

    @pytest.fixture(name='kv_validator')
    def kv_validator_fixture(self, cb_env: SyncTestEnvironment) -> KeyValueSpanValidator:
        validator = TracingEnvironment.kv_span_validator(cb_env)
        validator.reset()
        return validator

    # ------------------------------------------------------------------
    # Operations that succeed
    # ------------------------------------------------------------------

    def test_append(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc_by_type(DocType.BYTES_EMPTY, key_only=True)
        cb_env.collection.binary().append(key, b'XXX')
        kv_validator.validate_kv_op(OpName.Append)

    def test_prepend(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc_by_type(DocType.BYTES_EMPTY, key_only=True)
        cb_env.collection.binary().prepend(key, b'XXX')
        kv_validator.validate_kv_op(OpName.Prepend)

    def test_increment(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc_by_type(DocType.COUNTER, key_only=True)
        cb_env.collection.binary().increment(key)
        kv_validator.validate_kv_op(OpName.Increment)

    def test_decrement(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc_by_type(DocType.COUNTER, key_only=True)
        cb_env.collection.binary().decrement(key)
        kv_validator.validate_kv_op(OpName.Decrement)

    # ------------------------------------------------------------------
    # Operations that carry a durability requirement
    # ------------------------------------------------------------------

    @SYNC_WRITE_AMBIGUOUS_RERUN
    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_append_with_durability(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc_by_type(DocType.BYTES_EMPTY, key_only=True)
        opts = AppendOptions(durability=MAJORITY, timeout=SERVER_DURABLE_TIMEOUT)
        cb_env.collection.binary().append(key, b'XXX', opts)
        kv_validator.validate_kv_op(OpName.Append, durability=MAJORITY_LABEL)

    @SYNC_WRITE_AMBIGUOUS_RERUN
    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_prepend_with_durability(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc_by_type(DocType.BYTES_EMPTY, key_only=True)
        opts = PrependOptions(durability=MAJORITY, timeout=SERVER_DURABLE_TIMEOUT)
        cb_env.collection.binary().prepend(key, b'XXX', opts)
        kv_validator.validate_kv_op(OpName.Prepend, durability=MAJORITY_LABEL)

    @SYNC_WRITE_AMBIGUOUS_RERUN
    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_increment_with_durability(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc_by_type(DocType.COUNTER, key_only=True)
        opts = IncrementOptions(durability=MAJORITY, timeout=SERVER_DURABLE_TIMEOUT)
        cb_env.collection.binary().increment(key, opts)
        kv_validator.validate_kv_op(OpName.Increment, durability=MAJORITY_LABEL)

    @SYNC_WRITE_AMBIGUOUS_RERUN
    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_decrement_with_durability(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc_by_type(DocType.COUNTER, key_only=True)
        opts = DecrementOptions(durability=MAJORITY, timeout=SERVER_DURABLE_TIMEOUT)
        cb_env.collection.binary().decrement(key, opts)
        kv_validator.validate_kv_op(OpName.Decrement, durability=MAJORITY_LABEL)

    # ------------------------------------------------------------------
    # Operations the server refuses, which still produce a full span tree
    # ------------------------------------------------------------------

    def test_append_missing_key(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.binary().append(TestEnvironment.NOT_A_KEY, b'XXX')
        kv_validator.validate_kv_op(OpName.Append, validate_error=True)

    def test_prepend_missing_key(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.binary().prepend(TestEnvironment.NOT_A_KEY, b'XXX')
        kv_validator.validate_kv_op(OpName.Prepend, validate_error=True)

    def test_increment_missing_key(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.binary().increment(TestEnvironment.NOT_A_KEY,
                                                 IncrementOptions(initial=NEGATIVE_INITIAL))
        kv_validator.validate_kv_op(OpName.Increment, validate_error=True)

    def test_decrement_missing_key(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.binary().decrement(TestEnvironment.NOT_A_KEY,
                                                 DecrementOptions(initial=NEGATIVE_INITIAL))
        kv_validator.validate_kv_op(OpName.Decrement, validate_error=True)

    # ------------------------------------------------------------------
    # Operations the client refuses, which never reach the wire
    # ------------------------------------------------------------------

    def test_append_invalid_value(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_new_doc(key_only=True)
        with pytest.raises((InvalidArgumentException, ValueError)):
            cb_env.collection.binary().append(key, NON_BINARY_VALUE)
        kv_validator.validate_kv_op(OpName.Append, validate_error=True, error_before_dispatch=True)

    def test_prepend_invalid_value(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_new_doc(key_only=True)
        with pytest.raises((InvalidArgumentException, ValueError)):
            cb_env.collection.binary().prepend(key, NON_BINARY_VALUE)
        kv_validator.validate_kv_op(OpName.Prepend, validate_error=True, error_before_dispatch=True)

    def test_increment_invalid_initial(self, cb_env: SyncTestEnvironment,
                                       kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_new_doc(key_only=True)
        with pytest.raises((InvalidArgumentException, ValueError)):
            cb_env.collection.binary().increment(key, IncrementOptions(initial=UNWRAPPED_INITIAL))
        kv_validator.validate_kv_op(OpName.Increment, validate_error=True, error_before_dispatch=True)

    def test_decrement_invalid_initial(self, cb_env: SyncTestEnvironment,
                                       kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_new_doc(key_only=True)
        with pytest.raises((InvalidArgumentException, ValueError)):
            cb_env.collection.binary().decrement(key, DecrementOptions(initial=UNWRAPPED_INITIAL))
        kv_validator.validate_kv_op(OpName.Decrement, validate_error=True, error_before_dispatch=True)

    # ------------------------------------------------------------------
    # Operations handed a parent span
    # ------------------------------------------------------------------
    # The option is `parent_span`; `span` is the name it used to have, still accepted and now
    # deprecated in favour of it.  Each test covers both, and both ways of delivering them.

    def test_append_with_parent(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc_by_type(DocType.BYTES_EMPTY, key_only=True)
        parent = kv_validator.new_parent_span(OpName.Append)
        cb_env.collection.binary().append(key, b'XXX', AppendOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.Append, parent_span=parent)

        parent = kv_validator.new_parent_span(OpName.Append)
        cb_env.collection.binary().append(key, b'XXX', span=parent)
        kv_validator.validate_kv_op(OpName.Append, parent_span=parent)

    def test_prepend_with_parent(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc_by_type(DocType.BYTES_EMPTY, key_only=True)
        parent = kv_validator.new_parent_span(OpName.Prepend)
        cb_env.collection.binary().prepend(key, b'XXX', PrependOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.Prepend, parent_span=parent)

        parent = kv_validator.new_parent_span(OpName.Prepend)
        cb_env.collection.binary().prepend(key, b'XXX', span=parent)
        kv_validator.validate_kv_op(OpName.Prepend, parent_span=parent)

    def test_increment_with_parent(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc_by_type(DocType.COUNTER, key_only=True)
        parent = kv_validator.new_parent_span(OpName.Increment)
        cb_env.collection.binary().increment(key, IncrementOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.Increment, parent_span=parent)

        parent = kv_validator.new_parent_span(OpName.Increment)
        cb_env.collection.binary().increment(key, span=parent)
        kv_validator.validate_kv_op(OpName.Increment, parent_span=parent)

    def test_decrement_with_parent(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc_by_type(DocType.COUNTER, key_only=True)
        parent = kv_validator.new_parent_span(OpName.Decrement)
        cb_env.collection.binary().decrement(key, DecrementOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.Decrement, parent_span=parent)

        parent = kv_validator.new_parent_span(OpName.Decrement)
        cb_env.collection.binary().decrement(key, span=parent)
        kv_validator.validate_kv_op(OpName.Decrement, parent_span=parent)


class BinaryTracingTests(BinaryTracingTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(BinaryTracingTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env', params=[TracingType.Basic,
                                                          TracingType.Legacy,
                                                          TracingType.NoOp,
                                                          TracingType.ThresholdLogging])
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment,
                                   request: pytest.FixtureRequest
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env,
                                BinaryTracingTestSuite,
                                tracer=TracingEnvironment.make_tracer(request.param))
        yield env
        teardown_environment(env, BinaryTracingTestSuite)
