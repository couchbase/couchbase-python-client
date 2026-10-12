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
from typing import Any, Optional

import pytest

from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.observability import OpName
from couchbase.options import (ExistsMultiOptions,
                               GetAllReplicasMultiOptions,
                               GetAndLockMultiOptions,
                               GetAnyReplicaMultiOptions,
                               GetMultiOptions,
                               InsertMultiOptions,
                               RemoveMultiOptions,
                               ReplaceMultiOptions,
                               TouchMultiOptions,
                               UnlockMultiOptions,
                               UpsertMultiOptions)
from tests.environments.test_environment import (ConnectionStrategy,
                                                 SuiteRequirements,
                                                 SyncTestEnvironment,
                                                 TestProtocol,
                                                 build_environment,
                                                 teardown_environment)
from tests.environments.tracing_environment import TracingEnvironment, TracingType
from tests.manifest import validate_test_manifest
from tests.tracing_validators import KeyValueSpanValidator

NUM_KEYS = 3
ONE_SECOND = timedelta(seconds=1)
HALF_SECOND = timedelta(milliseconds=500)
# The replica reads run the active copy and the replicas as separate operations, each with its
# own span underneath the one the caller asked for, per key.
REPLICA_READ_OPS = [OpName.Get, OpName.GetReplica]


class KeyValueMultiTracingTestSuite(SuiteRequirements):
    # A multi operation gets one span covering every key, the opposite of how it is metered:
    # the meter records per key and never records the multi operation itself.  So every test
    # names the multi operation and how many keys it covered, and the counts underneath scale
    # with that number.
    #
    # Sync-only, and so not generated: `upsert_multi` and the rest are defined on
    # `couchbase.Collection` alone.  That is also why the operations are reached through
    # `getattr` here where the single-key tracing suites name each one literally, the literal
    # naming being there to let the generator type a call's receiver.
    #
    # No `@pytest.mark.flaky` over the class: a blanket rerun hides a real intermittent failure
    # as effectively as it hides a nuisance one.  Where it is warranted it goes on the test.

    # Observability support in CNG is pending.
    UNSUPPORTED_PROTOCOLS = [TestProtocol.COUCHBASE2]
    # A tracer is a cluster option, so the suite needs a cluster of its own.
    CONNECTION_STRATEGY = ConnectionStrategy.NEW_CONNECTION
    # Each drawn key is marked used and never handed back, and most of these cases take three,
    # so the seeded count is a real floor rather than a default worth inheriting silently.
    NUM_DOCS = 150

    TEST_MANIFEST = [
        'test_multi_kv_op',
        'test_multi_kv_op_not_dispatched',
        'test_multi_kv_op_with_error',
        'test_multi_kv_op_with_parent',
        'test_multi_mutation_op',
        'test_multi_mutation_op_with_error',
        'test_multi_mutation_op_with_parent',
        'test_multi_unlock_op',
        'test_multi_unlock_op_with_parent',
    ]

    @pytest.fixture(name='kv_validator')
    def kv_validator_fixture(self, cb_env: SyncTestEnvironment) -> KeyValueSpanValidator:
        validator = TracingEnvironment.kv_span_validator(cb_env)
        validator.reset()
        return validator

    @pytest.mark.parametrize('multi_op, nested_ops', [
        (OpName.ExistsMulti, None),
        (OpName.GetMulti, None),
        (OpName.GetAllReplicasMulti, REPLICA_READ_OPS),
        (OpName.GetAnyReplicaMulti, REPLICA_READ_OPS),
    ])
    def test_multi_kv_op(self,
                         cb_env: SyncTestEnvironment,
                         kv_validator: KeyValueSpanValidator,
                         multi_op: OpName,
                         nested_ops: Optional[list[OpName]]) -> None:
        keys = list(cb_env.get_docs(NUM_KEYS))
        operation = getattr(cb_env.collection, multi_op.value)
        operation(keys)
        kv_validator.validate_kv_op(multi_op, nested_ops=nested_ops, multi_op_key_count=NUM_KEYS)

    @pytest.mark.parametrize('multi_op, nested_ops', [
        (OpName.GetMulti, None),
        (OpName.GetAllReplicasMulti, REPLICA_READ_OPS),
        (OpName.GetAnyReplicaMulti, REPLICA_READ_OPS),
        (OpName.GetAndLockMulti, None),
    ])
    def test_multi_kv_op_with_error(self,
                                    cb_env: SyncTestEnvironment,
                                    kv_validator: KeyValueSpanValidator,
                                    multi_op: OpName,
                                    nested_ops: Optional[list[OpName]]) -> None:
        # `return_exceptions=False` turns every missing key into a raise, and raising is what
        # puts the error status on the span; left to itself a multi operation collects failures
        # into the result instead.
        keys = [cb_env.get_new_doc(key_only=True) for _ in range(NUM_KEYS)]
        # Stopping at the first failure means the later keys are never attempted, so there is
        # one key's worth of spans underneath rather than three.  The deprecated tracer does
        # not short-circuit that way and still produces all three.
        if multi_op is OpName.GetAnyReplicaMulti and not kv_validator.is_legacy:
            key_count = 1
        else:
            key_count = NUM_KEYS
        operation = getattr(cb_env.collection, multi_op.value)
        with pytest.raises(Exception):
            if multi_op is OpName.GetAndLockMulti:
                operation(keys, ONE_SECOND, return_exceptions=False)
            else:
                operation(keys, return_exceptions=False)
        kv_validator.validate_kv_op(multi_op,
                                    nested_ops=nested_ops,
                                    multi_op_key_count=key_count,
                                    validate_error=True)

    @pytest.mark.parametrize('multi_op', [
        OpName.InsertMulti,
        OpName.RemoveMulti,
        OpName.ReplaceMulti,
        OpName.TouchMulti,
        OpName.UpsertMulti,
    ])
    def test_multi_mutation_op(self,
                               cb_env: SyncTestEnvironment,
                               kv_validator: KeyValueSpanValidator,
                               multi_op: OpName) -> None:
        docs: Any
        if multi_op is OpName.InsertMulti:
            docs = cb_env.get_new_docs(NUM_KEYS)
        elif multi_op in (OpName.ReplaceMulti, OpName.UpsertMulti):
            docs = cb_env.get_docs(NUM_KEYS)
        else:
            docs = list(cb_env.get_docs(NUM_KEYS))

        operation = getattr(cb_env.collection, multi_op.value)
        if multi_op is OpName.TouchMulti:
            operation(docs, ONE_SECOND)
        else:
            operation(docs)
        kv_validator.validate_kv_op(multi_op, multi_op_key_count=NUM_KEYS)

    @pytest.mark.parametrize('multi_op', [
        OpName.RemoveMulti,
        OpName.ReplaceMulti,
        OpName.TouchMulti,
    ])
    def test_multi_mutation_op_with_error(self,
                                          cb_env: SyncTestEnvironment,
                                          kv_validator: KeyValueSpanValidator,
                                          multi_op: OpName) -> None:
        # Insert and upsert are absent for a reason rather than an oversight: an insert against
        # a key that does not exist succeeds, and an upsert always does, so neither has a
        # per-key failure to produce.
        docs: Any
        keys = [cb_env.get_new_doc(key_only=True) for _ in range(NUM_KEYS)]
        if multi_op is OpName.ReplaceMulti:
            docs = {key: {'test': 'data'} for key in keys}
        else:
            docs = keys

        operation = getattr(cb_env.collection, multi_op.value)
        with pytest.raises(Exception):
            if multi_op is OpName.TouchMulti:
                operation(docs, ONE_SECOND, return_exceptions=False)
            else:
                operation(docs, return_exceptions=False)
        kv_validator.validate_kv_op(multi_op, multi_op_key_count=NUM_KEYS, validate_error=True)

    def test_multi_unlock_op(self,
                             cb_env: SyncTestEnvironment,
                             kv_validator: KeyValueSpanValidator) -> None:
        # `unlock_multi` needs a cas per key and `get_and_lock_multi` is the only thing that
        # produces them, so the lock is asserted on rather than waved through and the tracer is
        # cleared between the two.
        keys = list(cb_env.get_docs(NUM_KEYS))
        locked = cb_env.collection.get_and_lock_multi(keys, HALF_SECOND)
        kv_validator.validate_kv_op(OpName.GetAndLockMulti, multi_op_key_count=NUM_KEYS)

        kv_validator.reset()
        cb_env.collection.unlock_multi(locked)
        kv_validator.validate_kv_op(OpName.UnlockMulti, multi_op_key_count=NUM_KEYS)

    @pytest.mark.parametrize('multi_op, opts_cls, nested_ops', [
        (OpName.ExistsMulti, ExistsMultiOptions, None),
        (OpName.GetMulti, GetMultiOptions, None),
        (OpName.GetAllReplicasMulti, GetAllReplicasMultiOptions, REPLICA_READ_OPS),
        (OpName.GetAnyReplicaMulti, GetAnyReplicaMultiOptions, REPLICA_READ_OPS),
    ])
    def test_multi_kv_op_with_parent(self,
                                     cb_env: SyncTestEnvironment,
                                     kv_validator: KeyValueSpanValidator,
                                     multi_op: OpName,
                                     opts_cls: Any,
                                     nested_ops: Optional[list[OpName]]) -> None:
        keys = list(cb_env.get_docs(NUM_KEYS))
        parent = kv_validator.new_parent_span(multi_op)
        operation = getattr(cb_env.collection, multi_op.value)
        operation(keys, opts_cls(parent_span=parent))
        kv_validator.validate_kv_op(multi_op,
                                    parent_span=parent,
                                    nested_ops=nested_ops,
                                    multi_op_key_count=NUM_KEYS)

    @pytest.mark.parametrize('multi_op, opts_cls', [
        (OpName.InsertMulti, InsertMultiOptions),
        (OpName.RemoveMulti, RemoveMultiOptions),
        (OpName.ReplaceMulti, ReplaceMultiOptions),
        (OpName.TouchMulti, TouchMultiOptions),
        (OpName.UpsertMulti, UpsertMultiOptions),
    ])
    def test_multi_mutation_op_with_parent(self,
                                           cb_env: SyncTestEnvironment,
                                           kv_validator: KeyValueSpanValidator,
                                           multi_op: OpName,
                                           opts_cls: Any) -> None:
        docs: Any
        if multi_op is OpName.InsertMulti:
            docs = cb_env.get_new_docs(NUM_KEYS)
        elif multi_op in (OpName.ReplaceMulti, OpName.UpsertMulti):
            docs = cb_env.get_docs(NUM_KEYS)
        else:
            docs = list(cb_env.get_docs(NUM_KEYS))

        parent = kv_validator.new_parent_span(multi_op)
        operation = getattr(cb_env.collection, multi_op.value)
        if multi_op is OpName.TouchMulti:
            operation(docs, ONE_SECOND, opts_cls(parent_span=parent))
        else:
            operation(docs, opts_cls(parent_span=parent))
        kv_validator.validate_kv_op(multi_op, parent_span=parent, multi_op_key_count=NUM_KEYS)

    def test_multi_unlock_op_with_parent(self,
                                         cb_env: SyncTestEnvironment,
                                         kv_validator: KeyValueSpanValidator) -> None:
        keys = list(cb_env.get_docs(NUM_KEYS))
        parent = kv_validator.new_parent_span(OpName.GetAndLockMulti)
        lock_opts = GetAndLockMultiOptions(parent_span=parent)  # type: ignore[arg-type]
        locked = cb_env.collection.get_and_lock_multi(keys, HALF_SECOND, lock_opts)
        kv_validator.validate_kv_op(OpName.GetAndLockMulti, parent_span=parent, multi_op_key_count=NUM_KEYS)

        parent = kv_validator.new_parent_span(OpName.UnlockMulti)
        unlock_opts = UnlockMultiOptions(parent_span=parent)  # type: ignore[arg-type]
        cb_env.collection.unlock_multi(locked, unlock_opts)
        kv_validator.validate_kv_op(OpName.UnlockMulti, parent_span=parent, multi_op_key_count=NUM_KEYS)

    @pytest.mark.parametrize('multi_op, keys_as_mapping', [
        (OpName.ExistsMulti, True),
        (OpName.GetMulti, True),
        (OpName.GetAllReplicasMulti, True),
        (OpName.GetAndLockMulti, True),
        (OpName.GetAnyReplicaMulti, True),
        (OpName.InsertMulti, False),
        (OpName.RemoveMulti, True),
        (OpName.ReplaceMulti, False),
        (OpName.TouchMulti, True),
        (OpName.UnlockMulti, True),
        (OpName.UpsertMulti, False),
    ])
    def test_multi_kv_op_not_dispatched(self,
                                        cb_env: SyncTestEnvironment,
                                        kv_validator: KeyValueSpanValidator,
                                        multi_op: OpName,
                                        keys_as_mapping: bool) -> None:
        # An operation taking a list of keys is given a mapping, and one taking a mapping is
        # given a list.  The client rejects it without reaching the bindings, so the request
        # span has nothing underneath it.  `unlock_multi` is the exception and gets no span at
        # all: it reads the cas out of the argument while the options are still being parsed,
        # which is before the span exists.
        keys = [cb_env.get_new_doc(key_only=True) for _ in range(NUM_KEYS)]
        argument: Any = {key: {'test': 'data'} for key in keys} if keys_as_mapping else keys
        operation = getattr(cb_env.collection, multi_op.value)
        with pytest.raises(InvalidArgumentException):
            if multi_op in (OpName.GetAndLockMulti, OpName.TouchMulti):
                operation(argument, ONE_SECOND, return_exceptions=False)
            else:
                operation(argument, return_exceptions=False)
        kv_validator.validate_kv_op(multi_op,
                                    multi_op_key_count=NUM_KEYS,
                                    validate_error=True,
                                    error_before_dispatch=True,
                                    expect_request_span=multi_op is not OpName.UnlockMulti)


class KeyValueMultiTracingTests(KeyValueMultiTracingTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(KeyValueMultiTracingTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env', params=[TracingType.Basic,
                                                          TracingType.Legacy,
                                                          TracingType.NoOp,
                                                          TracingType.ThresholdLogging])
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment,
                                   request: pytest.FixtureRequest
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env,
                                KeyValueMultiTracingTestSuite,
                                tracer=TracingEnvironment.make_tracer(request.param))
        yield env
        teardown_environment(env, KeyValueMultiTracingTestSuite)
