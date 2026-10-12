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

from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.observability import OpName
from tests.environments.metrics_environment import MeterType, MetricsEnvironment
from tests.environments.test_environment import (ConnectionStrategy,
                                                 SuiteRequirements,
                                                 SyncTestEnvironment,
                                                 TestProtocol,
                                                 build_environment,
                                                 teardown_environment)
from tests.manifest import validate_test_manifest
from tests.metrics_validators import KeyValueMeterValidator

NUM_KEYS = 3
ONE_SECOND = timedelta(seconds=1)
HALF_SECOND = timedelta(milliseconds=500)


class KeyValueMultiMetricsTestSuite(SuiteRequirements):
    # A multi operation is measured per key and never as a whole: three keys through
    # `get_multi` produce three `get` recorders and no `get_multi` recorder.  So every test
    # names both, the multi operation that must be absent and the single operation each
    # recorder must carry, which is what the single-key suites cannot assert.
    #
    # Sync-only, and so not generated: `upsert_multi` and the rest are defined on
    # `couchbase.Collection` alone.  That is also why the operations are reached through
    # `getattr` here where the single-key suites name each one literally, the literal naming
    # being there to let the generator type a call's receiver.

    # Observability support in CNG is pending.
    UNSUPPORTED_PROTOCOLS = [TestProtocol.COUCHBASE2]
    # A meter is a cluster option, so the suite needs a cluster of its own.
    CONNECTION_STRATEGY = ConnectionStrategy.NEW_CONNECTION
    # Each drawn key is marked used and never handed back, and nine of these cases take three,
    # so the seeded count is a real floor rather than a default worth inheriting silently.
    NUM_DOCS = 50

    TEST_MANIFEST = [
        'test_multi_kv_op',
        'test_multi_kv_op_not_dispatched',
        'test_multi_kv_op_with_error',
        'test_multi_mutation_op',
        'test_multi_unlock_op',
    ]

    @pytest.fixture(name='kv_validator')
    def kv_validator_fixture(self, cb_env: SyncTestEnvironment) -> KeyValueMeterValidator:
        # Cleared here rather than in a teardown so that each test's assertion is about its
        # own operation: the meter is class-scoped and has already recorded every upsert
        # `DATA_SET` seeding performed by the time the first test runs.
        validator = MetricsEnvironment.kv_meter_validator(cb_env)
        validator.reset()
        return validator

    @pytest.mark.parametrize('multi_op, sub_op', [
        (OpName.ExistsMulti, OpName.Exists),
        (OpName.GetMulti, OpName.Get),
        (OpName.GetAllReplicasMulti, OpName.GetAllReplicas),
        (OpName.GetAnyReplicaMulti, OpName.GetAnyReplica),
    ])
    def test_multi_kv_op(self,
                         cb_env: SyncTestEnvironment,
                         kv_validator: KeyValueMeterValidator,
                         multi_op: OpName,
                         sub_op: OpName) -> None:
        keys = list(cb_env.get_docs(NUM_KEYS))
        operation = getattr(cb_env.collection, multi_op.value)
        operation(keys)
        kv_validator.validate_multi_kv_op(multi_op, sub_op, NUM_KEYS)

    @pytest.mark.parametrize('multi_op, sub_op', [
        (OpName.GetMulti, OpName.Get),
        (OpName.GetAllReplicasMulti, OpName.GetAllReplicas),
        (OpName.GetAnyReplicaMulti, OpName.GetAnyReplica),
        (OpName.GetAndLockMulti, OpName.GetAndLock),
    ])
    def test_multi_kv_op_with_error(self,
                                    cb_env: SyncTestEnvironment,
                                    kv_validator: KeyValueMeterValidator,
                                    multi_op: OpName,
                                    sub_op: OpName) -> None:
        # No `pytest.raises`: a multi operation collects its failures into the result rather
        # than raising, which is what `return_exceptions` is for.  Under test is that each
        # failed key was still measured.
        keys = [cb_env.get_new_doc(key_only=True) for _ in range(NUM_KEYS)]
        operation = getattr(cb_env.collection, multi_op.value)
        if multi_op is OpName.GetAndLockMulti:
            operation(keys, ONE_SECOND)
        else:
            operation(keys)
        kv_validator.validate_multi_kv_op(multi_op, sub_op, NUM_KEYS, validate_error=True)

    @pytest.mark.parametrize('multi_op, sub_op', [
        (OpName.InsertMulti, OpName.Insert),
        (OpName.RemoveMulti, OpName.Remove),
        (OpName.ReplaceMulti, OpName.Replace),
        (OpName.TouchMulti, OpName.Touch),
        (OpName.UpsertMulti, OpName.Upsert),
    ])
    def test_multi_mutation_op(self,
                               cb_env: SyncTestEnvironment,
                               kv_validator: KeyValueMeterValidator,
                               multi_op: OpName,
                               sub_op: OpName) -> None:
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
        kv_validator.validate_multi_kv_op(multi_op, sub_op, NUM_KEYS)

    def test_multi_unlock_op(self,
                             cb_env: SyncTestEnvironment,
                             kv_validator: KeyValueMeterValidator) -> None:
        # `unlock_multi` needs a cas per key and `get_and_lock_multi` is the only thing that
        # produces them, so the lock is asserted on rather than waved through and the meter is
        # cleared between the two.
        keys = list(cb_env.get_docs(NUM_KEYS))
        locked = cb_env.collection.get_and_lock_multi(keys, HALF_SECOND)
        kv_validator.validate_multi_kv_op(OpName.GetAndLockMulti, OpName.GetAndLock, NUM_KEYS)

        kv_validator.reset()
        cb_env.collection.unlock_multi(locked)
        kv_validator.validate_multi_kv_op(OpName.UnlockMulti, OpName.Unlock, NUM_KEYS)

    @pytest.mark.parametrize('multi_op, sub_op, keys_as_mapping', [
        (OpName.ExistsMulti, OpName.Exists, True),
        (OpName.GetMulti, OpName.Get, True),
        (OpName.GetAllReplicasMulti, OpName.GetAllReplicas, True),
        (OpName.GetAndLockMulti, OpName.GetAndLock, True),
        (OpName.GetAnyReplicaMulti, OpName.GetAnyReplica, True),
        (OpName.InsertMulti, OpName.Insert, False),
        (OpName.RemoveMulti, OpName.Remove, True),
        (OpName.ReplaceMulti, OpName.Replace, False),
        (OpName.TouchMulti, OpName.Touch, True),
        (OpName.UnlockMulti, OpName.Unlock, True),
        (OpName.UpsertMulti, OpName.Upsert, False),
    ])
    def test_multi_kv_op_not_dispatched(self,
                                        cb_env: SyncTestEnvironment,
                                        kv_validator: KeyValueMeterValidator,
                                        multi_op: OpName,
                                        sub_op: OpName,
                                        keys_as_mapping: bool) -> None:
        # An operation taking a list of keys is given a mapping, and one taking a mapping is
        # given a list.  The client rejects it without reaching the bindings, so unlike the
        # single-key suites there is nothing to measure at all: no per-key work happened, and
        # per-key is the only way a multi operation is ever recorded.
        keys = [cb_env.get_new_doc(key_only=True) for _ in range(NUM_KEYS)]
        argument: Any = {key: {'test': 'data'} for key in keys} if keys_as_mapping else keys
        operation = getattr(cb_env.collection, multi_op.value)
        with pytest.raises(InvalidArgumentException):
            if multi_op in (OpName.GetAndLockMulti, OpName.TouchMulti):
                operation(argument, ONE_SECOND, return_exceptions=False)
            else:
                operation(argument, return_exceptions=False)
        kv_validator.validate_multi_kv_op(multi_op, sub_op, expected_count=0)


class KeyValueMultiMetricsTests(KeyValueMultiMetricsTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(KeyValueMultiMetricsTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env', params=[MeterType.Basic, MeterType.NoOp])
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment,
                                   request: pytest.FixtureRequest
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env,
                                KeyValueMultiMetricsTestSuite,
                                meter=MetricsEnvironment.make_meter(request.param))
        yield env
        teardown_environment(env, KeyValueMultiMetricsTestSuite)
