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
from typing import Any

import pytest

from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.observability import OpName
from couchbase.options import (DecrementMultiOptions,
                               IncrementMultiOptions,
                               SignedInt64)
from tests_v2.environments.metrics_environment import MeterType, MetricsEnvironment
from tests_v2.environments.test_environment import (ConnectionStrategy,
                                                    DocType,
                                                    SuiteRequirements,
                                                    SyncTestEnvironment,
                                                    TestProtocol,
                                                    build_environment,
                                                    teardown_environment)
from tests_v2.manifest import validate_test_manifest
from tests_v2.metrics_validators import KeyValueMeterValidator

NUM_KEYS = 3
# A counter that does not exist is created unless the initial is negative, and a created
# counter is a success rather than the failure the error cases are about.
NEGATIVE_INITIAL = SignedInt64(-1)


class BinaryMultiMetricsTestSuite(SuiteRequirements):
    # The binary half of `metrics_kv_multi_t`, and the same claim: a multi operation is measured
    # per key and never as a whole, so three keys through `append_multi` produce three `append`
    # recorders and no `append_multi` recorder.  Sync-only and not generated, because the multi
    # surface is defined on `couchbase.Collection` alone.

    # Observability support in CNG is pending.
    UNSUPPORTED_PROTOCOLS = [TestProtocol.COUCHBASE2]
    # A meter is a cluster option, so the suite needs a cluster of its own.
    CONNECTION_STRATEGY = ConnectionStrategy.NEW_CONNECTION
    # append and prepend need documents that are already empty bytes; increment and decrement
    # need ones holding an int.  Nothing here reads a populated BYTES document.
    DATA_SET = [DocType.BYTES_EMPTY, DocType.COUNTER]
    NUM_DOCS = 10

    TEST_MANIFEST = [
        'test_multi_binary_counter_op',
        'test_multi_binary_counter_op_with_error',
        'test_multi_binary_mutation_op',
        'test_multi_binary_mutation_op_with_error',
        'test_multi_binary_op_not_dispatched',
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
        (OpName.AppendMulti, OpName.Append),
        (OpName.PrependMulti, OpName.Prepend),
    ])
    def test_multi_binary_mutation_op(self,
                                      cb_env: SyncTestEnvironment,
                                      kv_validator: KeyValueMeterValidator,
                                      multi_op: OpName,
                                      sub_op: OpName) -> None:
        keys = cb_env.get_multiple_existing_docs_by_type(DocType.BYTES_EMPTY, NUM_KEYS)
        operation = getattr(cb_env.collection.binary(), multi_op.value)
        operation({key: b'extra' for key in keys})
        kv_validator.validate_multi_kv_op(multi_op, sub_op, NUM_KEYS)

    @pytest.mark.parametrize('multi_op, sub_op', [
        (OpName.AppendMulti, OpName.Append),
        (OpName.PrependMulti, OpName.Prepend),
    ])
    def test_multi_binary_mutation_op_with_error(self,
                                                 cb_env: SyncTestEnvironment,
                                                 kv_validator: KeyValueMeterValidator,
                                                 multi_op: OpName,
                                                 sub_op: OpName) -> None:
        keys = [cb_env.get_new_doc(key_only=True) for _ in range(NUM_KEYS)]
        operation = getattr(cb_env.collection.binary(), multi_op.value)
        operation({key: b'XXX' for key in keys})
        kv_validator.validate_multi_kv_op(multi_op, sub_op, NUM_KEYS, validate_error=True)

    @pytest.mark.parametrize('multi_op, sub_op', [
        (OpName.DecrementMulti, OpName.Decrement),
        (OpName.IncrementMulti, OpName.Increment),
    ])
    def test_multi_binary_counter_op(self,
                                     cb_env: SyncTestEnvironment,
                                     kv_validator: KeyValueMeterValidator,
                                     multi_op: OpName,
                                     sub_op: OpName) -> None:
        keys = cb_env.get_multiple_existing_docs_by_type(DocType.COUNTER, NUM_KEYS)
        operation = getattr(cb_env.collection.binary(), multi_op.value)
        operation(keys)
        kv_validator.validate_multi_kv_op(multi_op, sub_op, NUM_KEYS)

    @pytest.mark.parametrize('multi_op, sub_op', [
        (OpName.DecrementMulti, OpName.Decrement),
        (OpName.IncrementMulti, OpName.Increment),
    ])
    def test_multi_binary_counter_op_with_error(self,
                                                cb_env: SyncTestEnvironment,
                                                kv_validator: KeyValueMeterValidator,
                                                multi_op: OpName,
                                                sub_op: OpName) -> None:
        keys = cb_env.get_new_counter_doc_keys(NUM_KEYS)
        operation = getattr(cb_env.collection.binary(), multi_op.value)
        if multi_op is OpName.IncrementMulti:
            operation(keys, IncrementMultiOptions(initial=NEGATIVE_INITIAL))
        else:
            operation(keys, DecrementMultiOptions(initial=NEGATIVE_INITIAL))
        kv_validator.validate_multi_kv_op(multi_op, sub_op, NUM_KEYS, validate_error=True)

    @pytest.mark.parametrize('multi_op, sub_op, keys_as_mapping', [
        (OpName.AppendMulti, OpName.Append, False),
        (OpName.DecrementMulti, OpName.Decrement, True),
        (OpName.IncrementMulti, OpName.Increment, True),
        (OpName.PrependMulti, OpName.Prepend, False),
    ])
    def test_multi_binary_op_not_dispatched(self,
                                            cb_env: SyncTestEnvironment,
                                            kv_validator: KeyValueMeterValidator,
                                            multi_op: OpName,
                                            sub_op: OpName,
                                            keys_as_mapping: bool) -> None:
        # append and prepend take a mapping and are given a list; increment and decrement take
        # a list and are given a mapping.  Nothing is measured either way: no per-key work
        # happened, and per-key is the only way a multi operation is ever recorded.
        keys = [cb_env.get_new_doc(key_only=True) for _ in range(NUM_KEYS)]
        argument: Any = {key: {'test': 'data'} for key in keys} if keys_as_mapping else keys
        operation = getattr(cb_env.collection.binary(), multi_op.value)
        with pytest.raises((InvalidArgumentException, ValueError)):
            operation(argument)
        kv_validator.validate_multi_kv_op(multi_op, sub_op, expected_count=0)


class BinaryMultiMetricsTests(BinaryMultiMetricsTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(BinaryMultiMetricsTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env', params=[MeterType.Basic, MeterType.NoOp])
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment,
                                   request: pytest.FixtureRequest
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env,
                                BinaryMultiMetricsTestSuite,
                                meter=MetricsEnvironment.make_meter(request.param))
        yield env
        teardown_environment(env, BinaryMultiMetricsTestSuite)
