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
from couchbase.options import (AppendMultiOptions,
                               DecrementMultiOptions,
                               IncrementMultiOptions,
                               PrependMultiOptions,
                               SignedInt64)
from tests.environments.test_environment import (ConnectionStrategy,
                                                 DocType,
                                                 SuiteRequirements,
                                                 SyncTestEnvironment,
                                                 TestProtocol,
                                                 build_environment,
                                                 teardown_environment)
from tests.environments.tracing_environment import TracingEnvironment, TracingType
from tests.manifest import validate_test_manifest
from tests.tracing_validators import KeyValueSpanValidator

NUM_KEYS = 3
# Without a negative initial the server creates the counter document and the operation
# succeeds, which is a different test.
NEGATIVE_INITIAL = SignedInt64(-1)


class BinaryMultiTracingTestSuite(SuiteRequirements):
    # The binary half of `tracing_kv_multi_t`, and sync-only for the same reason: the multi
    # surface is defined on `couchbase.Collection` alone, which is also why the operations are
    # reached through `getattr` rather than named literally.
    #
    # No `@pytest.mark.flaky` over the class: a blanket rerun hides a real intermittent failure
    # as effectively as it hides a nuisance one.  Where it is warranted it goes on the test.

    # Observability support in CNG is pending.
    UNSUPPORTED_PROTOCOLS = [TestProtocol.COUCHBASE2]
    # A tracer is a cluster option, so the suite needs a cluster of its own.
    CONNECTION_STRATEGY = ConnectionStrategy.NEW_CONNECTION
    DATA_SET = [DocType.BYTES_EMPTY, DocType.COUNTER]
    # Twelve keys of each type are drawn and never handed back, so the seeded count is a real
    # floor here rather than a default worth inheriting silently.
    NUM_DOCS = 30

    TEST_MANIFEST = [
        'test_multi_binary_counter_op',
        'test_multi_binary_counter_op_with_error',
        'test_multi_binary_counter_op_with_parent',
        'test_multi_binary_mutation_op',
        'test_multi_binary_mutation_op_with_error',
        'test_multi_binary_mutation_op_with_parent',
        'test_multi_binary_op_not_dispatched',
    ]

    @pytest.fixture(name='kv_validator')
    def kv_validator_fixture(self, cb_env: SyncTestEnvironment) -> KeyValueSpanValidator:
        validator = TracingEnvironment.kv_span_validator(cb_env)
        validator.reset()
        return validator

    @pytest.mark.parametrize('multi_op', [OpName.AppendMulti, OpName.PrependMulti])
    def test_multi_binary_mutation_op(self,
                                      cb_env: SyncTestEnvironment,
                                      kv_validator: KeyValueSpanValidator,
                                      multi_op: OpName) -> None:
        keys = cb_env.get_multiple_existing_docs_by_type(DocType.BYTES_EMPTY, NUM_KEYS)
        operation = getattr(cb_env.collection.binary(), multi_op.value)
        operation({key: b'XXX' for key in keys})
        kv_validator.validate_kv_op(multi_op, multi_op_key_count=NUM_KEYS)

    @pytest.mark.parametrize('multi_op', [OpName.DecrementMulti, OpName.IncrementMulti])
    def test_multi_binary_counter_op(self,
                                     cb_env: SyncTestEnvironment,
                                     kv_validator: KeyValueSpanValidator,
                                     multi_op: OpName) -> None:
        keys = cb_env.get_multiple_existing_docs_by_type(DocType.COUNTER, NUM_KEYS)
        operation = getattr(cb_env.collection.binary(), multi_op.value)
        operation(keys)
        kv_validator.validate_kv_op(multi_op, multi_op_key_count=NUM_KEYS)

    @pytest.mark.parametrize('multi_op', [OpName.AppendMulti, OpName.PrependMulti])
    def test_multi_binary_mutation_op_with_error(self,
                                                 cb_env: SyncTestEnvironment,
                                                 kv_validator: KeyValueSpanValidator,
                                                 multi_op: OpName) -> None:
        keys = [cb_env.get_new_doc(key_only=True) for _ in range(NUM_KEYS)]
        operation = getattr(cb_env.collection.binary(), multi_op.value)
        with pytest.raises(Exception):
            operation({key: b'XXX' for key in keys}, return_exceptions=False)
        kv_validator.validate_kv_op(multi_op, multi_op_key_count=NUM_KEYS, validate_error=True)

    @pytest.mark.parametrize('multi_op', [OpName.DecrementMulti, OpName.IncrementMulti])
    def test_multi_binary_counter_op_with_error(self,
                                                cb_env: SyncTestEnvironment,
                                                kv_validator: KeyValueSpanValidator,
                                                multi_op: OpName) -> None:
        keys = [cb_env.get_new_doc(key_only=True) for _ in range(NUM_KEYS)]
        operation = getattr(cb_env.collection.binary(), multi_op.value)
        with pytest.raises(Exception):
            operation(keys, initial=NEGATIVE_INITIAL, return_exceptions=False)
        kv_validator.validate_kv_op(multi_op, multi_op_key_count=NUM_KEYS, validate_error=True)

    @pytest.mark.parametrize('multi_op, opts_cls', [
        (OpName.AppendMulti, AppendMultiOptions),
        (OpName.PrependMulti, PrependMultiOptions),
    ])
    def test_multi_binary_mutation_op_with_parent(self,
                                                  cb_env: SyncTestEnvironment,
                                                  kv_validator: KeyValueSpanValidator,
                                                  multi_op: OpName,
                                                  opts_cls: Any) -> None:
        keys = cb_env.get_multiple_existing_docs_by_type(DocType.BYTES_EMPTY, NUM_KEYS)
        operation = getattr(cb_env.collection.binary(), multi_op.value)

        parent = kv_validator.new_parent_span(multi_op)
        operation({key: b'XXX' for key in keys}, opts_cls(parent_span=parent))
        kv_validator.validate_kv_op(multi_op, parent_span=parent, multi_op_key_count=NUM_KEYS)

        parent = kv_validator.new_parent_span(multi_op)
        operation({key: b'extra' for key in keys}, parent_span=parent)
        kv_validator.validate_kv_op(multi_op, parent_span=parent, multi_op_key_count=NUM_KEYS)

    @pytest.mark.parametrize('multi_op, opts_cls', [
        (OpName.DecrementMulti, DecrementMultiOptions),
        (OpName.IncrementMulti, IncrementMultiOptions),
    ])
    def test_multi_binary_counter_op_with_parent(self,
                                                 cb_env: SyncTestEnvironment,
                                                 kv_validator: KeyValueSpanValidator,
                                                 multi_op: OpName,
                                                 opts_cls: Any) -> None:
        keys = cb_env.get_multiple_existing_docs_by_type(DocType.COUNTER, NUM_KEYS)
        operation = getattr(cb_env.collection.binary(), multi_op.value)

        parent = kv_validator.new_parent_span(multi_op)
        operation(keys, opts_cls(parent_span=parent))
        kv_validator.validate_kv_op(multi_op, parent_span=parent, multi_op_key_count=NUM_KEYS)

        parent = kv_validator.new_parent_span(multi_op)
        operation(keys, parent_span=parent)
        kv_validator.validate_kv_op(multi_op, parent_span=parent, multi_op_key_count=NUM_KEYS)

    @pytest.mark.parametrize('multi_op, keys_as_mapping', [
        (OpName.AppendMulti, False),
        (OpName.DecrementMulti, True),
        (OpName.IncrementMulti, True),
        (OpName.PrependMulti, False),
    ])
    def test_multi_binary_op_not_dispatched(self,
                                            cb_env: SyncTestEnvironment,
                                            kv_validator: KeyValueSpanValidator,
                                            multi_op: OpName,
                                            keys_as_mapping: bool) -> None:
        # Handed the wrong shape, and so refused before the per-key work begins.
        keys = [cb_env.get_new_doc(key_only=True) for _ in range(NUM_KEYS)]
        argument: Any = {key: {'test': 'data'} for key in keys} if keys_as_mapping else keys
        operation = getattr(cb_env.collection.binary(), multi_op.value)
        with pytest.raises((InvalidArgumentException, ValueError)):
            operation(argument, return_exceptions=False)
        kv_validator.validate_kv_op(multi_op,
                                    multi_op_key_count=NUM_KEYS,
                                    validate_error=True,
                                    error_before_dispatch=True)


class BinaryMultiTracingTests(BinaryMultiTracingTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(BinaryMultiTracingTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env', params=[TracingType.Basic,
                                                          TracingType.Legacy,
                                                          TracingType.NoOp,
                                                          TracingType.ThresholdLogging])
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment,
                                   request: pytest.FixtureRequest
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env,
                                BinaryMultiTracingTestSuite,
                                tracer=TracingEnvironment.make_tracer(request.param))
        yield env
        teardown_environment(env, BinaryMultiTracingTestSuite)
