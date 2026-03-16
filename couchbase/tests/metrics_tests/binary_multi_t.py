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

from typing import List

import pytest

from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.observability import OpName
from couchbase.options import SignedInt64
from tests.environments.metrics import BinaryMultiMetricsEnvironment
from tests.environments.metrics.metrics_environment import MeterType


class BinaryMultiMetricsTestsSuite:

    TEST_MANIFEST = [
        'test_multi_binary_mutation_op',
        'test_multi_binary_mutation_op_error',
        'test_multi_binary_counter_op',
        'test_multi_binary_counter_op_error',
        'test_multi_binary_op_no_dispatch_failure',
    ]

    @pytest.fixture(autouse=True)
    def reset_validator(self, cb_env: BinaryMultiMetricsEnvironment):
        yield
        cb_env.kv_meter_validator.reset()

    @pytest.mark.parametrize('op_name, nested_ops',
                             [(OpName.AppendMulti, [OpName.Append]),
                              (OpName.PrependMulti, [OpName.Prepend]),
                              ])
    def test_multi_binary_mutation_op(
        self,
        cb_env: BinaryMultiMetricsEnvironment,
        op_name: OpName,
        nested_ops: List[OpName]
    ) -> None:
        keys = cb_env.get_multiple_existing_binary_docs_by_type('bytes_empty', 3)
        docs = {k: b'extra' for k in keys}

        validator = cb_env.kv_meter_validator
        validator.reset(op_name=op_name, clear_parent_span=True, nested_ops=nested_ops*3)
        operation = getattr(cb_env.collection.binary(), op_name.value)
        operation(docs)
        validator.validate_multi_kv_op()

    @pytest.mark.parametrize('op_name, nested_ops',
                             [(OpName.AppendMulti, [OpName.Append]),
                              (OpName.PrependMulti, [OpName.Prepend]),
                              ])
    def test_multi_binary_mutation_op_error(
        self,
        cb_env: BinaryMultiMetricsEnvironment,
        op_name: OpName,
        nested_ops: List[OpName]
    ) -> None:
        missing_keys = [cb_env.get_new_doc(key_only=True) for _ in range(3)]
        docs = {k: b'XXX' for k in missing_keys}

        validator = cb_env.kv_meter_validator
        validator.reset(op_name=op_name, nested_ops=nested_ops*3, validate_error=True)
        operation = getattr(cb_env.collection.binary(), op_name.value)
        try:
            operation(docs)
        except Exception:
            pass
        validator.validate_multi_kv_op()

    @pytest.mark.parametrize('op_name, nested_ops',
                             [(OpName.IncrementMulti, [OpName.Increment]),
                              (OpName.DecrementMulti, [OpName.Decrement]),
                              ])
    def test_multi_binary_counter_op(
        self,
        cb_env: BinaryMultiMetricsEnvironment,
        op_name: OpName,
        nested_ops: List[OpName]
    ) -> None:
        keys = cb_env.get_multiple_existing_binary_docs_by_type('counter', 3)

        validator = cb_env.kv_meter_validator
        validator.reset(op_name=op_name, nested_ops=nested_ops*3)
        operation = getattr(cb_env.collection.binary(), op_name.value)
        operation(keys)
        validator.validate_multi_kv_op()

    @pytest.mark.parametrize('op_name, nested_ops',
                             [(OpName.IncrementMulti, [OpName.Increment]),
                              (OpName.DecrementMulti, [OpName.Decrement]),
                              ])
    def test_multi_binary_counter_op_error(
        self,
        cb_env: BinaryMultiMetricsEnvironment,
        op_name: OpName,
        nested_ops: List[OpName]
    ) -> None:
        keys = [cb_env.get_new_doc(key_only=True) for _ in range(3)]

        validator = cb_env.kv_meter_validator
        validator.reset(op_name=op_name, nested_ops=nested_ops*3, validate_error=True)
        operation = getattr(cb_env.collection.binary(), op_name.value)
        try:
            # if we don't provide the negative initial value, the counter doc will be created
            operation(keys, initial=SignedInt64(-1))
        except Exception:
            pass
        validator.validate_multi_kv_op()

    @pytest.mark.parametrize('op_name, keys_as_list, nested_ops', [
        (OpName.AppendMulti, False, [OpName.Append]),
        (OpName.DecrementMulti, True, [OpName.Decrement]),
        (OpName.IncrementMulti, True, [OpName.Increment]),
        (OpName.PrependMulti, False, [OpName.Prepend]),
    ])
    def test_multi_binary_op_no_dispatch_failure(
        self,
        cb_env: BinaryMultiMetricsEnvironment,
        op_name: OpName,
        keys_as_list: bool,
        nested_ops: List[OpName]
    ) -> None:
        keys = [cb_env.get_new_doc(key_only=True) for _ in range(3)]
        validator = cb_env.kv_meter_validator
        validator.reset(op_name=op_name,
                        nested_ops=nested_ops*len(keys),
                        validate_error=True,
                        error_before_dispatch=True)
        operation = getattr(cb_env.collection.binary(), op_name.value)
        try:
            # we do the opposite of what the op wants to trigger an InvalidArgumentException
            if keys_as_list:
                keys_and_docs = {key: {'test': 'data'} for key in keys}
                operation(keys_and_docs)
            else:
                operation(keys)
        except (InvalidArgumentException, ValueError):
            pass
        validator.validate_multi_kv_op()

# exceptions are handled differently for multi-ops.  It is easy to just let a test rerun than to try and flesh out
# the error in one of the ops.


@pytest.mark.flaky(reruns=5, reruns_delay=1)
class ClassicBinaryMultiMetricsTests(BinaryMultiMetricsTestsSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicBinaryMultiMetricsTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicBinaryMultiMetricsTests) if valid_test_method(meth)]
        test_list = set(BinaryMultiMetricsTestsSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='cb_env', params=[MeterType.Basic, MeterType.NoOp])
    def couchbase_test_environment(self, cb_base_env, request):
        # a new environment and cluster is created
        cb_env = BinaryMultiMetricsEnvironment.from_environment(cb_base_env, meter_type=request.param)
        cb_env.setup_binary_data()
        yield cb_env
        cb_env.teardown()
