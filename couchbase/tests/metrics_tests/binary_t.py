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

from typing import Any, Optional

import pytest

from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.observability import OpName
from couchbase.options import (DecrementOptions,
                               IncrementOptions,
                               SignedInt64)
from tests.environments.metrics import BinaryMetricsEnvironment
from tests.environments.metrics.metrics_environment import MeterType


class BinaryMetricsTestsSuite:

    TEST_MANIFEST = [
        'test_binary_mutation_op',
        'test_binary_mutation_op_error',
        'test_binary_counter_op',
        'test_binary_counter_op_error',
        'test_binary_op_no_dispatch_failure',
    ]

    @pytest.fixture(autouse=True)
    def reset_validator(self, cb_env: BinaryMetricsEnvironment):
        yield
        cb_env.kv_meter_validator.reset()

    @pytest.mark.parametrize('op_name', [OpName.Append, OpName.Prepend])
    def test_binary_mutation_op(self, cb_env: BinaryMetricsEnvironment, op_name: OpName) -> None:
        key = cb_env.get_existing_binary_doc_by_type('bytes_empty', key_only=True)

        validator = cb_env.kv_meter_validator
        validator.reset(op_name=op_name)
        operation = getattr(cb_env.collection.binary(), op_name.value)
        operation(key, b'XXX')
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name', [OpName.Append, OpName.Prepend])
    def test_binary_mutation_op_error(self, cb_env: BinaryMetricsEnvironment, op_name: OpName) -> None:
        key = 'not-a-key'

        validator = cb_env.kv_meter_validator
        validator.reset(op_name=op_name, validate_error=True)
        operation = getattr(cb_env.collection.binary(), op_name.value)
        try:
            operation(key, b'XXX')
        except Exception:
            pass
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name', [OpName.Increment, OpName.Decrement])
    def test_binary_counter_op(self, cb_env: BinaryMetricsEnvironment, op_name: OpName) -> None:
        key = cb_env.get_existing_binary_doc_by_type('counter', key_only=True)

        validator = cb_env.kv_meter_validator
        validator.reset(op_name=op_name)
        operation = getattr(cb_env.collection.binary(), op_name.value)
        operation(key)
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name', [OpName.Increment, OpName.Decrement])
    def test_binary_counter_op_error(self, cb_env: BinaryMetricsEnvironment, op_name: OpName) -> None:
        key = 'not-a-key'

        validator = cb_env.kv_meter_validator
        validator.reset(op_name=op_name, validate_error=True)
        operation = getattr(cb_env.collection.binary(), op_name.value)
        try:
            # if we don't provide the negative initial value, the counter doc will be created
            operation(key, initial=SignedInt64(-1))
        except Exception:
            pass
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name, opts', [
        (OpName.Append, None),
        (OpName.Decrement, DecrementOptions),
        (OpName.Increment, IncrementOptions),
        (OpName.Prepend, None),
    ])
    def test_binary_op_no_dispatch_failure(
        self,
        cb_env: BinaryMetricsEnvironment,
        op_name: OpName,
        opts: Optional[Any]
    ) -> None:

        key = cb_env.get_new_doc(key_only=True)
        validator = cb_env.kv_meter_validator
        validator.reset(op_name=op_name, validate_error=True)
        operation = getattr(cb_env.collection.binary(), op_name.value)
        try:
            if op_name is OpName.Append or op_name is OpName.Prepend:
                operation(key, 123)
            else:
                operation(key, opts(initial=123))
        except (InvalidArgumentException, ValueError):
            pass

        validator.validate_kv_op()


class ClassicBinaryMetricsTests(BinaryMetricsTestsSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicBinaryMetricsTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicBinaryMetricsTests) if valid_test_method(meth)]
        test_list = set(BinaryMetricsTestsSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='cb_env', params=[MeterType.Basic,
                                                          MeterType.NoOp,
                                                          ])
    def couchbase_test_environment(self, cb_base_env, request):
        # a new environment and cluster is created
        cb_env = BinaryMetricsEnvironment.from_environment(cb_base_env, meter_type=request.param)
        cb_env.setup_binary_data()
        cb_env.enable_bucket_mgmt()
        yield cb_env
        cb_env.disable_bucket_mgmt()
        cb_env.teardown()
