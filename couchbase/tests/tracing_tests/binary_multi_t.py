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

from typing import Any

import pytest

from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.observability import OpName
from couchbase.options import (AppendMultiOptions,
                               DecrementMultiOptions,
                               IncrementMultiOptions,
                               PrependMultiOptions,
                               SignedInt64)
from tests.environments.tracing import BinaryMultiTracingEnvironment
from tests.environments.tracing.base_tracing_environment import TracingType


class BinaryMultiTracingTestsSuite:

    TEST_MANIFEST = [
        'test_multi_binary_mutation_op',
        'test_multi_binary_mutation_op_with_parent',
        'test_multi_binary_mutation_op_error',
        'test_multi_binary_counter_op',
        'test_multi_binary_counter_op_with_parent',
        'test_multi_binary_counter_op_error',
        'test_multi_binary_op_no_dispatch_failure',
    ]

    @pytest.fixture(autouse=True)
    def reset_validator(self, cb_env: BinaryMultiTracingEnvironment):
        yield
        cb_env.kv_span_validator.reset()

    @pytest.mark.parametrize('op_name', [OpName.AppendMulti, OpName.PrependMulti])
    def test_multi_binary_mutation_op(
        self,
        cb_env: BinaryMultiTracingEnvironment,
        op_name: OpName
    ) -> None:
        keys = cb_env.get_multiple_existing_binary_docs_by_type('bytes_empty', 3)
        docs = {k: b'extra' for k in keys}

        validator = cb_env.kv_span_validator
        validator.reset(op_name=op_name, clear_parent_span=True, multi_op_key_count=3)
        operation = getattr(cb_env.collection.binary(), op_name.value)
        operation(docs)
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name, opts_cls', [
        (OpName.AppendMulti, AppendMultiOptions),
        (OpName.PrependMulti, PrependMultiOptions),
    ])
    def test_multi_binary_mutation_op_with_parent(
        self,
        cb_env: BinaryMultiTracingEnvironment,
        op_name: OpName,
        opts_cls: Any
    ) -> None:
        keys = cb_env.get_multiple_existing_binary_docs_by_type('bytes_empty', 3)
        docs = {k: b'XXX' for k in keys}

        validator = cb_env.kv_span_validator
        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(do_not_clear_spans=True, op_name=op_name, parent_span=parent_span, multi_op_key_count=3)
        operation = getattr(cb_env.collection.binary(), op_name.value)
        operation(docs, opts_cls(parent_span=parent_span))
        validator.validate_kv_op(end_parent=True)

        # span in kwargs
        docs = {k: b'extra' for k in keys}
        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(do_not_clear_spans=True, parent_span=parent_span, multi_op_key_count=3)
        operation(docs, parent_span=parent_span)
        validator.validate_kv_op(end_parent=True)

    @pytest.mark.parametrize('op_name', [OpName.AppendMulti, OpName.PrependMulti])
    def test_multi_binary_mutation_op_error(
        self,
        cb_env: BinaryMultiTracingEnvironment,
        op_name: OpName
    ) -> None:
        missing_keys = [cb_env.get_new_doc(key_only=True) for _ in range(3)]
        docs = {k: b'XXX' for k in missing_keys}

        validator = cb_env.kv_span_validator
        validator.reset(op_name=op_name, clear_parent_span=True, multi_op_key_count=3, validate_error=True)
        operation = getattr(cb_env.collection.binary(), op_name.value)
        try:
            operation(docs, return_exceptions=False)
        except Exception:
            pass
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name', [OpName.IncrementMulti, OpName.DecrementMulti])
    def test_multi_binary_counter_op(
        self,
        cb_env: BinaryMultiTracingEnvironment,
        op_name: OpName
    ) -> None:
        keys = cb_env.get_multiple_existing_binary_docs_by_type('counter', 3)

        validator = cb_env.kv_span_validator
        validator.reset(op_name=op_name, clear_parent_span=True, multi_op_key_count=3)
        operation = getattr(cb_env.collection.binary(), op_name.value)
        operation(keys)
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name, opts_cls', [
        (OpName.IncrementMulti, IncrementMultiOptions),
        (OpName.DecrementMulti, DecrementMultiOptions),
    ])
    def test_multi_binary_counter_op_with_parent(
        self,
        cb_env: BinaryMultiTracingEnvironment,
        op_name: OpName,
        opts_cls: Any
    ) -> None:
        keys = cb_env.get_multiple_existing_binary_docs_by_type('counter', 3)

        validator = cb_env.kv_span_validator
        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(do_not_clear_spans=True, op_name=op_name, parent_span=parent_span, multi_op_key_count=3)
        operation = getattr(cb_env.collection.binary(), op_name.value)
        operation(keys, opts_cls(parent_span=parent_span))
        validator.validate_kv_op(end_parent=True)

        # span in kwargs
        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(do_not_clear_spans=True, parent_span=parent_span, multi_op_key_count=3)
        operation(keys, parent_span=parent_span)
        validator.validate_kv_op(end_parent=True)

    @pytest.mark.parametrize('op_name', [OpName.DecrementMulti, OpName.IncrementMulti])
    def test_multi_binary_counter_op_error(
        self,
        cb_env: BinaryMultiTracingEnvironment,
        op_name: OpName
    ) -> None:
        keys = [cb_env.get_new_doc(key_only=True) for _ in range(3)]

        validator = cb_env.kv_span_validator
        validator.reset(op_name=op_name, clear_parent_span=True, multi_op_key_count=3, validate_error=True)
        operation = getattr(cb_env.collection.binary(), op_name.value)
        try:
            # if we don't provide the negative initial value, the counter doc will be created
            operation(keys, initial=SignedInt64(-1), return_exceptions=False)
        except Exception:
            pass
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name, keys_as_list', [
        (OpName.AppendMulti, False),
        (OpName.DecrementMulti, True),
        (OpName.IncrementMulti, True),
        (OpName.PrependMulti, False),
    ])
    def test_multi_binary_op_no_dispatch_failure(
        self,
        cb_env: BinaryMultiTracingEnvironment,
        op_name: OpName,
        keys_as_list: bool
    ) -> None:
        keys = [cb_env.get_new_doc(key_only=True) for _ in range(3)]
        validator = cb_env.kv_span_validator
        validator.reset(op_name=op_name,
                        multi_op_key_count=len(keys),
                        validate_error=True,
                        error_before_dispatch=True)
        operation = getattr(cb_env.collection.binary(), op_name.value)
        try:
            # we do the opposite of what the op wants to trigger an InvalidArgumentException
            if keys_as_list:
                keys_and_docs = {key: {'test': 'data'} for key in keys}
                operation(keys_and_docs, return_exceptions=False)
            else:
                operation(keys, return_exceptions=False)
        except (InvalidArgumentException, ValueError):
            pass
        validator.validate_kv_op()

# exceptions are handled differently for multi-ops.  It is easy to just let a test rerun than to try and flesh out
# the error in one of the ops.


@pytest.mark.flaky(reruns=5, reruns_delay=1)
class ClassicBinaryMultiTracingTests(BinaryMultiTracingTestsSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicBinaryMultiTracingTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicBinaryMultiTracingTests) if valid_test_method(meth)]
        test_list = set(BinaryMultiTracingTestsSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='cb_env', params=[TracingType.ThresholdLogging,
                                                          TracingType.Legacy,
                                                          TracingType.NoOp,
                                                          TracingType.Modern
                                                          ])
    def couchbase_test_environment(self, cb_base_env, request):
        # a new environment and cluster is created
        cb_env = BinaryMultiTracingEnvironment.from_environment(cb_base_env, tracing_type=request.param)
        cb_env.setup_binary_data()
        yield cb_env
        cb_env.teardown()
        # TODO(PYCBC-1746): Update once legacy tracing logic is removed.
        #                   See PYCBC-1748 for details on issue, this seems to mainly impact the error scenarios.
        if request.param == TracingType.Legacy:
            cb_env.cluster.close()
