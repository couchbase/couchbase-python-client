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
import pytest_asyncio

from couchbase.durability import DurabilityLevel, ServerDurability
from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.observability import OpName
from couchbase.options import (AppendOptions,
                               DecrementOptions,
                               IncrementOptions,
                               PrependOptions,
                               SignedInt64)
from tests.environments.tracing import AsyncBinaryTracingEnvironment
from tests.environments.tracing.base_tracing_environment import TracingType
from tests.test_features import EnvironmentFeatures


class AsyncBinaryTracingTestsSuite:

    TEST_MANIFEST = [
        'test_binary_mutation_op',
        'test_binary_mutation_op_with_durability',
        'test_binary_mutation_op_with_error',
        'test_binary_mutation_op_with_parent',
        'test_binary_counter_op',
        'test_binary_counter_op_with_durability',
        'test_binary_counter_op_with_error',
        'test_binary_counter_op_with_parent',
        'test_binary_op_no_dispatch_failure',
    ]

    @pytest.fixture(autouse=True)
    def reset_validator(self, acb_env: AsyncBinaryTracingEnvironment):
        yield
        acb_env.kv_span_validator.reset()

    @pytest.fixture(scope='class')
    def check_has_replicas(self, num_replicas):
        if num_replicas == 0:
            pytest.skip("No replicas to test durability.")

    @pytest.fixture(scope='class')
    def check_multi_node(self, num_nodes):
        if num_nodes == 1:
            pytest.skip("Test only for clusters with more than a single node.")

    @pytest.fixture(scope='class')
    def check_sync_durability_supported(self, acb_env: AsyncBinaryTracingEnvironment):
        EnvironmentFeatures.check_if_feature_supported('sync_durability',
                                                       acb_env.server_version_short,
                                                       acb_env.mock_server_type)

    @pytest.fixture(scope='class')
    def num_nodes(self, acb_env: AsyncBinaryTracingEnvironment):
        return len(acb_env.cluster._impl.cluster_info.nodes)

    @pytest_asyncio.fixture(scope='class')
    async def num_replicas(self, acb_env: AsyncBinaryTracingEnvironment):
        bucket_settings = await acb_env.try_n_times(10, 1, acb_env.bm.get_bucket, acb_env.bucket.name)
        num_replicas = bucket_settings.get('num_replicas')
        return num_replicas

    @pytest.mark.parametrize('op_name', [OpName.Append, OpName.Prepend])
    @pytest.mark.asyncio
    async def test_binary_mutation_op(self, acb_env: AsyncBinaryTracingEnvironment, op_name: OpName) -> None:
        key = acb_env.get_existing_binary_doc_by_type('bytes_empty', key_only=True)

        validator = acb_env.kv_span_validator
        validator.reset(op_name=op_name)
        operation = getattr(acb_env.collection.binary(), op_name.value)
        await operation(key, b'XXX')
        validator.validate_kv_op()

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    @pytest.mark.parametrize('op_name, opts', [
        (OpName.Append, AppendOptions),
        (OpName.Prepend, PrependOptions),
    ])
    @pytest.mark.asyncio
    async def test_binary_mutation_op_with_durability(self,
                                                      acb_env: AsyncBinaryTracingEnvironment,
                                                      op_name: OpName,
                                                      opts: Any) -> None:
        key = acb_env.get_existing_binary_doc_by_type('bytes_empty', key_only=True)
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        validator = acb_env.kv_span_validator
        validator.reset(op_name=op_name, durability=DurabilityLevel.to_server_str(durability.level))
        operation = getattr(acb_env.collection.binary(), op_name.value)
        await operation(key, b'XXX', opts(durability=durability))
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name', [OpName.Append, OpName.Prepend])
    @pytest.mark.asyncio
    async def test_binary_mutation_op_with_error(self,
                                                 acb_env: AsyncBinaryTracingEnvironment,
                                                 op_name: OpName) -> None:
        key = 'not-a-key'

        validator = acb_env.kv_span_validator
        validator.reset(op_name=op_name, validate_error=True)
        operation = getattr(acb_env.collection.binary(), op_name.value)
        try:
            await operation(key, b'XXX')
        except Exception:
            pass
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name, opts', [
        (OpName.Append, AppendOptions),
        (OpName.Prepend, PrependOptions),
    ])
    @pytest.mark.asyncio
    async def test_binary_mutation_op_with_parent(
        self,
        acb_env: AsyncBinaryTracingEnvironment,
        op_name: OpName,
        opts: Any
    ) -> None:
        key = acb_env.get_existing_binary_doc_by_type('bytes_empty', key_only=True)

        validator = acb_env.kv_span_validator
        acb_env.tracer.clear_spans()
        parent_span = acb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(do_not_clear_spans=True, op_name=op_name, parent_span=parent_span)
        operation = getattr(acb_env.collection.binary(), op_name.value)
        await operation(key, b'XXX', opts(span=parent_span))
        validator.validate_kv_op(end_parent=True)

        # span in kwargs
        acb_env.tracer.clear_spans()
        parent_span = acb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(do_not_clear_spans=True, parent_span=parent_span)
        await operation(key, b'XXX', span=parent_span)
        validator.validate_kv_op(end_parent=True)

    @pytest.mark.parametrize('op_name', [OpName.Increment, OpName.Decrement])
    @pytest.mark.asyncio
    async def test_binary_counter_op(self, acb_env: AsyncBinaryTracingEnvironment, op_name: OpName) -> None:
        key = acb_env.get_existing_binary_doc_by_type('counter', key_only=True)

        validator = acb_env.kv_span_validator
        validator.reset(op_name=op_name)
        operation = getattr(acb_env.collection.binary(), op_name.value)
        await operation(key)
        validator.validate_kv_op()

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    @pytest.mark.parametrize('op_name, opts', [
        (OpName.Increment, IncrementOptions),
        (OpName.Decrement, DecrementOptions),
    ])
    @pytest.mark.asyncio
    async def test_binary_counter_op_with_durability(self,
                                                     acb_env: AsyncBinaryTracingEnvironment,
                                                     op_name: OpName,
                                                     opts: Any) -> None:
        key = acb_env.get_existing_binary_doc_by_type('counter', key_only=True)
        validator = acb_env.kv_span_validator
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        validator = acb_env.kv_span_validator
        validator.reset(op_name=op_name, durability=DurabilityLevel.to_server_str(durability.level))
        operation = getattr(acb_env.collection.binary(), op_name.value)
        await operation(key, opts(durability=durability))
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name', [OpName.Increment, OpName.Decrement])
    @pytest.mark.asyncio
    async def test_binary_counter_op_with_error(self,
                                                acb_env: AsyncBinaryTracingEnvironment,
                                                op_name: OpName) -> None:
        key = 'not-a-key'

        validator = acb_env.kv_span_validator
        validator.reset(op_name=op_name, validate_error=True)
        operation = getattr(acb_env.collection.binary(), op_name.value)
        try:
            # if we don't provide the negative initial value, the counter doc will be created
            await operation(key, initial=SignedInt64(-1))
        except Exception:
            pass
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name, opts', [
        (OpName.Increment, IncrementOptions),
        (OpName.Decrement, DecrementOptions),
    ])
    @pytest.mark.asyncio
    async def test_binary_counter_op_with_parent(
        self,
        acb_env: AsyncBinaryTracingEnvironment,
        op_name: OpName,
        opts: Any
    ) -> None:
        key = acb_env.get_existing_binary_doc_by_type('counter', key_only=True)

        validator = acb_env.kv_span_validator
        acb_env.tracer.clear_spans()
        parent_span = acb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(do_not_clear_spans=True, op_name=op_name, parent_span=parent_span)
        operation = getattr(acb_env.collection.binary(), op_name.value)
        await operation(key, opts(span=parent_span))
        validator.validate_kv_op(end_parent=True)

        # span in kwargs
        acb_env.tracer.clear_spans()
        parent_span = acb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(do_not_clear_spans=True, parent_span=parent_span)
        await operation(key, span=parent_span)
        validator.validate_kv_op(end_parent=True)

    @pytest.mark.parametrize('op_name, opts', [
        (OpName.Append, None),
        (OpName.Decrement, DecrementOptions),
        (OpName.Increment, IncrementOptions),
        (OpName.Prepend, None),
    ])
    @pytest.mark.asyncio
    async def test_binary_op_no_dispatch_failure(
        self,
        acb_env: AsyncBinaryTracingEnvironment,
        op_name: OpName,
        opts: Optional[Any]
    ) -> None:

        key = acb_env.get_new_doc(key_only=True)
        validator = acb_env.kv_span_validator
        validator.reset(op_name=op_name,
                        validate_error=True,
                        error_before_dispatch=True)
        operation = getattr(acb_env.collection.binary(), op_name.value)
        try:
            if op_name is OpName.Append or op_name is OpName.Prepend:
                await operation(key, 123)
            else:
                await operation(key, opts(initial=123))
        except (InvalidArgumentException, ValueError):
            pass

        validator.validate_kv_op()


class AsyncBinaryTracingTests(AsyncBinaryTracingTestsSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(AsyncBinaryTracingTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(AsyncBinaryTracingTests) if valid_test_method(meth)]
        test_list = set(AsyncBinaryTracingTestsSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')

    @pytest_asyncio.fixture(scope='class', name='acb_env', params=[TracingType.ThresholdLogging,
                                                                   TracingType.Legacy,
                                                                   TracingType.NoOp,
                                                                   TracingType.Modern
                                                                   ])
    async def acouchbase_test_environment(self, acb_base_env, request):

        # a new environment and cluster is created
        acb_env = await AsyncBinaryTracingEnvironment.from_environment(acb_base_env, tracing_type=request.param)
        await acb_env.setup_binary_data()
        acb_env.enable_bucket_mgmt()
        yield acb_env
        acb_env.disable_bucket_mgmt()
        await acb_env.teardown()
        # TODO(PYCBC-1746): Update once legacy tracing logic is removed.
        #                   See PYCBC-1748 for details on issue, this seems to mainly impact the error scenarios.
        if request.param == TracingType.Legacy:
            await acb_env.cluster.close()
