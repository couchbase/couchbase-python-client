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

from datetime import timedelta
from typing import (Any,
                    List,
                    Optional,
                    Tuple)

import pytest
import pytest_asyncio

import couchbase.subdocument as SD
from couchbase.durability import DurabilityLevel, ServerDurability
from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.observability import OpName
from couchbase.options import (ExistsOptions,
                               GetAllReplicasOptions,
                               GetAndLockOptions,
                               GetAndTouchOptions,
                               GetAnyReplicaOptions,
                               GetOptions,
                               InsertOptions,
                               LookupInAllReplicasOptions,
                               LookupInAnyReplicaOptions,
                               LookupInOptions,
                               MutateInOptions,
                               RemoveOptions,
                               ReplaceOptions,
                               TouchOptions,
                               UnlockOptions,
                               UpsertOptions)
from couchbase.transcoder import Transcoder
from tests.environments.tracing import AsyncKeyValueTracingEnvironment
from tests.environments.tracing.base_tracing_environment import TracingType
from tests.test_features import EnvironmentFeatures


class ErrorTranscoder(Transcoder):
    def encode_value(self, value: Any) -> Tuple[bytes, int]:
        raise Exception('transcoder error')

    def decode_value(self, value: bytes, flags: int) -> Any:
        raise Exception('transcoder error')


class AsyncKeyValueTracingTestsSuite:
    TEST_MANIFEST = [
        'test_kv_mutation_op',
        'test_kv_mutation_op_with_durability',
        'test_kv_mutation_op_with_error',
        'test_kv_mutation_op_with_parent',
        'test_kv_op',
        'test_kv_op_no_dispatch_failure',
        'test_kv_op_with_error',
        'test_kv_op_with_parent',
        'test_kv_op_with_retries',
        'test_kv_remove_op',
        'test_kv_remove_op_with_durability',
        'test_kv_remove_op_with_error',
        'test_kv_remove_op_with_parent',
        'test_kv_subdoc_op',
        'test_kv_subdoc_op_error',
        'test_kv_subdoc_op_with_parent',
        'test_kv_unlock_op',
        'test_kv_unlock_op_with_error',
        'test_kv_unlock_op_with_parent',
        'test_kv_upsert_op_with_encoding_error',
    ]

    @pytest.fixture(autouse=True)
    def reset_validator(self, acb_env: AsyncKeyValueTracingEnvironment):
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
    def check_sync_durability_supported(self, acb_env: AsyncKeyValueTracingEnvironment):
        EnvironmentFeatures.check_if_feature_supported('sync_durability',
                                                       acb_env.server_version_short,
                                                       acb_env.mock_server_type)

    @pytest.fixture(scope='class')
    def num_nodes(self, acb_env: AsyncKeyValueTracingEnvironment):
        return len(acb_env.cluster._impl.cluster_info.nodes)

    @pytest_asyncio.fixture(scope='class')
    async def num_replicas(self, acb_env: AsyncKeyValueTracingEnvironment):
        bucket_settings = await acb_env.try_n_times(10, 1, acb_env.bm.get_bucket, acb_env.bucket.name)
        num_replicas = bucket_settings.get('num_replicas')
        return num_replicas

    @pytest.mark.parametrize('op_name', [OpName.Insert, OpName.Replace, OpName.Upsert])
    @pytest.mark.asyncio
    async def test_kv_mutation_op(self, acb_env: AsyncKeyValueTracingEnvironment, op_name: OpName) -> None:
        if op_name == OpName.Insert:
            key, value = acb_env.get_new_doc()
        else:
            key, value = acb_env.get_existing_doc()

        validator = acb_env.kv_span_validator
        validator.reset(op_name=op_name)
        operation = getattr(acb_env.collection, op_name.value)
        if op_name == OpName.Remove:
            await operation(key)
        else:
            await operation(key, value)
        validator.validate_kv_op()

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    @pytest.mark.parametrize('op_name, opts', [
        (OpName.Insert, InsertOptions),
        (OpName.MutateIn, MutateInOptions),
        (OpName.Replace, ReplaceOptions),
        (OpName.Upsert, UpsertOptions),
    ])
    @pytest.mark.asyncio
    async def test_kv_mutation_op_with_durability(self,
                                                  acb_env: AsyncKeyValueTracingEnvironment,
                                                  op_name: OpName,
                                                  opts: Any) -> None:
        if op_name == OpName.Insert:
            key, value = acb_env.get_new_doc()
        else:
            key, value = acb_env.get_existing_doc()

        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)

        validator = acb_env.kv_span_validator
        validator.reset(op_name=op_name, durability=DurabilityLevel.to_server_str(durability.level))
        operation = getattr(acb_env.collection, op_name.value)
        if op_name is OpName.MutateIn:
            await operation(key,
                            (SD.upsert('make', 'New Make'), SD.replace('model', 'New Model')),
                            opts(durability=durability))
        else:
            await operation(key, value, opts(durability=durability))
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name', [OpName.Insert, OpName.Replace])
    @pytest.mark.asyncio
    async def test_kv_mutation_op_with_error(self, acb_env: AsyncKeyValueTracingEnvironment, op_name: OpName) -> None:
        if op_name == OpName.Insert:
            key, value = acb_env.get_existing_doc()
        else:
            key, value = acb_env.get_new_doc()

        validator = acb_env.kv_span_validator
        validator.reset(op_name=op_name, validate_error=True)
        operation = getattr(acb_env.collection, op_name.value)
        try:
            await operation(key, value)
        except Exception:
            pass
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name, opts', [
        (OpName.Insert, InsertOptions),
        (OpName.Replace, ReplaceOptions),
        (OpName.Upsert, UpsertOptions),
    ])
    @pytest.mark.asyncio
    async def test_kv_mutation_op_with_parent(
        self,
        acb_env: AsyncKeyValueTracingEnvironment,
        op_name: OpName,
        opts: Any
    ) -> None:
        if op_name == OpName.Insert:
            key, value = acb_env.get_new_doc()
        else:
            key, value = acb_env.get_existing_doc()

        validator = acb_env.kv_span_validator
        acb_env.tracer.clear_spans()
        parent_span = acb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(op_name=op_name, do_not_clear_spans=True, parent_span=parent_span)
        operation = getattr(acb_env.collection, op_name.value)
        await operation(key, value, opts(span=parent_span))
        validator.validate_kv_op(end_parent=True)

        if op_name == OpName.Insert:
            key = acb_env.get_new_doc(key_only=True)

        # span in kwargs
        acb_env.tracer.clear_spans()
        parent_span = acb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(do_not_clear_spans=True, parent_span=parent_span)
        await operation(key, value, span=parent_span)
        validator.validate_kv_op(end_parent=True)

    @pytest.mark.parametrize('op_name, nested_ops', [
        (OpName.Exists, None),
        (OpName.Get, None),
        (OpName.GetAllReplicas, [OpName.Get, OpName.GetReplica]),
        (OpName.GetAnyReplica, [OpName.Get, OpName.GetReplica]),
        (OpName.GetAndLock, None),
        (OpName.GetAndTouch, None),
        (OpName.Touch, None)
    ])
    @pytest.mark.asyncio
    async def test_kv_op(
        self,
        acb_env: AsyncKeyValueTracingEnvironment,
        op_name: OpName,
        nested_ops: Optional[List[OpName]]
    ) -> None:
        key = acb_env.get_existing_doc(key_only=True)
        validator = acb_env.kv_span_validator
        validator.reset(op_name=op_name, clear_parent_span=True, nested_ops=nested_ops)
        operation = getattr(acb_env.collection, op_name.value)
        if op_name is OpName.GetAndLock or op_name is OpName.GetAndTouch or op_name is OpName.Touch:
            await operation(key, timedelta(seconds=1))
        else:
            await operation(key)
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name, opts', [
        (OpName.Get, None),
        (OpName.Insert, InsertOptions),
        (OpName.MutateIn, MutateInOptions),
        (OpName.Replace, None),
        (OpName.Upsert, UpsertOptions)
    ])
    @pytest.mark.asyncio
    async def test_kv_op_no_dispatch_failure(
        self,
        acb_env: AsyncKeyValueTracingEnvironment,
        op_name: OpName,
        opts: Optional[Any]
    ) -> None:
        key, value = acb_env.get_existing_doc()
        validator = acb_env.kv_span_validator
        if op_name is OpName.Insert or op_name is OpName.Upsert:
            # the expiry is validated prior to the request span being created
            validator.reset(op_name=op_name,
                            validate_error=True,
                            error_before_dispatch=True,
                            expect_request_span=False)
        else:
            validator.reset(op_name=op_name,
                            validate_error=True,
                            error_before_dispatch=True)

        try:
            if op_name is OpName.Get:
                await acb_env.collection.get(key, GetOptions(project=[1, 2, 3]))
            elif op_name is OpName.Insert or op_name is OpName.Upsert:
                expiry = timedelta(seconds=-1)
                operation = getattr(acb_env.collection, op_name.value)
                await operation(key, value, opts(expiry=expiry))
            elif op_name is OpName.MutateIn:
                await acb_env.collection.mutate_in(key,
                                                   (SD.insert('make', 'New Make'),),
                                                   MutateInOptions(preserve_expiry=True))
            elif op_name is OpName.Replace:
                replace_opts = ReplaceOptions(preserve_expiry=True, expiry=timedelta(seconds=5))
                await acb_env.collection.replace(key, value, replace_opts)
        except InvalidArgumentException:
            pass

        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name, nested_ops', [
        (OpName.Get, None),
        (OpName.GetAllReplicas, [OpName.Get, OpName.GetReplica]),
        (OpName.GetAnyReplica, [OpName.Get, OpName.GetReplica]),
        (OpName.GetAndLock, None),
        (OpName.GetAndTouch, None),
        (OpName.Touch, None)
    ])
    @pytest.mark.asyncio
    async def test_kv_op_with_error(
        self,
        acb_env: AsyncKeyValueTracingEnvironment,
        op_name: OpName,
        nested_ops: Optional[List[OpName]]
    ) -> None:
        key = acb_env.get_new_doc(key_only=True)
        validator = acb_env.kv_span_validator
        validator.reset(op_name=op_name, clear_parent_span=True, nested_ops=nested_ops, validate_error=True)
        operation = getattr(acb_env.collection, op_name.value)
        try:
            if op_name is OpName.GetAndLock or op_name is OpName.GetAndTouch or op_name is OpName.Touch:
                await operation(key, timedelta(seconds=1))
            else:
                await operation(key)
        except Exception:
            pass
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name, opts, nested_ops', [
        (OpName.Exists, ExistsOptions, None),
        (OpName.Get, GetOptions, None),
        (OpName.GetAllReplicas, GetAllReplicasOptions, [OpName.Get, OpName.GetReplica]),
        (OpName.GetAnyReplica, GetAnyReplicaOptions, [OpName.Get, OpName.GetReplica]),
        (OpName.GetAndLock, GetAndLockOptions, None),
        (OpName.GetAndTouch, GetAndTouchOptions, None),
        (OpName.Touch, TouchOptions, None)
    ])
    @pytest.mark.asyncio
    async def test_kv_op_with_parent(
        self,
        acb_env: AsyncKeyValueTracingEnvironment,
        op_name: OpName,
        opts: Any,
        nested_ops: Optional[List[OpName]],
    ) -> None:
        key = acb_env.get_existing_doc(key_only=True)
        validator = acb_env.kv_span_validator
        acb_env.tracer.clear_spans()
        parent_span = acb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(do_not_clear_spans=True, op_name=op_name, parent_span=parent_span, nested_ops=nested_ops)
        operation = getattr(acb_env.collection, op_name.value)
        if op_name is OpName.GetAndLock or op_name is OpName.GetAndTouch or op_name is OpName.Touch:
            await operation(key, timedelta(seconds=1), opts(span=parent_span))
        else:
            await operation(key, opts(span=parent_span))
        validator.validate_kv_op(end_parent=True)

        # span in kwargs
        acb_env.tracer.clear_spans()
        parent_span = acb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(do_not_clear_spans=True, parent_span=parent_span, nested_ops=nested_ops)
        if op_name is OpName.GetAndLock or op_name is OpName.GetAndTouch or op_name is OpName.Touch:
            # lets get another key as the doc may be locked or touched at this point
            key = acb_env.get_existing_doc(key_only=True)
            await operation(key, timedelta(seconds=1), span=parent_span)
        else:
            await operation(key, span=parent_span)
        validator.validate_kv_op(end_parent=True)

    @pytest.mark.asyncio
    async def test_kv_op_with_retries(self, acb_env: AsyncKeyValueTracingEnvironment) -> None:
        key = acb_env.get_existing_doc(key_only=True)
        await acb_env.collection.get_and_lock(key, timedelta(seconds=1))
        validator = acb_env.kv_span_validator
        validator.reset(op_name=OpName.Upsert, retry_count_gt_zero=True)
        await acb_env.collection.upsert(key, {'foo': 'bar'}, UpsertOptions(timeout=timedelta(seconds=3)))
        validator.validate_kv_op()

    @pytest.mark.asyncio
    async def test_kv_remove_op(self, acb_env: AsyncKeyValueTracingEnvironment) -> None:
        key, value = acb_env.get_new_doc()
        await acb_env.collection.upsert(key, value)
        validator = acb_env.kv_span_validator
        validator.reset(op_name=OpName.Remove)
        await acb_env.collection.remove(key)
        validator.validate_kv_op()

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    @pytest.mark.asyncio
    async def test_kv_remove_op_with_durability(self, acb_env: AsyncKeyValueTracingEnvironment) -> None:
        key, value = acb_env.get_new_doc()
        await acb_env.collection.upsert(key, value)
        validator = acb_env.kv_span_validator
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        validator = acb_env.kv_span_validator
        validator.reset(op_name=OpName.Remove, durability=DurabilityLevel.to_server_str(durability.level))
        await acb_env.collection.remove(key, RemoveOptions(durability=durability))
        validator.validate_kv_op()

    @pytest.mark.asyncio
    async def test_kv_remove_op_with_error(self, acb_env: AsyncKeyValueTracingEnvironment) -> None:
        key = acb_env.get_new_doc(key_only=True)
        validator = acb_env.kv_span_validator
        validator.reset(op_name=OpName.Remove, validate_error=True)
        try:
            await acb_env.collection.remove(key)
        except Exception:
            pass
        validator.validate_kv_op()

    @pytest.mark.asyncio
    async def test_kv_remove_op_with_parent(self, acb_env: AsyncKeyValueTracingEnvironment) -> None:
        key, value = acb_env.get_new_doc()
        await acb_env.collection.upsert(key, value)

        validator = acb_env.kv_span_validator
        acb_env.tracer.clear_spans()
        parent_span = acb_env.tracer.request_span(f'parent_{OpName.Remove.value}_span')
        validator.reset(op_name=OpName.Remove, do_not_clear_spans=True, parent_span=parent_span)
        await acb_env.collection.remove(key, RemoveOptions(span=parent_span))
        validator.validate_kv_op(end_parent=True)

        await acb_env.collection.upsert(key, value)

        # span in kwargs
        acb_env.tracer.clear_spans()
        parent_span = acb_env.tracer.request_span(f'parent_{OpName.Remove.value}_span')
        validator.reset(do_not_clear_spans=True, parent_span=parent_span)
        await acb_env.collection.remove(key, span=parent_span)
        validator.validate_kv_op(end_parent=True)

    @pytest.mark.parametrize('op_name, nested_ops', [
        (OpName.LookupIn, None),
        (OpName.LookupInAllReplicas, [OpName.LookupIn, OpName.LookupInReplica]),
        (OpName.LookupInAnyReplica, [OpName.LookupIn, OpName.LookupInReplica]),
        (OpName.MutateIn, None)])
    @pytest.mark.asyncio
    async def test_kv_subdoc_op(self,
                                acb_env: AsyncKeyValueTracingEnvironment,
                                op_name: OpName,
                                nested_ops: Optional[List[OpName]]) -> None:
        if op_name in (OpName.LookupInAllReplicas, OpName.LookupInAnyReplica):
            if not EnvironmentFeatures.is_feature_supported('subdoc_replica_read',
                                                            acb_env.server_version_short,
                                                            acb_env.mock_server_type):
                pytest.skip('Server does not support subdocument replica operations.')

        key = acb_env.get_existing_doc(key_only=True)
        validator = acb_env.kv_span_validator
        validator.reset(op_name=op_name, nested_ops=nested_ops)
        if op_name == OpName.MutateIn:
            await acb_env.collection.mutate_in(key, (SD.upsert('make', 'New Make'), SD.replace('model', 'New Model')))
        else:
            operation = getattr(acb_env.collection, op_name.value)
            await operation(key, (SD.get('batch'),))
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name, nested_ops', [
        (OpName.LookupIn, None),
        (OpName.LookupInAllReplicas, [OpName.LookupIn, OpName.LookupInReplica]),
        (OpName.LookupInAnyReplica, [OpName.LookupIn, OpName.LookupInReplica]),
        (OpName.MutateIn, None)])
    @pytest.mark.asyncio
    async def test_kv_subdoc_op_error(self,
                                      acb_env: AsyncKeyValueTracingEnvironment,
                                      op_name: OpName,
                                      nested_ops: Optional[List[OpName]]) -> None:
        if op_name in (OpName.LookupInAllReplicas, OpName.LookupInAnyReplica):
            if not EnvironmentFeatures.is_feature_supported('subdoc_replica_read',
                                                            acb_env.server_version_short,
                                                            acb_env.mock_server_type):
                pytest.skip('Server does not support subdocument replica operations.')

        key = acb_env.get_new_doc(key_only=True)
        validator = acb_env.kv_span_validator
        validator.reset(op_name=op_name, nested_ops=nested_ops, validate_error=True)
        try:
            if op_name == OpName.MutateIn:
                await acb_env.collection.mutate_in(key, (SD.upsert('make', 'New Make'),
                                                         SD.replace('model', 'New Model')))
            else:
                operation = getattr(acb_env.collection, op_name.value)
                await operation(key, (SD.get('batch'),))
        except Exception:
            pass
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name, opts, nested_ops', [
        (OpName.LookupIn, LookupInOptions, None),
        (OpName.LookupInAllReplicas, LookupInAllReplicasOptions, [OpName.LookupIn, OpName.LookupInReplica]),
        (OpName.LookupInAnyReplica, LookupInAnyReplicaOptions, [OpName.LookupIn, OpName.LookupInReplica]),
        (OpName.MutateIn, MutateInOptions, None)])
    @pytest.mark.asyncio
    async def test_kv_subdoc_op_with_parent(self,
                                            acb_env: AsyncKeyValueTracingEnvironment,
                                            op_name: OpName,
                                            opts: Any,
                                            nested_ops: Optional[List[OpName]]) -> None:
        if op_name in (OpName.LookupInAllReplicas, OpName.LookupInAnyReplica):
            if not EnvironmentFeatures.is_feature_supported('subdoc_replica_read',
                                                            acb_env.server_version_short,
                                                            acb_env.mock_server_type):
                pytest.skip('Server does not support subdocument replica operations.')

        key = acb_env.get_existing_doc(key_only=True)
        validator = acb_env.kv_span_validator
        acb_env.tracer.clear_spans()
        parent_span = acb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(do_not_clear_spans=True, op_name=op_name, parent_span=parent_span, nested_ops=nested_ops)
        if op_name == OpName.MutateIn:
            await acb_env.collection.mutate_in(key,
                                               (SD.upsert('make', 'New Make'), SD.replace('model', 'New Model')),
                                               opts(span=parent_span))
        else:
            operation = getattr(acb_env.collection, op_name.value)
            await operation(key, (SD.get('batch'),), opts(span=parent_span))
        validator.validate_kv_op(end_parent=True)

        # span in kwargs
        acb_env.tracer.clear_spans()
        parent_span = acb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(do_not_clear_spans=True, parent_span=parent_span, nested_ops=nested_ops)
        if op_name == OpName.MutateIn:
            await acb_env.collection.mutate_in(key,
                                               (SD.upsert('make', 'New Make'), SD.replace('model', 'Newer Model')),
                                               span=parent_span)
        else:
            operation = getattr(acb_env.collection, op_name.value)
            await operation(key, (SD.get('batch'),), span=parent_span)
        validator.validate_kv_op(end_parent=True)

    @pytest.mark.asyncio
    async def test_kv_unlock_op(self, acb_env: AsyncKeyValueTracingEnvironment) -> None:
        key = acb_env.get_existing_doc(key_only=True)
        result = await acb_env.collection.get_and_lock(key, timedelta(seconds=5))
        cas = result.cas
        validator = acb_env.kv_span_validator
        validator.reset(op_name=OpName.Unlock)
        await acb_env.collection.unlock(key, cas)
        validator.validate_kv_op()

    @pytest.mark.asyncio
    async def test_kv_unlock_op_with_error(self, acb_env: AsyncKeyValueTracingEnvironment) -> None:
        key = acb_env.get_new_doc(key_only=True)
        validator = acb_env.kv_span_validator
        validator.reset(op_name=OpName.Unlock, validate_error=True)
        try:
            await acb_env.collection.unlock(key, 12345)
        except Exception:
            pass
        validator.validate_kv_op()

    @pytest.mark.asyncio
    async def test_kv_unlock_op_with_parent(self, acb_env: AsyncKeyValueTracingEnvironment) -> None:
        key = acb_env.get_existing_doc(key_only=True)
        result = await acb_env.collection.get_and_lock(key, timedelta(seconds=5))
        cas = result.cas
        validator = acb_env.kv_span_validator
        acb_env.tracer.clear_spans()
        parent_span = acb_env.tracer.request_span(f'parent_{OpName.Unlock.value}_span')
        validator.reset(op_name=OpName.Unlock, do_not_clear_spans=True, parent_span=parent_span)
        await acb_env.collection.unlock(key, cas, UnlockOptions(span=parent_span))
        validator.validate_kv_op(end_parent=True)

        result = await acb_env.collection.get_and_lock(key, timedelta(seconds=5))
        cas = result.cas

        # span in kwargs
        acb_env.tracer.clear_spans()
        parent_span = acb_env.tracer.request_span(f'parent_{OpName.Unlock.value}_span')
        validator.reset(do_not_clear_spans=True, parent_span=parent_span)
        await acb_env.collection.unlock(key, cas, span=parent_span)
        validator.validate_kv_op(end_parent=True)

    @pytest.mark.asyncio
    async def test_kv_upsert_op_with_encoding_error(self, acb_env: AsyncKeyValueTracingEnvironment) -> None:
        key, value = acb_env.get_new_doc()
        validator = acb_env.kv_span_validator
        validator.reset(op_name=OpName.Upsert, validate_error=True, error_before_dispatch=True)
        try:
            await acb_env.collection.upsert(key, value, UpsertOptions(transcoder=ErrorTranscoder()))
        except Exception:
            pass
        validator.validate_kv_op()


class AsyncKeyValueTracingTests(AsyncKeyValueTracingTestsSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(AsyncKeyValueTracingTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(AsyncKeyValueTracingTests) if valid_test_method(meth)]
        test_list = set(AsyncKeyValueTracingTestsSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')

    @pytest_asyncio.fixture(scope='class', name='acb_env', params=[TracingType.ThresholdLogging,
                                                                   TracingType.Legacy,
                                                                   TracingType.NoOp,
                                                                   TracingType.Modern
                                                                   ])
    async def acouchbase_test_environment(self, acb_base_env, request):

        # a new environment and cluster is created
        acb_env = await AsyncKeyValueTracingEnvironment.from_environment(acb_base_env, tracing_type=request.param)
        await acb_env.setup(num_docs=50)
        acb_env.enable_bucket_mgmt()
        yield acb_env
        acb_env.disable_bucket_mgmt()
        await acb_env.teardown()
