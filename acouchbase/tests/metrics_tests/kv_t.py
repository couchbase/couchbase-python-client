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
                    Optional,
                    Tuple)

import pytest
import pytest_asyncio

import couchbase.subdocument as SD
from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.observability import OpName
from couchbase.options import (GetOptions,
                               InsertOptions,
                               MutateInOptions,
                               ReplaceOptions,
                               UpsertOptions)
from couchbase.transcoder import Transcoder
from tests.environments.metrics import AsyncMetricsEnvironment
from tests.environments.metrics.metrics_environment import MeterType
from tests.test_features import EnvironmentFeatures


class ErrorTranscoder(Transcoder):
    def encode_value(self, value: Any) -> Tuple[bytes, int]:
        raise Exception('transcoder error')

    def decode_value(self, value: bytes, flags: int) -> Any:
        raise Exception('transcoder error')


class KeyValueMetricsTestsSuite:

    TEST_MANIFEST = [
        'test_kv_mutation_op',
        'test_kv_mutation_op_with_error',
        'test_kv_op',
        'test_kv_op_no_dispatch_failure',
        'test_kv_op_with_error',
        'test_kv_remove_op',
        'test_kv_remove_op_with_error',
        'test_kv_subdoc_op',
        'test_kv_subdoc_op_error',
        'test_kv_unlock_op',
        'test_kv_unlock_op_with_error',
        'test_kv_upsert_op_with_encoding_error',
    ]

    @pytest.fixture(autouse=True)
    def reset_validator(self, acb_env: AsyncMetricsEnvironment):
        yield
        acb_env.kv_meter_validator.reset()

    @pytest.mark.parametrize('op_name', [OpName.Insert, OpName.Replace, OpName.Upsert])
    @pytest.mark.asyncio
    async def test_kv_mutation_op(self, acb_env: AsyncMetricsEnvironment, op_name: OpName) -> None:
        if op_name == OpName.Insert:
            key, value = acb_env.get_new_doc()
        else:
            key, value = acb_env.get_existing_doc()

        validator = acb_env.kv_meter_validator
        validator.reset(op_name=op_name)
        operation = getattr(acb_env.collection, op_name.value)
        await operation(key, value)
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name', [OpName.Insert, OpName.Replace])
    @pytest.mark.asyncio
    async def test_kv_mutation_op_with_error(self, acb_env: AsyncMetricsEnvironment, op_name: OpName) -> None:
        if op_name == OpName.Insert:
            key, value = acb_env.get_existing_doc()
        else:
            key, value = acb_env.get_new_doc()

        validator = acb_env.kv_meter_validator
        validator.reset(op_name=op_name, validate_error=True)
        operation = getattr(acb_env.collection, op_name.value)
        try:
            await operation(key, value)
        except Exception:
            pass
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name', [
        OpName.Exists,
        OpName.Get,
        OpName.GetAllReplicas,
        OpName.GetAnyReplica,
        OpName.GetAndLock,
        OpName.GetAndTouch,
        OpName.Touch,
    ])
    @pytest.mark.asyncio
    async def test_kv_op(
        self,
        acb_env: AsyncMetricsEnvironment,
        op_name: OpName
    ) -> None:
        key = acb_env.get_existing_doc(key_only=True)
        validator = acb_env.kv_meter_validator
        validator.reset(op_name=op_name)
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
        acb_env: AsyncMetricsEnvironment,
        op_name: OpName,
        opts: Optional[Any]
    ) -> None:
        key, value = acb_env.get_existing_doc()
        validator = acb_env.kv_meter_validator
        if op_name is OpName.Insert or op_name is OpName.Upsert:
            # the expiry is validated prior to the request span being created
            validator.reset(op_name=op_name,
                            validate_error=True,
                            expect_request_span=False)
        else:
            validator.reset(op_name=op_name,
                            validate_error=True)

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

    @pytest.mark.parametrize('op_name', [
        OpName.Get,
        OpName.GetAllReplicas,
        OpName.GetAnyReplica,
        OpName.GetAndLock,
        OpName.GetAndTouch,
        OpName.Touch,
    ])
    @pytest.mark.asyncio
    async def test_kv_op_with_error(
        self,
        acb_env: AsyncMetricsEnvironment,
        op_name: OpName
    ) -> None:
        key = acb_env.get_new_doc(key_only=True)
        validator = acb_env.kv_meter_validator
        validator.reset(op_name=op_name, validate_error=True)
        operation = getattr(acb_env.collection, op_name.value)
        try:
            if op_name is OpName.GetAndLock or op_name is OpName.GetAndTouch or op_name is OpName.Touch:
                await operation(key, timedelta(seconds=1))
            else:
                await operation(key)
        except Exception:
            pass
        validator.validate_kv_op()

    @pytest.mark.asyncio
    async def test_kv_remove_op(self, acb_env: AsyncMetricsEnvironment) -> None:
        key, value = acb_env.get_new_doc()
        await acb_env.collection.upsert(key, value)
        validator = acb_env.kv_meter_validator
        validator.reset(op_name=OpName.Remove)
        await acb_env.collection.remove(key)
        validator.validate_kv_op()

    @pytest.mark.asyncio
    async def test_kv_remove_op_with_error(self, acb_env: AsyncMetricsEnvironment) -> None:
        key = acb_env.get_new_doc(key_only=True)
        validator = acb_env.kv_meter_validator
        validator.reset(op_name=OpName.Remove, validate_error=True)
        try:
            await acb_env.collection.remove(key)
        except Exception:
            pass
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name', [
        OpName.LookupIn,
        OpName.LookupInAllReplicas,
        OpName.LookupInAnyReplica,
        OpName.MutateIn,
    ])
    @pytest.mark.asyncio
    async def test_kv_subdoc_op(self, acb_env: AsyncMetricsEnvironment, op_name: OpName) -> None:
        if op_name in (OpName.LookupInAllReplicas, OpName.LookupInAnyReplica):
            if not EnvironmentFeatures.is_feature_supported('subdoc_replica_read',
                                                            acb_env.server_version_short,
                                                            acb_env.mock_server_type):
                pytest.skip('Server does not support subdocument replica operations.')

        key = acb_env.get_existing_doc(key_only=True)
        validator = acb_env.kv_meter_validator
        validator.reset(op_name=op_name)
        if op_name == OpName.MutateIn:
            await acb_env.collection.mutate_in(key, (SD.upsert('make', 'New Make'), SD.replace('model', 'New Model')))
        else:
            operation = getattr(acb_env.collection, op_name.value)
            await operation(key, (SD.get('batch'),))
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name', [
        OpName.LookupIn,
        OpName.LookupInAllReplicas,
        OpName.LookupInAnyReplica,
        OpName.MutateIn,
    ])
    @pytest.mark.asyncio
    async def test_kv_subdoc_op_error(self, acb_env: AsyncMetricsEnvironment, op_name: OpName) -> None:
        if op_name in (OpName.LookupInAllReplicas, OpName.LookupInAnyReplica):
            if not EnvironmentFeatures.is_feature_supported('subdoc_replica_read',
                                                            acb_env.server_version_short,
                                                            acb_env.mock_server_type):
                pytest.skip('Server does not support subdocument replica operations.')

        key = acb_env.get_new_doc(key_only=True)
        validator = acb_env.kv_meter_validator
        validator.reset(op_name=op_name, validate_error=True)
        try:
            if op_name == OpName.MutateIn:
                await acb_env.collection.mutate_in(key,
                                                   (SD.upsert('make', 'New Make'), SD.replace('model', 'New Model')))
            else:
                operation = getattr(acb_env.collection, op_name.value)
                await operation(key, (SD.get('batch'),))
        except Exception:
            pass
        validator.validate_kv_op()

    @pytest.mark.asyncio
    async def test_kv_unlock_op(self, acb_env: AsyncMetricsEnvironment) -> None:
        key = acb_env.get_existing_doc(key_only=True)
        result = await acb_env.collection.get_and_lock(key, timedelta(seconds=5))
        cas = result.cas
        validator = acb_env.kv_meter_validator
        validator.reset(op_name=OpName.Unlock)
        await acb_env.collection.unlock(key, cas)
        validator.validate_kv_op()

    @pytest.mark.asyncio
    async def test_kv_unlock_op_with_error(self, acb_env: AsyncMetricsEnvironment) -> None:
        key = acb_env.get_new_doc(key_only=True)
        validator = acb_env.kv_meter_validator
        validator.reset(op_name=OpName.Unlock, validate_error=True)
        try:
            await acb_env.collection.unlock(key, 12345)
        except Exception:
            pass
        validator.validate_kv_op()

    @pytest.mark.asyncio
    async def test_kv_upsert_op_with_encoding_error(self, acb_env: AsyncMetricsEnvironment) -> None:
        key, value = acb_env.get_new_doc()
        validator = acb_env.kv_meter_validator
        validator.reset(op_name=OpName.Upsert, validate_error=True)
        try:
            await acb_env.collection.upsert(key, value, UpsertOptions(transcoder=ErrorTranscoder()))
        except Exception:
            pass
        validator.validate_kv_op()


class ClassicKeyValueMetricsTests(KeyValueMetricsTestsSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicKeyValueMetricsTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicKeyValueMetricsTests) if valid_test_method(meth)]
        test_list = set(KeyValueMetricsTestsSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')

    @pytest_asyncio.fixture(scope='class', name='acb_env', params=[
        MeterType.Basic,
        MeterType.NoOp,
    ])
    async def couchbase_test_environment(self, cb_base_env, request):
        # a new environment and cluster is created
        acb_env = await AsyncMetricsEnvironment.from_environment(cb_base_env, meter_type=request.param)
        acb_env.enable_bucket_mgmt()
        await acb_env.setup(num_docs=50)
        yield acb_env
        acb_env.disable_bucket_mgmt()
        await acb_env.teardown()
