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
from typing import List, Optional

import pytest

from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.observability import OpName
from tests.environments.metrics import MetricsEnvironment
from tests.environments.metrics.metrics_environment import MeterType


class KeyValueMultiMetricsTestsSuite:

    TEST_MANIFEST = [
        'test_multi_kv_op_no_dispatch_failure',
        'test_multi_kv_op',
        'test_multi_kv_op_with_error',
        'test_multi_mutation_op',
        # 'test_multi_mutation_op_with_error',
        'test_multi_kv_unlock_op',
    ]

    @pytest.fixture(autouse=True)
    def reset_validator(self, cb_env: MetricsEnvironment):
        yield
        cb_env.kv_meter_validator.reset(clear_nested_ops=True)

    @pytest.fixture()
    def unlock_keys(self) -> List[str]:
        return ['unlock_doc_1', 'unlock_doc_2', 'unlock_doc_3']

    @pytest.fixture()
    def setup_unlock_multi(self, cb_env: MetricsEnvironment, unlock_keys: List[str]) -> None:
        docs = {key: {'test': 'data'} for key in unlock_keys}
        cb_env.collection.upsert_multi(docs)

    @pytest.fixture()
    def teardown_unlock_multi(self, cb_env: MetricsEnvironment, unlock_keys: List[str]):
        yield
        for _ in range(3):
            res = cb_env.collection.remove_multi(unlock_keys)
            if res.all_ok:
                break
            cb_env.sleep(1)

    @pytest.mark.parametrize('op_name, keys_as_list, nested_ops', [
        (OpName.ExistsMulti, True, [OpName.Exists]),
        (OpName.GetMulti, True, [OpName.Get]),
        (OpName.GetAllReplicasMulti, True, [OpName.GetAllReplicas]),
        (OpName.GetAndLockMulti, True, [OpName.GetAndLock]),
        (OpName.GetAnyReplicaMulti, True, [OpName.GetAnyReplica]),
        (OpName.InsertMulti, False, [OpName.Insert]),
        (OpName.RemoveMulti, True, [OpName.Remove]),
        (OpName.ReplaceMulti, False, [OpName.Replace]),
        (OpName.TouchMulti, True, [OpName.Touch]),
        (OpName.UnlockMulti, True, [OpName.Unlock]),
        (OpName.UpsertMulti, False, [OpName.Upsert]),
    ])
    def test_multi_kv_op_no_dispatch_failure(
        self,
        cb_env: MetricsEnvironment,
        op_name: OpName,
        keys_as_list: bool,
        nested_ops: List[OpName],
    ) -> None:
        keys = [cb_env.get_new_doc(key_only=True) for _ in range(3)]
        validator = cb_env.kv_meter_validator
        validator.reset(op_name=op_name,
                        nested_ops=nested_ops*len(keys),
                        error_before_dispatch=True,
                        validate_error=True)
        operation = getattr(cb_env.collection, op_name.value)
        try:
            # we do the opposite of what the op wants to trigger an InvalidArgumentException
            if keys_as_list:
                keys_and_docs = {key: {'test': 'data'} for key in keys}
                if op_name is OpName.GetAndLockMulti or op_name is OpName.TouchMulti:
                    operation(keys_and_docs, timedelta(seconds=1), return_exceptions=False)
                else:
                    operation(keys_and_docs, return_exceptions=False)
            else:
                operation(keys, return_exceptions=False)
        except InvalidArgumentException:
            pass
        validator.validate_multi_kv_op()

    @pytest.mark.parametrize('op_name, nested_ops', [
        (OpName.GetMulti, [OpName.Get]),
        (OpName.GetAllReplicasMulti, [OpName.GetAllReplicas]),
        (OpName.GetAnyReplicaMulti, [OpName.GetAnyReplica]),
        (OpName.ExistsMulti, [OpName.Exists]),
    ])
    def test_multi_kv_op(
        self,
        cb_env: MetricsEnvironment,
        op_name: OpName,
        nested_ops: Optional[List[OpName]]
    ) -> None:
        """Test multi-document read operations with tracing."""
        keys_and_docs = cb_env.get_docs(3)
        keys = list(keys_and_docs.keys())
        validator = cb_env.kv_meter_validator
        validator.reset(op_name=op_name, nested_ops=nested_ops*len(keys))
        operation = getattr(cb_env.collection, op_name.value)
        operation(keys)
        validator.validate_multi_kv_op()

    @pytest.mark.parametrize('op_name, nested_ops', [
        (OpName.GetMulti, [OpName.Get]),
        (OpName.GetAllReplicasMulti, [OpName.GetAllReplicas]),
        (OpName.GetAnyReplicaMulti, [OpName.GetAnyReplica]),
        (OpName.GetAndLockMulti, [OpName.GetAndLock]),
    ])
    def test_multi_kv_op_with_error(
        self,
        cb_env: MetricsEnvironment,
        op_name: OpName,
        nested_ops: List[OpName],
    ) -> None:
        keys = [cb_env.get_new_doc(key_only=True) for _ in range(3)]
        validator = cb_env.kv_meter_validator
        validator.reset(op_name=op_name,
                        clear_parent_span=True,
                        nested_ops=nested_ops*len(keys),
                        validate_error=True)
        operation = getattr(cb_env.collection, op_name.value)
        try:
            if op_name is OpName.GetAndLockMulti:
                operation(keys, timedelta(seconds=1))
            else:
                operation(keys)
        except Exception:
            pass
        validator.validate_multi_kv_op()

    @pytest.mark.parametrize('op_name, nested_ops', [
        (OpName.InsertMulti, [OpName.Insert]),
        (OpName.UpsertMulti, [OpName.Upsert]),
        (OpName.ReplaceMulti, [OpName.Replace]),
        (OpName.RemoveMulti, [OpName.Remove]),
        (OpName.TouchMulti, [OpName.Touch]),
    ])
    def test_multi_mutation_op(self,
                               cb_env: MetricsEnvironment,
                               op_name: OpName,
                               nested_ops: List[OpName]) -> None:
        validator = cb_env.kv_meter_validator
        num_keys = 3
        if op_name == OpName.InsertMulti:
            docs = {cb_env.get_new_doc(key_only=True): {'test': 'data'} for _ in range(3)}
        else:
            # For upsert, replace, remove, touch - use existing docs
            keys_and_docs = cb_env.get_docs(3)
            keys = list(keys_and_docs.keys())
            if op_name in [OpName.UpsertMulti, OpName.ReplaceMulti]:
                docs = {k: {'test': 'updated'} for k in keys}
            else:
                docs = keys

        validator.reset(op_name=op_name, nested_ops=nested_ops*num_keys)
        operation = getattr(cb_env.collection, op_name.value)
        if op_name == OpName.TouchMulti:
            operation(docs, timedelta(seconds=1))
        else:
            operation(docs)
        validator.validate_multi_kv_op()

    @pytest.mark.usefixtures('setup_unlock_multi')
    @pytest.mark.usefixtures('teardown_unlock_multi')
    def test_multi_kv_unlock_op(self, cb_env: MetricsEnvironment, unlock_keys: List[str]) -> None:
        validator = cb_env.kv_meter_validator

        # Get and lock documents first to get CAS values
        # since we need get_and_lock_multi to test unlock_multi, we validate here
        validator.reset(op_name=OpName.GetAndLockMulti, nested_ops=[OpName.GetAndLock]*len(unlock_keys))
        docs = cb_env.collection.get_and_lock_multi(unlock_keys, timedelta(milliseconds=500))
        validator.validate_multi_kv_op()

        validator.reset(op_name=OpName.UnlockMulti, nested_ops=[OpName.Unlock]*len(unlock_keys))
        cb_env.collection.unlock_multi(docs)
        validator.validate_multi_kv_op()


# exceptions are handled differently for multi-ops.  It is easy to just let a test rerun than to try and flesh out
# the error in one of the ops.
@pytest.mark.flaky(reruns=5, reruns_delay=1)
class ClassicKeyValueMultiMetricsTests(KeyValueMultiMetricsTestsSuite):
    """Classic (non-async) multi key-value tracing tests."""

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicKeyValueMultiMetricsTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicKeyValueMultiMetricsTests) if valid_test_method(meth)]
        test_list = set(KeyValueMultiMetricsTestsSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='cb_env', params=[MeterType.Basic, MeterType.NoOp])
    def couchbase_test_environment(self, cb_base_env, request):
        # a new environment and cluster is created
        cb_env = MetricsEnvironment.from_environment(cb_base_env, meter_type=request.param)
        cb_env.setup(num_docs=100)  # More docs since multi-ops consume 3 docs per test
        yield cb_env
        cb_env.teardown()
