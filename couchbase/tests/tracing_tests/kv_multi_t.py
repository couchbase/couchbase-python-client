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
                    Optional)

import pytest

from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.observability import OpName
from couchbase.options import (ExistsMultiOptions,
                               GetAllReplicasOptions,
                               GetAndLockMultiOptions,
                               GetAnyReplicaOptions,
                               GetMultiOptions,
                               InsertMultiOptions,
                               RemoveMultiOptions,
                               ReplaceMultiOptions,
                               TouchMultiOptions,
                               UnlockMultiOptions,
                               UpsertMultiOptions)
from tests.environments.tracing import KeyValueMultiTracingEnvironment
from tests.environments.tracing.base_tracing_environment import TracingType


class KeyValueMultiTracingTestsSuite:
    """Test suite for multi key-value operation tracing.

    Tests multi-document read operations (get_multi, get_all_replicas_multi, etc.)
    with and without parent spans, including error handling.
    """

    TEST_MANIFEST = [
        'test_multi_kv_op_no_dispatch_failure',
        'test_multi_kv_op',
        'test_multi_kv_op_with_error',
        'test_multi_kv_op_with_parent',
        'test_multi_mutation_op',
        # 'test_multi_mutation_op_with_error',
        'test_multi_mutation_op_with_parent',
        'test_multi_kv_unlock_op',
        'test_multi_kv_unlock_op_with_parent'
    ]

    @pytest.fixture(autouse=True)
    def reset_validator(self, cb_env: KeyValueMultiTracingEnvironment):
        yield
        cb_env.kv_span_validator.reset(clear_nested_ops=True)

    @pytest.fixture()
    def unlock_keys(self) -> List[str]:
        return ['unlock_doc_1', 'unlock_doc_2', 'unlock_doc_3']

    @pytest.fixture()
    def setup_unlock_multi(self, cb_env: KeyValueMultiTracingEnvironment, unlock_keys: List[str]) -> None:
        docs = {key: {'test': 'data'} for key in unlock_keys}
        cb_env.collection.upsert_multi(docs)

    @pytest.fixture()
    def teardown_unlock_multi(self, cb_env: KeyValueMultiTracingEnvironment, unlock_keys: List[str]):
        yield
        for _ in range(3):
            res = cb_env.collection.remove_multi(unlock_keys)
            if res.all_ok:
                break
            cb_env.sleep(1)

    @pytest.mark.parametrize('op_name, keys_as_list', [
        (OpName.ExistsMulti, True),
        (OpName.GetMulti, True),
        (OpName.GetAllReplicasMulti, True),
        (OpName.GetAndLockMulti, True),
        (OpName.GetAnyReplicaMulti, True),
        (OpName.InsertMulti, False),
        (OpName.RemoveMulti, True),
        (OpName.ReplaceMulti, False),
        (OpName.TouchMulti, True),
        (OpName.UnlockMulti, True),
        (OpName.UpsertMulti, False),
    ])
    def test_multi_kv_op_no_dispatch_failure(
        self,
        cb_env: KeyValueMultiTracingEnvironment,
        op_name: OpName,
        keys_as_list: bool
    ) -> None:
        keys = [cb_env.get_new_doc(key_only=True) for _ in range(3)]
        validator = cb_env.kv_span_validator
        if op_name is OpName.UnlockMulti:
            validator.reset(op_name=op_name,
                            multi_op_key_count=len(keys),
                            validate_error=True,
                            error_before_dispatch=True,
                            expect_request_span=False)
        else:
            validator.reset(op_name=op_name,
                            multi_op_key_count=len(keys),
                            validate_error=True,
                            error_before_dispatch=True)
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
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name, nested_ops', [
        (OpName.GetMulti, None),
        (OpName.GetAllReplicasMulti, [OpName.Get, OpName.GetReplica]),
        (OpName.GetAnyReplicaMulti, [OpName.Get, OpName.GetReplica]),
        (OpName.ExistsMulti, None),
    ])
    def test_multi_kv_op(
        self,
        cb_env: KeyValueMultiTracingEnvironment,
        op_name: OpName,
        nested_ops: Optional[List[OpName]]
    ) -> None:
        """Test multi-document read operations with tracing."""
        keys_and_docs = cb_env.get_docs(3)
        keys = list(keys_and_docs.keys())
        validator = cb_env.kv_span_validator
        validator.reset(op_name=op_name, clear_parent_span=True, nested_ops=nested_ops, multi_op_key_count=len(keys))
        operation = getattr(cb_env.collection, op_name.value)
        operation(keys)
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name, nested_ops', [
        (OpName.GetMulti, None),
        (OpName.GetAllReplicasMulti, [OpName.Get, OpName.GetReplica]),
        (OpName.GetAnyReplicaMulti, [OpName.Get, OpName.GetReplica]),
        (OpName.GetAndLockMulti, None),
    ])
    def test_multi_kv_op_with_error(
        self,
        cb_env: KeyValueMultiTracingEnvironment,
        op_name: OpName,
        nested_ops: Optional[List[OpName]],
    ) -> None:
        """Test multi-document read operations with errors and verify error span status."""
        keys = [cb_env.get_new_doc(key_only=True) for _ in range(3)]
        validator = cb_env.kv_span_validator
        # We set return_exceptions to False to confirm we get the span set to an error status.
        # Since we don't loop through all the ops, GetAnyReplicaMulti needs to "cheat" and only count 1 key
        # for the multi_op_key_count since it will stop after the first failure and not attempt the other keys.
        # This does not apply to legacy tracing
        if not validator.is_legacy and op_name == OpName.GetAnyReplicaMulti:
            multi_op_key_count = 1
        else:
            multi_op_key_count = len(keys)
        validator.reset(op_name=op_name,
                        clear_parent_span=True,
                        nested_ops=nested_ops,
                        multi_op_key_count=multi_op_key_count,
                        validate_error=True)
        operation = getattr(cb_env.collection, op_name.value)
        try:
            if op_name is OpName.GetAndLockMulti:
                operation(keys, timedelta(seconds=1), return_exceptions=False)
            else:
                operation(keys, return_exceptions=False)
        except Exception:
            pass
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name, opts_cls, nested_ops', [
        (OpName.GetMulti, GetMultiOptions, None),
        (OpName.GetAllReplicasMulti, GetAllReplicasOptions, [OpName.Get, OpName.GetReplica]),
        (OpName.GetAnyReplicaMulti, GetAnyReplicaOptions, [OpName.Get, OpName.GetReplica]),
        (OpName.ExistsMulti, ExistsMultiOptions, None),
    ])
    def test_multi_kv_op_with_parent(
        self,
        cb_env: KeyValueMultiTracingEnvironment,
        op_name: OpName,
        opts_cls: Any,
        nested_ops: Optional[List[OpName]]
    ) -> None:
        """Test multi-document read operations with parent span propagation."""
        keys_and_docs = cb_env.get_docs(3)
        keys = list(keys_and_docs.keys())
        validator = cb_env.kv_span_validator
        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(do_not_clear_spans=True,
                        op_name=op_name,
                        parent_span=parent_span,
                        nested_ops=nested_ops,
                        multi_op_key_count=len(keys))
        operation = getattr(cb_env.collection, op_name.value)
        operation(keys, opts_cls(parent_span=parent_span))
        validator.validate_kv_op(end_parent=True)

    @pytest.mark.parametrize('op_name', [
        OpName.InsertMulti,
        OpName.UpsertMulti,
        OpName.ReplaceMulti,
        OpName.RemoveMulti,
        OpName.TouchMulti,
    ])
    def test_multi_mutation_op(self,
                               cb_env: KeyValueMultiTracingEnvironment,
                               op_name: OpName) -> None:
        validator = cb_env.kv_span_validator
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

        validator.reset(op_name=op_name, multi_op_key_count=3)
        operation = getattr(cb_env.collection, op_name.value)
        if op_name == OpName.TouchMulti:
            operation(docs, timedelta(seconds=1))
        else:
            operation(docs)
        validator.validate_kv_op()

    @pytest.mark.parametrize('op_name, opts_cls', [
        (OpName.InsertMulti, InsertMultiOptions),
        (OpName.UpsertMulti, UpsertMultiOptions),
        (OpName.ReplaceMulti, ReplaceMultiOptions),
        (OpName.RemoveMulti, RemoveMultiOptions),
        (OpName.TouchMulti, TouchMultiOptions),
    ])
    def test_multi_mutation_op_with_parent(self,
                                           cb_env: KeyValueMultiTracingEnvironment,
                                           op_name: OpName,
                                           opts_cls: Any) -> None:
        validator = cb_env.kv_span_validator
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

        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{op_name.value}_span')
        validator.reset(do_not_clear_spans=True, op_name=op_name, parent_span=parent_span, multi_op_key_count=3)
        operation = getattr(cb_env.collection, op_name.value)
        if op_name == OpName.TouchMulti:
            operation(docs, timedelta(seconds=1), opts_cls(parent_span=parent_span))
        else:
            operation(docs, opts_cls(parent_span=parent_span))
        validator.validate_kv_op(end_parent=True)

    @pytest.mark.usefixtures('setup_unlock_multi')
    @pytest.mark.usefixtures('teardown_unlock_multi')
    def test_multi_kv_unlock_op(self, cb_env: KeyValueMultiTracingEnvironment, unlock_keys: List[str]) -> None:
        validator = cb_env.kv_span_validator

        # Get and lock documents first to get CAS values
        # since we need get_and_lock_multi to test unlock_multi, we validate here
        validator.reset(op_name=OpName.GetAndLockMulti, multi_op_key_count=3)
        docs = cb_env.collection.get_and_lock_multi(unlock_keys, timedelta(milliseconds=500))
        validator.validate_kv_op()

        validator.reset(op_name=OpName.UnlockMulti, multi_op_key_count=3)
        cb_env.collection.unlock_multi(docs)
        validator.validate_kv_op()

    @pytest.mark.usefixtures('setup_unlock_multi')
    @pytest.mark.usefixtures('teardown_unlock_multi')
    def test_multi_kv_unlock_op_with_parent(self,
                                            cb_env: KeyValueMultiTracingEnvironment,
                                            unlock_keys: List[str]) -> None:
        validator = cb_env.kv_span_validator

        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{OpName.GetAndLockMulti.value}_span')
        # Get and lock documents first to get CAS values
        # since we need get_and_lock_multi to test unlock_multi, we validate here
        validator.reset(do_not_clear_spans=True,
                        op_name=OpName.GetAndLockMulti,
                        parent_span=parent_span,
                        multi_op_key_count=len(unlock_keys))
        docs = cb_env.collection.get_and_lock_multi(unlock_keys,
                                                    timedelta(milliseconds=500),
                                                    GetAndLockMultiOptions(parent_span=parent_span))
        validator.validate_kv_op(end_parent=True)

        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{OpName.UnlockMulti.value}_span')
        validator.reset(do_not_clear_spans=True,
                        op_name=OpName.UnlockMulti,
                        parent_span=parent_span,
                        multi_op_key_count=len(unlock_keys))
        cb_env.collection.unlock_multi(docs, UnlockMultiOptions(parent_span=parent_span))
        validator.validate_kv_op(end_parent=True)


# exceptions are handled differently for multi-ops.  It is easy to just let a test rerun than to try and flesh out
# the error in one of the ops.
@pytest.mark.flaky(reruns=5, reruns_delay=1)
class ClassicKeyValueMultiTracingTests(KeyValueMultiTracingTestsSuite):
    """Classic (non-async) multi key-value tracing tests."""

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicKeyValueMultiTracingTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicKeyValueMultiTracingTests) if valid_test_method(meth)]
        test_list = set(KeyValueMultiTracingTestsSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='cb_env', params=[TracingType.ThresholdLogging,
                                                          TracingType.Legacy,
                                                          TracingType.NoOp,
                                                          TracingType.Modern
                                                          ])
    def couchbase_test_environment(self, cb_base_env, request):
        # a new environment and cluster is created
        cb_env = KeyValueMultiTracingEnvironment.from_environment(cb_base_env, tracing_type=request.param)
        cb_env.setup(num_docs=100)  # More docs since multi-ops consume 3 docs per test
        yield cb_env
        cb_env.teardown()
        # TODO(PYCBC-1746): Update once legacy tracing logic is removed.
        #                   See PYCBC-1748 for details on issue, this seems to mainly impact the error scenarios.
        if request.param == TracingType.Legacy:
            cb_env.cluster.close()
