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

import pytest

from couchbase.exceptions import InvalidArgumentException, QueueEmpty
from couchbase.logic.observability import OpName
from tests.environments.metrics import MetricsEnvironment
from tests.environments.metrics.metrics_environment import MeterType


class DataStructuresMetricsTestsSuite:

    TEST_MANIFEST = [
        'test_kv_ds_list',
        'test_kv_ds_map',
        'test_kv_ds_queue',
        'test_kv_ds_set',
    ]

    def test_kv_ds_list(self, cb_env: MetricsEnvironment) -> None:
        """Test CouchbaseList operations with tracing."""
        test_key = cb_env.get_new_doc(key_only=True)
        cb_list = cb_env.collection.couchbase_list(test_key)

        validator = cb_env.kv_meter_validator
        # we have multiple sub-ops as the doc does not exist initially
        sub_ops = [(OpName.MutateIn, True), (OpName.Insert, False), (OpName.MutateIn, False)]
        validator.reset(op_name=OpName.ListAppend, sub_op_names=sub_ops)
        cb_list.append('world')
        validator.validate_ds_op()

        sub_ops = [(OpName.LookupIn, False)]
        validator.reset(op_name=OpName.ListGetAt, sub_op_names=sub_ops)
        rv = cb_list.get_at(0)
        assert str(rv) == 'world'
        validator.validate_ds_op()

        sub_ops = [(OpName.MutateIn, False)]
        validator.reset(op_name=OpName.ListPrepend, sub_op_names=sub_ops)
        cb_list.prepend('hello')
        validator.validate_ds_op()

        sub_ops = [(OpName.LookupIn, False)]
        validator.reset(op_name=OpName.ListGetAt, sub_op_names=sub_ops)
        rv = cb_list.get_at(0)
        assert str(rv) == 'hello'
        validator.validate_ds_op()

        sub_ops = [(OpName.LookupIn, False)]
        validator.reset(op_name=OpName.ListGetAt, sub_op_names=sub_ops)
        rv = cb_list.get_at(1)
        assert str(rv) == 'world'
        validator.validate_ds_op()

        sub_ops = [(OpName.LookupIn, False)]
        validator.reset(op_name=OpName.ListSize, sub_op_names=sub_ops)
        assert 2 == cb_list.size()
        validator.validate_ds_op()

        sub_ops = [(OpName.MutateIn, False)]
        validator.reset(op_name=OpName.ListRemoveAt, sub_op_names=sub_ops)
        cb_list.remove_at(1)
        validator.validate_ds_op()

        sub_ops = [(OpName.MutateIn, False)]
        validator.reset(op_name=OpName.ListAppend, sub_op_names=sub_ops)
        cb_list.append('world')
        validator.validate_ds_op()

        sub_ops = [(OpName.MutateIn, False)]
        validator.reset(op_name=OpName.ListSetAt, sub_op_names=sub_ops)
        cb_list.set_at(1, 'after')
        validator.validate_ds_op()

        sub_ops = [(OpName.Get, False)]
        validator.reset(op_name=OpName.ListGetAll, sub_op_names=sub_ops)
        res = cb_list.get_all()
        assert ['hello', 'after'] == res
        validator.validate_ds_op()

        sub_ops = [(OpName.Get, False)]
        validator.reset(op_name=OpName.ListIndexOf, sub_op_names=sub_ops)
        res = cb_list.index_of('after')
        assert res == 1
        validator.validate_ds_op()

        expected = ['hello', 'after']
        for idx, v in enumerate(cb_list):
            assert expected[idx] == v
        # __iter()__ will call a get under the hood
        cb_env.meter.clear()

        sub_ops = [(OpName.Remove, False)]
        validator.reset(op_name=OpName.ListClear, sub_op_names=sub_ops)
        cb_list.clear()
        validator.validate_ds_op()

        # we have multiple sub-ops as the doc does not exist initially
        sub_ops = [(OpName.LookupIn, True), (OpName.Insert, False), (OpName.LookupIn, False)]
        validator.reset(op_name=OpName.ListSize, sub_op_names=sub_ops)
        assert 0 == cb_list.size()
        validator.validate_ds_op()
        cb_env.collection.remove(test_key)

    def test_kv_ds_map(self, cb_env: MetricsEnvironment) -> None:
        """Test CouchbaseMap operations with tracing."""
        test_key = cb_env.get_new_doc(key_only=True)
        cb_map = cb_env.collection.couchbase_map(test_key)

        validator = cb_env.kv_meter_validator
        sub_ops = [(OpName.MutateIn, True), (OpName.Insert, False), (OpName.MutateIn, False)]
        validator.reset(op_name=OpName.MapAdd, sub_op_names=sub_ops)
        cb_map.add('key1', 'val1')
        validator.validate_ds_op()

        sub_ops = [(OpName.LookupIn, False)]
        validator.reset(op_name=OpName.MapGet, sub_op_names=sub_ops)
        rv = cb_map.get('key1')
        assert rv == 'val1'
        validator.validate_ds_op()

        sub_ops = [(OpName.LookupIn, False)]
        validator.reset(op_name=OpName.MapSize, sub_op_names=sub_ops)
        assert 1 == cb_map.size()
        validator.validate_ds_op()

        sub_ops = [(OpName.MutateIn, True)]
        validator.reset(op_name=OpName.MapRemove, sub_op_names=sub_ops, validate_error=True)
        try:
            cb_map.remove('key2')
        except InvalidArgumentException:
            pass
        validator.validate_ds_op()

        sub_ops = [(OpName.MutateIn, False)]
        # reset validate_error as previous call was validating
        validator.reset(op_name=OpName.MapAdd, sub_op_names=sub_ops, validate_error=False)
        cb_map.add('key2', 'val2')
        validator.validate_ds_op()

        sub_ops = [(OpName.Get, False)]
        validator.reset(op_name=OpName.MapKeys, sub_op_names=sub_ops)
        keys = cb_map.keys()
        assert ['key1', 'key2'] == keys
        validator.validate_ds_op()

        sub_ops = [(OpName.Get, False)]
        validator.reset(op_name=OpName.MapValues, sub_op_names=sub_ops)
        values = cb_map.values()
        assert ['val1', 'val2'] == values
        validator.validate_ds_op()

        sub_ops = [(OpName.LookupIn, False)]
        validator.reset(op_name=OpName.MapExists, sub_op_names=sub_ops)
        assert cb_map.exists('key1') is True
        validator.validate_ds_op()

        validator.reset(op_name=OpName.MapExists, sub_op_names=sub_ops)
        assert cb_map.exists('no-key') is False
        validator.validate_ds_op()

        sub_ops = [(OpName.Get, False)]
        validator.reset(op_name=OpName.MapItems, sub_op_names=sub_ops)
        expected_keys = ['key1', 'key2']
        expected_values = ['val1', 'val2']
        for k, v in cb_map.items():
            assert k in expected_keys
            assert v in expected_values

        validator.validate_ds_op()

        sub_ops = [(OpName.MutateIn, False)]
        validator.reset(op_name=OpName.MapRemove, sub_op_names=sub_ops)
        cb_map.remove('key1')
        validator.validate_ds_op()

        sub_ops = [(OpName.LookupIn, False)]
        validator.reset(op_name=OpName.MapSize, sub_op_names=sub_ops)
        assert 1 == cb_map.size()
        validator.validate_ds_op()

        sub_ops = [(OpName.Remove, False)]
        validator.reset(op_name=OpName.MapClear, sub_op_names=sub_ops)
        cb_map.clear()
        validator.validate_ds_op()

        sub_ops = [(OpName.LookupIn, True), (OpName.Insert, False), (OpName.LookupIn, False)]
        validator.reset(op_name=OpName.MapSize, sub_op_names=sub_ops)
        assert 0 == cb_map.size()
        validator.validate_ds_op()

    def test_kv_ds_queue(self, cb_env: MetricsEnvironment) -> None:
        """Test CouchbaseQueue operations with tracing."""
        test_key = cb_env.get_new_doc(key_only=True)
        cb_queue = cb_env.collection.couchbase_queue(test_key)

        validator = cb_env.kv_meter_validator
        sub_ops = [(OpName.MutateIn, True), (OpName.Insert, False), (OpName.MutateIn, False)]
        validator.reset(op_name=OpName.QueuePush, sub_op_names=sub_ops)
        cb_queue.push(1)
        validator.validate_ds_op()

        sub_ops = [(OpName.MutateIn, False)]
        validator.reset(op_name=OpName.QueuePush, sub_op_names=sub_ops)
        cb_queue.push(2)
        validator.validate_ds_op()

        validator.reset(op_name=OpName.QueuePush, sub_op_names=sub_ops)
        cb_queue.push(3)
        validator.validate_ds_op()

        sub_ops = [(OpName.LookupIn, False), (OpName.MutateIn, False)]
        validator.reset(op_name=OpName.QueuePop, sub_op_names=sub_ops)
        assert cb_queue.pop() == 1
        validator.validate_ds_op()

        validator.reset(op_name=OpName.QueuePop, sub_op_names=sub_ops)
        assert cb_queue.pop() == 2
        validator.validate_ds_op()

        validator.reset(op_name=OpName.QueuePop, sub_op_names=sub_ops)
        assert cb_queue.pop() == 3
        validator.validate_ds_op()

        sub_ops = [(OpName.LookupIn, False), (OpName.MutateIn, True)]
        validator.reset(op_name=OpName.QueuePop, sub_op_names=sub_ops, validate_error=True)
        try:
            cb_queue.pop()
        except QueueEmpty:
            pass
        validator.validate_ds_op()

        sub_ops = [(OpName.MutateIn, False)]
        validator.reset(op_name=OpName.QueuePush, sub_op_names=sub_ops, validate_error=False)
        cb_queue.push(1)
        validator.validate_ds_op()

        validator.reset(op_name=OpName.QueuePush, sub_op_names=sub_ops)
        cb_queue.push(2)
        validator.validate_ds_op()

        validator.reset(op_name=OpName.QueuePush, sub_op_names=sub_ops)
        cb_queue.push(3)
        validator.validate_ds_op()

        sub_ops = [(OpName.LookupIn, False)]
        validator.reset(op_name=OpName.QueueSize, sub_op_names=sub_ops)
        assert cb_queue.size() == 3
        validator.validate_ds_op()

        sub_ops = [(OpName.Remove, False)]
        validator.reset(op_name=OpName.QueueClear, sub_op_names=sub_ops)
        cb_queue.clear()
        validator.validate_ds_op()

        sub_ops = [(OpName.LookupIn, True), (OpName.Insert, False), (OpName.LookupIn, False)]
        validator.reset(op_name=OpName.QueueSize, sub_op_names=sub_ops)
        assert 0 == cb_queue.size()
        validator.validate_ds_op()

    def test_kv_ds_set(self, cb_env: MetricsEnvironment) -> None:
        """Test CouchbaseSet operations with tracing."""
        test_key = cb_env.get_new_doc(key_only=True)
        cb_set = cb_env.collection.couchbase_set(test_key)

        validator = cb_env.kv_meter_validator
        sub_ops = [(OpName.MutateIn, True), (OpName.Insert, False), (OpName.MutateIn, False)]
        validator.reset(op_name=OpName.SetAdd, sub_op_names=sub_ops)
        rv = cb_set.add(123)
        validator.validate_ds_op()

        sub_ops = [(OpName.MutateIn, False)]
        validator.reset(op_name=OpName.SetAdd, sub_op_names=sub_ops)
        rv = cb_set.add(123)
        validator.validate_ds_op()

        sub_ops = [(OpName.LookupIn, False)]
        validator.reset(op_name=OpName.SetSize, sub_op_names=sub_ops)
        assert 1 == cb_set.size()
        validator.validate_ds_op()

        sub_ops = [(OpName.Get, False)]
        validator.reset(op_name=OpName.SetContains, sub_op_names=sub_ops)
        assert cb_set.contains(123) is True
        validator.validate_ds_op()

        sub_ops = [(OpName.Get, False), (OpName.MutateIn, False)]
        validator.reset(op_name=OpName.SetRemove, sub_op_names=sub_ops)
        rv = cb_set.remove(123)
        validator.validate_ds_op()

        sub_ops = [(OpName.LookupIn, False)]
        validator.reset(op_name=OpName.SetSize, sub_op_names=sub_ops)
        assert 0 == cb_set.size()
        validator.validate_ds_op()

        # remove logic does not get to mutate_in op as value does not exist in the set
        sub_ops = [(OpName.Get, False)]
        validator.reset(op_name=OpName.SetRemove, sub_op_names=sub_ops)
        rv = cb_set.remove(123)
        assert rv is None
        validator.validate_ds_op()

        sub_ops = [(OpName.Get, False)]
        validator.reset(op_name=OpName.SetContains, sub_op_names=sub_ops)
        assert cb_set.contains(123) is False
        validator.validate_ds_op()

        sub_ops = [(OpName.MutateIn, False)]
        validator.reset(op_name=OpName.SetAdd, sub_op_names=sub_ops)
        cb_set.add(1)
        validator.validate_ds_op()

        validator.reset(op_name=OpName.SetAdd, sub_op_names=sub_ops)
        cb_set.add(2)
        validator.validate_ds_op()

        validator.reset(op_name=OpName.SetAdd, sub_op_names=sub_ops)
        cb_set.add(3)
        validator.validate_ds_op()

        validator.reset(op_name=OpName.SetAdd, sub_op_names=sub_ops)
        cb_set.add(4)
        validator.validate_ds_op()

        sub_ops = [(OpName.Get, False)]
        validator.reset(op_name=OpName.SetValues, sub_op_names=sub_ops)
        values = cb_set.values()
        assert values == [1, 2, 3, 4]
        validator.validate_ds_op()

        sub_ops = [(OpName.Remove, False)]
        validator.reset(op_name=OpName.SetClear, sub_op_names=sub_ops)
        cb_set.clear()
        validator.validate_ds_op()

        sub_ops = [(OpName.LookupIn, True), (OpName.Insert, False), (OpName.LookupIn, False)]
        validator.reset(op_name=OpName.SetSize, sub_op_names=sub_ops)
        assert 0 == cb_set.size()
        validator.validate_ds_op()


class ClassicDataStructuresMetricsTests(DataStructuresMetricsTestsSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicDataStructuresMetricsTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicDataStructuresMetricsTests) if valid_test_method(meth)]
        test_list = set(DataStructuresMetricsTestsSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='cb_env', params=[MeterType.Basic, MeterType.NoOp])
    def couchbase_test_environment(self, cb_base_env, request):
        # Data structures use MetricsEnvironment (same as single KV ops)
        cb_env = MetricsEnvironment.from_environment(cb_base_env, meter_type=request.param)
        cb_env.setup(num_docs=50)
        yield cb_env
        cb_env.teardown()
