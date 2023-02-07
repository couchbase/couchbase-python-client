#  Copyright 2016-2023. Couchbase, Inc.
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

import pytest

from couchbase.datastructures import (CouchbaseList,
                                      CouchbaseMap,
                                      CouchbaseQueue,
                                      CouchbaseSet)
from couchbase.exceptions import (DocumentNotFoundException,
                                  InvalidArgumentException,
                                  QueueEmpty)
from couchbase.result import OperationResult
from tests.environments import CollectionType


class DatastructuresTestSuite:

    TEST_MANIFEST = [
        'test_list',
        'test_map',
        'test_queue',
        'test_sets',
    ]

    def test_list(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        cb_list = cb_env.collection.couchbase_list(key)
        assert isinstance(cb_list, CouchbaseList)

        cb_list.append('world')
        rv = cb_list.get_at(0)
        assert str(rv) == 'world'

        cb_list.prepend('hello')
        rv = cb_list.get_at(0)
        assert str(rv) == 'hello'

        rv = cb_list.get_at(1)
        assert str(rv) == 'world'
        assert 2 == cb_list.size()

        cb_list.remove_at(1)
        res = cb_env.collection.get(key)
        assert ['hello'] == res.content_as[list]

        cb_list.append('world')
        res = cb_env.collection.get(key)
        assert ['hello', 'world'] == res.content_as[list]

        cb_list.set_at(1, 'after')
        res = cb_env.collection.get(key)
        assert ['hello', 'after'] == res.content_as[list]

        res = cb_list.get_all()
        assert ['hello', 'after'] == res

        res = cb_list.index_of('after')
        assert res == 1

        expected = ['hello', 'after']
        for idx, v in enumerate(cb_list):
            assert expected[idx] == v

        cb_list.clear()

        assert 0 == cb_list.size()

    def test_map(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        cb_map = cb_env.collection.couchbase_map(key)
        assert isinstance(cb_map, CouchbaseMap)

        cb_map.add('key1', 'val1')

        rv = cb_map.get('key1')
        assert rv == 'val1'

        assert 1 == cb_map.size()

        with pytest.raises(InvalidArgumentException):
            cb_map.remove('key2')

        cb_map.add('key2', 'val2')

        keys = cb_map.keys()
        assert ['key1', 'key2'] == keys

        values = cb_map.values()
        assert ['val1', 'val2'] == values

        assert cb_map.exists('key1') is True
        assert cb_map.exists('no-key') is False

        expected_keys = ['key1', 'key2']
        expected_values = ['val1', 'val2']
        for k, v in cb_map.items():
            assert k in expected_keys
            assert v in expected_values

        with pytest.raises(TypeError):
            for k, v in cb_map:
                assert k in expected_keys
                assert v in expected_values

        cb_map.remove('key1')
        assert 1 == cb_map.size()

        cb_map.clear()
        assert 0 == cb_map.size()

    def test_sets(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        cb_set = cb_env.collection.couchbase_set(key)
        assert isinstance(cb_set, CouchbaseSet)

        rv = cb_set.add(123)
        rv = cb_set.add(123)
        assert 1 == cb_set.size()
        assert cb_set.contains(123) is True

        rv = cb_set.remove(123)
        assert 0 == cb_set.size()
        rv = cb_set.remove(123)
        assert rv is None
        assert cb_set.contains(123) is False

        cb_set.add(1)
        cb_set.add(2)
        cb_set.add(3)
        cb_set.add(4)

        values = cb_set.values()
        assert values == [1, 2, 3, 4]
        cb_set.clear()
        assert 0 == cb_set.size()

    def test_queue(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        cb_queue = cb_env.collection.couchbase_queue(key)
        assert isinstance(cb_queue, CouchbaseQueue)

        cb_queue.push(1)
        cb_queue.push(2)
        cb_queue.push(3)

        # Pop the items now
        assert cb_queue.pop() == 1
        assert cb_queue.pop() == 2
        assert cb_queue.pop() == 3
        with pytest.raises(QueueEmpty):
            cb_queue.pop()

        cb_queue.push(1)
        cb_queue.push(2)
        cb_queue.push(3)

        assert cb_queue.size() == 3

        expected = [3, 2, 1]
        for idx, v in enumerate(cb_queue):
            assert expected[idx] == v

        cb_queue.clear()

        assert 0 == cb_queue.size()


class LegacyDatastructuresTestSuite:

    TEST_MANIFEST = [
        'test_list',
        'test_map',
        'test_queue',
        'test_sets',
    ]

    def test_list(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.list_append(key, 'world')

        cb_env.collection.list_append(key, 'world', create=True)
        rv = cb_env.collection.list_get(key, 0)
        assert str(rv) == 'world'

        cb_env.collection.list_prepend(key, 'hello')
        rv = cb_env.collection.list_get(key, 0)
        assert str(rv) == 'hello'
        rv = cb_env.collection.list_get(key, 1)
        assert str(rv) == 'world'
        assert 2 == cb_env.collection.list_size(key)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.list_get('not-a-key', 0)

        cb_env.collection.list_remove(key, 1)
        res = cb_env.collection.get(key)
        assert ['hello'] == res.content_as[list]

        cb_env.collection.list_append(key, 'world')
        res = cb_env.collection.get(key)
        assert ['hello', 'world'] == res.content_as[list]

        cb_env.collection.list_set(key, 1, 'after')
        res = cb_env.collection.get(key)
        assert ['hello', 'after'] == res.content_as[list]

    def test_map(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.map_add(key, 'key1', 'val1')

        cb_env.collection.map_add(key, 'key1', 'val1', create=True)

        rv = cb_env.collection.map_get(key, 'key1')
        assert rv == 'val1'

        assert 1 == cb_env.collection.map_size(key)

        with pytest.raises(IndexError):
            cb_env.collection.map_remove(key, 'key2')

        cb_env.collection.map_remove(key, 'key1')
        assert 0 == cb_env.collection.map_size(key)

    def test_sets(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.set_add(key, 123)

        rv = cb_env.collection.set_add(key, 123, create=True)
        assert isinstance(rv, OperationResult)
        rv = cb_env.collection.set_add(key, 123)
        assert rv is None
        assert 1 == cb_env.collection.set_size(key)
        assert cb_env.collection.set_contains(key, 123) is True

        rv = cb_env.collection.set_remove(key, 123)
        assert 0 == cb_env.collection.set_size(key)
        rv = cb_env.collection.set_remove(key, 123)
        assert rv is None
        assert cb_env.collection.set_contains(key, 123) is False

    def test_queue(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.queue_push(key, 1)

        rv = cb_env.collection.queue_push(key, 1, create=True)
        assert isinstance(rv, OperationResult)
        cb_env.collection.queue_push(key, 2)
        cb_env.collection.queue_push(key, 3)

        # Pop the items now
        assert cb_env.collection.queue_pop(key) == 1
        assert cb_env.collection.queue_pop(key) == 2
        assert cb_env.collection.queue_pop(key) == 3
        with pytest.raises(QueueEmpty):
            cb_env.collection.queue_pop(key)


class ClassicDatastructuresTests(DatastructuresTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicDatastructuresTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicDatastructuresTests) if valid_test_method(meth)]
        compare = set(DatastructuresTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_base_env.enable_bucket_mgmt()
        cb_base_env.setup(request.param, __name__)

        yield cb_base_env

        cb_base_env.teardown(request.param)


class ClassicLegacyDatastructuresTests(LegacyDatastructuresTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicLegacyDatastructuresTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicLegacyDatastructuresTests) if valid_test_method(meth)]
        compare = set(LegacyDatastructuresTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_base_env.enable_bucket_mgmt()
        cb_base_env.setup(request.param, __name__)

        yield cb_base_env

        cb_base_env.teardown(request.param)
