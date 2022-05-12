#  Copyright 2016-2022. Couchbase, Inc.
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

from ._test_utils import CollectionType, TestEnvironment


class LegacyDatastructuresTests:

    TEST_DS_KEY = 'ds-key'

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, couchbase_config, request):
        cb_env = TestEnvironment.get_environment(__name__, couchbase_config, request.param)

        if request.param == CollectionType.NAMED:
            cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        yield cb_env
        if request.param == CollectionType.NAMED:
            cb_env.try_n_times_till_exception(5, 3,
                                              cb_env.teardown_named_collections,
                                              raise_if_no_exception=False)

    @pytest.fixture()
    def remove_ds(self, cb_env) -> None:
        yield
        try:
            cb_env.collection.remove(self.TEST_DS_KEY)
        except DocumentNotFoundException:
            pass

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.usefixtures("remove_ds")
    def test_list(self, cb_env):
        cb = cb_env.collection

        with pytest.raises(DocumentNotFoundException):
            cb.list_append(self.TEST_DS_KEY, 'world')

        cb.list_append(self.TEST_DS_KEY, 'world', create=True)
        rv = cb.list_get(self.TEST_DS_KEY, 0)
        assert str(rv) == 'world'

        cb.list_prepend(self.TEST_DS_KEY, 'hello')
        rv = cb.list_get(self.TEST_DS_KEY, 0)
        assert str(rv) == 'hello'
        rv = cb.list_get(self.TEST_DS_KEY, 1)
        assert str(rv) == 'world'
        assert 2 == cb.list_size(self.TEST_DS_KEY)

        with pytest.raises(DocumentNotFoundException):
            cb.list_get('not-a-key', 0)

        cb.list_remove(self.TEST_DS_KEY, 1)
        res = cb.get(self.TEST_DS_KEY)
        assert ['hello'] == res.content_as[list]

        cb.list_append(self.TEST_DS_KEY, 'world')
        res = cb.get(self.TEST_DS_KEY)
        assert ['hello', 'world'] == res.content_as[list]

        cb.list_set(self.TEST_DS_KEY, 1, 'after')
        res = cb.get(self.TEST_DS_KEY)
        assert ['hello', 'after'] == res.content_as[list]

    @pytest.mark.usefixtures("remove_ds")
    def test_map(self, cb_env):
        cb = cb_env.collection

        with pytest.raises(DocumentNotFoundException):
            cb.map_add(self.TEST_DS_KEY, 'key1', 'val1')

        cb.map_add(self.TEST_DS_KEY, 'key1', 'val1', create=True)

        rv = cb.map_get(self.TEST_DS_KEY, 'key1')
        assert rv == 'val1'

        assert 1 == cb.map_size(self.TEST_DS_KEY)

        with pytest.raises(IndexError):
            cb.map_remove(self.TEST_DS_KEY, 'key2')

        cb.map_remove(self.TEST_DS_KEY, 'key1')
        assert 0 == cb.map_size(self.TEST_DS_KEY)

    @pytest.mark.usefixtures("remove_ds")
    def test_sets(self, cb_env):
        cb = cb_env.collection

        with pytest.raises(DocumentNotFoundException):
            cb.set_add(self.TEST_DS_KEY, 123)

        rv = cb.set_add(self.TEST_DS_KEY, 123, create=True)
        assert isinstance(rv, OperationResult)
        rv = cb.set_add(self.TEST_DS_KEY, 123)
        assert rv is None
        assert 1 == cb.set_size(self.TEST_DS_KEY)
        assert cb.set_contains(self.TEST_DS_KEY, 123) is True

        rv = cb.set_remove(self.TEST_DS_KEY, 123)
        assert 0 == cb.set_size(self.TEST_DS_KEY)
        rv = cb.set_remove(self.TEST_DS_KEY, 123)
        assert rv is None
        assert cb.set_contains(self.TEST_DS_KEY, 123) is False

    @pytest.mark.usefixtures("remove_ds")
    def test_queue(self, cb_env):
        cb = cb_env.collection

        with pytest.raises(DocumentNotFoundException):
            cb.queue_push(self.TEST_DS_KEY, 1)

        rv = cb.queue_push(self.TEST_DS_KEY, 1, create=True)
        assert isinstance(rv, OperationResult)
        cb.queue_push(self.TEST_DS_KEY, 2)
        cb.queue_push(self.TEST_DS_KEY, 3)

        # Pop the items now
        assert cb.queue_pop(self.TEST_DS_KEY) == 1
        assert cb.queue_pop(self.TEST_DS_KEY) == 2
        assert cb.queue_pop(self.TEST_DS_KEY) == 3
        with pytest.raises(QueueEmpty):
            cb.queue_pop(self.TEST_DS_KEY)


class DatastructuresTests:

    TEST_DS_KEY = 'ds-key'

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT])
    def couchbase_test_environment(self, couchbase_config, request):
        cb_env = TestEnvironment.get_environment(__name__, couchbase_config, request.param)

        if request.param == CollectionType.NAMED:
            cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        yield cb_env
        if request.param == CollectionType.NAMED:
            cb_env.try_n_times_till_exception(5, 3,
                                              cb_env.teardown_named_collections,
                                              raise_if_no_exception=False)

    @pytest.fixture()
    def remove_ds(self, cb_env) -> None:
        yield
        try:
            cb_env.collection.remove(self.TEST_DS_KEY)
        except DocumentNotFoundException:
            pass

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.usefixtures("remove_ds")
    def test_list(self, cb_env):

        cb_list = cb_env.collection.couchbase_list(self.TEST_DS_KEY)
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
        res = cb_env.collection.get(self.TEST_DS_KEY)
        assert ['hello'] == res.content_as[list]

        cb_list.append('world')
        res = cb_env.collection.get(self.TEST_DS_KEY)
        assert ['hello', 'world'] == res.content_as[list]

        cb_list.set_at(1, 'after')
        res = cb_env.collection.get(self.TEST_DS_KEY)
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

    @pytest.mark.usefixtures("remove_ds")
    def test_map(self, cb_env):

        cb_map = cb_env.collection.couchbase_map(self.TEST_DS_KEY)
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

    @pytest.mark.usefixtures("remove_ds")
    def test_sets(self, cb_env):

        cb_set = cb_env.collection.couchbase_set(self.TEST_DS_KEY)
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

    @pytest.mark.usefixtures("remove_ds")
    def test_queue(self, cb_env):
        cb_queue = cb_env.collection.couchbase_queue(self.TEST_DS_KEY)
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
