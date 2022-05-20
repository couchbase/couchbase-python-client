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
import pytest_asyncio

from acouchbase.cluster import get_event_loop
from acouchbase.datastructures import (CouchbaseList,
                                       CouchbaseMap,
                                       CouchbaseQueue,
                                       CouchbaseSet)
from couchbase.exceptions import (DocumentNotFoundException,
                                  InvalidArgumentException,
                                  QueueEmpty)

from ._test_utils import CollectionType, TestEnvironment


class DatastructuresTests:

    TEST_DS_KEY = 'ds-key'

    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest_asyncio.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    async def couchbase_test_environment(self, couchbase_config, request):
        cb_env = await TestEnvironment.get_environment(__name__,
                                                       couchbase_config,
                                                       request.param,
                                                       manage_buckets=True)
        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        yield cb_env
        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times_till_exception(5, 3,
                                                    cb_env.teardown_named_collections,
                                                    raise_if_no_exception=False)

    @pytest_asyncio.fixture()
    async def remove_ds(self, cb_env) -> None:
        yield
        try:
            await cb_env.collection.remove(self.TEST_DS_KEY)
        except DocumentNotFoundException:
            pass

    @pytest.mark.usefixtures("remove_ds")
    @pytest.mark.asyncio
    async def test_list(self, cb_env):

        cb_list = cb_env.collection.couchbase_list(self.TEST_DS_KEY)
        assert isinstance(cb_list, CouchbaseList)

        await cb_list.append('world')
        rv = await cb_list.get_at(0)
        assert str(rv) == 'world'

        await cb_list.prepend('hello')
        rv = await cb_list.get_at(0)
        assert str(rv) == 'hello'

        rv = await cb_list.get_at(1)
        assert str(rv) == 'world'
        assert 2 == await cb_list.size()

        await cb_list.remove_at(1)
        res = await cb_env.collection.get(self.TEST_DS_KEY)
        assert ['hello'] == res.content_as[list]

        await cb_list.append('world')
        res = await cb_env.collection.get(self.TEST_DS_KEY)
        assert ['hello', 'world'] == res.content_as[list]

        await cb_list.set_at(1, 'after')
        res = await cb_env.collection.get(self.TEST_DS_KEY)
        assert ['hello', 'after'] == res.content_as[list]

        res = await cb_list.get_all()
        assert ['hello', 'after'] == res

        res = await cb_list.index_of('after')
        assert res == 1

        expected = ['hello', 'after']
        idx = 0
        async for v in cb_list:
            assert expected[idx] == v
            idx += 1

        await cb_list.clear()

        assert 0 == await cb_list.size()

    @pytest.mark.usefixtures("remove_ds")
    @pytest.mark.asyncio
    async def test_map(self, cb_env):

        cb_map = cb_env.collection.couchbase_map(self.TEST_DS_KEY)
        assert isinstance(cb_map, CouchbaseMap)

        await cb_map.add('key1', 'val1')

        rv = await cb_map.get('key1')
        assert rv == 'val1'

        assert 1 == await cb_map.size()

        with pytest.raises(InvalidArgumentException):
            await cb_map.remove('key2')

        await cb_map.add('key2', 'val2')

        keys = await cb_map.keys()
        assert ['key1', 'key2'] == keys

        values = await cb_map.values()
        assert ['val1', 'val2'] == values

        assert await cb_map.exists('key1') is True
        assert await cb_map.exists('no-key') is False

        expected_keys = ['key1', 'key2']
        expected_values = ['val1', 'val2']
        items = await cb_map.items()
        for k, v in items:
            assert k in expected_keys
            assert v in expected_values

        with pytest.raises(TypeError):
            async for k, v in cb_map:
                assert k in expected_keys
                assert v in expected_values

        await cb_map.remove('key1')
        assert 1 == await cb_map.size()

        await cb_map.clear()
        assert 0 == await cb_map.size()

    @pytest.mark.usefixtures("remove_ds")
    @pytest.mark.asyncio
    async def test_sets(self, cb_env):

        cb_set = cb_env.collection.couchbase_set(self.TEST_DS_KEY)
        assert isinstance(cb_set, CouchbaseSet)

        rv = await cb_set.add(123)
        rv = await cb_set.add(123)
        assert 1 == await cb_set.size()
        assert await cb_set.contains(123) is True

        rv = await cb_set.remove(123)
        assert 0 == await cb_set.size()
        rv = await cb_set.remove(123)
        assert rv is None
        assert await cb_set.contains(123) is False

        await cb_set.add(1)
        await cb_set.add(2)
        await cb_set.add(3)
        await cb_set.add(4)

        values = await cb_set.values()
        assert values == [1, 2, 3, 4]
        await cb_set.clear()
        assert 0 == await cb_set.size()

    @pytest.mark.usefixtures("remove_ds")
    @pytest.mark.asyncio
    async def test_queue(self, cb_env):
        cb_queue = cb_env.collection.couchbase_queue(self.TEST_DS_KEY)
        assert isinstance(cb_queue, CouchbaseQueue)

        await cb_queue.push(1)
        await cb_queue.push(2)
        await cb_queue.push(3)

        # Pop the items now
        assert await cb_queue.pop() == 1
        assert await cb_queue.pop() == 2
        assert await cb_queue.pop() == 3
        with pytest.raises(QueueEmpty):
            await cb_queue.pop()

        await cb_queue.push(1)
        await cb_queue.push(2)
        await cb_queue.push(3)

        assert await cb_queue.size() == 3

        expected = [3, 2, 1]
        idx = 0
        async for v in cb_queue:
            assert expected[idx] == v
            idx += 1

        await cb_queue.clear()

        assert 0 == await cb_queue.size()
