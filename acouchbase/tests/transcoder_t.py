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

import json

import pytest
import pytest_asyncio

from acouchbase.cluster import get_event_loop
from couchbase.exceptions import DocumentNotFoundException, ValueFormatException

from ._test_utils import (CollectionType,
                          KVPair,
                          TestEnvironment)


class DefaultTranscoderTests:

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

    @pytest_asyncio.fixture(name="new_kvp")
    async def new_key_and_value_with_reset(self, cb_env) -> KVPair:
        key, value = await cb_env.get_new_key_value()
        yield KVPair(key, value)
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

    @pytest.mark.flaky(reruns=5)
    @pytest.mark.asyncio
    async def test_default_tc_json_upsert(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value

        await cb.upsert(key, value)
        res = await cb.get(key)
        result = res.content_as[dict]
        assert result is not None
        assert isinstance(result, dict)
        assert result == value

    @pytest.mark.asyncio
    async def test_default_tc_json_insert(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        await cb.insert(key, value)

        res = cb.get(key)
        res = await cb.get(key)
        result = res.content_as[dict]
        assert result is not None
        assert isinstance(result, dict)
        assert result == value

    @pytest.mark.asyncio
    async def test_default_tc_json_replace(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        await cb.upsert(key, value)
        value['new_content'] = 'new content!'
        await cb.replace(key, value)
        res = await cb.get(key)
        result = res.content_as[dict]
        assert result is not None
        assert isinstance(result, dict)
        assert result == value

    # default TC: no transcoder set in ClusterOptions or KV options

    @pytest.mark.asyncio
    async def test_default_tc_string_upsert(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = "some string content"
        await cb.upsert(key, value)
        res = await cb.get(key)
        result = res.content_as[str]
        assert result is not None
        assert isinstance(result, str)
        assert result == value

    @pytest.mark.asyncio
    async def test_default_tc_string_insert(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = "some string content"
        await cb.insert(key, value)
        res = await cb.get(key)
        result = res.content_as[str]
        assert result is not None
        assert isinstance(result, str)
        assert result == value

    @pytest.mark.asyncio
    async def test_default_tc_string_replace(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = "some string content"
        await cb.upsert(key, value)
        new_content = "new string content"
        await cb.replace(key, new_content)
        res = await cb.get(key)
        result = res.content_as[str]
        assert result is not None
        assert isinstance(result, str)
        assert result == new_content

    @pytest.mark.asyncio
    async def test_default_tc_binary_upsert(self, cb_env):
        cb = cb_env.collection
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        with pytest.raises(ValueFormatException):
            await cb.upsert('some-test-bytes', content)

    @pytest.mark.asyncio
    async def test_default_tc_bytearray_upsert(self, cb_env):
        cb = cb_env.collection
        content = bytearray(json.dumps("Here are some bytes"), "utf-8")
        with pytest.raises(ValueFormatException):
            await cb.upsert('some-test-bytes', content)

    @pytest.mark.asyncio
    async def test_default_tc_binary_insert(self, cb_env):
        cb = cb_env.collection
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        with pytest.raises(ValueFormatException):
            await cb.insert('somet-test-bytes', content)

    @pytest.mark.asyncio
    async def test_default_tc_binary_replace(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = "some string content"
        await cb.upsert(key, value)
        new_content = bytes(json.dumps("Here are some newer bytes"), "utf-8")
        with pytest.raises(ValueFormatException):
            await cb.replace(key, new_content)
