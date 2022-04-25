import json

import pytest
import pytest_asyncio

from acouchbase.cluster import Cluster, get_event_loop
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import DocumentNotFoundException, ValueFormatException
from couchbase.options import ClusterOptions

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
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        opts = ClusterOptions(PasswordAuthenticator(username, pw))
        cluster = await Cluster.connect(conn_string, opts)
        bucket = cluster.bucket(f"{couchbase_config.bucket_name}")
        await bucket.on_connect()
        await cluster.cluster_info()

        coll = bucket.default_collection()
        if request.param == CollectionType.DEFAULT:
            cb_env = TestEnvironment(cluster, bucket, coll, couchbase_config, manage_buckets=True)
        elif request.param == CollectionType.NAMED:
            cb_env = TestEnvironment(cluster, bucket, coll, couchbase_config,
                                     manage_buckets=True, manage_collections=True)
            if cb_env.is_mock_server:
                pytest.skip('Jenkins + GoCAVES not playing nice...')
            await cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        await cb_env.try_n_times(5, 3, cb_env.load_data)
        yield cb_env
        await cb_env.try_n_times_till_exception(3, 5,
                                                cb_env.purge_data,
                                                raise_if_no_exception=False)
        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times_till_exception(5, 3,
                                                    cb_env.teardown_named_collections,
                                                    raise_if_no_exception=False)
        await cluster.close()

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
