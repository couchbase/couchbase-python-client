import json

import pytest

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import DocumentNotFoundException, ValueFormatException
from couchbase.options import ClusterOptions

from ._test_utils import (CollectionType,
                          KVPair,
                          TestEnvironment)


class DefaultTranscoderTests:

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, couchbase_config, request):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        opts = ClusterOptions(PasswordAuthenticator(username, pw))
        cluster = Cluster(
            conn_string, opts)
        cluster.cluster_info()
        bucket = cluster.bucket(f"{couchbase_config.bucket_name}")

        coll = bucket.default_collection()
        if request.param == CollectionType.DEFAULT:
            cb_env = TestEnvironment(cluster, bucket, coll, couchbase_config, manage_buckets=True)
        elif request.param == CollectionType.NAMED:
            cb_env = TestEnvironment(cluster, bucket, coll, couchbase_config,
                                     manage_buckets=True, manage_collections=True)
            cb_env.setup_named_collections()

        cb_env.load_data()
        yield cb_env
        cb_env.purge_data()
        if request.param == CollectionType.NAMED:
            cb_env.teardown_named_collections()
        cluster.close()

    @pytest.fixture(name="new_kvp")
    def new_key_and_value_with_reset(self, cb_env) -> KVPair:
        key, value = cb_env.get_new_key_value()
        yield KVPair(key, value)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,))

    def test_default_tc_json_upsert(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value

        cb.upsert(key, value)
        res = cb.get(key)
        result = res.content_as[dict]
        assert result is not None
        assert isinstance(result, dict)
        assert result == value

    def test_default_tc_json_insert(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        cb.insert(key, value)

        res = cb.get(key)
        res = cb.get(key)
        result = res.content_as[dict]
        assert result is not None
        assert isinstance(result, dict)
        assert result == value

    def test_default_tc_json_replace(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        cb.upsert(key, value)
        value['new_content'] = 'new content!'
        cb.replace(key, value)
        res = cb.get(key)
        result = res.content_as[dict]
        assert result is not None
        assert isinstance(result, dict)
        assert result == value

    # default TC: no transcoder set in ClusterOptions or KV options

    def test_default_tc_string_upsert(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = "some string content"
        cb.upsert(key, value)
        res = cb.get(key)
        result = res.content_as[str]
        assert result is not None
        assert isinstance(result, str)
        assert result == value

    def test_default_tc_string_insert(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = "some string content"
        cb.insert(key, value)
        res = cb.get(key)
        result = res.content_as[str]
        assert result is not None
        assert isinstance(result, str)
        assert result == value

    def test_default_tc_string_replace(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = "some string content"
        cb.upsert(key, value)
        new_content = "new string content"
        cb.replace(key, new_content)
        res = cb.get(key)
        result = res.content_as[str]
        assert result is not None
        assert isinstance(result, str)
        assert result == new_content

    def test_default_tc_binary_upsert(self, cb_env):
        cb = cb_env.collection
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        with pytest.raises(ValueFormatException):
            cb.upsert('some-test-bytes', content)

    def test_default_tc_bytearray_upsert(self, cb_env):
        cb = cb_env.collection
        content = bytearray(json.dumps("Here are some bytes"), "utf-8")
        with pytest.raises(ValueFormatException):
            cb.upsert('some-test-bytes', content)

    def test_default_tc_binary_insert(self, cb_env):
        cb = cb_env.collection
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        with pytest.raises(ValueFormatException):
            cb.insert('somet-test-bytes', content)

    def test_default_tc_binary_replace(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = "some string content"
        cb.upsert(key, value)
        new_content = bytes(json.dumps("Here are some newer bytes"), "utf-8")
        with pytest.raises(ValueFormatException):
            cb.replace(key, new_content)
