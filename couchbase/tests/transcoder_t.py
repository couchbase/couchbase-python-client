import json

import pytest

from couchbase.exceptions import DocumentNotFoundException, ValueFormatException

from ._test_utils import (CollectionType,
                          KVPair,
                          TestEnvironment)


class DefaultTranscoderTests:

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, couchbase_config, request):
        cb_env = TestEnvironment.get_environment(couchbase_config, request.param)

        if request.param == CollectionType.NAMED:
            cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        yield cb_env
        if request.param == CollectionType.NAMED:
            cb_env.try_n_times_till_exception(5, 3,
                                              cb_env.teardown_named_collections,
                                              raise_if_no_exception=False)
        cb_env.cluster.close()

    @pytest.fixture(name="new_kvp")
    def new_key_and_value_with_reset(self, cb_env) -> KVPair:
        cb_env.check_if_mock_unstable()
        key, value = cb_env.get_new_key_value()
        yield KVPair(key, value)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.mark.flaky(reruns=5)
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
        cb_env.check_if_mock_unstable()
        cb = cb_env.collection
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        with pytest.raises(ValueFormatException):
            cb.upsert('some-test-bytes', content)

    def test_default_tc_bytearray_upsert(self, cb_env):
        cb_env.check_if_mock_unstable()
        cb = cb_env.collection
        content = bytearray(json.dumps("Here are some bytes"), "utf-8")
        with pytest.raises(ValueFormatException):
            cb.upsert('some-test-bytes', content)

    def test_default_tc_binary_insert(self, cb_env):
        cb_env.check_if_mock_unstable()
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
