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
from datetime import timedelta

import pytest

from couchbase.exceptions import (DocumentLockedException,
                                  DocumentNotFoundException,
                                  ValueFormatException)
from couchbase.options import (GetAndLockOptions,
                               GetAndTouchOptions,
                               GetOptions,
                               InsertOptions,
                               ReplaceOptions,
                               UpsertOptions)
from couchbase.transcoder import (LegacyTranscoder,
                                  RawBinaryTranscoder,
                                  RawJSONTranscoder,
                                  RawStringTranscoder)

from ._test_utils import (CollectionType,
                          FakeTestObj,
                          KVPair,
                          TestEnvironment,
                          run_in_reactor_thread)


class DefaultTranscoderTests:

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, couchbase_config, request):
        cb_env = TestEnvironment.get_environment(__name__, couchbase_config, request.param)

        if request.param == CollectionType.NAMED:
            cb_env.try_n_times(5, 3, cb_env.setup_named_collections, is_deferred=False)

        yield cb_env
        if request.param == CollectionType.NAMED:
            cb_env.try_n_times_till_exception(5, 3,
                                              cb_env.teardown_named_collections,
                                              raise_if_no_exception=False,
                                              is_deferred=False)

    @pytest.fixture(name="new_kvp")
    def new_key_and_value_with_reset(self, cb_env) -> KVPair:
        key, value = cb_env.get_new_key_value()
        yield KVPair(key, value)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.fixture(name="str_kvp")
    def str_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_str'
        yield KVPair(key, 'some string content')
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.fixture(name="bytes_kvp")
    def bytes_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_bytes'
        yield KVPair(key, 'some bytes content'.encode('utf-8'),)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.fixture(name="json_kvp")
    def json_value_with_reset(self, cb_env) -> KVPair:
        key, value = cb_env.get_new_key_value()
        yield KVPair(key, value)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_default_tc_json_upsert(self, cb_env, json_kvp):
        key, value = json_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        result = res.content_as[dict]
        assert result is not None
        assert isinstance(result, dict)
        assert result == value

    def test_default_tc_json_insert(self, cb_env, json_kvp):
        key, value = json_kvp
        run_in_reactor_thread(cb_env.collection.insert, key, value)

        res = run_in_reactor_thread(cb_env.collection.get, key)
        result = res.content_as[dict]
        assert result is not None
        assert isinstance(result, dict)
        assert result == value

    def test_default_tc_json_replace(self, cb_env, json_kvp):
        key, value = json_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        value['new_content'] = 'new content!'
        run_in_reactor_thread(cb_env.collection.replace, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        result = res.content_as[dict]
        assert result is not None
        assert isinstance(result, dict)
        assert result == value

    # default TC: no transcoder set in ClusterOptions or KV options

    def test_default_tc_string_upsert(self, cb_env, str_kvp):
        key, value = str_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        result = res.content_as[str]
        assert result is not None
        assert isinstance(result, str)
        assert result == value

    def test_default_tc_string_insert(self, cb_env, str_kvp):
        key, value = str_kvp
        run_in_reactor_thread(cb_env.collection.insert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        result = res.content_as[str]
        assert result is not None
        assert isinstance(result, str)
        assert result == value

    def test_default_tc_string_replace(self, cb_env, str_kvp):
        key, value = str_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        new_content = "new string content"
        run_in_reactor_thread(cb_env.collection.replace, key, new_content)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        result = res.content_as[str]
        assert result is not None
        assert isinstance(result, str)
        assert result == new_content

    def test_default_tc_binary_upsert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.upsert, key, value)

    def test_default_tc_bytearray_upsert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.upsert, key, bytearray(value))

    def test_default_tc_binary_insert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.insert, key, value)

    def test_default_tc_binary_replace(self, cb_env, str_kvp, bytes_kvp):
        key, value = str_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.replace, key, bytes_kvp.value)


class RawJsonTranscoderTests:

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, couchbase_config, request):
        cb_env = TestEnvironment.get_environment(
            __name__, couchbase_config, request.param, transcoder=RawJSONTranscoder())

        if request.param == CollectionType.NAMED:
            cb_env.try_n_times(5, 3, cb_env.setup_named_collections, is_deferred=False)

        yield cb_env
        if request.param == CollectionType.NAMED:
            cb_env.try_n_times_till_exception(5, 3,
                                              cb_env.teardown_named_collections,
                                              raise_if_no_exception=False,
                                              is_deferred=False)

    @pytest.fixture(name="str_kvp")
    def str_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_str'
        yield KVPair(key, 'some string content')
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.fixture(name="bytes_kvp")
    def bytes_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_bytes'
        yield KVPair(key, 'some bytes content'.encode('utf-8'),)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.fixture(name="json_kvp")
    def json_value_with_reset(self, cb_env) -> KVPair:
        key, value = cb_env.get_new_key_value()
        yield KVPair(key, value)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_raw_json_tc_string_upsert(self, cb_env, str_kvp):
        key, value = str_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes].decode('utf-8')

    def test_raw_json_tc_string_insert(self, cb_env, str_kvp):
        key, value = str_kvp
        run_in_reactor_thread(cb_env.collection.insert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes].decode('utf-8')

    def test_raw_json_tc_string_replace(self, cb_env, str_kvp):
        key, value = str_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        new_content = "new string content"
        run_in_reactor_thread(cb_env.collection.replace, key, new_content)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, bytes)
        assert new_content == res.content_as[bytes].decode('utf-8')

    def test_raw_json_tc_bytes_upsert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    def test_raw_json_tc_bytes_insert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        run_in_reactor_thread(cb_env.collection.insert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    def test_raw_json_tc_bytes_replace(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        new_content = 'new string content'.encode('utf-8')
        run_in_reactor_thread(cb_env.collection.replace, key, new_content)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, bytes)
        assert new_content == res.content_as[bytes]

    def test_pass_through(self, cb_env, json_kvp):
        key, value = json_kvp
        json_str = json.dumps(value)
        run_in_reactor_thread(cb_env.collection.upsert, key, json_str)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, bytes)
        assert res.content_as[bytes] != value

        decoded = json.loads(res.content_as[bytes].decode('utf-8'))
        assert decoded == value

    def test_raw_json_tc_json_upsert(self, cb_env, json_kvp):
        key, value = json_kvp
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.upsert, key, value)

    def test_raw_json_tc_json_insert(self, cb_env, json_kvp):
        key, value = json_kvp
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.insert, key, value)

    def test_raw_json_tc_json_replace(self, cb_env, str_kvp, json_kvp):
        key, value = str_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.replace, key, json_kvp.value)


class RawStringTranscoderTests:

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, couchbase_config, request):
        cb_env = TestEnvironment.get_environment(
            __name__, couchbase_config, request.param, transcoder=RawStringTranscoder())

        if request.param == CollectionType.NAMED:
            cb_env.try_n_times(5, 3, cb_env.setup_named_collections, is_deferred=False)

        yield cb_env
        if request.param == CollectionType.NAMED:
            cb_env.try_n_times_till_exception(5, 3,
                                              cb_env.teardown_named_collections,
                                              raise_if_no_exception=False,
                                              is_deferred=False)

    @pytest.fixture(name="str_kvp")
    def str_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_str'
        yield KVPair(key, 'some string content')
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.fixture(name="bytes_kvp")
    def bytes_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_bytes'
        yield KVPair(key, 'some bytes content'.encode('utf-8'),)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.fixture(name="json_kvp")
    def json_value_with_reset(self, cb_env) -> KVPair:
        key, value = cb_env.get_new_key_value()
        yield KVPair(key, value)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_raw_string_tc_string_upsert(self, cb_env, str_kvp):
        key, value = str_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, str)
        assert value == res.content_as[str]

    def test_raw_string_tc_string_insert(self, cb_env, str_kvp):
        key, value = str_kvp
        run_in_reactor_thread(cb_env.collection.insert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, str)
        assert value == res.content_as[str]

    def test_raw_string_tc_string_replace(self, cb_env, str_kvp):
        key, value = str_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        new_content = "new string content"
        run_in_reactor_thread(cb_env.collection.replace, key, new_content)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, str)
        assert new_content == res.content_as[str]

    def test_raw_string_tc_bytes_upsert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.upsert, key, value)

    def test_raw_string_tc_bytes_insert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.insert, key, value)

    def test_raw_string_tc_bytes_replace(self, cb_env, str_kvp, bytes_kvp):
        key, value = str_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.replace, key, bytes_kvp.value)

    def test_raw_string_tc_json_upsert(self, cb_env, json_kvp):
        key = json_kvp.key
        value = json_kvp.value
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.upsert, key, value)

    def test_raw_string_tc_json_insert(self, cb_env, json_kvp):
        key, value = json_kvp
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.insert, key, value)

    def test_raw_string_tc_json_replace(self, cb_env, str_kvp, json_kvp):
        key, value = str_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.replace, key, json_kvp.value)


class RawBinaryTranscoderTests:

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, couchbase_config, request):
        cb_env = TestEnvironment.get_environment(
            __name__, couchbase_config, request.param, transcoder=RawBinaryTranscoder())

        if request.param == CollectionType.NAMED:
            cb_env.try_n_times(5, 3, cb_env.setup_named_collections, is_deferred=False)

        yield cb_env
        if request.param == CollectionType.NAMED:
            cb_env.try_n_times_till_exception(5, 3,
                                              cb_env.teardown_named_collections,
                                              raise_if_no_exception=False,
                                              is_deferred=False)

    @pytest.fixture(name="str_kvp")
    def str_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_str'
        yield KVPair(key, 'some string content')
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.fixture(name="bytes_kvp")
    def bytes_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_bytes'
        yield KVPair(key, 'some bytes content'.encode('utf-8'),)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.fixture(name="hex_kvp")
    def hex_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_hex_bytes'
        hex_arr = ['ff0102030405060708090a0b0c0d0e0f',
                   '101112131415161718191a1b1c1d1e1f',
                   '202122232425262728292a2b2c2d2e2f',
                   '303132333435363738393a3b3c3d3e3f']
        value = bytes.fromhex(''.join(hex_arr))
        yield KVPair(key, value)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.fixture(name="json_kvp")
    def json_value_with_reset(self, cb_env) -> KVPair:
        key, value = cb_env.get_new_key_value()
        yield KVPair(key, value)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_raw_binary_tc_bytes_upsert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    def test_raw_binary_tc_bytes_insert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        run_in_reactor_thread(cb_env.collection.insert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    def test_raw_binary_tc_bytes_replace(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        new_content = 'new string content'.encode('utf-8')
        run_in_reactor_thread(cb_env.collection.replace, key, new_content)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, bytes)
        assert new_content == res.content_as[bytes]

    def test_raw_binary_tc_hex_upsert(self, cb_env, hex_kvp):
        key, value = hex_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    def test_raw_binary_tc_hex_insert(self, cb_env, hex_kvp):
        key, value = hex_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    def test_raw_binary_tc_hex_replace(self, cb_env, hex_kvp):
        key, value = hex_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        new_content = b'\xFF'
        run_in_reactor_thread(cb_env.collection.replace, key, new_content)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, bytes)
        assert new_content == res.content_as[bytes]

    def test_raw_binary_tc_string_upsert(self, cb_env, str_kvp):
        key, value = str_kvp
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.upsert, key, value)

    def test_raw_binary_tc_string_insert(self, cb_env, str_kvp):
        key, value = str_kvp
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.insert, key, value)

    def test_raw_binary_tc_string_replace(self, cb_env, bytes_kvp, str_kvp):
        key, value = bytes_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.replace, key, str_kvp.value)

    def test_raw_binary_tc_json_upsert(self, cb_env, json_kvp):
        key, value = json_kvp
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.upsert, key, value)

    def test_raw_binary_tc_json_insert(self, cb_env, json_kvp):
        key, value = json_kvp
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.insert, key, value)

    def test_raw_binary_tc_json_replace(self, cb_env, bytes_kvp, json_kvp):
        key, value = bytes_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.replace, key, json_kvp.value)


class LegacyTranscoderTests:

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, couchbase_config, request):
        cb_env = TestEnvironment.get_environment(
            __name__, couchbase_config, request.param, transcoder=LegacyTranscoder())

        if request.param == CollectionType.NAMED:
            cb_env.try_n_times(5, 3, cb_env.setup_named_collections, is_deferred=False)

        yield cb_env
        if request.param == CollectionType.NAMED:
            cb_env.try_n_times_till_exception(5, 3,
                                              cb_env.teardown_named_collections,
                                              raise_if_no_exception=False,
                                              is_deferred=False)

    @pytest.fixture(name="str_kvp")
    def str_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_str'
        yield KVPair(key, 'some string content')
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.fixture(name="bytes_kvp")
    def bytes_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_bytes'
        yield KVPair(key, 'some bytes content'.encode('utf-8'),)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.fixture(name="json_kvp")
    def json_value_with_reset(self, cb_env) -> KVPair:
        key, value = cb_env.get_new_key_value()
        yield KVPair(key, value)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.fixture(name="obj_kvp")
    def obj_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_obj'
        yield KVPair(key, FakeTestObj())
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_legacy_tc_bytes_upsert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    def test_legacy_tc_bytes_insert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        run_in_reactor_thread(cb_env.collection.insert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    def test_legacy_tc_bytes_replace(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        new_content = 'new string content'.encode('utf-8')
        run_in_reactor_thread(cb_env.collection.replace, key, new_content)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, bytes)
        assert new_content == res.content_as[bytes]

    def test_legacy_tc_obj_upsert(self, cb_env, obj_kvp):
        key, value = obj_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, FakeTestObj)
        assert value.PROP == res.value.PROP
        assert value.PROP1 == res.value.PROP1

    def test_legacy_tc_obj_insert(self, cb_env, obj_kvp):
        key, value = obj_kvp
        run_in_reactor_thread(cb_env.collection.insert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, FakeTestObj)
        assert value.PROP == res.value.PROP
        assert value.PROP1 == res.value.PROP1

    def test_legacy_tc_obj_replace(self, cb_env, bytes_kvp, obj_kvp):
        key, value = bytes_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        run_in_reactor_thread(cb_env.collection.replace, key, obj_kvp.value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, FakeTestObj)
        assert obj_kvp.value.PROP == res.value.PROP
        assert obj_kvp.value.PROP1 == res.value.PROP1

    def test_legacy_tc_string_upsert(self, cb_env, str_kvp):
        key, value = str_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, str)
        assert value == res.content_as[str]

    def test_legacy_tc_string_insert(self, cb_env, str_kvp):
        key, value = str_kvp
        run_in_reactor_thread(cb_env.collection.insert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, str)
        assert value == res.content_as[str]

    def test_legacy_tc_string_replace(self, cb_env, bytes_kvp, str_kvp):
        key, value = bytes_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        run_in_reactor_thread(cb_env.collection.replace, key, str_kvp.value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, str)
        assert str_kvp.value == res.content_as[str]

    def test_legacy_tc_json_upsert(self, cb_env, json_kvp):
        key, value = json_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, dict)
        assert value == res.content_as[dict]

    def test_legacy_tc_json_insert(self, cb_env, json_kvp):
        key, value = json_kvp
        run_in_reactor_thread(cb_env.collection.insert, key, value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, dict)
        assert value == res.content_as[dict]

    def test_legacy_tc_json_replace(self, cb_env, bytes_kvp, json_kvp):
        key, value = bytes_kvp
        run_in_reactor_thread(cb_env.collection.upsert, key, value)
        run_in_reactor_thread(cb_env.collection.replace, key, json_kvp.value)
        res = run_in_reactor_thread(cb_env.collection.get, key)
        assert isinstance(res.value, dict)
        assert json_kvp.value == res.content_as[dict]


class KeyValueOpTranscoderTests:

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, couchbase_config, request):
        cb_env = TestEnvironment.get_environment(__name__, couchbase_config, request.param)

        if request.param == CollectionType.NAMED:
            cb_env.try_n_times(5, 3, cb_env.setup_named_collections, is_deferred=False)

        yield cb_env
        if request.param == CollectionType.NAMED:
            cb_env.try_n_times_till_exception(5, 3,
                                              cb_env.teardown_named_collections,
                                              raise_if_no_exception=False,
                                              is_deferred=False)

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.fixture(name="str_kvp")
    def str_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_str'
        yield KVPair(key, 'some string content')
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.fixture(name="bytes_kvp")
    def bytes_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_bytes'
        yield KVPair(key, 'some bytes content'.encode('utf-8'),)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    def test_upsert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        # use RawBinaryTranscoder() so that get() fails as expected
        # since get() w/o passing in transcoder uses the default JSONTranscoder()
        run_in_reactor_thread(cb_env.collection.upsert,
                              key,
                              value,
                              UpsertOptions(transcoder=RawBinaryTranscoder()))
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.get, key)

    def test_insert(self, cb_env, str_kvp):
        key, value = str_kvp
        # use RawStringTranscoder() so that get() fails as expected
        # since get() w/o passing in transcoder uses the default JSONTranscoder()
        run_in_reactor_thread(cb_env.collection.upsert, key, value, InsertOptions(transcoder=RawStringTranscoder()))
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.get, key)

    def test_replace(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        # use RawBinaryTranscoder() so that get() fails as expected
        # since get() w/o passing in transcoder uses the default JSONTranscoder()
        tc = RawBinaryTranscoder()
        run_in_reactor_thread(cb_env.collection.upsert, key, value, UpsertOptions(transcoder=tc))
        new_content = 'some new bytes content'.encode('utf-8')
        run_in_reactor_thread(cb_env.collection.replace, key, new_content, ReplaceOptions(transcoder=tc))
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.get, key)

    def test_get(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        tc = RawBinaryTranscoder()
        run_in_reactor_thread(cb_env.collection.upsert, key, value, UpsertOptions(transcoder=tc))
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.get, key)
        res = run_in_reactor_thread(cb_env.collection.get, key, GetOptions(transcoder=tc))
        assert isinstance(res.value, bytes)
        assert res.content_as[bytes] == value

    def test_get_and_touch(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        tc = RawBinaryTranscoder()
        run_in_reactor_thread(cb_env.collection.upsert, key, value, UpsertOptions(transcoder=tc))
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.get_and_touch, key, timedelta(seconds=30))

        res = run_in_reactor_thread(cb_env.collection.get_and_touch,
                                    key,
                                    timedelta(seconds=3),
                                    GetAndTouchOptions(transcoder=tc))
        assert isinstance(res.value, bytes)
        assert res.content_as[bytes] == value
        cb_env.try_n_times_till_exception(
            10, 3, cb_env.collection.get, key, GetOptions(transcoder=tc), DocumentNotFoundException)

    def test_get_and_lock(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        tc = RawBinaryTranscoder()
        run_in_reactor_thread(cb_env.collection.upsert, key, value, UpsertOptions(transcoder=tc))
        with pytest.raises(ValueFormatException):
            run_in_reactor_thread(cb_env.collection.get_and_lock, key, timedelta(seconds=1))

        cb_env.try_n_times(10, 1, cb_env.collection.upsert, key,
                           value, UpsertOptions(transcoder=tc))
        res = run_in_reactor_thread(cb_env.collection.get_and_lock,
                                    key,
                                    timedelta(seconds=3),
                                    GetAndLockOptions(transcoder=tc))
        assert isinstance(res.value, bytes)
        assert res.content_as[bytes] == value
        # upsert should definitely fail
        with pytest.raises(DocumentLockedException):
            run_in_reactor_thread(cb_env.collection.upsert, key, value, transcoder=tc)
        # but succeed eventually
        cb_env.try_n_times(10, 1, cb_env.collection.upsert, key, value, transcoder=tc)
