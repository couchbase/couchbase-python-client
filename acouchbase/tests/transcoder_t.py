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
from typing import Any, Tuple

import pytest
import pytest_asyncio

from acouchbase.cluster import get_event_loop
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
                                  RawStringTranscoder,
                                  Transcoder)

from ._test_utils import (CollectionType,
                          FakeTestObj,
                          KVPair,
                          TestEnvironment)


class ZeroFlagsTranscoder(Transcoder):
    def encode_value(self,
                     value,  # type: Any
                     ) -> Tuple[bytes, int]:
        return json.dumps(value, ensure_ascii=False).encode('utf-8'), 0

    def decode_value(self,
                     value,  # type: bytes
                     flags  # type: int
                     ) -> Any:
        # ignoring flags...only for test purposes
        return json.loads(value.decode('utf-8'))


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

    @pytest.mark.asyncio
    async def test_default_tc_flags_zero(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value

        await cb.upsert(key, value, transcoder=ZeroFlagsTranscoder())
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


class RawJsonTranscoderTests:

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
                                                       transcoder=RawJSONTranscoder())

        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        yield cb_env
        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times_till_exception(5, 3,
                                                    cb_env.teardown_named_collections,
                                                    raise_if_no_exception=False)

    @pytest_asyncio.fixture(name="str_kvp")
    async def str_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_str'
        yield KVPair(key, 'some string content')
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

    @pytest_asyncio.fixture(name="bytes_kvp")
    async def bytes_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_bytes'
        yield KVPair(key, 'some bytes content'.encode('utf-8'),)
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

    @pytest_asyncio.fixture(name="json_kvp")
    async def json_value_with_reset(self, cb_env) -> KVPair:
        key, value = await cb_env.get_new_key_value()
        yield KVPair(key, value)
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.asyncio
    async def test_raw_json_tc_string_upsert(self, cb_env, str_kvp):
        key, value = str_kvp
        await cb_env.collection.upsert(key, value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes].decode('utf-8')

    @pytest.mark.asyncio
    async def test_raw_json_tc_string_insert(self, cb_env, str_kvp):
        key, value = str_kvp
        await cb_env.collection.insert(key, value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes].decode('utf-8')

    @pytest.mark.asyncio
    async def test_raw_json_tc_string_replace(self, cb_env, str_kvp):
        key, value = str_kvp
        await cb_env.collection.upsert(key, value)
        new_content = "new string content"
        await cb_env.collection.replace(key, new_content)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert new_content == res.content_as[bytes].decode('utf-8')

    @pytest.mark.asyncio
    async def test_raw_json_tc_bytes_upsert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        await cb_env.collection.upsert(key, value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    @pytest.mark.asyncio
    async def test_raw_json_tc_bytes_insert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        await cb_env.collection.insert(key, value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    @pytest.mark.asyncio
    async def test_raw_json_tc_bytes_replace(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        await cb_env.collection.upsert(key, value)
        new_content = 'new string content'.encode('utf-8')
        await cb_env.collection.replace(key, new_content)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert new_content == res.content_as[bytes]

    @pytest.mark.asyncio
    async def test_pass_through(self, cb_env, json_kvp):
        key, value = json_kvp
        json_str = json.dumps(value)
        await cb_env.collection.upsert(key, json_str)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert res.content_as[bytes] != value

        decoded = json.loads(res.content_as[bytes].decode('utf-8'))
        assert decoded == value

    @pytest.mark.asyncio
    async def test_raw_json_tc_json_upsert(self, cb_env, json_kvp):
        key, value = json_kvp
        with pytest.raises(ValueFormatException):
            await cb_env.collection.upsert(key, value)

    @pytest.mark.asyncio
    async def test_raw_json_tc_json_insert(self, cb_env, json_kvp):
        key, value = json_kvp
        with pytest.raises(ValueFormatException):
            await cb_env.collection.insert(key, value)

    @pytest.mark.asyncio
    async def test_raw_json_tc_json_replace(self, cb_env, str_kvp, json_kvp):
        key, value = str_kvp
        await cb_env.collection.upsert(key, value)
        with pytest.raises(ValueFormatException):
            await cb_env.collection.replace(key, json_kvp.value)


class RawStringTranscoderTests:

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
                                                       transcoder=RawStringTranscoder())

        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        yield cb_env
        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times_till_exception(5, 3,
                                                    cb_env.teardown_named_collections,
                                                    raise_if_no_exception=False)

    @pytest_asyncio.fixture(name="str_kvp")
    async def str_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_str'
        yield KVPair(key, 'some string content')
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

    @pytest_asyncio.fixture(name="bytes_kvp")
    async def bytes_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_bytes'
        yield KVPair(key, 'some bytes content'.encode('utf-8'),)
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

    @pytest_asyncio.fixture(name="json_kvp")
    async def json_value_with_reset(self, cb_env) -> KVPair:
        key, value = await cb_env.get_new_key_value()
        yield KVPair(key, value)
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.asyncio
    async def test_raw_string_tc_string_upsert(self, cb_env, str_kvp):
        key, value = str_kvp
        await cb_env.collection.upsert(key, value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, str)
        assert value == res.content_as[str]

    @pytest.mark.asyncio
    async def test_raw_string_tc_string_insert(self, cb_env, str_kvp):
        key, value = str_kvp
        await cb_env.collection.insert(key, value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, str)
        assert value == res.content_as[str]

    @pytest.mark.asyncio
    async def test_raw_string_tc_string_replace(self, cb_env, str_kvp):
        key, value = str_kvp
        await cb_env.collection.upsert(key, value)
        new_content = "new string content"
        await cb_env.collection.replace(key, new_content)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, str)
        assert new_content == res.content_as[str]

    @pytest.mark.asyncio
    async def test_raw_string_tc_bytes_upsert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        with pytest.raises(ValueFormatException):
            await cb_env.collection.upsert(key, value)

    @pytest.mark.asyncio
    async def test_raw_string_tc_bytes_insert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        with pytest.raises(ValueFormatException):
            await cb_env.collection.insert(key, value)

    @pytest.mark.asyncio
    async def test_raw_string_tc_bytes_replace(self, cb_env, str_kvp, bytes_kvp):
        key, value = str_kvp
        await cb_env.collection.upsert(key, value)
        with pytest.raises(ValueFormatException):
            await cb_env.collection.replace(key, bytes_kvp.value)

    @pytest.mark.asyncio
    async def test_raw_string_tc_json_upsert(self, cb_env, json_kvp):
        key = json_kvp.key
        value = json_kvp.value
        with pytest.raises(ValueFormatException):
            await cb_env.collection.upsert(key, value)

    @pytest.mark.asyncio
    async def test_raw_string_tc_json_insert(self, cb_env, json_kvp):
        key, value = json_kvp
        with pytest.raises(ValueFormatException):
            await cb_env.collection.insert(key, value)

    @pytest.mark.asyncio
    async def test_raw_string_tc_json_replace(self, cb_env, str_kvp, json_kvp):
        key, value = str_kvp
        await cb_env.collection.upsert(key, value)
        with pytest.raises(ValueFormatException):
            await cb_env.collection.replace(key, json_kvp.value)


class RawBinaryTranscoderTests:

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
                                                       transcoder=RawBinaryTranscoder())

        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        yield cb_env
        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times_till_exception(5, 3,
                                                    cb_env.teardown_named_collections,
                                                    raise_if_no_exception=False)

    @pytest_asyncio.fixture(name="str_kvp")
    async def str_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_str'
        yield KVPair(key, 'some string content')
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

    @pytest_asyncio.fixture(name="bytes_kvp")
    async def bytes_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_bytes'
        yield KVPair(key, 'some bytes content'.encode('utf-8'),)
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

    @pytest_asyncio.fixture(name="hex_kvp")
    async def hex_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_hex_bytes'
        hex_arr = ['ff0102030405060708090a0b0c0d0e0f',
                   '101112131415161718191a1b1c1d1e1f',
                   '202122232425262728292a2b2c2d2e2f',
                   '303132333435363738393a3b3c3d3e3f']
        value = bytes.fromhex(''.join(hex_arr))
        yield KVPair(key, value)
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

    @pytest_asyncio.fixture(name="json_kvp")
    async def json_value_with_reset(self, cb_env) -> KVPair:
        key, value = await cb_env.get_new_key_value()
        yield KVPair(key, value)
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.asyncio
    async def test_raw_binary_tc_bytes_upsert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        await cb_env.collection.upsert(key, value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    @pytest.mark.asyncio
    async def test_raw_binary_tc_bytes_insert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        await cb_env.collection.insert(key, value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    @pytest.mark.asyncio
    async def test_raw_binary_tc_bytes_replace(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        await cb_env.collection.upsert(key, value)
        new_content = 'new string content'.encode('utf-8')
        await cb_env.collection.replace(key, new_content)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert new_content == res.content_as[bytes]

    @pytest.mark.asyncio
    async def test_raw_binary_tc_hex_upsert(self, cb_env, hex_kvp):
        key, value = hex_kvp
        await cb_env.collection.upsert(key, value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    @pytest.mark.asyncio
    async def test_raw_binary_tc_hex_insert(self, cb_env, hex_kvp):
        key, value = hex_kvp
        await cb_env.collection.upsert(key, value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    @pytest.mark.asyncio
    async def test_raw_binary_tc_hex_replace(self, cb_env, hex_kvp):
        key, value = hex_kvp
        await cb_env.collection.upsert(key, value)
        new_content = b'\xFF'
        await cb_env.collection.replace(key, new_content)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert new_content == res.content_as[bytes]

    @pytest.mark.asyncio
    async def test_raw_binary_tc_string_upsert(self, cb_env, str_kvp):
        key, value = str_kvp
        with pytest.raises(ValueFormatException):
            await cb_env.collection.upsert(key, value)

    @pytest.mark.asyncio
    async def test_raw_binary_tc_string_insert(self, cb_env, str_kvp):
        key, value = str_kvp
        with pytest.raises(ValueFormatException):
            await cb_env.collection.insert(key, value)

    @pytest.mark.asyncio
    async def test_raw_binary_tc_string_replace(self, cb_env, bytes_kvp, str_kvp):
        key, value = bytes_kvp
        await cb_env.collection.upsert(key, value)
        with pytest.raises(ValueFormatException):
            await cb_env.collection.replace(key, str_kvp.value)

    @pytest.mark.asyncio
    async def test_raw_binary_tc_json_upsert(self, cb_env, json_kvp):
        key, value = json_kvp
        with pytest.raises(ValueFormatException):
            await cb_env.collection.upsert(key, value)

    @pytest.mark.asyncio
    async def test_raw_binary_tc_json_insert(self, cb_env, json_kvp):
        key, value = json_kvp
        with pytest.raises(ValueFormatException):
            await cb_env.collection.insert(key, value)

    @pytest.mark.asyncio
    async def test_raw_binary_tc_json_replace(self, cb_env, bytes_kvp, json_kvp):
        key, value = bytes_kvp
        await cb_env.collection.upsert(key, value)
        with pytest.raises(ValueFormatException):
            await cb_env.collection.replace(key, json_kvp.value)


class LegacyTranscoderTests:

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
                                                       transcoder=LegacyTranscoder())

        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        yield cb_env
        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times_till_exception(5, 3,
                                                    cb_env.teardown_named_collections,
                                                    raise_if_no_exception=False)

    @pytest_asyncio.fixture(name="str_kvp")
    async def str_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_str'
        yield KVPair(key, 'some string content')
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

    @pytest_asyncio.fixture(name="bytes_kvp")
    async def bytes_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_bytes'
        yield KVPair(key, 'some bytes content'.encode('utf-8'),)
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

    @pytest_asyncio.fixture(name="json_kvp")
    async def json_value_with_reset(self, cb_env) -> KVPair:
        key, value = await cb_env.get_new_key_value()
        yield KVPair(key, value)
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

    @pytest_asyncio.fixture(name="obj_kvp")
    async def obj_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_obj'
        yield KVPair(key, FakeTestObj())
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.asyncio
    async def test_legacy_tc_bytes_upsert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        await cb_env.collection.upsert(key, value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    @pytest.mark.asyncio
    async def test_legacy_tc_bytes_insert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        await cb_env.collection.insert(key, value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    @pytest.mark.asyncio
    async def test_legacy_tc_bytes_replace(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        await cb_env.collection.upsert(key, value)
        new_content = 'new string content'.encode('utf-8')
        await cb_env.collection.replace(key, new_content)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert new_content == res.content_as[bytes]

    @pytest.mark.asyncio
    async def test_legacy_tc_obj_upsert(self, cb_env, obj_kvp):
        key, value = obj_kvp
        await cb_env.collection.upsert(key, value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, FakeTestObj)
        assert value.PROP == res.value.PROP
        assert value.PROP1 == res.value.PROP1

    @pytest.mark.asyncio
    async def test_legacy_tc_obj_insert(self, cb_env, obj_kvp):
        key, value = obj_kvp
        await cb_env.collection.insert(key, value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, FakeTestObj)
        assert value.PROP == res.value.PROP
        assert value.PROP1 == res.value.PROP1

    @pytest.mark.asyncio
    async def test_legacy_tc_obj_replace(self, cb_env, bytes_kvp, obj_kvp):
        key, value = bytes_kvp
        await cb_env.collection.upsert(key, value)
        await cb_env.collection.replace(key, obj_kvp.value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, FakeTestObj)
        assert obj_kvp.value.PROP == res.value.PROP
        assert obj_kvp.value.PROP1 == res.value.PROP1

    @pytest.mark.asyncio
    async def test_legacy_tc_string_upsert(self, cb_env, str_kvp):
        key, value = str_kvp
        await cb_env.collection.upsert(key, value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, str)
        assert value == res.content_as[str]

    @pytest.mark.asyncio
    async def test_legacy_tc_string_insert(self, cb_env, str_kvp):
        key, value = str_kvp
        await cb_env.collection.insert(key, value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, str)
        assert value == res.content_as[str]

    @pytest.mark.asyncio
    async def test_legacy_tc_string_replace(self, cb_env, bytes_kvp, str_kvp):
        key, value = bytes_kvp
        await cb_env.collection.upsert(key, value)
        await cb_env.collection.replace(key, str_kvp.value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, str)
        assert str_kvp.value == res.content_as[str]

    @pytest.mark.asyncio
    async def test_legacy_tc_json_upsert(self, cb_env, json_kvp):
        key, value = json_kvp
        await cb_env.collection.upsert(key, value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, dict)
        assert value == res.content_as[dict]

    @pytest.mark.asyncio
    async def test_legacy_tc_flags_zero(self, cb_env, json_kvp):
        key, value = json_kvp
        await cb_env.collection.upsert(key, value, transcoder=ZeroFlagsTranscoder())
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, dict)
        assert value == res.content_as[dict]

    @pytest.mark.asyncio
    async def test_legacy_tc_json_insert(self, cb_env, json_kvp):
        key, value = json_kvp
        await cb_env.collection.insert(key, value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, dict)
        assert value == res.content_as[dict]

    @pytest.mark.asyncio
    async def test_legacy_tc_json_replace(self, cb_env, bytes_kvp, json_kvp):
        key, value = bytes_kvp
        await cb_env.collection.upsert(key, value)
        await cb_env.collection.replace(key, json_kvp.value)
        res = await cb_env.collection.get(key)
        assert isinstance(res.value, dict)
        assert json_kvp.value == res.content_as[dict]


class KeyValueOpTranscoderTests:

    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest_asyncio.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    async def couchbase_test_environment(self, couchbase_config, request):
        cb_env = await TestEnvironment.get_environment(__name__, couchbase_config, request.param)

        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        yield cb_env
        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times_till_exception(5, 3,
                                                    cb_env.teardown_named_collections,
                                                    raise_if_no_exception=False)

    @pytest_asyncio.fixture(name="str_kvp")
    async def str_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_str'
        yield KVPair(key, 'some string content')
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

    @pytest_asyncio.fixture(name="bytes_kvp")
    async def bytes_value_with_reset(self, cb_env) -> KVPair:
        key = 'key_tc_bytes'
        yield KVPair(key, 'some bytes content'.encode('utf-8'),)
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

    @pytest.mark.asyncio
    async def test_upsert(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        # use RawBinaryTranscoder() so that get() fails as expected
        # since get() w/o passing in transcoder uses the default JSONTranscoder()
        await cb_env.collection.upsert(key, value, UpsertOptions(
            transcoder=RawBinaryTranscoder()))
        with pytest.raises(ValueFormatException):
            await cb_env.collection.get(key)

    @pytest.mark.asyncio
    async def test_insert(self, cb_env, str_kvp):
        key, value = str_kvp
        # use RawStringTranscoder() so that get() fails as expected
        # since get() w/o passing in transcoder uses the default JSONTranscoder()
        await cb_env.collection.upsert(key, value, InsertOptions(transcoder=RawStringTranscoder()))
        with pytest.raises(ValueFormatException):
            await cb_env.collection.get(key)

    @pytest.mark.asyncio
    async def test_replace(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        # use RawBinaryTranscoder() so that get() fails as expected
        # since get() w/o passing in transcoder uses the default JSONTranscoder()
        tc = RawBinaryTranscoder()
        await cb_env.collection.upsert(key, value, UpsertOptions(transcoder=tc))
        new_content = 'some new bytes content'.encode('utf-8')
        await cb_env.collection.replace(key, new_content, ReplaceOptions(transcoder=tc))
        with pytest.raises(ValueFormatException):
            await cb_env.collection.get(key)

    @pytest.mark.asyncio
    async def test_get(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        tc = RawBinaryTranscoder()
        await cb_env.collection.upsert(key, value, UpsertOptions(transcoder=tc))
        with pytest.raises(ValueFormatException):
            await cb_env.collection.get(key)
        res = await cb_env.collection.get(key, GetOptions(transcoder=tc))
        assert isinstance(res.value, bytes)
        assert res.content_as[bytes] == value

    @pytest.mark.asyncio
    async def test_get_and_touch(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        tc = RawBinaryTranscoder()
        await cb_env.collection.upsert(key, value, UpsertOptions(transcoder=tc))
        with pytest.raises(ValueFormatException):
            await cb_env.collection.get_and_touch(key, timedelta(seconds=30))

        res = await cb_env.collection.get_and_touch(key, timedelta(
            seconds=3), GetAndTouchOptions(transcoder=tc))
        assert isinstance(res.value, bytes)
        assert res.content_as[bytes] == value
        await cb_env.try_n_times_till_exception(
            10, 3, cb_env.collection.get, key, GetOptions(transcoder=tc), DocumentNotFoundException)

    @pytest.mark.asyncio
    async def test_get_and_lock(self, cb_env, bytes_kvp):
        key, value = bytes_kvp
        tc = RawBinaryTranscoder()
        await cb_env.collection.upsert(key, value, UpsertOptions(transcoder=tc))
        with pytest.raises(ValueFormatException):
            await cb_env.collection.get_and_lock(key, timedelta(seconds=1))

        await cb_env.try_n_times(10, 1, cb_env.collection.upsert, key,
                                 value, UpsertOptions(transcoder=tc))
        res = await cb_env.collection.get_and_lock(key, timedelta(
            seconds=3), GetAndLockOptions(transcoder=tc))
        assert isinstance(res.value, bytes)
        assert res.content_as[bytes] == value
        # upsert should definitely fail
        with pytest.raises(DocumentLockedException):
            await cb_env.collection.upsert(key, value, transcoder=tc)
        # but succeed eventually
        await cb_env.try_n_times(10, 1, cb_env.collection.upsert, key, value, transcoder=tc)
