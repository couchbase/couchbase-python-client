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

from couchbase.constants import FMT_JSON
from couchbase.exceptions import (DocumentLockedException,
                                  DocumentNotFoundException,
                                  ValueFormatException)
from couchbase.options import (GetAndLockOptions,
                               GetAndTouchOptions,
                               GetOptions,
                               ReplaceOptions)
from couchbase.transcoder import (JSONTranscoder,
                                  LegacyTranscoder,
                                  RawBinaryTranscoder,
                                  RawJSONTranscoder,
                                  RawStringTranscoder,
                                  Transcoder)
from tests.environments import CollectionType
from tests.environments.test_environment import TestEnvironment
from tests.environments.transcoder_environment import FakeTestObj, TranscoderTestEnvironment


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


class DefaultTranscoderTestSuite:
    TEST_MANIFEST = [
        'test_default_tc_binary_insert',
        'test_default_tc_binary_replace',
        'test_default_tc_binary_upsert',
        'test_default_tc_bytearray_upsert',
        'test_default_tc_decoding',
        'test_default_tc_flags_zero',
        'test_default_tc_json_insert',
        'test_default_tc_json_replace',
        'test_default_tc_json_upsert',
        'test_default_tc_string_insert',
        'test_default_tc_string_replace',
        'test_default_tc_string_upsert',
    ]

    def test_default_tc_binary_insert(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('bytes')
        with pytest.raises(ValueFormatException):
            cb_env.collection.insert(key, value)

    def test_default_tc_binary_replace(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('bytes')
        with pytest.raises(ValueFormatException):
            cb_env.collection.replace(key, value)

    def test_default_tc_binary_upsert(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('bytes')
        with pytest.raises(ValueFormatException):
            cb_env.collection.upsert(key, value)

    def test_default_tc_bytearray_upsert(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('bytes')
        with pytest.raises(ValueFormatException):
            cb_env.collection.upsert(key, bytearray(value))

    def test_default_tc_decoding(self):
        tc = JSONTranscoder()
        content = {'foo': 'bar'}
        value, flags = tc.encode_value(content)
        assert flags == FMT_JSON
        decoded = tc.decode_value(value, None)
        assert content == decoded
        decoded = tc.decode_value(value, 0)
        assert content == decoded

    def test_default_tc_flags_zero(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('json')
        cb_env.collection.upsert(key, value, transcoder=ZeroFlagsTranscoder())
        res = cb_env.collection.get(key)
        assert isinstance(res.value, dict)
        assert value == res.content_as[dict]

    def test_default_tc_json_insert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('json')
        cb_env.collection.insert(key, value)

        res = cb_env.collection.get(key)
        result = res.content_as[dict]
        assert result is not None
        assert isinstance(result, dict)
        assert result == value

    def test_default_tc_json_replace(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('json')
        value['new_content'] = 'new content!'
        cb_env.collection.replace(key, value)
        res = cb_env.collection.get(key)
        result = res.content_as[dict]
        assert result is not None
        assert isinstance(result, dict)
        assert result == value

    def test_default_tc_json_upsert(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('json')
        cb_env.collection.upsert(key, value)
        res = cb_env.collection.get(key)
        result = res.content_as[dict]
        assert result is not None
        assert isinstance(result, dict)
        assert result == value

    def test_default_tc_string_insert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('utf8')
        cb_env.collection.insert(key, value)
        res = cb_env.collection.get(key)
        result = res.content_as[str]
        assert result is not None
        assert isinstance(result, str)
        assert result == value

    def test_default_tc_string_replace(self, cb_env):
        key = cb_env.get_existing_doc_by_type('utf8', key_only=True)
        new_content = "new string content"
        cb_env.collection.replace(key, new_content)
        res = cb_env.collection.get(key)
        result = res.content_as[str]
        assert result is not None
        assert isinstance(result, str)
        assert result == new_content

    def test_default_tc_string_upsert(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('utf8')
        cb_env.collection.upsert(key, value)
        res = cb_env.collection.get(key)
        result = res.content_as[str]
        assert result is not None
        assert isinstance(result, str)
        assert result == value


class KeyValueOpTranscoderTestSuite:
    TEST_MANIFEST = [
        'test_get',
        'test_get_and_lock',
        'test_get_and_touch',
        'test_insert',
        'test_replace',
        'test_upsert',
    ]

    def test_get(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('bytes')
        tc = RawBinaryTranscoder()
        with pytest.raises(ValueFormatException):
            cb_env.collection.get(key)
        res = cb_env.collection.get(key, GetOptions(transcoder=tc))
        assert isinstance(res.value, bytes)
        assert res.content_as[bytes] == value

    def test_get_and_touch(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('bytes')
        tc = RawBinaryTranscoder()
        with pytest.raises(ValueFormatException):
            cb_env.collection.get_and_touch(key, timedelta(seconds=30))

        res = cb_env.collection.get_and_touch(key,
                                              timedelta(seconds=3),
                                              GetAndTouchOptions(transcoder=tc))
        assert isinstance(res.value, bytes)
        assert res.content_as[bytes] == value
        TestEnvironment.try_n_times_till_exception(10,
                                                   3,
                                                   cb_env.collection.get,
                                                   key,
                                                   GetOptions(transcoder=tc), DocumentNotFoundException)

    def test_get_and_lock(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('bytes')
        tc = RawBinaryTranscoder()
        with pytest.raises(ValueFormatException):
            cb_env.collection.get_and_lock(key, timedelta(seconds=1))

        # lets get another doc
        key, value = cb_env.get_existing_doc_by_type('bytes')
        res = cb_env.collection.get_and_lock(key,
                                             timedelta(seconds=3),
                                             GetAndLockOptions(transcoder=tc))
        assert isinstance(res.value, bytes)
        assert res.content_as[bytes] == value
        # upsert should definitely fail
        with pytest.raises(DocumentLockedException):
            cb_env.collection.upsert(key, value, transcoder=tc)
        # but succeed eventually
        TestEnvironment.try_n_times(10, 1, cb_env.collection.upsert, key, value, transcoder=tc)

    def test_insert(self, cb_env):
        key = cb_env.get_existing_doc_by_type('utf8', key_only=True)
        # use RawStringTranscoder() so that get() fails as expected
        # since get() w/o passing in transcoder uses the default JSONTranscoder()
        with pytest.raises(ValueFormatException):
            cb_env.collection.get(key)

    def test_replace(self, cb_env):
        key = cb_env.get_existing_doc_by_type('bytes', key_only=True)
        # use RawBinaryTranscoder() so that get() fails as expected
        # since get() w/o passing in transcoder uses the default JSONTranscoder()
        tc = RawBinaryTranscoder()
        new_content = 'some new bytes content'.encode('utf-8')
        cb_env.collection.replace(key, new_content, ReplaceOptions(transcoder=tc))
        with pytest.raises(ValueFormatException):
            cb_env.collection.get(key)

    def test_upsert(self, cb_env):
        key = cb_env.get_existing_doc_by_type('bytes', key_only=True)
        # use RawBinaryTranscoder() so that get() fails as expected
        # since get() w/o passing in transcoder uses the default JSONTranscoder()
        with pytest.raises(ValueFormatException):
            cb_env.collection.get(key)


class LegacyTranscoderTestSuite:
    TEST_MANIFEST = [
        'test_legacy_tc_bytes_insert',
        'test_legacy_tc_bytes_replace',
        'test_legacy_tc_bytes_upsert',
        'test_legacy_tc_decoding',
        'test_legacy_tc_flags_zero',
        'test_legacy_tc_json_insert',
        'test_legacy_tc_json_replace',
        'test_legacy_tc_json_upsert',
        'test_legacy_tc_obj_insert',
        'test_legacy_tc_obj_replace',
        'test_legacy_tc_obj_upsert',
        'test_legacy_tc_string_insert',
        'test_legacy_tc_string_replace',
        'test_legacy_tc_string_upsert',
    ]

    def test_legacy_tc_bytes_insert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('bytes')
        cb_env.collection.insert(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    def test_legacy_tc_bytes_replace(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('bytes')
        cb_env.collection.upsert(key, value)
        new_content = 'new string content'.encode('utf-8')
        cb_env.collection.replace(key, new_content)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert new_content == res.content_as[bytes]

    def test_legacy_tc_bytes_upsert(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('bytes')
        cb_env.collection.upsert(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    def test_legacy_tc_decoding(self):
        tc = LegacyTranscoder()
        content = {'foo': 'bar'}
        value, flags = tc.encode_value(content)
        assert flags == FMT_JSON
        decoded = tc.decode_value(value, None)
        assert content == decoded
        decoded = tc.decode_value(value, 0)
        assert content == decoded

    def test_legacy_tc_flags_zero(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('json')
        cb_env.collection.upsert(key, value, transcoder=ZeroFlagsTranscoder())
        res = cb_env.collection.get(key)
        assert isinstance(res.value, dict)
        assert value == res.content_as[dict]

    def test_legacy_tc_json_insert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('json')
        cb_env.collection.insert(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, dict)
        assert value == res.content_as[dict]

    def test_legacy_tc_json_replace(self, cb_env):
        key = cb_env.get_existing_doc_by_type('bytes', key_only=True)
        _, value = cb_env.get_new_doc_by_type('json')
        cb_env.collection.replace(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, dict)
        assert value == res.content_as[dict]

    def test_legacy_tc_json_upsert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('json')
        cb_env.collection.upsert(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, dict)
        assert value == res.content_as[dict]

    def test_legacy_tc_obj_insert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('obj')
        cb_env.collection.insert(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, FakeTestObj)
        assert value.PROP == res.value.PROP
        assert value.PROP1 == res.value.PROP1

    def test_legacy_tc_obj_replace(self, cb_env):
        key = cb_env.get_existing_doc_by_type('bytes', key_only=True)
        _, value = cb_env.get_new_doc_by_type('obj')
        cb_env.collection.replace(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, FakeTestObj)
        assert value.PROP == res.value.PROP
        assert value.PROP1 == res.value.PROP1

    def test_legacy_tc_obj_upsert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('obj')
        cb_env.collection.upsert(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, FakeTestObj)
        assert value.PROP == res.value.PROP
        assert value.PROP1 == res.value.PROP1

    def test_legacy_tc_string_insert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('utf8')
        cb_env.collection.insert(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, str)
        assert value == res.content_as[str]

    def test_legacy_tc_string_replace(self, cb_env):
        key = cb_env.get_existing_doc_by_type('bytes', key_only=True)
        _, value = cb_env.get_new_doc_by_type('utf8')
        cb_env.collection.replace(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, str)
        assert value == res.content_as[str]

    def test_legacy_tc_string_upsert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('utf8')
        cb_env.collection.upsert(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, str)
        assert value == res.content_as[str]


class RawBinaryTranscoderTestSuite:
    TEST_MANIFEST = [
        'test_raw_binary_tc_bytes_insert',
        'test_raw_binary_tc_bytes_replace',
        'test_raw_binary_tc_bytes_upsert',
        'test_raw_binary_tc_hex_insert',
        'test_raw_binary_tc_hex_replace',
        'test_raw_binary_tc_hex_upsert',
        'test_raw_binary_tc_json_insert',
        'test_raw_binary_tc_json_replace',
        'test_raw_binary_tc_json_upsert',
        'test_raw_binary_tc_string_insert',
        'test_raw_binary_tc_string_replace',
        'test_raw_binary_tc_string_upsert',
    ]

    def test_raw_binary_tc_bytes_insert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('bytes')
        cb_env.collection.insert(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    def test_raw_binary_tc_bytes_replace(self, cb_env):
        key = cb_env.get_existing_doc_by_type('bytes', key_only=True)
        new_content = 'new string content'.encode('utf-8')
        cb_env.collection.replace(key, new_content)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert new_content == res.content_as[bytes]

    def test_raw_binary_tc_bytes_upsert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('bytes')
        cb_env.collection.upsert(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    def test_raw_binary_tc_hex_insert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('hex')
        cb_env.collection.insert(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    def test_raw_binary_tc_hex_replace(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('hex')
        cb_env.collection.upsert(key, value)
        new_content = b'\xFF'
        cb_env.collection.replace(key, new_content)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert new_content == res.content_as[bytes]

    def test_raw_binary_tc_hex_upsert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('hex')
        cb_env.collection.upsert(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    def test_raw_binary_tc_json_insert(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('json')
        with pytest.raises(ValueFormatException):
            cb_env.collection.insert(key, value)

    def test_raw_binary_tc_json_replace(self, cb_env):
        key = cb_env.get_existing_doc_by_type('bytes', key_only=True)
        _, value = cb_env.get_new_doc_by_type('json')
        with pytest.raises(ValueFormatException):
            cb_env.collection.replace(key, value)

    def test_raw_binary_tc_json_upsert(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('json')
        with pytest.raises(ValueFormatException):
            cb_env.collection.upsert(key, value)

    def test_raw_binary_tc_string_upsert(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('utf8')
        with pytest.raises(ValueFormatException):
            cb_env.collection.upsert(key, value)

    def test_raw_binary_tc_string_insert(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('utf8')
        with pytest.raises(ValueFormatException):
            cb_env.collection.insert(key, value)

    def test_raw_binary_tc_string_replace(self, cb_env):
        key = cb_env.get_existing_doc_by_type('bytes', key_only=True)
        _, value = cb_env.get_new_doc_by_type('utf8')
        with pytest.raises(ValueFormatException):
            cb_env.collection.replace(key, value)


class RawJsonTranscoderTestSuite:
    TEST_MANIFEST = [
        'test_pass_through',
        'test_raw_json_tc_bytes_insert',
        'test_raw_json_tc_bytes_replace',
        'test_raw_json_tc_bytes_upsert',
        'test_raw_json_tc_json_insert',
        'test_raw_json_tc_json_replace',
        'test_raw_json_tc_json_upsert',
        'test_raw_json_tc_string_insert',
        'test_raw_json_tc_string_replace',
        'test_raw_json_tc_string_upsert',
    ]

    def test_pass_through(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('json')
        json_str = json.dumps(value)
        cb_env.collection.upsert(key, json_str)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        # should not be equal
        assert res.content_as[bytes] != value
        decoded = json.loads(res.content_as[bytes].decode('utf-8'))
        # should be good now
        assert decoded == value

    def test_raw_json_tc_bytes_insert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('bytes')
        cb_env.collection.insert(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    def test_raw_json_tc_bytes_replace(self, cb_env):
        key = cb_env.get_existing_doc_by_type('json', key_only=True)
        new_content = 'new string content'.encode('utf-8')
        cb_env.collection.replace(key, new_content)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert new_content == res.content_as[bytes]

    def test_raw_json_tc_bytes_upsert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('bytes')
        cb_env.collection.upsert(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes]

    def test_raw_json_tc_json_insert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('json')
        with pytest.raises(ValueFormatException):
            cb_env.collection.insert(key, value)

    def test_raw_json_tc_json_replace(self, cb_env):
        key = cb_env.get_existing_doc_by_type('utf8', key_only=True)
        _, value = cb_env.get_new_doc_by_type('json')
        with pytest.raises(ValueFormatException):
            cb_env.collection.replace(key, value)

    def test_raw_json_tc_json_upsert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('json')
        with pytest.raises(ValueFormatException):
            cb_env.collection.upsert(key, value)

    def test_raw_json_tc_string_insert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('utf8')
        cb_env.collection.insert(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes].decode('utf-8')

    def test_raw_json_tc_string_replace(self, cb_env):
        key = cb_env.get_existing_doc_by_type('utf8', key_only=True)
        new_content = "new string content"
        cb_env.collection.replace(key, new_content)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert new_content == res.content_as[bytes].decode('utf-8')

    def test_raw_json_tc_string_upsert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('utf8')
        cb_env.collection.upsert(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, bytes)
        assert value == res.content_as[bytes].decode('utf-8')


class RawStringTranscoderTestSuite:
    TEST_MANIFEST = [
        'test_raw_string_tc_bytes_insert',
        'test_raw_string_tc_bytes_replace',
        'test_raw_string_tc_bytes_upsert',
        'test_raw_string_tc_json_insert',
        'test_raw_string_tc_json_replace',
        'test_raw_string_tc_json_upsert',
        'test_raw_string_tc_string_insert',
        'test_raw_string_tc_string_replace',
        'test_raw_string_tc_string_upsert',
    ]

    def test_raw_string_tc_bytes_insert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('bytes')
        with pytest.raises(ValueFormatException):
            cb_env.collection.insert(key, value)

    def test_raw_string_tc_bytes_replace(self, cb_env):
        key = cb_env.get_existing_doc_by_type('utf8', key_only=True)
        _, value = cb_env.get_new_doc_by_type('bytes')
        with pytest.raises(ValueFormatException):
            cb_env.collection.replace(key, value)

    def test_raw_string_tc_bytes_upsert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('bytes')
        with pytest.raises(ValueFormatException):
            cb_env.collection.upsert(key, value)

    def test_raw_string_tc_json_insert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('json')
        with pytest.raises(ValueFormatException):
            cb_env.collection.insert(key, value)

    def test_raw_string_tc_json_replace(self, cb_env):
        key = cb_env.get_existing_doc_by_type('utf8', key_only=True)
        _, value = cb_env.get_new_doc_by_type('json')
        with pytest.raises(ValueFormatException):
            cb_env.collection.replace(key, value)

    def test_raw_string_tc_json_upsert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('json')
        with pytest.raises(ValueFormatException):
            cb_env.collection.upsert(key, value)

    def test_raw_string_tc_string_insert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('utf8')
        cb_env.collection.insert(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, str)
        assert value == res.content_as[str]

    def test_raw_string_tc_string_replace(self, cb_env):
        key = cb_env.get_existing_doc_by_type('utf8', key_only=True)
        new_content = "new string content"
        cb_env.collection.replace(key, new_content)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, str)
        assert new_content == res.content_as[str]

    def test_raw_string_tc_string_upsert(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('utf8')
        cb_env.collection.upsert(key, value)
        res = cb_env.collection.get(key)
        assert isinstance(res.value, str)
        assert value == res.content_as[str]


class ClassicDefaultTranscoderTests(DefaultTranscoderTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicDefaultTranscoderTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicDefaultTranscoderTests) if valid_test_method(meth)]
        compare = set(DefaultTranscoderTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = TranscoderTestEnvironment.from_environment(cb_base_env)
        cb_env.setup(request.param)
        yield cb_env
        cb_env.teardown(request.param)


class KeyValueOpTranscoderTests(KeyValueOpTranscoderTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(KeyValueOpTranscoderTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(KeyValueOpTranscoderTests) if valid_test_method(meth)]
        compare = set(KeyValueOpTranscoderTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = TranscoderTestEnvironment.from_environment(cb_base_env)
        cb_env.setup(request.param)
        yield cb_env
        cb_env.teardown(request.param)


class ClassicLegacyTranscoderTests(LegacyTranscoderTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicLegacyTranscoderTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(LegacyTranscoderTestSuite) if valid_test_method(meth)]
        compare = set(ClassicLegacyTranscoderTests.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = TranscoderTestEnvironment.from_environment(cb_base_env)
        cb_env.setup(request.param)
        cb_env.cluster.default_transcoder = LegacyTranscoder()
        yield cb_env
        cb_env.teardown(request.param)
        # reset the transcoder
        cb_env.cluster.default_transcoder = JSONTranscoder()


class ClassicRawBinaryTranscoderTests(RawBinaryTranscoderTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicRawBinaryTranscoderTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(RawBinaryTranscoderTestSuite) if valid_test_method(meth)]
        compare = set(ClassicRawBinaryTranscoderTests.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = TranscoderTestEnvironment.from_environment(cb_base_env)
        cb_env.setup(request.param)
        cb_env.cluster.default_transcoder = RawBinaryTranscoder()
        yield cb_env
        cb_env.teardown(request.param)
        # reset the transcoder
        cb_env.cluster.default_transcoder = JSONTranscoder()


class ClassicRawJsonTranscoderTests(RawJsonTranscoderTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicRawJsonTranscoderTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(RawJsonTranscoderTestSuite) if valid_test_method(meth)]
        compare = set(ClassicRawJsonTranscoderTests.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = TranscoderTestEnvironment.from_environment(cb_base_env)
        cb_env.setup(request.param)
        cb_env.cluster.default_transcoder = RawJSONTranscoder()
        yield cb_env
        cb_env.teardown(request.param)
        # reset the transcoder
        cb_env.cluster.default_transcoder = JSONTranscoder()


class ClassicRawStringTranscoderTests(RawStringTranscoderTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicRawStringTranscoderTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(RawStringTranscoderTestSuite) if valid_test_method(meth)]
        compare = set(ClassicRawStringTranscoderTests.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = TranscoderTestEnvironment.from_environment(cb_base_env)
        cb_env.setup(request.param)
        cb_env.cluster.default_transcoder = RawStringTranscoder()
        yield cb_env
        cb_env.teardown(request.param)
        # reset the transcoder
        cb_env.cluster.default_transcoder = JSONTranscoder()
