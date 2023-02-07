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

from couchbase.exceptions import DocumentNotFoundException, InvalidArgumentException
from couchbase.options import (DecrementOptions,
                               DeltaValue,
                               IncrementOptions,
                               SignedInt64)
from couchbase.result import CounterResult, MutationResult
from couchbase.transcoder import RawBinaryTranscoder, RawStringTranscoder
from tests.environments import CollectionType
from tests.environments.binary_environment import BinaryTestEnvironment
from tests.environments.test_environment import TestEnvironment


class BinaryCollectionTestSuite:

    TEST_MANIFEST = [
        'test_append_bytes',
        'test_append_bytes_not_empty',
        'test_append_string',
        'test_append_string_nokey',
        'test_append_string_not_empty',
        'test_counter_bad_delta_value',
        'test_counter_bad_initial_value',
        'test_counter_decrement',
        'test_counter_decrement_initial_value',
        'test_counter_decrement_non_default',
        'test_counter_increment',
        'test_counter_increment_initial_value',
        'test_counter_increment_non_default',
        'test_prepend_bytes',
        'test_prepend_bytes_not_empty',
        'test_prepend_string',
        'test_prepend_string_nokey',
        'test_prepend_string_not_empty',
        'test_signed_int_64',
        'test_unsigned_int',
    ]

    def test_append_bytes(self, cb_env):
        key = cb_env.get_existing_doc_by_type('bytes_empty', key_only=True)
        result = cb_env.collection.binary().append(key, b'XXX')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        # make sure it really worked
        result = cb_env.collection.get(key, transcoder=RawBinaryTranscoder())
        assert result.content_as[bytes] == b'XXX'

    def test_append_bytes_not_empty(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('bytes')

        result = cb_env.collection.binary().append(key, 'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        result = cb_env.collection.get(key, transcoder=RawBinaryTranscoder())
        assert result.content_as[bytes] == value + b'foo'

    def test_append_string(self, cb_env):
        key = cb_env.get_existing_doc_by_type('utf8_empty', key_only=True)
        result = cb_env.collection.binary().append(key, 'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        # make sure it really worked
        result = cb_env.collection.get(key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == 'foo'

    def test_append_string_not_empty(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('utf8')
        result = cb_env.collection.binary().append(key, 'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        result = cb_env.collection.get(key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == value + 'foo'

    def test_append_string_nokey(self, cb_env):
        # @TODO(jc):  3.2.x SDK tests for NotStoredException
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.binary().append(TestEnvironment.NOT_A_KEY, 'foo')

    def test_counter_bad_delta_value(self, cb_env):
        key = cb_env.get_existing_doc_by_type('counter_empty')

        with pytest.raises(InvalidArgumentException):
            cb_env.collection.binary().increment(key, delta=5)

        with pytest.raises(InvalidArgumentException):
            cb_env.collection.binary().decrement(key, delta=5)

    def test_counter_bad_initial_value(self, cb_env):
        key = cb_env.get_existing_doc_by_type('counter_empty')

        with pytest.raises(InvalidArgumentException):
            cb_env.collection.binary().increment(key, initial=100)

        with pytest.raises(InvalidArgumentException):
            cb_env.collection.binary().decrement(key, initial=100)

    def test_counter_decrement(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('counter')
        result = cb_env.collection.binary().decrement(key)
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == value - 1

    def test_counter_decrement_initial_value(self, cb_env):
        key = cb_env.get_existing_doc_by_type('counter_empty')
        result = cb_env.collection.binary().decrement(key, DecrementOptions(initial=SignedInt64(100)))
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == 100

    def test_counter_decrement_non_default(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('counter')
        result = cb_env.collection.binary().decrement(key, DecrementOptions(delta=DeltaValue(3)))
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == value - 3

    def test_counter_increment(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('counter')
        result = cb_env.collection.binary().increment(key)
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == value + 1

    def test_counter_increment_initial_value(self, cb_env):
        key = cb_env.get_existing_doc_by_type('counter_empty')
        result = cb_env.collection.binary().increment(key, IncrementOptions(initial=SignedInt64(100)))
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == 100

    def test_counter_increment_non_default(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('counter')
        result = cb_env.collection.binary().increment(key, IncrementOptions(delta=DeltaValue(3)))
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == value + 3

    def test_prepend_bytes(self, cb_env):
        key = cb_env.get_existing_doc_by_type('bytes_empty', key_only=True)
        result = cb_env.collection.binary().prepend(key, b'XXX')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        # make sure it really worked
        result = cb_env.collection.get(key, transcoder=RawBinaryTranscoder())
        assert result.content_as[bytes] == b'XXX'

    def test_prepend_bytes_not_empty(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('bytes')
        result = cb_env.collection.binary().prepend(key, b'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        result = cb_env.collection.get(key, transcoder=RawBinaryTranscoder())
        assert result.content_as[bytes] == b'foo' + value

    def test_prepend_string(self, cb_env):
        key = cb_env.get_existing_doc_by_type('utf8_empty', key_only=True)
        result = cb_env.collection.binary().prepend(key, 'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        # make sure it really worked
        result = cb_env.collection.get(key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == 'foo'

    def test_prepend_string_nokey(self, cb_env):
        # @TODO(jc):  3.2.x SDK tests for NotStoredException
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.binary().prepend(TestEnvironment.NOT_A_KEY, 'foo')

    def test_prepend_string_not_empty(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('utf8')
        result = cb_env.collection.binary().prepend(key, 'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        result = cb_env.collection.get(key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == 'foo' + value

    def test_unsigned_int(self):
        with pytest.raises(InvalidArgumentException):
            x = DeltaValue(-1)
        with pytest.raises(InvalidArgumentException):
            x = DeltaValue(0x7FFFFFFFFFFFFFFF + 1)

        x = DeltaValue(5)
        assert 5 == x.value

    def test_signed_int_64(self):
        with pytest.raises(InvalidArgumentException):
            x = SignedInt64(-0x7FFFFFFFFFFFFFFF - 2)

        with pytest.raises(InvalidArgumentException):
            x = SignedInt64(0x7FFFFFFFFFFFFFFF + 1)

        x = SignedInt64(0x7FFFFFFFFFFFFFFF)
        assert 0x7FFFFFFFFFFFFFFF == x.value
        x = SignedInt64(-0x7FFFFFFFFFFFFFFF - 1)
        assert -0x7FFFFFFFFFFFFFFF - 1 == x.value


class ClassicBinaryCollectionTests(BinaryCollectionTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicBinaryCollectionTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicBinaryCollectionTests) if valid_test_method(meth)]
        compare = set(BinaryCollectionTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = BinaryTestEnvironment.from_environment(cb_base_env)
        cb_env.enable_bucket_mgmt()
        cb_env.setup(request.param, __name__)

        yield cb_env

        cb_env.teardown(request.param)
