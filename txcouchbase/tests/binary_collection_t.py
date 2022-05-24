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

from couchbase.exceptions import DocumentNotFoundException, InvalidArgumentException
from couchbase.options import (DecrementOptions,
                               DeltaValue,
                               IncrementOptions,
                               SignedInt64)
from couchbase.result import CounterResult, MutationResult
from couchbase.transcoder import RawBinaryTranscoder, RawStringTranscoder

from ._test_utils import (CollectionType,
                          KVPair,
                          TestEnvironment,
                          run_in_reactor_thread)


class BinaryCollectionTests:

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

    # key/value fixtures

    @pytest.fixture(name='utf8_empty_kvp')
    def utf8_key_and_empty_value(self, cb_env) -> KVPair:
        key, value = cb_env.try_n_times(5, 3, cb_env.load_utf8_binary_data, is_deferred=False)
        yield KVPair(key, value)
        run_in_reactor_thread(cb_env.collection.upsert, key, '', transcoder=RawStringTranscoder())

    @pytest.fixture(name='utf8_kvp')
    def utf8_key_and_value(self, cb_env) -> KVPair:
        key, value = cb_env.try_n_times(5, 3, cb_env.load_utf8_binary_data, start_value='XXXX', is_deferred=False)
        yield KVPair(key, value)
        run_in_reactor_thread(cb_env.collection.upsert, key, '', transcoder=RawStringTranscoder())

    @pytest.fixture(name='bytes_empty_kvp')
    def bytes_key_and_empty_value(self, cb_env) -> KVPair:
        key, value = cb_env.try_n_times(5, 3, cb_env.load_bytes_binary_data, is_deferred=False)
        yield KVPair(key, value)
        run_in_reactor_thread(cb_env.collection.upsert, key, b'', transcoder=RawBinaryTranscoder())

    @pytest.fixture(name='bytes_kvp')
    def bytes_key_and_value(self, cb_env) -> KVPair:
        key, value = cb_env.try_n_times(5, 3, cb_env.load_bytes_binary_data, start_value=b'XXXX', is_deferred=False)
        yield KVPair(key, value)
        run_in_reactor_thread(cb_env.collection.upsert, key, b'', transcoder=RawBinaryTranscoder())

    @pytest.fixture(name='counter_empty_kvp')
    def counter_key_and_empty_value(self, cb_env) -> KVPair:
        key, value = cb_env.try_n_times(5, 3, cb_env.load_counter_binary_data, is_deferred=False)
        yield KVPair(key, value)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,))

    @pytest.fixture(name='counter_kvp')
    def counter_key_and_value(self, cb_env) -> KVPair:
        key, value = cb_env.try_n_times(5, 3, cb_env.load_counter_binary_data, start_value=100, is_deferred=False)
        yield KVPair(key, value)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,))

    # tests

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_append_string(self, cb_env, utf8_empty_kvp):
        cb = cb_env.collection
        key = utf8_empty_kvp.key
        result = run_in_reactor_thread(cb.binary().append, key, 'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        # make sure it really worked
        result = run_in_reactor_thread(cb.get, key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == 'foo'

    def test_append_string_not_empty(self, cb_env, utf8_kvp):
        cb = cb_env.collection
        key = utf8_kvp.key
        value = utf8_kvp.value
        result = run_in_reactor_thread(cb.binary().append, key, 'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        result = run_in_reactor_thread(cb.get, key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == value + 'foo'

    def test_append_string_nokey(self, cb_env, utf8_empty_kvp):
        cb = cb_env.collection
        key = utf8_empty_kvp.key
        run_in_reactor_thread(cb.remove, key)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb.get,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,))

        # @TODO(jc):  3.2.x SDK tests for NotStoredException
        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(cb.binary().append, key, 'foo')

    def test_append_bytes(self, cb_env, bytes_empty_kvp):
        cb = cb_env.collection
        key = bytes_empty_kvp.key
        result = run_in_reactor_thread(cb.binary().append, key, b'XXX')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        # make sure it really worked
        result = run_in_reactor_thread(cb.get, key, transcoder=RawBinaryTranscoder())
        assert result.content_as[bytes] == b'XXX'

    def test_append_bytes_not_empty(self, cb_env, bytes_kvp):
        cb = cb_env.collection
        key = bytes_kvp.key
        value = bytes_kvp.value

        result = run_in_reactor_thread(cb.binary().append, key, 'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        result = run_in_reactor_thread(cb.get, key, transcoder=RawBinaryTranscoder())
        assert result.content_as[bytes] == value + b'foo'

    def test_prepend_string(self, cb_env, utf8_empty_kvp):
        cb = cb_env.collection
        key = utf8_empty_kvp.key
        result = run_in_reactor_thread(cb.binary().prepend, key, 'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        # make sure it really worked
        result = run_in_reactor_thread(cb.get, key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == 'foo'

    def test_prepend_string_not_empty(self, cb_env, utf8_kvp):
        cb = cb_env.collection
        key = utf8_kvp.key
        value = utf8_kvp.value

        result = run_in_reactor_thread(cb.binary().prepend, key, 'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        result = run_in_reactor_thread(cb.get, key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == 'foo' + value

    def test_prepend_string_nokey(self, cb_env, utf8_empty_kvp):
        cb = cb_env.collection
        key = utf8_empty_kvp.key
        run_in_reactor_thread(cb.remove, key)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb.get,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,))

        # @TODO(jc):  3.2.x SDK tests for NotStoredException
        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(cb.binary().prepend, key, 'foo')

    def test_prepend_bytes(self, cb_env, bytes_empty_kvp):
        cb = cb_env.collection
        key = bytes_empty_kvp.key
        result = run_in_reactor_thread(cb.binary().prepend, key, b'XXX')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        # make sure it really worked
        result = run_in_reactor_thread(cb.get, key, transcoder=RawBinaryTranscoder())
        assert result.content_as[bytes] == b'XXX'

    def test_prepend_bytes_not_empty(self, cb_env, bytes_kvp):
        cb = cb_env.collection
        key = bytes_kvp.key
        value = bytes_kvp.value

        result = run_in_reactor_thread(cb.binary().prepend, key, b'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        result = run_in_reactor_thread(cb.get, key, transcoder=RawBinaryTranscoder())
        assert result.content_as[bytes] == b'foo' + value

    def test_counter_increment_initial_value(self, cb_env, counter_empty_kvp):
        cb = cb_env.collection
        key = counter_empty_kvp.key

        result = run_in_reactor_thread(cb.binary().increment, key, IncrementOptions(initial=SignedInt64(100)))
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == 100

    def test_counter_decrement_initial_value(self, cb_env, counter_empty_kvp):
        cb = cb_env.collection
        key = counter_empty_kvp.key

        result = run_in_reactor_thread(cb.binary().decrement, key, DecrementOptions(initial=SignedInt64(100)))
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == 100

    def test_counter_increment(self, cb_env, counter_kvp):
        cb = cb_env.collection
        key = counter_kvp.key
        value = counter_kvp.value

        result = run_in_reactor_thread(cb.binary().increment, key)
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == value + 1

    def test_counter_decrement(self, cb_env, counter_kvp):
        cb = cb_env.collection
        key = counter_kvp.key
        value = counter_kvp.value

        result = run_in_reactor_thread(cb.binary().decrement, key)
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == value - 1

    def test_counter_increment_non_default(self, cb_env, counter_kvp):
        cb = cb_env.collection
        key = counter_kvp.key
        value = counter_kvp.value

        result = run_in_reactor_thread(cb.binary().increment, key, IncrementOptions(delta=DeltaValue(3)))
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == value + 3

    def test_counter_decrement_non_default(self, cb_env, counter_kvp):
        cb = cb_env.collection
        key = counter_kvp.key
        value = counter_kvp.value

        result = run_in_reactor_thread(cb.binary().decrement, key, DecrementOptions(delta=DeltaValue(3)))
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == value - 3

    def test_counter_bad_initial_value(self, cb_env, counter_empty_kvp):
        cb = cb_env.collection
        key = counter_empty_kvp.key

        with pytest.raises(InvalidArgumentException):
            run_in_reactor_thread(cb.binary().increment, key, initial=100)

        with pytest.raises(InvalidArgumentException):
            run_in_reactor_thread(cb.binary().decrement, key, initial=100)

    def test_counter_bad_delta_value(self, cb_env, counter_empty_kvp):
        cb = cb_env.collection
        key = counter_empty_kvp.key

        with pytest.raises(InvalidArgumentException):
            run_in_reactor_thread(cb.binary().increment, key, delta=5)

        with pytest.raises(InvalidArgumentException):
            run_in_reactor_thread(cb.binary().decrement, key, delta=5)

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
