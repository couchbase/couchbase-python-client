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
from couchbase.exceptions import DocumentNotFoundException, InvalidArgumentException
from couchbase.options import (DecrementOptions,
                               DeltaValue,
                               IncrementOptions,
                               SignedInt64)
from couchbase.result import CounterResult, MutationResult
from couchbase.transcoder import RawBinaryTranscoder, RawStringTranscoder

from ._test_utils import (CollectionType,
                          KVPair,
                          TestEnvironment)


class BinaryCollectionTests:

    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest_asyncio.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    async def couchbase_test_environment(self, couchbase_config, request):
        cb_env = await TestEnvironment.get_environment(__name__,
                                                       couchbase_config,
                                                       manage_buckets=True)
        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        yield cb_env

        # teardown
        await cb_env.try_n_times(5, 3, cb_env.purge_binary_data)
        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times_till_exception(5, 3,
                                                    cb_env.teardown_named_collections,
                                                    raise_if_no_exception=False)

    # key/value fixtures

    @pytest_asyncio.fixture(name='utf8_empty_kvp')
    async def utf8_key_and_empty_value(self, cb_env) -> KVPair:
        key, value = await cb_env.try_n_times(5, 3, cb_env.load_utf8_binary_data)
        yield KVPair(key, value)
        await cb_env.collection.upsert(key, '', transcoder=RawStringTranscoder())

    @pytest_asyncio.fixture(name='utf8_kvp')
    async def utf8_key_and_value(self, cb_env) -> KVPair:
        key, value = await cb_env.try_n_times(5, 3, cb_env.load_utf8_binary_data, start_value='XXXX')
        yield KVPair(key, value)
        await cb_env.collection.upsert(key, '', transcoder=RawStringTranscoder())

    @pytest_asyncio.fixture(name='bytes_empty_kvp')
    async def bytes_key_and_empty_value(self, cb_env) -> KVPair:
        key, value = await cb_env.try_n_times(5, 3, cb_env.load_bytes_binary_data)
        yield KVPair(key, value)
        await cb_env.collection.upsert(key, b'', transcoder=RawBinaryTranscoder())

    @pytest_asyncio.fixture(name='bytes_kvp')
    async def bytes_key_and_value(self, cb_env) -> KVPair:
        key, value = await cb_env.try_n_times(5, 3, cb_env.load_bytes_binary_data, start_value=b'XXXX')
        yield KVPair(key, value)
        await cb_env.collection.upsert(key, b'', transcoder=RawBinaryTranscoder())

    @pytest_asyncio.fixture(name='counter_empty_kvp')
    async def counter_key_and_empty_value(self, cb_env) -> KVPair:
        key, value = await cb_env.try_n_times(5, 3, cb_env.load_counter_binary_data)
        yield KVPair(key, value)
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,))

    @pytest_asyncio.fixture(name='counter_kvp')
    async def counter_key_and_value(self, cb_env) -> KVPair:
        key, value = await cb_env.try_n_times(5, 3, cb_env.load_counter_binary_data, start_value=100)
        yield KVPair(key, value)
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,))

    # tests

    @pytest.mark.asyncio
    async def test_append_string(self, cb_env, utf8_empty_kvp):
        cb = cb_env.collection
        key = utf8_empty_kvp.key
        result = await cb.binary().append(key, 'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        # make sure it really worked
        result = await cb.get(key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == 'foo'

    @pytest.mark.asyncio
    async def test_append_string_not_empty(self, cb_env, utf8_kvp):
        cb = cb_env.collection
        key = utf8_kvp.key
        value = utf8_kvp.value
        result = await cb.binary().append(key, 'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        result = await cb.get(key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == value + 'foo'

    @pytest.mark.asyncio
    async def test_append_string_nokey(self, cb_env, utf8_empty_kvp):
        cb = cb_env.collection
        key = utf8_empty_kvp.key
        await cb.remove(key)
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb.get,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,))

        # @TODO(jc):  3.2.x SDK tests for NotStoredException
        with pytest.raises(DocumentNotFoundException):
            await cb.binary().append(key, 'foo')

    @pytest.mark.asyncio
    async def test_append_bytes(self, cb_env, bytes_empty_kvp):
        cb = cb_env.collection
        key = bytes_empty_kvp.key
        result = await cb.binary().append(key, b'XXX')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        # make sure it really worked
        result = await cb.get(key, transcoder=RawBinaryTranscoder())
        assert result.content_as[bytes] == b'XXX'

    @pytest.mark.asyncio
    async def test_append_bytes_not_empty(self, cb_env, bytes_kvp):
        cb = cb_env.collection
        key = bytes_kvp.key
        value = bytes_kvp.value

        result = await cb.binary().append(key, 'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        result = await cb.get(key, transcoder=RawBinaryTranscoder())
        assert result.content_as[bytes] == value + b'foo'

    @pytest.mark.asyncio
    async def test_prepend_string(self, cb_env, utf8_empty_kvp):
        cb = cb_env.collection
        key = utf8_empty_kvp.key
        result = await cb.binary().prepend(key, 'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        # make sure it really worked
        result = await cb.get(key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == 'foo'

    @pytest.mark.asyncio
    async def test_prepend_string_not_empty(self, cb_env, utf8_kvp):
        cb = cb_env.collection
        key = utf8_kvp.key
        value = utf8_kvp.value

        result = await cb.binary().prepend(key, 'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        result = await cb.get(key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == 'foo' + value

    @pytest.mark.asyncio
    async def test_prepend_string_nokey(self, cb_env, utf8_empty_kvp):
        cb = cb_env.collection
        key = utf8_empty_kvp.key
        await cb.remove(key)
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb.get,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,))

        # @TODO(jc):  3.2.x SDK tests for NotStoredException
        with pytest.raises(DocumentNotFoundException):
            await cb.binary().prepend(key, 'foo')

    @pytest.mark.asyncio
    async def test_prepend_bytes(self, cb_env, bytes_empty_kvp):
        cb = cb_env.collection
        key = bytes_empty_kvp.key
        result = await cb.binary().prepend(key, b'XXX')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        # make sure it really worked
        result = await cb.get(key, transcoder=RawBinaryTranscoder())
        assert result.content_as[bytes] == b'XXX'

    @pytest.mark.asyncio
    async def test_prepend_bytes_not_empty(self, cb_env, bytes_kvp):
        cb = cb_env.collection
        key = bytes_kvp.key
        value = bytes_kvp.value

        result = await cb.binary().prepend(key, b'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        result = await cb.get(key, transcoder=RawBinaryTranscoder())
        assert result.content_as[bytes] == b'foo' + value

    @pytest.mark.asyncio
    async def test_counter_increment_initial_value(self, cb_env, counter_empty_kvp):
        cb = cb_env.collection
        key = counter_empty_kvp.key

        result = await cb.binary().increment(key, IncrementOptions(initial=SignedInt64(100)))
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == 100

    @pytest.mark.asyncio
    async def test_counter_decrement_initial_value(self, cb_env, counter_empty_kvp):
        cb = cb_env.collection
        key = counter_empty_kvp.key

        result = await cb.binary().decrement(key, DecrementOptions(initial=SignedInt64(100)))
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == 100

    @pytest.mark.asyncio
    async def test_counter_increment(self, cb_env, counter_kvp):
        cb = cb_env.collection
        key = counter_kvp.key
        value = counter_kvp.value

        result = await cb.binary().increment(key)
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == value + 1

    @pytest.mark.asyncio
    async def test_counter_decrement(self, cb_env, counter_kvp):
        cb = cb_env.collection
        key = counter_kvp.key
        value = counter_kvp.value

        result = await cb.binary().decrement(key)
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == value - 1

    @pytest.mark.asyncio
    async def test_counter_increment_non_default(self, cb_env, counter_kvp):
        cb = cb_env.collection
        key = counter_kvp.key
        value = counter_kvp.value

        result = await cb.binary().increment(key, IncrementOptions(delta=DeltaValue(3)))
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == value + 3

    @pytest.mark.asyncio
    async def test_counter_decrement_non_default(self, cb_env, counter_kvp):
        cb = cb_env.collection
        key = counter_kvp.key
        value = counter_kvp.value

        result = await cb.binary().decrement(key, DecrementOptions(delta=DeltaValue(3)))
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == value - 3

    @pytest.mark.asyncio
    async def test_counter_bad_initial_value(self, cb_env, counter_empty_kvp):
        cb = cb_env.collection
        key = counter_empty_kvp.key

        with pytest.raises(InvalidArgumentException):
            await cb.binary().increment(key, initial=100)

        with pytest.raises(InvalidArgumentException):
            await cb.binary().decrement(key, initial=100)

    @pytest.mark.asyncio
    async def test_counter_bad_delta_value(self, cb_env, counter_empty_kvp):
        cb = cb_env.collection
        key = counter_empty_kvp.key

        with pytest.raises(InvalidArgumentException):
            await cb.binary().increment(key, delta=5)

        with pytest.raises(InvalidArgumentException):
            await cb.binary().decrement(key, delta=5)

    @pytest.mark.asyncio
    async def test_unsigned_int(self):
        with pytest.raises(InvalidArgumentException):
            x = DeltaValue(-1)
        with pytest.raises(InvalidArgumentException):
            x = DeltaValue(0x7FFFFFFFFFFFFFFF + 1)

        x = DeltaValue(5)
        assert 5 == x.value

    @pytest.mark.asyncio
    async def test_signed_int_64(self):
        with pytest.raises(InvalidArgumentException):
            x = SignedInt64(-0x7FFFFFFFFFFFFFFF - 2)

        with pytest.raises(InvalidArgumentException):
            x = SignedInt64(0x7FFFFFFFFFFFFFFF + 1)

        x = SignedInt64(0x7FFFFFFFFFFFFFFF)
        assert 0x7FFFFFFFFFFFFFFF == x.value
        x = SignedInt64(-0x7FFFFFFFFFFFFFFF - 1)
        assert -0x7FFFFFFFFFFFFFFF - 1 == x.value
