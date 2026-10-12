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

# Generated for both acouchbase and txcouchbase from this one file and its one
# TEST_MANIFEST, which is what holds the three api styles to the same set of tests.  This is
# also the suite that first exercises the populated DocType variants (BYTES, COUNTER, UTF8).

from __future__ import annotations

from collections.abc import Generator

import pytest

from couchbase.exceptions import DocumentNotFoundException, InvalidArgumentException
from couchbase.options import (DecrementOptions,
                               DeltaValue,
                               IncrementOptions,
                               SignedInt64)
from couchbase.result import CounterResult, MutationResult
from couchbase.transcoder import RawBinaryTranscoder, RawStringTranscoder
from tests.environments.test_environment import (CollectionStrategy,
                                                 DocType,
                                                 SuiteRequirements,
                                                 SyncTestEnvironment,
                                                 TestEnvironment,
                                                 build_environment,
                                                 teardown_environment)
from tests.manifest import validate_test_manifest


class BinaryCollectionTestSuite(SuiteRequirements):

    # Both the empty and the populated variants: the append/prepend tests come in pairs, one
    # starting from nothing and one from an existing value, and the counter tests need a doc
    # that already holds an integer.  Counter keys for the `initial` cases are not seeded, since
    # the point of an `initial` is that the document is absent; get_new_counter_doc_keys() mints
    # those.
    DATA_SET = [DocType.BYTES,
                DocType.BYTES_EMPTY,
                DocType.COUNTER,
                DocType.UTF8,
                DocType.UTF8_EMPTY]
    NUM_DOCS = 10

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
        'test_counter_decrement_no_initial_value',
        'test_counter_increment',
        'test_counter_increment_initial_value',
        'test_counter_increment_non_default',
        'test_counter_increment_no_initial_value',
        'test_prepend_bytes',
        'test_prepend_bytes_not_empty',
        'test_prepend_string',
        'test_prepend_string_nokey',
        'test_prepend_string_not_empty',
    ]

    def test_append_bytes(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_existing_doc_by_type(DocType.BYTES_EMPTY, key_only=True)
        result = cb_env.collection.binary().append(key, b'XXX')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        stored = cb_env.collection.get(key, transcoder=RawBinaryTranscoder())
        assert stored.content_as[bytes] == b'XXX'

    def test_append_bytes_not_empty(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc_by_type(DocType.BYTES)
        # b'foo', not 'foo'.  Appending the *str* to a document written through
        # RawBinaryTranscoder and asserting `value + b'foo'` also passes, because the SDK encodes
        # str to utf-8 on the way out, so it exercises everything except the one case this test
        # is for: bytes appended to a non-empty binary document.
        result = cb_env.collection.binary().append(key, b'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        stored = cb_env.collection.get(key, transcoder=RawBinaryTranscoder())
        assert stored.content_as[bytes] == value + b'foo'

    def test_append_string(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_existing_doc_by_type(DocType.UTF8_EMPTY, key_only=True)
        result = cb_env.collection.binary().append(key, 'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        stored = cb_env.collection.get(key, transcoder=RawStringTranscoder())
        assert stored.content_as[str] == 'foo'

    def test_append_string_not_empty(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc_by_type(DocType.UTF8)
        result = cb_env.collection.binary().append(key, 'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        stored = cb_env.collection.get(key, transcoder=RawStringTranscoder())
        assert stored.content_as[str] == value + 'foo'

    def test_append_string_nokey(self, cb_env: SyncTestEnvironment) -> None:
        # Appending to an absent document raises DocumentNotFoundException.  The 3.2.x SDK
        # raised NotStoredException here; 4.x has no such exception class at all, so there is
        # nothing left to reconcile and the assertion below is the whole story.
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.binary().append(TestEnvironment.NOT_A_KEY, 'foo')

    def test_counter_bad_delta_value(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_new_counter_doc_keys(1)[0]

        with pytest.raises(InvalidArgumentException):
            cb_env.collection.binary().increment(key, delta=5)

        with pytest.raises(InvalidArgumentException):
            cb_env.collection.binary().decrement(key, delta=5)

    def test_counter_bad_initial_value(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_new_counter_doc_keys(1)[0]

        with pytest.raises(InvalidArgumentException):
            cb_env.collection.binary().increment(key, initial=100)

        with pytest.raises(InvalidArgumentException):
            cb_env.collection.binary().decrement(key, initial=100)

    def test_counter_decrement(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc_by_type(DocType.COUNTER)
        result = cb_env.collection.binary().decrement(key)
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == value - 1

    def test_counter_decrement_initial_value(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_new_counter_doc_keys(1)[0]
        result = cb_env.collection.binary().decrement(key, DecrementOptions(initial=SignedInt64(100)))
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == 100

    def test_counter_decrement_non_default(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc_by_type(DocType.COUNTER)
        result = cb_env.collection.binary().decrement(key, DecrementOptions(delta=DeltaValue(3)))
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == value - 3

    def test_counter_decrement_no_initial_value(self, cb_env: SyncTestEnvironment) -> None:
        # A minted key rather than a fixed literal such as 'non-existent-doc'.  On the DEFAULT
        # strategy the collection is shared bucket-wide and written by other suites and
        # other xdist workers, so a test whose correctness depends on one fixed key never having
        # been written is relying on a guarantee nothing provides.
        key = cb_env.get_new_counter_doc_keys(1)[0]
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.binary().decrement(key, DecrementOptions(initial=SignedInt64(-1)))

    def test_counter_increment(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc_by_type(DocType.COUNTER)
        result = cb_env.collection.binary().increment(key)
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == value + 1

    def test_counter_increment_initial_value(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_new_counter_doc_keys(1)[0]
        result = cb_env.collection.binary().increment(key, IncrementOptions(initial=SignedInt64(100)))
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == 100

    def test_counter_increment_non_default(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc_by_type(DocType.COUNTER)
        result = cb_env.collection.binary().increment(key, IncrementOptions(delta=DeltaValue(3)))
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        assert result.content == value + 3

    def test_counter_increment_no_initial_value(self, cb_env: SyncTestEnvironment) -> None:
        # See test_counter_decrement_no_initial_value on why the key is minted.
        key = cb_env.get_new_counter_doc_keys(1)[0]
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.binary().increment(key, IncrementOptions(initial=SignedInt64(-1)))

    def test_prepend_bytes(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_existing_doc_by_type(DocType.BYTES_EMPTY, key_only=True)
        result = cb_env.collection.binary().prepend(key, b'XXX')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        stored = cb_env.collection.get(key, transcoder=RawBinaryTranscoder())
        assert stored.content_as[bytes] == b'XXX'

    def test_prepend_bytes_not_empty(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc_by_type(DocType.BYTES)
        result = cb_env.collection.binary().prepend(key, b'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        stored = cb_env.collection.get(key, transcoder=RawBinaryTranscoder())
        assert stored.content_as[bytes] == b'foo' + value

    def test_prepend_string(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_existing_doc_by_type(DocType.UTF8_EMPTY, key_only=True)
        result = cb_env.collection.binary().prepend(key, 'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        stored = cb_env.collection.get(key, transcoder=RawStringTranscoder())
        assert stored.content_as[str] == 'foo'

    def test_prepend_string_nokey(self, cb_env: SyncTestEnvironment) -> None:
        # See test_append_string_nokey on the retired NotStoredException.
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.binary().prepend(TestEnvironment.NOT_A_KEY, 'foo')

    def test_prepend_string_not_empty(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc_by_type(DocType.UTF8)
        result = cb_env.collection.binary().prepend(key, 'foo')
        assert isinstance(result, MutationResult)
        assert result.cas is not None
        stored = cb_env.collection.get(key, transcoder=RawStringTranscoder())
        assert stored.content_as[str] == 'foo' + value


class BinaryCollectionTests(BinaryCollectionTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(BinaryCollectionTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionStrategy.DEFAULT, CollectionStrategy.NAMED])
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment,
                                   request: pytest.FixtureRequest
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, BinaryCollectionTestSuite, request.param)
        yield env
        teardown_environment(env, BinaryCollectionTestSuite, request.param)


class BinaryValueUnitTests:
    # Split out of the suite class, following RangeScanRequestBuilderUnitTests and
    # TransactionOptionsUnitTests.  TEST_MANIFEST is the parity contract for a suite, and two
    # tests that construct an options value and check its bounds belong to no suite's
    # contract; inside one, the manifest would be asserting coverage of the binary KV surface
    # through two tests that touch none of it.

    def test_unsigned_int(self) -> None:
        # No `x = ...` on the raising calls: the assignment never happens, and flake8 reports
        # the dead local as F841.
        with pytest.raises(InvalidArgumentException):
            DeltaValue(-1)
        with pytest.raises(InvalidArgumentException):
            DeltaValue(0x7FFFFFFFFFFFFFFF + 1)

        x = DeltaValue(5)
        assert 5 == x.value

    def test_signed_int_64(self) -> None:
        with pytest.raises(InvalidArgumentException):
            SignedInt64(-0x7FFFFFFFFFFFFFFF - 2)

        with pytest.raises(InvalidArgumentException):
            SignedInt64(0x7FFFFFFFFFFFFFFF + 1)

        x = SignedInt64(0x7FFFFFFFFFFFFFFF)
        assert 0x7FFFFFFFFFFFFFFF == x.value
        x = SignedInt64(-0x7FFFFFFFFFFFFFFF - 1)
        assert -0x7FFFFFFFFFFFFFFF - 1 == x.value
