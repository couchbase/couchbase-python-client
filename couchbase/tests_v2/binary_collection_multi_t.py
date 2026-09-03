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

# Sync-only, like the other two multi-op suites, and not registered for mirror generation:
# the multi-key surface is defined on `couchbase.Collection` alone.  Seeding is declared
# rather than inferred: DATA_SET and NUM_DOCS below say what corpus this suite wants, so
# renaming the file cannot change it silently.

from __future__ import annotations

from collections.abc import Generator
from types import MappingProxyType

import pytest

from couchbase.durability import (ClientDurability,
                                  DurabilityLevel,
                                  PersistTo,
                                  ReplicateTo,
                                  ServerDurability)
from couchbase.exceptions import InvalidArgumentException
from couchbase.options import (AppendMultiOptions,
                               AppendOptions,
                               DecrementMultiOptions,
                               DecrementOptions,
                               IncrementMultiOptions,
                               IncrementOptions,
                               PrependMultiOptions,
                               PrependOptions,
                               SignedInt64)
from couchbase.result import (CounterResult,
                              MultiCounterResult,
                              MultiMutationResult,
                              MutationResult)
from tests_v2.environments.test_environment import (CollectionStrategy,
                                                    DocType,
                                                    SuiteRequirements,
                                                    SyncTestEnvironment,
                                                    build_environment,
                                                    teardown_environment)
from tests_v2.manifest import validate_test_manifest


class BinaryCollectionMultiTestSuite(SuiteRequirements):

    # Only the empty variants are drawn from: every test here appends to, prepends to, or
    # counts up from nothing.  Counter keys are not seeded at all, since the point of an
    # `initial` is that the document does not exist yet; get_new_counter_doc_keys() mints them.
    DATA_SET = [DocType.BYTES_EMPTY, DocType.UTF8_EMPTY]
    NUM_DOCS = 20

    TEST_MANIFEST = [
        'test_append_multi_bytes',
        'test_append_multi_mapping',
        'test_append_multi_string',
        'test_counter_multi_decrement',
        'test_counter_multi_decrement_non_default',
        'test_counter_multi_decrement_non_default_per_key',
        'test_counter_multi_increment',
        'test_counter_multi_increment_non_default',
        'test_counter_multi_increment_non_default_per_key',
        'test_multi_mixed_durability_binary_fail',
        'test_prepend_multi_bytes',
        'test_prepend_multi_string',
    ]

    def test_append_multi_bytes(self, cb_env: SyncTestEnvironment) -> None:
        keys = cb_env.get_multiple_existing_docs_by_type(DocType.BYTES_EMPTY, 4)
        values = [b'foo', b'bar', b'baz', b'qux']
        # Annotated, not inferred: append_multi/prepend_multi declare
        # Dict[str, Union[str, bytes, bytearray]], and Dict is invariant in its value type,
        # so the dict[str, bytes] this would otherwise infer is rejected.  Mapping would
        # accept it, and any caller building a homogeneous batch hits the same wall.
        keys_and_docs: dict[str, str | bytes | bytearray] = dict(zip(keys, values))
        res = cb_env.collection.binary().append_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        # One result per key, which holds only because the draw does not repeat: a repeated
        # key collapses keys_and_docs, and every other assertion here passes on the short dict.
        assert len(res.results) == len(keys)

    def test_append_multi_mapping(self, cb_env: SyncTestEnvironment) -> None:
        # PYCBC-1893.  See test_multi_upsert_mapping in collection_multi_t: the binary multi
        # surface takes the same Mapping and had the same hole.
        keys = cb_env.get_multiple_existing_docs_by_type(DocType.BYTES_EMPTY, 4)
        values = [b'foo', b'bar', b'baz', b'qux']
        keys_and_docs: dict[str, str | bytes | bytearray] = dict(zip(keys, values))
        res = cb_env.collection.binary().append_multi(MappingProxyType(keys_and_docs))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    def test_append_multi_string(self, cb_env: SyncTestEnvironment) -> None:
        keys = cb_env.get_multiple_existing_docs_by_type(DocType.UTF8_EMPTY, 4)
        values = ['foo', 'bar', 'baz', 'qux']
        # See test_append_multi_bytes on why this is annotated rather than inferred.
        keys_and_docs: dict[str, str | bytes | bytearray] = dict(zip(keys, values))
        res = cb_env.collection.binary().append_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        assert len(res.results) == len(keys)

    def test_counter_multi_decrement(self, cb_env: SyncTestEnvironment) -> None:
        keys = cb_env.get_new_counter_doc_keys(4)
        res = cb_env.collection.binary().decrement_multi(keys)
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True
        assert len(res.results) == len(keys)

    def test_counter_multi_decrement_non_default(self, cb_env: SyncTestEnvironment) -> None:
        keys = cb_env.get_new_counter_doc_keys(4)
        res = cb_env.collection.binary().decrement_multi(keys, DecrementMultiOptions(initial=SignedInt64(3)))
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True
        assert len(res.results) == len(keys)
        for r in res.results.values():
            assert r.content == 3

    def test_counter_multi_decrement_non_default_per_key(self, cb_env: SyncTestEnvironment) -> None:
        keys = cb_env.get_new_counter_doc_keys(4)
        key1 = keys[0]
        opts = DecrementMultiOptions(initial=SignedInt64(3), per_key_options={
                                     key1: DecrementOptions(initial=SignedInt64(100))})
        res = cb_env.collection.binary().decrement_multi(keys, opts)
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True
        assert len(res.results) == len(keys)
        for k, v in res.results.items():
            if k == key1:
                assert v.content == 100
            else:
                assert v.content == 3

    def test_counter_multi_increment(self, cb_env: SyncTestEnvironment) -> None:
        keys = cb_env.get_new_counter_doc_keys(4)
        res = cb_env.collection.binary().increment_multi(keys)
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True
        assert len(res.results) == len(keys)

    def test_counter_multi_increment_non_default(self, cb_env: SyncTestEnvironment) -> None:
        keys = cb_env.get_new_counter_doc_keys(4)
        res = cb_env.collection.binary().increment_multi(keys, IncrementMultiOptions(initial=SignedInt64(3)))
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True
        assert len(res.results) == len(keys)
        for r in res.results.values():
            assert r.content == 3

    def test_counter_multi_increment_non_default_per_key(self, cb_env: SyncTestEnvironment) -> None:
        keys = cb_env.get_new_counter_doc_keys(4)
        key1 = keys[0]
        opts = IncrementMultiOptions(initial=SignedInt64(3), per_key_options={
                                     key1: IncrementOptions(initial=SignedInt64(100))})
        res = cb_env.collection.binary().increment_multi(keys, opts)
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True
        assert len(res.results) == len(keys)
        for k, v in res.results.items():
            if k == key1:
                assert v.content == 100
            else:
                assert v.content == 3

    def test_prepend_multi_bytes(self, cb_env: SyncTestEnvironment) -> None:
        keys = cb_env.get_multiple_existing_docs_by_type(DocType.BYTES_EMPTY, 4)
        values = [b'foo', b'bar', b'baz', b'qux']
        # See test_append_multi_bytes on why this is annotated rather than inferred.
        keys_and_docs: dict[str, str | bytes | bytearray] = dict(zip(keys, values))
        res = cb_env.collection.binary().prepend_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        assert len(res.results) == len(keys)

    def test_prepend_multi_string(self, cb_env: SyncTestEnvironment) -> None:
        keys = cb_env.get_multiple_existing_docs_by_type(DocType.UTF8_EMPTY, 4)
        values = ['foo', 'bar', 'baz', 'qux']
        # See test_append_multi_bytes on why this is annotated rather than inferred.
        keys_and_docs: dict[str, str | bytes | bytearray] = dict(zip(keys, values))
        res = cb_env.collection.binary().prepend_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        assert len(res.results) == len(keys)

    @pytest.mark.parametrize(
        'op_info',
        [
            ('append_multi', True, AppendMultiOptions, AppendOptions),
            ('prepend_multi', True, PrependMultiOptions, PrependOptions),
            ('increment_multi', False, IncrementMultiOptions, IncrementOptions),
            ('decrement_multi', False, DecrementMultiOptions, DecrementOptions),
        ]
    )
    def test_multi_mixed_durability_binary_fail(self,
                                                cb_env: SyncTestEnvironment,
                                                op_info: tuple[str, bool, type, type]) -> None:
        op_name, has_docs, multi_opts_type, key_opts_type = op_info

        keys_and_docs: dict[str, str | bytes | bytearray] = {}
        if op_name in ('append_multi', 'prepend_multi'):
            keys = cb_env.get_multiple_existing_docs_by_type(DocType.BYTES_EMPTY, 4)
            values = [b'foo', b'bar', b'baz', b'qux']
            keys_and_docs = dict(zip(keys, values))
        else:
            keys = cb_env.get_new_counter_doc_keys(4)

        key1 = keys[0]

        bin_collection = cb_env.collection.binary()
        collection_op = getattr(bin_collection, op_name)

        # Scenario 1: global client durability, per-key server durability
        opts = multi_opts_type(
            durability=ServerDurability(level=DurabilityLevel.MAJORITY),
            per_key_options={key1: key_opts_type(durability=ClientDurability(replicate_to=ReplicateTo.ONE))}
        )

        err_msg = 'Durability must be either client or server for all operations.'
        with pytest.raises(InvalidArgumentException, match=err_msg):
            if has_docs:
                collection_op(keys_and_docs, opts)
            else:
                collection_op(keys, opts)

        # Scenario 2: global server durability, per-key client durability
        opts = multi_opts_type(
            durability=ClientDurability(persist_to=PersistTo.ONE),
            per_key_options={key1: key_opts_type(durability=ServerDurability(
                level=DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE))}
        )

        with pytest.raises(InvalidArgumentException, match=err_msg):
            if has_docs:
                collection_op(keys_and_docs, opts)
            else:
                collection_op(keys, opts)


class BinaryCollectionMultiTests(BinaryCollectionMultiTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(BinaryCollectionMultiTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionStrategy.DEFAULT, CollectionStrategy.NAMED])
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment,
                                   request: pytest.FixtureRequest
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, BinaryCollectionMultiTestSuite, request.param)
        yield env
        teardown_environment(env, BinaryCollectionMultiTestSuite, request.param)
