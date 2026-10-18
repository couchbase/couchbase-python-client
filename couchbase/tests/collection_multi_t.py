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

# Sync-only, and not registered for mirror generation: the multi-key surface is defined on
# `couchbase.Collection` alone.  The node-count, replica and server-group fixtures come from
# `TopologySuite`, and inheriting it is also what makes the environment gather the facts
# they read.  The two dict constants are read-only fixtures of the suite, each stated once:
# eight inline copies of a key set that must not exist is eight chances for one to drift.

from __future__ import annotations

import sys
from collections.abc import Generator
from datetime import timedelta
from types import MappingProxyType
from typing import Any, cast

import pytest

from couchbase._utils import JSONType
from couchbase.exceptions import (CouchbaseException,
                                  DocumentExistsException,
                                  DocumentNotFoundException,
                                  DocumentUnretrievableException,
                                  InvalidArgumentException)
from couchbase.options import (GetAllReplicasMultiOptions,
                               GetAnyReplicaMultiOptions,
                               GetMultiOptions,
                               InsertMultiOptions,
                               InsertOptions,
                               RemoveMultiOptions,
                               ReplaceMultiOptions,
                               ReplaceOptions,
                               TouchMultiOptions,
                               UpsertMultiOptions,
                               UpsertOptions)
from couchbase.replica_reads import ReadPreference
from couchbase.result import (ExistsResult,
                              GetReplicaResult,
                              GetResult,
                              MultiExistsResult,
                              MultiGetReplicaResult,
                              MultiGetResult,
                              MultiMutationResult,
                              MutationResult)
from tests.assert_helpers import assert_all_expired
from tests.environments.managers import TestEnvironmentBucketManager
from tests.environments.test_environment import (CollectionStrategy,
                                                 DocType,
                                                 SyncTestEnvironment,
                                                 build_environment,
                                                 teardown_environment,
                                                 verify_seeded_docs_readable)
from tests.manifest import validate_test_manifest
from tests.suite_fixtures import TopologySuite


class CollectionMultiTestSuite(TopologySuite):

    MANAGERS = [TestEnvironmentBucketManager]
    DATA_SET = [DocType.SIMPLE]
    NUM_DOCS = 100

    # Keys that must not exist, for the "fail" half of every operation.
    FAKE_DOCS: dict[str, Any] = {
        'not-a-key1': {'what': 'a fake test doc!', 'id': 'not-a-key1'},
        'not-a-key2': {'what': 'a fake test doc!', 'id': 'not-a-key2'},
        'not-a-key3': {'what': 'a fake test doc!', 'id': 'not-a-key3'},
        'not-a-key4': {'what': 'a fake test doc!', 'id': 'not-a-key4'},
    }

    # A key->doc mapping handed to an operation that wants a key *list*, so the SDK must reject
    # it before it ever reaches the wire.  Never written, so the keys need not be absent.
    INVALID_INPUT_DOCS: dict[str, Any] = {
        'test-key1': {'what': 'a test doc!', 'id': 'test-key1'},
        'test-key2': {'what': 'a test doc!', 'id': 'test-key2'},
        'test-key3': {'what': 'a test doc!', 'id': 'test-key3'},
        'test-key4': {'what': 'a test doc!', 'id': 'test-key4'},
    }

    TEST_MANIFEST = [
        'test_multi_exists_invalid_input',
        'test_multi_exists_not_exist',
        'test_multi_exists_simple',
        'test_multi_get_all_replicas_fail',
        'test_multi_get_all_replicas_invalid_input',
        'test_multi_get_all_replicas_simple',
        'test_multi_get_all_replicas_read_preference',
        'test_multi_get_any_replica_fail',
        'test_multi_get_any_replica_invalid_input',
        'test_multi_get_any_replica_simple',
        'test_multi_get_any_replica_read_preference',
        'test_multi_get_fail',
        'test_multi_get_invalid_input',
        'test_multi_get_simple',
        'test_multi_insert_fail',
        'test_multi_insert_global_opts',
        'test_multi_insert_invalid_input',
        'test_multi_insert_key_opts',
        'test_multi_insert_simple',
        'test_multi_lock_and_unlock_simple',
        'test_multi_lock_invalid_input',
        'test_multi_remove_fail',
        'test_multi_remove_invalid_input',
        'test_multi_remove_simple',
        'test_multi_replace_fail',
        'test_multi_replace_global_opts',
        'test_multi_replace_invalid_input',
        'test_multi_replace_key_opts',
        'test_multi_replace_simple',
        'test_multi_result_keys_not_interned',
        'test_multi_touch_fail',
        'test_multi_touch_invalid_input',
        'test_multi_touch_simple',
        'test_multi_unlock_invalid_input',
        'test_multi_upsert_global_opts',
        'test_multi_upsert_invalid_input',
        'test_multi_upsert_key_opts',
        'test_multi_upsert_mapping',
        'test_multi_upsert_simple',
    ]

    def test_multi_exists_simple(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
        res = cb_env.collection.exists_multi(keys)
        assert isinstance(res, MultiExistsResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, ExistsResult), res.results.values())) is True
        for r in res.results.values():
            assert r.exists is True

    def test_multi_exists_not_exist(self, cb_env: SyncTestEnvironment) -> None:
        keys = list(self.FAKE_DOCS.keys())
        res = cb_env.collection.exists_multi(keys)
        assert isinstance(res, MultiExistsResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, ExistsResult), res.results.values())) is True
        for r in res.results.values():
            assert r.exists is False

    def test_multi_exists_invalid_input(self, cb_env: SyncTestEnvironment) -> None:
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.exists_multi(cast('list[str]', self.INVALID_INPUT_DOCS))

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_multi_get_all_replicas_fail(self, cb_env: SyncTestEnvironment) -> None:
        keys = list(self.FAKE_DOCS.keys())
        res = cb_env.collection.get_all_replicas_multi(keys)
        assert isinstance(res, MultiGetReplicaResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), CouchbaseException), res.exceptions.values())) is True

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get_all_replicas_multi(keys, return_exceptions=False)

        with pytest.raises(DocumentNotFoundException):
            # GetAllReplicasMultiOptions, not GetAnyReplicaMultiOptions.  Third instance of the
            # same copy-paste as in test_multi_remove_fail: return_exceptions is the only field
            # set and every *MultiOptions has one, so the wrong class behaved identically.
            cb_env.collection.get_all_replicas_multi(keys, GetAllReplicasMultiOptions(return_exceptions=False))

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_multi_get_all_replicas_invalid_input(self, cb_env: SyncTestEnvironment) -> None:
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.get_all_replicas_multi(cast('list[str]', self.INVALID_INPUT_DOCS))

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_multi_get_all_replicas_simple(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
        res = cb_env.collection.get_all_replicas_multi(keys)
        assert isinstance(res, MultiGetReplicaResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, list), res.results.values())) is True
        # get_all_replicas_multi and get_any_replica_multi share MultiGetReplicaResult, whose
        # results property is declared Dict[str, GetReplicaResult].  That is right for
        # any-replica and wrong here, where all-replicas yields a list per key; the cast
        # records that the annotation covers only one of the two operations.
        all_replicas = cast('dict[str, list[GetReplicaResult]]', res.results)
        for k, v in all_replicas.items():
            for replica in v:
                assert isinstance(replica, GetReplicaResult)
                assert isinstance(replica.is_replica, bool)
                assert replica.content_as[dict] == keys_and_docs[k]

    @pytest.mark.usefixtures('check_replicas_online')
    @pytest.mark.usefixtures('check_server_groups_supported')
    def test_multi_get_all_replicas_read_preference(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
        res = cb_env.collection.get_all_replicas_multi(
            keys, GetAllReplicasMultiOptions(read_preference=ReadPreference.SELECTED_SERVER_GROUP,
                                             return_exceptions=True))
        assert isinstance(res, MultiGetReplicaResult)
        assert res.all_ok is False
        assert isinstance(res.results, dict)
        assert all(map(lambda r: isinstance(r, DocumentUnretrievableException), res.exceptions.values())) is True
        assert len(res.results) == 0

    @pytest.mark.usefixtures('check_replicas_online')
    def test_multi_get_any_replica_fail(self, cb_env: SyncTestEnvironment) -> None:
        keys = list(self.FAKE_DOCS.keys())
        res = cb_env.collection.get_any_replica_multi(keys)
        assert isinstance(res, MultiGetReplicaResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), CouchbaseException), res.exceptions.values())) is True

        with pytest.raises(DocumentUnretrievableException):
            cb_env.collection.get_any_replica_multi(keys, return_exceptions=False)

        with pytest.raises(DocumentUnretrievableException):
            cb_env.collection.get_any_replica_multi(keys, GetAnyReplicaMultiOptions(return_exceptions=False))

    @pytest.mark.usefixtures('check_replicas_online')
    def test_multi_get_any_replica_invalid_input(self, cb_env: SyncTestEnvironment) -> None:
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.get_any_replica_multi(cast('list[str]', self.INVALID_INPUT_DOCS))

    @pytest.mark.usefixtures('check_replicas_online')
    def test_multi_get_any_replica_simple(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
        res = cb_env.collection.get_any_replica_multi(keys)
        assert isinstance(res, MultiGetReplicaResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, GetReplicaResult), res.results.values())) is True
        for k, v in res.results.items():
            assert isinstance(v.is_replica, bool)
            assert v.content_as[dict] == keys_and_docs[k]

    @pytest.mark.usefixtures('check_replicas_online')
    @pytest.mark.usefixtures('check_server_groups_supported')
    def test_multi_get_any_replica_read_preference(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
        res = cb_env.collection.get_any_replica_multi(
            keys, GetAnyReplicaMultiOptions(read_preference=ReadPreference.SELECTED_SERVER_GROUP,
                                            return_exceptions=True))
        assert isinstance(res, MultiGetReplicaResult)
        assert res.all_ok is False
        assert isinstance(res.results, dict)
        assert all(map(lambda r: isinstance(r, DocumentUnretrievableException), res.exceptions.values())) is True
        assert len(res.results) == 0

    def test_multi_get_fail(self, cb_env: SyncTestEnvironment) -> None:
        keys = list(self.FAKE_DOCS.keys())
        res = cb_env.collection.get_multi(keys)
        assert isinstance(res, MultiGetResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), CouchbaseException), res.exceptions.values())) is True

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get_multi(keys, return_exceptions=False)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get_multi(keys, GetMultiOptions(return_exceptions=False))

    def test_multi_get_invalid_input(self, cb_env: SyncTestEnvironment) -> None:
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.get_multi(cast('list[str]', self.INVALID_INPUT_DOCS))

    def test_multi_get_simple(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
        res = cb_env.collection.get_multi(keys)
        assert isinstance(res, MultiGetResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, GetResult), res.results.values())) is True
        for k, v in res.results.items():
            assert v.content_as[dict] == keys_and_docs[k]

    def test_multi_insert_fail(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        res = cb_env.collection.insert_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), CouchbaseException), res.exceptions.values())) is True

        with pytest.raises(DocumentExistsException):
            cb_env.collection.insert_multi(keys_and_docs, return_exceptions=False)

        with pytest.raises(DocumentExistsException):
            cb_env.collection.insert_multi(keys_and_docs, InsertMultiOptions(return_exceptions=False))

    def test_multi_insert_global_opts(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_new_docs(4)
        opts = InsertMultiOptions(expiry=timedelta(seconds=2))
        res = cb_env.collection.insert_multi(keys_and_docs, opts)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        assert_all_expired(cb_env, list(keys_and_docs.keys()), ctx='global 2s expiry on insert_multi')

    def test_multi_insert_invalid_input(self, cb_env: SyncTestEnvironment) -> None:
        keys = list(self.INVALID_INPUT_DOCS.keys())
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.insert_multi(cast('dict[str, JSONType]', keys))

    def test_multi_insert_key_opts(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_new_docs(4)
        key1 = list(keys_and_docs.keys())[0]
        opts = InsertMultiOptions(expiry=timedelta(seconds=2), per_key_options={
                                  key1: InsertOptions(expiry=timedelta(seconds=0))})
        res = cb_env.collection.insert_multi(keys_and_docs, opts)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        assert_all_expired(cb_env,
                           list(keys_and_docs.keys()),
                           okay_key=key1,
                           ctx='per-key expiry of 0 overrides the global 2s expiry')

    def test_multi_insert_simple(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_new_docs(4)
        res = cb_env.collection.insert_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    def test_multi_lock_and_unlock_simple(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
        res = cb_env.collection.lock_multi(keys, timedelta(seconds=5))
        assert isinstance(res, MultiGetResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, GetResult), res.results.values())) is True

        unlock_res = cb_env.collection.unlock_multi(res)
        assert isinstance(unlock_res, dict)

    def test_multi_lock_invalid_input(self, cb_env: SyncTestEnvironment) -> None:
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.lock_multi(cast('list[str]', self.INVALID_INPUT_DOCS), timedelta(seconds=5))

    def test_multi_remove_fail(self, cb_env: SyncTestEnvironment) -> None:
        keys = list(self.FAKE_DOCS.keys())
        res = cb_env.collection.remove_multi(keys)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), CouchbaseException), res.exceptions.values())) is True

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.remove_multi(keys, return_exceptions=False)

        with pytest.raises(DocumentNotFoundException):
            # RemoveMultiOptions, not ReplaceMultiOptions: `return_exceptions` is the only
            # field set and every *MultiOptions carries it, so the wrong class here passes
            # just as well and this line covers nothing it names.
            cb_env.collection.remove_multi(keys, RemoveMultiOptions(return_exceptions=False))

    def test_multi_remove_invalid_input(self, cb_env: SyncTestEnvironment) -> None:
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.remove_multi(cast('list[str]', self.INVALID_INPUT_DOCS))

    def test_multi_remove_simple(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
        res = cb_env.collection.remove_multi(keys)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        assert_all_expired(cb_env, keys, ctx='after remove_multi')

    def test_multi_replace_fail(self, cb_env: SyncTestEnvironment) -> None:
        res = cb_env.collection.replace_multi(self.FAKE_DOCS)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), CouchbaseException), res.exceptions.values())) is True

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.replace_multi(self.FAKE_DOCS, return_exceptions=False)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.replace_multi(self.FAKE_DOCS, ReplaceMultiOptions(return_exceptions=False))

    def test_multi_replace_global_opts(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        opts = ReplaceMultiOptions(expiry=timedelta(seconds=2))
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        res = cb_env.collection.replace_multi(keys_and_docs, opts)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        assert_all_expired(cb_env, list(keys_and_docs.keys()), ctx='global 2s expiry on replace_multi')

    def test_multi_replace_invalid_input(self, cb_env: SyncTestEnvironment) -> None:
        keys = list(self.INVALID_INPUT_DOCS.keys())
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.replace_multi(cast('dict[str, JSONType]', keys))

    def test_multi_replace_key_opts(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        key1 = list(keys_and_docs.keys())[0]
        # ReplaceOptions, not UpsertOptions.  Same shape of copy-paste as in
        # test_multi_remove_fail: per_key_options is declared Dict[str, ReplaceOptions], both
        # classes carry expiry, and the wrong one behaved identically, so nothing caught it.
        opts = ReplaceMultiOptions(expiry=timedelta(seconds=2), per_key_options={
                                   key1: ReplaceOptions(expiry=timedelta(seconds=0))})
        res = cb_env.collection.replace_multi(keys_and_docs, opts)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        assert_all_expired(cb_env,
                           list(keys_and_docs.keys()),
                           okay_key=key1,
                           ctx='per-key expiry of 0 overrides the global 2s expiry')

    def test_multi_replace_simple(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        res = cb_env.collection.replace_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        for k, v in keys_and_docs.items():
            r = cb_env.collection.get(k)
            assert r.content_as[dict] == v

    def test_multi_result_keys_not_interned(self, cb_env: SyncTestEnvironment) -> None:
        # PYCBC-1967: multi-op result keys must not be interned. Interned keys are immortal on
        # Python 3.12+, so every distinct key passed through a *_multi op would never be freed.
        keys = list(cb_env.get_docs(2).keys()) + list(self.FAKE_DOCS.keys())
        res = cb_env.collection.get_multi(keys)
        returned = list(res.results.keys()) + list(res.exceptions.keys())
        assert sorted(returned) == sorted(keys)
        # getattr, not hasattr: sys._is_immortal only exists (and is only in the stubs) on 3.14+.
        is_immortal = getattr(sys, '_is_immortal', None)
        for k in returned:
            # k.encode().decode() is an equal but distinct str; sys.intern() returns k itself
            # only when k is already in the interned table, i.e. the C extension interned it.
            assert sys.intern(k.encode().decode()) is not k
            if is_immortal is not None:
                assert is_immortal(k) is False

    def test_multi_touch_fail(self, cb_env: SyncTestEnvironment) -> None:
        keys = list(self.FAKE_DOCS.keys())
        res = cb_env.collection.touch_multi(keys, timedelta(seconds=2))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), CouchbaseException), res.exceptions.values())) is True

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.touch_multi(keys, timedelta(seconds=2), return_exceptions=False)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.touch_multi(keys, timedelta(seconds=2), TouchMultiOptions(return_exceptions=False))

    def test_multi_touch_invalid_input(self, cb_env: SyncTestEnvironment) -> None:
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.touch_multi(cast('list[str]', self.INVALID_INPUT_DOCS), timedelta(seconds=2))

    def test_multi_touch_simple(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
        res = cb_env.collection.touch_multi(keys, timedelta(seconds=2))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        assert_all_expired(cb_env, keys, ctx='2s expiry applied by touch_multi')

    def test_multi_unlock_invalid_input(self, cb_env: SyncTestEnvironment) -> None:
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.unlock_multi(cast('dict[str, int]', self.INVALID_INPUT_DOCS))

        with pytest.raises(InvalidArgumentException):
            cb_env.collection.unlock_multi(cast('dict[str, int]', list(self.INVALID_INPUT_DOCS.keys())))

    def test_multi_upsert_global_opts(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        opts = UpsertMultiOptions(expiry=timedelta(seconds=2))
        res = cb_env.collection.upsert_multi(keys_and_docs, opts)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        assert_all_expired(cb_env, list(keys_and_docs.keys()), ctx='global 2s expiry on upsert_multi')

    def test_multi_upsert_invalid_input(self, cb_env: SyncTestEnvironment) -> None:
        keys = list(self.INVALID_INPUT_DOCS.keys())
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.upsert_multi(cast('dict[str, JSONType]', keys))

    def test_multi_upsert_key_opts(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        key1 = list(keys_and_docs.keys())[0]
        opts = UpsertMultiOptions(expiry=timedelta(seconds=2), per_key_options={
                                  key1: UpsertOptions(expiry=timedelta(seconds=0))})
        res = cb_env.collection.upsert_multi(keys_and_docs, opts)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        assert_all_expired(cb_env,
                           list(keys_and_docs.keys()),
                           okay_key=key1,
                           ctx='per-key expiry of 0 overrides the global 2s expiry')

    def test_multi_upsert_mapping(self, cb_env: SyncTestEnvironment) -> None:
        # PYCBC-1893.  The multi write surface is annotated for Mapping and only ever reads what
        # it is handed, so anything that reads like one has to reach the server.  A read-only
        # Mapping is the case that catches an implementation reaching for a dict method.
        keys_and_docs = MappingProxyType(cb_env.get_docs(4))
        res = cb_env.collection.upsert_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    def test_multi_upsert_simple(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        res = cb_env.collection.upsert_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True


class CollectionMultiTests(CollectionMultiTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(CollectionMultiTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionStrategy.DEFAULT, CollectionStrategy.NAMED])
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment,
                                   request: pytest.FixtureRequest
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, CollectionMultiTestSuite, request.param)
        # Kept per-suite rather than promoted into setup(); see verify_seeded_docs_readable.
        verify_seeded_docs_readable(env)
        yield env
        teardown_environment(env, CollectionMultiTestSuite, request.param)
