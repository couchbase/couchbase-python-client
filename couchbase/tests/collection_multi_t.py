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

from datetime import timedelta

import pytest

from couchbase.diagnostics import ServiceType
from couchbase.exceptions import (CouchbaseException,
                                  DocumentExistsException,
                                  DocumentNotFoundException,
                                  DocumentUnretrievableException,
                                  InvalidArgumentException)
from couchbase.options import (GetAnyReplicaMultiOptions,
                               GetMultiOptions,
                               InsertMultiOptions,
                               InsertOptions,
                               ReplaceMultiOptions,
                               TouchMultiOptions,
                               UpsertMultiOptions,
                               UpsertOptions)
from couchbase.result import (ExistsResult,
                              GetReplicaResult,
                              GetResult,
                              MultiExistsResult,
                              MultiGetReplicaResult,
                              MultiGetResult,
                              MultiMutationResult,
                              MutationResult)

from ._test_utils import CollectionType, TestEnvironment


class CollectionMultiTests:

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, couchbase_config, request):
        cb_env = TestEnvironment.get_environment(__name__, couchbase_config, request.param, manage_buckets=True)

        if request.param == CollectionType.NAMED:
            cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        yield cb_env
        if request.param == CollectionType.NAMED:
            cb_env.try_n_times_till_exception(5, 3,
                                              cb_env.teardown_named_collections,
                                              raise_if_no_exception=False)

    @pytest.fixture(name='kds')
    def get_keys_and_docs(self, cb_env):
        keys_and_docs = {
            'test-key1': {'what': 'a test doc!', 'id': 'test-key1'},
            'test-key2': {'what': 'a test doc!', 'id': 'test-key2'},
            'test-key3': {'what': 'a test doc!', 'id': 'test-key3'},
            'test-key4': {'what': 'a test doc!', 'id': 'test-key4'}
        }
        yield keys_and_docs
        keys = list(keys_and_docs.keys())
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove_multi,
                                          keys,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3,
                                          return_exceptions=False)

    @pytest.fixture(name='fake_kds')
    def get_fake_keys_and_docs(self, cb_env):
        keys_and_docs = {
            'not-a-key1': {'what': 'a fake test doc!', 'id': 'not-a-key1'},
            'not-a-key2': {'what': 'a fake test doc!', 'id': 'not-a-key2'},
            'not-a-key3': {'what': 'a fake test doc!', 'id': 'not-a-key3'},
            'not-a-key4': {'what': 'a fake test doc!', 'id': 'not-a-key4'}
        }
        yield keys_and_docs
        keys = list(keys_and_docs.keys())
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove_multi,
                                          keys,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3,
                                          return_exceptions=False)

    @pytest.fixture(scope="class")
    def check_replicas(self, cb_env):
        bucket_settings = cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, cb_env.bucket.name)
        num_replicas = bucket_settings.get("num_replicas")
        ping_res = cb_env.bucket.ping()
        kv_endpoints = ping_res.endpoints.get(ServiceType.KeyValue, None)
        if kv_endpoints is None or len(kv_endpoints) < (num_replicas + 1):
            pytest.skip("Not all replicas are online")

    def _make_sure_docs_exists(self, cb_env, keys):
        found = 0
        for k in keys:
            doc = cb_env.try_n_times(10, 3, cb_env.collection.get, k)
            if isinstance(doc, GetResult):
                found += 1

        if len(keys) != found:
            raise Exception('Unable to find all docs.')

    def _check_all_not_found(self, cb_env, keys, okay_key=None):
        not_found = 0
        for k in keys:
            try:
                cb_env.collection.get(k)
                if okay_key and k == okay_key:
                    not_found += 1  # this is okay, it shouldn't have expired
            except DocumentNotFoundException:
                not_found += 1

        if not_found != len(keys):
            raise Exception('Not all docs were expired')

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_multi_exists_simple(self, cb_env, kds):
        keys_and_docs = kds
        res = cb_env.collection.upsert_multi(keys_and_docs)
        keys = list(keys_and_docs.keys())
        cb_env.try_n_times(5, 3, self._make_sure_docs_exists, cb_env, keys)
        res = cb_env.collection.exists_multi(keys)
        assert isinstance(res, MultiExistsResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, ExistsResult), res.results.values())) is True
        for r in res.results.values():
            assert r.exists is True

    def test_multi_exists_not_exist(self, cb_env, fake_kds):
        keys_and_docs = fake_kds
        keys = list(keys_and_docs.keys())
        res = cb_env.collection.exists_multi(keys)
        assert isinstance(res, MultiExistsResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, ExistsResult), res.results.values())) is True
        for r in res.results.values():
            assert r.exists is False

    def test_multi_exists_invalid_input(self, cb_env):
        keys_and_docs = {
            'test-key1': {'what': 'a test doc!', 'id': 'test-key1'},
            'test-key2': {'what': 'a test doc!', 'id': 'test-key2'},
            'test-key3': {'what': 'a test doc!', 'id': 'test-key3'},
            'test-key4': {'what': 'a test doc!', 'id': 'test-key4'}
        }
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.exists_multi(keys_and_docs)

    def test_multi_get_simple(self, cb_env, kds):
        keys_and_docs = kds
        res = cb_env.collection.upsert_multi(keys_and_docs)
        keys = list(keys_and_docs.keys())
        cb_env.try_n_times(5, 3, self._make_sure_docs_exists, cb_env, keys)
        res = cb_env.collection.get_multi(keys)
        assert isinstance(res, MultiGetResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, GetResult), res.results.values())) is True
        for k, v in res.results.items():
            assert v.content_as[dict] == keys_and_docs[k]

    def test_multi_get_invalid_input(self, cb_env):
        keys_and_docs = {
            'test-key1': {'what': 'a test doc!', 'id': 'test-key1'},
            'test-key2': {'what': 'a test doc!', 'id': 'test-key2'},
            'test-key3': {'what': 'a test doc!', 'id': 'test-key3'},
            'test-key4': {'what': 'a test doc!', 'id': 'test-key4'}
        }
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.get_multi(keys_and_docs)

    def test_multi_get_fail(self, cb_env, fake_kds):
        keys_and_docs = fake_kds
        keys = list(keys_and_docs.keys())
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

    @pytest.mark.usefixtures("check_replicas")
    def test_multi_get_any_replica_simple(self, cb_env, kds):
        keys_and_docs = kds
        res = cb_env.collection.upsert_multi(keys_and_docs)
        keys = list(keys_and_docs.keys())
        cb_env.try_n_times(5, 3, self._make_sure_docs_exists, cb_env, keys)
        res = cb_env.collection.get_any_replica_multi(keys)
        assert isinstance(res, MultiGetReplicaResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, GetReplicaResult), res.results.values())) is True
        for k, v in res.results.items():
            assert isinstance(v.is_replica, bool)
            assert v.content_as[dict] == keys_and_docs[k]

    @pytest.mark.usefixtures("check_replicas")
    def test_multi_get_any_replica_invalid_input(self, cb_env):
        keys_and_docs = {
            'test-key1': {'what': 'a test doc!', 'id': 'test-key1'},
            'test-key2': {'what': 'a test doc!', 'id': 'test-key2'},
            'test-key3': {'what': 'a test doc!', 'id': 'test-key3'},
            'test-key4': {'what': 'a test doc!', 'id': 'test-key4'}
        }
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.get_any_replica_multi(keys_and_docs)

    @pytest.mark.usefixtures("check_replicas")
    def test_multi_get_any_replica_fail(self, cb_env, fake_kds):
        keys_and_docs = fake_kds
        keys = list(keys_and_docs.keys())
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

    @pytest.mark.usefixtures("check_replicas")
    def test_multi_get_all_replicas_simple(self, cb_env, kds):
        keys_and_docs = kds
        res = cb_env.collection.upsert_multi(keys_and_docs)
        keys = list(keys_and_docs.keys())
        # sleep a bit so that we give replicas a chance to exist...
        cb_env.sleep(5)
        cb_env.try_n_times(5, 3, self._make_sure_docs_exists, cb_env, keys)
        res = cb_env.collection.get_all_replicas_multi(keys)
        assert isinstance(res, MultiGetReplicaResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, list), res.results.values())) is True
        for k, v in res.results.items():
            for replica in v:
                assert isinstance(replica, GetReplicaResult)
                assert isinstance(replica.is_replica, bool)
                assert replica.content_as[dict] == keys_and_docs[k]

    @pytest.mark.usefixtures("check_replicas")
    def test_multi_get_all_replicas_invalid_input(self, cb_env):
        keys_and_docs = {
            'test-key1': {'what': 'a test doc!', 'id': 'test-key1'},
            'test-key2': {'what': 'a test doc!', 'id': 'test-key2'},
            'test-key3': {'what': 'a test doc!', 'id': 'test-key3'},
            'test-key4': {'what': 'a test doc!', 'id': 'test-key4'}
        }
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.get_all_replicas_multi(keys_and_docs)

    @pytest.mark.usefixtures("check_replicas")
    def test_multi_get_all_replicas_fail(self, cb_env, fake_kds):
        keys_and_docs = fake_kds
        keys = list(keys_and_docs.keys())
        res = cb_env.collection.get_all_replicas_multi(keys)
        assert isinstance(res, MultiGetReplicaResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), CouchbaseException), res.exceptions.values())) is True

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get_all_replicas_multi(keys, return_exceptions=False)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get_all_replicas_multi(keys, GetAnyReplicaMultiOptions(return_exceptions=False))

    def test_multi_upsert_simple(self, cb_env, kds):
        keys_and_docs = kds
        res = cb_env.collection.upsert_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    def test_multi_upsert_global_opts(self, cb_env, kds):
        keys_and_docs = kds
        opts = UpsertMultiOptions(expiry=timedelta(seconds=2))
        res = cb_env.collection.upsert_multi(keys_and_docs, opts)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        # lets verify they all expired...
        cb_env.try_n_times(5, 3, self._check_all_not_found, cb_env, list(keys_and_docs.keys()))

    def test_multi_upsert_key_opts(self, cb_env, kds):
        keys_and_docs = kds
        key1 = list(keys_and_docs.keys())[0]
        opts = UpsertMultiOptions(expiry=timedelta(seconds=2), per_key_options={
                                  key1: UpsertOptions(expiry=timedelta(seconds=0))})
        res = cb_env.collection.upsert_multi(keys_and_docs, opts)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

        # lets verify they all expired...
        cb_env.try_n_times(5, 3, self._check_all_not_found, cb_env, list(keys_and_docs.keys()), okay_key=key1)

    def test_multi_upsert_invalid_input(self, cb_env):
        keys = ['test-key1', 'test-key2', 'test-key3', 'test-key4']
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.upsert_multi(keys)

    def test_multi_insert_simple(self, cb_env, kds):
        keys_and_docs = kds
        res = cb_env.collection.insert_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    def test_multi_insert_global_opts(self, cb_env, kds):
        keys_and_docs = kds
        opts = InsertMultiOptions(expiry=timedelta(seconds=2))
        res = cb_env.collection.insert_multi(keys_and_docs, opts)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        # lets verify they all expired...
        cb_env.try_n_times(5, 3, self._check_all_not_found, cb_env, list(keys_and_docs.keys()))

    def test_multi_insert_key_opts(self, cb_env, kds):
        keys_and_docs = kds
        key1 = list(keys_and_docs.keys())[0]
        opts = InsertMultiOptions(expiry=timedelta(seconds=2), per_key_options={
                                  key1: InsertOptions(expiry=timedelta(seconds=0))})
        res = cb_env.collection.insert_multi(keys_and_docs, opts)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        # lets verify they all expired...
        cb_env.try_n_times(5, 3, self._check_all_not_found, cb_env, list(keys_and_docs.keys()), okay_key=key1)

    def test_multi_insert_invalid_input(self, cb_env):
        keys = ['test-key1', 'test-key2', 'test-key3', 'test-key4']
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.insert_multi(keys)

    def test_multi_insert_fail(self, cb_env, kds):
        keys_and_docs = kds
        res = cb_env.collection.upsert_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

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

    def test_multi_replace_simple(self, cb_env, kds):
        keys_and_docs = kds
        res = cb_env.collection.upsert_multi(keys_and_docs)
        cb_env.try_n_times(5, 3, self._make_sure_docs_exists, cb_env, list(keys_and_docs.keys()))
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

    def test_multi_replace_global_opts(self, cb_env, kds):
        keys_and_docs = kds
        res = cb_env.collection.upsert_multi(keys_and_docs)
        cb_env.try_n_times(5, 3, self._make_sure_docs_exists, cb_env, list(keys_and_docs.keys()))
        opts = ReplaceMultiOptions(expiry=timedelta(seconds=2))
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        res = cb_env.collection.replace_multi(keys_and_docs, opts)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        # lets verify they all expired...
        cb_env.try_n_times(5, 3, self._check_all_not_found, cb_env, list(keys_and_docs.keys()))

    def test_multi_replace_key_opts(self, cb_env, kds):
        keys_and_docs = kds
        res = cb_env.collection.upsert_multi(keys_and_docs)
        cb_env.try_n_times(5, 3, self._make_sure_docs_exists, cb_env, list(keys_and_docs.keys()))
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        key1 = list(keys_and_docs.keys())[0]
        opts = ReplaceMultiOptions(expiry=timedelta(seconds=2), per_key_options={
                                   key1: UpsertOptions(expiry=timedelta(seconds=0))})
        res = cb_env.collection.replace_multi(keys_and_docs, opts)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        # lets verify they all expired...
        cb_env.try_n_times(5, 3, self._check_all_not_found, cb_env, list(keys_and_docs.keys()), okay_key=key1)

    def test_multi_replace_invalid_input(self, cb_env):
        keys = ['test-key1', 'test-key2', 'test-key3', 'test-key4']
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.replace_multi(keys)

    def test_multi_replace_fail(self, cb_env, fake_kds):
        keys_and_docs = fake_kds
        res = cb_env.collection.replace_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), CouchbaseException), res.exceptions.values())) is True

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.replace_multi(keys_and_docs, return_exceptions=False)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.replace_multi(keys_and_docs, ReplaceMultiOptions(return_exceptions=False))

    def test_multi_remove_simple(self, cb_env, kds):
        keys_and_docs = kds
        res = cb_env.collection.upsert_multi(keys_and_docs)
        keys = list(keys_and_docs.keys())
        cb_env.try_n_times(5, 3, self._make_sure_docs_exists, cb_env, keys)
        res = cb_env.collection.remove_multi(keys)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        # lets verify they all expired...
        cb_env.try_n_times(5, 3, self._check_all_not_found, cb_env, list(keys_and_docs.keys()))

    def test_multi_remove_invalid_input(self, cb_env):
        keys_and_docs = {
            'test-key1': {'what': 'a test doc!', 'id': 'test-key1'},
            'test-key2': {'what': 'a test doc!', 'id': 'test-key2'},
            'test-key3': {'what': 'a test doc!', 'id': 'test-key3'},
            'test-key4': {'what': 'a test doc!', 'id': 'test-key4'}
        }
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.remove_multi(keys_and_docs)

    def test_multi_remove_fail(self, cb_env, fake_kds):
        keys_and_docs = fake_kds
        keys = list(keys_and_docs.keys())
        res = cb_env.collection.remove_multi(keys)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), CouchbaseException), res.exceptions.values())) is True

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.remove_multi(keys, return_exceptions=False)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.remove_multi(keys, ReplaceMultiOptions(return_exceptions=False))

    def test_multi_touch_simple(self, cb_env, kds):
        keys_and_docs = kds
        res = cb_env.collection.upsert_multi(keys_and_docs)
        keys = list(keys_and_docs.keys())
        cb_env.try_n_times(5, 3, self._make_sure_docs_exists, cb_env, keys)
        res = cb_env.collection.touch_multi(keys, timedelta(seconds=2))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        # lets verify they all expired...
        cb_env.try_n_times(5, 3, self._check_all_not_found, cb_env, list(keys_and_docs.keys()))

    def test_multi_touch_invalid_input(self, cb_env):
        keys_and_docs = {
            'test-key1': {'what': 'a test doc!', 'id': 'test-key1'},
            'test-key2': {'what': 'a test doc!', 'id': 'test-key2'},
            'test-key3': {'what': 'a test doc!', 'id': 'test-key3'},
            'test-key4': {'what': 'a test doc!', 'id': 'test-key4'}
        }
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.touch_multi(keys_and_docs, timedelta(seconds=2))

    def test_multi_touch_fail(self, cb_env, fake_kds):
        keys_and_docs = fake_kds
        keys = list(keys_and_docs.keys())
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

    def test_multi_lock_and_unlock_simple(self, cb_env, kds):
        keys_and_docs = kds
        res = cb_env.collection.upsert_multi(keys_and_docs)
        keys = list(keys_and_docs.keys())
        cb_env.try_n_times(5, 3, self._make_sure_docs_exists, cb_env, keys)
        res = cb_env.collection.lock_multi(keys, timedelta(seconds=5))
        assert isinstance(res, MultiGetResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, GetResult), res.results.values())) is True

        res = cb_env.collection.unlock_multi(res)
        assert isinstance(res, dict)

    def test_multi_lock_invalid_input(self, cb_env):
        keys_and_docs = {
            'test-key1': {'what': 'a test doc!', 'id': 'test-key1'},
            'test-key2': {'what': 'a test doc!', 'id': 'test-key2'},
            'test-key3': {'what': 'a test doc!', 'id': 'test-key3'},
            'test-key4': {'what': 'a test doc!', 'id': 'test-key4'}
        }
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.lock_multi(keys_and_docs, timedelta(seconds=5))

    def test_multi_unlock_invalid_input(self, cb_env):
        keys_and_docs = {
            'test-key1': {'what': 'a test doc!', 'id': 'test-key1'},
            'test-key2': {'what': 'a test doc!', 'id': 'test-key2'},
            'test-key3': {'what': 'a test doc!', 'id': 'test-key3'},
            'test-key4': {'what': 'a test doc!', 'id': 'test-key4'}
        }
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.unlock_multi(keys_and_docs)

        with pytest.raises(InvalidArgumentException):
            cb_env.collection.unlock_multi(list(keys_and_docs.keys()))
