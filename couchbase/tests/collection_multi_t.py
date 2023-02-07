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
from tests.environments import CollectionType
from tests.environments.collection_multi_environment import CollectionMultiTestEnvironment
from tests.environments.test_environment import TestEnvironment
from tests.mock_server import MockServerType


class CollectionMultiTestSuite:

    TEST_MANIFEST = [
        'test_multi_exists_invalid_input',
        'test_multi_exists_not_exist',
        'test_multi_exists_simple',
        'test_multi_get_all_replicas_fail',
        'test_multi_get_all_replicas_invalid_input',
        'test_multi_get_all_replicas_simple',
        'test_multi_get_any_replica_fail',
        'test_multi_get_any_replica_invalid_input',
        'test_multi_get_any_replica_simple',
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
        'test_multi_touch_fail',
        'test_multi_touch_invalid_input',
        'test_multi_touch_simple',
        'test_multi_unlock_invalid_input',
        'test_multi_upsert_global_opts',
        'test_multi_upsert_invalid_input',
        'test_multi_upsert_key_opts',
        'test_multi_upsert_simple',
    ]

    @pytest.fixture(scope='class')
    def check_multi_node(self, num_nodes):
        if num_nodes == 1:
            pytest.skip('Test only for clusters with more than a single node.')

    @pytest.fixture(scope='class')
    def check_replicas(self, cb_env):
        if cb_env.is_mock_server and cb_env.mock_server_type == MockServerType.GoCAVES:
            pytest.skip('GoCaves inconstent w/ replicas')
        bucket_settings = TestEnvironment.try_n_times(10, 1, cb_env.bm.get_bucket, cb_env.bucket.name)
        num_replicas = bucket_settings.get('num_replicas')
        ping_res = cb_env.bucket.ping()
        kv_endpoints = ping_res.endpoints.get(ServiceType.KeyValue, None)
        if kv_endpoints is None or len(kv_endpoints) < (num_replicas + 1):
            pytest.skip('Not all replicas are online')

    @pytest.fixture(scope='class')
    def num_nodes(self, cb_env):
        return len(cb_env.cluster._cluster_info.nodes)

    def test_multi_exists_simple(self, cb_env):
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

    def test_multi_exists_not_exist(self, cb_env):
        keys_and_docs = cb_env.FAKE_DOCS
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

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_replicas")
    def test_multi_get_all_replicas_fail(self, cb_env):
        keys_and_docs = cb_env.FAKE_DOCS
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

    @pytest.mark.usefixtures("check_multi_node")
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

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_replicas")
    def test_multi_get_all_replicas_simple(self, cb_env):
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
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
    def test_multi_get_any_replica_fail(self, cb_env):
        keys_and_docs = cb_env.FAKE_DOCS
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
    def test_multi_get_any_replica_simple(self, cb_env):
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

    def test_multi_get_fail(self, cb_env):
        keys_and_docs = cb_env.FAKE_DOCS
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

    def test_multi_get_invalid_input(self, cb_env):
        keys_and_docs = {
            'test-key1': {'what': 'a test doc!', 'id': 'test-key1'},
            'test-key2': {'what': 'a test doc!', 'id': 'test-key2'},
            'test-key3': {'what': 'a test doc!', 'id': 'test-key3'},
            'test-key4': {'what': 'a test doc!', 'id': 'test-key4'}
        }
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.get_multi(keys_and_docs)

    def test_multi_get_simple(self, cb_env):
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

    def test_multi_insert_fail(self, cb_env):
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

    def test_multi_insert_global_opts(self, cb_env):
        keys_and_docs = cb_env.get_new_docs(4)
        opts = InsertMultiOptions(expiry=timedelta(seconds=2))
        res = cb_env.collection.insert_multi(keys_and_docs, opts)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        # lets verify they all expired...
        TestEnvironment.try_n_times(5, 3, cb_env.check_all_not_found, cb_env, list(keys_and_docs.keys()))

    def test_multi_insert_invalid_input(self, cb_env):
        keys = ['test-key1', 'test-key2', 'test-key3', 'test-key4']
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.insert_multi(keys)

    def test_multi_insert_key_opts(self, cb_env):
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
        # lets verify they all expired...
        TestEnvironment.try_n_times(5, 3, cb_env.check_all_not_found, cb_env, list(keys_and_docs.keys()), okay_key=key1)

    def test_multi_insert_simple(self, cb_env):
        keys_and_docs = cb_env.get_new_docs(4)
        res = cb_env.collection.insert_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    def test_multi_lock_and_unlock_simple(self, cb_env):
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
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

    def test_multi_remove_fail(self, cb_env):
        keys_and_docs = cb_env.FAKE_DOCS
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

    def test_multi_remove_invalid_input(self, cb_env):
        keys_and_docs = {
            'test-key1': {'what': 'a test doc!', 'id': 'test-key1'},
            'test-key2': {'what': 'a test doc!', 'id': 'test-key2'},
            'test-key3': {'what': 'a test doc!', 'id': 'test-key3'},
            'test-key4': {'what': 'a test doc!', 'id': 'test-key4'}
        }
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.remove_multi(keys_and_docs)

    def test_multi_remove_simple(self, cb_env):
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
        res = cb_env.collection.remove_multi(keys)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        # lets verify they all expired...
        TestEnvironment.try_n_times(5, 3, cb_env.check_all_not_found, cb_env, list(keys_and_docs.keys()))

    def test_multi_replace_fail(self, cb_env):
        keys_and_docs = cb_env.FAKE_DOCS
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

    def test_multi_replace_global_opts(self, cb_env):
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
        # lets verify they all expired...
        TestEnvironment.try_n_times(5, 3, cb_env.check_all_not_found, cb_env, list(keys_and_docs.keys()))

    def test_multi_replace_invalid_input(self, cb_env):
        keys = ['test-key1', 'test-key2', 'test-key3', 'test-key4']
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.replace_multi(keys)

    def test_multi_replace_key_opts(self, cb_env):
        keys_and_docs = cb_env.get_docs(4)
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
        TestEnvironment.try_n_times(5, 3, cb_env.check_all_not_found, cb_env, list(keys_and_docs.keys()), okay_key=key1)

    def test_multi_replace_simple(self, cb_env):
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

    def test_multi_touch_fail(self, cb_env):
        keys_and_docs = cb_env.FAKE_DOCS
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

    def test_multi_touch_invalid_input(self, cb_env):
        keys_and_docs = {
            'test-key1': {'what': 'a test doc!', 'id': 'test-key1'},
            'test-key2': {'what': 'a test doc!', 'id': 'test-key2'},
            'test-key3': {'what': 'a test doc!', 'id': 'test-key3'},
            'test-key4': {'what': 'a test doc!', 'id': 'test-key4'}
        }
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.touch_multi(keys_and_docs, timedelta(seconds=2))

    def test_multi_touch_simple(self, cb_env):
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
        res = cb_env.collection.touch_multi(keys, timedelta(seconds=2))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        # lets verify they all expired...
        TestEnvironment.try_n_times(5, 3, cb_env.check_all_not_found, cb_env, list(keys_and_docs.keys()))

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

    def test_multi_upsert_global_opts(self, cb_env):
        keys_and_docs = cb_env.get_docs(4)
        opts = UpsertMultiOptions(expiry=timedelta(seconds=2))
        res = cb_env.collection.upsert_multi(keys_and_docs, opts)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        # lets verify they all expired...
        TestEnvironment.try_n_times(5, 3, cb_env.check_all_not_found, cb_env, list(keys_and_docs.keys()))

    def test_multi_upsert_invalid_input(self, cb_env):
        keys = ['test-key1', 'test-key2', 'test-key3', 'test-key4']
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.upsert_multi(keys)

    def test_multi_upsert_key_opts(self, cb_env):
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

        # lets verify they all expired...
        TestEnvironment.try_n_times(5, 3, cb_env.check_all_not_found, cb_env, list(keys_and_docs.keys()), okay_key=key1)

    def test_multi_upsert_simple(self, cb_env):
        keys_and_docs = cb_env.get_docs(4)
        res = cb_env.collection.upsert_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True


class ClassicCollectionMultiTests(CollectionMultiTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicCollectionMultiTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicCollectionMultiTests) if valid_test_method(meth)]
        compare = set(CollectionMultiTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = CollectionMultiTestEnvironment.from_environment(cb_base_env)
        cb_env.enable_bucket_mgmt()
        cb_env.setup(request.param)

        yield cb_env

        cb_env.teardown(request.param)
