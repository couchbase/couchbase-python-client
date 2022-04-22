from datetime import timedelta

import pytest

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import (CouchbaseException,
                                  DocumentExistsException,
                                  DocumentNotFoundException,
                                  InvalidArgumentException)
from couchbase.options import (ClusterOptions,
                               GetMultiOptions,
                               InsertMultiOptions,
                               InsertOptions,
                               ReplaceMultiOptions,
                               TouchMultiOptions,
                               UpsertMultiOptions,
                               UpsertOptions)
from couchbase.result import (ExistsResult,
                              GetResult,
                              MultiExistsResult,
                              MultiGetResult,
                              MultiMutationResult,
                              MutationResult)

from ._test_utils import CollectionType, TestEnvironment


class CollectionMultiTests:

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, couchbase_config, request):
        conn_string = couchbase_config.get_connection_string()
        opts = ClusterOptions(PasswordAuthenticator(
            couchbase_config.admin_username, couchbase_config.admin_password))
        cluster = Cluster(conn_string, opts)
        cluster.cluster_info()
        bucket = cluster.bucket(f"{couchbase_config.bucket_name}")
        coll = bucket.default_collection()
        if request.param == CollectionType.DEFAULT:
            cb_env = TestEnvironment(cluster, bucket, coll, couchbase_config, manage_buckets=True)
        elif request.param == CollectionType.NAMED:
            cb_env = TestEnvironment(cluster, bucket, coll, couchbase_config,
                                     manage_buckets=True, manage_collections=True)
            cb_env.setup_named_collections()

        cb_env.load_data()
        yield cb_env
        cb_env.purge_data()
        if request.param == CollectionType.NAMED:
            cb_env.teardown_named_collections()
        cluster.close()

    @pytest.fixture(name='kds')
    def get_keys_and_docs(self, cb_env):
        keys_and_docs = {
            'test-key1': {'what': 'a test doc!', 'id': 'test-key1'},
            'test-key2': {'what': 'a test doc!', 'id': 'test-key2'},
            'test-key3': {'what': 'a test doc!', 'id': 'test-key3'},
            'test-key4': {'what': 'a test doc!', 'id': 'test-key4'}
        }
        yield keys_and_docs
        cb_env.collection.remove_multi(list(keys_and_docs.keys()))

    @pytest.fixture(name='fake_kds')
    def get_fake_keys_and_docs(self, cb_env):
        keys_and_docs = {
            'not-a-key1': {'what': 'a fake test doc!', 'id': 'not-a-key1'},
            'not-a-key2': {'what': 'a fake test doc!', 'id': 'not-a-key2'},
            'not-a-key3': {'what': 'a fake test doc!', 'id': 'not-a-key3'},
            'not-a-key4': {'what': 'a fake test doc!', 'id': 'not-a-key4'}
        }
        yield keys_and_docs
        cb_env.collection.remove_multi(list(keys_and_docs.keys()))

    def test_multi_exists_simple(self, cb_env, kds):
        keys_and_docs = kds
        res = cb_env.collection.upsert_multi(keys_and_docs)
        keys = list(keys_and_docs.keys())
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
        res = cb_env.collection.get_multi(keys)
        assert isinstance(res, MultiGetResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, GetResult), res.results.values())) is True
        for k in keys:
            r = cb_env.collection.get(k)
            assert r.content_as[dict] == keys_and_docs[k]

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
        cb_env.sleep(3.0)
        for k in keys_and_docs.keys():
            with pytest.raises(DocumentNotFoundException):
                cb_env.collection.get(k)

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
        cb_env.sleep(3.0)
        for k in keys_and_docs.keys():
            if k == key1:
                r = cb_env.collection.get(k)
                assert isinstance(r, GetResult)
            else:
                with pytest.raises(DocumentNotFoundException):
                    cb_env.collection.get(k)

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
        cb_env.sleep(3.0)
        for k in keys_and_docs.keys():
            with pytest.raises(DocumentNotFoundException):
                cb_env.collection.get(k)

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
        cb_env.sleep(3.0)
        for k in keys_and_docs.keys():
            if k == key1:
                r = cb_env.collection.get(k)
                assert isinstance(r, GetResult)
            else:
                with pytest.raises(DocumentNotFoundException):
                    cb_env.collection.get(k)

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
        cb_env.sleep(3.0)
        for k in keys_and_docs.keys():
            with pytest.raises(DocumentNotFoundException):
                cb_env.collection.get(k)

    def test_multi_replace_key_opts(self, cb_env, kds):
        keys_and_docs = kds
        res = cb_env.collection.upsert_multi(keys_and_docs)
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
        cb_env.sleep(3.0)
        for k in keys_and_docs.keys():
            if k == key1:
                r = cb_env.collection.get(k)
                assert isinstance(r, GetResult)
            else:
                with pytest.raises(DocumentNotFoundException):
                    cb_env.collection.get(k)

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
        res = cb_env.collection.remove_multi(keys)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        for k in keys:
            with pytest.raises(DocumentNotFoundException):
                cb_env.collection.get(k)

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
        res = cb_env.collection.touch_multi(keys, timedelta(seconds=2))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        # let the docs expire...
        cb_env.sleep(3.0)
        for k in keys:
            with pytest.raises(DocumentNotFoundException):
                cb_env.collection.get(k)

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
