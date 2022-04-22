import pytest

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import (ClusterOptions,
                               DecrementMultiOptions,
                               DecrementOptions,
                               IncrementMultiOptions,
                               IncrementOptions,
                               SignedInt64)
from couchbase.result import (CounterResult,
                              MultiCounterResult,
                              MultiMutationResult,
                              MutationResult)
from couchbase.transcoder import RawBinaryTranscoder, RawStringTranscoder

from ._test_utils import CollectionType, TestEnvironment


class BinaryCollectionMultiTests:

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, couchbase_config, request):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        opts = ClusterOptions(PasswordAuthenticator(username, pw))
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

        yield cb_env

        if request.param == CollectionType.NAMED:
            cb_env.teardown_named_collections()
        cluster.close()

    @pytest.fixture(name='utf8_keys')
    def get_utf8_keys(self, cb_env):
        keys = ['test-key1', 'test-key2', 'test-key3', 'test-key4']

        tc = RawStringTranscoder()
        for k in keys:
            cb_env.collection.upsert(k, '', transcoder=tc)
            cb_env.try_n_times(10, 1, cb_env.collection.get, k, transcoder=tc)

        yield keys
        cb_env.collection.remove_multi(keys)

    @pytest.fixture(name='byte_keys')
    def get_byte_keys(self, cb_env):
        keys = ['test-key1', 'test-key2', 'test-key3', 'test-key4']

        tc = RawBinaryTranscoder()
        for k in keys:
            cb_env.collection.upsert(k, b'', transcoder=tc)
            cb_env.try_n_times(10, 1, cb_env.collection.get, k, transcoder=tc)

        yield keys
        cb_env.collection.remove_multi(keys)

    @pytest.fixture(name='counter_keys')
    def get_counter_keys(self, cb_env):
        keys = ['test-key1', 'test-key2', 'test-key3', 'test-key4']
        yield keys
        cb_env.collection.remove_multi(keys)

    def test_append_multi_string(self, cb_env, utf8_keys):
        keys = utf8_keys
        values = ['foo', 'bar', 'baz', 'qux']
        keys_and_docs = dict(zip(keys, values))
        res = cb_env.collection.binary().append_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    def test_prepend_multi_string(self, cb_env, utf8_keys):
        keys = utf8_keys
        values = ['foo', 'bar', 'baz', 'qux']
        keys_and_docs = dict(zip(keys, values))
        res = cb_env.collection.binary().prepend_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    def test_append_multi_bytes(self, cb_env, byte_keys):
        keys = byte_keys
        values = [b'foo', b'bar', b'baz', b'qux']
        keys_and_docs = dict(zip(keys, values))
        res = cb_env.collection.binary().append_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    def test_prepend_multi_bytes(self, cb_env, byte_keys):
        keys = byte_keys
        values = [b'foo', b'bar', b'baz', b'qux']
        keys_and_docs = dict(zip(keys, values))
        res = cb_env.collection.binary().prepend_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    def test_counter_multi_increment(self, cb_env, counter_keys):
        res = cb_env.collection.binary().increment_multi(counter_keys)
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True

    def test_counter_multi_decrement(self, cb_env, counter_keys):
        res = cb_env.collection.binary().decrement_multi(counter_keys)
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True

    def test_counter_multi_increment_non_default(self, cb_env, counter_keys):
        res = cb_env.collection.binary().increment_multi(counter_keys, IncrementMultiOptions(initial=SignedInt64(3)))
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True
        for r in res.results.values():
            assert r.content == 3

    def test_counter_multi_decrement_non_default(self, cb_env, counter_keys):
        res = cb_env.collection.binary().decrement_multi(counter_keys, DecrementMultiOptions(initial=SignedInt64(3)))
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True
        for r in res.results.values():
            assert r.content == 3

    def test_counter_multi_increment_non_default_per_key(self, cb_env, counter_keys):
        key1 = counter_keys[0]
        opts = IncrementMultiOptions(initial=SignedInt64(3), per_key_options={
                                     key1: IncrementOptions(initial=SignedInt64(100))})
        res = cb_env.collection.binary().increment_multi(counter_keys, opts)
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True
        for k, v in res.results.items():
            if k == key1:
                assert v.content == 100
            else:
                assert v.content == 3

    def test_counter_multi_decrement_non_default_per_key(self, cb_env, counter_keys):
        key1 = counter_keys[0]
        opts = DecrementMultiOptions(initial=SignedInt64(3), per_key_options={
                                     key1: DecrementOptions(initial=SignedInt64(100))})
        res = cb_env.collection.binary().decrement_multi(counter_keys, opts)
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True
        for k, v in res.results.items():
            if k == key1:
                assert v.content == 100
            else:
                assert v.content == 3
