import pytest

import couchbase.subdocument as SD
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import DocumentNotFoundException
from couchbase.options import ClusterOptions

from ._test_utils import (CollectionType,
                          KVPair,
                          TestEnvironment)


class MutationTokensEnabledTests:

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, couchbase_config, request):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        opts = ClusterOptions(PasswordAuthenticator(username, pw))
        cluster = Cluster(
            conn_string, opts)
        cluster.cluster_info()
        bucket = cluster.bucket(f"{couchbase_config.bucket_name}")

        coll = bucket.default_collection()
        if request.param == CollectionType.DEFAULT:
            cb_env = TestEnvironment(cluster, bucket, coll, couchbase_config, manage_buckets=True)
        elif request.param == CollectionType.NAMED:
            cb_env = TestEnvironment(cluster, bucket, coll, couchbase_config,
                                     manage_buckets=True, manage_collections=True)
            cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        yield cb_env
        if request.param == CollectionType.NAMED:
            cb_env.try_n_times_till_exception(5, 3,
                                              cb_env.teardown_named_collections,
                                              raise_if_no_exception=False)
        cluster.close()

    @pytest.fixture(name="new_kvp")
    def new_key_and_value_with_reset(self, cb_env) -> KVPair:
        key, value = cb_env.get_new_key_value()
        yield KVPair(key, value)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,))

    def verify_mutation_tokens(self, bucket_name, result):
        mutation_token = result.mutation_token()
        assert mutation_token is not None
        partition_id, partition_uuid, sequence_number, mt_bucket_name = mutation_token.as_tuple()
        assert isinstance(partition_id, int)
        assert isinstance(partition_uuid, int)
        assert isinstance(sequence_number, int)
        assert bucket_name == mt_bucket_name

    def test_mutation_tokens_upsert(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        result = cb.upsert(key, value)
        self.verify_mutation_tokens(cb_env.bucket.name, result)

    def test_mutation_tokens_insert(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        result = cb.insert(key, value)
        self.verify_mutation_tokens(cb_env.bucket.name, result)

    def test_mutation_tokens_replace(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        cb.upsert(key, value)
        result = cb.replace(key, value)
        self.verify_mutation_tokens(cb_env.bucket.name, result)

    def test_mutation_tokens_remove(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        cb.upsert(key, value)
        result = cb.remove(key)
        self.verify_mutation_tokens(cb_env.bucket.name, result)

    # @TODO: c++ client does not provide mutation token for touch
    # def test_mutation_tokens_touch(self, cb_env, new_kvp):
    #     cb = cb_env.collection
    #     key = new_kvp.key
    #     value = new_kvp.value
    #     cb.upsert(key, value)
    #     result = cb.touch(key, timedelta(seconds=3))
    #     self.verify_mutation_tokens(cb_env.bucket.name, result)

    def test_mutation_tokens_mutate_in(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key

        def cas_matches(key, cas):
            result = cb.get(key)
            if result.cas == cas:
                return result
            raise Exception("nope")

        res = cb.upsert(key, {"a": "aaa", "b": {"c": {"d": "yo!"}}})
        cas = res.cas
        cb_env.try_n_times(10, 3, cas_matches, key, cas)
        result = cb.mutate_in(key, (SD.upsert("c", "ccc"), SD.replace("b", "XXX"),))
        self.verify_mutation_tokens(cb_env.bucket.name, result)


# @TODO: need to update client settings first
class MutationTokensDisabledTests:
    pass
