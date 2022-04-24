import pytest
import pytest_asyncio

import couchbase.subdocument as SD
from acouchbase.cluster import Cluster, get_event_loop
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import DocumentNotFoundException
from couchbase.options import ClusterOptions

from ._test_utils import (CollectionType,
                          KVPair,
                          TestEnvironment)


class MutationTokensEnabledTests:

    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest_asyncio.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    async def couchbase_test_environment(self, couchbase_config, request):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        opts = ClusterOptions(PasswordAuthenticator(username, pw))
        c = Cluster(
            conn_string, opts)
        await c.on_connect()
        await c.cluster_info()
        b = c.bucket(f"{couchbase_config.bucket_name}")
        await b.on_connect()

        coll = b.default_collection()
        if request.param == CollectionType.DEFAULT:
            cb_env = TestEnvironment(c, b, coll, couchbase_config, manage_buckets=True)
        elif request.param == CollectionType.NAMED:
            cb_env = TestEnvironment(c, b, coll, couchbase_config, manage_buckets=True, manage_collections=True)
            await cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        yield cb_env
        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times_till_exception(5, 3,
                                                    cb_env.teardown_named_collections,
                                                    raise_if_no_exception=False)
        await c.close()

    @pytest_asyncio.fixture(name="new_kvp")
    async def new_key_and_value_with_reset(self, cb_env) -> KVPair:
        key, value = await cb_env.get_new_key_value()
        yield KVPair(key, value)
        await cb_env.try_n_times_till_exception(10,
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

    @pytest.mark.asyncio
    async def test_mutation_tokens_upsert(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        result = await cb.upsert(key, value)
        self.verify_mutation_tokens(cb_env.bucket.name, result)

    @pytest.mark.asyncio
    async def test_mutation_tokens_insert(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        result = await cb.insert(key, value)
        self.verify_mutation_tokens(cb_env.bucket.name, result)

    @pytest.mark.asyncio
    async def test_mutation_tokens_replace(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        await cb.upsert(key, value)
        result = await cb.replace(key, value)
        self.verify_mutation_tokens(cb_env.bucket.name, result)

    @pytest.mark.asyncio
    async def test_mutation_tokens_remove(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        await cb.upsert(key, value)
        result = await cb.remove(key)
        self.verify_mutation_tokens(cb_env.bucket.name, result)

    # @TODO: c++ client does not provide mutation token for touch
    # @pytest.mark.asyncio
    # async def test_mutation_tokens_touch(self, cb_env, new_kvp):
    #     cb = cb_env.collection
    #     key = new_kvp.key
    #     value = new_kvp.value
    #     await cb.upsert(key, value)
    #     result = await cb.touch(key, timedelta(seconds=3))
    #     self.verify_mutation_tokens(cb_env.bucket.name, result)

    @pytest.mark.asyncio
    async def test_mutation_tokens_mutate_in(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key

        async def cas_matches(key, cas):
            result = await cb.get(key)
            if result.cas == cas:
                return result
            raise Exception("nope")

        res = await cb.upsert(key, {"a": "aaa", "b": {"c": {"d": "yo!"}}})
        cas = res.cas
        await cb_env.try_n_times(10, 3, cas_matches, key, cas)
        result = await cb.mutate_in(key, (SD.upsert("c", "ccc"), SD.replace("b", "XXX"),))
        self.verify_mutation_tokens(cb_env.bucket.name, result)


# @TODO: need to update client settings first
class MutationTokensDisabledTests:
    pass
