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

import couchbase.subdocument as SD
from acouchbase.cluster import get_event_loop
from couchbase.exceptions import DocumentNotFoundException

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
        cb_env = await TestEnvironment.get_environment(__name__,
                                                       couchbase_config,
                                                       request.param,
                                                       manage_buckets=True)
        if request.param == CollectionType.NAMED:
            if cb_env.is_mock_server:
                pytest.skip('Jenkins + GoCAVES not playing nice...')
            await cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        yield cb_env
        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times_till_exception(5, 3,
                                                    cb_env.teardown_named_collections,
                                                    raise_if_no_exception=False)

    @pytest_asyncio.fixture(name="new_kvp")
    async def new_key_and_value_with_reset(self, cb_env) -> KVPair:
        key, value = await cb_env.get_new_key_value()
        yield KVPair(key, value)
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

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
        result = await cb_env.try_n_times(5, 3, cb.upsert, key, value)
        self.verify_mutation_tokens(cb_env.bucket.name, result)

    @pytest.mark.asyncio
    async def test_mutation_tokens_insert(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        result = await cb_env.try_n_times(5, 3, cb.insert, key, value)
        self.verify_mutation_tokens(cb_env.bucket.name, result)

    @pytest.mark.asyncio
    async def test_mutation_tokens_replace(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        await cb_env.try_n_times(5, 3, cb.upsert, key, value)
        result = await cb_env.try_n_times(5, 3, cb.replace, key, value)
        self.verify_mutation_tokens(cb_env.bucket.name, result)

    @pytest.mark.asyncio
    async def test_mutation_tokens_remove(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        await cb_env.try_n_times(5, 3, cb.upsert, key, value)
        result = await cb_env.try_n_times(5, 3, cb.remove, key)
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

        res = await cb_env.try_n_times(5, 3, cb.upsert, key, {"a": "aaa", "b": {"c": {"d": "yo!"}}})
        cas = res.cas
        await cb_env.try_n_times(10, 3, cas_matches, key, cas)
        result = await cb_env.try_n_times(5, 3, cb.mutate_in, key, (SD.upsert("c", "ccc"), SD.replace("b", "XXX"),))
        self.verify_mutation_tokens(cb_env.bucket.name, result)


# @TODO: need to update client settings first
class MutationTokensDisabledTests:
    pass
