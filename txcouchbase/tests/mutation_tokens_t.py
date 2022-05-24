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

import couchbase.subdocument as SD
from couchbase.exceptions import DocumentNotFoundException

from ._test_utils import (CollectionType,
                          KVPair,
                          TestEnvironment,
                          run_in_reactor_thread)


class MutationTokensEnabledTests:

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, couchbase_config, request):
        cb_env = TestEnvironment.get_environment(__name__, couchbase_config, request.param)

        if request.param == CollectionType.NAMED:
            cb_env.try_n_times(5, 3, cb_env.setup_named_collections, is_deferred=False)

        yield cb_env
        if request.param == CollectionType.NAMED:
            cb_env.try_n_times_till_exception(5, 3,
                                              cb_env.teardown_named_collections,
                                              raise_if_no_exception=False,
                                              is_deferred=False)

    @pytest.fixture(name="new_kvp")
    def new_key_and_value_with_reset(self, cb_env) -> KVPair:
        key, value = cb_env.get_new_key_value()
        yield KVPair(key, value)
        cb_env.try_n_times_till_exception(10,
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

    def test_mutation_tokens_upsert(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        result = cb_env.try_n_times(5, 3, cb.upsert, key, value)
        self.verify_mutation_tokens(cb_env.bucket.name, result)

    def test_mutation_tokens_insert(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        result = cb_env.try_n_times(5, 3, cb.insert, key, value)
        self.verify_mutation_tokens(cb_env.bucket.name, result)

    def test_mutation_tokens_replace(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        cb_env.try_n_times(5, 3, cb.upsert, key, value)
        result = cb_env.try_n_times(5, 3, cb.replace, key, value)
        self.verify_mutation_tokens(cb_env.bucket.name, result)

    def test_mutation_tokens_remove(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        cb_env.try_n_times(5, 3, cb.upsert, key, value)
        result = cb_env.try_n_times(5, 3, cb.remove, key)
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
            result = run_in_reactor_thread(cb.get, key)
            if result.cas == cas:
                return result
            raise Exception("nope")

        res = cb_env.try_n_times(5, 3, cb.upsert, key, {"a": "aaa", "b": {"c": {"d": "yo!"}}})
        cas = res.cas
        cb_env.try_n_times(10, 3, cas_matches, key, cas, is_deferred=False)
        result = cb_env.try_n_times(5, 3, cb.mutate_in, key, (SD.upsert("c", "ccc"), SD.replace("b", "XXX"),))
        self.verify_mutation_tokens(cb_env.bucket.name, result)


# @TODO: need to update client settings first
class MutationTokensDisabledTests:
    pass
