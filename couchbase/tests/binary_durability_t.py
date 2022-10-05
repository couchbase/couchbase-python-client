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

from couchbase.durability import (ClientDurability,
                                  DurabilityLevel,
                                  PersistTo,
                                  PersistToExtended,
                                  ReplicateTo,
                                  ServerDurability)
from couchbase.exceptions import DocumentNotFoundException, DurabilityImpossibleException
from couchbase.options import (AppendOptions,
                               DecrementOptions,
                               IncrementOptions,
                               PrependOptions)
from couchbase.transcoder import RawStringTranscoder

from ._test_utils import (CollectionType,
                          KVPair,
                          TestEnvironment)


class DurabilityTests:
    NO_KEY = "not-a-key"
    TEST_BUCKET = 'test-bucket'

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

    @pytest.fixture(name='utf8_empty_kvp')
    def utf8_key_and_empty_value(self, cb_env) -> KVPair:
        key, value = cb_env.try_n_times(5, 3, cb_env.load_utf8_binary_data)
        yield KVPair(key, value)
        cb_env.collection.upsert(key, '', transcoder=RawStringTranscoder())

    @pytest.fixture(name='counter_kvp')
    def counter_key_and_value(self, cb_env) -> KVPair:
        key, value = cb_env.try_n_times(5, 3, cb_env.load_counter_binary_data, start_value=100)
        yield KVPair(key, value)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,))

    @pytest.fixture(scope="class")
    def check_sync_durability_supported(self, cb_env):
        cb_env.check_if_feature_supported('sync_durability')

    @pytest.fixture(scope="class")
    def num_nodes(self, cb_env):
        return len(cb_env.cluster._cluster_info.nodes)

    @pytest.fixture(scope="class")
    def check_multi_node(self, num_nodes):
        if num_nodes == 1:
            pytest.skip("Test only for clusters with more than a single node.")

    @pytest.fixture(scope="class")
    def check_single_node(self, num_nodes):
        if num_nodes != 1:
            pytest.skip("Test only for clusters with a single node.")

    @pytest.fixture(scope="class")
    def num_replicas(self, cb_env):
        bucket_settings = cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, cb_env.bucket.name)
        num_replicas = bucket_settings.get("num_replicas")
        return num_replicas

    @pytest.fixture(scope="class")
    def check_has_replicas(self, num_replicas):
        if num_replicas == 0:
            pytest.skip("No replicas to test durability.")

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_server_durable_append(self, cb_env, utf8_empty_kvp):
        cb = cb_env.collection
        key = utf8_empty_kvp.key
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)

        cb.binary().append(key, 'foo',  AppendOptions(durability=durability))
        result = cb.get(key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == 'foo'

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_server_durable_append_single_node(self, cb_env, utf8_empty_kvp):
        cb = cb_env.collection
        key = utf8_empty_kvp.key
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)

        with pytest.raises(DurabilityImpossibleException):
            cb.binary().append(key, 'foo',  AppendOptions(durability=durability))

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_server_durable_prepend(self, cb_env, utf8_empty_kvp):
        cb = cb_env.collection
        key = utf8_empty_kvp.key
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)

        cb.binary().append(key, 'foo',  AppendOptions(durability=durability))
        result = cb.get(key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == 'foo'

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_server_durable_prepend_single_node(self, cb_env, utf8_empty_kvp):
        cb = cb_env.collection
        key = utf8_empty_kvp.key
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)

        with pytest.raises(DurabilityImpossibleException):
            cb.binary().prepend(key, 'foo',  PrependOptions(durability=durability))

    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_client_durable_append(self, cb_env, utf8_empty_kvp, num_replicas):
        cb = cb_env.collection
        key = utf8_empty_kvp.key

        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))

        cb.binary().append(key, 'foo',  AppendOptions(durability=durability))
        result = cb.get(key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == 'foo'

    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_client_durable_append_fail(self, cb_env, utf8_empty_kvp, num_replicas):
        cb = cb_env.collection
        key = utf8_empty_kvp.key

        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))

        with pytest.raises(DurabilityImpossibleException):
            cb.binary().append(key, 'foo',  AppendOptions(durability=durability))

    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_client_durable_append_single_node(self, cb_env, utf8_empty_kvp, num_replicas):
        cb = cb_env.collection
        key = utf8_empty_kvp.key

        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))

        with pytest.raises(DurabilityImpossibleException):
            cb.binary().append(key, 'foo',  AppendOptions(durability=durability))

    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_client_durable_prepend(self, cb_env, utf8_empty_kvp, num_replicas):
        cb = cb_env.collection
        key = utf8_empty_kvp.key

        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))

        cb.binary().prepend(key, 'foo',  PrependOptions(durability=durability))
        result = cb.get(key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == 'foo'

    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_client_durable_prepend_fail(self, cb_env, utf8_empty_kvp, num_replicas):
        cb = cb_env.collection
        key = utf8_empty_kvp.key

        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))

        with pytest.raises(DurabilityImpossibleException):
            cb.binary().prepend(key, 'foo',  PrependOptions(durability=durability))

    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_client_durable_prepend_single_node(self, cb_env, utf8_empty_kvp, num_replicas):
        cb = cb_env.collection
        key = utf8_empty_kvp.key

        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))

        with pytest.raises(DurabilityImpossibleException):
            cb.binary().prepend(key, 'foo',  PrependOptions(durability=durability))

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_server_durable_increment(self, cb_env, counter_kvp):
        cb = cb_env.collection
        key = counter_kvp.key
        value = counter_kvp.value

        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        result = cb.binary().increment(key, IncrementOptions(durability=durability))
        assert result.content == value + 1

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_server_durable_increment_single_node(self, cb_env, counter_kvp):
        cb = cb_env.collection
        key = counter_kvp.key

        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        with pytest.raises(DurabilityImpossibleException):
            cb.binary().increment(key, IncrementOptions(durability=durability))

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_server_durable_decrement(self, cb_env, counter_kvp):
        cb = cb_env.collection
        key = counter_kvp.key
        value = counter_kvp.value

        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)

        result = cb.binary().decrement(key, DecrementOptions(durability=durability))
        assert result.content == value - 1

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_server_durable_decrement_single_node(self, cb_env, counter_kvp):
        cb = cb_env.collection
        key = counter_kvp.key

        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        with pytest.raises(DurabilityImpossibleException):
            cb.binary().decrement(key, DecrementOptions(durability=durability))

    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_client_durable_increment(self, cb_env, counter_kvp, num_replicas):
        cb = cb_env.collection
        key = counter_kvp.key
        value = counter_kvp.value

        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))

        result = cb.binary().increment(key, IncrementOptions(durability=durability))
        assert result.content == value + 1

    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_client_durable_increment_fail(self, cb_env, counter_kvp, num_replicas):
        cb = cb_env.collection
        key = counter_kvp.key

        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))

        with pytest.raises(DurabilityImpossibleException):
            cb.binary().increment(key, IncrementOptions(durability=durability))

    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_client_durable_increment_single_node(self, cb_env, counter_kvp, num_replicas):
        cb = cb_env.collection
        key = counter_kvp.key

        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))

        with pytest.raises(DurabilityImpossibleException):
            cb.binary().increment(key, IncrementOptions(durability=durability))

    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_client_durable_decrement(self, cb_env, counter_kvp, num_replicas):
        cb = cb_env.collection
        key = counter_kvp.key
        value = counter_kvp.value

        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))

        result = cb.binary().decrement(key, DecrementOptions(durability=durability))
        assert result.content == value - 1

    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_client_durable_decrement_fail(self, cb_env, counter_kvp, num_replicas):
        cb = cb_env.collection
        key = counter_kvp.key

        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))

        with pytest.raises(DurabilityImpossibleException):
            cb.binary().decrement(key, DecrementOptions(durability=durability))

    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures("check_has_replicas")
    def test_client_durable_decrement_single_node(self, cb_env, counter_kvp, num_replicas):
        cb = cb_env.collection
        key = counter_kvp.key

        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))

        with pytest.raises(DurabilityImpossibleException):
            cb.binary().decrement(key, DecrementOptions(durability=durability))
