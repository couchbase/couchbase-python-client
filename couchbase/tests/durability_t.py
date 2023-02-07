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

import couchbase.subdocument as SD
from couchbase.durability import (ClientDurability,
                                  DurabilityLevel,
                                  PersistTo,
                                  PersistToExtended,
                                  ReplicateTo,
                                  ServerDurability)
from couchbase.exceptions import DocumentNotFoundException, DurabilityImpossibleException
from couchbase.options import (InsertOptions,
                               MutateInOptions,
                               RemoveOptions,
                               ReplaceOptions,
                               UpsertOptions)
from tests.environments import CollectionType
from tests.environments.test_environment import TestEnvironment
from tests.test_features import EnvironmentFeatures


class DurabilityTestSuite:

    TEST_MANIFEST = [
        'test_client_durable_insert',
        'test_client_durable_insert_fail',
        'test_client_durable_mutate_in',
        'test_client_durable_mutate_in_fail',
        'test_client_durable_remove',
        'test_client_durable_remove_fail',
        'test_client_durable_replace',
        'test_client_durable_replace_fail',
        'test_client_durable_upsert',
        'test_client_durable_upsert_fail',
        'test_client_durable_upsert_single_node',
        'test_client_persist_to_extended',
        'test_server_durable_insert',
        'test_server_durable_insert_single_node',
        'test_server_durable_mutate_in',
        'test_server_durable_mutate_in_single_node',
        'test_server_durable_remove',
        'test_server_durable_remove_single_node',
        'test_server_durable_replace',
        'test_server_durable_replace_single_node',
        'test_server_durable_upsert',
        'test_server_durable_upsert_single_node',
    ]

    @pytest.fixture(scope='class')
    def check_has_replicas(self, num_replicas):
        if num_replicas == 0:
            pytest.skip('No replicas to test durability.')

    @pytest.fixture(scope='class')
    def check_multi_node(self, num_nodes):
        if num_nodes == 1:
            pytest.skip('Test only for clusters with more than a single node.')

    @pytest.fixture(scope='class')
    def check_single_node(self, num_nodes):
        if num_nodes != 1:
            pytest.skip('Test only for clusters with a single node.')

    @pytest.fixture(scope='class')
    def check_sync_durability_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('sync_durability',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture(scope='class')
    def num_replicas(self, cb_env):
        bucket_settings = TestEnvironment.try_n_times(10, 1, cb_env.bm.get_bucket, cb_env.bucket.name)
        num_replicas = bucket_settings.get("num_replicas")
        return num_replicas

    @pytest.fixture(scope='class')
    def num_nodes(self, cb_env):
        return len(cb_env.cluster._cluster_info.nodes)

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_insert(self, cb_env, num_replicas):
        key, value = cb_env.get_new_doc()
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        cb_env.collection.insert(key, value, InsertOptions(durability=durability))
        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_insert_fail(self, cb_env, num_replicas):
        if num_replicas > 2:
            pytest.skip('Too many replicas enabled.')
        key, value = cb_env.get_new_doc()
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.insert(key, value, InsertOptions(durability=durability))

    # @TODO: why DurabilityImpossibleException not raised?
    # @pytest.mark.usefixtures('check_multi_node')
    # @pytest.mark.usefixtures('check_has_replicas')
    # def test_client_durable_insert_single_node(self, cb_env, num_replicas):
    #     key, value = cb_env.get_new_doc()

    #     durability = ClientDurability(
    #         persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))

    #     with pytest.raises(DurabilityImpossibleException):
    #         cb_env.collection.insert(key, value, InsertOptions(durability=durability))

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_mutate_in(self, cb_env, num_replicas):
        key, value = cb_env.get_existing_doc()
        value['make'] = 'New Make'
        value['model'] = 'New Model'
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        cb_env.collection.mutate_in(key,
                                    (SD.upsert('make', 'New Make'), SD.replace('model', 'New Model')),
                                    MutateInOptions(durability=durability))
        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_mutate_in_fail(self, cb_env, num_replicas):
        if num_replicas > 2:
            pytest.skip('Too many replicas enabled.')
        key = cb_env.get_existing_doc(key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.mutate_in(key, (SD.upsert('make', 'New Make'),), MutateInOptions(durability=durability))

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_remove(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc(key_only=True)
        durability = ClientDurability(persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        cb_env.collection.remove(key, RemoveOptions(durability=durability))
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get(key)

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_remove_fail(self, cb_env, num_replicas):
        if num_replicas > 2:
            pytest.skip("Too many replicas enabled.")
        key = cb_env.get_existing_doc(key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.remove(key, RemoveOptions(durability=durability))

    # @TODO: why DurabilityImpossibleException not raised?
    # @pytest.mark.usefixtures('check_multi_node')
    # @pytest.mark.usefixtures('check_has_replicas')
    # def test_client_durable_remove_single_node(self, cb_env, num_replicas):
    #     key = cb_env.get_existing_doc(key_only=True)

    #     durability = ClientDurability(
    #         persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))

    #     with pytest.raises(DurabilityImpossibleException):
    #         cb_env.collection.remove(key, RemoveOptions(durability=durability))

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_replace(self, cb_env, num_replicas):
        key, value = cb_env.get_existing_doc()
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        cb_env.collection.replace(key, value, ReplaceOptions(durability=durability))
        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_replace_fail(self, cb_env, num_replicas):
        if num_replicas > 2:
            pytest.skip("Too many replicas enabled.")
        key, value = cb_env.get_existing_doc()
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.replace(key, value, ReplaceOptions(durability=durability))

    # @TODO: why DurabilityImpossibleException not raised?
    # @pytest.mark.usefixtures('check_multi_node')
    # @pytest.mark.usefixtures('check_has_replicas')
    # def test_client_durable_replace_single_node(self, cb_env, num_replicas):
    #     key, value = cb_env.get_existing_doc()

    #     durability = ClientDurability(
    #         persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))

    #     with pytest.raises(DurabilityImpossibleException):
    #         cb_env.collection.replace(key, value, ReplaceOptions(durability=durability))

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_upsert(self, cb_env, num_replicas):
        key, value = cb_env.get_existing_doc()
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        cb_env.collection.upsert(key, value,
                                 UpsertOptions(durability=durability), timeout=timedelta(seconds=3))
        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_upsert_fail(self, cb_env, num_replicas):
        if num_replicas > 2:
            pytest.skip("Too many replicas enabled.")
        key, value = cb_env.get_existing_doc()
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.upsert(key, value, UpsertOptions(durability=durability))

    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_upsert_single_node(self, cb_env, num_replicas):
        key, value = cb_env.get_existing_doc()
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.upsert(key, value, UpsertOptions(durability=durability))

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    @pytest.mark.parametrize('persist_to', [PersistToExtended.NONE, PersistToExtended.ACTIVE, PersistToExtended.ONE])
    def test_client_persist_to_extended(self, cb_env, persist_to):
        key, value = cb_env.get_existing_doc()
        durability = ClientDurability(
            persist_to=persist_to, replicate_to=ReplicateTo.ONE)
        cb_env.collection.upsert(key, value, UpsertOptions(durability=durability))
        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_server_durable_insert(self, cb_env):
        key, value = cb_env.get_new_doc()
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        cb_env.collection.insert(key, value, InsertOptions(durability=durability))
        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_server_durable_insert_single_node(self, cb_env):
        key, value = cb_env.get_new_doc()
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.insert(key, value, InsertOptions(durability=durability))

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_server_durable_mutate_in(self, cb_env):
        key, value = cb_env.get_existing_doc()
        value['make'] = 'New Make'
        value['model'] = 'New Model'
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        cb_env.collection.mutate_in(key,
                                    (SD.upsert('make', 'New Make'), SD.replace('model', 'New Model')),
                                    MutateInOptions(durability=durability))
        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_server_durable_mutate_in_single_node(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.mutate_in(key, (SD.upsert('make', 'New Make'),), MutateInOptions(durability=durability))

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_server_durable_remove(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        cb_env.collection.remove(key, RemoveOptions(durability=durability))
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get(key)

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_server_durable_remove_single_node(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.remove(key, RemoveOptions(durability=durability))

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_server_durable_replace(self, cb_env):
        key, value = cb_env.get_existing_doc()
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        cb_env.collection.replace(key, value, ReplaceOptions(durability=durability))
        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_server_durable_replace_single_node(self, cb_env):
        key, value = cb_env.get_existing_doc()
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.replace(key, value, ReplaceOptions(durability=durability))

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_server_durable_upsert(self, cb_env):
        key, value = cb_env.get_existing_doc()
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        cb_env.collection.upsert(key, value, UpsertOptions(durability=durability))
        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_server_durable_upsert_single_node(self, cb_env):
        key, value = cb_env.get_existing_doc()
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.upsert(key, value, UpsertOptions(durability=durability))


class ClassicDurabilityTests(DurabilityTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicDurabilityTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicDurabilityTests) if valid_test_method(meth)]
        compare = set(DurabilityTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_base_env.enable_bucket_mgmt()
        cb_base_env.setup(request.param)

        yield cb_base_env

        cb_base_env.teardown(request.param)
