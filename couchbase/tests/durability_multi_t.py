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

import pytest

from couchbase.diagnostics import ServiceType
from couchbase.durability import (ClientDurability,
                                  DurabilityLevel,
                                  PersistTo,
                                  PersistToExtended,
                                  ReplicateTo,
                                  ServerDurability)
from couchbase.exceptions import DurabilityImpossibleException
from couchbase.options import (InsertMultiOptions,
                               RemoveMultiOptions,
                               ReplaceMultiOptions,
                               UpsertMultiOptions)
from couchbase.result import MultiMutationResult, MutationResult
from tests.environments import CollectionType
from tests.environments.collection_multi_environment import CollectionMultiTestEnvironment
from tests.environments.test_environment import TestEnvironment
from tests.mock_server import MockServerType
from tests.test_features import EnvironmentFeatures


class DurabilityTestSuite:

    TEST_MANIFEST = [
        'test_client_durable_insert_multi',
        'test_client_durable_insert_multi_fail',
        'test_client_durable_remove_multi',
        'test_client_durable_remove_multi_fail',
        'test_client_durable_replace_multi',
        'test_client_durable_replace_multi_fail',
        'test_client_durable_upsert_multi',
        'test_client_durable_upsert_multi_fail',
        'test_server_durable_insert_multi',
        'test_server_durable_insert_multi_single_node',
        'test_server_durable_remove_multi',
        'test_server_durable_remove_multi_single_node',
        'test_server_durable_replace_multi',
        'test_server_durable_replace_multi_single_node',
        'test_server_durable_upsert_multi',
        'test_server_durable_upsert_multi_single_node',
    ]

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
    @pytest.mark.usefixtures('check_replicas')
    def test_client_durable_insert_multi(self, cb_env, num_replicas):
        keys_and_docs = cb_env.get_new_docs(4)
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        res = cb_env.collection.insert_multi(keys_and_docs, InsertMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas')
    def test_client_durable_insert_multi_fail(self, cb_env, num_replicas):
        if num_replicas > 2:
            pytest.skip('Too many replicas enabled.')
        keys_and_docs = cb_env.get_new_docs(4)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        res = cb_env.collection.insert_multi(keys_and_docs, InsertMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), DurabilityImpossibleException), res.exceptions.values())) is True

        keys_and_docs = cb_env.get_new_docs(4)
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.insert_multi(keys_and_docs, durability=durability, return_exceptions=False)

        keys_and_docs = cb_env.get_new_docs(4)
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.insert_multi(keys_and_docs, InsertMultiOptions(durability=durability,
                                                                             return_exceptions=False))

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas')
    def test_client_durable_remove_multi(self, cb_env, num_replicas):
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
        durability = ClientDurability(persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        res = cb_env.collection.remove_multi(keys, RemoveMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        # lets verify all docs were removed...
        TestEnvironment.try_n_times(5, 3, cb_env.check_all_not_found, cb_env, list(keys_and_docs.keys()))

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas')
    def test_client_durable_remove_multi_fail(self, cb_env, num_replicas):
        if num_replicas > 2:
            pytest.skip("Too many replicas enabled.")
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        res = cb_env.collection.remove_multi(keys, RemoveMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), DurabilityImpossibleException), res.exceptions.values())) is True

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas')
    def test_client_durable_replace_multi(self, cb_env, num_replicas):
        keys_and_docs = cb_env.get_docs(4)
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        res = cb_env.collection.replace_multi(keys_and_docs, ReplaceMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas')
    def test_client_durable_replace_multi_fail(self, cb_env, num_replicas):
        if num_replicas > 2:
            pytest.skip("Too many replicas enabled.")
        keys_and_docs = cb_env.get_docs(4)
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        res = cb_env.collection.replace_multi(keys_and_docs, ReplaceMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), DurabilityImpossibleException), res.exceptions.values())) is True

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas')
    def test_client_durable_upsert_multi(self, cb_env, num_replicas):
        keys_and_docs = cb_env.get_docs(4)
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        res = cb_env.collection.upsert_multi(keys_and_docs, UpsertMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas')
    def test_client_durable_upsert_multi_fail(self, cb_env, num_replicas):
        if num_replicas > 2:
            pytest.skip("Too many replicas enabled.")
        keys_and_docs = cb_env.get_docs(4)
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        res = cb_env.collection.upsert_multi(keys_and_docs, UpsertMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), DurabilityImpossibleException), res.exceptions.values())) is True

        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.upsert_multi(keys_and_docs, durability=durability, return_exceptions=False)

        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.upsert_multi(keys_and_docs, UpsertMultiOptions(durability=durability,
                                                                             return_exceptions=False))

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas')
    def test_server_durable_insert_multi(self, cb_env):
        keys_and_docs = cb_env.get_new_docs(4)
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        res = cb_env.collection.insert_multi(keys_and_docs, InsertMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_replicas')
    def test_server_durable_insert_multi_single_node(self, cb_env):
        keys_and_docs = cb_env.get_new_docs(4)
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        res = cb_env.collection.insert_multi(keys_and_docs, InsertMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), DurabilityImpossibleException), res.exceptions.values())) is True

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas')
    def test_server_durable_remove_multi(self, cb_env):
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        res = cb_env.collection.remove_multi(keys, RemoveMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        # lets verify all docs were removed...
        TestEnvironment.try_n_times(5, 3, cb_env.check_all_not_found, cb_env, list(keys_and_docs.keys()))

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_replicas')
    def test_server_durable_remove_multi_single_node(self, cb_env):
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        res = cb_env.collection.remove_multi(keys, RemoveMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), DurabilityImpossibleException), res.exceptions.values())) is True

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas')
    def test_server_durable_replace_multi(self, cb_env):
        keys_and_docs = cb_env.get_docs(4)
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        res = cb_env.collection.replace_multi(keys_and_docs, ReplaceMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_replicas')
    def test_server_durable_replace_multi_single_node(self, cb_env):
        keys_and_docs = cb_env.get_docs(4)
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        res = cb_env.collection.replace_multi(keys_and_docs, ReplaceMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), DurabilityImpossibleException), res.exceptions.values())) is True

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas')
    def test_server_durable_upsert_multi(self, cb_env):
        keys_and_docs = cb_env.get_docs(4)
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        res = cb_env.collection.upsert_multi(keys_and_docs, UpsertMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_replicas')
    def test_server_durable_upsert_multi_single_node(self, cb_env):
        keys_and_docs = cb_env.get_docs(4)
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        res = cb_env.collection.upsert_multi(keys_and_docs, UpsertMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), DurabilityImpossibleException), res.exceptions.values())) is True


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

        cb_env = CollectionMultiTestEnvironment.from_environment(cb_base_env)
        cb_env.enable_bucket_mgmt()
        cb_env.setup(request.param)

        yield cb_env

        cb_env.teardown(request.param)
