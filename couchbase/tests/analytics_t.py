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

from datetime import datetime, timedelta

import pytest

from couchbase.analytics import (AnalyticsMetaData,
                                 AnalyticsMetrics,
                                 AnalyticsQuery,
                                 AnalyticsScanConsistency,
                                 AnalyticsStatus,
                                 AnalyticsWarning)
from couchbase.exceptions import DatasetNotFoundException, DataverseNotFoundException
from couchbase.options import AnalyticsOptions, UnsignedInt64
from couchbase.result import AnalyticsResult

from ._test_utils import TestEnvironment


class AnalyticsTests:

    DATASET_NAME = 'test-dataset'

    @pytest.fixture(scope="class", name="cb_env")
    def couchbase_test_environment(self, couchbase_config):
        cb_env = TestEnvironment.get_environment(__name__, couchbase_config, manage_analytics=True)

        # setup
        cb_env.try_n_times(3, 5, cb_env.load_data)
        cb_env.try_n_times(3, 5, self._setup_analytics, cb_env)

        yield cb_env
        # teardown
        cb_env.try_n_times(3, 5, self._teardown_analytics, cb_env)

    def _setup_analytics(self, cb_env):
        cb_env.try_n_times(10, 3,
                           cb_env.am.create_dataset,
                           self.DATASET_NAME,
                           cb_env.bucket.name,
                           ignore_if_exists=True)
        cb_env.am.connect_link()

        q_str = f'SELECT COUNT(1) AS doc_count FROM `{self.DATASET_NAME}`;'

        for _ in range(10):
            res = cb_env.cluster.analytics_query(q_str)
            rows = [r for r in res.rows()]
            if len(rows) > 0 and rows[0].get('doc_count', 0) > 10:
                break
            print(f'Found {len(rows)} records, waiting a bit...')
            cb_env.sleep(5)

    def _teardown_analytics(self, cb_env):
        cb_env.try_n_times(3, 5, cb_env.purge_data)
        cb_env.am.disconnect_link()
        cb_env.try_n_times(10, 3,
                           cb_env.am.drop_dataset,
                           self.DATASET_NAME,
                           ignore_if_not_exists=True)

    def assert_rows(self,
                    result,  # type: AnalyticsResult
                    expected_count):
        count = 0
        assert isinstance(result, AnalyticsResult)
        for row in result.rows():
            assert row is not None
            count += 1
        assert count >= expected_count

    def test_simple_query(self, cb_env):
        result = cb_env.cluster.analytics_query(f"SELECT * FROM `{self.DATASET_NAME}` LIMIT 1")
        self.assert_rows(result, 1)

    def test_query_positional_params(self, cb_env):
        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{self.DATASET_NAME}` WHERE `type` = $1 LIMIT 1',
                                                AnalyticsOptions(positional_parameters=["airline"]))
        self.assert_rows(result, 1)

    def test_query_positional_params_no_option(self, cb_env):
        result = cb_env.cluster.analytics_query(
            f'SELECT * FROM `{self.DATASET_NAME}` WHERE `type` = $1 LIMIT 1', 'airline')
        self.assert_rows(result, 1)

    def test_query_positional_params_override(self, cb_env):
        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{self.DATASET_NAME}` WHERE `type` = $1 LIMIT 1',
                                                AnalyticsOptions(positional_parameters=['abcdefg']), 'airline')
        self.assert_rows(result, 1)

    def test_query_named_parameters(self, cb_env):
        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{self.DATASET_NAME}` WHERE `type` = $atype LIMIT 1',
                                                AnalyticsOptions(named_parameters={'atype': 'airline'}))
        self.assert_rows(result, 1)

    def test_query_named_parameters_no_options(self, cb_env):
        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{self.DATASET_NAME}` WHERE `type` = $atype LIMIT 1',
                                                atype='airline')
        self.assert_rows(result, 1)

    def test_query_named_parameters_override(self, cb_env):
        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{self.DATASET_NAME}` WHERE `type` = $atype LIMIT 1',
                                                AnalyticsOptions(named_parameters={'atype': 'abcdefg'}),
                                                atype='airline')
        self.assert_rows(result, 1)

    def test_query_raw_options(self, cb_env):
        # via raw, we should be able to pass any option
        # if using named params, need to match full name param in query
        # which is different for when we pass in name_parameters via their specific
        # query option (i.e. include the $ when using raw)
        result = cb_env.cluster.analytics_query(f"SELECT * FROM `{self.DATASET_NAME}` WHERE `type` = $atype LIMIT $1",
                                                AnalyticsOptions(raw={"$atype": "airline", "args": [1]}))
        self.assert_rows(result, 1)

        result = cb_env.cluster.analytics_query(f"SELECT * FROM `{self.DATASET_NAME}` WHERE `type` = $1 LIMIT 1",
                                                AnalyticsOptions(raw={"args": ['airline']}))
        self.assert_rows(result, 1)

    def test_analytics_with_metrics(self, cb_env):
        initial = datetime.now()
        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{self.DATASET_NAME}` LIMIT 1')
        self.assert_rows(result, 1)
        taken = datetime.now() - initial
        metadata = result.metadata()  # type: AnalyticsMetaData
        metrics = metadata.metrics()
        assert isinstance(metrics, AnalyticsMetrics)
        assert isinstance(metrics.elapsed_time(), timedelta)
        assert metrics.elapsed_time() < taken
        assert metrics.elapsed_time() > timedelta(milliseconds=0)
        assert isinstance(metrics.execution_time(), timedelta)
        assert metrics.execution_time() < taken
        assert metrics.execution_time() > timedelta(milliseconds=0)

        expected_counts = {metrics.error_count: 0,
                           metrics.result_count: 1,
                           metrics.warning_count: 0}
        for method, expected in expected_counts.items():
            count_result = method()
            fail_msg = "{} failed".format(method)
            assert isinstance(count_result, UnsignedInt64), fail_msg
            assert count_result == UnsignedInt64(expected), fail_msg
        assert metrics.result_size() > UnsignedInt64(0)
        assert isinstance(metrics.processed_objects(), UnsignedInt64)
        assert metrics.error_count() == UnsignedInt64(0)

    def test_analytics_metadata(self, cb_env):
        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{self.DATASET_NAME}` LIMIT 2')
        self.assert_rows(result, 2)
        metadata = result.metadata()  # type: AnalyticsMetaData
        isinstance(metadata, AnalyticsMetaData)
        for id_meth in (metadata.client_context_id, metadata.request_id):
            id_res = id_meth()
            fail_msg = "{} failed".format(id_meth)
            assert isinstance(id_res, str), fail_msg
        assert metadata.status() == AnalyticsStatus.SUCCESS
        assert isinstance(metadata.signature(), (str, dict))
        assert isinstance(metadata.warnings(), (list))
        for warning in metadata.warnings():
            assert isinstance(warning, AnalyticsWarning)
            assert isinstance(warning.message(), str)
            assert isinstance(warning.code(), int)


class AnalyticsCollectionTests:

    DATASET_NAME = 'test-dataset'

    @pytest.fixture(scope="class", name="cb_env")
    def couchbase_test_environment(self, couchbase_config):
        cb_env = TestEnvironment.get_environment(__name__,
                                                 couchbase_config,
                                                 manage_analytics=True,
                                                 manage_collections=True)

        cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        # setup
        cb_env.try_n_times(3, 5, cb_env.load_data)
        self.create_analytics_collections(cb_env)

        yield cb_env

        # teardown
        cb_env.try_n_times(3, 5, cb_env.purge_data)
        self.teardown_analytics_collections(cb_env)
        cb_env.try_n_times_till_exception(5, 3,
                                          cb_env.teardown_named_collections,
                                          raise_if_no_exception=False)

    """
        Setup queries:
            Create dataverse:
                CREATE DATAVERSE `default`.`test-scope` IF NOT EXISTS;

            Create dataset:
                USE `default`.`test-scope`;
                CREATE DATASET IF NOT EXISTS `test-collection` ON `default`.`test-scope`.`test-collection`;

            Connect Link:
                USE `default`.`test-scope`; CONNECT LINK Local;
    """

    def create_analytics_collections(self, cb_env):

        dv_fqdn = f'`{cb_env.bucket.name}`.`{cb_env.scope.name}`'
        q_str = f'CREATE DATAVERSE {dv_fqdn} IF NOT EXISTS;'
        res = cb_env.cluster.analytics_query(q_str)
        [_ for _ in res.rows()]

        q_str = f'USE {dv_fqdn}; CREATE DATASET IF NOT EXISTS `{cb_env.collection.name}` ON {cb_env.fqdn}'
        res = cb_env.cluster.analytics_query(q_str)
        [_ for _ in res.rows()]

        q_str = f'USE {dv_fqdn}; CONNECT LINK Local;'
        res = cb_env.cluster.analytics_query(q_str)
        [_ for _ in res.rows()]

        context_fqdn = f'default:`{cb_env.bucket.name}`.`{cb_env.scope.name}`'
        q_str = f'SELECT COUNT(1) AS doc_count FROM `{cb_env.collection.name}`;'
        for _ in range(10):
            res = cb_env.cluster.analytics_query(q_str, query_context=context_fqdn)
            rows = [r for r in res.rows()]
            if len(rows) > 0 and rows[0].get('doc_count', 0) > 10:
                break
            print(f'Found {len(rows)} records, waiting a bit...')
            cb_env.sleep(5)

    """
        Tear-down queries:
            Disconnect Link:
                USE `default`.`test-scope`; DISCONNECT LINK Local;

            Droo dataset:
                USE `default`.`test-scope`; DROP DATASET `test-collection` IF EXISTS;

            Drop dataverse:
                DROP DATAVERSE `default`.`test-scope` IF EXISTS;
    """

    def teardown_analytics_collections(self, cb_env):
        dv_fqdn = f'`{cb_env.bucket.name}`.`{cb_env.scope.name}`'
        q_str = f'USE {dv_fqdn}; DISCONNECT LINK Local;'
        res = cb_env.cluster.analytics_query(q_str)
        [_ for _ in res.rows()]

        q_str = f'USE {dv_fqdn}; DROP DATASET `{cb_env.collection.name}` IF EXISTS;'
        res = cb_env.cluster.analytics_query(q_str)
        [_ for _ in res.rows()]

        q_str = f'DROP DATAVERSE {dv_fqdn} IF EXISTS;'
        res = cb_env.cluster.analytics_query(q_str)
        [_ for _ in res.rows()]

    def assert_rows(self,
                    result,  # type: AnalyticsResult
                    expected_count):
        count = 0
        assert isinstance(result, AnalyticsResult)
        for row in result.rows():
            assert row is not None
            count += 1
        assert count >= expected_count

    def test_query_fully_qualified(self, cb_env):
        result = cb_env.cluster.analytics_query(f'SELECT * FROM {cb_env.fqdn} LIMIT 2')
        self.assert_rows(result, 2)

    def test_cluster_query_context(self, cb_env):
        q_context = f'default:`{cb_env.bucket.name}`.`{cb_env.scope.name}`'
        # test with QueryOptions
        a_opts = AnalyticsOptions(query_context=q_context)
        result = cb_env.cluster.analytics_query(
            f'SELECT * FROM `{cb_env.collection.name}` LIMIT 2', a_opts)
        self.assert_rows(result, 2)

        # test with kwargs
        result = cb_env.cluster.analytics_query(
            f'SELECT * FROM `{cb_env.collection.name}` LIMIT 2', query_context=q_context)
        self.assert_rows(result, 2)

    def test_bad_query_context(self, cb_env):
        # test w/ no context
        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{cb_env.collection.name} `LIMIT 2')
        # @TODO: DatasetNotFoundException
        with pytest.raises(DatasetNotFoundException):
            self.assert_rows(result, 2)

        # test w/ bad scope
        q_context = f'default:`{cb_env.bucket.name}`.`fake-scope`'
        result = cb_env.cluster.analytics_query(
            f'SELECT * FROM `{cb_env.collection.name}` LIMIT 2', AnalyticsOptions(query_context=q_context))
        # @TODO: DataverseNotFoundException
        with pytest.raises(DataverseNotFoundException):
            self.assert_rows(result, 2)

    def test_scope_query(self, cb_env):
        result = cb_env.scope.analytics_query(f'SELECT * FROM `{cb_env.collection.name}` LIMIT 2')
        self.assert_rows(result, 2)

    def test_scope_query_fqdn(self, cb_env):
        # @TODO:  look into this...
        if cb_env.server_version_short >= 7.1:
            pytest.skip("Analytics scope query format not allowed on server versions >= 7.1")

        result = cb_env.scope.analytics_query(f'SELECT * FROM {cb_env.fqdn} LIMIT 2', query_context='')
        self.assert_rows(result, 2)

    def test_bad_scope_query(self, cb_env):
        q_context = f'default:`{cb_env.bucket.name}`.`fake-scope`'
        result = cb_env.scope.analytics_query(f'SELECT * FROM {cb_env.fqdn} LIMIT 2',
                                              AnalyticsOptions(query_context=q_context))
        with pytest.raises(DataverseNotFoundException):
            self.assert_rows(result, 2)

        q_context = f'default:`fake-bucket`.`{cb_env.scope.name}`'
        result = cb_env.scope.analytics_query(f'SELECT * FROM {cb_env.fqdn} LIMIT 2',
                                              query_context=q_context)
        with pytest.raises(DataverseNotFoundException):
            self.assert_rows(result, 2)

    def test_scope_query_with_positional_params_in_options(self, cb_env):
        result = cb_env.scope.analytics_query(f'SELECT * FROM `{cb_env.collection.name}` WHERE country LIKE $1 LIMIT 1',
                                              AnalyticsOptions(positional_parameters=['United%']))
        self.assert_rows(result, 1)

    def test_scope_query_with_named_params_in_options(self, cb_env):
        q_str = f'SELECT * FROM `{cb_env.collection.name}` WHERE country LIKE $country LIMIT 1'
        result = cb_env.scope.analytics_query(q_str,
                                              AnalyticsOptions(named_parameters={'country': 'United%'}))
        self.assert_rows(result, 1)

    def test_analytics_with_metrics(self, cb_env):
        initial = datetime.now()
        result = cb_env.scope.analytics_query(f'SELECT * FROM `{cb_env.collection.name}` LIMIT 1')
        self.assert_rows(result, 1)
        taken = datetime.now() - initial
        metadata = result.metadata()  # type: AnalyticsMetaData
        metrics = metadata.metrics()
        assert isinstance(metrics, AnalyticsMetrics)
        assert isinstance(metrics.elapsed_time(), timedelta)
        assert metrics.elapsed_time() < taken
        assert metrics.elapsed_time() > timedelta(milliseconds=0)
        assert isinstance(metrics.execution_time(), timedelta)
        assert metrics.execution_time() < taken
        assert metrics.execution_time() > timedelta(milliseconds=0)

        expected_counts = {metrics.error_count: 0,
                           metrics.result_count: 1,
                           metrics.warning_count: 0}
        for method, expected in expected_counts.items():
            count_result = method()
            fail_msg = "{} failed".format(method)
            assert isinstance(count_result, UnsignedInt64), fail_msg
            assert count_result == UnsignedInt64(expected), fail_msg
        assert metrics.result_size() > UnsignedInt64(0)
        assert isinstance(metrics.processed_objects(), UnsignedInt64)
        assert metrics.error_count() == UnsignedInt64(0)

    def test_analytics_metadata(self, cb_env):
        result = cb_env.scope.analytics_query(f'SELECT * FROM `{cb_env.collection.name}` LIMIT 2')
        self.assert_rows(result, 2)
        metadata = result.metadata()  # type: AnalyticsMetaData
        assert isinstance(metadata, AnalyticsMetaData)
        for id_meth in (metadata.client_context_id, metadata.request_id):
            id_res = id_meth()
            fail_msg = "{} failed".format(id_meth)
            assert isinstance(id_res, str), fail_msg
        assert metadata.status() == AnalyticsStatus.SUCCESS
        assert isinstance(metadata.signature(), (str, dict))
        assert isinstance(metadata.warnings(), (list))
        for warning in metadata.warnings():
            assert isinstance(warning, AnalyticsWarning)
            assert isinstance(warning.message(), str)
            assert isinstance(warning.code(), int)


class AnalyticsParamTests:
    def test_encoded_consistency(self):
        q_str = 'SELECT * FROM default'
        q_opts = AnalyticsOptions(scan_consistency=AnalyticsScanConsistency.REQUEST_PLUS)
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        assert query.params.get('scan_consistency', None) == AnalyticsScanConsistency.REQUEST_PLUS.value
        assert query.consistency == AnalyticsScanConsistency.REQUEST_PLUS.value

        q_opts = AnalyticsOptions(scan_consistency=AnalyticsScanConsistency.NOT_BOUNDED)
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        assert query.params.get('scan_consistency', None) == AnalyticsScanConsistency.NOT_BOUNDED.value
        assert query.consistency == AnalyticsScanConsistency.NOT_BOUNDED.value

    @pytest.fixture(scope='class')
    def base_opts(self):
        return {'statement': 'SELECT * FROM default',
                'metrics': True}

    def test_params_base(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = AnalyticsOptions()
        query = AnalyticsQuery.create_query_object(q_str, q_opts)
        assert query.params == base_opts

    def test_params_timeout(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = AnalyticsOptions(timeout=timedelta(seconds=120))
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 120000000
        assert query.params == exp_opts

        q_opts = AnalyticsOptions(timeout=20)
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 20000000
        assert query.params == exp_opts

        q_opts = AnalyticsOptions(timeout=25.5)
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 25500000
        assert query.params == exp_opts

    def test_params_read_only(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = AnalyticsOptions(read_only=True)
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['readonly'] = True
        assert query.params == exp_opts

    def test_params_priority(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = AnalyticsOptions(priority=True)
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['priority'] = True
        assert query.params == exp_opts

    def test_params_client_context_id(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = AnalyticsOptions(client_context_id='test-string-id')
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['client_context_id'] = 'test-string-id'
        assert query.params == exp_opts

    def test_params_query_context(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = AnalyticsOptions(query_context='bucket.scope')
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['scope_qualifier'] = 'bucket.scope'
        assert query.params == exp_opts

    def test_params_serializer(self, base_opts):
        from couchbase.serializer import DefaultJsonSerializer

        # serializer
        serializer = DefaultJsonSerializer()
        q_str = 'SELECT * FROM default'
        q_opts = AnalyticsOptions(serializer=serializer)
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['serializer'] = serializer
        assert query.params == exp_opts

    # def test_params(self):

    #     # no opts - metrics will default to False + statement
    #     q_str = 'SELECT * FROM default'
    #     q_opts = AnalyticsOptions()
    #     query = AnalyticsQuery.create_query_object(q_str, q_opts)
    #     base_opts = {'statement': q_str, 'metrics': True}
    #     assert query.params == base_opts

    #     # timeout

    #     # readonly

    #     # priority

    #     # client_context_id

    #     # query_context

    #     # raw:  SEE test_query_raw_options
