from datetime import datetime, timedelta

import pytest

from couchbase.analytics import (AnalyticsMetaData,
                                 AnalyticsMetrics,
                                 AnalyticsStatus,
                                 AnalyticsWarning)
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.management.options import ConnectLinkOptions, DisconnectLinkOptions
from couchbase.options import (AnalyticsOptions,
                               ClusterOptions,
                               UnsignedInt64)
from couchbase.result import AnalyticsResult

from ._test_utils import TestEnvironment


class AnalyticsTests:

    DATASET_NAME = 'test-dataset'

    @pytest.fixture(scope="class", name="cb_env")
    def couchbase_test_environment(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        opts = ClusterOptions(PasswordAuthenticator(username, pw))
        cluster = Cluster(
            conn_string, opts)
        cluster.cluster_info()
        bucket = cluster.bucket(f"{couchbase_config.bucket_name}")

        coll = bucket.default_collection()
        cb_env = TestEnvironment(cluster, bucket, coll, couchbase_config, manage_analytics=True)

        # setup
        cb_env.load_data()
        dv_name = 'test/dataverse' if cb_env.server_version_short >= 7.0 else 'test_dataverse'
        cb_env.am.create_dataverse(dv_name, ignore_if_exists=True)
        cb_env.am.create_dataset(self.DATASET_NAME, cb_env.bucket.name, ignore_if_exists=True)
        cb_env.am.connect_link(ConnectLinkOptions(dataverse_name=dv_name))

        q_str = f'SELECT COUNT(1) AS doc_count FROM `{self.DATASET_NAME}`;'

        for _ in range(10):
            res = cluster.analytics_query(q_str)
            rows = [r for r in res.rows()]
            print(rows)
            if len(rows) > 0 and rows[0].get('doc_count', 0) > 10:
                break
            print(f'Found {len(rows)} records, waiting a bit...')
            cb_env.sleep(5)

        yield cb_env

        # teardown
        cb_env.purge_data()
        cb_env.am.disconnect_link(DisconnectLinkOptions(dataverse_name=dv_name))
        cb_env.am.drop_dataset(self.DATASET_NAME, ignore_if_not_exists=True)
        cb_env.am.drop_dataverse(dv_name, ignore_if_not_exists=True)
        cluster.close()

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
        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{self.DATASET_NAME}` WHERE `type` = $type LIMIT 1',
                                                AnalyticsOptions(named_parameters={'type': 'airline'}))
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
