import json
from datetime import timedelta
from uuid import uuid4

import pytest

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.diagnostics import (ClusterState,
                                   EndpointPingReport,
                                   EndpointState,
                                   PingState,
                                   ServiceType)
from couchbase.exceptions import InvalidArgumentException, ParsingFailedException
from couchbase.options import (ClusterOptions,
                               DiagnosticsOptions,
                               PingOptions)
from couchbase.result import DiagnosticsResult, PingResult

from ._test_utils import TestEnvironment


class ClusterDiagnosticsTests:

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
        cb_env = TestEnvironment(cluster, bucket, coll, couchbase_config, manage_buckets=True)

        yield cb_env
        cluster.close()

    @pytest.fixture(scope="class")
    def check_diagnostics_supported(self, cb_env):
        cb_env.check_if_feature_supported('diagnostics')

    @pytest.mark.usefixtures("check_diagnostics_supported")
    def test_ping(self, cb_env):
        cluster = cb_env.cluster
        result = cluster.ping()
        assert isinstance(result, PingResult)

        assert result.sdk is not None
        assert result.id is not None
        assert result.version is not None
        assert result.endpoints is not None
        for ping_reports in result.endpoints.values():
            for report in ping_reports:
                assert isinstance(report, EndpointPingReport)
                print(report)
                if report.state == PingState.OK:
                    assert report.id is not None
                    assert report.latency is not None
                    assert report.remote is not None
                    assert report.local is not None
                    assert report.service_type is not None

    @pytest.mark.usefixtures("check_diagnostics_supported")
    def test_ping_report_id(self, cb_env):
        cluster = cb_env.cluster
        report_id = uuid4()
        result = cluster.ping(PingOptions(report_id=report_id))
        assert str(report_id) == result.id

    @pytest.mark.usefixtures("check_diagnostics_supported")
    def test_ping_restrict_services(self, cb_env):
        cluster = cb_env.cluster
        services = [ServiceType.KeyValue]
        result = cluster.ping(PingOptions(service_types=services))
        keys = list(result.endpoints.keys())
        assert len(keys) == 1
        assert keys[0] == ServiceType.KeyValue

    @pytest.mark.usefixtures("check_diagnostics_supported")
    def test_ping_str_services(self, cb_env):
        cluster = cb_env.cluster
        services = [ServiceType.KeyValue.value, ServiceType.Query.value]
        result = cluster.ping(PingOptions(service_types=services))
        assert len(result.endpoints) >= 1

    @pytest.mark.usefixtures("check_diagnostics_supported")
    def test_ping_mixed_services(self, cb_env):
        cluster = cb_env.cluster
        services = [ServiceType.KeyValue, ServiceType.Query.value]
        result = cluster.ping(PingOptions(service_types=services))
        assert len(result.endpoints) >= 1

    @pytest.mark.usefixtures("check_diagnostics_supported")
    def test_ping_invalid_services(self, cb_env):
        cluster = cb_env.cluster
        with pytest.raises(InvalidArgumentException):
            cluster.ping(PingOptions(service_types=ServiceType.KeyValue))

    @pytest.mark.usefixtures("check_diagnostics_supported")
    def test_ping_as_json(self, cb_env):
        cluster = cb_env.cluster
        result = cluster.ping()
        assert isinstance(result, PingResult)
        result_str = result.as_json()
        assert isinstance(result_str, str)
        result_json = json.loads(result_str)
        assert result_json['version'] is not None
        assert result_json['id'] is not None
        assert result_json['sdk'] is not None
        assert result_json['services'] is not None
        for _, data in result_json['services'].items():
            if len(data):
                assert data[0]['id'] is not None
                assert data[0]['latency_us'] is not None
                assert data[0]['remote'] is not None
                assert data[0]['local'] is not None
                assert data[0]['state'] is not None

    @pytest.mark.usefixtures("check_diagnostics_supported")
    def test_diagnostics(self, cb_env):
        cluster = cb_env.cluster
        report_id = str(uuid4())
        result = cluster.diagnostics(
            DiagnosticsOptions(report_id=report_id))
        assert isinstance(result, DiagnosticsResult)
        assert result.id == report_id
        assert result.sdk is not None
        assert result.version is not None
        assert result.state == ClusterState.Online

        kv_endpoints = result.endpoints[ServiceType.KeyValue]
        assert len(kv_endpoints) > 0
        assert kv_endpoints[0].id is not None
        assert kv_endpoints[0].local is not None
        assert kv_endpoints[0].remote is not None
        assert kv_endpoints[0].last_activity_us is not None
        assert kv_endpoints[0].state == EndpointState.Connected
        assert kv_endpoints[0].service_type == ServiceType.KeyValue

    @pytest.mark.usefixtures("check_diagnostics_supported")
    def test_diagnostics_after_query(self, cb_env):
        cluster = cb_env.cluster
        # lets make sure there is at least 1 row
        key, value = cb_env.get_default_key_value()
        cb_env.collection.upsert(key, value)
        bucket_name = cb_env.bucket.name
        report_id = str(uuid4())
        # the mock will fail on query, but diagnostics should
        # still return a query service type
        try:
            rows = cluster.query(f'SELECT * FROM `{bucket_name}` LIMIT 1').execute()
            assert len(rows) > 0
        except ParsingFailedException:
            pass

        result = cluster.diagnostics(
            DiagnosticsOptions(report_id=report_id))
        assert result.id == report_id

        q = result.endpoints[ServiceType.Query]
        assert len(q) >= 1
        assert q[0].id is not None
        assert q[0].local is not None
        assert q[0].remote is not None
        assert isinstance(q[0].last_activity_us, timedelta)
        assert q[0].state == EndpointState.Connected
        assert q[0].service_type == ServiceType.Query

    @pytest.mark.usefixtures("check_diagnostics_supported")
    def test_diagnostics_as_json(self, cb_env):
        cluster = cb_env.cluster
        report_id = str(uuid4())
        result = cluster.diagnostics(
            DiagnosticsOptions(report_id=report_id))

        assert isinstance(result, DiagnosticsResult)
        result_str = result.as_json()
        assert isinstance(result_str, str)
        result_json = json.loads(result_str)
        assert result_json['version'] is not None
        assert result_json['id'] is not None
        assert result_json['sdk'] is not None
        assert result_json['services'] is not None
        for _, data in result_json['services'].items():
            if len(data):
                assert data[0]['id'] is not None
                assert data[0]['last_activity_us'] is not None
                assert data[0]['remote'] is not None
                assert data[0]['local'] is not None
                assert data[0]['state'] is not None
