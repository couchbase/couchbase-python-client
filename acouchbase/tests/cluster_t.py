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

import json
from datetime import timedelta
from uuid import uuid4

import pytest
import pytest_asyncio

from acouchbase.cluster import get_event_loop
from couchbase.diagnostics import (ClusterState,
                                   EndpointPingReport,
                                   EndpointState,
                                   PingState,
                                   ServiceType)
from couchbase.exceptions import (InternalServerFailureException,
                                  InvalidArgumentException,
                                  ParsingFailedException,
                                  QueryIndexNotFoundException)
from couchbase.options import DiagnosticsOptions, PingOptions
from couchbase.result import DiagnosticsResult, PingResult

from ._test_utils import TestEnvironment


class ClusterDiagnosticsTests:

    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest_asyncio.fixture(scope="class", name="cb_env")
    async def couchbase_test_environment(self, couchbase_config):
        cb_env = await TestEnvironment.get_environment(__name__,
                                                       couchbase_config,
                                                       manage_buckets=True)

        yield cb_env

    @pytest.fixture(scope="class")
    def check_diagnostics_supported(self, cb_env):
        cb_env.check_if_feature_supported('diagnostics')

    @pytest.mark.usefixtures("check_diagnostics_supported")
    @pytest.mark.asyncio
    async def test_ping(self, cb_env):
        cluster = cb_env.cluster
        result = await cluster.ping()
        assert isinstance(result, PingResult)

        assert result.sdk is not None
        assert result.id is not None
        assert result.version is not None
        assert result.endpoints is not None
        for ping_reports in result.endpoints.values():
            for report in ping_reports:
                assert isinstance(report, EndpointPingReport)
                if report.state == PingState.OK:
                    assert report.id is not None
                    assert report.latency is not None
                    assert report.remote is not None
                    assert report.local is not None
                    assert report.service_type is not None

    @pytest.mark.usefixtures("check_diagnostics_supported")
    @pytest.mark.asyncio
    async def test_ping_report_id(self, cb_env):
        cluster = cb_env.cluster
        report_id = uuid4()
        result = await cluster.ping(PingOptions(report_id=report_id))
        assert str(report_id) == result.id

    @pytest.mark.usefixtures("check_diagnostics_supported")
    @pytest.mark.asyncio
    async def test_ping_restrict_services(self, cb_env):
        cluster = cb_env.cluster
        services = [ServiceType.KeyValue]
        result = await cluster.ping(PingOptions(service_types=services))
        keys = list(result.endpoints.keys())
        assert len(keys) == 1
        assert keys[0] == ServiceType.KeyValue

    @pytest.mark.usefixtures("check_diagnostics_supported")
    @pytest.mark.asyncio
    async def test_ping_str_services(self, cb_env):
        cluster = cb_env.cluster
        services = [ServiceType.KeyValue.value, ServiceType.Query.value]
        result = await cluster.ping(PingOptions(service_types=services))
        assert len(result.endpoints) >= 1

    @pytest.mark.usefixtures("check_diagnostics_supported")
    @pytest.mark.asyncio
    async def test_ping_mixed_services(self, cb_env):
        cluster = cb_env.cluster
        services = [ServiceType.KeyValue, ServiceType.Query.value]
        result = await cluster.ping(PingOptions(service_types=services))
        assert len(result.endpoints) >= 1

    @pytest.mark.usefixtures("check_diagnostics_supported")
    @pytest.mark.asyncio
    async def test_ping_invalid_services(self, cb_env):
        cluster = cb_env.cluster
        with pytest.raises(InvalidArgumentException):
            await cluster.ping(PingOptions(service_types=ServiceType.KeyValue))

    @pytest.mark.usefixtures("check_diagnostics_supported")
    @pytest.mark.asyncio
    async def test_ping_as_json(self, cb_env):
        cluster = cb_env.cluster
        result = await cluster.ping()
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
    @pytest.mark.asyncio
    async def test_diagnostics(self, cb_env):
        cluster = cb_env.cluster
        report_id = str(uuid4())
        result = await cluster.diagnostics(
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
    @pytest.mark.asyncio
    async def test_diagnostics_after_query(self, cb_env):
        cluster = cb_env.cluster
        # lets make sure there is at least 1 row
        key, value = cb_env.get_default_key_value()
        await cb_env.collection.upsert(key, value)
        bucket_name = cb_env.bucket.name
        report_id = str(uuid4())
        # the mock will fail on query, but diagnostics should
        # still return a query service type
        try:
            rows = await cluster.query(f'SELECT * FROM `{bucket_name}` LIMIT 1').execute()
            assert len(rows) > 0
        except (InternalServerFailureException, ParsingFailedException, QueryIndexNotFoundException):
            pass
        except Exception as e:
            print(f'exception: {e.__class__.__name__}, {e}')
            raise e

        result = await cluster.diagnostics(
            DiagnosticsOptions(report_id=report_id))
        assert result.id == report_id

        query_diag_result = result.endpoints[ServiceType.Query]
        assert len(query_diag_result) >= 1
        for q in query_diag_result:
            assert q.id is not None
            assert q.local is not None
            assert q.remote is not None
            assert isinstance(q.last_activity_us, timedelta)
            assert q.state == EndpointState.Connected
            assert q.service_type == ServiceType.Query

    @pytest.mark.usefixtures("check_diagnostics_supported")
    @pytest.mark.asyncio
    async def test_diagnostics_as_json(self, cb_env):
        cluster = cb_env.cluster
        report_id = str(uuid4())
        result = await cluster.diagnostics(
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
