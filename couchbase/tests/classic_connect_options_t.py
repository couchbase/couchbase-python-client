#  Copyright 2016-2026. Couchbase, Inc.
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

from __future__ import annotations

import platform
from copy import copy
from datetime import timedelta
from typing import Any

import pytest

from acouchbase.protocol.classic._cluster_impl import AsyncClusterImpl
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import InvalidArgumentException
from couchbase.options import (ClusterMetricsOptions,
                               ClusterOptions,
                               ClusterOrphanReportingOptions,
                               ClusterTimeoutOptions,
                               ClusterTracingOptions,
                               IpProtocol,
                               TLSVerifyMode)
from couchbase.protocol.classic._cluster_impl import ClusterImpl
from couchbase.serializer import DefaultJsonSerializer
from couchbase.transcoder import JSONTranscoder
from tests.test_features import TestProtocol
from txcouchbase.protocol.classic._cluster_impl import TxClusterImpl

# What is checked here is classic's cluster impls, and the options they hand the binding at connect.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

CONNSTR = 'couchbase://localhost'
USER_AGENT = f'python/{platform.python_version()}'

# Each timeout as the binding reads it, alone and then together.
TIMEOUT_CASES: list[tuple[dict[str, Any], dict[str, Any]]] = [
    ({'bootstrap_timeout': timedelta(seconds=30)}, {'bootstrap_timeout': 30000}),
    ({'resolve_timeout': timedelta(seconds=30)}, {'resolve_timeout': 30000}),
    ({'connect_timeout': timedelta(seconds=30)}, {'connect_timeout': 30000}),
    ({'kv_timeout': timedelta(seconds=30)}, {'key_value_timeout': 30000}),
    ({'kv_durable_timeout': timedelta(seconds=30)}, {'key_value_durable_timeout': 30000}),
    ({'views_timeout': timedelta(seconds=30)}, {'view_timeout': 30000}),
    ({'query_timeout': timedelta(seconds=30)}, {'query_timeout': 30000}),
    ({'analytics_timeout': timedelta(seconds=30)}, {'analytics_timeout': 30000}),
    ({'search_timeout': timedelta(seconds=30)}, {'search_timeout': 30000}),
    ({'management_timeout': timedelta(seconds=30)}, {'management_timeout': 30000}),
    ({'dns_srv_timeout': timedelta(seconds=30)}, {'dns_srv_timeout': 30000}),
    ({'idle_http_connection_timeout': timedelta(seconds=30)}, {'idle_http_connection_timeout': 30000}),
    ({'config_idle_redial_timeout': timedelta(seconds=30)}, {'config_idle_redial_timeout': 30000}),
    ({'bootstrap_timeout': timedelta(seconds=60),
      'kv_timeout': timedelta(seconds=5),
      'query_timeout': timedelta(seconds=30),
      'search_timeout': timedelta(seconds=120),
      'management_timeout': timedelta(seconds=60)},
     {'bootstrap_timeout': 60000,
      'key_value_timeout': 5000,
      'query_timeout': 30000,
      'search_timeout': 120000,
      'management_timeout': 60000}),
]


class ClassicConnectOptionsUnitTests:
    # No cb_env and no TEST_MANIFEST: `skip_connect` stops the client adapter opening a
    # connection.  What the options parse to is in connection_t; this is what classic makes of
    # the parsed settings.

    USERNAME = 'Administrator'
    PASSWORD = 'password'

    @classmethod
    def _auth(cls) -> PasswordAuthenticator:
        return PasswordAuthenticator(cls.USERNAME, cls.PASSWORD)

    @classmethod
    def _conn_options(cls, *options: ClusterOptions, connstr: str = CONNSTR, **kwargs: Any) -> dict[str, Any]:
        return ClusterImpl(connstr, *options, skip_connect='TEST_SKIP_CONNECT', **kwargs).connect_options

    @pytest.mark.parametrize('opts, expected_opts', TIMEOUT_CASES)
    def test_cluster_timeout_options(self, opts: dict[str, Any], expected_opts: dict[str, Any]) -> None:
        nested = ClusterOptions(self._auth(), timeout_options=ClusterTimeoutOptions(**opts))
        for conn_opts in (self._conn_options(nested), self._conn_options(ClusterOptions(self._auth()), **opts)):
            assert conn_opts['timeout_options'] == expected_opts

    @pytest.mark.parametrize('opts', [{name: 30} for name, _ in
                                      [('bootstrap_timeout', 0), ('resolve_timeout', 0), ('connect_timeout', 0),
                                       ('kv_timeout', 0), ('kv_durable_timeout', 0), ('views_timeout', 0),
                                       ('query_timeout', 0), ('analytics_timeout', 0), ('search_timeout', 0),
                                       ('management_timeout', 0), ('dns_srv_timeout', 0),
                                       ('idle_http_connection_timeout', 0), ('config_idle_redial_timeout', 0)]]
                             + [{'bootstrap_timeout': 60000,
                                 'kv_timeout': 5000,
                                 'query_timeout': 30000,
                                 'search_timeout': 120000,
                                 'management_timeout': 60000}])
    def test_cluster_timeout_options_fail(self, opts: dict[str, Any]) -> None:
        # An int is the wrong type whatever its magnitude, which is the last case: 60000 reads
        # like a plausible millisecond count and is rejected for the same reason 30 is.
        with pytest.raises(InvalidArgumentException):
            self._conn_options(ClusterOptions(self._auth()), **opts)
        with pytest.raises(InvalidArgumentException):
            self._conn_options(ClusterOptions(self._auth(), timeout_options=opts))  # type: ignore[arg-type]

    def test_cluster_orphan_reporting_options(self) -> None:
        orphan_opts = ClusterOrphanReportingOptions(enable_orphan_reporting=True,
                                                    emit_interval=timedelta(seconds=30),
                                                    sample_size=20)
        conn_opts = self._conn_options(ClusterOptions(self._auth(), orphan_reporting_options=orphan_opts))
        assert conn_opts['orphan_reporting_options'] == {'enable_orphan_reporting': True,
                                                         'orphan_emit_interval': 30000,
                                                         'orphan_sample_size': 20}

    @pytest.mark.parametrize('opts', [{'enable_orphan_reporting': 1},
                                      {'orphan_emit_interval': 30},
                                      {'orphan_sample_size': 35.1},
                                      {'tracing_orphaned_queue_flush_interval': 30}])
    def test_cluster_orphan_reporting_options_fail(self, opts: dict[str, Any]) -> None:
        with pytest.raises(InvalidArgumentException):
            self._conn_options(ClusterOptions(self._auth()), **opts)
        with pytest.raises(InvalidArgumentException):
            self._conn_options(ClusterOptions(self._auth(), orphan_reporting_options=opts))  # type: ignore[arg-type]

    # A connection string spells a duration as text: a whole number of milliseconds, or one or
    # more number-and-unit parts, which add up in any order.  What is left below a millisecond is
    # dropped, as it is from a timedelta passed in code, so 0 and 1ns both send 0.
    @pytest.mark.parametrize('value, expected', [('2500', 2500), ('2500ms', 2500), ('2.5s', 2500),
                                                 ('1m30s', 90000), ('1h', 3600000), ('0', 0), ('007', 7),
                                                 ('1ns', 0), ('500us', 0), ('1500us', 1), ('1.5ms', 1),
                                                 ('.5s', 500), ('5.s', 5000), ('30s1m', 90000),
                                                 ('1m1m', 120000), ('1h1ms', 3600001)])
    def test_connection_string_duration(self, value: str, expected: int) -> None:
        conn_opts = self._conn_options(ClusterOptions(self._auth()),
                                       connstr=f'couchbase://10.0.0.1?kv_timeout={value}&config_poll_interval={value}')
        assert conn_opts['timeout_options'] == {'key_value_timeout': expected}
        assert conn_opts['config_poll_interval'] == expected

    def test_cluster_options(self) -> None:
        opts: dict[str, Any] = {
            'enable_tls': True,
            'enable_mutation_tokens': True,
            'enable_tcp_keep_alive': True,
            'ip_protocol': IpProtocol.Any,
            'enable_dns_srv': True,
            'show_queries': True,
            'enable_unordered_execution': True,
            'enable_clustermap_notification': True,
            'enable_compression': True,
            'enable_tracing': True,
            'enable_metrics': True,
            'enable_orphan_reporting': True,
            'network': 'external',
            'tls_verify': TLSVerifyMode.NO_VERIFY,
            'disable_mozilla_ca_certificates': False,
            'serializer': DefaultJsonSerializer(),
            'transcoder': JSONTranscoder(),
            'tcp_keep_alive_interval': timedelta(seconds=30),
            'config_poll_interval': timedelta(seconds=30),
            'config_poll_floor': timedelta(seconds=30),
            'max_http_connections': 10,
            'num_io_threads': 1,
            'dump_configuration': True,
            'preferred_server_group': 'group1',
            'enable_app_telemetry': True,
            'app_telemetry_endpoint': 'ws://localhost:8093',
            'app_telemetry_backoff': timedelta(seconds=10),
            'app_telemetry_ping_interval': timedelta(seconds=60),
            'app_telemetry_ping_timeout': timedelta(seconds=5),
            'enable_lazy_connections': True,
        }

        expected_opts = copy(opts)
        # serializer and transcoder stay on the Python side; the core never sees either.
        del expected_opts['serializer'], expected_opts['transcoder']
        for key in ('tcp_keep_alive_interval', 'config_poll_interval', 'config_poll_floor',
                    'app_telemetry_backoff', 'app_telemetry_ping_interval', 'app_telemetry_ping_timeout'):
            expected_opts[key] = int(opts[key].total_seconds() * 1000)
        del expected_opts['ip_protocol']
        expected_opts['use_ip_protocol'] = 'any'
        expected_opts['tls_verify'] = 'none'
        # Orphan reporting is nested rather than flat, tracing's switch becomes
        # setup_sdk_tracing, and metrics stay on the Python side with the meter.
        del expected_opts['enable_metrics'], expected_opts['enable_orphan_reporting'], expected_opts['enable_tracing']
        expected_opts['orphan_reporting_options'] = {'enable_orphan_reporting': True}
        expected_opts['setup_sdk_tracing'] = True

        for cluster_opts in (self._conn_options(ClusterOptions(self._auth(), **opts)),
                             self._conn_options(ClusterOptions(self._auth()), **opts)):
            assert cluster_opts.pop('user_agent_extra', None) == USER_AGENT
            assert cluster_opts == expected_opts

    def test_connect_options_nest_each_block(self) -> None:
        # The plain options stay flat, the timeout and orphan blocks nest under the names the
        # binding reads, and tracing reaches it only as whether to forward spans.  Metrics and
        # the tracing thresholds stay on the Python side.
        options = ClusterOptions(self._auth(),
                                 max_http_connections=10,
                                 timeout_options=ClusterTimeoutOptions(kv_timeout=timedelta(seconds=5)),
                                 orphan_reporting_options=ClusterOrphanReportingOptions(sample_size=20),
                                 tracing_options=ClusterTracingOptions(tracing_threshold_kv=timedelta(milliseconds=30)),
                                 metrics_options=ClusterMetricsOptions(enable_metrics=True))
        conn_opts = self._conn_options(options)
        assert conn_opts.pop('user_agent_extra', None) == USER_AGENT
        assert conn_opts == {'max_http_connections': 10,
                             'timeout_options': {'key_value_timeout': 5000},
                             'orphan_reporting_options': {'orphan_sample_size': 20},
                             'setup_sdk_tracing': True}

    def test_every_api_tree_parses_a_connection_the_same_way(self) -> None:
        # All three impls forward `*options, **kwargs` to the single
        # `ClusterSettings.build_cluster_settings` and none touches the result, which is why
        # connection_t's 120-odd tests of that parse call it directly and are not generated.
        # Each impl derives its streaming and durable timeouts from the result itself, so those
        # are compared on the impls.  `TxClusterImpl` inherits its constructor, so it is here to
        # catch an override.
        options = ClusterOptions(self._auth(),
                                 enable_tls=True,
                                 tls_verify=TLSVerifyMode.NO_VERIFY,
                                 ip_protocol=IpProtocol.Any,
                                 max_http_connections=10,
                                 timeout_options=ClusterTimeoutOptions(kv_timeout=timedelta(seconds=5),
                                                                       kv_durable_timeout=timedelta(seconds=6),
                                                                       analytics_timeout=timedelta(seconds=7),
                                                                       query_timeout=timedelta(seconds=8),
                                                                       search_timeout=timedelta(seconds=9),
                                                                       views_timeout=timedelta(seconds=10)),
                                 tracing_options=ClusterTracingOptions(
                                     tracing_threshold_kv=timedelta(milliseconds=30)),
                                 metrics_options=ClusterMetricsOptions(enable_metrics=True))
        connstr = f'{CONNSTR}?sasl_mech_force=PLAIN&num_io_threads=1'
        fields = ['connstr', 'auth', 'cluster_options', 'metrics_options', 'orphan_options',
                  'timeout_options', 'tracing_options']

        # One options object across all three, which also pins that building a cluster does
        # not consume it: build_cluster_settings pops from a copy, not from what it was given.
        impls = [impl(connstr, options, skip_connect='TEST_SKIP_CONNECT')
                 for impl in (ClusterImpl, AsyncClusterImpl, TxClusterImpl)]
        sync_settings = impls[0].cluster_settings
        for field in fields:
            expected = getattr(sync_settings, field)
            for other in impls[1:]:
                assert getattr(other.cluster_settings, field) == expected, field

        assert impls[0].streaming_timeouts == {'analytics_timeout': 7000, 'query_timeout': 8000,
                                               'search_timeout': 9000, 'view_timeout': 10000}
        assert impls[0].kv_durable_timeout == 6000
        for other in impls[1:]:
            assert other.streaming_timeouts == impls[0].streaming_timeouts
            assert other.kv_durable_timeout == impls[0].kv_durable_timeout
