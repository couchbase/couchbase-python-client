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
import warnings
from collections.abc import Generator
from copy import copy
from datetime import timedelta
from typing import Any

import pytest

from acouchbase.logic.cluster_impl import AsyncClusterImpl
from couchbase.auth import CertificateAuthenticator, PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import InvalidArgumentException, UnAmbiguousTimeoutException
from couchbase.logic.cluster_impl import ClusterImpl
from couchbase.logic.cluster_settings import ClusterSettings
from couchbase.options import (CONFIG_PROFILES,
                               ClusterMetricsOptions,
                               ClusterOptions,
                               ClusterOrphanReportingOptions,
                               ClusterTimeoutOptions,
                               ClusterTracingOptions,
                               ConfigProfile,
                               IpProtocol,
                               KnownConfigProfiles,
                               TLSVerifyMode)
from couchbase.serializer import DefaultJsonSerializer
from couchbase.transcoder import JSONTranscoder
from tests.environments.test_environment import (SuiteRequirements,
                                                 SyncTestEnvironment,
                                                 build_environment,
                                                 teardown_environment)
from tests.manifest import validate_test_manifest
from txcouchbase.logic.cluster_impl import TxClusterImpl

CONNSTR = 'couchbase://localhost'
USER_AGENT = f'python/{platform.python_version()}'

# The timeout and tracing groups drive the same cases through two spellings, so the case
# lists are shared: two copies can stop agreeing without anything reporting it.
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

TRACING_CASES: list[tuple[dict[str, Any], dict[str, Any]]] = [
    ({'tracing_threshold_kv': timedelta(milliseconds=30)}, {'key_value_threshold': 30}),
    ({'tracing_threshold_view': timedelta(milliseconds=30)}, {'view_threshold': 30}),
    ({'tracing_threshold_query': timedelta(milliseconds=30)}, {'query_threshold': 30}),
    ({'tracing_threshold_search': timedelta(milliseconds=30)}, {'search_threshold': 30}),
    ({'tracing_threshold_analytics': timedelta(milliseconds=30)}, {'analytics_threshold': 30}),
    ({'tracing_threshold_eventing': timedelta(milliseconds=30)}, {'eventing_threshold': 30}),
    ({'tracing_threshold_management': timedelta(milliseconds=30)}, {'management_threshold': 30}),
    ({'tracing_threshold_queue_size': 20}, {'threshold_sample_size': 20}),
    ({'tracing_threshold_queue_flush_interval': timedelta(seconds=30)}, {'threshold_emit_interval': 30000}),
    ({'tracing_orphaned_queue_size': 20}, {'orphan_sample_size': 20}),
    ({'tracing_orphaned_queue_flush_interval': timedelta(seconds=30)}, {'orphan_emit_interval': 30000}),
    ({'enable_tracing': True,
      'tracing_threshold_kv': timedelta(milliseconds=60),
      'tracing_threshold_query': timedelta(milliseconds=5),
      'tracing_threshold_management': timedelta(milliseconds=30),
      'tracing_threshold_queue_size': 20},
     {'enable_tracing': True,
      'key_value_threshold': 60,
      'query_threshold': 5,
      'management_threshold': 30,
      'threshold_sample_size': 20}),
]


def _settings(connstr: str, *options: ClusterOptions, **kwargs: Any) -> ClusterSettings:
    # `skip_connect` stops the client adapter opening a connection, so everything built on
    # this is cluster-free.  The read goes to `cluster_settings`, the INTERNAL property,
    # rather than `ClusterImpl._get_connection_opts`, which returns a union every call site
    # has to narrow and has no other caller in the repo.
    return ClusterImpl(connstr, *options, skip_connect='TEST_SKIP_CONNECT', **kwargs).cluster_settings


@pytest.mark.testgen_exclude('acouchbase',
                             reason='drives ClusterSettings, which every api tree imports from couchbase')
class ClusterAuthUnitTests:
    # No cb_env and no TEST_MANIFEST: nothing here connects, and the manifest is a parity
    # contract between api trees.  There is no `acouchbase.auth` or `txcouchbase.auth`, and
    # all three impls forward to the one `ClusterSettings.build_cluster_settings`, which
    # test_every_api_tree_parses_a_connection_the_same_way below asserts once.

    CERT_AUTH = {'cert_path': 'path/to/a/cert',
                 'key_path': 'path/to/a/key',
                 'trust_store_path': 'path/to/truststore'}
    TS_PATH = 'path/to/truststore'
    CERT_PATH = 'path/to/cert'
    USERNAME = 'Administrator'
    PASSWORD = 'password'

    @classmethod
    def _pw_auth(cls, **kwargs: Any) -> PasswordAuthenticator:
        return PasswordAuthenticator(cls.USERNAME, cls.PASSWORD, **kwargs)

    def test_cluster_auth_fail(self) -> None:
        with pytest.raises(InvalidArgumentException):
            ClusterImpl(CONNSTR)

    def test_cluster_cert_auth(self) -> None:
        auth = CertificateAuthenticator(**self.CERT_AUTH)
        assert _settings(CONNSTR, ClusterOptions(auth)).auth == self.CERT_AUTH
        assert _settings(CONNSTR, authenticator=auth).auth == self.CERT_AUTH

    def test_cluster_cert_auth_fail(self) -> None:
        # The dict has to arrive as keywords: the constructor takes three named paths and
        # rejects a single positional mapping.
        with pytest.raises(InvalidArgumentException):
            CertificateAuthenticator(self.CERT_AUTH)  # type: ignore[arg-type]

        bad_values: list[tuple[str, Any]] = [('cert_path', None), ('cert_path', {}),
                                             ('key_path', None), ('key_path', {}),
                                             ('trust_store_path', {})]
        for field, value in bad_values:
            auth_copy: dict[str, Any] = dict(self.CERT_AUTH)
            auth_copy[field] = value
            with pytest.raises(InvalidArgumentException):
                CertificateAuthenticator(**auth_copy)

        # trust_store_path is already in the authenticator, so repeating it as a cluster
        # option is the conflict, not the value.
        auth = CertificateAuthenticator(**self.CERT_AUTH)
        with pytest.raises(InvalidArgumentException):
            ClusterImpl(CONNSTR, ClusterOptions(auth), trust_store_path=self.TS_PATH)
        with pytest.raises(InvalidArgumentException):
            ClusterImpl(CONNSTR, authenticator=auth, trust_store_path=self.TS_PATH)

    def test_cluster_cert_auth_ts_connstr(self) -> None:
        expected_auth = {'cert_path': 'path/to/a/cert', 'key_path': 'path/to/a/key'}
        auth = CertificateAuthenticator(**expected_auth)

        settings = _settings(f'{CONNSTR}?truststorepath={self.TS_PATH}', ClusterOptions(auth))
        assert settings.auth == expected_auth
        assert settings.cluster_options['trust_store_path'] == self.TS_PATH

        settings = _settings(f'{CONNSTR}?truststorepath={self.TS_PATH}',
                             authenticator=auth,
                             trust_store_path=self.TS_PATH)
        assert settings.auth == expected_auth
        assert settings.cluster_options['trust_store_path'] == self.TS_PATH

    def test_cluster_cert_auth_ts_kwargs(self) -> None:
        expected_auth = {'cert_path': 'path/to/a/cert', 'key_path': 'path/to/a/key'}
        auth = CertificateAuthenticator(**expected_auth)

        settings = _settings(CONNSTR, ClusterOptions(auth), trust_store_path=self.TS_PATH)
        assert settings.auth == expected_auth
        assert settings.cluster_options['trust_store_path'] == self.TS_PATH

        settings = _settings(CONNSTR, authenticator=auth, trust_store_path=self.TS_PATH)
        assert settings.auth == expected_auth
        assert settings.cluster_options['trust_store_path'] == self.TS_PATH

    def test_cluster_ldap_auth(self) -> None:
        auth = PasswordAuthenticator.ldap_compatible(self.USERNAME, self.PASSWORD)
        assert isinstance(auth, PasswordAuthenticator)
        # The whole of what ldap_compatible does is pin the mechanism list, and that is what
        # test_cluster_ldap_auth_real then reads back off a live connection.
        assert auth.as_dict()['allowed_sasl_mechanisms'] == ['PLAIN']
        assert _settings(CONNSTR, ClusterOptions(auth)).auth == auth.as_dict()

    def test_cluster_legacy_sasl_mech_force(self) -> None:
        auth = self._pw_auth()
        expected_auth = dict(auth.as_dict(), allowed_sasl_mechanisms=['PLAIN'])
        assert _settings(f'{CONNSTR}?sasl_mech_force=PLAIN', ClusterOptions(auth)).auth == expected_auth

    def test_cluster_legacy_ssl_no_verify(self) -> None:
        settings = _settings(f'{CONNSTR}?ssl=no_verify', ClusterOptions(self._pw_auth()))
        assert settings.cluster_options['tls_verify'] == 'none'

    def test_cluster_pw_auth(self) -> None:
        auth = self._pw_auth()
        for settings in (_settings(CONNSTR, ClusterOptions(auth)), _settings(CONNSTR, authenticator=auth)):
            assert settings.auth == auth.as_dict()
            # None rather than [] at this layer: the empty list is what the core reports back
            # for an unrestricted connection, which test_cluster_sasl_mech_default asserts.
            assert settings.auth['allowed_sasl_mechanisms'] is None

    def test_cluster_pw_auth_fail(self) -> None:
        assert isinstance(self._pw_auth(), PasswordAuthenticator)

        bad_calls: list[tuple[tuple[Any, Any], dict[str, Any]]] = [
            ((None, None), {}),
            ((None, self.PASSWORD), {}),
            ((self.USERNAME, None), {}),
            ((self.USERNAME, self.PASSWORD), {'cert_path': {}}),
            ((self.USERNAME, self.PASSWORD), {'allowed_sasl_mechanisms': {}}),
            ((self.USERNAME, self.PASSWORD), {'allowed_sasl_mechanisms': ['PLAIN', 1]}),
        ]
        for args, kwargs in bad_calls:
            with pytest.raises(InvalidArgumentException):
                PasswordAuthenticator(*args, **kwargs)

    def test_cluster_pw_auth_with_cert(self) -> None:
        auth = self._pw_auth(cert_path=self.CERT_PATH)
        assert _settings(CONNSTR, ClusterOptions(auth)).auth == auth.as_dict()
        assert _settings(CONNSTR, authenticator=auth).auth == auth.as_dict()

    def test_cluster_pw_auth_with_cert_connstr(self) -> None:
        auth = self._pw_auth()
        expected_auth = dict(auth.as_dict(), cert_path=self.CERT_PATH)
        settings = _settings(f'{CONNSTR}?certpath={self.CERT_PATH}', ClusterOptions(auth))
        assert settings.auth == expected_auth

    def test_cluster_pw_auth_with_cert_kwargs(self) -> None:
        auth = self._pw_auth()
        expected_auth = dict(auth.as_dict(), cert_path=self.CERT_PATH)
        assert _settings(CONNSTR, ClusterOptions(auth), cert_path=self.CERT_PATH).auth == expected_auth
        assert _settings(CONNSTR, authenticator=auth, cert_path=self.CERT_PATH).auth == expected_auth

    @pytest.mark.parametrize('connstr_suffix',
                             ['?sasl_mech_force=SCRAM-SHA512&sasl_mech_force=SCRAM-SHA256',
                              '?sasl_mech_force=SCRAM-SHA512,SCRAM-SHA256'])
    def test_cluster_sasl_mech_legacy_multiple(self, connstr_suffix: str) -> None:
        auth = self._pw_auth()
        expected_auth = dict(auth.as_dict(), allowed_sasl_mechanisms=['SCRAM-SHA512', 'SCRAM-SHA256'])
        assert _settings(f'{CONNSTR}{connstr_suffix}', ClusterOptions(auth)).auth == expected_auth

    @pytest.mark.parametrize('connstr_suffix, auth_kwargs',
                             [('?allowed_sasl_mechanisms=SCRAM-SHA512&allowed_sasl_mechanisms=SCRAM-SHA256', {}),
                              ('?allowed_sasl_mechanisms=SCRAM-SHA512,SCRAM-SHA256', {}),
                              ('', {'allowed_sasl_mechanisms': 'SCRAM-SHA512,SCRAM-SHA256'}),
                              ('', {'allowed_sasl_mechanisms': ['SCRAM-SHA512', 'SCRAM-SHA256']})])
    def test_cluster_sasl_mech_multiple(self, connstr_suffix: str, auth_kwargs: dict[str, Any]) -> None:
        # Four spellings of one list, two in the connection string and two on the
        # authenticator, and the point is that they are indistinguishable downstream.
        auth = self._pw_auth(**auth_kwargs)
        expected_auth = dict(self._pw_auth().as_dict(),
                             allowed_sasl_mechanisms=['SCRAM-SHA512', 'SCRAM-SHA256'])
        assert _settings(f'{CONNSTR}{connstr_suffix}', ClusterOptions(auth)).auth == expected_auth


@pytest.mark.testgen_exclude('acouchbase',
                             reason='drives ClusterSettings, which every api tree imports from couchbase')
class ClusterOptionsUnitTests:
    # Every group here has the same three shapes: `_options` passes a typed options object,
    # `_kwargs` the same settings as loose keyword arguments, and `_override` sets a value
    # both ways to pin which wins.  The kwargs spelling prefixes some names because metrics
    # and orphan reporting both want `emit_interval` and a flat namespace holds one.

    USERNAME = 'Administrator'
    PASSWORD = 'password'

    @classmethod
    def _auth(cls) -> PasswordAuthenticator:
        return PasswordAuthenticator(cls.USERNAME, cls.PASSWORD)

    @classmethod
    def _conn_options(cls, *options: ClusterOptions, **kwargs: Any) -> dict[str, Any]:
        return dict(_settings(CONNSTR, *options, **kwargs).cluster_options)

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
        # The three observability switches are nested rather than flat, and tracing's key is
        # renamed on the way: enable_tracing becomes setup_sdk_tracing.
        del expected_opts['enable_metrics'], expected_opts['enable_orphan_reporting'], expected_opts['enable_tracing']
        expected_opts['metrics_options'] = {'enable_metrics': True}
        expected_opts['orphan_reporting_options'] = {'enable_orphan_reporting': True}
        expected_opts['setup_sdk_tracing'] = True

        for cluster_opts in (self._conn_options(ClusterOptions(self._auth(), **opts)),
                             self._conn_options(ClusterOptions(self._auth()), **opts)):
            assert cluster_opts.pop('user_agent_extra', None) == USER_AGENT
            assert cluster_opts == expected_opts

    def test_every_api_tree_parses_a_connection_the_same_way(self) -> None:
        # All three impls forward `*options, **kwargs` to the single
        # `ClusterSettings.build_cluster_settings` and none touches the result, so a generated
        # copy of the 120-odd tests above would exercise one parser through three wrappers.
        # `TxClusterImpl` inherits its constructor, so it is here to catch an override.
        options = ClusterOptions(self._auth(),
                                 enable_tls=True,
                                 tls_verify=TLSVerifyMode.NO_VERIFY,
                                 ip_protocol=IpProtocol.Any,
                                 max_http_connections=10,
                                 timeout_options=ClusterTimeoutOptions(kv_timeout=timedelta(seconds=5)),
                                 tracing_options=ClusterTracingOptions(
                                     tracing_threshold_kv=timedelta(milliseconds=30)),
                                 metrics_options=ClusterMetricsOptions(enable_metrics=True))
        connstr = f'{CONNSTR}?sasl_mech_force=PLAIN&num_io_threads=1'
        fields = ['connstr', 'auth', 'cluster_options', 'metrics_options', 'orphan_options',
                  'streaming_timeouts', 'timeout_options', 'tracing_options']

        # One options object across all three, which also pins that building a cluster does
        # not consume it: build_cluster_settings pops from a copy, not from what it was given.
        settings = [impl(connstr, options, skip_connect='TEST_SKIP_CONNECT').cluster_settings
                    for impl in (ClusterImpl, AsyncClusterImpl, TxClusterImpl)]
        sync_settings = settings[0]
        for field in fields:
            expected = getattr(sync_settings, field)
            for other in settings[1:]:
                assert getattr(other, field) == expected, field

    @pytest.mark.parametrize('opts, expected_opts',
                             [({'enable_metrics': True, 'emit_interval': timedelta(seconds=30)},
                               {'enable_metrics': True, 'metrics_emit_interval': 30000})])
    def test_cluster_metrics_options(self, opts: dict[str, Any], expected_opts: dict[str, Any]) -> None:
        cluster_opts = self._conn_options(ClusterOptions(self._auth(),
                                                         metrics_options=ClusterMetricsOptions(**opts)))
        assert cluster_opts.get('metrics_options') == expected_opts

    @pytest.mark.parametrize('opts', [{'enable_metrics': 1}, {'metrics_emit_interval': 30}])
    def test_cluster_metrics_options_fail(self, opts: dict[str, Any]) -> None:
        with pytest.raises(InvalidArgumentException):
            ClusterImpl(CONNSTR, ClusterOptions(self._auth()), **opts)
        with pytest.raises(InvalidArgumentException):
            ClusterImpl(CONNSTR,
                        ClusterOptions(self._auth(), metrics_options=opts))  # type: ignore[call-overload]

    @pytest.mark.parametrize('opts, expected_opts',
                             [({'enable_metrics': True, 'metrics_emit_interval': timedelta(seconds=30)},
                               {'enable_metrics': True, 'metrics_emit_interval': 30000})])
    def test_cluster_metrics_options_kwargs(self, opts: dict[str, Any], expected_opts: dict[str, Any]) -> None:
        cluster_opts = self._conn_options(ClusterOptions(self._auth()), **opts)
        assert cluster_opts.get('metrics_options') == expected_opts

    @pytest.mark.parametrize('opts, cluster_opts, expected_opts',
                             [(None,
                               {'enable_metrics': False, 'logging_meter_emit_interval': timedelta(seconds=60)},
                               {'enable_metrics': False, 'metrics_emit_interval': 60000}),
                              ({'enable_metrics': True, 'emit_interval': timedelta(seconds=60)},
                               {'enable_metrics': False, 'logging_meter_emit_interval': timedelta(seconds=30)},
                               {'enable_metrics': True, 'metrics_emit_interval': 60000})])
    def test_cluster_metrics_options_override(self,
                                              opts: dict[str, Any] | None,
                                              cluster_opts: dict[str, Any],
                                              expected_opts: dict[str, Any]) -> None:
        metrics_options = ClusterMetricsOptions(**opts) if opts else None
        conn_opts = self._conn_options(ClusterOptions(self._auth(),
                                                      metrics_options=metrics_options,
                                                      **cluster_opts))
        # The deprecated flat name is consumed rather than forwarded, so its absence is as
        # much the assertion as the merged value is.
        assert 'logging_meter_emit_interval' not in conn_opts
        assert conn_opts.get('metrics_options') == expected_opts

    @pytest.mark.parametrize('opts, expected_opts',
                             [({'enable_orphan_reporting': True,
                                'emit_interval': timedelta(seconds=30),
                                'sample_size': 20},
                               {'enable_orphan_reporting': True,
                                'orphan_emit_interval': 30000,
                                'orphan_sample_size': 20})])
    def test_cluster_orphan_reporting_options(self, opts: dict[str, Any], expected_opts: dict[str, Any]) -> None:
        cluster_opts = self._conn_options(
            ClusterOptions(self._auth(), orphan_reporting_options=ClusterOrphanReportingOptions(**opts)))
        assert cluster_opts.get('orphan_reporting_options') == expected_opts

    @pytest.mark.parametrize('opts', [{'enable_orphan_reporting': 1},
                                      {'orphan_emit_interval': 30},
                                      {'orphan_sample_size': 35.1}])
    def test_cluster_orphan_reporting_options_fail(self, opts: dict[str, Any]) -> None:
        with pytest.raises(InvalidArgumentException):
            ClusterImpl(CONNSTR, ClusterOptions(self._auth()), **opts)
        with pytest.raises(InvalidArgumentException):
            ClusterImpl(CONNSTR,
                        ClusterOptions(self._auth(), orphan_reporting_options=opts))  # type: ignore[call-overload]

    @pytest.mark.parametrize('opts, expected_opts',
                             [({'enable_orphan_reporting': True,
                                'orphan_emit_interval': timedelta(seconds=30),
                                'orphan_sample_size': 20},
                               {'enable_orphan_reporting': True,
                                'orphan_emit_interval': 30000,
                                'orphan_sample_size': 20})])
    def test_cluster_orphan_reporting_options_kwargs(self,
                                                     opts: dict[str, Any],
                                                     expected_opts: dict[str, Any]) -> None:
        cluster_opts = self._conn_options(ClusterOptions(self._auth()), **opts)
        assert cluster_opts.get('orphan_reporting_options') == expected_opts

    @pytest.mark.parametrize('opts, cluster_opts, tracing_opts, expected_opts',
                             [(None,
                               {'enable_orphan_reporting': False},
                               {'tracing_orphaned_queue_flush_interval': timedelta(seconds=60),
                                'tracing_orphaned_queue_size': 50},
                               {'enable_orphan_reporting': False,
                                'orphan_emit_interval': 60000,
                                'orphan_sample_size': 50}),
                              ({'enable_orphan_reporting': True,
                                'emit_interval': timedelta(seconds=60),
                                'sample_size': 50},
                               {'enable_orphan_reporting': False},
                               {'tracing_orphaned_queue_flush_interval': timedelta(seconds=30),
                                'tracing_orphaned_queue_size': 20},
                               {'enable_orphan_reporting': True,
                                'orphan_emit_interval': 60000,
                                'orphan_sample_size': 50})])
    def test_cluster_orphan_reporting_options_override(self,
                                                       opts: dict[str, Any] | None,
                                                       cluster_opts: dict[str, Any],
                                                       tracing_opts: dict[str, Any],
                                                       expected_opts: dict[str, Any]) -> None:
        # The two orphan settings that live on ClusterTracingOptions are rehomed here rather
        # than forwarded, so a tracing_options block holding only those leaves nothing behind.
        conn_opts = self._conn_options(
            ClusterOptions(self._auth(),
                           orphan_reporting_options=ClusterOrphanReportingOptions(**opts) if opts else None,
                           tracing_options=ClusterTracingOptions(**tracing_opts),
                           **cluster_opts))
        assert conn_opts.get('tracing_options') is None
        assert conn_opts.get('orphan_reporting_options') == expected_opts

    @pytest.mark.parametrize('opts, expected_opts', TIMEOUT_CASES)
    def test_cluster_timeout_options(self, opts: dict[str, Any], expected_opts: dict[str, Any]) -> None:
        cluster_opts = self._conn_options(ClusterOptions(self._auth(),
                                                         timeout_options=ClusterTimeoutOptions(**opts)))
        assert cluster_opts.get('timeout_options') == expected_opts

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
            ClusterImpl(CONNSTR, ClusterOptions(self._auth()), **opts)
        with pytest.raises(InvalidArgumentException):
            ClusterImpl(CONNSTR,
                        ClusterOptions(self._auth(), timeout_options=opts))  # type: ignore[call-overload]

    @pytest.mark.parametrize('opts, expected_opts', TIMEOUT_CASES)
    def test_cluster_timeout_options_kwargs(self, opts: dict[str, Any], expected_opts: dict[str, Any]) -> None:
        cluster_opts = self._conn_options(ClusterOptions(self._auth()), **opts)
        assert cluster_opts.get('timeout_options') == expected_opts

    @staticmethod
    def _assert_expected_tracing(settings: ClusterSettings, expected_opts: dict[str, Any]) -> None:
        # The two orphan settings are rehomed into `orphan_reporting_options` and go to the
        # core.  Every other one stays on the Python side in `cluster_settings.tracing_options`
        # and the core is handed no tracing block at all, on both paths.
        assert settings.cluster_options.get('tracing_options') is None
        if 'orphan_sample_size' in expected_opts or 'orphan_emit_interval' in expected_opts:
            assert settings.cluster_options.get('orphan_reporting_options') == expected_opts
        else:
            assert settings.cluster_options.get('orphan_reporting_options') is None
            # enable_tracing is written whether or not the caller asked for it, so it goes into
            # a copy: writing it into the parametrize case dict mutates a list two tests share.
            assert settings.tracing_options == dict(expected_opts, enable_tracing=True)

    @pytest.mark.parametrize('opts, expected_opts', TRACING_CASES)
    def test_cluster_tracing_options(self, opts: dict[str, Any], expected_opts: dict[str, Any]) -> None:
        settings = _settings(CONNSTR, ClusterOptions(self._auth(),
                                                     tracing_options=ClusterTracingOptions(**opts)))
        self._assert_expected_tracing(settings, expected_opts)

    @pytest.mark.parametrize('opts', [{'enable_tracing': 1},
                                      {'tracing_threshold_kv': 30},
                                      {'tracing_threshold_view': 30},
                                      {'tracing_threshold_query': 30},
                                      {'tracing_threshold_search': 30},
                                      {'tracing_threshold_analytics': 30},
                                      {'tracing_threshold_eventing': 30},
                                      {'tracing_threshold_management': 30},
                                      {'tracing_threshold_queue_flush_interval': 30},
                                      {'tracing_orphaned_queue_flush_interval': 30},
                                      {'tracing_threshold_kv': 60000,
                                       'tracing_threshold_query': 5000,
                                       'tracing_threshold_search': 30000,
                                       'tracing_threshold_management': 120000,
                                       'tracing_threshold_queue_flush_interval': 60000}])
    def test_cluster_tracing_options_fail(self, opts: dict[str, Any]) -> None:
        with pytest.raises(InvalidArgumentException):
            ClusterImpl(CONNSTR, ClusterOptions(self._auth()), **opts)
        with pytest.raises(InvalidArgumentException):
            ClusterImpl(CONNSTR,
                        ClusterOptions(self._auth(), tracing_options=opts))  # type: ignore[call-overload]

    @pytest.mark.parametrize('opts, expected_opts', TRACING_CASES)
    def test_cluster_tracing_options_kwargs(self, opts: dict[str, Any], expected_opts: dict[str, Any]) -> None:
        settings = _settings(CONNSTR, ClusterOptions(self._auth()), **opts)
        self._assert_expected_tracing(settings, expected_opts)

    @pytest.mark.parametrize('tracing_opts, cluster_opts, expected_opts',
                             [(None, {'enable_tracing': False}, {'enable_tracing': False}),
                              ({'enable_tracing': True}, {'enable_tracing': False}, {'enable_tracing': True})])
    def test_cluster_tracing_options_override(self,
                                              tracing_opts: dict[str, Any] | None,
                                              cluster_opts: dict[str, Any],
                                              expected_opts: dict[str, Any]) -> None:
        settings = _settings(CONNSTR,
                             ClusterOptions(self._auth(),
                                            tracing_options=ClusterTracingOptions(**tracing_opts)
                                            if tracing_opts else None,
                                            **cluster_opts))
        assert settings.cluster_options.get('tracing_options') is None
        assert settings.tracing_options == expected_opts


@pytest.mark.testgen_exclude('acouchbase',
                             reason='drives ClusterSettings, which every api tree imports from couchbase')
class ConfigProfileUnitTests:
    # The two profile tests that read their timeouts back off a live connection are in
    # ConnectionTestSuite below, because that read is all they need a connection for.

    def test_config_profile_fail(self) -> None:
        opts = ClusterOptions(PasswordAuthenticator('Administrator', 'password'))
        with pytest.raises(InvalidArgumentException):
            opts.apply_profile('test_profile')

    def test_custom_config_profile_fail(self) -> None:
        class NotAProfile:
            def apply(self, options: ClusterOptions) -> None:
                options['kv_timeout'] = timedelta(seconds=5)

        # Registration checks the base class, not the shape: NotAProfile has the apply() the
        # registry will call and is still rejected.
        with pytest.raises(InvalidArgumentException):
            CONFIG_PROFILES.register_profile('test_profile', NotAProfile())  # type: ignore[arg-type]


@pytest.mark.testgen_exclude('acouchbase',
                             reason='drives ClusterSettings, which every api tree imports from couchbase')
class ConnectionStringUnitTests:
    # Nothing here swallows an exception.  A body wrapped in `except CouchbaseException:
    # pass` passes having asserted nothing on the day the SDK starts rejecting a string it
    # should accept.  Measured across all 15 strings below: none raises today.

    AUTH = PasswordAuthenticator('Administrator', 'password')

    @pytest.mark.parametrize('conn_str', ['http://host1,http://host2',
                                          'https://host2:8091,host3:8091',
                                          'http://::ffff:00ee:2122',
                                          'couchbase://12345:qwer###/default'])
    def test_invalid_connection_strings(self, conn_str: str) -> None:
        # The one test in this class that opens a socket, and it needs no test cluster: three
        # of the four reach a 1 second bootstrap timeout against hosts that do not resolve.
        # No `flaky` marker: if a search domain ever resolves `host1`, so does every rerun.
        with pytest.raises((InvalidArgumentException, UnAmbiguousTimeoutException)):
            Cluster(conn_str, authenticator=self.AUTH, bootstrap_timeout=timedelta(seconds=1))

    @pytest.mark.parametrize('conn_str', ['10.0.0.1:8091',
                                          'http://10.0.0.1',
                                          'couchbase://10.0.0.1',
                                          'couchbases://10.0.0.1:11222,10.0.0.2,10.0.0.3:11207',
                                          'couchbase://10.0.0.1;10.0.0.2:11210;10.0.0.3',
                                          'couchbase://[3ffe:2a00:100:7031::1]',
                                          'couchbases://[::ffff:192.168.0.1]:11207,[::ffff:192.168.0.2]:11207',
                                          'couchbase://test.local:11210?key=value',
                                          'http://fqdn',
                                          'http://fqdn?key=value',
                                          'couchbases://fqdn'])
    def test_valid_connection_strings(self, conn_str: str) -> None:
        expected_opts = {'timeout_options': {'bootstrap_timeout': 1000}, 'setup_sdk_tracing': True}
        if conn_str == '10.0.0.1:8091':
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter('always')
                settings = _settings(conn_str, authenticator=self.AUTH, bootstrap_timeout=timedelta(seconds=1))
            # A bare host and port with no scheme is the deprecated spelling, and it is the
            # only one of the eleven that warns.
            assert len(caught) == 1
            assert issubclass(caught[-1].category, DeprecationWarning)
            assert 'deprecated' in str(caught[-1].message)
        else:
            settings = _settings(conn_str, authenticator=self.AUTH, bootstrap_timeout=timedelta(seconds=1))

        cluster_options = dict(settings.cluster_options)
        assert cluster_options.pop('user_agent_extra', None) == USER_AGENT
        assert cluster_options == expected_opts
        # The query string is consumed into options and does not survive on the connstr.
        assert settings.connstr == conn_str.split('?')[0]

    @pytest.mark.parametrize(
        'conn_str, expected_opts',
        [('couchbase://10.0.0.1?num_io_threads=1&dump_configuration=true&enable_lazy_connections=true',
          {'num_io_threads': 1, 'dump_configuration': True, 'enable_lazy_connections': True}),
         ('couchbase://10.0.0.1?max_http_connections=4&disable_mozilla_ca_certificates=False',
          {'max_http_connections': 4, 'disable_mozilla_ca_certificates': False}),
         ('couchbase://localhost?enable_app_telemetry=true&app_telemetry_endpoint=ws://localhost:8091',
          {'enable_app_telemetry': True, 'app_telemetry_endpoint': 'ws://localhost:8091'}),
         ('couchbase://10.0.0.1?an_invalid_option=10', {})])
    def test_connection_string_options(self, conn_str: str, expected_opts: dict[str, Any]) -> None:
        settings = _settings(conn_str, authenticator=self.AUTH)
        cluster_options = dict(settings.cluster_options)
        assert cluster_options.pop('user_agent_extra', None) == USER_AGENT
        # An unrecognised query option is dropped rather than raising, which is the last case.
        assert cluster_options == dict(expected_opts, setup_sdk_tracing=True)
        assert settings.connstr == conn_str.split('?')[0]


@pytest.mark.testgen_exclude('txcouchbase',
                             reason='txcouchbase.Cluster has no connect() classmethod')
class ConnectionTestSuite(SuiteRequirements):
    # The rest of this file asserts what the SDK hands the core.  These seven assert what
    # comes back out of `cluster._impl.get_connection_info()`, which is the only reason any
    # of them needs a connection; it is INTERNAL in both impls, the same deliberate reach
    # cluster_t makes for `cluster._impl.connected`.  Not generated for txcouchbase, whose
    # Cluster has no `connect` classmethod.  Every test closes in a `finally`: since
    # PYCBC-1915 a connection left open leaks observability reporter threads, and this file
    # opens eight.  The `flaky(reruns=5)` markers mean "opens a connection", not "observed
    # flaky".

    NUM_DOCS = 0

    TEST_MANIFEST = [
        'test_cluster_ldap_auth_real',
        'test_cluster_legacy_sasl_mech_force_real',
        'test_cluster_options_core_round_trip',
        'test_cluster_sasl_mech_default',
        'test_custom_config_profile',
        'test_wan_config_profile',
        'test_wan_config_profile_with_auth',
    ]

    WAN_TIMEOUTS = {'bootstrap_timeout': 120000,
                    'resolve_timeout': 20000,
                    'connect_timeout': 20000,
                    'dns_srv_timeout': 20000,
                    'key_value_timeout': 20000,
                    'key_value_durable_timeout': 20000,
                    'view_timeout': 120000,
                    'query_timeout': 120000,
                    'analytics_timeout': 120000,
                    'search_timeout': 120000,
                    'management_timeout': 120000}

    @staticmethod
    def _connect(cb_env: SyncTestEnvironment, *options: ClusterOptions, **kwargs: Any) -> Cluster:
        return Cluster.connect(kwargs.pop('connstr', cb_env.config.get_connection_string()), *options)

    @staticmethod
    def _pw_auth(cb_env: SyncTestEnvironment, **kwargs: Any) -> PasswordAuthenticator:
        username, pw = cb_env.config.get_username_and_pw()
        return PasswordAuthenticator(username, pw, **kwargs)

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_cluster_ldap_auth_real(self, cb_env: SyncTestEnvironment) -> None:
        username, pw = cb_env.config.get_username_and_pw()
        auth = PasswordAuthenticator.ldap_compatible(username, pw)
        cluster: Cluster = self._connect(cb_env, ClusterOptions(auth))
        try:
            credentials = cluster._impl.get_connection_info()['credentials']
            assert credentials['allowed_sasl_mechanisms'] == ['PLAIN']
        finally:
            cluster.close()

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_cluster_legacy_sasl_mech_force_real(self, cb_env: SyncTestEnvironment) -> None:
        conn_string = cb_env.config.get_connection_string()
        cluster: Cluster = self._connect(cb_env,
                                         ClusterOptions(self._pw_auth(cb_env)),
                                         connstr=f'{conn_string}?sasl_mech_force=PLAIN')
        try:
            credentials = cluster._impl.get_connection_info()['credentials']
            assert credentials['allowed_sasl_mechanisms'] == ['PLAIN']
        finally:
            cluster.close()

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_cluster_sasl_mech_default(self, cb_env: SyncTestEnvironment) -> None:
        cluster: Cluster = self._connect(cb_env, ClusterOptions(self._pw_auth(cb_env)))
        try:
            credentials = cluster._impl.get_connection_info()['credentials']
            # The empty list is normalized in, not sent: the binding's cluster_credentials
            # to_py omits the field when the vector is empty as well as when it is unset, so
            # `[]` says the connection restricts no mechanism.  PYCBC-1925 fixed reading it
            # on acouchbase, which is why this test generates there rather than being excluded.
            assert credentials['allowed_sasl_mechanisms'] == []
        finally:
            cluster.close()

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_cluster_options_core_round_trip(self, cb_env: SyncTestEnvironment) -> None:
        # Asserted against what the core holds, not against what was parsed.  Six groups are
        # absent for their own reasons: `trust_store_path` needs TLS and a certificate on
        # disk; `network='external'` can route to unreachable endpoints; the three
        # observability switches are covered against the settings elsewhere;
        # `app_telemetry_endpoint` is meaningless while telemetry is off; and
        # `num_io_threads` is a connection-level parameter rather than a cluster option.
        opts: dict[str, Any] = {
            'enable_tls': False,
            'tls_verify': TLSVerifyMode.NO_VERIFY,
            'ip_protocol': IpProtocol.Any,
            'enable_dns_srv': False,
            'enable_mutation_tokens': True,
            'enable_tcp_keep_alive': True,
            'enable_compression': True,
            'show_queries': True,
            'enable_unordered_execution': False,
            'enable_clustermap_notification': False,
            'disable_mozilla_ca_certificates': False,
            'tcp_keep_alive_interval': timedelta(seconds=30),
            'config_poll_interval': timedelta(seconds=30),
            'config_poll_floor': timedelta(seconds=5),
            'max_http_connections': 5,
            'dump_configuration': True,
            'preferred_server_group': 'test_group',
            'enable_app_telemetry': False,
            'allow_enterprise_analytics': True,
            'app_telemetry_backoff': timedelta(seconds=10),
            'app_telemetry_ping_interval': timedelta(seconds=60),
            'app_telemetry_ping_timeout': timedelta(seconds=5),
        }
        expected = dict(opts,
                        tls_verify='none',
                        use_ip_protocol='any',
                        tcp_keep_alive_interval=30000,
                        config_poll_interval=30000,
                        config_poll_floor=5000,
                        app_telemetry_backoff=10000,
                        app_telemetry_ping_interval=60000,
                        app_telemetry_ping_timeout=5000)
        del expected['ip_protocol']

        cluster: Cluster = self._connect(cb_env, ClusterOptions(self._pw_auth(cb_env), **opts))
        try:
            client_opts = cluster._impl.get_connection_info()
            for key, expected_value in expected.items():
                assert client_opts.get(key) == expected_value, (
                    f'Expected {key}={expected_value!r}, got {client_opts.get(key)!r}')
        finally:
            cluster.close()

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_custom_config_profile(self, cb_env: SyncTestEnvironment) -> None:
        expected_opts = {'bootstrap_timeout': 10000,
                         'resolve_timeout': 2000,
                         'dns_srv_timeout': 5000,
                         'connect_timeout': 5000,
                         'key_value_timeout': 5000,
                         'key_value_durable_timeout': 5000,
                         'view_timeout': 60000,
                         'query_timeout': 60000,
                         'analytics_timeout': 60000,
                         'search_timeout': 60000,
                         'management_timeout': 60000}

        class TestProfile(ConfigProfile):
            def apply(self, options: ClusterOptions) -> None:
                for name in ('kv_timeout', 'kv_durable_timeout', 'dns_srv_timeout', 'connect_timeout'):
                    options[name] = timedelta(seconds=5)
                for name in ('analytics_timeout', 'query_timeout', 'search_timeout',
                             'management_timeout', 'views_timeout'):
                    options[name] = timedelta(seconds=60)

        CONFIG_PROFILES.register_profile('test_profile', TestProfile())
        try:
            opts = ClusterOptions(self._pw_auth(cb_env))
            opts.apply_profile('test_profile')
            cluster: Cluster = self._connect(cb_env, opts)
            try:
                timeout_opts = cluster._impl.get_connection_info()['timeout_options']
                for key, expected_value in expected_opts.items():
                    assert timeout_opts[key] == expected_value, key
            finally:
                cluster.close()
        finally:
            # The registry is process-global, so a failure above would otherwise leave
            # 'test_profile' registered and make ConfigProfileUnitTests.test_config_profile_fail
            # pass for the wrong reason, or fail, depending on order.
            assert isinstance(CONFIG_PROFILES.unregister_profile('test_profile'), ConfigProfile)

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.parametrize('profile', [KnownConfigProfiles.WanDevelopment, 'wan_development'])
    def test_wan_config_profile(self, cb_env: SyncTestEnvironment, profile: Any) -> None:
        # The enum and the bare string are the same profile by two names, which is the case.
        opts = ClusterOptions(self._pw_auth(cb_env))
        opts.apply_profile(profile)
        cluster: Cluster = self._connect(cb_env, opts)
        try:
            timeout_opts = cluster._impl.get_connection_info()['timeout_options']
            for key, expected_value in self.WAN_TIMEOUTS.items():
                assert timeout_opts[key] == expected_value, key
        finally:
            cluster.close()

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.parametrize('profile', [KnownConfigProfiles.WanDevelopment, 'wan_development'])
    def test_wan_config_profile_with_auth(self, cb_env: SyncTestEnvironment, profile: Any) -> None:
        # Same profile reached through the constructor rather than through apply_profile, so
        # the two spellings are pinned to produce the same timeouts.
        cluster: Cluster = self._connect(cb_env,
                                         ClusterOptions.create_options_with_profile(
                                             self._pw_auth(cb_env), profile))
        try:
            timeout_opts = cluster._impl.get_connection_info()['timeout_options']
            for key, expected_value in self.WAN_TIMEOUTS.items():
                assert timeout_opts[key] == expected_value, key
        finally:
            cluster.close()


@pytest.mark.testgen_exclude('txcouchbase',
                             reason='txcouchbase.Cluster has no connect() classmethod')
class ConnectionTests(ConnectionTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(ConnectionTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, ConnectionTestSuite)
        yield env
        teardown_environment(env, ConnectionTestSuite)
