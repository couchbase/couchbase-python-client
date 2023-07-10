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

import warnings
from copy import copy
from datetime import timedelta

import pytest

from couchbase.auth import CertificateAuthenticator, PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import (CouchbaseException,
                                  InvalidArgumentException,
                                  UnAmbiguousTimeoutException)
from couchbase.logic.cluster import ClusterLogic
from couchbase.options import (CONFIG_PROFILES,
                               ClusterOptions,
                               ClusterTimeoutOptions,
                               ClusterTracingOptions,
                               ConfigProfile,
                               IpProtocol,
                               KnownConfigProfiles,
                               TLSVerifyMode)
from couchbase.serializer import DefaultJsonSerializer
from couchbase.transcoder import JSONTranscoder
from tests.environments import CollectionType


class ConnectionTestSuite:
    TEST_MANIFEST = [
        'test_cluster_auth_fail',
        'test_cluster_cert_auth',
        'test_cluster_cert_auth_fail',
        'test_cluster_cert_auth_ts_connstr',
        'test_cluster_cert_auth_ts_kwargs',
        'test_cluster_ldap_auth',
        'test_cluster_ldap_auth_real',
        'test_cluster_legacy_sasl_mech_force',
        'test_cluster_legacy_sasl_mech_force_real',
        'test_cluster_legacy_ssl_no_verify',
        'test_cluster_options',
        'test_cluster_pw_auth',
        'test_cluster_pw_auth_with_cert',
        'test_cluster_pw_auth_with_cert_connstr',
        'test_cluster_pw_auth_with_cert_kwargs',
        'test_cluster_sasl_mech_default',
        'test_cluster_sasl_mech_legacy_multiple',
        'test_cluster_sasl_mech_multiple',
        'test_cluster_timeout_options',
        'test_cluster_timeout_options_fail',
        'test_cluster_timeout_options_kwargs',
        'test_cluster_tracing_options',
        'test_cluster_tracing_options_fail',
        'test_cluster_tracing_options_kwargs',
        'test_config_profile_fail',
        'test_custom_config_profile',
        'test_custom_config_profile_fail',
        'test_invalid_connection_strings',
        'test_connection_string_options',
        'test_valid_connection_strings',
        'test_wan_config_profile',
        'test_wan_config_profile_with_auth',
    ]

    def test_cluster_auth_fail(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        with pytest.raises(InvalidArgumentException):
            ClusterLogic(conn_string)

    def test_cluster_cert_auth(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()

        expected_auth = {
            'cert_path': 'path/to/a/cert',
            'key_path': 'path/to/a/key',
            'trust_store_path': 'path/to/truststore'
        }

        auth = CertificateAuthenticator(**expected_auth)
        cluster = ClusterLogic(conn_string, ClusterOptions(auth))
        auth_opts, _ = cluster._get_connection_opts()
        assert auth_opts is not None
        assert isinstance(auth_opts, dict)
        assert auth_opts == expected_auth

        # check as kwargs
        cluster = ClusterLogic(conn_string, authenticator=auth)
        auth_opts, _ = cluster._get_connection_opts()
        assert auth_opts is not None
        assert isinstance(auth_opts, dict)
        assert auth_opts == expected_auth

    def test_cluster_cert_auth_fail(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        ts_path = 'path/to/truststore'
        expected_auth = {
            'cert_path': 'path/to/a/cert',
            'key_path': 'path/to/a/key',
            'trust_store_path': ts_path
        }

        with pytest.raises(InvalidArgumentException):
            auth = CertificateAuthenticator(expected_auth)

        auth_copy = copy(expected_auth)
        auth_copy['cert_path'] = None
        with pytest.raises(InvalidArgumentException):
            auth = CertificateAuthenticator(expected_auth)

        auth_copy = copy(expected_auth)
        auth_copy['cert_path'] = {}
        with pytest.raises(InvalidArgumentException):
            auth = CertificateAuthenticator(expected_auth)

        auth_copy = copy(expected_auth)
        auth_copy['key_path'] = None
        with pytest.raises(InvalidArgumentException):
            auth = CertificateAuthenticator(expected_auth)

        auth_copy = copy(expected_auth)
        auth_copy['key_path'] = {}
        with pytest.raises(InvalidArgumentException):
            auth = CertificateAuthenticator(expected_auth)

        auth_copy = copy(expected_auth)
        auth_copy['trust_store_path'] = {}
        with pytest.raises(InvalidArgumentException):
            auth = CertificateAuthenticator(expected_auth)

        auth = CertificateAuthenticator(**expected_auth)
        with pytest.raises(InvalidArgumentException):
            ClusterLogic(conn_string, ClusterOptions(auth), trust_store_path=ts_path)

        with pytest.raises(InvalidArgumentException):
            ClusterLogic(conn_string, authenticator=auth, trust_store_path=ts_path)

    def test_cluster_cert_auth_ts_connstr(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()

        expected_auth = {
            'cert_path': 'path/to/a/cert',
            'key_path': 'path/to/a/key'
        }

        auth = CertificateAuthenticator(**expected_auth)
        ts_path = 'path/to/truststore'
        cluster = ClusterLogic(f'{conn_string}?truststorepath={ts_path}', ClusterOptions(auth))
        auth_opts, cluster_opts = cluster._get_connection_opts()
        assert auth_opts is not None
        assert isinstance(auth_opts, dict)
        assert auth_opts == expected_auth
        assert cluster_opts is not None
        assert isinstance(cluster_opts, dict)
        assert 'trust_store_path' in cluster_opts
        assert cluster_opts['trust_store_path'] == ts_path

        # check as kwargs
        cluster = ClusterLogic(f'{conn_string}?truststorepath={ts_path}',
                               authenticator=auth,
                               trust_store_path=ts_path)
        auth_opts, cluster_opts = cluster._get_connection_opts()
        assert auth_opts is not None
        assert isinstance(auth_opts, dict)
        assert auth_opts == expected_auth
        assert cluster_opts is not None
        assert isinstance(cluster_opts, dict)
        assert 'trust_store_path' in cluster_opts
        assert cluster_opts['trust_store_path'] == ts_path

    def test_cluster_cert_auth_ts_kwargs(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()

        expected_auth = {
            'cert_path': 'path/to/a/cert',
            'key_path': 'path/to/a/key'
        }

        auth = CertificateAuthenticator(**expected_auth)
        ts_path = 'path/to/truststore'
        cluster = ClusterLogic(conn_string, ClusterOptions(auth), trust_store_path=ts_path)
        auth_opts, cluster_opts = cluster._get_connection_opts()
        assert auth_opts is not None
        assert isinstance(auth_opts, dict)
        assert auth_opts == expected_auth
        assert cluster_opts is not None
        assert isinstance(cluster_opts, dict)
        assert 'trust_store_path' in cluster_opts
        assert cluster_opts['trust_store_path'] == ts_path

        # check as kwargs
        cluster = ClusterLogic(conn_string, authenticator=auth, trust_store_path=ts_path)
        auth_opts, cluster_opts = cluster._get_connection_opts()
        assert auth_opts is not None
        assert isinstance(auth_opts, dict)
        assert auth_opts == expected_auth
        assert cluster_opts is not None
        assert isinstance(cluster_opts, dict)
        assert 'trust_store_path' in cluster_opts
        assert cluster_opts['trust_store_path'] == ts_path

    def test_cluster_ldap_auth(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        auth = PasswordAuthenticator.ldap_compatible(username, pw)
        assert isinstance(auth, PasswordAuthenticator)
        expected_auth = auth.as_dict()
        cluster = ClusterLogic(conn_string, ClusterOptions(auth))
        auth_opts = cluster._get_connection_opts(auth_only=True)
        assert auth_opts is not None
        assert isinstance(auth_opts, dict)
        assert auth_opts == expected_auth

    # creating a new connection, allow retries
    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_cluster_ldap_auth_real(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        auth = PasswordAuthenticator.ldap_compatible(username, pw)
        cluster = Cluster.connect(conn_string, ClusterOptions(auth))

        client_opts = cluster._get_client_connection_info()
        assert client_opts['credentials'] is not None
        assert client_opts['credentials']['allowed_sasl_mechanisms'] == ['PLAIN']

    def test_cluster_legacy_sasl_mech_force(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        auth = PasswordAuthenticator(username, pw)
        expected_auth = auth.as_dict()
        expected_auth['allowed_sasl_mechanisms'] = ['PLAIN']
        cluster = ClusterLogic(f'{conn_string}?sasl_mech_force=PLAIN', ClusterOptions(auth))
        auth_opts, cluster_opts = cluster._get_connection_opts()
        assert cluster_opts is not None
        assert auth_opts is not None
        assert auth_opts == expected_auth

    # creating a new connection, allow retries
    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_cluster_legacy_sasl_mech_force_real(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        auth = PasswordAuthenticator(username, pw)
        cluster = Cluster.connect(f'{conn_string}?sasl_mech_force=PLAIN', ClusterOptions(auth))

        client_opts = cluster._get_client_connection_info()
        assert client_opts['credentials'] is not None
        assert client_opts['credentials']['allowed_sasl_mechanisms'] == ['PLAIN']

    def test_cluster_legacy_ssl_no_verify(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        auth = PasswordAuthenticator(username, pw)
        cluster = ClusterLogic(f'{conn_string}?ssl=no_verify', ClusterOptions(auth))
        cluster_opts = cluster._get_connection_opts(conn_only=True)
        assert cluster_opts is not None
        assert cluster_opts['tls_verify'] == 'none'

    def test_cluster_options(self, couchbase_config):
        opts = {
            "enable_tls": True,
            "enable_mutation_tokens": True,
            "enable_tcp_keep_alive": True,
            "ip_protocol":  IpProtocol.Any,
            "enable_dns_srv": True,
            "show_queries": True,
            "enable_unordered_execution": True,
            "enable_clustermap_notification": True,
            "enable_compression": True,
            "enable_tracing": True,
            "enable_metrics": True,
            "network": 'external',
            "tls_verify": TLSVerifyMode.NO_VERIFY,
            "disable_mozilla_ca_certificates": False,
            "serializer": DefaultJsonSerializer(),
            "transcoder": JSONTranscoder(),
            "tcp_keep_alive_interval": timedelta(seconds=30),
            "config_poll_interval": timedelta(seconds=30),
            "config_poll_floor": timedelta(seconds=30),
            "max_http_connections": 10,
            "logging_meter_emit_interval": timedelta(seconds=30),
            "num_io_threads": 1,
            'dump_configuration': True,
        }

        expected_opts = copy(opts)
        # serializer is not passed to connection options
        expected_opts.pop('serializer')
        # transcoder is not passed to connection options
        expected_opts.pop('transcoder')
        # timedeltas are translated to microseconds
        expected_opts['tcp_keep_alive_interval'] = 30000000
        expected_opts['config_poll_interval'] = 30000000
        expected_opts['config_poll_floor'] = 30000000
        # IpProtocol is translated to string and has another name
        expected_opts.pop('ip_protocol')
        expected_opts['use_ip_protocol'] = 'any'
        # TLSVerifyMode is translated to string
        expected_opts['tls_verify'] = 'none'
        # change name of logging meter emit int.
        expected_opts.pop('logging_meter_emit_interval')
        expected_opts['emit_interval'] = 30000000

        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        auth = PasswordAuthenticator(username, pw)

        # check via ClusterOptions
        cluster_opts = ClusterOptions(auth, **opts)
        cluster = ClusterLogic(conn_string, cluster_opts)
        cluster_opts = cluster._get_connection_opts(conn_only=True)
        user_agent = cluster_opts.pop('user_agent_extra', None)
        assert cluster_opts is not None
        assert isinstance(cluster_opts, dict)
        assert cluster_opts == expected_opts
        assert user_agent is not None
        assert 'pycbc/' in user_agent
        assert 'python/' in user_agent

        # check via kwargs
        cluster_opts = ClusterOptions(auth)
        cluster = ClusterLogic(conn_string, cluster_opts, **opts)
        cluster_opts = cluster._get_connection_opts(conn_only=True)
        user_agent = cluster_opts.pop('user_agent_extra', None)
        assert cluster_opts is not None
        assert isinstance(cluster_opts, dict)
        assert cluster_opts == expected_opts
        assert user_agent is not None
        assert 'pycbc/' in user_agent
        assert 'python/' in user_agent

    def test_cluster_pw_auth(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        auth = PasswordAuthenticator(username, pw)
        expected_auth = auth.as_dict()
        cluster = ClusterLogic(conn_string, ClusterOptions(auth))
        auth_opts = cluster._get_connection_opts(auth_only=True)
        assert auth_opts is not None
        assert isinstance(auth_opts, dict)
        assert auth_opts == expected_auth
        assert auth_opts['allowed_sasl_mechanisms'] is None

        # check as kwargs
        cluster = ClusterLogic(conn_string, authenticator=auth)
        auth_opts = cluster._get_connection_opts(auth_only=True)
        assert auth_opts is not None
        assert isinstance(auth_opts, dict)
        assert auth_opts == expected_auth
        assert auth_opts['allowed_sasl_mechanisms'] is None

    def test_cluster_pw_auth_with_cert(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        auth = PasswordAuthenticator(username, pw, cert_path='path/to/cert')
        expected_auth = auth.as_dict()
        cluster = ClusterLogic(conn_string, ClusterOptions(auth))
        auth_opts = cluster._get_connection_opts(auth_only=True)
        assert auth_opts is not None
        assert isinstance(auth_opts, dict)
        assert auth_opts == expected_auth

        # check as kwargs
        cluster = ClusterLogic(conn_string, authenticator=auth)
        auth_opts = cluster._get_connection_opts(auth_only=True)
        assert auth_opts is not None
        assert isinstance(auth_opts, dict)
        assert auth_opts == expected_auth

    def test_cluster_pw_auth_with_cert_connstr(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        auth = PasswordAuthenticator(username, pw)
        cert_path = 'path/to/cert'
        expected_auth = auth.as_dict()
        expected_auth['cert_path'] = cert_path
        cluster = ClusterLogic(f'{conn_string}?certpath={cert_path}', ClusterOptions(auth))
        auth_opts = cluster._get_connection_opts(auth_only=True)
        assert auth_opts is not None
        assert isinstance(auth_opts, dict)
        assert auth_opts == expected_auth

    def test_cluster_pw_auth_with_cert_kwargs(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        auth = PasswordAuthenticator(username, pw)
        cert_path = 'path/to/cert'
        expected_auth = auth.as_dict()
        expected_auth['cert_path'] = cert_path
        cluster = ClusterLogic(conn_string, ClusterOptions(auth), cert_path=cert_path)
        auth_opts = cluster._get_connection_opts(auth_only=True)
        assert auth_opts is not None
        assert isinstance(auth_opts, dict)
        assert auth_opts == expected_auth

        # check as kwargs
        cluster = ClusterLogic(conn_string, authenticator=auth, cert_path=cert_path)
        auth_opts = cluster._get_connection_opts(auth_only=True)
        assert auth_opts is not None
        assert isinstance(auth_opts, dict)
        assert auth_opts == expected_auth

    # creating a new connection, allow retries
    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_cluster_sasl_mech_default(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        auth = PasswordAuthenticator(username, pw)
        cluster = Cluster.connect(f'{conn_string}', ClusterOptions(auth))

        client_opts = cluster._get_client_connection_info()
        assert client_opts['credentials'] is not None
        assert client_opts['credentials']['allowed_sasl_mechanisms'] == []

    def test_cluster_sasl_mech_legacy_multiple(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        auth = PasswordAuthenticator(username, pw)
        expected_auth = auth.as_dict()
        expected_auth['allowed_sasl_mechanisms'] = ['SCRAM-SHA512', 'SCRAM-SHA256']
        conn_str = f'{conn_string}?sasl_mech_force=SCRAM-SHA512&sasl_mech_force=SCRAM-SHA256'
        cluster = ClusterLogic(conn_str, ClusterOptions(auth))
        auth_opts, cluster_opts = cluster._get_connection_opts()
        assert cluster_opts is not None
        assert auth_opts is not None
        assert auth_opts == expected_auth

        cluster = ClusterLogic(f'{conn_string}?sasl_mech_force=SCRAM-SHA512,SCRAM-SHA256', ClusterOptions(auth))
        auth_opts, cluster_opts = cluster._get_connection_opts()
        assert cluster_opts is not None
        assert auth_opts is not None
        assert auth_opts == expected_auth

    def test_cluster_sasl_mech_multiple(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        auth = PasswordAuthenticator(username, pw)
        expected_auth = auth.as_dict()
        expected_auth['allowed_sasl_mechanisms'] = ['SCRAM-SHA512', 'SCRAM-SHA256']
        conn_str = f'{conn_string}?allowed_sasl_mechanisms=SCRAM-SHA512&allowed_sasl_mechanisms=SCRAM-SHA256'
        cluster = ClusterLogic(conn_str, ClusterOptions(auth))
        auth_opts, cluster_opts = cluster._get_connection_opts()
        assert cluster_opts is not None
        assert auth_opts is not None
        assert auth_opts == expected_auth

        cluster = ClusterLogic(f'{conn_string}?allowed_sasl_mechanisms=SCRAM-SHA512,SCRAM-SHA256', ClusterOptions(auth))
        auth_opts, cluster_opts = cluster._get_connection_opts()
        assert cluster_opts is not None
        assert auth_opts is not None
        assert auth_opts == expected_auth

    @pytest.mark.parametrize('opts, expected_opts',
                             [({'bootstrap_timeout': timedelta(seconds=30)},
                               {'bootstrap_timeout': 30000000}),
                              ({'resolve_timeout': timedelta(seconds=30)},
                               {'resolve_timeout': 30000000}),
                              ({'connect_timeout': timedelta(seconds=30)},
                               {'connect_timeout': 30000000}),
                              ({'kv_timeout': timedelta(seconds=30)},
                               {'key_value_timeout': 30000000}),
                              ({'kv_durable_timeout': timedelta(seconds=30)},
                               {'key_value_durable_timeout': 30000000}),
                              ({'views_timeout': timedelta(seconds=30)},
                               {'view_timeout': 30000000}),
                              ({'query_timeout': timedelta(seconds=30)},
                               {'query_timeout': 30000000}),
                              ({'analytics_timeout': timedelta(seconds=30)},
                               {'analytics_timeout': 30000000}),
                              ({'search_timeout': timedelta(seconds=30)},
                               {'search_timeout': 30000000}),
                              ({'management_timeout': timedelta(seconds=30)},
                               {'management_timeout': 30000000}),
                              ({'dns_srv_timeout': timedelta(seconds=30)},
                               {'dns_srv_timeout': 30000000}),
                              ({'idle_http_connection_timeout': timedelta(seconds=30)},
                               {'idle_http_connection_timeout': 30000000}),
                              ({'config_idle_redial_timeout': timedelta(seconds=30)},
                               {'config_idle_redial_timeout': 30000000}),
                              ({'bootstrap_timeout': timedelta(seconds=60),
                                'kv_timeout': timedelta(seconds=5),
                                'query_timeout': timedelta(seconds=30),
                                'search_timeout': timedelta(seconds=120),
                                'management_timeout': timedelta(seconds=60)},
                               {'bootstrap_timeout': 60000000,
                               'key_value_timeout': 5000000,
                                'query_timeout': 30000000,
                                'search_timeout': 120000000,
                                'management_timeout': 60000000}),
                              ])
    def test_cluster_timeout_options(self, couchbase_config, opts, expected_opts):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        auth = PasswordAuthenticator(username, pw)
        cluster_opts = ClusterOptions(auth, timeout_options=ClusterTimeoutOptions(**opts))
        cluster = ClusterLogic(conn_string, cluster_opts)
        cluster_opts = cluster._get_connection_opts(conn_only=True)
        timeout_opts = cluster_opts.get('timeout_options', None)
        assert timeout_opts is not None
        assert isinstance(timeout_opts, dict)
        assert timeout_opts == expected_opts

    @pytest.mark.parametrize('opts',
                             [
                                 {'bootstrap_timeout': 30},
                                 {'resolve_timeout': 30},
                                 {'connect_timeout': 30},
                                 {'kv_timeout': 30},
                                 {'kv_durable_timeout': 30},
                                 {'views_timeout': 30},
                                 {'query_timeout': 30},
                                 {'analytics_timeout': 30},
                                 {'search_timeout': 30},
                                 {'management_timeout': 30},
                                 {'dns_srv_timeout': 30},
                                 {'idle_http_connection_timeout': 30},
                                 {'config_idle_redial_timeout': 30},
                                 {'bootstrap_timeout': 60000000,
                                  'kv_timeout': 5000000,
                                  'query_timeout': 30000000,
                                  'search_timeout': 120000000,
                                  'management_timeout': 60000000}
                             ])
    def test_cluster_timeout_options_fail(self, couchbase_config, opts):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        auth = PasswordAuthenticator(username, pw)
        cluster_opts = ClusterOptions(auth)
        with pytest.raises(InvalidArgumentException):
            ClusterLogic(conn_string, cluster_opts, **opts)

        cluster_opts = ClusterOptions(auth, timeout_options=opts)
        with pytest.raises(InvalidArgumentException):
            ClusterLogic(conn_string, cluster_opts)

    @pytest.mark.parametrize('opts, expected_opts',
                             [({'bootstrap_timeout': timedelta(seconds=30)},
                               {'bootstrap_timeout': 30000000}),
                              ({'resolve_timeout': timedelta(seconds=30)},
                               {'resolve_timeout': 30000000}),
                              ({'connect_timeout': timedelta(seconds=30)},
                               {'connect_timeout': 30000000}),
                              ({'kv_timeout': timedelta(seconds=30)},
                               {'key_value_timeout': 30000000}),
                              ({'kv_durable_timeout': timedelta(seconds=30)},
                               {'key_value_durable_timeout': 30000000}),
                              ({'views_timeout': timedelta(seconds=30)},
                               {'view_timeout': 30000000}),
                              ({'query_timeout': timedelta(seconds=30)},
                               {'query_timeout': 30000000}),
                              ({'analytics_timeout': timedelta(seconds=30)},
                               {'analytics_timeout': 30000000}),
                              ({'search_timeout': timedelta(seconds=30)},
                               {'search_timeout': 30000000}),
                              ({'management_timeout': timedelta(seconds=30)},
                               {'management_timeout': 30000000}),
                              ({'dns_srv_timeout': timedelta(seconds=30)},
                               {'dns_srv_timeout': 30000000}),
                              ({'idle_http_connection_timeout': timedelta(seconds=30)},
                               {'idle_http_connection_timeout': 30000000}),
                              ({'config_idle_redial_timeout': timedelta(seconds=30)},
                               {'config_idle_redial_timeout': 30000000}),
                              ({'bootstrap_timeout': timedelta(seconds=60),
                                'kv_timeout': timedelta(seconds=5),
                                'query_timeout': timedelta(seconds=30),
                                'search_timeout': timedelta(seconds=120),
                                'management_timeout': timedelta(seconds=60)},
                               {'bootstrap_timeout': 60000000,
                               'key_value_timeout': 5000000,
                                'query_timeout': 30000000,
                                'search_timeout': 120000000,
                                'management_timeout': 60000000}),
                              ])
    def test_cluster_timeout_options_kwargs(self, couchbase_config, opts, expected_opts):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        auth = PasswordAuthenticator(username, pw)
        cluster_opts = ClusterOptions(auth)
        cluster = ClusterLogic(conn_string, cluster_opts, **opts)
        cluster_opts = cluster._get_connection_opts(conn_only=True)
        timeout_opts = cluster_opts.get('timeout_options', None)
        assert timeout_opts is not None
        assert isinstance(timeout_opts, dict)
        assert timeout_opts == expected_opts

    @pytest.mark.parametrize('opts, expected_opts',
                             [({'tracing_threshold_kv': timedelta(milliseconds=30)},
                               {'key_value_threshold': 30000}),
                              ({'tracing_threshold_view': timedelta(milliseconds=30)},
                              {'view_threshold': 30000}),
                              ({'tracing_threshold_query': timedelta(milliseconds=30)},
                              {'query_threshold': 30000}),
                              ({'tracing_threshold_search': timedelta(milliseconds=30)},
                              {'search_threshold': 30000}),
                              ({'tracing_threshold_analytics': timedelta(milliseconds=30)},
                              {'analytics_threshold': 30000}),
                              ({'tracing_threshold_eventing': timedelta(milliseconds=30)},
                              {'eventing_threshold': 30000}),
                              ({'tracing_threshold_management': timedelta(milliseconds=30)},
                              {'management_threshold': 30000}),
                              ({'tracing_threshold_queue_size': 20},
                              {'threshold_sample_size': 20}),
                              ({'tracing_threshold_queue_flush_interval': timedelta(
                                  seconds=30)}, {'threshold_emit_interval': 30000000}),
                              ({'tracing_orphaned_queue_size': 20},
                              {'orphaned_sample_size': 20}),
                              ({'tracing_orphaned_queue_flush_interval': timedelta(
                                  seconds=30)}, {'orphaned_emit_interval': 30000000}),
                              ({'tracing_threshold_kv': timedelta(milliseconds=60),
                                'tracing_threshold_query': timedelta(milliseconds=5),
                                'tracing_threshold_management': timedelta(milliseconds=30),
                                'tracing_threshold_queue_size': 20},
                               {'key_value_threshold': 60000,
                               'query_threshold': 5000,
                                'management_threshold': 30000,
                                'threshold_sample_size': 20}),
                              ])
    def test_cluster_tracing_options(self, couchbase_config, opts, expected_opts):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        auth = PasswordAuthenticator(username, pw)
        cluster_opts = ClusterOptions(auth, tracing_options=ClusterTracingOptions(**opts))
        cluster = ClusterLogic(conn_string, cluster_opts)
        cluster_opts = cluster._get_connection_opts(conn_only=True)
        tracing_options = cluster_opts.get('tracing_options', None)
        assert tracing_options is not None
        assert isinstance(tracing_options, dict)
        assert tracing_options == expected_opts

    @pytest.mark.parametrize('opts',
                             [
                                 {'tracing_threshold_kv': 30},
                                 {'tracing_threshold_view': 30},
                                 {'tracing_threshold_query': 30},
                                 {'tracing_threshold_search': 30},
                                 {'tracing_threshold_analytics': 30},
                                 {'tracing_threshold_eventing': 30},
                                 {'tracing_threshold_management': 30},
                                 {'tracing_threshold_queue_flush_interval': 30},
                                 {'tracing_orphaned_queue_flush_interval': 30},
                                 {'tracing_threshold_kv': 60000000,
                                  'tracing_threshold_query': 5000000,
                                  'tracing_threshold_search': 30000000,
                                  'tracing_threshold_management': 120000000,
                                  'tracing_threshold_queue_flush_interval': 60000000}
                             ])
    def test_cluster_tracing_options_fail(self, couchbase_config, opts):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        auth = PasswordAuthenticator(username, pw)
        cluster_opts = ClusterOptions(auth)
        with pytest.raises(InvalidArgumentException):
            ClusterLogic(conn_string, cluster_opts, **opts)

        cluster_opts = ClusterOptions(auth, tracing_options=opts)
        with pytest.raises(InvalidArgumentException):
            ClusterLogic(conn_string, cluster_opts)

    @pytest.mark.parametrize('opts, expected_opts',
                             [({'tracing_threshold_kv': timedelta(milliseconds=30)},
                               {'key_value_threshold': 30000}),
                              ({'tracing_threshold_view': timedelta(milliseconds=30)},
                               {'view_threshold': 30000}),
                              ({'tracing_threshold_query': timedelta(milliseconds=30)},
                               {'query_threshold': 30000}),
                              ({'tracing_threshold_search': timedelta(milliseconds=30)},
                               {'search_threshold': 30000}),
                              ({'tracing_threshold_analytics': timedelta(milliseconds=30)},
                               {'analytics_threshold': 30000}),
                              ({'tracing_threshold_eventing': timedelta(milliseconds=30)},
                               {'eventing_threshold': 30000}),
                              ({'tracing_threshold_management': timedelta(milliseconds=30)},
                               {'management_threshold': 30000}),
                              ({'tracing_threshold_queue_size': 20},
                               {'threshold_sample_size': 20}),
                              ({'tracing_threshold_queue_flush_interval': timedelta(
                                  seconds=30)}, {'threshold_emit_interval': 30000000}),
                              ({'tracing_orphaned_queue_size': 20},
                               {'orphaned_sample_size': 20}),
                              ({'tracing_orphaned_queue_flush_interval': timedelta(
                                  seconds=30)}, {'orphaned_emit_interval': 30000000}),
                              ({'tracing_threshold_kv': timedelta(milliseconds=60),
                                'tracing_threshold_query': timedelta(milliseconds=5),
                                'tracing_threshold_management': timedelta(milliseconds=30),
                                'tracing_threshold_queue_size': 20},
                               {'key_value_threshold': 60000,
                               'query_threshold': 5000,
                                'management_threshold': 30000,
                                'threshold_sample_size': 20}),
                              ])
    def test_cluster_tracing_options_kwargs(self, couchbase_config, opts, expected_opts):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        auth = PasswordAuthenticator(username, pw)
        cluster_opts = ClusterOptions(auth)
        cluster = ClusterLogic(conn_string, cluster_opts, **opts)
        cluster_opts = cluster._get_connection_opts(conn_only=True)
        tracing_options = cluster_opts.get('tracing_options', None)
        assert tracing_options is not None
        assert isinstance(tracing_options, dict)
        assert tracing_options == expected_opts

    def test_config_profile_fail(self, couchbase_config):
        username, pw = couchbase_config.get_username_and_pw()
        auth = PasswordAuthenticator(username, pw)
        opts = ClusterOptions(auth)
        with pytest.raises(InvalidArgumentException):
            opts.apply_profile('test_profile')

    # creating a new connection, allow retries
    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_custom_config_profile(self, couchbase_config):
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
            def __init__(self):
                super().__init__()

            def apply(self,
                      options  # type: ClusterOptions
                      ) -> None:
                options['kv_timeout'] = timedelta(seconds=5)
                options['kv_durable_timeout'] = timedelta(seconds=5)
                options['dns_srv_timeout'] = timedelta(seconds=5)
                options['connect_timeout'] = timedelta(seconds=5)
                options['analytics_timeout'] = timedelta(seconds=60)
                options['query_timeout'] = timedelta(seconds=60)
                options['search_timeout'] = timedelta(seconds=60)
                options['management_timeout'] = timedelta(seconds=60)
                options['views_timeout'] = timedelta(seconds=60)

        CONFIG_PROFILES.register_profile('test_profile', TestProfile())

        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        auth = PasswordAuthenticator(username, pw)
        opts = ClusterOptions(auth)
        opts.apply_profile('test_profile')
        cluster = Cluster.connect(conn_string, opts)
        client_opts = cluster._get_client_connection_info()
        for k in expected_opts.keys():
            assert client_opts[k] == expected_opts[k]

        profile = CONFIG_PROFILES.unregister_profile('test_profile')
        assert isinstance(profile, ConfigProfile)

    def test_custom_config_profile_fail(self):

        class TestProfile():
            def __init__(self):
                super().__init__()

            def apply(self,
                      options  # type: ClusterOptions
                      ) -> None:
                options['kv_timeout'] = timedelta(seconds=5)
                options['kv_durable_timeout'] = timedelta(seconds=5)
                options['connect_timeout'] = timedelta(seconds=5)
                options['analytics_timeout'] = timedelta(seconds=60)
                options['query_timeout'] = timedelta(seconds=60)
                options['search_timeout'] = timedelta(seconds=60)
                options['management_timeout'] = timedelta(seconds=60)
                options['views_timeout'] = timedelta(seconds=60)

        with pytest.raises(InvalidArgumentException):
            CONFIG_PROFILES.register_profile('test_profile', TestProfile())

    # creating a new connection, allow retries
    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.parametrize("conn_str", ['http://host1,http://host2',
                                          'https://host2:8091,host3:8091',
                                          'http://::ffff:00ee:2122',
                                          'couchbase://12345:qwer###/default'])
    def test_invalid_connection_strings(self, conn_str):
        try:
            Cluster(conn_str, authenticator=PasswordAuthenticator(
                'Administrator', 'password'), bootstrap_timeout=timedelta(seconds=1))
        except (InvalidArgumentException, UnAmbiguousTimeoutException):
            pass
        except Exception as ex:
            pytest.fail(f'Unexpected exception occurred: {ex}')

    @pytest.mark.parametrize("conn_str", ['10.0.0.1:8091',
                                          'http://10.0.0.1',
                                          'couchbase://10.0.0.1',
                                          'couchbases://10.0.0.1:11222,10.0.0.2,10.0.0.3:11207',
                                          'couchbase://10.0.0.1;10.0.0.2:11210;10.0.0.3',
                                          'couchbase://[3ffe:2a00:100:7031::1]',
                                          'couchbases://[::ffff:192.168.0.1]:11207,[::ffff:192.168.0.2]:11207',
                                          'couchbase://test.local:11210?key=value',
                                          'http://fqdn',
                                          'http://fqdn?key=value',
                                          'couchbases://fqdn'
                                          ])
    def test_valid_connection_strings(self, conn_str):
        expected_opts = {'timeout_options': {'bootstrap_timeout': 1000000}}
        try:
            if conn_str == '10.0.0.1:8091':
                with warnings.catch_warnings(record=True) as w:
                    # Cause all warnings to always be triggered.
                    warnings.simplefilter("always")

                    cl = ClusterLogic(conn_str, authenticator=PasswordAuthenticator(
                        'Administrator', 'password'), bootstrap_timeout=timedelta(seconds=1))
                    assert len(w) == 1
                    assert issubclass(w[-1].category, DeprecationWarning)
                    assert "deprecated" in str(w[-1].message)
            else:
                cl = ClusterLogic(conn_str, authenticator=PasswordAuthenticator(
                    'Administrator', 'password'), bootstrap_timeout=timedelta(seconds=1))

            user_agent = cl._cluster_opts.pop('user_agent_extra', None)
            assert expected_opts == cl._cluster_opts
            assert user_agent is not None
            assert 'pycbc/' in user_agent
            assert 'python/' in user_agent
            expected_conn_str = conn_str.split('?')[0]
            assert expected_conn_str == cl._connstr
        except CouchbaseException:
            pass
        except Exception as ex:
            pytest.fail(f'Unexpected exception occurred: {ex}')

    @pytest.mark.parametrize('conn_str, expected_opts',
                             [('couchbase://10.0.0.1?num_io_threads=1&dump_configuration=true',
                               {'num_io_threads': 1, 'dump_configuration': True}),
                              ('couchbase://10.0.0.1?max_http_connections=4&disable_mozilla_ca_certificates=False',
                               {'max_http_connections': 4, 'disable_mozilla_ca_certificates': False}),
                              ('couchbase://10.0.0.1?an_invalid_option=10',
                               {}),
                              ])
    def test_connection_string_options(self, conn_str, expected_opts):
        try:
            cl = ClusterLogic(conn_str, authenticator=PasswordAuthenticator('Administrator', 'password'))

            user_agent = cl._cluster_opts.pop('user_agent_extra', None)
            assert expected_opts == cl._cluster_opts
            assert user_agent is not None
            assert 'pycbc/' in user_agent
            assert 'python/' in user_agent
            expected_conn_str = conn_str.split('?')[0]
            assert expected_conn_str == cl._connstr
        except CouchbaseException:
            pass
        except Exception as ex:
            pytest.fail(f'Unexpected exception occurred: {ex}')

    # creating a new connection, allow retries
    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.parametrize('profile',
                             [KnownConfigProfiles.WanDevelopment, 'wan_development'])
    def test_wan_config_profile(self, couchbase_config, profile):
        expected_opts = {'bootstrap_timeout': 120000,
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

        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        auth = PasswordAuthenticator(username, pw)
        opts = ClusterOptions(auth)
        opts.apply_profile(profile)
        cluster = Cluster.connect(conn_string, opts)
        client_opts = cluster._get_client_connection_info()
        for k in expected_opts.keys():
            assert client_opts[k] == expected_opts[k]

    # creating a new connection, allow retries
    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.parametrize('profile',
                             [KnownConfigProfiles.WanDevelopment, 'wan_development'])
    def test_wan_config_profile_with_auth(self, couchbase_config, profile):
        expected_opts = {'bootstrap_timeout': 120000,
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

        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        auth = PasswordAuthenticator(username, pw)
        cluster = Cluster.connect(conn_string, ClusterOptions.create_options_with_profile(auth, profile))
        client_opts = cluster._get_client_connection_info()
        for k in expected_opts.keys():
            assert client_opts[k] == expected_opts[k]


class ClassicConnectionTests(ConnectionTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicConnectionTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicConnectionTests) if valid_test_method(meth)]
        test_list = set(ConnectionTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT])
    def couchbase_test_environment(self, cb_base_env, request):
        cb_base_env.setup(request.param)
        yield cb_base_env
        cb_base_env.teardown(request.param)
