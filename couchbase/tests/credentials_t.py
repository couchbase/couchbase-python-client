#  Copyright 2016-2025. Couchbase, Inc.
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

import uuid

import pytest

from couchbase.auth import (CertificateAuthenticator,
                            JwtAuthenticator,
                            PasswordAuthenticator)
from couchbase.cluster import Cluster
from couchbase.exceptions import InvalidArgumentException
from couchbase.options import ClusterOptions, ClusterTracingOptions


class JwtAuthenticatorUnitTests:
    """Unit tests for JwtAuthenticator that don't require a cluster connection."""

    def test_jwt_authenticator_creation(self):
        token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.test.signature"
        auth = JwtAuthenticator(token)
        assert auth.as_dict() == {'jwt_token': token}

    def test_jwt_authenticator_valid_keys(self):
        auth = JwtAuthenticator("test.jwt.token")
        assert auth.valid_keys() == ['jwt_token']

    def test_jwt_authenticator_rejects_non_string(self):
        with pytest.raises(InvalidArgumentException):
            JwtAuthenticator(12345)

    def test_jwt_authenticator_rejects_none(self):
        with pytest.raises(InvalidArgumentException):
            JwtAuthenticator(None)


class ClassicCredentialsTests:

    def test_set_authenticator_reflected_in_connection_info(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        tracing_opts = ClusterTracingOptions(enable_tracing=False)
        cluster = Cluster.connect(conn_string, ClusterOptions(PasswordAuthenticator(username, pw),
                                                              tracing_options=tracing_opts))

        # capture original
        info_before = cluster._impl.get_connection_info()
        assert 'credentials' in info_before
        orig_creds = info_before['credentials']

        # update to new creds; we only assert that core origin is updated
        new_user = f"pycbc_{uuid.uuid4().hex[:8]}"
        new_pass = f"pw_{uuid.uuid4().hex[:8]}"
        cluster.set_authenticator(PasswordAuthenticator(new_user, new_pass))

        info_after = cluster._impl.get_connection_info()
        assert info_after['credentials']['username'] == new_user
        assert info_after['credentials']['password'] == new_pass

        # restore original to avoid impacting other tests
        cluster.set_authenticator(PasswordAuthenticator(orig_creds.get('username', username),
                                                        orig_creds.get('password', pw)))

        cluster.close()

    def test_set_authenticator_password_to_certificate_fails(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        # Non-TLS connection (expected couchbase://)
        tracing_opts = ClusterTracingOptions(enable_tracing=False)
        cluster = Cluster.connect(conn_string, ClusterOptions(PasswordAuthenticator(username, pw),
                                                              tracing_options=tracing_opts))

        # Core should reject this at validation step, surfacing as InvalidArgumentException
        with pytest.raises(InvalidArgumentException):
            cluster.set_authenticator(CertificateAuthenticator(cert_path='path/to/cert',
                                                               key_path='path/to/key'))

        cluster.close()

    def test_set_authenticator_failure_does_not_change_state(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        tracing_opts = ClusterTracingOptions(enable_tracing=False)
        cluster = Cluster.connect(conn_string, ClusterOptions(PasswordAuthenticator(username, pw),
                                                              tracing_options=tracing_opts))

        # capture original
        info_before = cluster._impl.get_connection_info()
        assert 'credentials' in info_before
        orig_creds = info_before['credentials']

        # attempt to switch to certificate auth on non-TLS connection; expect failure
        with pytest.raises(InvalidArgumentException):
            cluster.set_authenticator(CertificateAuthenticator(cert_path='path/to/cert',
                                                               key_path='path/to/key'))

        # ensure credentials are unchanged after the failed update
        info_after = cluster._impl.get_connection_info()
        assert info_after['credentials'] == orig_creds

        cluster.close()

    def test_set_authenticator_password_to_jwt_fails(self, couchbase_config):
        """RFC: Cannot switch from PasswordAuthenticator to JwtAuthenticator."""
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        cluster = Cluster.connect(conn_string, ClusterOptions(PasswordAuthenticator(username, pw)))

        # RFC: Cannot switch authenticator types
        with pytest.raises(InvalidArgumentException):
            cluster.set_authenticator(JwtAuthenticator("some.jwt.token"))

        cluster.close()
