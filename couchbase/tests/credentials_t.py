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

from couchbase.auth import CertificateAuthenticator, PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import InvalidArgumentException
from couchbase.options import ClusterOptions


class ClassicCredentialsTests:

    def test_update_credentials_reflected_in_connection_info(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        cluster = Cluster.connect(conn_string, ClusterOptions(PasswordAuthenticator(username, pw)))

        # capture original
        info_before = cluster._get_client_connection_info()
        assert 'credentials' in info_before
        orig_creds = info_before['credentials']

        # update to new (likely invalid) creds; we only assert that core origin is updated
        new_user = f"pycbc_{uuid.uuid4().hex[:8]}"
        new_pass = f"pw_{uuid.uuid4().hex[:8]}"
        cluster.update_credentials(PasswordAuthenticator(new_user, new_pass))

        info_after = cluster._get_client_connection_info()
        assert info_after['credentials']['username'] == new_user
        assert info_after['credentials']['password'] == new_pass

        # restore original to avoid impacting other tests
        cluster.update_credentials(PasswordAuthenticator(orig_creds.get('username', username),
                                                         orig_creds.get('password', pw)))

        cluster.close()

    def test_update_to_certificate_auth_without_tls_fails(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        # Non-TLS connection (expected couchbase://)
        cluster = Cluster.connect(conn_string, ClusterOptions(PasswordAuthenticator(username, pw)))

        # Core should reject this at validation step, surfacing as InvalidArgumentException
        with pytest.raises(InvalidArgumentException):
            cluster.update_credentials(CertificateAuthenticator(cert_path='path/to/cert',
                                                                key_path='path/to/key'))

        cluster.close()

    def test_update_credentials_failure_does_not_change_state(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()

        cluster = Cluster.connect(conn_string, ClusterOptions(PasswordAuthenticator(username, pw)))

        # capture original
        info_before = cluster._get_client_connection_info()
        assert 'credentials' in info_before
        orig_creds = info_before['credentials']

        # attempt to switch to certificate auth on non-TLS connection; expect failure
        with pytest.raises(InvalidArgumentException):
            cluster.update_credentials(CertificateAuthenticator(cert_path='path/to/cert',
                                                                key_path='path/to/key'))

        # ensure credentials are unchanged after the failed update
        info_after = cluster._get_client_connection_info()
        assert info_after['credentials'] == orig_creds

        cluster.close()
