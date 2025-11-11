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
import pytest_asyncio

from acouchbase.cluster import Cluster as AsyncCluster
from couchbase.auth import CertificateAuthenticator, PasswordAuthenticator
from couchbase.exceptions import InvalidArgumentException
from couchbase.options import ClusterOptions


class AsyncCredentialsTests:

    @pytest_asyncio.fixture(scope="class")
    async def cb_env(self, couchbase_config):
        class Env:
            def __init__(self, cfg):
                self.cfg = cfg
                self.cluster = None
        env = Env(couchbase_config)
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        env.cluster = await AsyncCluster.connect(conn_string, ClusterOptions(PasswordAuthenticator(username, pw)))
        yield env
        await env.cluster.close()

    @pytest.mark.asyncio
    async def test_update_credentials_async(self, cb_env):
        cluster = cb_env.cluster

        new_user = f"pycbc_{uuid.uuid4().hex[:8]}"
        new_pass = f"pw_{uuid.uuid4().hex[:8]}"
        cluster.update_credentials(PasswordAuthenticator(new_user, new_pass))

        info_after = cluster._get_client_connection_info()
        assert info_after['credentials']['username'] == new_user
        assert info_after['credentials']['password'] == new_pass

    @pytest.mark.asyncio
    async def test_update_to_certificate_auth_without_tls_fails_async(self, cb_env):
        cluster = cb_env.cluster
        # Attempt to switch to certificate auth without TLS should raise
        with pytest.raises(InvalidArgumentException):
            cluster.update_credentials(CertificateAuthenticator(cert_path='path/to/cert',
                                                                key_path='path/to/key'))

    @pytest.mark.asyncio
    async def test_update_credentials_failure_does_not_change_state_async(self, cb_env):
        cluster = cb_env.cluster

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
