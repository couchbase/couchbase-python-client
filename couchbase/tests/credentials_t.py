#  Copyright 2016-2024. Couchbase, Inc.
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

from collections.abc import Generator
from typing import Any
from uuid import uuid4

import pytest

from couchbase.auth import (CertificateAuthenticator,
                            JwtAuthenticator,
                            PasswordAuthenticator)
from couchbase.cluster import Cluster
from couchbase.exceptions import InvalidArgumentException
from couchbase.options import ClusterOptions
from tests.environments.test_environment import (SuiteRequirements,
                                                 SyncTestEnvironment,
                                                 build_environment,
                                                 teardown_environment)
from tests.manifest import validate_test_manifest


@pytest.mark.testgen_exclude('acouchbase',
                             reason='JwtAuthenticator is couchbase.auth in every api tree')
class JwtAuthenticatorUnitTests:
    # No TEST_MANIFEST: the manifest is a parity contract between api trees, and
    # `JwtAuthenticator` has no per-tree form.  It shares a file with a suite that does need
    # a cluster deliberately: both halves are about the same object graph.

    TOKEN = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.test.signature'

    def test_jwt_authenticator_creation(self) -> None:
        auth = JwtAuthenticator(self.TOKEN)
        assert auth.as_dict() == {'jwt_token': self.TOKEN}
        # Authenticator subclasses dict and the base constructor is handed as_dict(), so the
        # authenticator *is* its own mapping.  That is what the request builders forward, and
        # it is a separate claim from as_dict() returning the right thing.
        assert auth == {'jwt_token': self.TOKEN}

    def test_jwt_authenticator_valid_keys(self) -> None:
        assert JwtAuthenticator(self.TOKEN).valid_keys() == ['jwt_token']

    @pytest.mark.parametrize('token', [None, 12345, 1.5, b'not.a.str', ['not.a.str'], {'jwt_token': 'x'}])
    def test_jwt_authenticator_rejects_non_string(self, token: Any) -> None:
        # One parametrized test rather than one per input: they all drive the same isinstance
        # check, so None is a case and not a test of its own.  The bytes case is the one worth
        # having, being the input a caller is most likely to arrive with by accident.
        with pytest.raises(InvalidArgumentException):
            JwtAuthenticator(token)


class ClusterCredentialsTestSuite(SuiteRequirements):
    # Not generated for txcouchbase, and the reason is capability rather than shape:
    # `txcouchbase.Cluster` has no `set_authenticator`, and its cluster impl has neither
    # `update_credentials` nor `get_connection_info`.  On the async impl both are plain
    # `def` going through `execute_cluster_request_sync`, so nothing here is awaited except
    # the connect and the close.
    #
    # Only test_set_authenticator_reflected_in_connection_info opens a cluster of its own,
    # and that is the rule: it is the one test that succeeds in changing credentials, so it
    # is the one that must not hold a cluster anything else will use.  Reading
    # `cluster._impl.get_connection_info()` is the same deliberate reach into internal state
    # that cluster_t settles for `cluster._impl.connected`.

    NUM_DOCS = 0

    TEST_MANIFEST = [
        'test_set_authenticator_failure_does_not_change_state',
        'test_set_authenticator_password_to_certificate_fails',
        'test_set_authenticator_password_to_jwt_fails',
        'test_set_authenticator_reflected_in_connection_info',
    ]

    # Never read: a plain connection cannot complete a certificate handshake, and rejecting the
    # swap is what these paths are testing.
    CERT_PATH = 'path/to/cert'
    KEY_PATH = 'path/to/key'

    @staticmethod
    def _connect(cb_env: SyncTestEnvironment) -> Cluster:
        conn_string = cb_env.config.get_connection_string()
        username, pw = cb_env.config.get_username_and_pw()
        return Cluster.connect(conn_string, ClusterOptions(PasswordAuthenticator(username, pw)))

    def test_set_authenticator_reflected_in_connection_info(self, cb_env: SyncTestEnvironment) -> None:
        cluster: Cluster = self._connect(cb_env)
        try:
            assert 'credentials' in cluster._impl.get_connection_info()

            new_user = f'pycbc_{uuid4().hex[:8]}'
            new_pass = f'pw_{uuid4().hex[:8]}'
            cluster.set_authenticator(PasswordAuthenticator(new_user, new_pass))

            # The credentials are never used: nothing here issues an operation after the swap,
            # and a PasswordAuthenticator does not re-authenticate the connections already
            # open.  What is under test is that the core origin now carries them.
            credentials = cluster._impl.get_connection_info()['credentials']
            assert credentials['username'] == new_user
            assert credentials['password'] == new_pass
        finally:
            # A cluster of this test's own needs no credential restore, and closing it in a
            # `finally` is what a restore is reaching for anyway.  A restore written on the
            # success path skips exactly the run that needed it.
            cluster.close()

    def test_set_authenticator_password_to_certificate_fails(self, cb_env: SyncTestEnvironment) -> None:
        # The core rejects the swap during validation.  Nothing on the Python side looks at the
        # authenticator's type: build_update_credential_request forwards as_dict() and no more.
        with pytest.raises(InvalidArgumentException):
            cb_env.cluster.set_authenticator(CertificateAuthenticator(cert_path=self.CERT_PATH,
                                                                      key_path=self.KEY_PATH))

    def test_set_authenticator_password_to_jwt_fails(self, cb_env: SyncTestEnvironment) -> None:
        with pytest.raises(InvalidArgumentException):
            cb_env.cluster.set_authenticator(JwtAuthenticator('some.jwt.token'))

    def test_set_authenticator_failure_does_not_change_state(self, cb_env: SyncTestEnvironment) -> None:
        credentials_before = cb_env.cluster._impl.get_connection_info()['credentials']
        # update_credentials has two side effects and writes the Python-side one second, only
        # once the core call has returned.  So a rejected swap has to leave both views alone,
        # and the Python-side mirror is the one that would drift if that order ever changed.
        auth_before = dict(cb_env.cluster._impl._cluster_settings.auth)

        with pytest.raises(InvalidArgumentException):
            cb_env.cluster.set_authenticator(CertificateAuthenticator(cert_path=self.CERT_PATH,
                                                                      key_path=self.KEY_PATH))

        assert cb_env.cluster._impl.get_connection_info()['credentials'] == credentials_before
        assert cb_env.cluster._impl._cluster_settings.auth == auth_before


class ClusterCredentialsTests(ClusterCredentialsTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(ClusterCredentialsTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, ClusterCredentialsTestSuite)
        yield env
        teardown_environment(env, ClusterCredentialsTestSuite)
