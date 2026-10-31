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

from collections.abc import Generator

import pytest

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.protocol.classic._cluster_impl import ClusterImpl
from tests.environments.test_environment import (SuiteRequirements,
                                                 SyncTestEnvironment,
                                                 TestProtocol,
                                                 build_environment,
                                                 teardown_environment)

# What is checked here is classic's client adapter and the connection it reports.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

BUCKET_NAME = 'default'
CONNSTR = 'couchbase://localhost'


class SyncConnectionLifecycleUnitTests:
    # The model is three rules: an operation raises on a cluster that never connected and on
    # one that is closed, a close is idempotent, and no close needs a connection in order to
    # tear one down.  PYCBC-1928 settled them: cluster close already obeyed all three, and
    # bucket close asserted the connection instead.  No TEST_MANIFEST: the acouchbase
    # counterpart is hand-written rather than generated, reaching the same three rules
    # through a differently named surface.  Nothing connects; every cluster is built with
    # `skip_connect='TEST_SKIP_CONNECT'`, and the adapter is driven directly rather than
    # through `Cluster`, because two of these rules are about guards the public close paths
    # call and never expose.

    @pytest.fixture(name='unconnected_cluster')
    def cluster_without_core_connection(self) -> ClusterImpl:
        return ClusterImpl(CONNSTR,
                           ClusterOptions(PasswordAuthenticator('Administrator', 'password')),
                           skip_connect='TEST_SKIP_CONNECT')

    # Returning is the assertion in both bucket-close tests below: each call raised before
    # PYCBC-1928, so there is nothing to observe afterwards beyond having got that far.
    def test_close_bucket_after_cluster_close_is_a_noop(self, unconnected_cluster: ClusterImpl) -> None:
        adapter = unconnected_cluster.client_adapter
        adapter.close_connection()

        adapter.close_bucket(BUCKET_NAME)

    def test_close_bucket_when_never_connected_is_a_noop(self, unconnected_cluster: ClusterImpl) -> None:
        unconnected_cluster.client_adapter.close_bucket(BUCKET_NAME)

    def test_close_connection_is_idempotent(self, unconnected_cluster: ClusterImpl) -> None:
        adapter = unconnected_cluster.client_adapter
        adapter.close_connection()
        adapter.close_connection()

        assert adapter._closed is True

    def test_close_connection_when_never_connected_marks_closed(self, unconnected_cluster: ClusterImpl) -> None:
        adapter = unconnected_cluster.client_adapter
        adapter.close_connection()

        assert adapter._closed is True

    def test_operation_raises_after_close(self, unconnected_cluster: ClusterImpl) -> None:
        adapter = unconnected_cluster.client_adapter
        adapter.close_connection()

        with pytest.raises(RuntimeError):
            adapter._ensure_not_closed()

    def test_operation_raises_when_never_connected(self, unconnected_cluster: ClusterImpl) -> None:
        with pytest.raises(RuntimeError):
            unconnected_cluster.client_adapter._ensure_connected()


class SyncConnectedPropertyTests(SuiteRequirements):
    # The connect end of the lifecycle, so it opens a real cluster where the class above opens
    # none.  `connected` is classic's report of its core connection and no protocol-neutral
    # interface carries it.

    NUM_DOCS = 0

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, SyncConnectedPropertyTests)
        yield env
        teardown_environment(env, SyncConnectedPropertyTests)

    # `flaky` means "opens a connection", as it does on the close tests in cluster_t.
    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_connected_reflects_connect_and_close(self, cb_env: SyncTestEnvironment) -> None:
        username, pw = cb_env.config.get_username_and_pw()
        cluster = Cluster.connect(cb_env.config.get_connection_string(),
                                  ClusterOptions(PasswordAuthenticator(username, pw)))
        impl = cluster._impl
        assert isinstance(impl, ClusterImpl)
        try:
            assert impl.connected is True
        finally:
            cluster.close()
        assert impl.connected is False
