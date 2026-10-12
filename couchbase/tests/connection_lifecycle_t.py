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

import pytest

from couchbase.auth import PasswordAuthenticator
from couchbase.logic.cluster_impl import ClusterImpl
from couchbase.options import ClusterOptions

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
