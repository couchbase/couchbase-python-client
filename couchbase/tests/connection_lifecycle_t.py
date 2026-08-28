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

"""
The close paths, held to one lifecycle model: an operation raises on a cluster that
never connected and on one that is closed, and a close is idempotent and never needs a
connection in order to tear one down.

No live cluster is needed.  Every cluster here is built with
skip_connect='TEST_SKIP_CONNECT', the mechanism connection_t.py uses, so it is in the
never-connected state without touching the network.
"""

import pytest

from couchbase.auth import PasswordAuthenticator
from couchbase.logic.cluster_impl import ClusterImpl
from couchbase.options import ClusterOptions

BUCKET_NAME = 'default'


class ConnectionLifecycleTestSuite:
    TEST_MANIFEST = [
        'test_close_bucket_after_cluster_close_is_a_noop',
        'test_close_bucket_when_never_connected_is_a_noop',
        'test_close_connection_is_idempotent',
        'test_close_connection_when_never_connected_marks_closed',
        'test_operation_raises_after_close',
        'test_operation_raises_when_never_connected',
    ]

    @pytest.fixture(name='unconnected_cluster')
    def cluster_without_core_connection(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        return ClusterImpl(conn_string,
                           ClusterOptions(PasswordAuthenticator(username, pw)),
                           skip_connect='TEST_SKIP_CONNECT')

    def test_operation_raises_when_never_connected(self, unconnected_cluster):
        adapter = unconnected_cluster.client_adapter

        with pytest.raises(RuntimeError):
            adapter._ensure_connected()

    def test_operation_raises_after_close(self, unconnected_cluster):
        adapter = unconnected_cluster.client_adapter
        adapter.close_connection()

        with pytest.raises(RuntimeError):
            adapter._ensure_not_closed()

    def test_close_connection_when_never_connected_marks_closed(self, unconnected_cluster):
        adapter = unconnected_cluster.client_adapter
        adapter.close_connection()

        assert adapter._closed is True

    def test_close_connection_is_idempotent(self, unconnected_cluster):
        adapter = unconnected_cluster.client_adapter
        adapter.close_connection()
        adapter.close_connection()

        assert adapter._closed is True

    def test_close_bucket_when_never_connected_is_a_noop(self, unconnected_cluster):
        unconnected_cluster.client_adapter.close_bucket(BUCKET_NAME)

    def test_close_bucket_after_cluster_close_is_a_noop(self, unconnected_cluster):
        adapter = unconnected_cluster.client_adapter
        adapter.close_connection()

        adapter.close_bucket(BUCKET_NAME)


class ClassicConnectionLifecycleTests(ConnectionLifecycleTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self):
        def valid_test_method(meth):
            attr = getattr(ClassicConnectionLifecycleTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicConnectionLifecycleTests) if valid_test_method(meth)]
        manifest_invalid = set(ConnectionLifecycleTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if manifest_invalid:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {manifest_invalid}.')
