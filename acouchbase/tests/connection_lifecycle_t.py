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
The async close paths, held to the same lifecycle model as the blocking API: an operation
raises on a cluster that never connected and on one that is closed, and a close is
idempotent and never starts a connection in order to tear one down.

The async tree makes the last of those easy to get wrong, because a connect is normally
already in flight by the time close is called, so close has a future to chain onto.
Chaining is right only while that connect may still succeed; a connect that failed leaves
nothing to close, and its error must not come back out of close().

No live cluster is needed.  Every cluster here is built with
skip_connect='TEST_SKIP_CONNECT', so no connect is ever started and the cluster sits in
the never-connected state.
"""

import pytest

from acouchbase.logic.cluster_impl import AsyncClusterImpl
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions

BUCKET_NAME = 'default'


class AsyncConnectionLifecycleTestSuite:
    TEST_MANIFEST = [
        'test_close_bucket_after_cluster_close_is_a_noop',
        'test_close_bucket_when_never_connected_is_a_noop',
        'test_close_connection_is_idempotent',
        'test_close_connection_when_never_connected_does_not_connect',
        'test_close_does_not_surface_a_failed_connect',
        'test_transactions_raises_when_never_connected',
    ]

    @pytest.fixture(name='unconnected_cluster')
    def cluster_without_core_connection(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        return AsyncClusterImpl(conn_string,
                                ClusterOptions(PasswordAuthenticator(username, pw)),
                                skip_connect='TEST_SKIP_CONNECT')

    @pytest.mark.asyncio
    async def test_close_connection_when_never_connected_does_not_connect(self, unconnected_cluster):
        adapter = unconnected_cluster.client_adapter
        adapter._execute_connect_request = _fail_if_called

        await adapter.close_connection()

        assert adapter._closed is True

    @pytest.mark.asyncio
    async def test_close_connection_is_idempotent(self, unconnected_cluster):
        adapter = unconnected_cluster.client_adapter
        adapter._execute_connect_request = _fail_if_called

        await adapter.close_connection()
        await adapter.close_connection()

        assert adapter._closed is True

    @pytest.mark.asyncio
    async def test_close_does_not_surface_a_failed_connect(self, unconnected_cluster):
        adapter = unconnected_cluster.client_adapter
        failed = adapter.loop.create_future()
        failed.set_exception(RuntimeError('connect failed'))
        adapter._connect_ft = failed

        await adapter.close_connection()

        assert adapter._closed is True

    @pytest.mark.asyncio
    async def test_close_bucket_when_never_connected_is_a_noop(self, unconnected_cluster):
        await unconnected_cluster.client_adapter.execute_close_bucket_request(BUCKET_NAME)

    @pytest.mark.asyncio
    async def test_close_bucket_after_cluster_close_is_a_noop(self, unconnected_cluster):
        adapter = unconnected_cluster.client_adapter
        adapter._execute_connect_request = _fail_if_called
        await adapter.close_connection()

        await adapter.execute_close_bucket_request(BUCKET_NAME)

    @pytest.mark.asyncio
    async def test_transactions_raises_when_never_connected(self, unconnected_cluster):
        with pytest.raises(RuntimeError):
            unconnected_cluster.transactions


def _fail_if_called(*args, **kwargs):
    raise AssertionError('close started a connection in order to tear one down')


class ClassicAsyncConnectionLifecycleTests(AsyncConnectionLifecycleTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self):
        def valid_test_method(meth):
            attr = getattr(ClassicAsyncConnectionLifecycleTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicAsyncConnectionLifecycleTests) if valid_test_method(meth)]
        manifest_invalid = set(AsyncConnectionLifecycleTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if manifest_invalid:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {manifest_invalid}.')
