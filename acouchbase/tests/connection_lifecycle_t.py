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

# Hand-written and neither generated nor merged.  Two halves, one per end of the lifecycle:
# the close half is cluster-free and comes first, the connect half opens real clusters.
#
# The sync file this appears to pair with asserts the same three close rules against a
# surface this tree does not have: `close_bucket` is `execute_close_bucket_request` here and
# returns a future, and `close_connection` is a coroutine.  Two of the close tests below have
# no sync form at all, because they are about a connect future only the async tree keeps, and
# the connect half has none because a blocking constructor connects before it returns, leaving
# no chain to observe.  There is no sidecar either: a sidecar merges into a generated class,
# and nothing here is generated.
#
# The close rule this tree makes easy to break is the third, that a close never starts a
# connection in order to tear one down.  A connect is normally already in flight when close
# is called, and chaining onto it is right only while that connect may still succeed: a
# connect that has already failed leaves nothing to close, and its error must not come back
# out of `close()`, where the caller cannot act on it.
from __future__ import annotations

from typing import Any, AsyncGenerator
from uuid import uuid4

import pytest
import pytest_asyncio

from acouchbase.cluster import Cluster
from acouchbase.logic.cluster_impl import AsyncClusterImpl
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import DocumentNotFoundException
from couchbase.options import ClusterOptions
from tests.environments.async_environment import (AsyncTestEnvironment,
                                                  build_async_environment,
                                                  teardown_async_environment)
from tests.environments.test_environment import SuiteRequirements

BUCKET_NAME = 'default'
CONNSTR = 'couchbase://localhost'


def _fail_if_called(*args: Any, **kwargs: Any) -> None:
    raise AssertionError('close started a connection in order to tear one down')


class AsyncConnectionLifecycleUnitTests:
    # No TEST_MANIFEST: the manifest is a parity contract between api trees, and this file is
    # one tree's own.  Nothing connects; every cluster is built with
    # `skip_connect='TEST_SKIP_CONNECT'`, and the stand-in for a failed connect is a future
    # carrying an exception.  `transactions` on a never-connected cluster is not asserted
    # here even though PYCBC-1928 added that guard too: transactions_property_race_t.py
    # covers it in both trees, with the race it gates.  The adapter is driven directly rather
    # than through `Cluster`, because two of these rules are about guards the public close
    # paths call and never expose.

    @pytest.fixture(name='unconnected_cluster')
    def cluster_without_core_connection(self) -> AsyncClusterImpl:
        return AsyncClusterImpl(CONNSTR,
                                ClusterOptions(PasswordAuthenticator('Administrator', 'password')),
                                skip_connect='TEST_SKIP_CONNECT')

    # Returning is the assertion in both bucket-close tests below: each call raised or chained a
    # connect before PYCBC-1928, so there is nothing to observe beyond having got that far.
    @pytest.mark.asyncio
    async def test_close_bucket_after_cluster_close_is_a_noop(self,
                                                              unconnected_cluster: AsyncClusterImpl,
                                                              monkeypatch: pytest.MonkeyPatch) -> None:
        adapter = unconnected_cluster.client_adapter
        monkeypatch.setattr(adapter, '_execute_connect_request', _fail_if_called)
        await adapter.close_connection()

        await adapter.execute_close_bucket_request(BUCKET_NAME)

    @pytest.mark.asyncio
    async def test_close_bucket_when_never_connected_is_a_noop(self,
                                                               unconnected_cluster: AsyncClusterImpl) -> None:
        await unconnected_cluster.client_adapter.execute_close_bucket_request(BUCKET_NAME)

    @pytest.mark.asyncio
    async def test_close_connection_is_idempotent(self,
                                                  unconnected_cluster: AsyncClusterImpl,
                                                  monkeypatch: pytest.MonkeyPatch) -> None:
        adapter = unconnected_cluster.client_adapter
        monkeypatch.setattr(adapter, '_execute_connect_request', _fail_if_called)

        await adapter.close_connection()
        await adapter.close_connection()

        assert adapter._closed is True

    @pytest.mark.asyncio
    async def test_close_connection_when_never_connected_does_not_connect(self,
                                                                          unconnected_cluster: AsyncClusterImpl,
                                                                          monkeypatch: pytest.MonkeyPatch
                                                                          ) -> None:
        adapter = unconnected_cluster.client_adapter
        monkeypatch.setattr(adapter, '_execute_connect_request', _fail_if_called)

        await adapter.close_connection()

        assert adapter._closed is True

    @pytest.mark.asyncio
    async def test_close_does_not_surface_a_failed_connect(self, unconnected_cluster: AsyncClusterImpl) -> None:
        adapter = unconnected_cluster.client_adapter
        failed = adapter.loop.create_future()
        failed.set_exception(RuntimeError('connect failed'))
        adapter._connect_ft = failed

        await adapter.close_connection()

        assert adapter._closed is True


class AsyncConnectionChainingTests(SuiteRequirements):
    # The other end of the lifecycle, and no TEST_MANIFEST for the reason the class above
    # gives.  Opens a real cluster of its own per test, because the subject is a cluster that
    # has not finished connecting and `cb_env.cluster` is not that.
    #
    # `acouchbase.Cluster(...)` returns before it is connected, as does every handle taken
    # from it, and what makes that usable is that each connect chains onto the one above it.
    # Neither test calls `on_connect()`, because the point is that nothing has to.

    # Both tests write their own key through a cluster of their own, so the shared corpus
    # would be seeded and never read.
    NUM_DOCS = 0

    @pytest_asyncio.fixture(scope='class', name='cb_env')
    async def couchbase_test_environment(self,
                                         acb_base_env: AsyncTestEnvironment
                                         ) -> AsyncGenerator[AsyncTestEnvironment, None]:
        env = await build_async_environment(acb_base_env, AsyncConnectionChainingTests)
        yield env
        await teardown_async_environment(env, AsyncConnectionChainingTests)

    @staticmethod
    def _unconnected_cluster(cb_env: AsyncTestEnvironment) -> Cluster:
        username, pw = cb_env.config.get_username_and_pw()
        return Cluster(cb_env.config.get_connection_string(),
                       ClusterOptions(PasswordAuthenticator(username, pw)))

    # `flaky` here means "opens a connection", as it does on the five close tests in the sync
    # cluster_t: a connection attempt against a shared cluster is the retryable part.
    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.asyncio
    async def test_bucket_on_connect_completes_the_cluster_connect(self,
                                                                   cb_env: AsyncTestEnvironment) -> None:
        cluster = self._unconnected_cluster(cb_env)
        try:
            connect_ft = cluster._impl.client_adapter.connect_ft
            # The constructor starts the connect without waiting, so the future is here and
            # pending.  A cluster built to skip connecting has None.
            assert connect_ft is not None and connect_ft.done() is False

            bucket = cluster.bucket(cb_env.config.bucket_name)
            await bucket.on_connect()

            # The only await in this test, and what completes the cluster connect: the bucket
            # had nothing to open a bucket on until then.
            assert connect_ft.done() is True
            assert cluster._impl.connection is not None
            assert bucket._impl.bucket_connect_ft.done() is True
        finally:
            await cluster.close()

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.asyncio
    async def test_a_key_value_operation_completes_the_whole_chain(self,
                                                                   cb_env: AsyncTestEnvironment) -> None:
        cluster = self._unconnected_cluster(cb_env)
        # Held rather than re-derived: `cluster.bucket(name)` builds a new bucket every call,
        # each with a connect future of its own.
        bucket = cluster.bucket(cb_env.config.bucket_name)
        collection = bucket.default_collection()
        key = f'chained-kv-op-{uuid4()}'
        doc = {'id': key, 'what': 'written through a cluster that was never explicitly connected'}
        try:
            # The first await, and nothing before it connected anything.  A broken chain
            # fails here, on an operation submitted against a connection that does not exist.
            await collection.upsert(key, doc)

            connect_ft = cluster._impl.client_adapter.connect_ft
            assert connect_ft is not None and connect_ft.done() is True
            assert bucket._impl.bucket_connect_ft.done() is True

            result = await collection.get(key)
            assert result.content_as[dict] == doc
        finally:
            try:
                await collection.remove(key)
            except DocumentNotFoundException:
                # The upsert is what may not have happened, and its failure is the finding;
                # cleanup must not replace it with one about the cleanup.
                pass
            await cluster.close()
