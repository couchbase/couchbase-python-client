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

# Hand-written and neither generated nor merged.  The connect half of the lifecycle only: the
# sync file this appears to pair with is all close rules, spelled against a surface this tree
# does not have, and acouchbase/tests/connection_lifecycle_t.py covers those rules for the
# adapter txcouchbase shares.  What is this tree's own is that the connect is driven through
# deferreds on the reactor, which nothing generated from a blocking source can observe.
from __future__ import annotations

from collections.abc import Generator
from uuid import uuid4

import pytest

from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import DocumentNotFoundException
from couchbase.options import ClusterOptions
from tests.environments.twisted_environment import (SuiteRequirements,
                                                    TwistedTestEnvironment,
                                                    build_twisted_environment,
                                                    run_in_reactor_thread,
                                                    teardown_twisted_environment)
from txcouchbase.cluster import Cluster


class TxConnectionChainingTests(SuiteRequirements):
    # No TEST_MANIFEST: the manifest is a parity contract between api trees, and this file is
    # one tree's own.  Opens a real cluster of its own per test, because the subject is a
    # cluster that has not finished connecting and `cb_env.cluster` is not that.
    #
    # `txcouchbase.Cluster(...)` returns before it is connected, as does every handle taken
    # from it, and what makes that usable is that each connect chains onto the one above it.
    # The futures asserted on are the acouchbase adapter's, which txcouchbase wraps.

    # Both tests write their own key through a cluster of their own, so the shared corpus
    # would be seeded and never read.
    NUM_DOCS = 0

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self,
                                   txcb_base_env: TwistedTestEnvironment
                                   ) -> Generator[TwistedTestEnvironment, None, None]:
        env = build_twisted_environment(txcb_base_env, TxConnectionChainingTests)
        yield env
        teardown_twisted_environment(env, TxConnectionChainingTests)

    @staticmethod
    def _unconnected_cluster(cb_env: TwistedTestEnvironment) -> Cluster:
        username, pw = cb_env.config.get_username_and_pw()
        return Cluster(cb_env.config.get_connection_string(),
                       ClusterOptions(PasswordAuthenticator(username, pw)))

    # `flaky` here means "opens a connection", as it does on the five close tests in the sync
    # cluster_t: a connection attempt against a shared cluster is the retryable part.
    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_bucket_on_connect_completes_the_cluster_connect(self, cb_env: TwistedTestEnvironment) -> None:
        cluster = self._unconnected_cluster(cb_env)
        try:
            connect_ft = cluster._impl.client_adapter.connect_ft
            # The constructor starts the connect without waiting, so the future is here and
            # pending.  A cluster built to skip connecting has None.
            assert connect_ft is not None and connect_ft.done() is False

            bucket = cluster.bucket(cb_env.config.bucket_name)
            run_in_reactor_thread(bucket.on_connect)

            # The only wait in this test, and what completes the cluster connect: the bucket
            # had nothing to open a bucket on until then.
            assert connect_ft.done() is True
            assert cluster._impl.connection is not None
            assert bucket._impl.bucket_connect_ft.done() is True
        finally:
            run_in_reactor_thread(cluster.close)

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_a_key_value_operation_completes_the_whole_chain(self, cb_env: TwistedTestEnvironment) -> None:
        cluster = self._unconnected_cluster(cb_env)
        # Held rather than re-derived: `cluster.bucket(name)` builds a new bucket every call,
        # each with a connect future of its own.
        bucket = cluster.bucket(cb_env.config.bucket_name)
        collection = bucket.default_collection()
        key = f'chained-kv-op-{uuid4()}'
        doc = {'id': key, 'what': 'written through a cluster that was never explicitly connected'}
        try:
            # The first wait, and nothing before it connected anything.  A broken chain fails
            # here, on an operation submitted against a connection that does not exist.
            run_in_reactor_thread(collection.upsert, key, doc)

            connect_ft = cluster._impl.client_adapter.connect_ft
            assert connect_ft is not None and connect_ft.done() is True
            assert bucket._impl.bucket_connect_ft.done() is True

            result = run_in_reactor_thread(collection.get, key)
            assert result.content_as[dict] == doc
        finally:
            try:
                run_in_reactor_thread(collection.remove, key)
            except DocumentNotFoundException:
                # The upsert is what may not have happened, and its failure is the finding;
                # cleanup must not replace it with one about the cleanup.
                pass
            run_in_reactor_thread(cluster.close)
