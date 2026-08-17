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
Regression test for PYCBC-1821 (the >= 4.6.0 counterpart of PYCBC-1815).
AsyncClusterImpl.transactions was a bare check-then-set, so threads racing the
first access could each construct and orphan their own Transactions instance.
An orphaned instance still owns cleanup threads and a client-record
registration, and closing it on GC can deadlock the single core IO thread
against the GIL (see transactions.cxx's dealloc_transactions).

The property is synchronous and Transactions.__init__ drops the GIL for a full
cluster round-trip, so the racers here are OS threads, not tasks -- the guard
is a threading.Lock for the same reason.

No live cluster is needed.  The property never touches the network before
constructing a Transactions object, so the cluster is built with
skip_connect='TEST_SKIP_CONNECT' (same mechanism couchbase/tests/connection_t.py
uses) and exercised purely at the Python layer, with Transactions swapped for a
fake that holds the construction window open long enough to hit the race
reliably.
"""

import threading

import pytest

from acouchbase.cluster import AsyncCluster
from acouchbase.logic.client_adapter import AsyncClientAdapter
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions

NUM_THREADS = 16
# How long the fake Transactions() takes to "construct", wide enough that
# every thread's check-then-set race window stays open for the whole
# stampede on the old unguarded property.
CONSTRUCTION_DELAY = 0.05


class TransactionsPropertyRaceTestSuite:
    TEST_MANIFEST = [
        'test_close_during_first_access_closes_instance',
        'test_concurrent_first_access_returns_single_instance',
    ]

    @pytest.fixture(name='tracking_transactions')
    def tracking_transactions_cls(self, monkeypatch):
        """A stand-in for acouchbase.transactions.Transactions.

        Counts constructions (lock-guarded), signals once a construction is in
        flight, and sleeps briefly so the race window stays open regardless of
        thread scheduling.
        """
        lock = threading.Lock()

        class _Tracker:
            instances_created = 0
            construction_started = threading.Event()

            def __init__(self, cluster):
                self.close_count = 0
                with lock:
                    type(self).instances_created += 1
                type(self).construction_started.set()
                threading.Event().wait(CONSTRUCTION_DELAY)

            def close(self):
                with lock:
                    self.close_count += 1

        monkeypatch.setattr('acouchbase.logic.cluster_impl.Transactions', _Tracker)
        return _Tracker

    @pytest.fixture(name='unconnected_cluster')
    def cluster_without_core_connection(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        # No event loop is touched: skipping the create-connection request means the adapter's
        # lazy loop lookup never runs, and the faked Transactions never reads cluster.loop.
        return AsyncCluster(conn_string,
                            ClusterOptions(PasswordAuthenticator(username, pw)),
                            skip_connect='TEST_SKIP_CONNECT')

    @pytest.fixture(name='stub_adapter_close')
    def stub_async_adapter_close(self, monkeypatch):
        """Stub out the core half of close_connection().

        When not connected, AsyncClientAdapter chains a connect request ahead of the
        close (client_adapter.py:301), so under skip_connect the close would establish
        the very connection this test skipped.  The sync adapter early-outs instead and
        needs no stub, so this keeps both tests network-free and asserting only the
        lock ordering.
        """

        async def _noop(self):
            self._closed = True

        monkeypatch.setattr(AsyncClientAdapter, 'close_connection', _noop)

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('stub_adapter_close')
    async def test_close_during_first_access_closes_instance(self, tracking_transactions, unconnected_cluster):
        result = {}

        def touch():
            # Off-loop: how the property stays reachable while close() runs on the loop.
            result['txns'] = unconnected_cluster.transactions

        toucher = threading.Thread(target=touch)
        toucher.start()
        # Proceed only once the property is inside Transactions(), so close() races it.
        assert tracking_transactions.construction_started.wait(timeout=10), 'construction never started'
        await unconnected_cluster.close()
        toucher.join(timeout=10)
        assert not toucher.is_alive(), 'thread failed to join, possible deadlock'

        txns = result.get('txns')
        assert txns is not None
        assert tracking_transactions.instances_created == 1
        assert txns.close_count == 1, 'the instance published by the racing first access was never closed'
        assert unconnected_cluster._impl._transactions is None, 'a live Transactions outlived cluster.close()'

    def test_concurrent_first_access_returns_single_instance(self, tracking_transactions, unconnected_cluster):
        barrier = threading.Barrier(NUM_THREADS)
        results = [None] * NUM_THREADS

        def touch(idx):
            barrier.wait()
            results[idx] = unconnected_cluster.transactions

        threads = [threading.Thread(target=touch, args=(i,)) for i in range(NUM_THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
            assert not t.is_alive(), 'thread failed to join, possible deadlock'

        assert all(r is not None for r in results)
        first = results[0]
        assert all(r is first for r in results), 'racing threads observed different Transactions instances'
        assert tracking_transactions.instances_created == 1


class TransactionsPropertyRaceTests(TransactionsPropertyRaceTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(TransactionsPropertyRaceTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(TransactionsPropertyRaceTests) if valid_test_method(meth)]
        test_list = set(TransactionsPropertyRaceTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')
