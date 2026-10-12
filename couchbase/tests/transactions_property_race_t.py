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

import sys
import threading
from typing import Any, ClassVar

import pytest

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from tests.manifest import validate_test_manifest

CONNSTR = 'couchbase://localhost'
NUM_THREADS = 16
# How long the fake Transactions() takes to "construct", wide enough that a regression to a
# bare check-then-set is hit by every thread in the stampede rather than by an unlucky one.
CONSTRUCTION_DELAY = 0.05


@pytest.mark.testgen_exclude('txcouchbase', reason='no transactions support in the twisted tree')
class TransactionsPropertyRaceTestSuite:
    # Regression guard (PYCBC-1821, the >= 4.6.0 counterpart of PYCBC-1815).
    # `ClusterImpl.transactions` was a bare check-then-set, so threads racing the first
    # access could each construct and orphan their own Transactions instance, and an
    # orphaned one still owns cleanup threads and a client-record registration.  Generated
    # rather than sync-only, which is the reason it is worth carrying: the async tree holds
    # an independent second copy of the same double-checked lock.  No live cluster is
    # needed; the cluster is built with `skip_connect` and Transactions is swapped for a fake
    # that holds the construction window open.  The property reaches the check-then-set only
    # through a connection guard added by PYCBC-1928, so the two threaded tests stub that out
    # and the guard has two tests of its own.

    TEST_MANIFEST: ClassVar[list[str]] = [
        'test_close_during_first_access_closes_instance',
        'test_concurrent_first_access_returns_single_instance',
        'test_transactions_raises_when_closed',
        'test_transactions_raises_when_not_connected',
    ]

    @pytest.fixture(name='unconnected_cluster')
    def cluster_without_core_connection(self) -> Cluster:
        return Cluster(CONNSTR,
                       ClusterOptions(PasswordAuthenticator('Administrator', 'password')),
                       skip_connect='TEST_SKIP_CONNECT')

    @pytest.fixture(name='race_cluster')
    def cluster_with_the_connection_guard_stubbed(self,
                                                  unconnected_cluster: Cluster,
                                                  monkeypatch: pytest.MonkeyPatch) -> Cluster:
        # A cluster built with `skip_connect` cannot pass that guard, so without this the two
        # threaded tests below would never reach the check-then-set they exist to drive.
        monkeypatch.setattr(unconnected_cluster._impl._client_adapter, '_ensure_connected', lambda: None)
        return unconnected_cluster

    @pytest.fixture(name='tracking_transactions')
    def tracking_transactions_cls(self,
                                  monkeypatch: pytest.MonkeyPatch,
                                  unconnected_cluster: Cluster) -> type[Any]:
        # Counts constructions under a lock, signals once one is in flight, and sleeps
        # briefly so the race window stays open regardless of scheduling.  The patch target
        # is read off the impl rather than written out: each api tree resolves
        # `Transactions` as a global of its own module, so a literal module path would patch
        # the sync module and the fake would never be constructed in a generated copy.
        lock = threading.Lock()

        class _Tracker:
            instances_created = 0
            construction_started = threading.Event()

            def __init__(self, cluster: Any) -> None:
                self.close_count = 0
                with lock:
                    type(self).instances_created += 1
                type(self).construction_started.set()
                threading.Event().wait(CONSTRUCTION_DELAY)

            def close(self) -> None:
                with lock:
                    self.close_count += 1

        monkeypatch.setattr(sys.modules[type(unconnected_cluster._impl).__module__],
                            'Transactions',
                            _Tracker)
        return _Tracker

    def test_close_during_first_access_closes_instance(self,
                                                       tracking_transactions: type[Any],
                                                       race_cluster: Cluster) -> None:
        result: dict[str, Any] = {}

        def touch() -> None:
            result['txns'] = race_cluster.transactions

        toucher = threading.Thread(target=touch)
        toucher.start()
        # Proceed only once the property is inside Transactions(), so close() races it.
        assert tracking_transactions.construction_started.wait(timeout=10), 'construction never started'
        race_cluster.close()
        toucher.join(timeout=10)
        assert not toucher.is_alive(), 'thread failed to join, possible deadlock'

        txns = result.get('txns')
        assert txns is not None
        assert tracking_transactions.instances_created == 1
        assert txns.close_count == 1, 'the instance published by the racing first access was never closed'
        assert race_cluster._impl._transactions is None, 'a live Transactions outlived cluster.close()'

    def test_concurrent_first_access_returns_single_instance(self,
                                                             tracking_transactions: type[Any],
                                                             race_cluster: Cluster) -> None:
        barrier = threading.Barrier(NUM_THREADS)
        results: list[Any] = [None] * NUM_THREADS

        def touch(idx: int) -> None:
            barrier.wait()
            results[idx] = race_cluster.transactions

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

    def test_transactions_raises_when_closed(self,
                                             tracking_transactions: type[Any],
                                             race_cluster: Cluster) -> None:
        # The stubbed cluster, so that the closed guard is the only one left that can raise.
        race_cluster.close()

        with pytest.raises(RuntimeError):
            race_cluster.transactions

        assert tracking_transactions.instances_created == 0

    def test_transactions_raises_when_not_connected(self,
                                                    tracking_transactions: type[Any],
                                                    unconnected_cluster: Cluster) -> None:
        with pytest.raises(RuntimeError):
            unconnected_cluster.transactions

        assert tracking_transactions.instances_created == 0


@pytest.mark.testgen_exclude('txcouchbase', reason='no transactions support in the twisted tree')
class TransactionsPropertyRaceTests(TransactionsPropertyRaceTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(TransactionsPropertyRaceTestSuite, self.__class__)
