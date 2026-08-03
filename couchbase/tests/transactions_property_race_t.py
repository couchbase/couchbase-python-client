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
Regression test for PYCBC-1815 (v4.5.x branch only). Concurrent first access
to Cluster.transactions / AsyncCluster.transactions was a bare check-then-set,
so threads racing the first access could each construct and orphan their own
Transactions instance. An orphaned instance still owns cleanup threads and a
client-record registration, and closing it on GC can deadlock the single core
IO thread against the GIL (see transactions.cxx's dealloc_transactions).

No live cluster is needed. The property never touches the network before
constructing a Transactions object, so a Cluster/AsyncCluster is built via
ClusterLogic.__init__ directly (bypassing Cluster._connect() /
AsyncCluster._connect()) and exercised purely at the Python layer, with
Transactions swapped for a fake that holds the construction window open long
enough to hit the race reliably.
"""

import threading
import time

import pytest

from acouchbase.cluster import AsyncCluster
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.logic.cluster import ClusterLogic
from couchbase.options import ClusterOptions

NUM_THREADS = 16
# How long the fake Transactions() takes to "construct", wide enough that
# every thread's check-then-set race window stays open for the whole
# stampede on the old unguarded property.
CONSTRUCTION_DELAY = 0.05


def _make_tracking_transactions():
    """A stand-in for couchbase.transactions.Transactions / acouchbase's version.

    Counts constructions (lock-guarded) and sleeps briefly so the race
    window stays open regardless of thread scheduling.
    """
    lock = threading.Lock()

    class _Tracker:
        instances_created = 0

        def __init__(self, cluster, config):
            with lock:
                type(self).instances_created += 1
            time.sleep(CONSTRUCTION_DELAY)

    return _Tracker


def _make_unconnected_cluster(cluster_cls):
    cluster = cluster_cls.__new__(cluster_cls)
    ClusterLogic.__init__(cluster, 'couchbase://localhost',
                          ClusterOptions(PasswordAuthenticator('user', 'pw')))
    return cluster


class TransactionsPropertyRaceTestSuite:
    TEST_MANIFEST = [
        'test_concurrent_first_access_returns_single_instance',
    ]

    @pytest.mark.parametrize('cluster_cls,transactions_patch_target', [
        (Cluster, 'couchbase.cluster.Transactions'),
        (AsyncCluster, 'acouchbase.cluster.Transactions'),
    ])
    def test_concurrent_first_access_returns_single_instance(self,
                                                             monkeypatch,
                                                             cluster_cls,
                                                             transactions_patch_target):
        tracker = _make_tracking_transactions()
        monkeypatch.setattr(transactions_patch_target, tracker)
        cluster = _make_unconnected_cluster(cluster_cls)

        barrier = threading.Barrier(NUM_THREADS)
        results = [None] * NUM_THREADS

        def touch(idx):
            barrier.wait()
            results[idx] = cluster.transactions

        threads = [threading.Thread(target=touch, args=(i,)) for i in range(NUM_THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
            assert not t.is_alive(), 'thread failed to join, possible deadlock'

        assert all(r is not None for r in results)
        first = results[0]
        assert all(r is first for r in results), 'racing threads observed different Transactions instances'
        assert tracker.instances_created == 1


class ClassicTransactionsPropertyRaceTests(TransactionsPropertyRaceTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicTransactionsPropertyRaceTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicTransactionsPropertyRaceTests) if valid_test_method(meth)]
        test_list = set(TransactionsPropertyRaceTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')
