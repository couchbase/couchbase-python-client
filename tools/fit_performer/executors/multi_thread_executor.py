from __future__ import annotations

import logging
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, wait
from typing import (TYPE_CHECKING,
                    List,
                    Optional)

from ..workloads import WorkloadBuilder
from .workload_executor import WorkloadExecutor

if TYPE_CHECKING:
    from couchbase.cluster import Cluster

    from ..streaming import StreamOwner
    from ..workloads import Counters, Workload


def execute_workload(workloads,  # type: List[Workload]
                     counters,  # type: Counters
                     connection,  # type: Cluster
                     results,  # type: queue.Queue
                     stream_owner,  # type: Optional[StreamOwner]
                     ) -> None:
    """ Execute the given workloads in the current thread

    Args:
        workloads (List[Workload]): The workloads to be executed in the thread.
        counters (Counters): The performer-level counter registry used for counter bounds.
        connection (Cluster): The cluster object to be used for these workloads.
        results (queue.Queue): The queue where results are written
        stream_owner (StreamOwner, optional): The stream owner object to submit any streams created by this workload
    """
    logger = logging.getLogger(__name__)
    logger.info('Started thread')

    for workload in workloads:
        workload.set_connection(connection)
        workload.set_counters_and_bounds(counters)
        workload.execute(results, stream_owner)
    logger.info(f'Thread {threading.get_ident()} has finished its workloads')


class MultiThreadExecutor(WorkloadExecutor):

    _SUPPORTS_STREAMING = True

    def __init__(self, run_id, cluster, counters, stream_owner=None, span_owner=None):
        self._run_id = run_id
        self._complete_lock = threading.Lock()
        self._results = queue.Queue()
        self._cluster = cluster
        self._counters = counters
        self._pool = None
        self._workload_complete = False
        self._thread_executor = ThreadPoolExecutor(max_workers=1)
        self._num_workers = None
        self._workloads = None
        self._stream_owner = stream_owner
        self._span_owner = span_owner
        self._logger = logging.getLogger(__name__)

    @property
    def workload_complete(self) -> bool:
        with self._complete_lock:
            return self._workload_complete

    @property
    def results(self) -> queue.Queue:
        return self._results

    @property
    def supports_streaming(self) -> bool:
        return self._SUPPORTS_STREAMING and (self._stream_owner is not None)

    def set_connection(self, conn=None, hostname=None, options=None) -> None:
        pass

    def build_workloads(self, request) -> None:
        self._workloads = [[WorkloadBuilder.build_workload(wl, self._run_id, span_owner=self._span_owner)
                            for wl in hs.workloads]
                           for hs in request.workloads.horizontal_scaling]
        self._num_workers = len(self._workloads)
        self._logger.info(f"There are {self._num_workers} workloads")

    def create_pool(self) -> None:
        if self._pool:
            raise RuntimeError('Pool has already been created.')

        self._pool = ThreadPoolExecutor(max_workers=self._num_workers)

    def execute_workloads(self) -> None:
        self._workload_complete = None
        ft = self._thread_executor.submit(self._execute_workloads, self._workloads)
        ft.add_done_callback(self._workloads_complete)

    def _execute_workloads(self, workloads):
        args = list(map(
            lambda wl: (wl, self._counters, self._cluster, self._results, self._stream_owner),
            workloads)
        )
        futures = set()
        for a in args:
            f = self._pool.submit(execute_workload, *a)
            futures.add(f)
        wait(futures)
        for f in futures:
            e = f.exception()
            if e:
                raise e

    def _workloads_complete(self, future):
        exc = future.exception()
        if exc:
            raise exc
        self._stream_owner.wait_for_all_streams_from_run(self._run_id)
        with self._complete_lock:
            self._workload_complete = True
        self._logger.info('Workloads complete')
        self._results.put(None)  # Executor done sentinel

    def shutdown(self) -> None:
        self._pool.shutdown()

    @classmethod
    def build_executor(cls, run_id, request, cluster, counters, stream_owner=None, span_owner=None):
        executor = cls(run_id, cluster, counters, stream_owner, span_owner=span_owner)
        executor.build_workloads(request)
        executor.create_pool()
        return executor
