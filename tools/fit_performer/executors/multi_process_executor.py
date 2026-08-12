from __future__ import annotations

import logging
import multiprocessing as mp
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from time import sleep
from typing import (TYPE_CHECKING,
                    List,
                    Optional,
                    Union)

from acouchbase.cluster import AsyncCluster
from couchbase.cluster import Cluster

from ..workloads import WorkloadBuilder
from .workload_executor import WorkloadExecutor

if TYPE_CHECKING:
    from couchbase.options import ClusterOptions

    from ..workloads import Counters, Workload

# forkserver avoids inheriting the performer's active gRPC threads/fds into workers.
# Workers are forked from a clean forkserver process (itself spawned fresh, not forked),
# so they never see the parent's gRPC state.
# **IMPORTANT** Not supported on Windows.
_CTX = mp.get_context('forkserver')


class PerformerProcess(_CTX.Process):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._results_queue = None
        self._tracer = None
        self._meter = None
        self._tracer_provider = None
        self._meter_provider = None
        self._span_contexts = {}

    @property
    def results(self):
        return self._results_queue

    @property
    def meter(self):
        return self._meter

    @property
    def meter_provider(self):
        return self._meter_provider

    @property
    def span_contexts(self):
        return self._span_contexts

    @property
    def tracer(self):
        return self._tracer

    @property
    def tracer_provider(self):
        return self._tracer_provider

    def set_otel(self, tracer, tracer_provider, meter, meter_provider, span_contexts):
        self._tracer = tracer
        self._tracer_provider = tracer_provider
        self._meter = meter
        self._meter_provider = meter_provider
        self._span_contexts = span_contexts

    def set_parent_items(self, queue):
        self._results_queue = queue

    @classmethod
    def register_with_context(cls, context):
        context.Process = cls


# Make sure the process pool uses our custom process
PerformerProcess.register_with_context(_CTX)


def init_process(queue,  # type: mp.Queue
                 obs_config=None,
                 span_contexts=None
                 ) -> None:
    """ Initialize the performer process

    Sets the items shared with the parent process

    Args:
        queue (mp.Queue): The results queue shared with the parent process
        obs_config: The observability config used to create OTel tracer/meter in this process
        span_contexts: Dict mapping span ID to W3C traceparent string, for ambient context injection
    """
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s'))
        root.addHandler(handler)
        root.setLevel(logging.INFO)

    current_p = mp.current_process()
    current_p.set_parent_items(queue)

    tracer = tracer_provider = meter = meter_provider = None
    if obs_config is not None:
        _logger = logging.getLogger(__name__)
        pid = current_p.pid
        if not obs_config.use_noop_tracer and obs_config.HasField('tracing'):
            from ..telemetry.otel import create_tracer_provider
            tracer, tracer_provider = create_tracer_provider(obs_config.tracing)
            # If the tracer provider is GC'd before the process exits, spans won't be exported and we'll log a warning
            # about it.
            # We should _only_ see this warning after this message: [INFO] Process {PID} has finished its workloads
            weakref.finalize(tracer_provider, _logger.warning,
                             'OTel TracerProvider GC\'d in process %d - spans will not be exported', pid)
        if obs_config.HasField('metrics'):
            from ..telemetry.otel import create_meter_provider
            meter, meter_provider = create_meter_provider(obs_config.metrics)
            # If the meter provider is GC'd before the process exits, metrics won't be exported and we'll log a warning
            # about it.
            # We should _only_ see this warning after this message: [INFO] Process {PID} has finished its workloads
            weakref.finalize(meter_provider, _logger.warning,
                             'OTel MeterProvider GC\'d in process %d - metrics will not be exported', pid)
    current_p.set_otel(tracer, tracer_provider, meter, meter_provider, span_contexts or {})


def execute_workload(workloads,  # type: List[Workload]
                     counters,  # type: Counters
                     hostname,  # type: str
                     options,  # type: ClusterOptions
                     retries=5  # type: Optional[int]
                     ) -> None:
    """ Execute the given workloads in the current process

    Args:
        workloads (List[Workload]): The workloads to be executed in the process.
        counters (Counters): The performer-level counter registry used for counter bounds.  Backed by
            a multiprocessing.Manager, so its state is shared with the parent process and other workers.
        hostname (str): The hostname for the cluster used for these workloads.
        options (ClusterOptions): The cluster options for the cluster used for these workloads.
        retries (int, optional): The number of attempts to establish a connection with the cluster.

    Raises:
        RuntimeError: If no connection can be established with the cluster after the given number of retries.
    """
    logger = logging.getLogger(__name__)
    current_p = mp.current_process()
    results = current_p.results

    from ..telemetry.otel import worker_otel_setup, worker_otel_teardown
    otel_tokens, worker_span_owner = worker_otel_setup(
        current_p.tracer, current_p.meter, current_p.span_contexts, options
    )

    connection = None
    for _ in range(retries):
        try:
            connection = Cluster(hostname, options)
            connection.wait_until_ready(timedelta(seconds=5))
            break
        except Exception as ex:
            print(f'set_connection exception: type: {type(ex)} - {ex}')
            sleep(1)

    if not connection:
        raise RuntimeError('No connection established, cannot execute workload.')

    try:
        for workload in workloads:
            workload.set_connection(connection)
            workload.set_counters_and_bounds(counters)
            if worker_span_owner is not None and hasattr(workload, 'set_span_owner'):
                workload.set_span_owner(worker_span_owner)
            workload.execute(results)
    finally:
        worker_otel_teardown(otel_tokens, current_p.tracer_provider, current_p.meter_provider)
    logger.info(f"Process {current_p.pid} has finished its workloads")


class MultiProcessExecutor(WorkloadExecutor):

    _SUPPORTS_STREAMING = False

    def __init__(self, run_id, counters, hostname=None, options=None, obs_config=None, span_contexts=None):
        self._run_id = run_id
        self._span_contexts = span_contexts or {}
        self._complete_lock = threading.Lock()
        self._results = _CTX.SimpleQueue()
        self._counters = counters
        self._pool = None
        self._workload_complete = False
        self._thread_executor = ThreadPoolExecutor(max_workers=1)
        self._num_workers = None
        self._workloads = None
        self._hostname = hostname
        self._options = options
        self._obs_config = obs_config
        self._logger = logging.getLogger(__name__)

    @property
    def pool(self):
        return self._pool

    @property
    def workload_complete(self) -> bool:
        with self._complete_lock:
            return self._workload_complete

    @property
    def results(self) -> mp.SimpleQueue:
        return self._results

    @property
    def supports_streaming(self) -> bool:
        return self._SUPPORTS_STREAMING

    def create_pool(self):
        if self._pool:
            raise RuntimeError('Pool has already been created.')

        # If num_workers is None it will default to multiprocessing.cpu_count() for the number of workers
        args = (self._results, self._obs_config, self._span_contexts)
        self._pool = _CTX.Pool(self._num_workers, initializer=init_process, initargs=args)

    def set_connection(self,
                       conn=None,  # type: Optional[Union[AsyncCluster, Cluster]]
                       hostname=None,  # type: Optional[str]
                       options=None,  # type: Optional[ClusterOptions]
                       retries=5,  # type: Optional[int]
                       ) -> None:
        pass

    def build_workloads(self, request):
        # The number of workloads equals # of gRPC HorizontalScaling
        # which also equals # of processes to use to execute workloads

        # List[ List[ Workload ] ]
        self._workloads = [[WorkloadBuilder.build_workload(wl, self._run_id) for wl in hs.workloads]
                           for hs in request.workloads.horizontal_scaling]
        self._num_workers = len(self._workloads)
        self._logger.info(f"There are {self._num_workers} workloads")

    def _execute_workloads(self, workloads):
        args = list(map(lambda wl: (wl, self._counters, self._hostname, self._options), workloads))
        self._pool.starmap(execute_workload, args)

    def _workloads_complete(self, future):
        exc = future.exception()
        if exc:
            raise exc
        with self._complete_lock:
            self._workload_complete = True
        self._logger.info('Workloads complete')
        self._results.put(None)  # Executor done sentinel

    def execute_workloads(self):
        self._workload_complete = False
        ft = self._thread_executor.submit(self._execute_workloads, self._workloads)
        ft.add_done_callback(self._workloads_complete)

    def shutdown(self):
        self._pool.close()
        self._pool.join()

    @classmethod
    def build_executor(cls, run_id, request, hostname, options, counters, obs_config=None, span_owner=None
                       ) -> MultiProcessExecutor:
        # Worker processes can't see this process's in-process dict, so upgrade the registry to a
        # Manager backing (shared via picklable proxies) before the pool is created.
        counters.enable_cross_process_sharing()
        span_contexts = span_owner.export_contexts() if span_owner is not None else {}
        executor = cls(run_id, counters, hostname=hostname, options=options, obs_config=obs_config,
                       span_contexts=span_contexts)
        executor.build_workloads(request)
        executor.create_pool()
        return executor
