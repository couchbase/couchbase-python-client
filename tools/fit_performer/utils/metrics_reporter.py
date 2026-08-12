import json
import logging
import sys
from collections import namedtuple
from queue import (Empty,
                   Full,
                   Queue)
from threading import Timer
from time import perf_counter_ns
from typing import (Dict,
                    Optional,
                    TypedDict)

import psutil
from google.protobuf import timestamp_pb2 as timestamp

from ..generated.metrics.top_level_pb2 import Result as MetricsResult
from ..generated.run.top_level_pb2 import Result

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

logger = logging.getLogger(__name__)

# copy of psutil.Process.cpu_times()
proc_cpu = namedtuple(
    'proc_cpu', ['user', 'system', 'children_user', 'children_system']
)


class ThreadCPU(TypedDict):
    user_time: int
    system_time: int


threads_cpu: TypeAlias = Dict[int, ThreadCPU]


class ReportQueue(Queue):
    def __init__(self):
        super().__init__()

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self.get_nowait()
        except Empty:
            raise StopIteration


class MetricsReporter(Timer):
    def __init__(self,
                 run_id: str,
                 interval: float,
                 proc: Optional[psutil.Process] = None,
                 args=None,
                 kwargs=None):
        super().__init__(interval, self._compute_metrics, args, kwargs)
        self._run_id = run_id
        self._proc = proc or psutil.Process()
        # base class has an _initialized attribute
        self._reporter_initialized = False
        self._last_report = None
        self._last_proc_cpu = None
        self._last_proc_threads = None
        self._report_queue = ReportQueue()

    @property
    def report_queue(self) -> ReportQueue:
        return self._report_queue

    def _calc_proc_cpu_percent(self, t1: proc_cpu, t2: proc_cpu, time_delta: float) -> float:
        if time_delta == 0:
            return 0.0

        delta_cpu = (t2.user - t1.user) + (t2.system - t1.system)
        # convert delta to milliseconds
        cpu_percent = ((delta_cpu * 1000) / time_delta) * 100
        return cpu_percent

    def _calc_proc_threads_cpu_percent(self, t1: threads_cpu, t2: threads_cpu, time_delta: float) -> Dict[str, float]:
        delta_threads = {}
        for tid, cpu in t2.items():
            t1_match = t1.get(tid, None)
            if t1_match is None:
                continue
            delta_cpu = (cpu['user_time'] - t1_match['user_time']) + (cpu['system_time'] - t1_match['system_time'])
            # convert delta to milliseconds
            delta_threads[tid] = delta_cpu * 1000

        thread_cpu_percent = {}
        total_thread_cpu = 0
        for tid, dcpu in delta_threads.items():
            if time_delta == 0:
                thread_cpu_percent[tid] = 0.0
                continue
            cpu_percent = (dcpu / time_delta) * 100
            thread_cpu_percent[tid] = cpu_percent
            total_thread_cpu += cpu_percent

        thread_cpu_percent['total_thread_cpu'] = total_thread_cpu
        return thread_cpu_percent

    def _compute_metrics(self):
        now = perf_counter_ns()
        # for linux/macOS, returns CPU ticks / ticks per second which yields seconds
        proc_cpu = self._proc.cpu_times()
        threads = self._proc.threads()
        tdict = {}
        for t in threads:
            if t.id not in tdict:
                tdict[t.id] = {'user_time': t.user_time, 'system_time': t.system_time}
        if not self._reporter_initialized:
            self._last_report = now
            self._last_proc_cpu = proc_cpu
            self._last_proc_threads = tdict
            self._reporter_initialized = True
            return

        time_delta = (now - self._last_report) / 1e6  # to milliseconds
        proc_cpu_percent = self._calc_proc_cpu_percent(self._last_proc_cpu, proc_cpu, time_delta)
        threads_cpu_percent = self._calc_proc_threads_cpu_percent(self._last_proc_threads, tdict, time_delta)
        mem_info = self._proc.memory_info()
        self._last_report = now
        self._last_proc_cpu = proc_cpu
        self._last_proc_threads = tdict
        # useful for debugging
        # print(f"CPU: {proc_cpu_percent}%")
        # for tid, cpu in threads_cpu_percent.items():
        #     if tid == 'total_thread_cpu':
        #         print(f"Total Thread CPU: {cpu}%")
        #     else:
        #         print(f"Thread ID: {tid}, CPU: {cpu}%")
        # print(f"Memory Info RSS: {mem_info.rss / (1024 * 1024)} MB")
        # print(f"Memory Info VMS: {mem_info.vms / (1024 * 1024)} MB")
        report = json.dumps({'processCpu': round(proc_cpu_percent, 2),
                             'memRssUsedMB': round(mem_info.rss / (1024 * 1024), 2),
                             'memVmsMB': round(mem_info.vms / (1024 * 1024), 2),
                             'threadCount': len(threads_cpu_percent) - 1,  # subtract 1 for total_thread_cpu
                             })
        logger.info(f'Metrics (run: {self._run_id}): {report}')
        try:
            metrics_result = Result(metrics=MetricsResult(metrics=report,
                                                          initiated=timestamp.Timestamp().GetCurrentTime()))
            self._report_queue.put_nowait(metrics_result)
        except Full:
            logger.debug(f'Queue is full, dropping report (run: {self._run_id}): {report}')
            pass  # ignore full queue

    # Override the run method to keep the timer running
    # Python3.10:
    # https://github.com/python/cpython/blob/10a2a9b3bcf237fd6183f84941632cda59395319/Lib/threading.py#L1375-L1379
    def run(self):
        while not self.finished.is_set():
            self.function(*self.args, **self.kwargs)
            self.finished.wait(self.interval)

    def stop(self):
        logger.info(f'Stopping metrics reporter (run: {self._run_id}).')
        self.cancel()
