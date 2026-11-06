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

import json
import logging
from queue import (Empty,
                   Full,
                   Queue)
from threading import Timer
from time import perf_counter_ns
from typing import (Any,
                    Protocol,
                    TypeAlias,
                    TypedDict)

import psutil
from google.protobuf import timestamp_pb2 as timestamp

from metrics.top_level_pb2 import Result as MetricsResult
from run.top_level_pb2 import Result

logger = logging.getLogger(__name__)


class ProcCPU(Protocol):
    """The fields read from `psutil.Process.cpu_times()`, whose tuple type is private and per platform."""
    @property
    def user(self) -> float: ...
    @property
    def system(self) -> float: ...


class ThreadCPU(TypedDict):
    user_time: float
    system_time: float


threads_cpu: TypeAlias = dict[int, ThreadCPU]


class ReportQueue(Queue[Result]):
    def __init__(self) -> None:
        super().__init__()

    def __iter__(self) -> ReportQueue:
        return self

    def __next__(self) -> Result:
        try:
            return self.get_nowait()
        except Empty:
            raise StopIteration


class MetricsReporter(Timer):
    def __init__(self,
                 run_id: str,
                 interval: float,
                 proc: psutil.Process | None = None,
                 args: list[Any] | None = None,
                 kwargs: dict[str, Any] | None = None,
                 ) -> None:
        super().__init__(interval, self._compute_metrics, args, kwargs)
        self._run_id = run_id
        self._proc = proc or psutil.Process()
        self._last_report = 0
        self._last_proc_cpu: ProcCPU | None = None
        self._last_proc_threads: threads_cpu = {}
        self._report_queue = ReportQueue()

    @property
    def report_queue(self) -> ReportQueue:
        return self._report_queue

    def _calc_proc_cpu_percent(self, t1: ProcCPU, t2: ProcCPU, time_delta: float) -> float:
        if time_delta == 0:
            return 0.0

        delta_cpu = (t2.user - t1.user) + (t2.system - t1.system)
        # convert delta to milliseconds
        cpu_percent = ((delta_cpu * 1000) / time_delta) * 100
        return cpu_percent

    def _calc_proc_threads_cpu_percent(self, t1: threads_cpu, t2: threads_cpu, time_delta: float) -> dict[str, float]:
        delta_threads: dict[int, float] = {}
        for tid, cpu in t2.items():
            t1_match = t1.get(tid, None)
            if t1_match is None:
                continue
            delta_cpu = (cpu['user_time'] - t1_match['user_time']) + (cpu['system_time'] - t1_match['system_time'])
            # convert delta to milliseconds
            delta_threads[tid] = delta_cpu * 1000

        thread_cpu_percent: dict[str, float] = {}
        total_thread_cpu = 0.0
        for tid, dcpu in delta_threads.items():
            if time_delta == 0:
                thread_cpu_percent[str(tid)] = 0.0
                continue
            cpu_percent = (dcpu / time_delta) * 100
            thread_cpu_percent[str(tid)] = cpu_percent
            total_thread_cpu += cpu_percent

        thread_cpu_percent['total_thread_cpu'] = total_thread_cpu
        return thread_cpu_percent

    def _compute_metrics(self) -> None:
        now = perf_counter_ns()
        # for linux/macOS, returns CPU ticks / ticks per second which yields seconds
        proc_cpu = self._proc.cpu_times()
        threads = self._proc.threads()
        tdict: threads_cpu = {}
        for t in threads:
            if t.id not in tdict:
                tdict[t.id] = {'user_time': t.user_time, 'system_time': t.system_time}
        if self._last_proc_cpu is None:
            self._last_report = now
            self._last_proc_cpu = proc_cpu
            self._last_proc_threads = tdict
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
            initiated = timestamp.Timestamp()
            initiated.GetCurrentTime()
            metrics_result = Result(metrics=MetricsResult(metrics=report, initiated=initiated))
            self._report_queue.put_nowait(metrics_result)
        except Full:
            logger.debug(f'Queue is full, dropping report (run: {self._run_id}): {report}')
            pass  # ignore full queue

    # Override the run method to keep the timer running
    # Python3.10:
    # https://github.com/python/cpython/blob/10a2a9b3bcf237fd6183f84941632cda59395319/Lib/threading.py#L1375-L1379
    def run(self) -> None:
        while not self.finished.is_set():
            self.function(*self.args, **self.kwargs)
            self.finished.wait(self.interval)

    def stop(self) -> None:
        logger.info(f'Stopping metrics reporter (run: {self._run_id}).')
        self.cancel()
