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
from threading import (Event,
                       Lock,
                       Thread)
from typing import (Callable,
                    Dict,
                    Generic,
                    Mapping,
                    Optional,
                    Tuple,
                    TypedDict,
                    TypeVar)

from couchbase.logic.observability.observability_types import (_ATTR_OPERATION_NAME,
                                                               _ATTR_SERVICE,
                                                               OpName,
                                                               ServiceType)
from couchbase.logic.pycbc_core import pycbc_hdr_histogram
from couchbase.observability.metrics import Meter, ValueRecorder

logger = logging.getLogger('couchbase.metrics')

K = TypeVar('K')
V = TypeVar('V')


class ConcurrentMap(Generic[K, V]):
    def __init__(self,
                 initial_data: Optional[Dict[K, V]] = None,
                 factory: Optional[Callable[[K], V]] = None):
        self._data = {}
        self._lock = Lock()
        self._data: Dict[K, V] = initial_data.copy() if initial_data else {}
        self._factory = factory

    def set(self, key: K, value: V) -> None:
        with self._lock:
            self._data[key] = value

    def get(self, key: K, default: Optional[V] = None):
        with self._lock:
            return self._data.get(key, default)

    def get_or_create(self, key: K) -> V:
        with self._lock:
            if key not in self._data:
                if self._factory is None:
                    raise KeyError(f'Key {key} not found and no factory provided.')
                self._data[key] = self._factory(key)
            return self._data[key]

    def flush(self) -> Dict[K, V]:
        with self._lock:
            snapshot = self._data
            self._data = {}
            return snapshot

    def items(self):
        with self._lock:
            # Return a static list so we don't get iteration size mutation errors
            return list(self._data.items())

    def __setitem__(self, key: K, value: V) -> None:
        """Allows: my_map['a'] = 1"""
        with self._lock:
            self._data[key] = value

    def __getitem__(self, key: K) -> V:
        """Allows: val = my_map['a']"""
        with self._lock:
            return self._data[key]

    def __delitem__(self, key: K) -> None:
        """Allows: del my_map['a']"""
        with self._lock:
            del self._data[key]

    def __contains__(self, key: K) -> bool:
        """Allows: if 'a' in my_map:"""
        with self._lock:
            return key in self._data

    def __len__(self) -> int:
        """Allows: count = len(my_map)"""
        with self._lock:
            return len(self._data)


class PercentileReport(TypedDict):
    total_count: int
    percentiles_us: Mapping[str, int]


class LoggingMeterReport(TypedDict):
    meta: Mapping[str, int]
    operations: Mapping[str, Mapping[str, PercentileReport]]


class LoggingMeterReporter(Thread):

    def __init__(self, *, logging_meter: LoggingMeter, interval: float) -> None:
        super().__init__()
        self.daemon = True
        self._lock = Lock()
        self._finished = Event()
        self._stopped = False
        self._logging_meter = logging_meter
        self._interval = interval

    @property
    def stopped(self) -> bool:
        return self._stopped

    def run(self):
        while not self._finished.is_set():
            if self._finished.wait(self._interval):
                break
            try:
                self._report()
            except Exception as e:
                logger.error(f'Failed to build report: {e}')

        try:
            self._report()
        except Exception as e:
            logger.error(f'Failed to build report: {e}')

    def stop(self) -> None:
        if self._stopped:
            return

        self._stopped = True
        if self.is_alive():
            self._finished.set()
            self.join(self._interval + 0.5)
            if self.is_alive():
                logger.warning('LoggingMeterReporter unable to shutdown.')

    def _report(self) -> None:
        report = self._logging_meter.create_report()
        if report:
            logger.info(json.dumps(report, separators=(',', ':')))


class LoggingValueRecorder(ValueRecorder):

    def __init__(self, op_name: OpName, service_type: ServiceType):
        self._op_name = op_name
        self._service_type = service_type
        self._lowest_discernible_value = 1  # 1 microsecond
        self._highest_trackable_value = 30_000_000  # 30 seconds
        self._significant_figures = 3
        # RFC required percentiles
        self._percentiles = [50.0, 90.0, 99.0, 99.9, 100.0]
        self._histogram = pycbc_hdr_histogram(
            lowest_discernible_value=self._lowest_discernible_value,
            highest_trackable_value=self._highest_trackable_value,
            significant_figures=self._significant_figures
        )

    def record_value(self, value: int) -> None:
        self._histogram.record_value(value)

    def get_percentiles_and_reset(self) -> PercentileReport:
        # what we get from the bindings:
        #   {
        #       "total_count": 100,               # An integer
        #       "percentiles": [5, 45, 25, 25]    # A list of integers (latencies in microseconds)
        #   }
        hdr_percentiles = self._histogram.get_percentiles_and_reset(self._percentiles)
        mapped_percentiles = dict(zip(
            [str(p) for p in self._percentiles],
            hdr_percentiles['percentiles']
        ))

        return {
            'total_count': hdr_percentiles['total_count'],
            'percentiles_us': mapped_percentiles
        }

    def close(self) -> None:
        self._histogram.reset()


class LoggingMeter(Meter):

    def __init__(self, emit_interval_ms: Optional[int] = None) -> None:
        if emit_interval_ms is None:
            emit_interval_ms = 600000
        self._emit_interval_s = emit_interval_ms / 1000

        recorded_services = {}
        for svc_type in ServiceType:
            def factory(op_name, svc_type=svc_type): return LoggingValueRecorder(op_name=op_name,
                                                                                 service_type=svc_type)
            recorded_services[svc_type] = ConcurrentMap(factory=factory)

        self._recorders: ConcurrentMap[
            ServiceType,
            ConcurrentMap[OpName, LoggingValueRecorder]
        ] = ConcurrentMap(recorded_services)

        # (service_str, op_str) -> recorder cache. Avoids OpName() and
        # ServiceType() enum construction on every value_recorder() call. There
        # are only ~8 KV op combinations so this fills up after the first ops.
        self._recorder_cache: Dict[Tuple[str, str], LoggingValueRecorder] = {}
        self._reporter = LoggingMeterReporter(logging_meter=self, interval=self._emit_interval_s)
        self._reporter.start()

    def value_recorder(self, name: str, tags: Dict[str, str]) -> ValueRecorder:
        # Use cached string constants instead of Enum.value on every call,
        # and cache the recorder by (service_str, op_str) to avoid OpName() and
        # ServiceType() enum construction.
        op_str = tags.get(_ATTR_OPERATION_NAME, None)
        svc_str = tags.get(_ATTR_SERVICE, None)
        lvr = self._recorder_cache.get((svc_str, op_str))
        if lvr is None:
            # Slow path: first encounter of this combination (~8 total for KV).
            # No lock needed — worst case two threads both miss and write the
            # same value; the result is identical and the assignment is atomic.
            op_name = OpName(op_str)
            service_type = ServiceType(svc_str)
            lvr = self._recorders[service_type].get_or_create(op_name)
            self._recorder_cache[(svc_str, op_str)] = lvr
        return lvr

    def create_report(self) -> LoggingMeterReport:
        report: LoggingMeterReport = {
            'meta': {'emit_interval_s': self._emit_interval_s},
            'operations': {}
        }
        for svc_type, op_map in self._recorders.items():
            svc_report = {}
            for op_name, recorder in op_map.flush().items():
                percentile_report = recorder.get_percentiles_and_reset()
                if percentile_report:
                    svc_report[op_name.value] = percentile_report
            if svc_report:
                report['operations'][svc_type.value] = svc_report
        return report

    def close(self) -> None:
        try:
            self._reporter.stop()
        except Exception:  # nosec
            # Don't raise exceptions during shutdown
            pass
