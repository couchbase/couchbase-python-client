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
from dataclasses import dataclass
from enum import Enum
from heapq import heappop, heappush
from threading import (Event,
                       Lock,
                       Thread)
from time import time_ns
from typing import (Any,
                    Generic,
                    List,
                    Mapping,
                    Optional,
                    Tuple,
                    TypedDict,
                    TypeVar,
                    Union)

from couchbase.logic.observability.observability_types import (_ATTR_DISPATCH_SPAN_NAME,
                                                               _ATTR_ENCODING_SPAN_NAME,
                                                               _ATTR_SERVICE,
                                                               _DISP_LOCAL_ID,
                                                               _DISP_OPERATION_ID,
                                                               _DISP_PEER_ADDRESS,
                                                               _DISP_PEER_PORT,
                                                               _DISP_SERVER_ADDRESS,
                                                               _DISP_SERVER_DURATION,
                                                               _DISP_SERVER_PORT,
                                                               ServiceType)
from couchbase.logic.options import ClusterTracingOptionsBase
from couchbase.observability.tracing import (RequestSpan,
                                             RequestTracer,
                                             SpanAttributeValue,
                                             SpanStatusCode)

logger = logging.getLogger('couchbase.threshold')

T = TypeVar('T')

ThresholdLogRecord = Mapping[str, Union[str, int, float]]


class ThresholdLoggingAttributeName(Enum):
    EncodeDuration = 'encode_duration_us'
    OperationName = 'operation_name'
    OperationId = 'operation_id'
    LastDispatchDuration = 'last_dispatch_duration_us'
    LastLocalId = 'last_local_id'
    LastLocalSocket = 'last_local_socket'
    LastRemoteSocket = 'last_remote_socket'
    LastServerDuration = 'last_server_duration_us'
    Timeout = 'timeout_ms'
    TotalDuration = 'total_duration_us'
    TotalDispatchDuration = 'total_dispatch_duration_us'
    TotalServerDuration = 'total_server_duration_us'


class IgnoredMultiOpSpan(Enum):
    AppendMulti = 'append_multi'
    DecrementMulti = 'decrement_multi'
    ExistsMulti = 'exists_multi'
    GetMulti = 'get_multi'
    GetAllReplicasMulti = 'get_all_replicas_multi'
    GetAndLockMulti = 'get_and_lock_multi'
    GetAndTouchMulti = 'get_and_touch_multi'
    GetAnyReplicaMulti = 'get_any_replica_multi'
    IncrementMulti = 'increment_multi'
    InsertMulti = 'insert_multi'
    PrependMulti = 'prepend_multi'
    RemoveMulti = 'remove_multi'
    ReplaceMulti = 'replace_multi'
    TouchMulti = 'touch_multi'
    UnlockMulti = 'unlock_multi'
    UpsertMulti = 'upsert_multi'


_IGNORED_MULTI_OP_SPAN_VALUES = frozenset(span.value for span in IgnoredMultiOpSpan)


class IgnoredParentSpan(Enum):
    ListAppend = 'list_append'
    ListClear = 'list_clear'
    ListGetAll = 'list_get_all'
    ListGetAt = 'list_get_at'
    ListIndexOf = 'list_index_of'
    ListPrepend = 'list_prepend'
    ListRemoveAt = 'list_remove_at'
    ListSetAt = 'list_set_at'
    ListSize = 'list_size'
    MapAdd = 'map_add'
    MapClear = 'map_clear'
    MapExists = 'map_exists'
    MapGet = 'map_get'
    MapGetAll = 'map_get_all'
    MapItems = 'map_items'
    MapKeys = 'map_keys'
    MapRemove = 'map_remove'
    MapSize = 'map_size'
    MapValues = 'map_values'
    QueueClear = 'queue_clear'
    QueuePop = 'queue_pop'
    QueuePush = 'queue_push'
    QueueSize = 'queue_size'
    SetAdd = 'set_add'
    SetClear = 'set_clear'
    SetContains = 'set_contains'
    SetRemove = 'set_remove'
    SetSize = 'set_size'
    SetValues = 'set_values'


_IGNORED_PARENT_SPAN_VALUES = frozenset(span.value for span in IgnoredParentSpan)

# Only these attribute keys are actually processed in ThresholdLoggingSpan.set_attribute.
_PROCESSED_ATTRIBUTE_KEYS = frozenset({
    _ATTR_SERVICE,
    _DISP_SERVER_DURATION,
    _DISP_LOCAL_ID,
    _DISP_OPERATION_ID,
    _DISP_PEER_ADDRESS,
    _DISP_PEER_PORT,
    _DISP_SERVER_ADDRESS,
    _DISP_SERVER_PORT,
})


@dataclass(frozen=True)
class ThresholdLoggingSpanSnapshot:
    """Immutable snapshot of span state for threshold logging."""
    name: str
    service_type: Optional[ServiceType]
    total_duration_ns: int
    encode_duration_ns: Optional[int]
    dispatch_duration_ns: Optional[int]
    total_dispatch_duration_ns: int
    server_duration_ns: Optional[int]
    total_server_duration_ns: int
    local_id: Optional[str]
    operation_id: Optional[str]
    local_socket: Optional[str]
    remote_socket: Optional[str]


class PriorityQueue(Generic[T]):
    """
    Bounded priority queue for tracking top N items by priority.

    Note on dropped_count:
        The dropped_count tracks the total number of items that were not kept in the queue
        (either because they had lower priority than existing items, or because the queue
        was at capacity). This allows reporting total_count = len(items) + dropped_count,
        which represents all items that exceeded the threshold, not just the top N.
    """

    def __init__(self, max_size: Optional[int] = None) -> None:
        self._heap: List[Tuple[int, int, T]] = []
        self._max_size = max_size or 10
        self._counter = 0
        self._dropped_count = 0

    def enqueue(self, item: T, priority: int) -> bool:
        if len(self._heap) >= self._max_size:
            self._dropped_count += 1
            # If at capacity, only add if new item has higher priority than min
            # heap[0] is the lowest priority item (since we store positive priorities)
            if self._heap and priority <= self._heap[0][0]:
                return False
            heappop(self._heap)

        new_item = (priority, self._counter, item)
        self._counter += 1
        heappush(self._heap, new_item)
        return True

    def peek(self) -> Optional[T]:
        return self._heap[0][2] if len(self._heap) > 0 else None

    def drain(self) -> Tuple[List[T], int]:
        # Sort entries by descending priority, then FIFO order (counter ascending)
        # This returns top requests with highest durations first
        sorted_entries = sorted(self._heap, key=lambda e: (-e[0], e[1]))
        items = [entry[2] for entry in sorted_entries]
        self._heap.clear()

        dropped_count = self._dropped_count
        self._dropped_count = 0
        self._counter = 0
        return items, dropped_count


class ThresholdLoggingServiceReport(TypedDict):
    total_count: int
    top_requests: List[ThresholdLogRecord]


class ThresholdLoggingReporter(Thread):

    def __init__(
            self, *, interval: float = 10, max_size: Optional[int] = 10
    ) -> None:
        super().__init__()
        self.daemon = True
        self._lock = Lock()
        self._finished = Event()
        self._stopped = False
        self._interval = interval
        self._queues = {
            ServiceType.KeyValue: PriorityQueue[ThresholdLogRecord](max_size),
            ServiceType.Query: PriorityQueue[ThresholdLogRecord](max_size),
            ServiceType.Search: PriorityQueue[ThresholdLogRecord](max_size),
            ServiceType.Analytics: PriorityQueue[ThresholdLogRecord](max_size),
            ServiceType.Views: PriorityQueue[ThresholdLogRecord](max_size),
            ServiceType.Management: PriorityQueue[ThresholdLogRecord](max_size),
            ServiceType.Eventing: PriorityQueue[ThresholdLogRecord](max_size)
        }

    @property
    def stopped(self) -> bool:
        return self._stopped

    def add_log_record(self, service_type: ServiceType, record: ThresholdLogRecord, total_duration: int) -> None:
        if service_type in self._queues:
            with self._lock:
                self._queues[service_type].enqueue(record, total_duration)

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
                logger.warning('ThresholdLoggingReporter unable to shutdown.')

    def _report(self) -> None:
        with self._lock:
            report: Mapping[str, ThresholdLoggingServiceReport] = {}
            for service_type, queue in self._queues.items():
                items, dropped_count = queue.drain()
                if items:
                    report[service_type.value] = {
                        'total_count': len(items) + dropped_count,
                        'top_requests': [rec for rec in items]
                    }

        if report:
            logger.info(json.dumps(report, separators=(',', ':')))


class ThresholdLoggingSpan(RequestSpan):

    # Use __slots__ to elimiate per-instance __dict__, making attribute access
    # faster (C-level slot lookup) and reducing memory allocation.
    __slots__ = (
        '_name', '_parent_span', '_start_time_ns', '_tracer',
        '_events', '_status', '_end_time_ns',
        '_service_type', '_local_id', '_operation_id',
        '_peer_address', '_peer_port', '_remote_address', '_remote_port',
        '_encode_duration_ns', '_dispatch_duration_ns', '_server_duration_ns',
        '_total_dispatch_duration_ns', '_total_encode_duration_ns',
        '_total_server_duration_ns', '_total_duration_ns', '_span_snapshot',
    )

    # Class-level flag allowing WrappedSpan to detect that this span type
    # supports the fast multi-op dispatch path (bypassing child span creation).
    # __slots__ only covers instance attributes; class attributes sit on the
    # class and are visible via getattr/instance lookup without a slot entry.
    _supports_multi_op_fast_dispatch: bool = True
    # Advertise the set of keys this span actually processes so callers can
    # skip set_attribute() for keys that would be no-ops.
    _processed_attribute_keys: frozenset = _PROCESSED_ATTRIBUTE_KEYS

    def __init__(
        self,
        name: str,
        parent_span: Optional[ThresholdLoggingSpan] = None,
        start_time: Optional[int] = None,
        tracer: Optional[ThresholdLoggingTracer] = None
    ) -> None:
        self._name = name
        self._parent_span = parent_span
        self._start_time_ns = start_time if start_time is not None else time_ns()
        self._tracer = tracer
        self._events = {}
        self._status = SpanStatusCode.UNSET
        self._end_time_ns: Optional[int] = None
        self._service_type: Optional[ServiceType] = None
        self._local_id: Optional[str] = None
        self._operation_id: Optional[str] = None
        self._peer_address: Optional[str] = None
        self._peer_port: Optional[int] = None
        self._remote_address: Optional[str] = None
        self._remote_port: Optional[int] = None
        self._encode_duration_ns: Optional[int] = None
        self._dispatch_duration_ns: Optional[int] = None
        self._server_duration_ns: Optional[int] = None
        self._total_dispatch_duration_ns = 0
        self._total_encode_duration_ns = 0
        self._total_server_duration_ns = 0
        self._total_duration_ns = 0
        self._span_snapshot: Optional[ThresholdLoggingSpanSnapshot] = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def local_socket(self) -> Optional[str]:
        if self._peer_address is not None or self._peer_port is not None:
            address = self._peer_address or ''
            port = self._peer_port or ''
            return f'{address}:{port}'
        return None

    @property
    def remote_socket(self) -> Optional[str]:
        if self._remote_address is not None or self._remote_port is not None:
            address = self._remote_address or ''
            port = self._remote_port or ''
            return f'{address}:{port}'
        return None

    def _propagate_dispatch_duration(self, value: int) -> None:
        """Set dispatch duration on this span and propagate to parent (non-recursive)."""
        span = self
        while span is not None:
            span._dispatch_duration_ns = value
            span._total_dispatch_duration_ns += value
            span = span._parent_span

    def _propagate_encode_duration(self, value: int) -> None:
        """Set encode duration on this span and propagate to parent (non-recursive)."""
        span = self
        while span is not None:
            span._encode_duration_ns = value
            span._total_encode_duration_ns += value
            span = span._parent_span

    def set_attribute(self, key: str, value: SpanAttributeValue) -> None:  # noqa: C901
        if key not in _PROCESSED_ATTRIBUTE_KEYS:
            return

        span = self
        while span is not None:
            if key == _ATTR_SERVICE:
                span._service_type = ServiceType.from_str(value)
            elif key == _DISP_SERVER_DURATION:
                int_val = int(value)
                span._server_duration_ns = int_val
                span._total_server_duration_ns += int_val
            elif key == _DISP_LOCAL_ID:
                span._local_id = str(value)
            elif key == _DISP_OPERATION_ID:
                span._operation_id = str(value)
            elif key == _DISP_PEER_ADDRESS:
                span._peer_address = str(value)
            elif key == _DISP_PEER_PORT:
                span._peer_port = int(value)
            elif key == _DISP_SERVER_ADDRESS:
                span._remote_address = str(value)
            elif key == _DISP_SERVER_PORT:
                span._remote_port = int(value)
            span = span._parent_span

    def set_attributes(self, attributes: Mapping[str, SpanAttributeValue]) -> None:
        for k, v in attributes.items():
            self.set_attribute(k, v)

    def apply_core_span_attributes(self, attributes: Mapping[str, Any]) -> None:  # noqa: C901
        """Apply a dict of dispatch-span attributes in a single parent-chain walk.

        Replaces N separate set_attribute() calls (each walking the parent chain)
        with one pass that processes all keys at every level in one traversal.
        """
        span = self
        while span is not None:
            server_duration: Optional[int] = None
            for key, value in attributes.items():
                if key == _DISP_SERVER_DURATION:
                    server_duration = int(value)
                    span._server_duration_ns = server_duration
                elif key == _DISP_LOCAL_ID:
                    span._local_id = str(value)
                elif key == _DISP_OPERATION_ID:
                    span._operation_id = str(value)
                elif key == _DISP_PEER_ADDRESS:
                    span._peer_address = str(value)
                elif key == _DISP_PEER_PORT:
                    span._peer_port = int(value)
                elif key == _DISP_SERVER_ADDRESS:
                    span._remote_address = str(value)
                elif key == _DISP_SERVER_PORT:
                    span._remote_port = int(value)
                elif key == _ATTR_SERVICE:
                    span._service_type = ServiceType.from_str(value)
            if server_duration is not None:
                span._total_server_duration_ns += server_duration
            span = span._parent_span

    def add_event(self, name: str, value: SpanAttributeValue) -> None:
        self._events[name] = value

    def set_status(self, status: SpanStatusCode) -> None:
        self._status = status

    def end(self, end_time: Optional[int] = None) -> None:
        if self._end_time_ns is not None:
            return

        self._end_time_ns = end_time if end_time is not None else time_ns()
        self._total_duration_ns = self._end_time_ns - self._start_time_ns

        # Encoding/dispatch spans only propagate duration to parent; no snapshot needed.
        if self._name == _ATTR_ENCODING_SPAN_NAME:
            self._propagate_encode_duration(self._total_duration_ns)
            return
        elif self._name == _ATTR_DISPATCH_SPAN_NAME:
            self._propagate_dispatch_duration(self._total_duration_ns)
            return

        # Multi-op wrapper spans and their direct children are not threshold-checked.
        if (self._name in _IGNORED_MULTI_OP_SPAN_VALUES
                or (self._parent_span and self._parent_span._name in _IGNORED_MULTI_OP_SPAN_VALUES)):
            return

        # Build snapshot only for spans that require threshold evaluation.
        snapshot = ThresholdLoggingSpanSnapshot(
            name=self._name,
            service_type=self._service_type,
            total_duration_ns=self._total_duration_ns,
            encode_duration_ns=self._total_encode_duration_ns,
            dispatch_duration_ns=self._dispatch_duration_ns,
            total_dispatch_duration_ns=self._total_dispatch_duration_ns,
            server_duration_ns=self._server_duration_ns,
            total_server_duration_ns=self._total_server_duration_ns,
            local_id=self._local_id,
            operation_id=self._operation_id,
            local_socket=self.local_socket,
            remote_socket=self.remote_socket,
        )
        self._span_snapshot = snapshot

        if self._parent_span is not None and self._parent_span._name in _IGNORED_PARENT_SPAN_VALUES:
            if self._tracer:
                self._tracer.check_threshold(snapshot)
        elif (self._parent_span is None
              and self._tracer
              and self._name not in _IGNORED_PARENT_SPAN_VALUES):
            self._tracer.check_threshold(snapshot)


class ThresholdLoggingTracer(RequestTracer):

    def __init__(self, config: Optional[ClusterTracingOptionsBase] = None) -> None:
        if config is None:
            config = {}
        self._emit_interval_ms: int = config.get('threshold_emit_interval', 10000)
        self._sample_size: int = config.get('threshold_sample_size', 10)
        self._service_thresholds = {
            ServiceType.KeyValue: config.get('key_value_threshold', 500),
            ServiceType.Query: config.get('query_threshold', 1000),
            ServiceType.Search: config.get('search_threshold', 1000),
            ServiceType.Analytics: config.get('analytics_threshold', 1000),
            ServiceType.Management: config.get('management_threshold', 1000),
            ServiceType.Eventing: config.get('eventing_threshold', 1000),
            ServiceType.Views: config.get('view_threshold', 1000),
        }
        self._reporter = ThresholdLoggingReporter(interval=self._emit_interval_ms / 1000, max_size=self._sample_size)
        self._reporter.start()

    def close(self) -> None:
        """Stop the reporter thread and perform a final report."""
        try:
            self._reporter.stop()
        except Exception:  # nosec
            # Don't raise exceptions during shutdown
            pass

    def check_threshold(self, snapshot: ThresholdLoggingSpanSnapshot) -> None:
        """Check if span threshold is exceeded and enqueue for reporting."""
        if snapshot.service_type is None:
            return

        service_threshold_us = self._get_service_type_threshold(snapshot.service_type)
        # convert to micros
        span_total_duration_us = snapshot.total_duration_ns / 1000
        if span_total_duration_us <= service_threshold_us:
            return

        threshold_log_record = self._build_threshold_log_record(snapshot, span_total_duration_us)
        self._reporter.add_log_record(snapshot.service_type, threshold_log_record, int(span_total_duration_us))

    def request_span(
        self,
        name: str,
        parent_span: Optional[RequestSpan] = None,
        start_time: Optional[int] = None
    ) -> RequestSpan:
        return ThresholdLoggingSpan(name, parent_span=parent_span, start_time=start_time, tracer=self)

    def _build_threshold_log_record(self,
                                    snapshot: ThresholdLoggingSpanSnapshot,
                                    span_total_duration: int,
                                    ) -> ThresholdLogRecord:
        threshold_log_record: ThresholdLogRecord = {
            ThresholdLoggingAttributeName.OperationName.value: snapshot.name,
            ThresholdLoggingAttributeName.TotalDuration.value: span_total_duration
        }

        if snapshot.encode_duration_ns is not None:
            # convert to micros
            threshold_log_record[
                ThresholdLoggingAttributeName.EncodeDuration.value
            ] = snapshot.encode_duration_ns / 1000

        if snapshot.dispatch_duration_ns is not None:
            threshold_log_record[
                ThresholdLoggingAttributeName.LastDispatchDuration.value
            ] = snapshot.dispatch_duration_ns / 1000

        threshold_log_record[
            ThresholdLoggingAttributeName.TotalDispatchDuration.value
        ] = snapshot.total_dispatch_duration_ns / 1000

        if snapshot.server_duration_ns is not None:
            threshold_log_record[
                ThresholdLoggingAttributeName.LastServerDuration.value
            ] = snapshot.server_duration_ns / 1000

        threshold_log_record[
            ThresholdLoggingAttributeName.TotalServerDuration.value
        ] = snapshot.total_server_duration_ns / 1000

        if snapshot.local_id is not None:
            threshold_log_record[
                ThresholdLoggingAttributeName.LastLocalId.value
            ] = snapshot.local_id

        if snapshot.operation_id is not None:
            threshold_log_record[
                ThresholdLoggingAttributeName.OperationId.value
            ] = snapshot.operation_id

        if snapshot.local_socket is not None:
            threshold_log_record[
                ThresholdLoggingAttributeName.LastLocalSocket.value
            ] = snapshot.local_socket

        if snapshot.remote_socket is not None:
            threshold_log_record[
                ThresholdLoggingAttributeName.LastRemoteSocket.value
            ] = snapshot.remote_socket

        return threshold_log_record

    def _get_service_type_threshold(self, service_type: ServiceType) -> int:
        base_threshold = 0
        if service_type in self._service_thresholds:
            base_threshold = self._service_thresholds[service_type]

        # convert to micros
        return base_threshold * 1000
