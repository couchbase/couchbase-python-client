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
from collections.abc import Callable, Generator
from time import time_ns
from typing import (Any,
                    ClassVar,
                    cast)

import pytest

from couchbase.logic.observability.observability_types import (DispatchAttributeName,
                                                               OpAttributeName,
                                                               ServiceType)
from couchbase.logic.observability.threshold_logging import ThresholdLoggingReporter, ThresholdLoggingTracer
from couchbase.logic.options import ClusterTracingOptionsBase
from tests.manifest import validate_test_manifest
from tests.observability_test_impls import RecordingReporter

# 0.5ms, which the tracer converts to 500us.  Every duration in this file is chosen to sit
# clearly on one side of it, so nothing here depends on how the boundary itself is rounded.
KV_THRESHOLD_MS = 0.5

# The reporter's own emit interval, long enough that it never fires during a test.  Only the
# queue-draining tests need it: the recording reporter has no interval at all.
NO_AUTO_EMIT_S = 100_000


class ThresholdLoggingTestSuite:
    # No cb_env: spans are built by hand with explicit start and end timestamps rather than by
    # running an operation, which is what makes every duration here exact instead of
    # approximate.  The tracer is api-agnostic, so nothing is generated from this.
    #
    # Two ways to observe what the tracer produced, and both are used because they answer
    # different questions.  `make_recording_tracer` swaps in `RecordingReporter` to see what the
    # tracer hands off, in order, per span.  `make_tracer` keeps the real reporter, stopped, and
    # drains its queues to see what survived the per-service priority queue, which is where the
    # sample-size cap and the drop count live.  Neither lets the reporter thread run: it wakes
    # on an emit interval and writes to a logger, so waiting for it would be timing-dependent
    # for no gain.

    TEST_MANIFEST: ClassVar[list[str]] = [
        'test_async_callback_lifecycle',
        'test_children_span_with_duration',
        'test_enqueue_only_above_threshold',
        'test_exceeding_sample_size',
        'test_high_concurrency_mixed_operations',
        'test_ignore_below_threshold',
        'test_multiple_services',
        'test_sequential_multi_dispatch_accumulation',
        'test_span_conversion_with_all_attributes',
        'test_span_end_is_idempotent',
    ]

    @pytest.fixture(name='make_tracer')
    def make_tracer_fixture(self) -> Generator[Callable[[dict[str, Any]], ThresholdLoggingTracer], None, None]:
        # The stop happens before the test gets the tracer, and `close()` runs however the test
        # ends.  A `tracer.close()` written as the last statement of a test body is skipped by
        # the first failing assertion above it, and easy to leave off a test entirely.
        tracers: list[ThresholdLoggingTracer] = []

        def _make(config: dict[str, Any]) -> ThresholdLoggingTracer:
            # ThresholdLoggingTracer declares ClusterTracingOptionsBase, which is a dict
            # subclass whose constructor takes the *public* option names and converts them.
            # The tracer reads the converted names, which is what these configs already spell,
            # so the mapping goes in as-is.
            tracer = ThresholdLoggingTracer(cast(ClusterTracingOptionsBase, config))
            tracer._reporter.stop()
            tracers.append(tracer)
            return tracer

        yield _make

        for tracer in tracers:
            tracer.close()

    @pytest.fixture(name='make_recording_tracer')
    def make_recording_tracer_fixture(
        self,
        make_tracer: Callable[[dict[str, Any]], ThresholdLoggingTracer],
    ) -> Callable[[dict[str, Any]], tuple[ThresholdLoggingTracer, RecordingReporter]]:
        def _make(config: dict[str, Any]) -> tuple[ThresholdLoggingTracer, RecordingReporter]:
            tracer = make_tracer(config)
            reporter = RecordingReporter()
            tracer._reporter = cast(ThresholdLoggingReporter, reporter)
            return tracer, reporter

        return _make

    @staticmethod
    def kv_span(tracer: ThresholdLoggingTracer, name: str, start_time: int) -> Any:
        # Tagged as key-value, which is what routes the span to the KV threshold.
        span = tracer.request_span(name, start_time=start_time)
        span.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)
        return span

    def test_enqueue_only_above_threshold(
        self,
        make_recording_tracer: Callable[[dict[str, Any]], tuple[ThresholdLoggingTracer, RecordingReporter]],
    ) -> None:
        tracer, reporter = make_recording_tracer({'key_value_threshold': KV_THRESHOLD_MS,
                                                  'threshold_sample_size': 10})

        start_ns = time_ns() - 10_000_000
        span = self.kv_span(tracer, 'test_operation', start_ns)
        span.end(end_time=start_ns + 1_000_000)  # 1ms, so 1000us, above the 500us threshold

        assert len(reporter.records) == 1
        service_type, record, _ = reporter.records[0]
        assert service_type == ServiceType.KeyValue
        assert record['operation_name'] == 'test_operation'
        assert record['total_duration_us'] == 1000.0

    def test_ignore_below_threshold(
        self,
        make_recording_tracer: Callable[[dict[str, Any]], tuple[ThresholdLoggingTracer, RecordingReporter]],
    ) -> None:
        tracer, reporter = make_recording_tracer({'key_value_threshold': KV_THRESHOLD_MS,
                                                  'threshold_sample_size': 10})

        start_ns = time_ns() - 10_000_000
        span = self.kv_span(tracer, 'test_operation', start_ns)
        span.end(end_time=start_ns + 100_000)  # 0.1ms, so 100us, below the 500us threshold

        assert reporter.records == []

    def test_children_span_with_duration(
        self,
        make_recording_tracer: Callable[[dict[str, Any]], tuple[ThresholdLoggingTracer, RecordingReporter]],
    ) -> None:
        tracer, reporter = make_recording_tracer({'key_value_threshold': KV_THRESHOLD_MS,
                                                  'threshold_sample_size': 10})

        parent_start_ns = time_ns() - 1_000_000
        parent = self.kv_span(tracer, 'parent_operation', parent_start_ns)

        dispatch = tracer.request_span(OpAttributeName.DispatchSpanName.value,
                                       parent_span=parent,
                                       start_time=parent_start_ns)
        dispatch.end(end_time=parent_start_ns + 200_000)

        parent.end(end_time=parent_start_ns + 1_500_000)

        # The point is that ending the child wrote through to the parent: the parent is the only
        # span reported, and it carries the child's duration.
        assert len(reporter.records) == 1
        service_type, record, _ = reporter.records[0]
        assert service_type == ServiceType.KeyValue
        assert record['last_dispatch_duration_us'] == 200.0

    def test_span_end_is_idempotent(
        self,
        make_recording_tracer: Callable[[dict[str, Any]], tuple[ThresholdLoggingTracer, RecordingReporter]],
    ) -> None:
        tracer, reporter = make_recording_tracer({'key_value_threshold': KV_THRESHOLD_MS,
                                                  'threshold_sample_size': 10})

        start_ns = time_ns() - 10_000_000
        span = self.kv_span(tracer, 'test_operation', start_ns)

        span.end(end_time=start_ns + 1_000_000)
        span.end(end_time=start_ns + 1_500_000)  # must be ignored, not re-reported or re-timed

        assert len(reporter.records) == 1
        _, record, _ = reporter.records[0]
        assert record['total_duration_us'] == 1000.0

    def test_exceeding_sample_size(
        self,
        make_tracer: Callable[[dict[str, Any]], ThresholdLoggingTracer],
    ) -> None:
        tracer = make_tracer({'key_value_threshold': KV_THRESHOLD_MS,
                              'threshold_sample_size': 3,
                              'threshold_emit_interval': NO_AUTO_EMIT_S})

        operations = [('replace', 1000), ('insert', 1100), ('upsert', 1050),
                      ('get', 1200), ('remove', 1150)]

        now = time_ns()
        for op_name, duration_us in operations:
            start_ns = now - duration_us * 1000
            span = self.kv_span(tracer, op_name, start_ns)
            span.set_attribute(OpAttributeName.BucketName.value, 'default')
            span.set_attribute(OpAttributeName.ScopeName.value, '_default')
            span.set_attribute(OpAttributeName.CollectionName.value, '_default')
            span.end(end_time=start_ns + duration_us * 1000)

        items, dropped_count = tracer._reporter._queues[ServiceType.KeyValue].drain()

        assert len(items) == 3
        assert dropped_count == 2

        durations = [item['total_duration_us'] for item in items]
        assert durations == sorted(durations, reverse=True)

        # The three slowest are kept and the two fastest are dropped, which is the whole
        # contract: the cap is on what is retained, not on what is accepted.
        assert [item['operation_name'] for item in items] == ['get', 'remove', 'insert']

    def test_multiple_services(
        self,
        make_tracer: Callable[[dict[str, Any]], ThresholdLoggingTracer],
    ) -> None:
        tracer = make_tracer({'key_value_threshold': KV_THRESHOLD_MS,
                              'query_threshold': KV_THRESHOLD_MS,
                              'threshold_sample_size': 10,
                              'threshold_emit_interval': NO_AUTO_EMIT_S})

        now = time_ns()

        for op_name, duration_us in [('replace', 1000), ('insert', 1100)]:
            start_ns = now - duration_us * 1000
            span = self.kv_span(tracer, op_name, start_ns)
            span.set_attribute(OpAttributeName.BucketName.value, 'default')
            span.set_attribute(OpAttributeName.ScopeName.value, '_default')
            span.set_attribute(OpAttributeName.CollectionName.value, '_default')
            span.end(end_time=start_ns + duration_us * 1000)

        for op_name, duration_us in [('query1', 600), ('query2', 1200)]:
            start_ns = now - duration_us * 1000
            span = tracer.request_span(op_name, start_time=start_ns)
            span.set_attribute(OpAttributeName.Service.value, ServiceType.Query.value)
            span.end(end_time=start_ns + duration_us * 1000)

        kv_items, _ = tracer._reporter._queues[ServiceType.KeyValue].drain()
        query_items, _ = tracer._reporter._queues[ServiceType.Query].drain()

        # Each service has its own queue, so neither set of operations can crowd out the other.
        assert sorted(item['operation_name'] for item in kv_items) == ['insert', 'replace']
        assert sorted(item['operation_name'] for item in query_items) == ['query1', 'query2']

    def test_span_conversion_with_all_attributes(
        self,
        make_tracer: Callable[[dict[str, Any]], ThresholdLoggingTracer],
    ) -> None:
        tracer = make_tracer({'key_value_threshold': KV_THRESHOLD_MS,
                              'threshold_sample_size': 10,
                              'threshold_emit_interval': NO_AUTO_EMIT_S})

        op_start_ns = time_ns() - 1_000_000
        encode_start_ns = op_start_ns + 100_000
        encode_end_ns = encode_start_ns + 100_000
        dispatch_start_ns = encode_end_ns + 200_000
        dispatch_end_ns = dispatch_start_ns + 400_000

        op_span = self.kv_span(tracer, 'replace', op_start_ns)
        op_span.set_attribute(OpAttributeName.BucketName.value, 'default')
        op_span.set_attribute(OpAttributeName.ScopeName.value, '_default')
        op_span.set_attribute(OpAttributeName.CollectionName.value, '_default')

        encoding_span = tracer.request_span(OpAttributeName.EncodingSpanName.value,
                                            parent_span=op_span,
                                            start_time=encode_start_ns)
        encoding_span.end(end_time=encode_end_ns)

        dispatch_span = tracer.request_span(OpAttributeName.DispatchSpanName.value,
                                            parent_span=op_span,
                                            start_time=dispatch_start_ns)
        dispatch_span.set_attribute(DispatchAttributeName.LocalId.value, 'local1')
        dispatch_span.set_attribute(DispatchAttributeName.OperationId.value, 'op1')
        dispatch_span.set_attribute(DispatchAttributeName.PeerAddress.value, '1.2.3.4')
        dispatch_span.set_attribute(DispatchAttributeName.PeerPort.value, 11210)
        dispatch_span.set_attribute(DispatchAttributeName.ServerAddress.value, '1.2.3.5')
        dispatch_span.set_attribute(DispatchAttributeName.ServerPort.value, 11210)
        dispatch_span.set_attribute(DispatchAttributeName.ServerDuration.value, 300_000)
        dispatch_span.end(end_time=dispatch_end_ns)

        op_span.end(end_time=op_start_ns + 1_000_000)

        kv_items, _ = tracer._reporter._queues[ServiceType.KeyValue].drain()
        assert len(kv_items) == 1
        record = kv_items[0]

        # Compared as one mapping rather than as twenty separate asserts, so a failure names
        # every field that moved instead of stopping at the first.  The two socket strings are
        # the only derived fields here: the conversion joins each address to its port.
        expected = {'operation_name': 'replace',
                    'total_duration_us': 1000.0,
                    'encode_duration_us': 100.0,
                    'last_dispatch_duration_us': 400.0,
                    'total_dispatch_duration_us': 400.0,
                    'last_server_duration_us': 300.0,
                    'total_server_duration_us': 300.0,
                    'last_local_id': 'local1',
                    'operation_id': 'op1',
                    'last_local_socket': '1.2.3.4:11210',
                    'last_remote_socket': '1.2.3.5:11210'}
        missing = [key for key in expected if key not in record]
        assert not missing, f'record is missing {missing}; it holds {sorted(record)}'
        assert {key: record[key] for key in expected} == expected

    def test_sequential_multi_dispatch_accumulation(
        self,
        make_tracer: Callable[[dict[str, Any]], ThresholdLoggingTracer],
    ) -> None:
        # The shape `_build_core_spans` produces: the C++ I/O callback thread creates and ends
        # each child in turn, so the parent sees them sequentially rather than all at once.
        tracer = make_tracer({'key_value_threshold': KV_THRESHOLD_MS,
                              'threshold_sample_size': 10,
                              'threshold_emit_interval': NO_AUTO_EMIT_S})

        parent_start_ns = time_ns() - 3_000_000
        parent = self.kv_span(tracer, 'upsert', parent_start_ns)

        encoding = tracer.request_span(OpAttributeName.EncodingSpanName.value,
                                       parent_span=parent,
                                       start_time=parent_start_ns + 100_000)
        encoding.end(end_time=parent_start_ns + 250_000)  # 150us

        for start_offset, end_offset in ((500_000, 900_000), (1_200_000, 1_550_000)):  # 400us, 350us
            dispatch = tracer.request_span(OpAttributeName.DispatchSpanName.value,
                                           parent_span=parent,
                                           start_time=parent_start_ns + start_offset)
            dispatch.end(end_time=parent_start_ns + end_offset)

        parent.end(end_time=parent_start_ns + 3_000_000)

        assert parent._total_encode_duration_ns == 150_000
        assert parent._total_dispatch_duration_ns == 750_000
        assert parent._span_snapshot is not None
        assert parent._span_snapshot.total_dispatch_duration_ns == 750_000

    def test_async_callback_lifecycle(
        self,
        make_recording_tracer: Callable[[dict[str, Any]], tuple[ThresholdLoggingTracer, RecordingReporter]],
    ) -> None:
        # `AsyncClientAdapter._callback` gives the I/O thread exclusive access to the root span
        # while the event loop is parked on `await future`, then hands back through
        # `call_soon_threadsafe`.  The two events below enforce that ordering without an event
        # loop, and the switch interval is dropped to its minimum so an ordering assumption that
        # only holds by luck gets many chances to fail.  The asyncio-driven version lives in
        # `acouchbase/tests/`, because what it exercises is those two calls themselves.
        tracer, reporter = make_recording_tracer({'key_value_threshold': KV_THRESHOLD_MS,
                                                  'threshold_sample_size': 200})

        iterations = 200
        original_interval = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)
        try:
            for _ in range(iterations):
                root = self.kv_span(tracer, 'upsert', time_ns() - 2_000_000)

                callback_may_proceed = threading.Event()
                callback_done = threading.Event()

                def simulate_io_callback(root: Any = root) -> None:
                    callback_may_proceed.wait()
                    dispatch = tracer.request_span(OpAttributeName.DispatchSpanName.value,
                                                   parent_span=root,
                                                   start_time=time_ns() - 500_000)
                    dispatch.set_attribute(DispatchAttributeName.ServerDuration.value, 1_000)
                    dispatch.set_attribute(DispatchAttributeName.PeerAddress.value, '127.0.0.1')
                    dispatch.set_attribute(DispatchAttributeName.PeerPort.value, 11210)
                    dispatch.end(end_time=time_ns())
                    callback_done.set()

                thread = threading.Thread(target=simulate_io_callback)
                thread.start()
                callback_may_proceed.set()
                callback_done.wait()
                thread.join()

                root.end(end_time=time_ns())

                assert root._total_dispatch_duration_ns > 0
                assert root._total_server_duration_ns == 1_000
                assert root._span_snapshot is not None
        finally:
            sys.setswitchinterval(original_interval)

        assert len(reporter.records) == iterations

    def test_high_concurrency_mixed_operations(
        self,
        make_recording_tracer: Callable[[dict[str, Any]], tuple[ThresholdLoggingTracer, RecordingReporter]],
    ) -> None:
        tracer, reporter = make_recording_tracer({'key_value_threshold': KV_THRESHOLD_MS,
                                                  'threshold_sample_size': 50})

        exceptions: list[tuple[int, Exception]] = []
        completed: list[int] = []

        def worker(thread_id: int) -> None:
            try:
                parent_start = time_ns() - 2_000_000
                parent = self.kv_span(tracer, f'parent_{thread_id}', parent_start)

                encoding = tracer.request_span(OpAttributeName.EncodingSpanName.value,
                                               parent_span=parent,
                                               start_time=parent_start + 100_000)
                encoding.end(end_time=parent_start + 250_000)

                dispatch = tracer.request_span(OpAttributeName.DispatchSpanName.value,
                                               parent_span=parent,
                                               start_time=parent_start + 500_000)
                dispatch.set_attribute(DispatchAttributeName.ServerDuration.value, 300_000)
                dispatch.set_attribute(DispatchAttributeName.LocalId.value, f'local_{thread_id}')
                dispatch.set_attribute(DispatchAttributeName.OperationId.value, f'op_{thread_id}')
                dispatch.end(end_time=parent_start + 900_000)

                parent.end(end_time=parent_start + 2_000_000)
                completed.append(thread_id)
            except Exception as ex:
                exceptions.append((thread_id, ex))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert not exceptions, f'Exceptions occurred in threads: {exceptions}'
        assert sorted(completed) == list(range(20))
        assert len(reporter.records) == 20

        # Every record must carry its *own* thread's attributes.  Twenty threads writing
        # children through twenty parents concurrently is exactly where a shared or misrouted
        # accumulator would show up, and it would show up as one record wearing another
        # thread's local id rather than as a raised exception.
        for service_type, record, _ in reporter.records:
            assert service_type == ServiceType.KeyValue
            assert record['encode_duration_us'] == 150.0
            assert record['last_dispatch_duration_us'] == 400.0
            assert record['total_dispatch_duration_us'] == 400.0
            assert record['last_server_duration_us'] == 300.0
            assert record['total_server_duration_us'] == 300.0
            thread_id = record['operation_name'].removeprefix('parent_')
            assert record['last_local_id'] == f'local_{thread_id}'
            assert record['operation_id'] == f'op_{thread_id}'


class ThresholdLoggingTests(ThresholdLoggingTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(ThresholdLoggingTestSuite, self.__class__)
