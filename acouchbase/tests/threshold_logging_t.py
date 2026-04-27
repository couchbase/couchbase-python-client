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

import asyncio
import threading
from time import time_ns

import pytest

from couchbase.logic.observability.observability_types import (DispatchAttributeName,
                                                               OpAttributeName,
                                                               ServiceType)
from couchbase.logic.observability.threshold_logging import ThresholdLoggingTracer


class FakeReporter:
    """Lightweight fake reporter for testing without thread delays."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.records = []

    def add_log_record(self, service_type, record, total_duration):
        with self.lock:
            self.records.append((service_type, record, total_duration))

    def stop(self):
        pass

    def start(self):
        pass


class AsyncThresholdLoggingTestSuite:
    TEST_MANIFEST = [
        'test_async_threshold_span_accumulation',
        'test_async_callback_lifecycle',
        'test_concurrent_async_operations',
        'test_async_threshold_reporter_receives_records',
    ]

    @pytest.mark.asyncio
    async def test_async_threshold_span_accumulation(self):
        """Async variant of test_span_conversion_with_all_attributes.

        Verifies that the full span hierarchy (root → encoding → dispatch) produces
        a correct log record with all expected fields and values.
        """
        config = {
            'key_value_threshold': 0.5,
            'threshold_sample_size': 10,
            'threshold_emit_interval': 100_000,
        }

        tracer = ThresholdLoggingTracer(config)
        tracer._reporter.stop()

        now = time_ns()
        op_start_ns = now - 1_000_000
        op_end_ns = op_start_ns + 1_000_000  # 1ms total
        encode_start_ns = op_start_ns + 100_000
        encode_end_ns = encode_start_ns + 100_000  # 100us encode
        dispatch_start_ns = encode_end_ns + 200_000
        dispatch_end_ns = dispatch_start_ns + 400_000  # 400us dispatch

        op_span = tracer.request_span('get', start_time=op_start_ns)
        op_span.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)

        encoding_span = tracer.request_span(
            OpAttributeName.EncodingSpanName.value,
            parent_span=op_span,
            start_time=encode_start_ns
        )
        encoding_span.end(end_time=encode_end_ns)

        dispatch_span = tracer.request_span(
            OpAttributeName.DispatchSpanName.value,
            parent_span=op_span,
            start_time=dispatch_start_ns
        )
        dispatch_span.set_attribute(DispatchAttributeName.LocalId.value, 'local1')
        dispatch_span.set_attribute(DispatchAttributeName.OperationId.value, 'op1')
        dispatch_span.set_attribute(DispatchAttributeName.PeerAddress.value, '127.0.0.1')
        dispatch_span.set_attribute(DispatchAttributeName.PeerPort.value, 11210)
        dispatch_span.set_attribute(DispatchAttributeName.ServerDuration.value, 200_000)
        dispatch_span.end(end_time=dispatch_end_ns)

        op_span.end(end_time=op_end_ns)

        kv_items, _ = tracer._reporter._queues[ServiceType.KeyValue].drain()

        assert len(kv_items) == 1
        record = kv_items[0]

        assert record['operation_name'] == 'get'
        assert record['total_duration_us'] == 1000.0
        assert record['encode_duration_us'] == 100.0
        assert record['last_dispatch_duration_us'] == 400.0
        assert record['total_dispatch_duration_us'] == 400.0
        assert record['last_server_duration_us'] == 200.0
        assert record['total_server_duration_us'] == 200.0
        assert record['last_local_id'] == 'local1'
        assert record['operation_id'] == 'op1'
        assert record['last_local_socket'] == '127.0.0.1:11210'

        tracer.close()

    @pytest.mark.asyncio
    async def test_async_callback_lifecycle(self):
        """Async variant of the callback lifecycle test.

        Uses asyncio + run_in_executor to model the actual AsyncClientAdapter pattern:
        the C++ I/O callback runs on a thread-pool thread, sets span attributes, then
        calls call_soon_threadsafe to hand back to the event loop, which then ends the span.
        """
        config = {
            'key_value_threshold': 0.5,
            'threshold_sample_size': 10,
        }

        tracer = ThresholdLoggingTracer(config)
        fake_reporter = FakeReporter()
        tracer._reporter.stop()
        tracer._reporter = fake_reporter

        loop = asyncio.get_event_loop()
        N = 20

        for _ in range(N):
            now = time_ns()
            root = tracer.request_span('upsert', start_time=now - 2_000_000)
            root.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)

            callback_done = asyncio.Event()

            def io_callback(root=root):
                # Runs in thread-pool executor, simulating C++ I/O thread
                dispatch = tracer.request_span(
                    OpAttributeName.DispatchSpanName.value,
                    parent_span=root,
                    start_time=time_ns() - 500_000
                )
                dispatch.set_attribute(DispatchAttributeName.ServerDuration.value, 500)
                dispatch.set_attribute(DispatchAttributeName.PeerAddress.value, '127.0.0.1')
                dispatch.end(end_time=time_ns())
                loop.call_soon_threadsafe(callback_done.set)

            fut = loop.run_in_executor(None, io_callback)
            await callback_done.wait()
            await fut

            root.end(end_time=time_ns())

            assert root._total_server_duration_ns == 500
            assert root._span_snapshot is not None

        assert len(fake_reporter.records) == N

        tracer.close()

    @pytest.mark.asyncio
    async def test_concurrent_async_operations(self):
        """N concurrent coroutines each owning independent span hierarchies.

        Each coroutine runs the full lifecycle (root → dispatch → end) with its own spans.
        Verifies that independent concurrent operations don't corrupt each other's state
        and that exactly N log records are produced.
        """
        config = {
            'key_value_threshold': 0.5,
            'threshold_sample_size': 100,
        }

        tracer = ThresholdLoggingTracer(config)
        fake_reporter = FakeReporter()
        tracer._reporter.stop()
        tracer._reporter = fake_reporter

        loop = asyncio.get_event_loop()
        N = 50

        async def run_op(op_id: int) -> None:
            now = time_ns()
            root = tracer.request_span(f'upsert_{op_id}', start_time=now - 2_000_000)
            root.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)

            callback_done = asyncio.Event()

            def io_callback(root=root):
                dispatch = tracer.request_span(
                    OpAttributeName.DispatchSpanName.value,
                    parent_span=root,
                    start_time=time_ns() - 400_000
                )
                dispatch.set_attribute(DispatchAttributeName.ServerDuration.value, 200)
                dispatch.end(end_time=time_ns())
                loop.call_soon_threadsafe(callback_done.set)

            fut = loop.run_in_executor(None, io_callback)
            await callback_done.wait()
            await fut

            root.end(end_time=time_ns())

        await asyncio.gather(*[run_op(i) for i in range(N)])

        assert len(fake_reporter.records) == N
        for _, record, duration in fake_reporter.records:
            assert duration > 0
            assert record['total_duration_us'] > 0
            assert record['total_dispatch_duration_us'] > 0

        tracer.close()

    def test_async_threshold_reporter_receives_records(self):
        """Async counterpart of test_exceeding_sample_size.

        Verifies that the reporter's priority queue correctly retains only the top N
        records when more than N operations exceed the threshold.
        """
        config = {
            'key_value_threshold': 0.5,
            'threshold_sample_size': 3,
            'threshold_emit_interval': 100_000,
        }

        tracer = ThresholdLoggingTracer(config)
        tracer._reporter.stop()

        operations = [
            ('get', 1_000_000),     # 1ms → 1000us
            ('insert', 1_100_000),  # 1.1ms → 1100us
            ('upsert', 1_050_000),  # 1.05ms → 1050us
            ('remove', 1_200_000),  # 1.2ms → 1200us
            ('replace', 1_150_000),  # 1.15ms → 1150us
        ]

        now = time_ns()
        for op_name, duration_us in operations:
            start_ns = now - duration_us * 1000
            end_ns = start_ns + duration_us * 1000

            span = tracer.request_span(op_name, start_time=start_ns)
            span.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)
            span.end(end_time=end_ns)

        kv_items, dropped_count = tracer._reporter._queues[ServiceType.KeyValue].drain()

        assert len(kv_items) == 3
        assert dropped_count == 2

        op_names = [item['operation_name'] for item in kv_items]
        assert 'remove' in op_names    # 1200us — highest
        assert 'replace' in op_names   # 1150us — second
        assert 'insert' in op_names    # 1100us — third
        assert 'get' not in op_names
        assert 'upsert' not in op_names

        tracer.close()


class ClassicAsyncThresholdLoggingTests(AsyncThresholdLoggingTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicAsyncThresholdLoggingTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicAsyncThresholdLoggingTests) if valid_test_method(meth)]
        test_list = set(AsyncThresholdLoggingTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')
