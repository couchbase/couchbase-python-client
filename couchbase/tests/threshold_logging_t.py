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

import sys
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
        """No-op for testing."""
        pass

    def start(self):
        """No-op for testing."""
        pass


class ThresholdLoggingTestSuite:
    TEST_MANIFEST = [
        'test_enqueue_only_above_threshold',
        'test_ignore_below_threshold',
        'test_children_span_with_duration',
        'test_exceeding_sample_size',
        'test_multiple_services',
        'test_span_conversion_with_all_attributes',
        'test_high_concurrency_mixed_operations',
        'test_span_end_is_idempotent',
        'test_sequential_multi_dispatch_accumulation',
        'test_async_callback_lifecycle',
    ]

    def test_enqueue_only_above_threshold(self):
        """Spans above threshold should be recorded."""
        config = {
            'key_value_threshold': 0.5,  # 0.5ms threshold => 500us after conversion
            'threshold_sample_size': 10,
        }

        tracer = ThresholdLoggingTracer(config)
        fake_reporter = FakeReporter()
        original_reporter = tracer._reporter
        original_reporter.stop()  # we don't need the orig report to be running

        # Replace with fake reporter for deterministic testing
        tracer._reporter = fake_reporter

        # Use explicit timestamps for deterministic duration
        start_ns = time_ns() - 10_000_000  # Fixed start time
        end_ns = start_ns + 1_000_000  # Exactly 1ms later => 1000us (above 500us threshold)

        # Create a span with explicit start_time
        span = tracer.request_span('test_operation', start_time=start_ns)

        # Set service type to KV
        span.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)

        # End the span with explicit end_time
        span.end(end_time=end_ns)

        # Verify that a record was added
        assert len(fake_reporter.records) == 1
        service_type, record, duration = fake_reporter.records[0]
        assert service_type == ServiceType.KeyValue
        assert record['operation_name'] == 'test_operation'
        assert record['total_duration_us'] == 1000.0  # 1ms in microseconds

    def test_ignore_below_threshold(self):
        """Spans below threshold should not be recorded."""
        config = {
            'key_value_threshold': 0.5,  # 0.5ms threshold => 500us after conversion
            'threshold_sample_size': 10,
        }

        tracer = ThresholdLoggingTracer(config)
        fake_reporter = FakeReporter()
        original_reporter = tracer._reporter
        original_reporter.stop()  # we don't need the orig report to be running

        # Replace with fake reporter for deterministic testing
        tracer._reporter = fake_reporter

        # Use explicit timestamps for deterministic duration below threshold
        start_ns = time_ns() - 10_000_000  # Fixed start time
        end_ns = start_ns + 100_000  # Exactly 0.1ms later => 100us (below 500us threshold)

        # Create a span with explicit start_time
        span = tracer.request_span('test_operation', start_time=start_ns)

        # Set service type to KV
        span.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)

        # End the span with explicit end_time
        span.end(end_time=end_ns)

        # Verify that no record was added
        assert len(fake_reporter.records) == 0

    def test_children_span_with_duration(self):
        """Test that children spans (dispatch, encoding) with duration propagate properly."""
        config = {
            'key_value_threshold': 0.5,  # 0.5ms threshold => 500us after conversion
            'threshold_sample_size': 10,
        }

        tracer = ThresholdLoggingTracer(config)
        fake_reporter = FakeReporter()
        original_reporter = tracer._reporter
        original_reporter.stop()  # we don't need the orig report to be running

        # Replace with fake reporter
        tracer._reporter = fake_reporter

        # Use explicit timestamps for deterministic durations
        now = time_ns()
        dispatch_start_ns = now - 1_000_000  # 1ms ago
        dispatch_end_ns = dispatch_start_ns + 200_000  # 200us dispatch duration
        parent_start_ns = dispatch_start_ns  # Parent starts with dispatch
        parent_end_ns = parent_start_ns + 1_500_000  # 1.5ms total parent duration

        # Create a parent span with explicit start_time
        parent_span = tracer.request_span('parent_operation', start_time=parent_start_ns)
        parent_span.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)

        # Create a dispatch span as child with explicit start_time
        dispatch_span = tracer.request_span(
            OpAttributeName.DispatchSpanName.value,
            parent_span=parent_span,
            start_time=dispatch_start_ns
        )
        # End the dispatch span with explicit end_time - this should update parent's dispatch duration
        dispatch_span.end(end_time=dispatch_end_ns)

        # End the parent span with explicit end_time
        parent_span.end(end_time=parent_end_ns)

        # Should have recorded the parent span
        assert len(fake_reporter.records) == 1
        service_type, record, duration = fake_reporter.records[0]
        assert service_type == ServiceType.KeyValue
        # Dispatch duration should be propagated to the record
        assert 'last_dispatch_duration_us' in record
        assert record['last_dispatch_duration_us'] == 200.0  # Exactly the dispatch span's duration

    def test_span_end_is_idempotent(self):
        """Sequential double-end produces exactly one record; second call is a no-op."""
        config = {
            'key_value_threshold': 0.5,
            'threshold_sample_size': 10,
        }

        tracer = ThresholdLoggingTracer(config)
        fake_reporter = FakeReporter()
        tracer._reporter.stop()
        tracer._reporter = fake_reporter

        start_ns = time_ns() - 10_000_000
        end_ns = start_ns + 1_000_000  # 1ms => 1000us, above threshold

        span = tracer.request_span('test_operation', start_time=start_ns)
        span.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)

        span.end(end_time=end_ns)
        span.end(end_time=end_ns + 500_000)  # second call must be silently ignored

        assert len(fake_reporter.records) == 1
        _, record, _ = fake_reporter.records[0]
        assert record['total_duration_us'] == 1000.0

    def test_exceeding_sample_size(self):
        """Test that when sample size is exceeded, only top N items are kept."""
        config = {
            'key_value_threshold': 0.5,  # 0.5ms threshold => 500us after conversion
            'threshold_sample_size': 3,  # Only keep top 3
            'threshold_emit_interval': 100_000,  # Long interval to prevent auto-reporting
        }

        tracer = ThresholdLoggingTracer(config)
        # Stop the actual reporter to prevent it from logging, but we'll access its internal queues
        tracer._reporter.stop()

        # Add 5 operations above threshold with different durations
        operations = [
            ('replace', 1_000_000),  # 1ms -> 1000us
            ('insert', 1_100_000),   # 1.1ms -> 1100us
            ('upsert', 1_050_000),   # 1.05ms -> 1050us
            ('get', 1_200_000),      # 1.2ms -> 1200us
            ('remove', 1_150_000),  # 1.15ms -> 1150us
        ]

        now = time_ns()
        for op_name, duration_us in operations:
            start_ns = now - duration_us * 1000  # Convert us to ns
            end_ns = start_ns + duration_us * 1000

            span = tracer.request_span(op_name, start_time=start_ns)
            span.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)
            span.set_attribute(OpAttributeName.BucketName.value, 'default')
            span.set_attribute(OpAttributeName.ScopeName.value, '_default')
            span.set_attribute(OpAttributeName.CollectionName.value, '_default')
            span.end(end_time=end_ns)

        # Access the internal KV queue directly to drain and get records
        kv_queue = tracer._reporter._queues[ServiceType.KeyValue]
        items, dropped_count = kv_queue.drain()

        # Verify that only 3 items were kept (sample size)
        assert len(items) == 3
        assert dropped_count == 2  # 2 items were dropped

        # Verify they are sorted by duration (highest first)
        items_with_durations = [(item['operation_name'], item['total_duration_us']) for item in items]
        durations = [d for _, d in items_with_durations]
        assert durations == sorted(durations, reverse=True)

        # Verify the top 3 operations are present
        operation_names = [name for name, _ in items_with_durations]
        assert 'get' in operation_names  # Highest duration (1200us)
        assert 'remove' in operation_names  # Second highest (1150us)
        assert 'insert' in operation_names  # Third highest (1100us)
        assert 'replace' not in operation_names  # Should be dropped (1000us)
        assert 'upsert' not in operation_names  # Should be dropped (1050us)

        # Clean up reporter
        tracer.close()

    def test_multiple_services(self):
        """Test operations from multiple services (kv and query)."""
        config = {
            'key_value_threshold': 0.5,     # 0.5ms threshold => 500us
            'query_threshold': 0.5,    # 0.5ms threshold => 500us
            'threshold_sample_size': 10,
            'threshold_emit_interval': 100_000,  # Long interval to prevent auto-reporting
        }

        tracer = ThresholdLoggingTracer(config)
        # Stop the actual reporter to prevent it from logging, but we'll access its internal queues
        tracer._reporter.stop()

        now = time_ns()

        # Add KV operations
        kv_operations = [
            ('replace', 1_000_000),  # 1ms -> 1000us
            ('insert', 1_100_000),   # 1.1ms -> 1100us
        ]

        for op_name, duration_us in kv_operations:
            start_ns = now - duration_us * 1000
            end_ns = start_ns + duration_us * 1000

            span = tracer.request_span(op_name, start_time=start_ns)
            span.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)
            span.set_attribute(OpAttributeName.BucketName.value, 'default')
            span.set_attribute(OpAttributeName.ScopeName.value, '_default')
            span.set_attribute(OpAttributeName.CollectionName.value, '_default')
            span.end(end_time=end_ns)

        # Add Query operations
        query_operations = [
            ('query1', 600_000),   # 0.6ms -> 600us (above 500us threshold)
            ('query2', 1_200_000),  # 1.2ms -> 1200us
        ]

        for op_name, duration_us in query_operations:
            start_ns = now - duration_us * 1000
            end_ns = start_ns + duration_us * 1000

            span = tracer.request_span(op_name, start_time=start_ns)
            span.set_attribute(OpAttributeName.Service.value, ServiceType.Query.value)
            span.end(end_time=end_ns)

        # Access internal queues directly to get records
        kv_items, _ = tracer._reporter._queues[ServiceType.KeyValue].drain()
        query_items, _ = tracer._reporter._queues[ServiceType.Query].drain()

        # Verify KV records
        assert len(kv_items) == 2
        kv_names = [item['operation_name'] for item in kv_items]
        assert 'replace' in kv_names
        assert 'insert' in kv_names

        # Verify Query records
        assert len(query_items) == 2
        query_names = [item['operation_name'] for item in query_items]
        assert 'query1' in query_names
        assert 'query2' in query_names

        # Clean up reporter
        tracer.close()

    def test_span_conversion_with_all_attributes(self):
        """Test that spans are properly converted to threshold logging items with all attributes."""
        config = {
            'key_value_threshold': 0.5,  # 0.5ms threshold => 500us after conversion
            'threshold_sample_size': 10,
            'threshold_emit_interval': 100_000,  # Long interval to prevent auto-reporting
        }

        tracer = ThresholdLoggingTracer(config)
        # Stop the actual reporter to prevent it from logging, but we'll access its internal queues
        tracer._reporter.stop()

        # Use explicit timestamps for deterministic durations
        now = time_ns()
        op_start_ns = now - 1_000_000  # 1ms ago
        op_end_ns = op_start_ns + 1_000_000  # 1ms total duration
        encode_start_ns = op_start_ns + 100_000  # 0.1ms after op start
        encode_end_ns = encode_start_ns + 100_000  # 0.1ms encoding duration
        dispatch_start_ns = encode_end_ns + 200_000  # 0.2ms after encoding
        dispatch_end_ns = dispatch_start_ns + 400_000  # 0.4ms dispatch duration

        # Create operation span
        op_span = tracer.request_span('replace', start_time=op_start_ns)
        op_span.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)
        op_span.set_attribute(OpAttributeName.BucketName.value, 'default')
        op_span.set_attribute(OpAttributeName.ScopeName.value, '_default')
        op_span.set_attribute(OpAttributeName.CollectionName.value, '_default')

        # Create encoding span
        encoding_span = tracer.request_span(
            OpAttributeName.EncodingSpanName.value,
            parent_span=op_span,
            start_time=encode_start_ns
        )
        encoding_span.end(end_time=encode_end_ns)

        # Create dispatch span with all attributes
        dispatch_span = tracer.request_span(
            OpAttributeName.DispatchSpanName.value,
            parent_span=op_span,
            start_time=dispatch_start_ns
        )
        dispatch_span.set_attribute(DispatchAttributeName.LocalId.value, 'local1')
        dispatch_span.set_attribute(DispatchAttributeName.OperationId.value, 'op1')
        dispatch_span.set_attribute(DispatchAttributeName.PeerAddress.value, '1.2.3.4')
        dispatch_span.set_attribute(DispatchAttributeName.PeerPort.value, 11210)
        dispatch_span.set_attribute(DispatchAttributeName.ServerAddress.value, '1.2.3.5')
        dispatch_span.set_attribute(DispatchAttributeName.ServerPort.value, 11210)
        dispatch_span.set_attribute(DispatchAttributeName.ServerDuration.value, 300_000)  # 300us
        dispatch_span.end(end_time=dispatch_end_ns)

        # End operation span
        op_span.end(end_time=op_end_ns)

        # Access internal KV queue directly to get records
        kv_items, _ = tracer._reporter._queues[ServiceType.KeyValue].drain()

        # Verify record was created
        assert len(kv_items) == 1
        record = kv_items[0]

        # Verify all attributes are present
        assert record['operation_name'] == 'replace'
        assert record['total_duration_us'] == 1000.0  # 1ms in microseconds

        # Verify encoding duration attributes
        assert 'encode_duration_us' in record
        assert record['encode_duration_us'] == 100.0  # 0.1ms encoding

        # Verify dispatch duration attributes
        assert 'last_dispatch_duration_us' in record
        assert record['last_dispatch_duration_us'] == 400.0  # 0.4ms dispatch

        assert 'total_dispatch_duration_us' in record
        assert record['total_dispatch_duration_us'] == 400.0

        # Verify server duration attributes
        assert 'last_server_duration_us' in record
        assert record['last_server_duration_us'] == 300.0  # 300us from dispatch

        assert 'total_server_duration_us' in record
        assert record['total_server_duration_us'] == 300.0

        # Verify connection attributes
        assert 'last_local_id' in record
        assert record['last_local_id'] == 'local1'

        assert 'operation_id' in record
        assert record['operation_id'] == 'op1'

        assert 'last_local_socket' in record
        assert record['last_local_socket'] == '1.2.3.4:11210'

        assert 'last_remote_socket' in record
        assert record['last_remote_socket'] == '1.2.3.5:11210'

        # Clean up reporter
        tracer.close()

    def test_sequential_multi_dispatch_accumulation(self):
        """Sequential encoding/dispatch children accumulate totals correctly on the parent.

        This models the real _build_core_spans pattern where children are built
        one at a time by the C++ I/O callback thread.
        """
        config = {
            'key_value_threshold': 0.5,
            'threshold_sample_size': 10,
            'threshold_emit_interval': 100_000,
        }

        tracer = ThresholdLoggingTracer(config)
        tracer._reporter.stop()

        now = time_ns()
        parent_start_ns = now - 3_000_000
        parent_end_ns = parent_start_ns + 3_000_000

        parent = tracer.request_span('upsert', start_time=parent_start_ns)
        parent.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)

        # Encoding child: 150us
        enc = tracer.request_span(
            OpAttributeName.EncodingSpanName.value,
            parent_span=parent,
            start_time=parent_start_ns + 100_000
        )
        enc.end(end_time=parent_start_ns + 250_000)

        # Dispatch child 1: 400us
        d1 = tracer.request_span(
            OpAttributeName.DispatchSpanName.value,
            parent_span=parent,
            start_time=parent_start_ns + 500_000
        )
        d1.end(end_time=parent_start_ns + 900_000)

        # Dispatch child 2: 350us
        d2 = tracer.request_span(
            OpAttributeName.DispatchSpanName.value,
            parent_span=parent,
            start_time=parent_start_ns + 1_200_000
        )
        d2.end(end_time=parent_start_ns + 1_550_000)

        parent.end(end_time=parent_end_ns)

        assert parent._total_encode_duration_ns == 150_000
        assert parent._total_dispatch_duration_ns == 750_000
        assert parent._span_snapshot is not None
        assert parent._span_snapshot.total_dispatch_duration_ns == 750_000

        tracer.close()

    def test_async_callback_lifecycle(self):
        """Simulate the async hand-off: event loop thread → C++ I/O thread → event loop thread.

        Uses threading.Event to enforce the same exclusive-access ordering that
        AsyncClientAdapter._callback / call_soon_threadsafe enforces in production.
        Runs many iterations under maximum context-switch pressure to catch any ordering
        issues that would have been prevented by the removed lock.
        """
        config = {
            'key_value_threshold': 0.5,
            'threshold_sample_size': 200,
        }

        tracer = ThresholdLoggingTracer(config)
        fake_reporter = FakeReporter()
        tracer._reporter.stop()
        tracer._reporter = fake_reporter

        N = 200
        original_interval = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)

        try:
            for _ in range(N):
                now = time_ns()
                root = tracer.request_span('upsert', start_time=now - 2_000_000)
                root.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)

                callback_may_proceed = threading.Event()
                callback_done = threading.Event()

                def simulate_io_callback(root=root):
                    callback_may_proceed.wait()
                    # C++ I/O thread exclusively owns root here; event loop is at 'await future'
                    dispatch = tracer.request_span(
                        OpAttributeName.DispatchSpanName.value,
                        parent_span=root,
                        start_time=time_ns() - 500_000
                    )
                    dispatch.set_attribute(DispatchAttributeName.ServerDuration.value, 1_000)
                    dispatch.set_attribute(DispatchAttributeName.PeerAddress.value, '127.0.0.1')
                    dispatch.set_attribute(DispatchAttributeName.PeerPort.value, 11210)
                    dispatch.end(end_time=time_ns())
                    callback_done.set()  # equivalent to call_soon_threadsafe(ft.set_result, result)

                t = threading.Thread(target=simulate_io_callback)
                t.start()
                callback_may_proceed.set()   # fire the C++ op; event loop "suspends at await"
                callback_done.wait()          # "future resolved" — event loop resumes
                t.join()

                root.end(end_time=time_ns())  # event loop thread ends the span

                assert root._total_dispatch_duration_ns > 0
                assert root._total_server_duration_ns == 1_000
                assert root._span_snapshot is not None
        finally:
            sys.setswitchinterval(original_interval)

        assert len(fake_reporter.records) == N

        tracer.close()

    def test_high_concurrency_mixed_operations(self):
        """
        Stress test with many threads performing mixed operations concurrently.
        Tests overall thread safety under realistic concurrent load.
        """
        config = {
            'key_value_threshold': 0.5,  # 0.5ms threshold => 500us after conversion
            'threshold_sample_size': 50,  # Larger queue for stress test
        }

        tracer = ThresholdLoggingTracer(config)
        fake_reporter = FakeReporter()
        original_reporter = tracer._reporter
        original_reporter.stop()

        tracer._reporter = fake_reporter

        exceptions = []
        completed_operations = []

        def worker_thread(thread_id):
            """Each thread creates a parent span with child spans and performs various operations."""
            try:
                now = time_ns()
                parent_start = now - 2_000_000
                parent_end = parent_start + 2_000_000

                # Create parent span
                parent = tracer.request_span(f'parent_{thread_id}', start_time=parent_start)
                parent.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)

                # Create encoding span
                encode_start = parent_start + 100_000
                encode_end = encode_start + 150_000
                encoding = tracer.request_span(
                    OpAttributeName.EncodingSpanName.value,
                    parent_span=parent,
                    start_time=encode_start
                )
                encoding.end(end_time=encode_end)

                # Create dispatch span with attributes
                dispatch_start = parent_start + 500_000
                dispatch_end = dispatch_start + 400_000
                dispatch = tracer.request_span(
                    OpAttributeName.DispatchSpanName.value,
                    parent_span=parent,
                    start_time=dispatch_start
                )
                dispatch.set_attribute(DispatchAttributeName.ServerDuration.value, 300_000)
                dispatch.set_attribute(DispatchAttributeName.LocalId.value, f'local_{thread_id}')
                dispatch.set_attribute(DispatchAttributeName.OperationId.value, f'op_{thread_id}')
                dispatch.end(end_time=dispatch_end)

                # End parent
                parent.end(end_time=parent_end)

                completed_operations.append(thread_id)

            except Exception as e:
                exceptions.append((thread_id, e))

        # Create 20 worker threads
        threads = [threading.Thread(target=worker_thread, args=(i,)) for i in range(20)]

        # Start all threads
        for t in threads:
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        # Verify no exceptions occurred
        assert not exceptions, f"Exceptions occurred in threads: {exceptions}"

        # Verify all operations completed
        assert len(completed_operations) == 20, f"Only {len(completed_operations)} operations completed"

        # We should have 20 parent spans recorded (all above threshold)
        assert len(fake_reporter.records) == 20, f"Expected 20 records, got {len(fake_reporter.records)}"

        for service_type, record, duration in fake_reporter.records:
            assert service_type == ServiceType.KeyValue
            assert record['encode_duration_us'] == 150.0
            assert record['last_dispatch_duration_us'] == 400.0
            assert record['total_dispatch_duration_us'] == 400.0
            assert record['last_server_duration_us'] == 300.0
            assert record['total_server_duration_us'] == 300.0
            thread_id = record['operation_name'].removeprefix('parent_')
            assert record['last_local_id'] == f'local_{thread_id}'
            assert record['operation_id'] == f'op_{thread_id}'

        tracer.close()


class ClassicThresholdLoggingTests(ThresholdLoggingTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicThresholdLoggingTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicThresholdLoggingTests) if valid_test_method(meth)]
        test_list = set(ThresholdLoggingTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')

    def couchbase_test_environment(self, cb_base_env):
        cb_base_env.setup()
        yield cb_base_env
        cb_base_env.teardown()
