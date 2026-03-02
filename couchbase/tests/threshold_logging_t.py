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
        'test_threaded_end_idempotency',
        'test_children_span_with_duration',
        'test_exceeding_sample_size',
        'test_multiple_services',
        'test_span_conversion_with_all_attributes',
        'test_concurrent_attribute_setting_with_parent',
        'test_concurrent_duration_propagation',
        'test_concurrent_parent_child_span_end',
        'test_high_concurrency_mixed_operations'
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

    def test_threaded_end_idempotency(self):
        """Concurrent end() calls should be safe and result in at most one record."""
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

        # Use explicit timestamps for deterministic duration above threshold
        start_ns = time_ns() - 10_000_000  # Fixed start time
        end_ns = start_ns + 1_000_000  # Exactly 1ms later => 1000us (above 500us threshold)

        # Create a span with explicit start_time
        span = tracer.request_span('test_operation', start_time=start_ns)

        # Set service type to KV
        span.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)

        # Call end() from multiple threads concurrently
        exceptions = []

        def end_span():
            try:
                span.end(end_time=end_ns)
            except Exception as e:
                exceptions.append(e)

        threads = []
        for _ in range(10):
            t = threading.Thread(target=end_span)
            threads.append(t)
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        # Verify no exceptions occurred
        assert not exceptions, f"Exceptions occurred: {exceptions}"

        # Verify that at most one record was added (due to idempotent end)
        assert len(fake_reporter.records) <= 1, "Should have at most one record due to idempotent end"

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

    def test_concurrent_attribute_setting_with_parent(self):  # noqa: C901
        """
        Multiple threads setting attributes on child spans with parent propagation.
        Tests that the fix for set_attribute race condition works correctly.
        """
        config = {
            'key_value_threshold': 0.5,  # 0.5ms threshold => 500us after conversion
            'threshold_sample_size': 10,
        }

        tracer = ThresholdLoggingTracer(config)
        fake_reporter = FakeReporter()
        original_reporter = tracer._reporter
        original_reporter.stop()

        tracer._reporter = fake_reporter

        # Create parent and child spans
        now = time_ns()
        parent_start_ns = now - 2_000_000
        parent_end_ns = parent_start_ns + 2_000_000  # 2ms total duration

        parent_span = tracer.request_span('parent_operation', start_time=parent_start_ns)
        parent_span.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)

        child_start_ns = parent_start_ns + 100_000
        child_end_ns = child_start_ns + 1_500_000  # 1.5ms child duration

        child_span = tracer.request_span('child_operation', parent_span=parent_span, start_time=child_start_ns)

        exceptions = []

        # Thread functions for concurrent attribute setting
        def set_server_duration():
            try:
                child_span.set_attribute(DispatchAttributeName.ServerDuration.value, 300_000)
            except Exception as e:
                exceptions.append(e)

        def set_local_id():
            try:
                child_span.set_attribute(DispatchAttributeName.LocalId.value, 'local123')
            except Exception as e:
                exceptions.append(e)

        def set_operation_id():
            try:
                child_span.set_attribute(DispatchAttributeName.OperationId.value, 'op456')
            except Exception as e:
                exceptions.append(e)

        def set_peer_address():
            try:
                child_span.set_attribute(DispatchAttributeName.PeerAddress.value, '192.168.1.1')
            except Exception as e:
                exceptions.append(e)

        # Create threads that set attributes concurrently
        threads = [
            threading.Thread(target=set_server_duration),
            threading.Thread(target=set_local_id),
            threading.Thread(target=set_operation_id),
            threading.Thread(target=set_peer_address),
        ]

        # Start all threads
        for t in threads:
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        # Verify no exceptions occurred
        assert not exceptions, f"Exceptions occurred: {exceptions}"

        # End child and parent spans
        child_span.end(end_time=child_end_ns)
        parent_span.end(end_time=parent_end_ns)

        # Verify that both spans completed successfully
        # Parent should have the propagated attributes
        assert parent_span.span_snapshot is not None
        assert parent_span.span_snapshot.server_duration_ns == 300_000
        assert parent_span.span_snapshot.local_id == 'local123'
        assert parent_span.span_snapshot.operation_id == 'op456'

        # Clean up
        tracer.close()

    def test_concurrent_duration_propagation(self):  # noqa: C901
        """
        Concurrent setting of encode_duration_ns and dispatch_duration_ns on child spans.
        Tests that the deadlock fix in property setters works correctly.
        """
        config = {
            'key_value_threshold': 0.5,  # 0.5ms threshold => 500us after conversion
            'threshold_sample_size': 10,
        }

        tracer = ThresholdLoggingTracer(config)
        fake_reporter = FakeReporter()
        original_reporter = tracer._reporter
        original_reporter.stop()

        tracer._reporter = fake_reporter

        # Create parent span
        now = time_ns()
        parent_start_ns = now - 3_000_000
        parent_end_ns = parent_start_ns + 3_000_000  # 3ms total duration

        parent_span = tracer.request_span('parent_operation', start_time=parent_start_ns)
        parent_span.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)

        exceptions = []

        # Create multiple child spans that will set durations concurrently
        def create_and_end_encoding_span():
            try:
                encode_start = parent_start_ns + 100_000
                encode_end = encode_start + 150_000  # 150us
                encoding_span = tracer.request_span(
                    OpAttributeName.EncodingSpanName.value,
                    parent_span=parent_span,
                    start_time=encode_start
                )
                encoding_span.end(end_time=encode_end)
            except Exception as e:
                exceptions.append(e)

        def create_and_end_dispatch_span():
            try:
                dispatch_start = parent_start_ns + 500_000
                dispatch_end = dispatch_start + 400_000  # 400us
                dispatch_span = tracer.request_span(
                    OpAttributeName.DispatchSpanName.value,
                    parent_span=parent_span,
                    start_time=dispatch_start
                )
                dispatch_span.end(end_time=dispatch_end)
            except Exception as e:
                exceptions.append(e)

        def create_and_end_second_dispatch_span():
            try:
                dispatch_start = parent_start_ns + 1_200_000
                dispatch_end = dispatch_start + 350_000  # 350us
                dispatch_span = tracer.request_span(
                    OpAttributeName.DispatchSpanName.value,
                    parent_span=parent_span,
                    start_time=dispatch_start
                )
                dispatch_span.end(end_time=dispatch_end)
            except Exception as e:
                exceptions.append(e)

        # Create threads that will trigger concurrent duration propagation
        threads = [
            threading.Thread(target=create_and_end_encoding_span),
            threading.Thread(target=create_and_end_dispatch_span),
            threading.Thread(target=create_and_end_second_dispatch_span),
        ]

        # Start all threads
        for t in threads:
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        # Verify no exceptions occurred (no deadlock)
        assert not exceptions, f"Exceptions occurred: {exceptions}"

        # End parent span
        parent_span.end(end_time=parent_end_ns)

        # Verify that durations were propagated correctly to parent
        assert parent_span.span_snapshot is not None
        # Total encode duration should include the encoding span (150us)
        assert parent_span.span_snapshot.encode_duration_ns == 150_000
        # Total dispatch duration should include both dispatch spans (400us + 350us = 750us)
        assert parent_span.span_snapshot.total_dispatch_duration_ns == 750_000

        # Clean up
        tracer.close()

    def test_concurrent_parent_child_span_end(self):  # noqa: C901
        """
        Parent and child spans ending concurrently from different threads.
        Tests that spans can be safely ended in any order without deadlock.
        """
        config = {
            'key_value_threshold': 0.5,  # 0.5ms threshold => 500us after conversion
            'threshold_sample_size': 10,
        }

        tracer = ThresholdLoggingTracer(config)
        fake_reporter = FakeReporter()
        original_reporter = tracer._reporter
        original_reporter.stop()

        tracer._reporter = fake_reporter

        # Create parent and multiple child spans
        now = time_ns()
        parent_start_ns = now - 2_000_000
        parent_end_ns = parent_start_ns + 2_000_000

        parent_span = tracer.request_span('parent_operation', start_time=parent_start_ns)
        parent_span.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)

        # Create 5 child spans
        child_spans = []
        for i in range(5):
            child_start = parent_start_ns + (i * 200_000)
            child_span = tracer.request_span(
                f'child_{i}',
                parent_span=parent_span,
                start_time=child_start
            )
            child_span.set_attribute(OpAttributeName.Service.value, ServiceType.KeyValue.value)
            child_spans.append((child_span, child_start + 100_000))  # 100us duration

        exceptions = []

        def end_parent():
            try:
                parent_span.end(end_time=parent_end_ns)
            except Exception as e:
                exceptions.append(e)

        def end_child(span, end_time):
            try:
                span.end(end_time=end_time)
            except Exception as e:
                exceptions.append(e)

        # Create threads to end parent and all children concurrently
        threads = [threading.Thread(target=end_parent)]
        for child_span, end_time in child_spans:
            threads.append(threading.Thread(target=end_child, args=(child_span, end_time)))

        # Start all threads (parent and children will end concurrently)
        for t in threads:
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        # Verify no exceptions occurred (no deadlock)
        assert not exceptions, f"Exceptions occurred: {exceptions}"

        # Verify all spans ended successfully
        assert parent_span.span_snapshot is not None
        for child_span, _ in child_spans:
            assert child_span.span_snapshot is not None

        # Clean up
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

        # Verify records were added to the reporter
        # We should have 20 parent spans recorded (all above threshold)
        assert len(fake_reporter.records) == 20, f"Expected 20 records, got {len(fake_reporter.records)}"

        # Verify all records have correct attributes from concurrent operations
        for service_type, record, duration in fake_reporter.records:
            assert service_type == ServiceType.KeyValue
            assert 'operation_name' in record
            assert 'total_duration_us' in record
            # Verify attributes were propagated correctly
            assert 'encode_duration_us' in record
            assert 'last_dispatch_duration_us' in record
            assert 'last_server_duration_us' in record
            assert 'last_local_id' in record
            assert 'operation_id' in record

        # Clean up
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
