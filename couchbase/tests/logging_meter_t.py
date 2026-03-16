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

import pytest

from couchbase.logic.observability.logging_meter import LoggingMeter
from couchbase.logic.observability.observability_types import (OpAttributeName,
                                                               OpName,
                                                               ServiceType)


class FakeReporter:
    """Lightweight fake reporter for testing without thread delays."""

    def __init__(self) -> None:
        self.stopped = False

    def stop(self):
        """Mark reporter as stopped."""
        self.stopped = True

    def start(self):
        """No-op for testing."""
        pass


class LoggingMeterTestSuite:
    TEST_MANIFEST = [
        'test_report_structure',
        'test_multiple_values_percentiles',
        'test_reset_after_report',
        'test_multiple_services',
        'test_multiple_operations_per_service',
        'test_recorder_reuse',
        'test_thread_safety',
        'test_large_value_ranges',
        'test_empty_histogram',
        'test_emit_interval_config',
        'test_concurrent_recording_and_reporting',
        'test_percentile_accuracy',
    ]

    def test_report_structure(self):
        """Report should have correct structure with meta and operations."""
        emit_interval_ms = 1000
        meter = LoggingMeter(emit_interval_ms=emit_interval_ms)
        fake_reporter = FakeReporter()
        original_reporter = meter._reporter
        original_reporter.stop()  # Stop the original reporter

        # Replace with fake reporter
        meter._reporter = fake_reporter

        # Record a single value
        recorder = meter.value_recorder(
            OpAttributeName.MeterOperationDuration,
            {
                OpAttributeName.Service.value: ServiceType.KeyValue.value,
                OpAttributeName.OperationName.value: OpName.Get.value,
            }
        )
        recorder.record_value(100)

        # Create report
        report = meter.create_report()

        # Verify report structure
        assert 'meta' in report
        assert 'emit_interval_s' in report['meta']
        assert report['meta']['emit_interval_s'] == emit_interval_ms / 1000

        assert 'operations' in report
        assert ServiceType.KeyValue.value in report['operations']
        assert OpName.Get.value in report['operations'][ServiceType.KeyValue.value]

        # Verify percentile structure
        stats = report['operations'][ServiceType.KeyValue.value][OpName.Get.value]
        assert 'total_count' in stats
        assert stats['total_count'] == 1
        assert 'percentiles_us' in stats
        assert isinstance(stats['percentiles_us'], dict)

        # Verify required percentiles are present
        assert '50.0' in stats['percentiles_us']
        assert '90.0' in stats['percentiles_us']
        assert '99.0' in stats['percentiles_us']
        assert '99.9' in stats['percentiles_us']
        assert '100.0' in stats['percentiles_us']

        # Clean up
        meter.close()

    def test_multiple_values_percentiles(self):
        """Record multiple values and verify percentiles are calculated."""
        meter = LoggingMeter()
        fake_reporter = FakeReporter()
        original_reporter = meter._reporter
        original_reporter.stop()

        meter._reporter = fake_reporter

        recorder = meter.value_recorder(
            OpAttributeName.MeterOperationDuration,
            {
                OpAttributeName.Service.value: ServiceType.KeyValue.value,
                OpAttributeName.OperationName.value: OpName.Get.value,
            }
        )

        # Record values: 100, 200, 300 microseconds
        recorder.record_value(100)
        recorder.record_value(200)
        recorder.record_value(300)

        report = meter.create_report()
        stats = report['operations'][ServiceType.KeyValue.value][OpName.Get.value]

        # Verify count
        assert stats['total_count'] == 3

        # Verify percentiles are in reasonable range
        assert stats['percentiles_us']['50.0'] >= 100
        assert stats['percentiles_us']['100.0'] >= 300

        # Clean up
        meter.close()

    def test_reset_after_report(self):
        """Histogram should reset after creating a report."""
        meter = LoggingMeter()
        fake_reporter = FakeReporter()
        original_reporter = meter._reporter
        original_reporter.stop()

        meter._reporter = fake_reporter

        recorder = meter.value_recorder(
            OpAttributeName.MeterOperationDuration,
            {
                OpAttributeName.Service.value: ServiceType.KeyValue.value,
                OpAttributeName.OperationName.value: OpName.Get.value,
            }
        )

        # Record a value
        recorder.record_value(100)

        # First report should have data
        report1 = meter.create_report()
        assert ServiceType.KeyValue.value in report1['operations']
        assert report1['operations'][ServiceType.KeyValue.value][OpName.Get.value]['total_count'] == 1

        # Second report should be empty (histograms reset)
        report2 = meter.create_report()
        assert len(report2['operations']) == 0

        # Clean up
        meter.close()

    def test_multiple_services(self):
        """Track operations from multiple services (KV, Query, etc.)."""
        meter = LoggingMeter()
        fake_reporter = FakeReporter()
        original_reporter = meter._reporter
        original_reporter.stop()

        meter._reporter = fake_reporter

        # KV recorder
        kv_recorder = meter.value_recorder(
            OpAttributeName.MeterOperationDuration,
            {
                OpAttributeName.Service.value: ServiceType.KeyValue.value,
                OpAttributeName.OperationName.value: OpName.Get.value,
            }
        )

        # Query recorder
        query_recorder = meter.value_recorder(
            OpAttributeName.MeterOperationDuration,
            {
                OpAttributeName.Service.value: ServiceType.Query.value,
                OpAttributeName.OperationName.value: OpName.Query.value,
            }
        )

        # Record values
        kv_recorder.record_value(100)
        query_recorder.record_value(5000)

        report = meter.create_report()

        # Verify both services are present
        assert ServiceType.KeyValue.value in report['operations']
        assert ServiceType.Query.value in report['operations']
        assert report['operations'][ServiceType.KeyValue.value][OpName.Get.value]['total_count'] == 1
        assert report['operations'][ServiceType.Query.value][OpName.Query.value]['total_count'] == 1

        # Clean up
        meter.close()

    def test_multiple_operations_per_service(self):
        """Track multiple operations within the same service."""
        meter = LoggingMeter()
        fake_reporter = FakeReporter()
        original_reporter = meter._reporter
        original_reporter.stop()

        meter._reporter = fake_reporter

        # Get recorder
        get_recorder = meter.value_recorder(
            OpAttributeName.MeterOperationDuration,
            {
                OpAttributeName.Service.value: ServiceType.KeyValue.value,
                OpAttributeName.OperationName.value: OpName.Get.value,
            }
        )

        # Upsert recorder
        upsert_recorder = meter.value_recorder(
            OpAttributeName.MeterOperationDuration,
            {
                OpAttributeName.Service.value: ServiceType.KeyValue.value,
                OpAttributeName.OperationName.value: OpName.Upsert.value,
            }
        )

        # Record values
        get_recorder.record_value(100)
        upsert_recorder.record_value(200)

        report = meter.create_report()

        # Verify both operations are present in KV service
        kv_operations = report['operations'][ServiceType.KeyValue.value]
        assert OpName.Get.value in kv_operations
        assert OpName.Upsert.value in kv_operations
        assert kv_operations[OpName.Get.value]['total_count'] == 1
        assert kv_operations[OpName.Upsert.value]['total_count'] == 1

        # Clean up
        meter.close()

    def test_recorder_reuse(self):
        """Same service and operation should return the same recorder."""
        meter = LoggingMeter()
        fake_reporter = FakeReporter()
        original_reporter = meter._reporter
        original_reporter.stop()

        meter._reporter = fake_reporter

        # Get recorder twice with same parameters
        recorder1 = meter.value_recorder(
            OpAttributeName.MeterOperationDuration,
            {
                OpAttributeName.Service.value: ServiceType.KeyValue.value,
                OpAttributeName.OperationName.value: OpName.Get.value,
            }
        )

        recorder2 = meter.value_recorder(
            OpAttributeName.MeterOperationDuration,
            {
                OpAttributeName.Service.value: ServiceType.KeyValue.value,
                OpAttributeName.OperationName.value: OpName.Get.value,
            }
        )

        # Record values through both recorders
        recorder1.record_value(100)
        recorder2.record_value(200)

        report = meter.create_report()

        # Should have 2 total values (both recorders write to same histogram)
        stats = report['operations'][ServiceType.KeyValue.value][OpName.Get.value]
        assert stats['total_count'] == 2

        # Clean up
        meter.close()

    def test_thread_safety(self):
        """Concurrent recording should be thread-safe."""
        meter = LoggingMeter()
        fake_reporter = FakeReporter()
        original_reporter = meter._reporter
        original_reporter.stop()

        meter._reporter = fake_reporter

        recorder = meter.value_recorder(
            OpAttributeName.MeterOperationDuration,
            {
                OpAttributeName.Service.value: ServiceType.KeyValue.value,
                OpAttributeName.OperationName.value: OpName.Get.value,
            }
        )

        exceptions = []

        def record_values():
            try:
                for i in range(100):
                    recorder.record_value(i * 10)
            except Exception as e:
                exceptions.append(e)

        # Create 10 threads each recording 100 values
        threads = []
        for _ in range(10):
            t = threading.Thread(target=record_values)
            threads.append(t)
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        # Verify no exceptions occurred
        assert not exceptions, f"Exceptions occurred: {exceptions}"

        report = meter.create_report()

        # Should have 1000 total values (10 threads × 100 values)
        stats = report['operations'][ServiceType.KeyValue.value][OpName.Get.value]
        assert stats['total_count'] == 1000

        # Clean up
        meter.close()

    def test_large_value_ranges(self):
        """Test with large value range (microseconds to seconds)."""
        meter = LoggingMeter()
        fake_reporter = FakeReporter()
        original_reporter = meter._reporter
        original_reporter.stop()

        meter._reporter = fake_reporter

        recorder = meter.value_recorder(
            OpAttributeName.MeterOperationDuration,
            {
                OpAttributeName.Service.value: ServiceType.KeyValue.value,
                OpAttributeName.OperationName.value: OpName.Get.value,
            }
        )

        # Record values from 1μs to 10 seconds (within histogram trackable range)
        # The default histogram max is 30 seconds (30,000,000 μs)
        recorder.record_value(1)           # 1 microsecond
        recorder.record_value(1000)        # 1 millisecond
        recorder.record_value(1000000)     # 1 second
        recorder.record_value(5000000)     # 5 seconds
        recorder.record_value(10000000)    # 10 seconds

        report = meter.create_report()
        stats = report['operations'][ServiceType.KeyValue.value][OpName.Get.value]

        # Verify all values were recorded
        assert stats['total_count'] == 5

        # Verify percentiles are in reasonable range
        assert stats['percentiles_us']['50.0'] > 0  # median
        assert stats['percentiles_us']['100.0'] > 0  # max

        # Clean up
        meter.close()

    def test_empty_histogram(self):
        """Querying an empty histogram should return zero count."""
        meter = LoggingMeter()
        fake_reporter = FakeReporter()
        original_reporter = meter._reporter
        original_reporter.stop()

        meter._reporter = fake_reporter

        # Create recorder but don't record any values
        meter.value_recorder(
            OpAttributeName.MeterOperationDuration,
            {
                OpAttributeName.Service.value: ServiceType.KeyValue.value,
                OpAttributeName.OperationName.value: OpName.Get.value,
            }
        )

        # Create report without recording values
        report = meter.create_report()

        # The logging meter implementation includes recorders even with zero count
        # Verify the report structure is correct and count is 0
        if ServiceType.KeyValue.value in report['operations']:
            stats = report['operations'][ServiceType.KeyValue.value][OpName.Get.value]
            assert stats['total_count'] == 0

        # Clean up
        meter.close()

    def test_emit_interval_config(self):
        """Custom emit interval should be reflected in report metadata."""
        custom_interval_ms = 5000
        meter = LoggingMeter(emit_interval_ms=custom_interval_ms)
        fake_reporter = FakeReporter()
        original_reporter = meter._reporter
        original_reporter.stop()

        meter._reporter = fake_reporter

        report = meter.create_report()

        # Verify emit interval in metadata
        assert report['meta']['emit_interval_s'] == custom_interval_ms / 1000

        # Clean up
        meter.close()

    def test_concurrent_recording_and_reporting(self):  # noqa: C901
        """Recording and reporting concurrently should not lose data or deadlock."""
        meter = LoggingMeter()
        fake_reporter = FakeReporter()
        original_reporter = meter._reporter
        original_reporter.stop()

        meter._reporter = fake_reporter

        recorder = meter.value_recorder(
            OpAttributeName.MeterOperationDuration,
            {
                OpAttributeName.Service.value: ServiceType.KeyValue.value,
                OpAttributeName.OperationName.value: OpName.Get.value,
            }
        )

        exceptions = []
        report_results = []

        def record_values():
            try:
                for i in range(50):
                    recorder.record_value(i * 10)
            except Exception as e:
                exceptions.append(e)

        def create_reports():
            try:
                for _ in range(5):
                    report = meter.create_report()
                    report_results.append(report)
            except Exception as e:
                exceptions.append(e)

        # Create threads for recording and reporting
        recording_threads = [threading.Thread(target=record_values) for _ in range(5)]
        reporting_threads = [threading.Thread(target=create_reports) for _ in range(2)]

        all_threads = recording_threads + reporting_threads

        # Start all threads
        for t in all_threads:
            t.start()

        # Wait for all threads to complete
        for t in all_threads:
            t.join()

        # Verify no exceptions occurred
        assert not exceptions, f"Exceptions occurred: {exceptions}"

        # Verify reports were created successfully
        assert len(report_results) > 0

        # Clean up
        meter.close()

    def test_percentile_accuracy(self):
        """Test percentile calculation accuracy with known distribution."""
        meter = LoggingMeter()
        fake_reporter = FakeReporter()
        original_reporter = meter._reporter
        original_reporter.stop()

        meter._reporter = fake_reporter

        recorder = meter.value_recorder(
            OpAttributeName.MeterOperationDuration,
            {
                OpAttributeName.Service.value: ServiceType.KeyValue.value,
                OpAttributeName.OperationName.value: OpName.Get.value,
            }
        )

        # Record 1000 values uniformly distributed from 1000 to 100000 microseconds
        for i in range(1, 1001):
            recorder.record_value(i * 100)

        report = meter.create_report()
        stats = report['operations'][ServiceType.KeyValue.value][OpName.Get.value]

        # Verify count
        assert stats['total_count'] == 1000

        # For uniform distribution, percentiles should be approximately:
        # P50 ~ 50,000, P90 ~ 90,000, P99 ~ 99,000
        p50 = stats['percentiles_us']['50.0']
        p90 = stats['percentiles_us']['90.0']
        p99 = stats['percentiles_us']['99.0']
        p100 = stats['percentiles_us']['100.0']

        # Verify percentiles are ordered
        assert p50 <= p90
        assert p90 <= p99
        assert p99 <= p100

        # Verify percentiles are in reasonable ranges (allowing for HDR histogram bucketing)
        assert 45000 < p50 < 55000, f"P50 {p50} out of expected range"
        assert 85000 < p90 < 95000, f"P90 {p90} out of expected range"
        assert 95000 < p99 < 105000, f"P99 {p99} out of expected range"

        # Clean up
        meter.close()


class ClassicLoggingMeterTests(LoggingMeterTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicLoggingMeterTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicLoggingMeterTests) if valid_test_method(meth)]
        test_list = set(LoggingMeterTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')

    def couchbase_test_environment(self, cb_base_env):
        cb_base_env.setup()
        yield cb_base_env
        cb_base_env.teardown()
