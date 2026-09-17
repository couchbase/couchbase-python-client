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

import threading
from collections.abc import Callable, Generator
from typing import (ClassVar,
                    Optional,
                    cast)

import pytest

from couchbase.logic.observability.logging_meter import LoggingMeter, LoggingMeterReporter
from couchbase.logic.observability.observability_types import (OpAttributeName,
                                                               OpName,
                                                               ServiceType)
from tests_v2.manifest import validate_test_manifest
from tests_v2.observability_test_impls import InertReporter


def kv_tags(op_name: OpName, service: ServiceType = ServiceType.KeyValue) -> dict[str, str]:
    # `value_recorder` reads exactly these two keys and ignores the rest, so building the
    # mapping in one place keeps the tests about what they record rather than how it is spelled.
    return {
        OpAttributeName.Service.value: service.value,
        OpAttributeName.OperationName.value: op_name.value,
    }


def run_concurrently(*workers: tuple[Callable[[], None], int]) -> list[Exception]:
    # An exception inside a `threading.Thread` is printed and dropped: the thread dies, the
    # test keeps going, and what fails is a count mismatch rather than the error that caused it.
    # Collecting them here is what makes the failure name itself.
    exceptions: list[Exception] = []

    def guarded(work: Callable[[], None]) -> Callable[[], None]:
        def run() -> None:
            try:
                work()
            except Exception as ex:  # noqa: B902 - re-raised by the caller's assertion
                exceptions.append(ex)
        return run

    threads = [threading.Thread(target=guarded(work))
               for work, count in workers for _ in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return exceptions


class LoggingMeterTestSuite:
    # No cb_env: `LoggingMeter` is reached through the same `couchbase/logic/observability/`
    # module from all three apis and nothing here calls an operation on a collection, so a
    # generated copy would assert the same thing about the same object three times.

    TEST_MANIFEST: ClassVar[list[str]] = [
        'test_concurrent_recording_and_reporting',
        'test_emit_interval_config',
        'test_empty_histogram',
        'test_large_value_ranges',
        'test_multiple_operations_per_service',
        'test_multiple_services',
        'test_multiple_values_percentiles',
        'test_percentile_accuracy',
        'test_recorder_reuse',
        'test_report_structure',
        'test_reset_after_report',
        'test_thread_safety',
    ]

    @pytest.fixture(name='make_meter')
    def make_meter_fixture(self) -> Generator[Callable[..., LoggingMeter], None, None]:
        # `LoggingMeter.__init__` starts a reporter thread that wakes every emit interval and
        # logs a report; tests call `create_report()` themselves, so it is stopped and replaced
        # before the meter is handed over.  The stop has to happen through the original object:
        # `close()` only stops whatever `_reporter` currently points at, so swapping first would
        # leave the real thread running with no reference left to stop it.
        #
        # The fixture also owns the close, because a close written as the last statement of a
        # test body is skipped by the first failing assertion above it, leaving a reporter
        # thread behind in exactly the runs where something has already gone wrong.
        meters: list[LoggingMeter] = []

        def _make(emit_interval_ms: Optional[int] = None) -> LoggingMeter:
            meter = LoggingMeter(emit_interval_ms=emit_interval_ms)
            meter._reporter.stop()
            # `InertReporter` stands in for the reporter thread; cast rather than suppress, so
            # any *other* error on this line stays visible.
            meter._reporter = cast(LoggingMeterReporter, InertReporter())
            meters.append(meter)
            return meter

        yield _make

        for meter in meters:
            meter.close()

    def test_report_structure(self, make_meter: Callable[..., LoggingMeter]) -> None:
        emit_interval_ms = 1000
        meter = make_meter(emit_interval_ms=emit_interval_ms)

        recorder = meter.value_recorder(OpAttributeName.MeterOperationDuration.value, kv_tags(OpName.Get))
        recorder.record_value(100)

        report = meter.create_report()

        assert 'meta' in report
        assert 'emit_interval_s' in report['meta']
        assert report['meta']['emit_interval_s'] == emit_interval_ms / 1000

        assert 'operations' in report
        assert ServiceType.KeyValue.value in report['operations']
        assert OpName.Get.value in report['operations'][ServiceType.KeyValue.value]

        stats = report['operations'][ServiceType.KeyValue.value][OpName.Get.value]
        assert stats['total_count'] == 1
        assert isinstance(stats['percentiles_us'], dict)
        for percentile in ('50.0', '90.0', '99.0', '99.9', '100.0'):
            assert percentile in stats['percentiles_us']

    def test_multiple_values_percentiles(self, make_meter: Callable[..., LoggingMeter]) -> None:
        meter = make_meter()

        recorder = meter.value_recorder(OpAttributeName.MeterOperationDuration.value, kv_tags(OpName.Get))
        recorder.record_value(100)
        recorder.record_value(200)
        recorder.record_value(300)

        stats = meter.create_report()['operations'][ServiceType.KeyValue.value][OpName.Get.value]
        assert stats['total_count'] == 3
        assert stats['percentiles_us']['50.0'] >= 100
        assert stats['percentiles_us']['100.0'] >= 300

    def test_reset_after_report(self, make_meter: Callable[..., LoggingMeter]) -> None:
        meter = make_meter()

        recorder = meter.value_recorder(OpAttributeName.MeterOperationDuration.value, kv_tags(OpName.Get))
        recorder.record_value(100)

        first = meter.create_report()
        assert first['operations'][ServiceType.KeyValue.value][OpName.Get.value]['total_count'] == 1

        # Reporting resets the histogram, so the second report has nothing to carry.
        assert meter.create_report()['operations'] == {}

        # The recorder itself survives the report.  A report that took it out of the map would
        # strand every handle already issued, and they would record into nothing for the life of
        # the meter; the only symptom is a report that stays empty.
        recorder.record_value(200)
        after = meter.create_report()
        assert after['operations'][ServiceType.KeyValue.value][OpName.Get.value]['total_count'] == 1

    def test_multiple_services(self, make_meter: Callable[..., LoggingMeter]) -> None:
        meter = make_meter()

        kv_recorder = meter.value_recorder(OpAttributeName.MeterOperationDuration.value, kv_tags(OpName.Get))
        query_recorder = meter.value_recorder(OpAttributeName.MeterOperationDuration.value,
                                              kv_tags(OpName.Query, service=ServiceType.Query))

        kv_recorder.record_value(100)
        query_recorder.record_value(5000)

        operations = meter.create_report()['operations']
        assert operations[ServiceType.KeyValue.value][OpName.Get.value]['total_count'] == 1
        assert operations[ServiceType.Query.value][OpName.Query.value]['total_count'] == 1

    def test_multiple_operations_per_service(self, make_meter: Callable[..., LoggingMeter]) -> None:
        meter = make_meter()

        get_recorder = meter.value_recorder(OpAttributeName.MeterOperationDuration.value, kv_tags(OpName.Get))
        upsert_recorder = meter.value_recorder(OpAttributeName.MeterOperationDuration.value, kv_tags(OpName.Upsert))

        get_recorder.record_value(100)
        upsert_recorder.record_value(200)

        kv_operations = meter.create_report()['operations'][ServiceType.KeyValue.value]
        assert kv_operations[OpName.Get.value]['total_count'] == 1
        assert kv_operations[OpName.Upsert.value]['total_count'] == 1

    def test_recorder_reuse(self, make_meter: Callable[..., LoggingMeter]) -> None:
        meter = make_meter()

        first = meter.value_recorder(OpAttributeName.MeterOperationDuration.value, kv_tags(OpName.Get))
        second = meter.value_recorder(OpAttributeName.MeterOperationDuration.value, kv_tags(OpName.Get))

        # Identity is the actual contract: value_recorder caches by (service, operation), and
        # two histograms for one operation would split the percentiles rather than merge them.
        # The count below is what a caller notices when that goes wrong.
        assert first is second

        first.record_value(100)
        second.record_value(200)

        stats = meter.create_report()['operations'][ServiceType.KeyValue.value][OpName.Get.value]
        assert stats['total_count'] == 2

    def test_thread_safety(self, make_meter: Callable[..., LoggingMeter]) -> None:
        meter = make_meter()
        recorder = meter.value_recorder(OpAttributeName.MeterOperationDuration.value, kv_tags(OpName.Get))

        def record_values() -> None:
            for i in range(100):
                recorder.record_value(i * 10)

        exceptions = run_concurrently((record_values, 10))
        assert not exceptions, f'Exceptions occurred: {exceptions}'

        stats = meter.create_report()['operations'][ServiceType.KeyValue.value][OpName.Get.value]
        assert stats['total_count'] == 1000

    def test_large_value_ranges(self, make_meter: Callable[..., LoggingMeter]) -> None:
        meter = make_meter()
        recorder = meter.value_recorder(OpAttributeName.MeterOperationDuration.value, kv_tags(OpName.Get))

        # 1us through 10s, all inside the histogram's 30s trackable maximum.  A value above
        # that maximum is dropped rather than clamped, so the count is what proves the whole
        # span was accepted.
        for value_us in (1, 1_000, 1_000_000, 5_000_000, 10_000_000):
            recorder.record_value(value_us)

        stats = meter.create_report()['operations'][ServiceType.KeyValue.value][OpName.Get.value]
        assert stats['total_count'] == 5
        assert stats['percentiles_us']['50.0'] > 0
        assert stats['percentiles_us']['100.0'] > 0

    def test_empty_histogram(self, make_meter: Callable[..., LoggingMeter]) -> None:
        meter = make_meter()

        # Created and never recorded to.
        meter.value_recorder(OpAttributeName.MeterOperationDuration.value, kv_tags(OpName.Get))

        # An idle recorder is left out of the report rather than reported with a zero count:
        # get_percentiles_and_reset() hands back a populated mapping either way, so create_report
        # reads `total_count` to tell the two apart, and a service whose operations are all idle
        # loses its own key along with them.
        assert meter.create_report()['operations'] == {}

    def test_emit_interval_config(self, make_meter: Callable[..., LoggingMeter]) -> None:
        custom_interval_ms = 5000
        meter = make_meter(emit_interval_ms=custom_interval_ms)

        assert meter.create_report()['meta']['emit_interval_s'] == custom_interval_ms / 1000

    def test_concurrent_recording_and_reporting(self, make_meter: Callable[..., LoggingMeter]) -> None:
        meter = make_meter()
        recorder = meter.value_recorder(OpAttributeName.MeterOperationDuration.value, kv_tags(OpName.Get))

        reports = []

        def record_values() -> None:
            for i in range(50):
                recorder.record_value(i * 10)

        def create_reports() -> None:
            for _ in range(5):
                reports.append(meter.create_report())

        exceptions = run_concurrently((record_values, 5), (create_reports, 2))

        assert not exceptions, f'Exceptions occurred: {exceptions}'
        assert len(reports) == 10

    def test_percentile_accuracy(self, make_meter: Callable[..., LoggingMeter]) -> None:
        meter = make_meter()
        recorder = meter.value_recorder(OpAttributeName.MeterOperationDuration.value, kv_tags(OpName.Get))

        # Uniform over 100us..100_000us, so the expected percentile is the value itself.
        for i in range(1, 1001):
            recorder.record_value(i * 100)

        stats = meter.create_report()['operations'][ServiceType.KeyValue.value][OpName.Get.value]
        assert stats['total_count'] == 1000

        p50 = stats['percentiles_us']['50.0']
        p90 = stats['percentiles_us']['90.0']
        p99 = stats['percentiles_us']['99.0']
        p100 = stats['percentiles_us']['100.0']

        assert p50 <= p90 <= p99 <= p100

        # The windows are wide because an HDR histogram reports the top of the bucket a value
        # landed in, not the value.
        assert 45_000 < p50 < 55_000, f'P50 {p50} out of expected range'
        assert 85_000 < p90 < 95_000, f'P90 {p90} out of expected range'
        assert 95_000 < p99 < 105_000, f'P99 {p99} out of expected range'


class LoggingMeterTests(LoggingMeterTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(LoggingMeterTestSuite, self.__class__)
