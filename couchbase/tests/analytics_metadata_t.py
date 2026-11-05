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

import copy
from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Any,
                    cast)

import pytest

from couchbase.analytics import (AnalyticsError,
                                 AnalyticsMetaData,
                                 AnalyticsStatus,
                                 AnalyticsWarning)
from couchbase.options import UnsignedInt64
from couchbase.protocol.classic._metadata import _analytics_metadata_from_core
from tests.test_features import TestProtocol

# Each record here is built by classic's adapter from what its binding returns.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

if TYPE_CHECKING:
    from couchbase.protocol.classic._pycbc_core.binding_cpp_types import CppAnalyticsMetaData


def _core_metadata(**overrides: Any) -> CppAnalyticsMetaData:
    raw: dict[str, Any] = {
        'request_id': 'a-request-id',
        'client_context_id': 'a-context-id',
        'status': 'success',
        'signature': '{"*": "*"}',
        'warnings': [{'code': 1, 'message': 'a warning'}],
        'errors': [{'code': 24045, 'message': 'an error'}],
        'metrics': {'elapsed_time': 123456789, 'execution_time': 1500, 'result_count': 3,
                    'result_size': 42, 'error_count': 1, 'processed_objects': 7,
                    'warning_count': 1},
    }
    raw.update(overrides)
    return cast('CppAnalyticsMetaData', raw)


def _metadata(**overrides: Any) -> AnalyticsMetaData:
    record = _analytics_metadata_from_core(_core_metadata(**overrides))
    assert record is not None
    return AnalyticsMetaData(record)


class AnalyticsMetaDataUnitTests:
    # No cb_env and no TEST_MANIFEST: the metadata is built straight from the dict the core hands
    # the classic adapter, and the three packages share one definition of each.  `analytics_t`
    # reads metadata off real queries; what it cannot reach is a status the server has not
    # invented yet, or a response built twice.

    def test_accessors_read_the_record(self) -> None:
        metadata = _metadata()

        assert metadata.request_id() == 'a-request-id'
        assert metadata.client_context_id() == 'a-context-id'
        assert metadata.status() is AnalyticsStatus.SUCCESS
        assert metadata.signature() == {'*': '*'}

    def test_adapter_leaves_the_core_dict_unchanged(self) -> None:
        raw = _core_metadata()
        before = copy.deepcopy(raw)

        _analytics_metadata_from_core(raw)

        assert raw == before

    def test_one_core_dict_builds_two_records(self) -> None:
        raw = _core_metadata()

        first = _analytics_metadata_from_core(raw)
        second = _analytics_metadata_from_core(raw)

        assert first == second

    def test_absent_metadata_builds_no_record(self) -> None:
        assert _analytics_metadata_from_core(None) is None

    @pytest.mark.parametrize('status', [s.value for s in AnalyticsStatus])
    def test_status_reads_every_member_by_its_value(self, status: str) -> None:
        assert _metadata(status=status).status() is AnalyticsStatus(status)

    def test_status_the_sdk_cannot_name_reads_as_unknown(self) -> None:
        assert _metadata(status='queued').status() is AnalyticsStatus.UNKNOWN

    def test_absent_status_reads_as_unknown(self) -> None:
        raw = _core_metadata()
        del raw['status']
        record = _analytics_metadata_from_core(raw)

        assert record is not None
        assert AnalyticsMetaData(record).status() is AnalyticsStatus.UNKNOWN

    def test_absent_signature_reads_as_none(self) -> None:
        assert _metadata(signature=None).signature() is None

    def test_errors_are_analytics_errors(self) -> None:
        errors = _metadata().errors()

        assert [type(e) for e in errors] == [AnalyticsError]
        assert (errors[0].code(), errors[0].message()) == (24045, 'an error')

    def test_warnings_are_analytics_warnings(self) -> None:
        warnings = _metadata().warnings()

        assert [type(w) for w in warnings] == [AnalyticsWarning]
        assert (warnings[0].code(), warnings[0].message()) == (1, 'a warning')

    def test_absent_problem_lists_read_as_empty(self) -> None:
        metadata = _metadata(warnings=None, errors=None)

        assert metadata.warnings() == []
        assert metadata.errors() == []

    def test_problem_repr(self) -> None:
        metadata = _metadata()

        assert repr(metadata.errors()[0]) == "AnalyticsError({'code': 24045, 'message': 'an error'})"
        assert repr(metadata.warnings()[0]) == "AnalyticsWarning({'code': 1, 'message': 'a warning'})"

    def test_metadata_repr_names_the_class(self) -> None:
        assert repr(_metadata()).startswith('AnalyticsMetaData({')
        assert repr(_metadata().metrics()).startswith('AnalyticsMetrics({')

    def test_metrics_counts(self) -> None:
        metrics = _metadata().metrics()

        assert metrics is not None
        counts = [metrics.result_count(), metrics.result_size(), metrics.error_count(),
                  metrics.processed_objects(), metrics.warning_count()]
        assert all(isinstance(c, UnsignedInt64) for c in counts)
        assert [c.value for c in counts] == [3, 42, 1, 7, 1]

    # The classic core sends neither count; the record carries both for the protocol that does.
    def test_classic_leaves_mutation_and_sort_counts_unset(self) -> None:
        record = _analytics_metadata_from_core(_core_metadata())

        assert record is not None and record.metrics is not None
        assert (record.metrics.mutation_count, record.metrics.sort_count) == (None, None)

    # The core carries durations in nanoseconds and the accessor divides into microseconds, so
    # the rounding is timedelta's, half to even.  1500ns is 1.5us and reads as 2us.
    def test_metrics_durations_round_in_the_accessor(self) -> None:
        metrics = _metadata().metrics()

        assert metrics is not None
        assert metrics.elapsed_time() == timedelta(microseconds=123457)
        assert metrics.execution_time() == timedelta(microseconds=2)

    @pytest.mark.parametrize('metrics', [None, {}])
    def test_absent_metrics_read_as_none(self, metrics: Any) -> None:
        assert _metadata(metrics=metrics).metrics() is None
