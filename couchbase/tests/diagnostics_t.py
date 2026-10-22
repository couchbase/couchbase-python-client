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

from dataclasses import replace
from datetime import timedelta
from typing import Any, cast

import pytest

from couchbase.diagnostics import EndpointDiagnosticsReport, EndpointPingReport
from couchbase.protocol.classic._pycbc_core.binding_cpp_types import CppEndpointDiagInfo, CppEndpointPingInfo
from couchbase.protocol.classic._result import _endpoint_diagnostics_from_core, _endpoint_ping_from_core
from tests.test_features import TestProtocol

# Each record here is built by classic's adapter from what its binding returns.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)


def _ping_report(**overrides: Any) -> EndpointPingReport:
    core = {'type': 'key_value', 'id': 'ep', 'latency': 120, 'remote': 'r', 'local': 'l', 'state': 'ok',
            'bucket': None, 'error': None, **overrides}
    return EndpointPingReport(_endpoint_ping_from_core(cast(CppEndpointPingInfo, core)))


def _diagnostics_report(**overrides: Any) -> EndpointDiagnosticsReport:
    core = {'type': 'key_value', 'id': 'ep', 'last_activity': None, 'remote': 'r', 'local': 'l',
            'state': 'connected', 'bucket': None, 'details': None, **overrides}
    return EndpointDiagnosticsReport(_endpoint_diagnostics_from_core(cast(CppEndpointDiagInfo, core)))


class DiagnosticsReportUnitTests:
    # No cb_env and no TEST_MANIFEST: each report is built from the dict the core would have
    # handed the classic adapter.  Not generated for that reason.
    #
    # Both classes distinguish an absent optional from a present zero, and a live ping produces
    # neither edge, so `bucket_t` and `cluster_t` cannot reach them.  The core sends a latency
    # of exactly 0 for a key-value session asked to ping before it has bootstrapped, which is
    # how PYCBC-1924 surfaced: an intermittent KeyError in one of those runs.

    def test_ping_report_keeps_a_zero_latency(self) -> None:
        # PYCBC-1924: `as_dict()` tested the latency for truth rather than presence, so the
        # JSON dropped a latency of 0 that the `latency` property still answered.
        report = _ping_report(latency=0)

        assert report.latency == timedelta(0)
        assert report.as_dict()['latency_us'] == 0

    def test_ping_report_omits_an_absent_latency(self) -> None:
        # The core always sends a latency, so its absence is a record a protocol could build
        # rather than a dict the core could send.
        report = EndpointPingReport(replace(_ping_report()._record, latency_us=None))

        assert report.latency is None
        assert 'latency_us' not in report.as_dict()

    def test_diagnostics_report_keeps_a_zero_last_activity(self) -> None:
        # PYCBC-1924 again: the same defect in the sibling class, fixed at the same time.
        report = _diagnostics_report(last_activity=0)

        assert report.last_activity == timedelta(0)
        assert report.as_dict()['last_activity_us'] == 0

    def test_diagnostics_report_omits_an_absent_last_activity(self) -> None:
        report = _diagnostics_report()

        assert report.last_activity is None
        assert 'last_activity_us' not in report.as_dict()

    def test_last_activity_us_is_a_timedelta_where_the_dict_key_is_the_raw_count(self) -> None:
        report = _diagnostics_report(last_activity=1500)

        assert report.last_activity_us == timedelta(microseconds=1500)
        assert report.as_dict()['last_activity_us'] == 1500

    def test_both_reports_read_namespace_from_the_bucket(self) -> None:
        # The core names an endpoint's bucket `bucket` in both reports.
        assert _ping_report(bucket='default').namespace == 'default'
        assert _diagnostics_report(bucket='default').namespace == 'default'
        assert _ping_report().namespace is None
        assert _diagnostics_report().namespace is None

    def test_as_dict_keeps_the_core_order_with_the_duration_last(self) -> None:
        ping = _ping_report(bucket='default', error='refused')
        diag = _diagnostics_report(last_activity=9, bucket='default', details='d')

        assert list(ping.as_dict()) == ['type', 'id', 'remote', 'local', 'state', 'bucket', 'error', 'latency_us']
        assert list(diag.as_dict()) == ['type', 'id', 'remote', 'local', 'state', 'bucket', 'details',
                                        'last_activity_us']
