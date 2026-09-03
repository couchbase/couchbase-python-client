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

from datetime import timedelta
from typing import Any

from couchbase.diagnostics import (EndpointDiagnosticsReport,
                                   EndpointPingReport,
                                   ServiceType)


def _ping_report(**overrides: Any) -> EndpointPingReport:
    # An override of None is not the same as an omission, and omission is what is under test,
    # so the absent cases below pass a source without the key rather than a None.
    return EndpointPingReport(ServiceType.KeyValue,
                              {'id': 'ep', 'remote': 'r', 'local': 'l', 'state': 'ok', **overrides})


def _diagnostics_report(**overrides: Any) -> EndpointDiagnosticsReport:
    return EndpointDiagnosticsReport(ServiceType.KeyValue,
                                     {'id': 'ep', 'remote': 'r', 'local': 'l',
                                      'state': 'connected', **overrides})


class DiagnosticsReportUnitTests:
    # No cb_env and no TEST_MANIFEST: both classes are constructed straight from the dict the
    # core would have handed them, and `couchbase.diagnostics` is the only definition of
    # either.  Not generated for that reason.
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
        report = _ping_report()

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

    def test_the_two_reports_read_namespace_from_different_keys(self) -> None:
        # One property name over three source keys, agreeing on none: a ping report reads
        # `namespace` and falls back to the `scope` the core used to send, a diagnostics
        # report reads `bucket` with no fallback.  The two sit three lines apart.
        assert _ping_report(namespace='inventory').namespace == 'inventory'
        assert _ping_report(scope='inventory').namespace == 'inventory'
        assert _ping_report(bucket='default').namespace is None

        assert _diagnostics_report(bucket='default').namespace == 'default'
        assert _diagnostics_report(namespace='inventory').namespace is None
