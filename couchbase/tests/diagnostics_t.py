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

from datetime import timedelta

from couchbase.diagnostics import (EndpointDiagnosticsReport,
                                   EndpointPingReport,
                                   ServiceType)


class DiagnosticsReportTestSuite:

    def test_ping_report_keeps_a_zero_latency(self):
        # The core reports a latency of exactly 0 for a key-value session asked to ping
        # before it has bootstrapped, so this is a value that reaches as_dict() in practice
        # and not only in principle.
        report = EndpointPingReport(ServiceType.KeyValue,
                                    {'id': 'ep', 'latency': 0, 'remote': 'r',
                                     'local': 'l', 'state': 'ok'})
        assert report.latency == timedelta(0)
        assert report.as_dict()['latency_us'] == 0

    def test_ping_report_omits_an_absent_latency(self):
        report = EndpointPingReport(ServiceType.KeyValue,
                                    {'id': 'ep', 'remote': 'r', 'local': 'l', 'state': 'ok'})
        assert report.latency is None
        assert 'latency_us' not in report.as_dict()

    def test_diagnostics_report_keeps_a_zero_last_activity(self):
        report = EndpointDiagnosticsReport(ServiceType.KeyValue,
                                           {'id': 'ep', 'last_activity': 0, 'remote': 'r',
                                            'local': 'l', 'state': 'connected'})
        assert report.last_activity == timedelta(0)
        assert report.as_dict()['last_activity_us'] == 0

    def test_diagnostics_report_omits_an_absent_last_activity(self):
        report = EndpointDiagnosticsReport(ServiceType.KeyValue,
                                           {'id': 'ep', 'remote': 'r', 'local': 'l',
                                            'state': 'connected'})
        assert report.last_activity is None
        assert 'last_activity_us' not in report.as_dict()


class DiagnosticsReportTests(DiagnosticsReportTestSuite):
    pass
