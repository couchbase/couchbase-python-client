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

from typing import NoReturn

import pytest

from couchbase.common.exceptions import GenericErrorContext
from couchbase.exceptions import DocumentNotFoundException, FeatureUnavailableException
from tests.couchbase_config import (CouchbaseConfig,
                                    describe_config_source,
                                    skip_unsupported_protocol)
from tests.hooks import pytest_runtest_makereport, pytest_runtest_setup
from tests.test_features import (EnvironmentFeatures,
                                 ServerContext,
                                 ServerFeatures,
                                 TestProtocol)

_MISMATCH = r'\[protocol-mismatch\]'


def _real_server(monkeypatch: pytest.MonkeyPatch, couchbase2: bool) -> None:
    monkeypatch.setenv('PYCBC_SERVER_ENABLED', 'true')
    monkeypatch.setenv('PYCBC_GOCAVES_ENABLED', 'false')
    monkeypatch.setenv('PYCBC_COUCHBASE2', 'true' if couchbase2 else 'false')
    monkeypatch.setenv('PYCBC_SERVER_VERSION', '8.0.0')


class ProtocolGatesUnitTests:

    @pytest.mark.parametrize('couchbase2, expected', [(False, TestProtocol.CLASSIC),
                                                      (True, TestProtocol.COUCHBASE2)])
    def test_the_gates_read_the_protocol_the_run_connects_with(self,
                                                               monkeypatch: pytest.MonkeyPatch,
                                                               couchbase2: bool,
                                                               expected: TestProtocol) -> None:
        _real_server(monkeypatch, couchbase2)
        assert describe_config_source().protocol is expected
        assert CouchbaseConfig.load_config().protocol is expected

    def test_the_mock_is_classic(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv('PYCBC_SERVER_ENABLED', 'false')
        monkeypatch.setenv('PYCBC_GOCAVES_ENABLED', 'true')
        monkeypatch.setenv('PYCBC_COUCHBASE2', 'true')
        assert describe_config_source().protocol is TestProtocol.CLASSIC

    def test_a_marked_test_with_no_config_fixture_skips(self,
                                                        monkeypatch: pytest.MonkeyPatch,
                                                        request: pytest.FixtureRequest) -> None:
        _real_server(monkeypatch, couchbase2=True)
        request.node.add_marker(pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2))
        with pytest.raises(pytest.skip.Exception, match=_MISMATCH):
            pytest_runtest_setup(request.node)

    def test_a_marked_test_runs_under_another_protocol(self,
                                                       monkeypatch: pytest.MonkeyPatch,
                                                       request: pytest.FixtureRequest) -> None:
        _real_server(monkeypatch, couchbase2=False)
        request.node.add_marker(pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2))
        pytest_runtest_setup(request.node)

    def test_unsupported_protocols_skip_without_an_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _real_server(monkeypatch, couchbase2=True)
        with pytest.raises(pytest.skip.Exception, match=_MISMATCH):
            skip_unsupported_protocol([TestProtocol.COUCHBASE2])


def _gate(feature: ServerFeatures, protocol: TestProtocol) -> str | None:
    return EnvironmentFeatures.supports_feature(feature, ServerContext(version=8.0, patch=0, protocol=protocol))


class ProtocolFeatureGateUnitTests:

    def test_a_feature_the_protocol_cannot_carry_skips_as_a_protocol_mismatch(self) -> None:
        reason = _gate(ServerFeatures.UserManagement, TestProtocol.COUCHBASE2)
        assert reason is not None
        assert reason.startswith('SKIP[protocol-mismatch] feature=UserManagement active=COUCHBASE2')

    def test_the_protocol_answers_ahead_of_the_basic_features(self) -> None:
        assert ServerFeatures.SpatialViews in EnvironmentFeatures.BASIC_FEATURES
        reason = _gate(ServerFeatures.SpatialViews, TestProtocol.COUCHBASE2)
        assert reason is not None
        assert reason.startswith('SKIP[protocol-mismatch]')

    def test_another_protocol_is_not_gated_by_it(self) -> None:
        assert _gate(ServerFeatures.UserManagement, TestProtocol.CLASSIC) is None


def _raise(exc: Exception) -> NoReturn:
    raise exc


def _rewritten(item: pytest.Item, exc: Exception) -> pytest.TestReport:
    """The report pytest_runtest_makereport hands back for a test that raised *exc*."""
    call = pytest.CallInfo.from_call(lambda: _raise(exc), when='call')
    wrapper = pytest_runtest_makereport(item, call)
    next(wrapper)
    try:
        wrapper.send(pytest.TestReport.from_item_and_call(item, call))
    except StopIteration as stop:
        report: pytest.TestReport = stop.value
        return report
    raise AssertionError('the report hook did not finish')


class FeatureUnavailableReportUnitTests:

    def test_under_couchbase2_it_is_reported_as_a_skip(self,
                                                       monkeypatch: pytest.MonkeyPatch,
                                                       request: pytest.FixtureRequest) -> None:
        _real_server(monkeypatch, couchbase2=True)
        report = _rewritten(request.node, FeatureUnavailableException('scan is not available here.'))
        assert report.skipped
        assert isinstance(report.longrepr, tuple)
        reason = report.longrepr[2]
        assert reason.startswith('Skipped: SKIP[feature-unavailable] active=COUCHBASE2 phase=call')
        assert 'note="scan is not available here."' in reason

    def test_under_classic_it_stays_a_failure(self,
                                              monkeypatch: pytest.MonkeyPatch,
                                              request: pytest.FixtureRequest) -> None:
        _real_server(monkeypatch, couchbase2=False)
        assert _rewritten(request.node, FeatureUnavailableException('scan is not available here.')).failed

    def test_one_the_gateway_answered_stays_a_failure(self,
                                                      monkeypatch: pytest.MonkeyPatch,
                                                      request: pytest.FixtureRequest) -> None:
        _real_server(monkeypatch, couchbase2=True)
        exc = FeatureUnavailableException('unimplemented', context=GenericErrorContext(grpc_status='UNIMPLEMENTED'))
        assert _rewritten(request.node, exc).failed

    def test_any_other_exception_stays_a_failure(self,
                                                 monkeypatch: pytest.MonkeyPatch,
                                                 request: pytest.FixtureRequest) -> None:
        _real_server(monkeypatch, couchbase2=True)
        assert _rewritten(request.node, DocumentNotFoundException('no such key')).failed
