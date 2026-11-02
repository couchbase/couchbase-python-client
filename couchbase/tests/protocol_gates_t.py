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

import pytest

from tests.couchbase_config import (CouchbaseConfig,
                                    describe_config_source,
                                    skip_unsupported_protocol)
from tests.hooks import pytest_runtest_setup
from tests.test_features import TestProtocol

_MISMATCH = r'\[protocol-mismatch\]'


def _real_server(monkeypatch: pytest.MonkeyPatch, couchbase2: bool) -> None:
    monkeypatch.setenv('PYCBC_SERVER_ENABLED', 'true')
    monkeypatch.setenv('PYCBC_GOCAVES_ENABLED', 'false')
    monkeypatch.setenv('PYCBC_COUCHBASE2', 'true' if couchbase2 else 'false')


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
