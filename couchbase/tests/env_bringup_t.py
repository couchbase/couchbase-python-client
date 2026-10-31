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

import pathlib
from typing import Any

import pytest

from couchbase.exceptions import UnAmbiguousTimeoutException
from tests import couchbase_config
from tests.couchbase_config import CouchbaseConfig
from tests.environments import CouchbaseTestEnvironmentException
from tests.environments import test_environment as sync_environment
from tests.environments.test_environment import TestEnvironment, TopologyFacts
from tests.mock_server import MockServerType
from tests.suite_fixtures import TopologySuite
from tests.test_features import TestProtocol

_TOPOLOGY = r'\[topology\]'


def _real_server(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, couchbase2: bool) -> None:
    # An absent file, so a developer's test_config.ini cannot supply connstr_params.
    monkeypatch.setattr(couchbase_config, 'CONFIG_FILE', str(tmp_path / 'absent.ini'))
    monkeypatch.setenv('PYCBC_SERVER_ENABLED', 'true')
    monkeypatch.setenv('PYCBC_GOCAVES_ENABLED', 'false')
    monkeypatch.setenv('PYCBC_COUCHBASE2', 'true' if couchbase2 else 'false')
    monkeypatch.setenv('PYCBC_HOST', 'gateway.local')
    monkeypatch.setenv('PYCBC_PORT', '443')
    monkeypatch.delenv('PYCBC_CONNSTR_PARAMS', raising=False)


class ConnectionStringUnitTests:

    @pytest.mark.parametrize('couchbase2, expected', [
        (False, 'couchbase://gateway.local?tls_verify=none'),
        (True, 'couchbase2://gateway.local:443?tls_verify=none'),
    ])
    def test_params_follow_either_scheme(self,
                                         monkeypatch: pytest.MonkeyPatch,
                                         tmp_path: pathlib.Path,
                                         couchbase2: bool,
                                         expected: str) -> None:
        _real_server(monkeypatch, tmp_path, couchbase2)
        monkeypatch.setenv('PYCBC_CONNSTR_PARAMS', 'tls_verify=none')
        config = CouchbaseConfig.load_config()
        assert config.get_connection_string() == expected
        assert config.protocol is (TestProtocol.COUCHBASE2 if couchbase2 else TestProtocol.CLASSIC)

    def test_a_leading_question_mark_is_not_doubled(self,
                                                    monkeypatch: pytest.MonkeyPatch,
                                                    tmp_path: pathlib.Path) -> None:
        _real_server(monkeypatch, tmp_path, couchbase2=True)
        monkeypatch.setenv('PYCBC_CONNSTR_PARAMS', '?tls_verify=none')
        assert CouchbaseConfig.load_config().get_connection_string() == 'couchbase2://gateway.local:443?tls_verify=none'

    def test_no_params_means_no_query_string(self,
                                             monkeypatch: pytest.MonkeyPatch,
                                             tmp_path: pathlib.Path) -> None:
        _real_server(monkeypatch, tmp_path, couchbase2=True)
        assert CouchbaseConfig.load_config().get_connection_string() == 'couchbase2://gateway.local:443'

    def test_the_ini_supplies_params(self,
                                     monkeypatch: pytest.MonkeyPatch,
                                     tmp_path: pathlib.Path) -> None:
        _real_server(monkeypatch, tmp_path, couchbase2=True)
        ini = tmp_path / 'test_config.ini'
        ini.write_text('[realserver]\nconnstr_params = tls_verify=none&timeout=5s\n')
        monkeypatch.setattr(couchbase_config, 'CONFIG_FILE', str(ini))
        assert (CouchbaseConfig.load_config().get_connection_string()
                == 'couchbase2://gateway.local:443?tls_verify=none&timeout=5s')


class _RaisingCluster:
    def __init__(self, error: Exception) -> None:
        self.error = error
        self.calls = 0

    def connect(self, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1
        raise self.error


class ConnectLoopUnitTests:

    def test_a_timeout_is_retried_then_reported(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _RaisingCluster(UnAmbiguousTimeoutException('no answer'))
        monkeypatch.setattr(sync_environment, 'Cluster', fake)
        with pytest.raises(CouchbaseTestEnvironmentException, match=r'stage=connect .*attempts=3 .*UnAmbiguousTimeout'):
            TestEnvironment.get_environment(couchbase_config=CouchbaseConfig())
        assert fake.calls == 3

    def test_anything_else_is_reported_on_the_first_attempt(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _RaisingCluster(ValueError('bad connection string'))
        monkeypatch.setattr(sync_environment, 'Cluster', fake)
        with pytest.raises(CouchbaseTestEnvironmentException,
                           match=r'stage=connect .*attempts=1 .*ValueError: bad connection string.*not retried'):
            TestEnvironment.get_environment(couchbase_config=CouchbaseConfig())
        assert fake.calls == 1


class _FakeEnv:
    """The members TopologySuite's fixtures read, with a protocol that sees no topology."""

    def __init__(self, facts: TopologyFacts) -> None:
        self.topology = facts
        self.protocol = TestProtocol.COUCHBASE2
        self.is_mock_server = False
        self.mock_server_type: MockServerType | None = None


class UnknownNodesUnitTests(TopologySuite):

    @pytest.fixture(scope='class')
    def cb_env(self) -> _FakeEnv:
        return _FakeEnv(TopologyFacts(num_nodes=None, test_bucket_replicas=1, test_bucket_kv_endpoints=None))

    @pytest.mark.parametrize('fixture', ['check_multi_node', 'check_single_node', 'check_replicas_online'])
    def test_unknown_nodes_skip(self, request: pytest.FixtureRequest, fixture: str) -> None:
        with pytest.raises(pytest.skip.Exception, match=_TOPOLOGY + r'.*nodes=unknown'):
            request.getfixturevalue(fixture)


class UnknownEndpointsUnitTests(TopologySuite):

    @pytest.fixture(scope='class')
    def cb_env(self) -> _FakeEnv:
        return _FakeEnv(TopologyFacts(num_nodes=3, test_bucket_replicas=1, test_bucket_kv_endpoints=None))

    def test_unknown_kv_endpoints_skip(self, request: pytest.FixtureRequest) -> None:
        with pytest.raises(pytest.skip.Exception, match=_TOPOLOGY + r'.*kv_endpoints=unknown'):
            request.getfixturevalue('check_replicas_online')
