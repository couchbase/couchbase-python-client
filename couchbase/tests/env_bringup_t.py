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

from couchbase.exceptions import FeatureUnavailableException, UnAmbiguousTimeoutException
from tests import couchbase_config
from tests.couchbase_config import CouchbaseConfig
from tests.environments import CouchbaseTestEnvironmentException
from tests.environments import test_environment as sync_environment
from tests.environments.test_environment import TestEnvironment, TopologyFacts
from tests.mock_server import MockServerType
from tests.protocols import view_of
from tests.suite_fixtures import TopologySuite
from tests.test_features import ServerFeatures, TestProtocol
from tests.test_utils import RetryHelper

_TOPOLOGY = r'\[topology\]'


def _real_server(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, couchbase2: bool) -> None:
    # An absent file, so a developer's test_config.ini cannot supply connstr_params.
    monkeypatch.setattr(couchbase_config, 'CONFIG_FILE', str(tmp_path / 'absent.ini'))
    monkeypatch.setenv('PYCBC_SERVER_ENABLED', 'true')
    monkeypatch.setenv('PYCBC_GOCAVES_ENABLED', 'false')
    monkeypatch.setenv('PYCBC_COUCHBASE2', 'true' if couchbase2 else 'false')
    monkeypatch.setenv('PYCBC_HOST', 'gateway.local')
    monkeypatch.setenv('PYCBC_PORT', '443')
    monkeypatch.setenv('PYCBC_SERVER_VERSION', '8.0.0')
    monkeypatch.delenv('PYCBC_CONNSTR_PARAMS', raising=False)
    monkeypatch.delenv('PYCBC_NUM_NODES', raising=False)


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


class _GatewayCluster:
    """All a cluster view reads off a couchbase2 cluster is its impl."""

    _impl = object()


class DeclaredVersionUnitTests:

    def test_a_couchbase2_run_needs_one(self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
        _real_server(monkeypatch, tmp_path, couchbase2=True)
        monkeypatch.delenv('PYCBC_SERVER_VERSION')
        with pytest.raises(CouchbaseTestEnvironmentException, match=r'\$PYCBC_SERVER_VERSION'):
            CouchbaseConfig.load_config()

    def test_it_must_be_a_server_version(self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
        _real_server(monkeypatch, tmp_path, couchbase2=True)
        monkeypatch.setenv('PYCBC_SERVER_VERSION', '8.0')
        with pytest.raises(CouchbaseTestEnvironmentException, match=r"'8\.0' is not major\.minor\.patch"):
            CouchbaseConfig.load_config()

    def test_a_classic_run_ignores_it(self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
        _real_server(monkeypatch, tmp_path, couchbase2=False)
        monkeypatch.delenv('PYCBC_SERVER_VERSION')
        assert CouchbaseConfig.load_config().server_version is None

    def test_the_couchbase2_view_answers_with_it(self,
                                                 monkeypatch: pytest.MonkeyPatch,
                                                 tmp_path: pathlib.Path) -> None:
        _real_server(monkeypatch, tmp_path, couchbase2=True)
        monkeypatch.setenv('PYCBC_SERVER_VERSION', '7.6.2-3721')
        view = view_of(_GatewayCluster(), CouchbaseConfig.load_config())
        assert view.server_version == '7.6.2-3721'
        assert view.server_version_full == '7.6.2-3721'
        assert view.server_version_short == 7.6


class DeclaredNodeCountUnitTests:

    def test_unset_the_couchbase2_view_cannot_see_the_nodes(self,
                                                            monkeypatch: pytest.MonkeyPatch,
                                                            tmp_path: pathlib.Path) -> None:
        _real_server(monkeypatch, tmp_path, couchbase2=True)
        view = view_of(_GatewayCluster(), CouchbaseConfig.load_config())
        assert view.reports_node_count is False
        assert view.node_count is None

    def test_the_couchbase2_view_answers_with_it(self,
                                                 monkeypatch: pytest.MonkeyPatch,
                                                 tmp_path: pathlib.Path) -> None:
        _real_server(monkeypatch, tmp_path, couchbase2=True)
        monkeypatch.setenv('PYCBC_NUM_NODES', '3')
        view = view_of(_GatewayCluster(), CouchbaseConfig.load_config())
        assert view.reports_node_count is True
        assert view.node_count == 3
        assert view.reports_kv_endpoints is False

    def test_the_ini_supplies_it(self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
        _real_server(monkeypatch, tmp_path, couchbase2=True)
        ini = tmp_path / 'test_config.ini'
        ini.write_text('[realserver]\nnum_nodes = 2\n')
        monkeypatch.setattr(couchbase_config, 'CONFIG_FILE', str(ini))
        assert CouchbaseConfig.load_config().num_nodes == 2

    def test_it_must_be_at_least_one(self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
        _real_server(monkeypatch, tmp_path, couchbase2=True)
        monkeypatch.setenv('PYCBC_NUM_NODES', '0')
        with pytest.raises(CouchbaseTestEnvironmentException, match='at least 1; got 0'):
            CouchbaseConfig.load_config()

    def test_a_classic_run_ignores_it(self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
        _real_server(monkeypatch, tmp_path, couchbase2=False)
        monkeypatch.setenv('PYCBC_NUM_NODES', '3')
        assert CouchbaseConfig.load_config().num_nodes is None


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
        self.unavailable: FeatureUnavailableException | None = None

    def unavailable_manager(self, feature: ServerFeatures) -> FeatureUnavailableException | None:
        return self.unavailable if feature is ServerFeatures.BasicBucketManagement else None


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


class UnavailableBucketManagerUnitTests(TopologySuite):

    @pytest.fixture(scope='class')
    def cb_env(self) -> _FakeEnv:
        env = _FakeEnv(TopologyFacts(num_nodes=None, test_bucket_replicas=None, test_bucket_kv_endpoints=None))
        env.unavailable = FeatureUnavailableException('Bucket management is not available here.')
        return env

    def test_num_replicas_raises_what_the_manager_raised(self, request: pytest.FixtureRequest) -> None:
        with pytest.raises(FeatureUnavailableException, match='Bucket management'):
            request.getfixturevalue('num_replicas')


class _Counted:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc
        self.calls = 0

    def __call__(self) -> None:
        self.calls += 1
        raise self.exc


class FeatureUnavailableIsNotRetriedUnitTests:

    def test_try_n_times_raises_it_at_once(self) -> None:
        op = _Counted(FeatureUnavailableException('scan is not available here.'))
        with pytest.raises(FeatureUnavailableException):
            RetryHelper.try_n_times(5, 0, op)
        assert op.calls == 1

    def test_wait_until_raises_it_at_once(self) -> None:
        op = _Counted(FeatureUnavailableException('scan is not available here.'))
        with pytest.raises(FeatureUnavailableException):
            RetryHelper.wait_until(op, condition='never', timeout=5, interval=0)
        assert op.calls == 1

    def test_anything_else_is_still_retried(self) -> None:
        op = _Counted(ValueError('not yet'))
        with pytest.raises(ValueError):
            RetryHelper.try_n_times(3, 0, op)
        assert op.calls == 3
