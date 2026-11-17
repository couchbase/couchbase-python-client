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

import ast
import pathlib
from typing import Any

import pytest

import couchbase
from couchbase.common import _registry
from couchbase.common._registry import ImplKey, resolve
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.exceptions import FeatureUnavailableException
from couchbase.protocol.classic import _registry as classic_registry
from couchbase.protocol.couchbase2 import _registry as couchbase2_registry
from tests.test_features import TestProtocol

# It reads classic's registration as well as couchbase2's, so a couchbase2 run skips it and a
# classic run checks both.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

_REGISTRIES = {
    CouchbaseProtocol.CLASSIC: classic_registry.IMPLS,
    CouchbaseProtocol.COUCHBASE2: couchbase2_registry.IMPLS,
}
_REPO_ROOT = pathlib.Path(couchbase.__file__).resolve().parents[1]


def _keys() -> dict[str, ImplKey[Any]]:
    return {key.name: key for key in vars(_registry).values() if isinstance(key, ImplKey)}


def _defines(module: str, name: str) -> bool:
    # Read from source, so the acouchbase and txcouchbase entries are checked without importing
    # either tree into the sync suite.
    path = _REPO_ROOT.joinpath(*module.split('.')).with_suffix('.py')
    tree = ast.parse(path.read_text())
    return any(isinstance(node, ast.ClassDef) and node.name == name for node in tree.body)


def _api(name: str) -> str:
    if name.startswith('Async'):
        return 'acouchbase'
    if name.startswith('Tx'):
        return 'txcouchbase'
    return 'couchbase'


class ImplRegistryUnitTests:

    @pytest.mark.parametrize('connstr, expected', [
        ('couchbase://localhost', CouchbaseProtocol.CLASSIC),
        ('couchbases://localhost', CouchbaseProtocol.CLASSIC),
        ('couchbase2://localhost', CouchbaseProtocol.COUCHBASE2),
        ('localhost', CouchbaseProtocol.CLASSIC),
        ('couchbase2', CouchbaseProtocol.CLASSIC),
        ('http://localhost:8091', CouchbaseProtocol.CLASSIC),
    ])
    def test_the_protocol_is_read_from_the_scheme(self, connstr: str, expected: CouchbaseProtocol) -> None:
        assert CouchbaseProtocol.from_connstr(connstr) is expected

    def test_every_protocol_registers(self) -> None:
        assert _registry.registered_protocols() == frozenset(CouchbaseProtocol)

    @pytest.mark.parametrize('protocol', list(CouchbaseProtocol))
    def test_a_protocol_registers_exactly_the_declared_keys(self, protocol: CouchbaseProtocol) -> None:
        assert set(_REGISTRIES[protocol]) == set(_keys())

    @pytest.mark.parametrize('protocol', list(CouchbaseProtocol))
    def test_each_entry_names_a_class_its_module_defines(self, protocol: CouchbaseProtocol) -> None:
        package = protocol.name.lower()
        missing = []
        for name, target in _REGISTRIES[protocol].items():
            module, _, cls = target.partition(':')
            if not module.startswith(f'{_api(name)}.protocol.{package}.') or not _defines(module, cls):
                missing.append(name)
        assert missing == []

    @pytest.mark.parametrize('protocol', list(CouchbaseProtocol))
    def test_the_sync_key_resolves_to_the_class_it_names(self, protocol: CouchbaseProtocol) -> None:
        impl = resolve(_registry.CLUSTER, protocol)
        assert impl.__name__ == 'ClusterImpl'
        assert impl.__module__ == f'couchbase.protocol.{protocol.name.lower()}._cluster_impl'

    def test_an_unregistered_protocol_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delitem(_registry._REGISTRATIONS, CouchbaseProtocol.COUCHBASE2)
        with pytest.raises(FeatureUnavailableException, match='couchbase2 protocol'):
            resolve(_registry.CLUSTER, CouchbaseProtocol.COUCHBASE2)

    def test_a_missing_dependency_is_unavailable(self, monkeypatch: pytest.MonkeyPatch,
                                                 tmp_path: pathlib.Path) -> None:
        tmp_path.joinpath('_needs_a_dependency.py').write_text('import _a_dependency_not_installed\n')
        monkeypatch.syspath_prepend(str(tmp_path))
        monkeypatch.setitem(couchbase2_registry.IMPLS, _registry.CLUSTER.name, '_needs_a_dependency:ClusterImpl')
        with pytest.raises(FeatureUnavailableException,
                           match=r'needs _a_dependency_not_installed.*reinstall couchbase') as ex:
            resolve(_registry.CLUSTER, CouchbaseProtocol.COUCHBASE2)
        assert isinstance(ex.value.__cause__, ModuleNotFoundError)

    def test_a_missing_module_of_the_sdks_own_is_raised_as_it_is(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(couchbase2_registry.IMPLS, _registry.CLUSTER.name,
                            'couchbase.protocol.couchbase2._no_such_module:ClusterImpl')
        with pytest.raises(ModuleNotFoundError):
            resolve(_registry.CLUSTER, CouchbaseProtocol.COUCHBASE2)

    def test_a_protocol_with_no_dependencies_raises_the_missing_module(self, monkeypatch: pytest.MonkeyPatch,
                                                                       tmp_path: pathlib.Path) -> None:
        tmp_path.joinpath('_needs_a_dependency.py').write_text('import _a_dependency_not_installed\n')
        monkeypatch.syspath_prepend(str(tmp_path))
        monkeypatch.setitem(classic_registry.IMPLS, _registry.CLUSTER.name, '_needs_a_dependency:ClusterImpl')
        with pytest.raises(ModuleNotFoundError):
            resolve(_registry.CLUSTER, CouchbaseProtocol.CLASSIC)

    def test_a_key_its_protocol_did_not_register_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delitem(classic_registry.IMPLS, _registry.CLUSTER.name)
        with pytest.raises(FeatureUnavailableException, match='Cluster'):
            resolve(_registry.CLUSTER, CouchbaseProtocol.CLASSIC)
