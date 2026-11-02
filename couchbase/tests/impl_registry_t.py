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
from tests.test_features import TestProtocol

# What is checked here is classic's registration.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

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

    def test_classic_registers_exactly_the_declared_keys(self) -> None:
        assert set(classic_registry.IMPLS) == set(_keys())

    def test_each_classic_entry_names_a_class_its_module_defines(self) -> None:
        missing = []
        for name, target in classic_registry.IMPLS.items():
            module, _, cls = target.partition(':')
            if not module.startswith(f'{_api(name)}.protocol.classic.') or not _defines(module, cls):
                missing.append(name)
        assert missing == []

    @pytest.mark.parametrize('name', sorted(name for name in classic_registry.IMPLS if _api(name) == 'couchbase'))
    def test_a_sync_key_resolves_to_the_class_it_names(self, name: str) -> None:
        impl = resolve(_keys()[name], CouchbaseProtocol.CLASSIC)
        assert impl.__name__ == f'{name}Impl'
        assert impl.__module__.startswith('couchbase.protocol.classic.')

    def test_an_unregistered_protocol_raises(self) -> None:
        with pytest.raises(FeatureUnavailableException, match='couchbase2 protocol'):
            resolve(_registry.CLUSTER, CouchbaseProtocol.COUCHBASE2)

    def test_a_key_its_protocol_did_not_register_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delitem(classic_registry.IMPLS, _registry.CLUSTER.name)
        with pytest.raises(FeatureUnavailableException, match='Cluster'):
            resolve(_registry.CLUSTER, CouchbaseProtocol.CLASSIC)
