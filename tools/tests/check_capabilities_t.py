import importlib
import inspect
import pathlib
import textwrap
import warnings
from typing import Any

import pytest

from couchbase.common._capabilities import Capability
from couchbase.exceptions import FeatureUnavailableException
from tools.check_api_parity import FACADES
from tools.check_capabilities import (ROOT,
                                      Site,
                                      check,
                                      main,
                                      role)

_FACADES = (('couchbase.collection.Collection', 'acouchbase.collection.AsyncCollection',
             'txcouchbase.collection.Collection'),)

_VOCABULARY = '''
from enum import Enum
from typing import Literal, overload


class Capability(Enum):
    THING = 'thing'


@overload
def require(impl: CollectionInterface, capability: Literal[Capability.THING]) -> SupportsThing: ...


def require(impl, capability):
    return impl
'''

_INTERFACES = '''
from typing import Protocol


class CollectionInterface(Protocol):
    @property
    def capabilities(self) -> frozenset[Capability]: ...


class SupportsThing(Protocol):
    def thing(self) -> None: ...
'''

_DECLARATION = '''
from typing import TYPE_CHECKING

CAPABILITIES = frozenset({
    Capability.THING,
})

if TYPE_CHECKING:
    _collection: CollectionImpl
    _thing: SupportsThing = _collection
'''

_FACADE = '''
class Collection:
    def thing(self) -> None:
        return require(self._impl, Capability.THING).thing()
'''


def _write(root: pathlib.Path, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text))


@pytest.fixture
def tree(tmp_path: pathlib.Path) -> pathlib.Path:
    _write(tmp_path, 'couchbase/common/_capabilities.py', _VOCABULARY)
    _write(tmp_path, 'couchbase/common/_interfaces/_collection.py', _INTERFACES)
    _write(tmp_path, 'couchbase/protocol/classic/__init__.py', '')
    _write(tmp_path, 'couchbase/protocol/classic/_capabilities.py', _DECLARATION)
    _write(tmp_path, 'couchbase/collection.py', _FACADE)
    _write(tmp_path, 'acouchbase/collection.py', 'class AsyncCollection:\n    pass\n')
    return tmp_path


def _edit(root: pathlib.Path, relative: str, old: str, new: str) -> None:
    path = root / relative
    text = path.read_text()
    assert old in text
    path.write_text(text.replace(old, new))


def test_a_wired_tree_is_clean(tree: pathlib.Path) -> None:
    sites, problems = check(tree, _FACADES)
    assert problems == []
    assert [(s.facade, s.method, s.capability) for s in sites] == [
        ('couchbase.collection.Collection', 'thing', 'THING')]


def test_role_drops_interface_and_a_leading_async() -> None:
    assert role('CollectionInterface') == 'Collection'
    assert role('AsyncAttemptContextInterface') == 'AttemptContext'
    assert role('AsynchronousInterface') == 'Asynchronous'


def test_reaching_a_capability_member_directly_is_a_finding(tree: pathlib.Path) -> None:
    _write(tree, 'acouchbase/collection.py', '''
        class AsyncCollection:
            async def thing(self) -> None:
                return await self._impl.thing()
        ''')
    _, problems = check(tree, _FACADES)
    assert len(problems) == 1
    assert "AsyncCollection.thing reaches capability member 'thing' as self._impl.thing" in problems[0]


def test_a_core_member_of_the_same_name_on_another_role_is_not_a_finding(tree: pathlib.Path) -> None:
    facades = (*_FACADES, ('couchbase.cluster.Cluster', 'acouchbase.cluster.AsyncCluster',
                           'txcouchbase.cluster.Cluster'))
    _write(tree, 'couchbase/cluster.py', 'class Cluster:\n    def thing(self):\n        return self._impl.thing()\n')
    assert check(tree, facades)[1] == []


def test_a_capability_without_an_overload_is_a_finding(tree: pathlib.Path) -> None:
    _edit(tree, 'couchbase/common/_capabilities.py', "    THING = 'thing'\n",
          "    THING = 'thing'\n    OTHER = 'other'\n")
    _, problems = check(tree, _FACADES)
    assert any('Capability.OTHER has no require() overload' in p for p in problems)
    assert any('Capability.OTHER is required by no facade' in p for p in problems)


def test_an_overload_naming_a_missing_interface_is_a_finding(tree: pathlib.Path) -> None:
    _edit(tree, 'couchbase/common/_capabilities.py', '-> SupportsThing', '-> SupportsNothing')
    _, problems = check(tree, _FACADES)
    assert any('SupportsNothing is not a Protocol' in p for p in problems)


def test_a_subject_without_capabilities_is_a_finding(tree: pathlib.Path) -> None:
    _edit(tree, 'couchbase/common/_interfaces/_collection.py', 'def capabilities', 'def name')
    _, problems = check(tree, _FACADES)
    assert any('does not declare the capabilities require() reads' in p for p in problems)


def test_a_subject_naming_no_facade_is_a_finding(tree: pathlib.Path) -> None:
    _, problems = check(tree, ())
    assert any("names the role 'Collection', which is not a facade" in p for p in problems)


def test_a_member_both_core_and_capability_is_a_finding(tree: pathlib.Path) -> None:
    capabilities = '    def capabilities(self) -> frozenset[Capability]: ...\n'
    _edit(tree, 'couchbase/common/_interfaces/_collection.py', capabilities,
          capabilities + '\n    def thing(self) -> None: ...\n')
    _, problems = check(tree, _FACADES)
    assert any('thing is both core and capability' in p for p in problems)


def test_requiring_another_roles_capability_is_a_finding(tree: pathlib.Path) -> None:
    facades = (*_FACADES, ('couchbase.cluster.Cluster', 'acouchbase.cluster.AsyncCluster',
                           'txcouchbase.cluster.Cluster'))
    _write(tree, 'couchbase/cluster.py', _FACADE.replace('class Collection', 'class Cluster'))
    _, problems = check(tree, facades)
    assert any('Cluster.thing requires Capability.THING, which is not a capability of the Cluster role' in p
               for p in problems)


def test_a_declared_capability_mypy_does_not_check_is_a_finding(tree: pathlib.Path) -> None:
    _edit(tree, 'couchbase/protocol/classic/_capabilities.py', '    _thing: SupportsThing = _collection\n', '')
    _, problems = check(tree, _FACADES)
    assert any('assigns no impl to SupportsThing under TYPE_CHECKING' in p for p in problems)


def test_an_undeclared_capability_needs_no_assignment(tree: pathlib.Path) -> None:
    _edit(tree, 'couchbase/protocol/classic/_capabilities.py', '    Capability.THING,\n', '')
    _edit(tree, 'couchbase/protocol/classic/_capabilities.py', '    _thing: SupportsThing = _collection\n', '')
    assert check(tree, _FACADES)[1] == []


def test_declaring_an_unknown_capability_is_a_finding(tree: pathlib.Path) -> None:
    _edit(tree, 'couchbase/protocol/classic/_capabilities.py',
          'Capability.THING,', 'Capability.THING, Capability.NOPE,')
    _, problems = check(tree, _FACADES)
    assert any('declares Capability.NOPE, which does not exist' in p for p in problems)


def test_a_protocol_without_a_declaration_is_a_finding(tree: pathlib.Path) -> None:
    _write(tree, 'couchbase/protocol/couchbase2/__init__.py', '')
    _, problems = check(tree, _FACADES)
    assert any("protocol 'couchbase2' declares no capabilities" in p for p in problems)


def test_the_tree_is_clean() -> None:
    sites, problems = check(ROOT, FACADES)
    assert problems == []
    assert sites
    assert main([]) == 0


class _RecordingImpl:
    """Any attribute or call on it is recorded and returns the impl itself, so a facade can reach through it."""

    def __init__(self, capabilities: frozenset[Capability]) -> None:
        self.capabilities = capabilities
        self.reached: list[str] = []

    def __getattr__(self, name: str) -> Any:
        self.reached.append(name)
        return self

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self


_SITES = check(ROOT, FACADES)[0]


def _call(site: Site, impl: _RecordingImpl) -> Any:
    """``site``'s facade method on a facade over ``impl``, with a placeholder for each required argument."""
    module, _, name = site.facade.rpartition('.')
    facade = object.__new__(getattr(importlib.import_module(module), name))
    facade._impl = impl
    method = getattr(facade, site.method)
    required = [p for p in inspect.signature(method).parameters.values()
                if p.default is p.empty and p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return method(*(object() for _ in required))


@pytest.mark.parametrize('site', _SITES, ids=[f'{s.facade}.{s.method}' for s in _SITES])
def test_every_require_site_raises_without_its_capability(site: Site) -> None:
    impl = _RecordingImpl(frozenset())
    with pytest.raises(FeatureUnavailableException):
        _call(site, impl)
    assert impl.reached == []


@pytest.mark.parametrize('site', _SITES, ids=[f'{s.facade}.{s.method}' for s in _SITES])
def test_every_require_site_reaches_the_impl_with_its_capability(site: Site) -> None:
    impl = _RecordingImpl(frozenset({Capability[site.capability]}))
    _call(site, impl)
    assert impl.reached
