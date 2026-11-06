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

import pathlib
import sys
import types
import warnings

import pytest

from tools.check_public_api import (BINDING,
                                    INVENTORY,
                                    Binding,
                                    Compat,
                                    Entry,
                                    Tree,
                                    agreement_problems,
                                    bindings,
                                    binds,
                                    common_problems,
                                    compat_problems,
                                    coverage_problems,
                                    defined_names,
                                    docs_problems,
                                    documented,
                                    example_api,
                                    example_problems,
                                    import_problems,
                                    is_public,
                                    main,
                                    module_name,
                                    module_problems,
                                    read_counterparts,
                                    read_inventory,
                                    render_deprecated_paths,
                                    render_shim,
                                    replacement_problems,
                                    runtime_imports,
                                    twin_problems)


@pytest.mark.parametrize('path, expected', [
    ('couchbase/__init__.py', True),
    ('couchbase/common/result.py', True),
    ('acouchbase/management/buckets.py', True),
    ('txcouchbase/protocol/classic/__init__.py', True),
    ('couchbase/common/_option_spec.py', False),
    ('couchbase/common/_interfaces/_collection.py', False),
    ('couchbase/protocol/classic/_cluster_impl.py', False),
    ('couchbase/_version.py', False),
    ('couchbase/tests/kv_t.py', False),
    ('acouchbase/tests/_generated/kv_t.py', False),
    ('tools/check_public_api.py', False),
    ('couchbase/common/README.md', False),
])
def test_is_public(path: str, expected: bool) -> None:
    assert is_public(path) is expected


def test_module_name() -> None:
    assert module_name('couchbase/common/result.py') == 'couchbase.common.result'
    assert module_name('acouchbase/management/__init__.py') == 'acouchbase.management'


def _write(tmp_path: pathlib.Path, text: str) -> pathlib.Path:
    path = tmp_path / 'public_api.yaml'
    path.write_text(text)
    return path


def test_read_inventory_reads_each_compat_form(tmp_path: pathlib.Path) -> None:
    path = _write(tmp_path, 'modules:\n  a.b:\n    public: []\n    compat:\n      X: a.c\n'
                            '      Y: {from: a._d, use: a.c.Z}\n      Z: {from: a._d, none: internal}\n')
    assert read_inventory(path)['a.b'].compat == {'X': Compat('a.c', 'a.c.X'), 'Y': Compat('a._d', 'a.c.Z'),
                                                  'Z': Compat('a._d', None, 'internal')}


def test_read_inventory(tmp_path: pathlib.Path) -> None:
    path = _write(tmp_path, 'modules:\n  a.b:\n    public:\n      - X\n      - Y\n    compat: []\n')
    assert read_inventory(path) == {'a.b': Entry(('X', 'Y'), {})}
    path = _write(tmp_path, 'modules:\n  a.b:\n    docs: true\n    public:\n      - X\n    compat: []\n')
    assert read_inventory(path) == {'a.b': Entry(('X',), {}, docs=True)}


@pytest.mark.parametrize('lists, message', [
    ('public:\n      - Y\n      - X\n    compat: []', 'not sorted'),
    ('public:\n      - X\n      - X\n    compat: []', 'not sorted'),
    ('public:\n      - _X\n    compat: []', 'underscored'),
    ('public:\n      - X\n    compat:\n      X: a.c', 'both public and compat'),
    ('public: []\n    compat:\n      - X', 'compat is a mapping'),
    ('public: []\n    compat:\n      X: {from: a.c}', r'a.b.X: write <module>'),
    ('public: []\n    compat:\n      X: {from: a.c, use: a.d.X, none: gone}', r'a.b.X: write <module>'),
    ('doc: true\n    public: []\n    compat: []', r"unknown keys \['doc'\]"),
    ('docs: false\n    public: []\n    compat: []', 'docs is False; write true, or leave it out'),
    ('docs: 1\n    public: []\n    compat: []', 'docs is 1'),
])
def test_read_inventory_refuses_a_malformed_entry(tmp_path: pathlib.Path, lists: str, message: str) -> None:
    path = _write(tmp_path, f'modules:\n  a.b:\n    {lists}\n')
    with pytest.raises(ValueError, match=message):
        read_inventory(path)


def test_defined_names_reach_into_if_and_try_and_skip_imports_and_underscores() -> None:
    source = '''
import os
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from x import Y
    Alias = int
else:
    Other = str
try:
    import z
except ImportError:
    Fallback = None
class Public: ...
class _Private: ...
def function(): ...
async def coroutine(): ...
CONSTANT: int = 1
FIRST, SECOND = 1, 2
_hidden = 3
__all__ = ['Public']
def _helper():
    Inner = 4
'''
    assert defined_names(source) == {'Alias', 'Other', 'Fallback', 'Public', 'function', 'coroutine',
                                     'CONSTANT', 'FIRST', 'SECOND'}


def _module(**names: object) -> types.ModuleType:
    module = types.ModuleType('_public_api_fake')
    vars(module).update(names)
    return module


def test_binds() -> None:
    module = _module(Here=1)
    assert binds(module, 'Here')
    assert not binds(module, 'Gone')


def test_a_name_served_from_an_absent_binding_counts_as_bound() -> None:
    def lazy(name: str) -> object:
        raise ModuleNotFoundError(f'No module named {BINDING!r}', name=BINDING)

    def broken(name: str) -> object:
        raise ModuleNotFoundError("No module named 'elsewhere'", name='elsewhere')

    assert binds(_module(__getattr__=lazy), 'get_metadata')
    with pytest.raises(ModuleNotFoundError):
        binds(_module(__getattr__=broken), 'get_metadata')


ORIGIN = '_public_api_origin'
KEPT = {'Kept': Compat(ORIGIN, f'{ORIGIN}.Kept')}


def _serving(module: types.ModuleType, served: object, warn: bool = True) -> None:
    def __getattr__(name: str) -> object:
        if name != 'Kept':
            raise AttributeError(name)
        if warn:
            warnings.warn(f'Importing Kept from {module.__name__} is deprecated', UserWarning)
        return served
    module.__getattr__ = __getattr__  # type: ignore[attr-defined]


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    module = _module(Public=1, Used=3)
    origin = types.ModuleType(ORIGIN)
    origin.Kept = object()  # type: ignore[attr-defined]
    _serving(module, origin.Kept)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setitem(sys.modules, ORIGIN, origin)
    return module


def test_a_module_that_agrees_has_no_problems(fake: types.ModuleType) -> None:
    fake.__all__ = ['Public']  # type: ignore[attr-defined]
    assert module_problems(fake.__name__, Entry(('Public',), KEPT)) == []


def test_a_compat_name_bound_at_import_cannot_warn(fake: types.ModuleType) -> None:
    fake.Kept = sys.modules[ORIGIN].Kept  # type: ignore[attr-defined]
    [problem] = compat_problems(fake.__name__, fake, Entry((), KEPT))
    assert problem.startswith(f'{fake.__name__}.Kept: bound at import, so importing it cannot warn')


def test_a_compat_name_served_as_another_object_fails(fake: types.ModuleType) -> None:
    _serving(fake, object())
    assert compat_problems(fake.__name__, fake, Entry((), KEPT)) == [
        f'{fake.__name__}.Kept: is not the object {ORIGIN}.Kept is']


def test_a_compat_name_served_without_a_warning_fails(fake: types.ModuleType) -> None:
    _serving(fake, sys.modules[ORIGIN].Kept, warn=False)  # type: ignore[attr-defined]
    assert compat_problems(fake.__name__, fake, Entry((), KEPT)) == [
        f'{fake.__name__}.Kept: served without the deprecation warning']


def test_replacement_problems_name_a_replacement_that_is_not_public_and_a_path_serving_itself() -> None:
    inventory = {'a.b': Entry(('X',), {}),
                 'a.c': Entry((), {'X': Compat('a.b', 'a.b.X'), 'Y': Compat('a._d', 'a.b.Y'),
                                   'Z': Compat('a.c', None, 'gone')})}
    assert replacement_problems(inventory) == ['a.c.Y: use a.b.Y, which is not a public name',
                                               'a.c.Z: served from its own module, which cannot then warn']


def test_render_deprecated_paths_gives_each_name_its_origin_and_advice() -> None:
    inventory = {'a.b': Entry((), {'X': Compat('a.c', 'a.c.X'), 'Y': Compat('a._d', None, 'internal')}),
                 'a.c': Entry(('X',), {})}
    text = render_deprecated_paths(inventory)
    assert ("    'a.b': {\n"
            "        'X': ('a.c', 'Import X from a.c instead.'),\n"
            "        'Y': ('a._d', 'Nothing replaces it: internal.'),\n"
            "    },\n}\n") in text
    assert "'a.c': {" not in text


def test_a_module_without_all_fails(fake: types.ModuleType) -> None:
    assert module_problems(fake.__name__, Entry(('Public',), {})) == [
        f'{fake.__name__}: has no __all__; write it from the public list']


def test_an_all_that_differs_names_each_difference(fake: types.ModuleType) -> None:
    fake.__all__ = ['Kept', 'Used', 'Used']  # type: ignore[attr-defined]
    [problem] = module_problems(fake.__name__, Entry(('Public', 'Used'), KEPT))
    assert problem.endswith("not public ['Kept']; missing ['Public']; twice ['Used']")


def test_a_listed_name_the_module_does_not_bind_fails(fake: types.ModuleType) -> None:
    fake.__all__ = ['Public', 'Gone']  # type: ignore[attr-defined]
    assert module_problems(fake.__name__, Entry(('Gone', 'Public'), {'Lost': Compat(ORIGIN, None, 'x')})) == [
        f'{fake.__name__}.Gone: listed as public and not bound',
        f'{fake.__name__}.Lost: listed as compat and not served; the module needs the __getattr__ '
        'couchbase/common/_deprecated_paths.py gives it']


def test_a_common_name_the_inventory_does_not_list_fails() -> None:
    source = 'class Listed: ...\nclass Kept: ...\nclass Leaked: ...\nclass _Private: ...\n'
    assert common_problems('m', source, Entry(('Listed',), KEPT)) == [
        'm.Leaked: defined in common/ and not in the inventory; list it as public, or underscore it']
    assert len(common_problems('m', source, None)) == 3


IMPORTS = """\
import json
from typing import TYPE_CHECKING
from couchbase.common.result import GetResult
from couchbase._version import __version__
from . import sibling
from ..options import QueryOptions

if TYPE_CHECKING:
    from couchbase.collection import Collection
else:
    from couchbase.auth import Authenticator


def build():
    from acouchbase.cluster import Cluster
"""


def test_runtime_imports_skip_type_checking_and_resolve_relative_imports() -> None:
    assert runtime_imports(IMPORTS, 'couchbase.common.transactions.keyspace') == [
        (1, 'json'), (2, 'typing'), (3, 'couchbase.common.result'), (4, 'couchbase._version'),
        (5, 'couchbase.common.transactions'), (6, 'couchbase.common.options'),
        (11, 'couchbase.auth'), (15, 'acouchbase.cluster')]
    assert runtime_imports('from . import sibling\n', 'couchbase.common.encryption', is_package=True) == [
        (1, 'couchbase.common.encryption')]


def test_a_runtime_import_from_outside_common_fails(tmp_path: pathlib.Path) -> None:
    path = tmp_path / 'keyspace.py'
    path.write_text(IMPORTS)
    assert import_problems('couchbase.common.transactions.keyspace', path) == [
        f'couchbase.common.transactions.keyspace:{line}: imports {target} at runtime; common/ imports only '
        'common/, outside TYPE_CHECKING' for line, target in ((11, 'couchbase.auth'), (15, 'acouchbase.cluster'))]


def test_bindings_say_where_each_name_comes_from_and_under_which_guard() -> None:
    source = """\
from typing import TYPE_CHECKING
from couchbase.common.auth import Authenticator
from .keyring import Keyring as Ring
from couchbase.common.otel import _HAS_OTEL
if TYPE_CHECKING:
    from couchbase.cluster import Cluster
if _HAS_OTEL:
    from couchbase.common.otel import Wrapper
class Local: ...
"""
    assert bindings(source, 'couchbase.encryption', is_package=True) == {
        'TYPE_CHECKING': Binding('typing', None),
        'Authenticator': Binding('couchbase.common.auth', None),
        'Ring': Binding('couchbase.encryption.keyring', None),
        '_HAS_OTEL': Binding('couchbase.common.otel', None),
        'Wrapper': Binding('couchbase.common.otel', '_HAS_OTEL'),
        'Local': Binding(None, None),
    }


def _tree(tmp_path: pathlib.Path, files: dict[str, str]) -> Tree:
    for path, text in files.items():
        (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / path).write_text(text)
    return Tree(tmp_path)


TREE = {
    'couchbase/common/thing.py': 'class Shared: ...\nclass VeryLongSharedName' + 'X' * 60 + ': ...\n',
    'couchbase/common/otel.py': '_HAS_OTEL = True\nclass Wrapper: ...\n',
    'couchbase/thing.py': ('from couchbase.common.thing import Shared\n'
                           'from couchbase.common.thing import VeryLongSharedName' + 'X' * 60 + '\n'
                           'from couchbase.common.otel import _HAS_OTEL\n'
                           'from couchbase.facade import Facade\n'
                           'if _HAS_OTEL:\n    from couchbase.common.otel import Wrapper\n'
                           'class SyncOnly: ...\n'),
    'couchbase/facade.py': 'from couchbase.thing import Shared\nclass Facade: ...\n',
}


def test_defined_in_follows_re_exports_and_shared_means_defined_in_common(tmp_path: pathlib.Path) -> None:
    tree = _tree(tmp_path, TREE)
    assert tree.defined_in('couchbase.facade', 'Shared') == 'couchbase.common.thing'
    assert tree.defined_in('couchbase.thing', 'SyncOnly') == 'couchbase.thing'
    assert tree.defined_in('couchbase.thing', 'Missing') is None
    entry = Entry(('Facade', 'Shared', 'SyncOnly', 'Wrapper'), {})
    assert tree.shared('couchbase.thing', entry) == ('Shared', 'Wrapper')


def test_render_shim_takes_shared_names_from_common_and_the_rest_from_the_apis_own_modules(
        tmp_path: pathlib.Path) -> None:
    tree = _tree(tmp_path, TREE)
    long = 'VeryLongSharedName' + 'X' * 60
    counterparts = {'couchbase.thing': {'acouchbase': 'shim', 'txcouchbase': 'none: no'},
                    'couchbase.facade': {'acouchbase': 'own', 'txcouchbase': 'none: no'}}
    text = render_shim(tree, counterparts, 'acouchbase', 'couchbase.thing',
                       Entry(('Facade', 'Shared', long, 'Wrapper'), {'SyncOnly': Compat('couchbase.thing', None, 'x')}))
    body = text.split('"""', 2)[2]
    assert "__all__ = [\n    'Facade',\n    'Shared',\n    '" + long + "',\n]" in body
    assert 'from acouchbase.facade import Facade  # noqa: F401\n' in body
    assert 'from couchbase.common.otel import _HAS_OTEL\n' in body
    assert 'from couchbase.common.thing import Shared  # noqa: F401\n' in body
    assert 'SyncOnly' not in body
    assert 'from typing import TYPE_CHECKING\n\nfrom acouchbase.facade' in body
    assert 'from couchbase.common._supportability import Supportability\n' in body
    assert f'from couchbase.common.thing import (  # nopep8 # isort:skip # noqa: F401\n    {long},\n)' in body
    assert body.endswith("if _HAS_OTEL:\n    __all__ += ['Wrapper']\n"
                         '    from couchbase.common.otel import Wrapper  # noqa: F401\n'
                         '\nif not TYPE_CHECKING:\n    __getattr__ = Supportability.deprecated_paths(__name__)\n')
    assert "the rest are asyncio's own, from `acouchbase.facade`" in ' '.join(text.split())


def test_render_shim_refuses_a_name_the_couchbase_module_does_not_bind(tmp_path: pathlib.Path) -> None:
    tree = _tree(tmp_path, TREE)
    with pytest.raises(ValueError, match='acouchbase.thing.Gone: couchbase.thing does not bind it'):
        render_shim(tree, {}, 'acouchbase', 'couchbase.thing', Entry(('Gone',), {}))


@pytest.mark.parametrize('apis, message', [
    ('acouchbase: own', 'must name'),
    ('acouchbase: own\n    txcouchbase: maybe', "is 'maybe'"),
    ("acouchbase: own\n    txcouchbase: 'none: '", "is 'none: '"),
])
def test_read_counterparts_refuses_a_malformed_entry(tmp_path: pathlib.Path, apis: str, message: str) -> None:
    path = _write(tmp_path, f'counterparts:\n  couchbase.x:\n    {apis}\n')
    with pytest.raises(ValueError, match=message):
        read_counterparts(path)


def test_coverage_problems_name_every_module_the_section_does_not_account_for() -> None:
    empty = Entry((), {})
    inventory = {m: empty for m in ('couchbase.a', 'couchbase.b', 'couchbase.common.c', 'acouchbase.a',
                                    'acouchbase.stray', 'txcouchbase.b')}
    counterparts = {'couchbase.a': {'acouchbase': 'own', 'txcouchbase': 'shim'},
                    'couchbase.gone': {'acouchbase': 'none: x', 'txcouchbase': 'none: x'}}
    assert coverage_problems(inventory, counterparts) == [
        'couchbase.b: no counterparts entry; say what acouchbase and txcouchbase have at its path',
        'couchbase.gone: has a counterparts entry and is not a public couchbase module outside common/',
        'acouchbase.stray: no couchbase module names it as a counterpart',
        'txcouchbase.b: no couchbase module names it as a counterpart',
        'txcouchbase.a: counterparts says shim, and it is not in the inventory',
    ]
    counterparts['couchbase.b'] = {'acouchbase': 'none: x', 'txcouchbase': 'own'}
    assert 'txcouchbase.b: in the inventory, and counterparts says couchbase.b has none on txcouchbase' not in (
        coverage_problems(inventory, counterparts))
    counterparts['couchbase.b'] = {'acouchbase': 'own', 'txcouchbase': 'none: x'}
    assert 'txcouchbase.b: in the inventory, and counterparts says couchbase.b has none on txcouchbase' in (
        coverage_problems(inventory, counterparts))


def test_agreement_problems_require_every_shared_name_as_the_same_object(monkeypatch: pytest.MonkeyPatch) -> None:
    shared, copy = type('Shared', (), {}), type('Shared', (), {})
    ours = types.ModuleType('_public_api_ours')
    theirs = types.ModuleType('_public_api_theirs')
    vars(ours).update(Shared=shared, Other=1)
    vars(theirs).update(Shared=copy)
    for module in (ours, theirs):
        monkeypatch.setitem(sys.modules, module.__name__, module)
    assert agreement_problems(ours.__name__, theirs.__name__, ['Shared', 'Other'], Entry(('Shared',), {})) == [
        "_public_api_theirs: does not publish what it shares with _public_api_ours: ['Other']",
        '_public_api_theirs.Shared: is not the object _public_api_ours.Shared is']
    theirs.Shared = shared  # type: ignore[attr-defined]
    assert agreement_problems(ours.__name__, theirs.__name__, ['Shared'], Entry(('Shared',), {})) == []


def test_twin_problems_name_a_common_twins_public_name_bound_and_not_listed(tmp_path: pathlib.Path) -> None:
    tree = _tree(tmp_path, TREE)
    inventory = {'couchbase.common.thing': Entry(('Shared', 'Unbound'), {}),
                 'couchbase.thing': Entry(('SyncOnly',), {})}
    assert twin_problems(tree, inventory) == [
        "couchbase.thing: binds ['Shared'], public on couchbase.common.thing, and does not list them"]
    inventory['couchbase.thing'] = Entry(('SyncOnly',), {'Shared': Compat('couchbase.common.thing', None, 'x')})
    assert twin_problems(tree, inventory) == []


def test_docs_problems_name_a_flag_where_no_page_belongs() -> None:
    page, plain = Entry(('X',), {}, docs=True), Entry(('X',), {})
    inventory = {'couchbase.a': page, 'acouchbase.a': page, 'txcouchbase.a': page,
                 'couchbase.b': page, 'acouchbase.b': plain,
                 'couchbase.c': plain, 'txcouchbase.c': page,
                 'couchbase.common.a': page, 'couchbase.protocol.classic': page,
                 'acouchbase.protocol.classic': page}
    counterparts = {'couchbase.a': {'acouchbase': 'own', 'txcouchbase': 'shim'},
                    'couchbase.b': {'acouchbase': 'own', 'txcouchbase': 'none: x'},
                    'couchbase.c': {'acouchbase': 'none: x', 'txcouchbase': 'own'},
                    'couchbase.protocol.classic': {'acouchbase': 'shim', 'txcouchbase': 'none: x'}}
    assert docs_problems(inventory, counterparts) == [
        "acouchbase.protocol.classic: docs: true on a module that is not own; its names have couchbase's page",
        'couchbase.common.a: docs: true under common/ or protocol/; flag the module that publishes it',
        'couchbase.protocol.classic: docs: true under common/ or protocol/; flag the module that publishes it',
        "txcouchbase.a: docs: true on a module that is not own; its names have couchbase's page",
        'acouchbase.b: docs: true on one of it and couchbase.b only; flag both or neither',
        'txcouchbase.c: docs: true on one of it and couchbase.c only; flag both or neither',
    ]
    inventory.update({'acouchbase.b': page, 'couchbase.c': page, 'txcouchbase.a': plain,
                      'couchbase.common.a': plain, 'couchbase.protocol.classic': plain,
                      'acouchbase.protocol.classic': plain})
    assert docs_problems(inventory, counterparts) == []


def test_documented_follows_each_flagged_name_to_its_definition(tmp_path: pathlib.Path) -> None:
    tree = _tree(tmp_path, TREE)
    inventory = {'couchbase.thing': Entry(('Shared', 'SyncOnly', 'Served'), KEPT, docs=True),
                 'couchbase.facade': Entry(('Facade', 'Shared'), {})}
    assert documented(tree, inventory) == {('couchbase.common.thing', 'Shared'),
                                           ('couchbase.thing', 'SyncOnly'), ('couchbase.thing', 'Served')}


@pytest.mark.parametrize('path, expected', [
    ('examples/couchbase/kv_operations.py', 'couchbase'),
    ('examples/acouchbase/management/acouchbase_bucketmgmt.py', 'acouchbase'),
    ('examples/txcouchbase/txcouchbase_kv_operations.py', 'txcouchbase'),
    ('examples/logging/filtering_logs.py', 'couchbase'),
])
def test_example_api(path: str, expected: str) -> None:
    assert example_api(path) == expected


EXAMPLE_INVENTORY = {
    'couchbase.options': Entry(('GetOptions',), {}),
    'couchbase.collection': Entry(('Collection',),
                                  {'GetOptions': Compat('couchbase.options', 'couchbase.options.GetOptions')}),
    'couchbase.subdocument': Entry(('upsert',), {}),
    'couchbase.otel_tracer': Entry(('OtelTracer',), {}),
    'acouchbase': Entry((), {}),
    'acouchbase.options': Entry(('GetOptions',), {}),
    'acouchbase.subdocument': Entry(('upsert',), {}),
}


@pytest.mark.parametrize('path, source, expected', [
    ('examples/couchbase/a.py', 'from couchbase.options import GetOptions\nimport couchbase.subdocument as SD\n', []),
    ('examples/couchbase/a.py', 'from couchbase.collection import GetOptions\n',
     ['examples/couchbase/a.py:1: couchbase.collection.GetOptions: not public at that path']),
    ('examples/couchbase/a.py', 'from couchbase.collection import MutateInOptions\n',
     ['examples/couchbase/a.py:1: couchbase.collection.MutateInOptions: not public at that path']),
    ('examples/couchbase/a.py', 'import couchbase._private\nimport json\n',
     ['examples/couchbase/a.py:1: couchbase._private: not a public module']),
    ('examples/couchbase/a.py', 'from couchbase import subdocument\n', []),
    ('examples/acouchbase/a.py', 'import acouchbase\nfrom acouchbase.options import GetOptions\n', []),
    ('examples/acouchbase/a.py', 'from couchbase.options import GetOptions\nimport couchbase.subdocument as SD\n',
     ['examples/acouchbase/a.py:1: couchbase.options.GetOptions: the acouchbase examples import it from '
      'acouchbase.options',
      'examples/acouchbase/a.py:2: couchbase.subdocument: the acouchbase examples import it from '
      'acouchbase.subdocument']),
    ('examples/acouchbase/a.py', 'from couchbase.otel_tracer import OtelTracer\n', []),
])
def test_example_problems(path: str, source: str, expected: list[str]) -> None:
    assert example_problems(path, source, EXAMPLE_INVENTORY) == expected


def test_the_inventory_reads() -> None:
    assert read_inventory(INVENTORY)
    assert read_counterparts(INVENTORY)


def test_the_tree_agrees_with_the_inventory() -> None:
    assert main([]) == 0
