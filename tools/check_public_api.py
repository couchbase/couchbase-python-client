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

"""Hold every public module to ``public_api.yaml``, the inventory of what each path supports.

A module is public when no part of its dotted path starts with an underscore, outside the tests.  The
inventory lists every one, and under it two lists of names:

* ``public``: the supported API at that path, published as the module's ``__all__``.
* ``compat``: a deprecated path, a name the module still serves so code that imports it keeps
  working, which is not supported and is not in ``__all__``.  Each says where it is served from and
  what replaces it: ``Name: <module>``, a public module publishing the same object, or
  ``Name: {from: <module>, use: <path>}`` or ``{from: <module>, none: <reason>}``.  The list only
  shrinks, and removing a name is a deliberate edit.

A compat name is not bound in its module.  ``--write`` renders the compat entries into
``couchbase/common/_deprecated_paths.py``, and the module's ``__getattr__``,
``Supportability.deprecated_paths``, serves each from there with a ``CouchbaseDeprecationWarning`` at
the import that names the replacement.  The module installs it under ``if not TYPE_CHECKING:``, so a
type checker does not see the old paths.

A name in neither list is private, or bound only because the module uses it, and is not a promise.
Private names are not listed: the underscore is the record.

The check fails on:

1. **A public module the inventory does not list**, or a listed module that does not exist.
2. **An ``__all__`` that differs from the public list**, or a module with no ``__all__``.
3. **A listed name the module does not bind.**  A name the module serves from the native binding
   through its ``__getattr__`` counts as bound where the binding is absent, as in the pre-commit
   environment, since the name exists wherever the binding does.  A compat name must be served and
   not bound: bound at import, it could not warn.  It must be the object its origin binds, served
   with the warning, and what replaces it must be a public name.  ``_deprecated_paths.py`` must be
   what the inventory renders.
4. **A name defined in a public ``couchbase/common/`` module that the inventory does not list**, by
   class, function or assignment at the top level, without an underscore.  So a ``common/`` module
   holds listed names and underscored names and nothing between, and a new public name enters
   ``common/`` only by an inventory edit a reviewer sees.
5. **A runtime import in ``couchbase/common/`` of an SDK module outside it**, other than
   ``couchbase._version``.  Every module there is checked, underscored or not.  So the shared code
   loads without any API package, and an API package that re-exports it pulls in nothing of another
   API.  An import under ``if TYPE_CHECKING:`` is not a runtime import and is not checked: an
   annotation may name the facades a function accepts.

The inventory's ``counterparts`` section says, for each public ``couchbase`` module outside ``common/``,
what ``acouchbase`` and ``txcouchbase`` have at the same path: ``own``, a module the API writes,
``shim``, one rendered from the inventory, or ``none:`` and the reason.  A name the ``couchbase`` module
gets from ``couchbase/common/``, following re-exports, is *shared*.  The check also fails on:

6. **A public module the section does not account for**: a ``couchbase`` module with no entry, an API
   module no entry names, or an entry the inventory contradicts.
7. **A counterpart that does not publish a shared name, or binds a different object for it.**  So
   ``isinstance`` and ``except`` agree whichever path a caller imports.  Every other name a counterpart
   publishes is its API's own, such as ``acouchbase.cluster.Cluster``.
8. **A shim that differs from what the template renders.**  A shim imports each name from where the
   ``couchbase`` module does, swapped for the API's counterpart of that module where it has one.
9. **A ``couchbase`` module that binds a public name of its ``common/`` twin and lists it nowhere**, as
   ``couchbase.management.views`` binding ``couchbase.common.management.views.DesignDocument``.  What
   it shares is what it publishes, so an unlisted name would reach no counterpart.

The examples under ``examples/`` are read too, as source.  The check fails on:

10. **An example importing an SDK name from a path that does not publish it**, a compat name
    included, or importing a module the inventory does not list.  An example under an
    ``acouchbase`` or ``txcouchbase`` directory imports from its own API wherever that API's
    counterpart publishes the name.  So what an example teaches is the supported path.

A module entry may also carry ``docs: true``: the module is user-facing, and its public names get a
reference page, a docstring and a repr.  ``documented()`` is that set, each name followed to where it
is defined, so the checks that hold those names to a standard read their scope from here.  The flag
is ``true`` or absent.  The check fails on:

11. **A flag where no page belongs**: on a ``couchbase`` module under ``common/`` or ``protocol/``, whose
    names reach a page through the module that publishes them, or on an ``acouchbase`` or
    ``txcouchbase`` module that is not ``own``, since a shim's names are ``couchbase``'s objects.
12. **An ``own`` counterpart flagged differently from its ``couchbase`` module.**  So no API's page for a
    path is missing while another API has one.

The inventory itself must keep each list sorted, with no duplicates, no underscored names and no name
in both lists.

Run directly, or via the ``check-public-api`` pre-commit hook::

    python tools/check_public_api.py

After editing a shim's inventory entry, or any compat entry, render the shims and the table again::

    python tools/check_public_api.py --write

The modules are imported, so the interpreter needs what they import at load, OpenTelemetry's API
included.  Exits 1 when the tree and the inventory disagree.  Tests::

    python -m pytest tools/tests -q
"""

from __future__ import annotations

import ast
import importlib
import pathlib
import subprocess
import sys
import textwrap
import warnings
from collections.abc import Iterable
from typing import NamedTuple

import yaml

INVENTORY = pathlib.Path(__file__).with_name('public_api.yaml')
PACKAGES = ('couchbase', 'acouchbase', 'txcouchbase')
COMMON = 'couchbase.common'
PROTOCOL = 'couchbase.protocol'
BINDING = 'couchbase.protocol.classic._pycbc_core'
EXCLUDED_DIRS = frozenset({'tests', '_generated'})
EXAMPLES = 'examples'
ALLOWED_OUTSIDE_COMMON = frozenset({'couchbase._version'})
DEPRECATED_PATHS = 'couchbase/common/_deprecated_paths.py'
SUPPORTABILITY = 'couchbase.common._supportability'


class Compat(NamedTuple):
    """What a deprecated path serves, and what replaces it.

    ``origin`` is the module the path serves the name from.  ``use`` is the dotted path to import
    instead, and ``reason`` says why nothing does when ``use`` is ``None``.
    """
    origin: str
    use: str | None
    reason: str | None = None

    @property
    def advice(self) -> str:
        """What the warning tells a caller to do instead."""
        if self.use is None:
            return f'Nothing replaces it: {self.reason}.'
        module, _, name = self.use.rpartition('.')
        return f'Import {name} from {module} instead.'


class Entry(NamedTuple):
    public: tuple[str, ...]
    compat: dict[str, Compat]
    docs: bool = False

    def tiers(self) -> dict[str, tuple[str, ...]]:
        return {'public': self.public, 'compat': tuple(self.compat)}


def _compat(where: str, name: str, raw: object) -> Compat:
    """A compat entry: ``<module>``, which publishes the name, ``{from: <module>, use: <path>}``, or
    ``{from: <module>, none: <reason>}``."""
    if isinstance(raw, str):
        return Compat(raw, f'{raw}.{name}')
    if isinstance(raw, dict) and len(raw) == 2 and 'from' in raw and ('use' in raw or 'none' in raw):
        if all(isinstance(v, str) and v.strip() for v in raw.values()):
            return Compat(raw['from'], raw.get('use'), raw.get('none'))
    raise ValueError(f'{where}.{name}: write <module>, or {{from: <module>, use: <path>}}, '
                     'or {from: <module>, none: <reason>}')


def module_name(path: str) -> str:
    parts = pathlib.PurePosixPath(path).with_suffix('').parts
    return '.'.join(parts[:-1] if parts[-1] == '__init__' else parts)


def is_public(path: str) -> bool:
    parts = pathlib.PurePosixPath(path).parts
    return (path.endswith('.py') and parts[0] in PACKAGES and not EXCLUDED_DIRS.intersection(parts)
            and not any(p.startswith('_') for p in module_name(path).split('.')))


def _sources(root: pathlib.Path, *paths: str) -> list[str]:
    # Untracked files are included so a new module is checked before it is staged.
    out = subprocess.run(['git', 'ls-files', '--cached', '--others', '--exclude-standard', '--', *paths],
                         cwd=root, check=True, capture_output=True, text=True).stdout
    return [p for p in sorted(set(out.splitlines())) if p.endswith('.py') and (root / p).is_file()]


def public_modules(root: pathlib.Path) -> dict[str, pathlib.Path]:
    """Every public module in the tree, by dotted name, with its source file."""
    return {module_name(p): root / p for p in _sources(root, *PACKAGES) if is_public(p)}


def common_modules(root: pathlib.Path) -> dict[str, pathlib.Path]:
    """Every module under ``couchbase/common/``, underscored or not."""
    return {module_name(p): root / p for p in _sources(root, COMMON.replace('.', '/'))}


def read_inventory(path: pathlib.Path) -> dict[str, Entry]:
    modules = yaml.safe_load(path.read_text())['modules']
    inventory = {}
    for module, lists in modules.items():
        unknown = sorted(set(lists) - {'public', 'compat', 'docs'})
        if unknown:
            raise ValueError(f'{path.name}: {module} has unknown keys {unknown}')
        if lists.get('docs', True) is not True:
            raise ValueError(f'{path.name}: {module} docs is {lists["docs"]!r}; write true, or leave it out')
        compat = lists['compat'] or {}
        if not isinstance(compat, dict):
            raise ValueError(f'{path.name}: {module} compat is a mapping from each name to where it is served from')
        entry = Entry(tuple(lists['public'] or ()),
                      {name: _compat(f'{path.name}: {module}', name, raw) for name, raw in compat.items()},
                      'docs' in lists)
        for tier, names in entry.tiers().items():
            if list(names) != sorted(set(names)):
                raise ValueError(f'{path.name}: {module} {tier} is not sorted, or holds a name twice')
            underscored = [n for n in names if n.startswith('_')]
            if underscored:
                raise ValueError(f'{path.name}: {module} {tier} lists underscored names {underscored}')
        both = sorted(set(entry.public) & set(entry.compat))
        if both:
            raise ValueError(f'{path.name}: {module} lists {both} as both public and compat')
        inventory[module] = entry
    return inventory


def _top_level(body: Iterable[ast.stmt]) -> Iterable[ast.stmt]:
    for node in body:
        if isinstance(node, ast.If):
            yield from _top_level(node.body)
            yield from _top_level(node.orelse)
        elif isinstance(node, ast.Try):
            for block in (node.body, *(h.body for h in node.handlers), node.orelse, node.finalbody):
                yield from _top_level(block)
        else:
            yield node


def defined_names(source: str) -> set[str]:
    """The names a module defines at its top level without an underscore, imports aside."""
    found: set[str] = set()
    for node in _top_level(ast.parse(source).body):
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            found.add(node.name)
        elif isinstance(node, ast.Assign):
            found.update(n.id for t in node.targets for n in ast.walk(t) if isinstance(n, ast.Name))
        elif isinstance(node, (ast.AnnAssign, ast.AugAssign)) and isinstance(node.target, ast.Name):
            found.add(node.target.id)
    return {n for n in found if not n.startswith('_')}


def _type_checking(test: ast.expr) -> bool:
    return ((isinstance(test, ast.Name) and test.id == 'TYPE_CHECKING')
            or (isinstance(test, ast.Attribute) and test.attr == 'TYPE_CHECKING'))


def _resolve(node: ast.ImportFrom, module: str, is_package: bool) -> str:
    if not node.level:
        return node.module or ''
    parts = module.split('.') if is_package else module.split('.')[:-1]
    base = parts[:len(parts) - node.level + 1]
    return '.'.join(base + ([node.module] if node.module else []))


def runtime_imports(source: str, module: str, is_package: bool = False) -> list[tuple[int, str]]:
    """Each module a source imports outside ``if TYPE_CHECKING:``, by line, relative imports resolved."""
    found: list[tuple[int, str]] = []

    def visit(node: ast.AST) -> None:
        if isinstance(node, ast.If) and _type_checking(node.test):
            for child in node.orelse:
                visit(child)
            return
        if isinstance(node, ast.Import):
            found.extend((node.lineno, alias.name) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            found.append((node.lineno, _resolve(node, module, is_package)))
        for child in ast.iter_child_nodes(node):
            visit(child)

    visit(ast.parse(source))
    return found


def binds(module: object, name: str) -> bool:
    try:
        getattr(module, name)
    except AttributeError:
        return False
    except ImportError as exc:
        # Served from the binding by the module's __getattr__, in an interpreter without the binding.
        # Any other import failure is a broken tree, and is raised rather than reported as unbound.
        if not (exc.name or '').startswith(BINDING):
            raise
    return True


def module_problems(name: str, entry: Entry) -> list[str]:
    module = importlib.import_module(name)
    found = []
    published = getattr(module, '__all__', None)
    if published is None:
        found.append(f'{name}: has no __all__; write it from the public list')
    elif sorted(published) != list(entry.public):
        extra = sorted(set(published) - set(entry.public))
        missing = sorted(set(entry.public) - set(published))
        twice = sorted({n for n in published if list(published).count(n) > 1})
        details = [f'{label} {names}' for label, names in (('not public', extra), ('missing', missing),
                                                           ('twice', twice)) if names]
        found.append(f'{name}: __all__ differs from the public list: {"; ".join(details)}')
    found += [f'{name}.{n}: listed as public and not bound' for n in entry.public if not binds(module, n)]
    return found + compat_problems(name, module, entry)


def compat_problems(name: str, module: object, entry: Entry) -> list[str]:
    """Each compat name is served on access, with a warning, as the object its origin binds."""
    found = []
    for n, compat in entry.compat.items():
        where = f'{name}.{n}'
        if n in vars(module):
            found.append(f'{where}: bound at import, so importing it cannot warn; leave it to {DEPRECATED_PATHS}')
            continue
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            served = getattr(module, n, None)
        if served is None:
            found.append(f'{where}: listed as compat and not served; the module needs the __getattr__ '
                         f'{DEPRECATED_PATHS} gives it')
        elif served is not getattr(importlib.import_module(compat.origin), n, None):
            found.append(f'{where}: is not the object {compat.origin}.{n} is')
        elif not any(str(w.message).startswith(f'Importing {n} from {name} is deprecated') for w in caught):
            found.append(f'{where}: served without the deprecation warning')
    return found


def replacement_problems(inventory: dict[str, Entry]) -> list[str]:
    """What replaces each compat name is a public name, and an entry is short wherever it can be."""
    found = []
    for module, entry in sorted(inventory.items()):
        for n, compat in entry.compat.items():
            if compat.origin == module:
                found.append(f'{module}.{n}: served from its own module, which cannot then warn')
            if compat.use is None:
                continue
            target, _, short = compat.use.rpartition('.')
            if target not in inventory or short not in inventory[target].public:
                found.append(f'{module}.{n}: use {compat.use}, which is not a public name')
    return found


def common_problems(name: str, source: str, entry: Entry | None) -> list[str]:
    listed = {*entry.public, *entry.compat} if entry else set()
    return [f'{name}.{n}: defined in common/ and not in the inventory; list it as public, or underscore it'
            for n in sorted(defined_names(source) - listed)]


def import_problems(name: str, path: pathlib.Path) -> list[str]:
    found = []
    for line, target in runtime_imports(path.read_text(), name, path.name == '__init__.py'):
        outside = (target.split('.')[0] in PACKAGES and target not in ALLOWED_OUTSIDE_COMMON
                   and target != COMMON and not target.startswith(f'{COMMON}.'))
        if outside:
            found.append(f'{name}:{line}: imports {target} at runtime; common/ imports only common/, '
                         'outside TYPE_CHECKING')
    return found


class Binding(NamedTuple):
    source: str | None
    guard: str | None


def bindings(source: str, module: str, is_package: bool = False) -> dict[str, Binding]:
    """Where a module binds each top-level name outside ``if TYPE_CHECKING:``.

    ``source`` is the module a name is imported from, relative imports resolved, and ``None`` where the
    module defines it.  ``guard`` is the name an enclosing ``if`` tests, as in ``if _HAS_OTEL:``.
    """
    found: dict[str, Binding] = {}

    def visit(body: Iterable[ast.stmt], guard: str | None) -> None:
        for node in body:
            if isinstance(node, ast.If):
                if not _type_checking(node.test):
                    visit(node.body, node.test.id if isinstance(node.test, ast.Name) else guard)
                visit(node.orelse, guard)
            elif isinstance(node, ast.Try):
                for block in (node.body, *(h.body for h in node.handlers), node.orelse, node.finalbody):
                    visit(block, guard)
            elif isinstance(node, ast.ImportFrom):
                origin = _resolve(node, module, is_package)
                found.update((a.asname or a.name, Binding(origin, guard)) for a in node.names)
            else:
                found.update((n, Binding(None, guard)) for n in _assigned(node))

    visit(ast.parse(source).body, None)
    return found


def _assigned(node: ast.stmt) -> list[str]:
    if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
        return [node.name]
    if isinstance(node, ast.Assign):
        return [n.id for t in node.targets for n in ast.walk(t) if isinstance(n, ast.Name)]
    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
        return [node.target.id]
    return []


def module_path(root: pathlib.Path, module: str) -> pathlib.Path | None:
    base = root.joinpath(*module.split('.'))
    for path in (base.with_suffix('.py'), base / '__init__.py'):
        if path.is_file():
            return path
    return None


class Tree:
    """The SDK's sources, read for where each name is bound and where it is defined."""

    def __init__(self, root: pathlib.Path) -> None:
        self.root = root
        self._bindings: dict[str, dict[str, Binding]] = {}

    def bindings(self, module: str) -> dict[str, Binding]:
        if module not in self._bindings:
            path = module_path(self.root, module)
            self._bindings[module] = ({} if path is None
                                      else bindings(path.read_text(), module, path.name == '__init__.py'))
        return self._bindings[module]

    def defined_in(self, module: str, name: str) -> str | None:
        """The SDK module that defines ``name`` as ``module`` binds it, following re-exports."""
        seen = set()
        while module not in seen:
            seen.add(module)
            binding = self.bindings(module).get(name)
            if binding is None:
                return None
            if binding.source is None:
                return module
            if binding.source.split('.')[0] not in PACKAGES:
                return None
            module = binding.source
        return None

    def shared(self, module: str, entry: Entry) -> tuple[str, ...]:
        """The public names of a ``couchbase`` module that are defined in ``couchbase/common/``."""
        return tuple(n for n in entry.public if _in_common(self.defined_in(module, n) or ''))


def _in_common(module: str) -> bool:
    return module == COMMON or module.startswith(f'{COMMON}.')


def _in_protocol(module: str) -> bool:
    return module == PROTOCOL or module.startswith(f'{PROTOCOL}.')


OWN, SHIM, NONE = 'own', 'shim', 'none'
APIS = {'acouchbase': 'asyncio', 'txcouchbase': 'Twisted'}


def read_counterparts(path: pathlib.Path) -> dict[str, dict[str, str]]:
    """Per public ``couchbase`` module outside ``common/``, what each other API has at its path.

    ``own`` is a module the API writes, ``shim`` one the template renders, and ``none: <why>`` no
    module at all.
    """
    counterparts = yaml.safe_load(path.read_text())['counterparts']
    for module, apis in counterparts.items():
        if sorted(apis) != sorted(APIS):
            raise ValueError(f'{path.name}: counterparts of {module} must name {sorted(APIS)}, and only those')
        for api, kind in apis.items():
            if kind not in (OWN, SHIM) and not (kind.startswith(f'{NONE}: ') and kind[len(NONE) + 2:].strip()):
                raise ValueError(f'{path.name}: {module} {api} is {kind!r}; '
                                 f'write {OWN}, {SHIM}, or {NONE}: and the reason')
    return counterparts


def twin(api: str, module: str) -> str:
    return api + module[len('couchbase'):]


def _has_twin(counterparts: dict[str, dict[str, str]], api: str, module: str) -> bool:
    return counterparts.get(module, {}).get(api, NONE) in (OWN, SHIM)


LICENSE = """\
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
"""
NOQA = '  # noqa: F401'


def _isort_key(name: str) -> tuple[int, str]:
    # isort's order_by_type: constants, then classes, then the rest, each ignoring case; a private name last.
    if name.startswith('_'):
        return 3, name.lower()
    if name.isupper() and len(name) > 1:
        return 0, name.lower()
    return (1 if name[:1].isupper() else 2), name.lower()


def _imports(lines: dict[str, list[str]], indent: str = '', used: frozenset[str] = frozenset()) -> list[str]:
    def line(origin: str, name: str) -> str:
        return f'{indent}from {origin} import {name}' + ('' if name in used else NOQA)

    pairs = sorted(((o, n) for o in lines for n in lines[o]), key=lambda p: (p[0].lower(), *_isort_key(p[1])))
    out = [line(o, n) for o, n in pairs if len(line(o, n)) <= 120]
    for origin in sorted(lines):
        names = [n for n in lines[origin] if len(line(origin, n)) > 120]
        if names:
            out += ['', f'{indent}from {origin} import (  # nopep8 # isort:skip # noqa: F401',
                    *(f'{indent}    {n},' for n in names), f'{indent})']
    return out


def _origins(tree: Tree, counterparts: dict[str, dict[str, str]], api: str, module: str,
             entry: Entry) -> tuple[dict[str, list[str]], dict[str, dict[str, list[str]]]]:
    """Where the shim imports each name from: unguarded, and under each guard."""
    found = tree.bindings(module)
    target = twin(api, module)

    def origin(name: str) -> str:
        if name not in found:
            raise ValueError(f'{target}.{name}: {module} does not bind it, so the template cannot render it')
        source = found[name].source or module
        return twin(api, source) if source != module and _has_twin(counterparts, api, source) else source

    plain: dict[str, list[str]] = {}
    guarded: dict[str, dict[str, list[str]]] = {}
    for name in sorted(entry.public):
        guard = found[name].guard if name in found else None
        (guarded.setdefault(guard, {}) if guard else plain).setdefault(origin(name), []).append(name)
    for guard in guarded:
        plain.setdefault(origin(guard), []).append(guard)
    # A package binds its submodules as attributes, as ``couchbase.observability`` does.
    for name, binding in sorted(found.items()):
        if binding.source == module and _has_twin(counterparts, api, f'{module}.{name}'):
            plain.setdefault(target, []).append(name)
    return plain, guarded


def _docstring(api: str, module: str, entry: Entry, facades: list[str]) -> str:
    target = twin(api, module)
    if not (entry.public or entry.compat):
        return f'"""The `{target}` package."""'
    flavour = APIS[api]
    if facades:
        own = ', '.join(f'`{f}`' for f in facades)
        text = (f'What `{module}` publishes, for {flavour} code.  The names every API shares are the same '
                f'objects as there, and the rest are {flavour}\'s own, from {own}.')
    else:
        text = f'The same objects as `{module}`, re-exported so {flavour} code imports them from its own package.'
    return f'"""The `{target}` import path.\n\n' + '\n'.join(textwrap.wrap(text, 100)) + '\n"""'


def render_shim(tree: Tree, counterparts: dict[str, dict[str, str]], api: str, module: str, entry: Entry) -> str:
    """The source of ``api``'s counterpart of ``module``, binding ``entry``'s names as ``module`` does.

    Each name is imported from where ``module`` imports it, or from ``module`` where it is defined
    there, the origin swapped for its ``api`` counterpart where it has one.  So the shared names are
    the same objects on every API, and a name ``module`` takes from a sync facade comes from the
    ``api`` facade instead.
    """
    plain, guarded = _origins(tree, counterparts, api, module, entry)
    target = twin(api, module)
    facades = sorted(o for o in plain if o != target and o.split('.')[0] == api
                     and counterparts.get(f'couchbase{o[len(api):]}', {}).get(api) == OWN)
    found = tree.bindings(module)
    unguarded = sorted(n for n in entry.public if not (n in found and found[n].guard))
    if unguarded:
        published = '__all__ = [\n' + ''.join(f"    '{n}',\n" for n in unguarded) + ']'
    else:
        published = '__all__: list[str] = []'
    out = [LICENSE, _docstring(api, module, entry, facades), '', published]
    used = set(guarded)
    if entry.compat:
        out += ['', 'from typing import TYPE_CHECKING']
        plain.setdefault(SUPPORTABILITY, []).append('Supportability')
        used.add('Supportability')
    body = _imports(plain, used=frozenset(used))
    if body:
        out += ['', *body]
    for guard, lines in sorted(guarded.items()):
        added = sorted(n for o in lines.values() for n in o if n in entry.public)
        out += ['', f'if {guard}:', f'    __all__ += {added!r}', *_imports(lines, '    ')]
    if entry.compat:
        out += ['', 'if not TYPE_CHECKING:', '    __getattr__ = Supportability.deprecated_paths(__name__)']
    return '\n'.join(out) + '\n'


def render_deprecated_paths(inventory: dict[str, Entry]) -> str:
    """The table ``Supportability.deprecated_paths`` serves each module's compat names from."""
    out = [LICENSE,
           '"""Each deprecated import path: the module it serves the name from, and what to do instead.',
           '',
           'Rendered from the compat entries in ``tools/public_api.yaml`` by',
           '``python tools/check_public_api.py --write``; do not edit.',
           '"""',
           '',
           'PATHS: dict[str, dict[str, tuple[str, str]]] = {']
    for module, entry in sorted(inventory.items()):
        if not entry.compat:
            continue
        out.append(f'    {module!r}: {{')
        for name, compat in entry.compat.items():
            line = f'        {name!r}: ({compat.origin!r}, {compat.advice!r}),'
            out += [line] if len(line) <= 120 else [f'        {name!r}: (', f'            {compat.origin!r},',
                                                    f'            {compat.advice!r},', '        ),']
        out.append('    },')
    return '\n'.join([*out, '}']) + '\n'


def deprecated_paths_problems(root: pathlib.Path, inventory: dict[str, Entry]) -> list[str]:
    path = root / DEPRECATED_PATHS
    if path.is_file() and path.read_text() == render_deprecated_paths(inventory):
        return []
    return [f'{DEPRECATED_PATHS}: is not what the inventory renders; run with --write']


def coverage_problems(inventory: dict[str, Entry], counterparts: dict[str, dict[str, str]]) -> list[str]:
    """Every public ``couchbase`` module outside ``common/`` says what each other API has at its path."""
    expected = {m for m in inventory if m.split('.')[0] == 'couchbase' and not _in_common(m)}
    found = [f'{m}: no counterparts entry; say what acouchbase and txcouchbase have at its path'
             for m in sorted(expected - set(counterparts))]
    found += [f'{m}: has a counterparts entry and is not a public couchbase module outside common/'
              for m in sorted(set(counterparts) - expected)]
    twins = {twin(api, m) for m, apis in counterparts.items() for api, kind in apis.items() if kind in (OWN, SHIM)}
    found += [f'{m}: no couchbase module names it as a counterpart'
              for m in sorted(m for m in inventory if m.split('.')[0] in APIS and m not in twins)]
    for module, apis in sorted(counterparts.items()):
        for api, kind in sorted(apis.items()):
            other = twin(api, module)
            if kind in (OWN, SHIM) and other not in inventory:
                found.append(f'{other}: counterparts says {kind}, and it is not in the inventory')
            elif kind not in (OWN, SHIM) and other in inventory:
                found.append(f'{other}: in the inventory, and counterparts says {module} has none on {api}')
    return found


def docs_problems(inventory: dict[str, Entry], counterparts: dict[str, dict[str, str]]) -> list[str]:
    """``docs: true`` sits only on a module that gets a page, and on every API's own module at a path."""
    found = []
    for module, entry in sorted(inventory.items()):
        api = module.split('.')[0]
        if not entry.docs:
            continue
        if api == 'couchbase' and (_in_common(module) or _in_protocol(module)):
            found.append(f'{module}: docs: true under common/ or protocol/; flag the module that publishes it')
        elif api in APIS and counterparts.get(f'couchbase{module[len(api):]}', {}).get(api) != OWN:
            found.append(f'{module}: docs: true on a module that is not own; its names have couchbase\'s page')
    for module, apis in sorted(counterparts.items()):
        for api, kind in sorted(apis.items()):
            other = twin(api, module)
            if kind == OWN and module in inventory and other in inventory and (
                    inventory[module].docs != inventory[other].docs):
                found.append(f'{other}: docs: true on one of it and {module} only; flag both or neither')
    return found


def documented(tree: Tree, inventory: dict[str, Entry]) -> set[tuple[str, str]]:
    """Each public name of a ``docs: true`` module, as the module that defines it and the name.

    A name the SDK does not define, as one the native binding serves, is given at the flagged module.
    """
    return {(tree.defined_in(module, name) or module, name)
            for module, entry in inventory.items() if entry.docs for name in entry.public}


def agreement_problems(module: str, other: str, shared: Iterable[str], entry: Entry) -> list[str]:
    """``other`` publishes every name it shares with ``module``, as the same object."""
    missing = [n for n in shared if n not in entry.public]
    found = [f'{other}: does not publish what it shares with {module}: {missing}'] if missing else []
    ours, theirs = importlib.import_module(module), importlib.import_module(other)
    found += [f'{other}.{n}: is not the object {module}.{n} is' for n in shared
              if n not in missing and binds(theirs, n) and getattr(theirs, n) is not getattr(ours, n)]
    return found


def twin_problems(tree: Tree, inventory: dict[str, Entry]) -> list[str]:
    """A ``couchbase`` module lists every public name of its ``common/`` twin that it binds."""
    found = []
    for module, entry in sorted(inventory.items()):
        common = f'{COMMON}{module[len("couchbase"):]}'
        if module.split('.')[0] != 'couchbase' or _in_common(module) or common not in inventory:
            continue
        bound, listed = tree.bindings(module), {*entry.public, *entry.compat}
        missing = [n for n in inventory[common].public
                   if n in bound and n not in listed and _in_common(tree.defined_in(module, n) or '')]
        if missing:
            found.append(f'{module}: binds {missing}, public on {common}, and does not list them')
    return found


def example_api(path: str) -> str:
    return next((p for p in pathlib.PurePosixPath(path).parts if p in APIS), 'couchbase')


def sdk_imports(source: str) -> Iterable[tuple[int, str, str | None]]:
    """Each SDK import as (line, module, name), the name ``None`` for ``import module``."""
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imported = [(alias.name, None) for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            imported = [(node.module, alias.name) for alias in node.names]
        else:
            continue
        yield from ((node.lineno, m, n) for m, n in imported if m.split('.')[0] in PACKAGES)


def example_problems(path: str, source: str, inventory: dict[str, Entry]) -> list[str]:
    """Every SDK import in one example names a public path, its own API's where that publishes it."""
    api = example_api(path)
    found = []
    for line, module, name in sdk_imports(source):
        where = f'{path}:{line}: {module if name is None else f"{module}.{name}"}'
        if name is not None and f'{module}.{name}' in inventory:
            module, name = f'{module}.{name}', None
        if module not in inventory:
            found.append(f'{where}: not a public module')
        elif name is not None and name not in inventory[module].public:
            found.append(f'{where}: not public at that path')
        elif api != 'couchbase' and module.split('.')[0] == 'couchbase':
            other = twin(api, module)
            if other in inventory and (name is None or name in inventory[other].public):
                found.append(f'{where}: the {api} examples import it from {other}')
    return found


def counterpart_problems(root: pathlib.Path, inventory: dict[str, Entry],
                         counterparts: dict[str, dict[str, str]]) -> list[str]:
    tree = Tree(root)
    found = coverage_problems(inventory, counterparts) + twin_problems(tree, inventory)
    found += docs_problems(inventory, counterparts)
    for module, apis in sorted(counterparts.items()):
        for api, kind in sorted(apis.items()):
            other = twin(api, module)
            if kind not in (OWN, SHIM) or module not in inventory or other not in inventory:
                continue
            found += agreement_problems(module, other, tree.shared(module, inventory[module]), inventory[other])
            if kind == SHIM:
                path = module_path(root, other)
                if path is None or path.read_text() != render_shim(tree, counterparts, api, module, inventory[other]):
                    found.append(f'{other}: is not what the template renders; run with --write')
    return found


def write_shims(root: pathlib.Path, inventory: dict[str, Entry], counterparts: dict[str, dict[str, str]]) -> int:
    tree = Tree(root)
    written = 0
    for module, apis in sorted(counterparts.items()):
        for api, kind in sorted(apis.items()):
            other = twin(api, module)
            if kind != SHIM or other not in inventory:
                continue
            source = module_path(root, module)
            assert source is not None
            path = root / api / source.relative_to(root / 'couchbase')
            rendered = render_shim(tree, counterparts, api, module, inventory[other])
            if not path.is_file() or path.read_text() != rendered:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(rendered)
                written += 1
    return written


def problems(root: pathlib.Path, inventory: dict[str, Entry]) -> list[str]:
    modules = public_modules(root)
    found = [f'{m}: a public module the inventory does not list; add it, or underscore it'
             for m in sorted(set(modules) - set(inventory))]
    found += [f'{m}: listed in the inventory and not a public module in the tree'
              for m in sorted(set(inventory) - set(modules))]
    for name in sorted(set(modules) & set(inventory)):
        found += module_problems(name, inventory[name])
    for name, path in sorted(modules.items()):
        if name == COMMON or name.startswith(f'{COMMON}.'):
            found += common_problems(name, path.read_text(), inventory.get(name))
    for name, path in sorted(common_modules(root).items()):
        found += import_problems(name, path)
    for path in _sources(root, EXAMPLES):
        found += example_problems(path, (root / path).read_text(), inventory)
    return found


def main(argv: list[str]) -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))
    inventory = read_inventory(INVENTORY)
    counterparts = read_counterparts(INVENTORY)
    if '--write' in argv:
        print(f'Wrote {write_shims(root, inventory, counterparts)} shim(s).')
        (root / DEPRECATED_PATHS).write_text(render_deprecated_paths(inventory))
    found = deprecated_paths_problems(root, inventory) + replacement_problems(inventory)
    found += problems(root, inventory) + counterpart_problems(root, inventory, counterparts)
    if not found:
        public = sum(len(e.public) for e in inventory.values())
        compat = sum(len(e.compat) for e in inventory.values())
        print(f'OK: {len(inventory)} public modules agree with {INVENTORY.name}, '
              f'{public} public name(s) and {compat} compat.')
        return 0
    print(f'FAIL: {len(found)} disagreement(s) with {INVENTORY.name}:', file=sys.stderr)
    for problem in found:
        print(f'    {problem}', file=sys.stderr)
    return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
