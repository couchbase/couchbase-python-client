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

"""Hold every capability to one source of truth: declared means complete, undeclared means it raises.

A protocol may provide a feature or not, and which ones it provides it declares in its own
``couchbase/protocol/<name>/_capabilities.py`` as ``CAPABILITIES``.  The vocabulary is ``Capability``
in ``couchbase/common/_capabilities.py``, each capability's surface is a ``Supports...`` interface in
``couchbase/common/_interfaces/``, and a facade reaches that surface only through
``require(self._impl, Capability.X)``, which raises ``FeatureUnavailableException`` when the impl's
protocol did not declare ``X``.  Two halves are enforced elsewhere: mypy checks that a declared
capability is complete, and ``require()`` raises at runtime.  This check holds the pieces between them
together, reading source only, so nothing is imported:

1. **A facade reaches a capability only through** ``require()``.  ``self._impl.<member>``, where the
   member belongs to a capability interface of that facade's role, skips the raise, and the type
   checker does not notice while a facade's ``_impl`` is annotated with a classic impl class.
2. **Every capability is wired end to end.**  It has a ``require()`` overload, the overload names
   interfaces that exist, its subject interface reads ``capabilities`` and names a facade role in
   ``check_api_parity.FACADES``, its capability members are not also core members of that subject,
   and some facade requires it.
3. **Every declared capability is checked by mypy.**  Each protocol package declares ``CAPABILITIES``
   from the vocabulary, and for each declared capability its ``_capabilities.py`` assigns an impl to
   every interface that capability's overloads return, under ``TYPE_CHECKING``.  Without the
   assignment, declaring a capability makes it complete by assertion.

A subject interface names its role by convention: ``CollectionInterface`` and
``AsyncAttemptContextInterface`` are the ``Collection`` and ``AttemptContext`` roles, whose facades on
all three APIs are checked.  An impl reached through a local alias of ``self._impl`` is not followed.

Run directly, or via the ``check-capabilities`` pre-commit hook::

    python tools/check_capabilities.py           # check
    python tools/check_capabilities.py --list    # every require() site

Exits 1 on any finding.  Tests::

    python -m pytest tools/tests -q
"""

from __future__ import annotations

import ast
import pathlib
import sys
from collections.abc import Iterable, Iterator
from typing import NamedTuple

try:
    from tools.check_api_parity import FACADES
except ModuleNotFoundError:
    # Run as a script, so tools/ itself is first on sys.path.
    from check_api_parity import FACADES

ROOT = pathlib.Path(__file__).resolve().parent.parent
VOCABULARY = 'couchbase/common/_capabilities.py'
INTERFACES = 'couchbase/common/_interfaces'
PROTOCOLS = 'couchbase/protocol'


class Overload(NamedTuple):
    subject: str
    capability: str
    interface: str


class Site(NamedTuple):
    """A ``require(self._impl, Capability.<capability>)`` call in ``facade.method``."""
    path: str
    line: int
    facade: str
    method: str
    capability: str


class Declaration(NamedTuple):
    protocol: str
    path: str
    capabilities: frozenset[str]
    checked: frozenset[str]


def _parse(root: pathlib.Path, relative: str) -> ast.Module:
    return ast.parse((root / relative).read_text(), filename=relative)


def _name(node: ast.expr | None) -> str | None:
    """The name an annotation or expression spells, through a string annotation and a dotted access."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        node = ast.parse(node.value, mode='eval').body
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _capability(node: ast.expr) -> str | None:
    """``X`` from ``Capability.X``, and from ``Literal[Capability.X]``."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        node = ast.parse(node.value, mode='eval').body
    if isinstance(node, ast.Subscript) and _name(node.value) == 'Literal':
        node = node.slice
    if isinstance(node, ast.Attribute) and _name(node.value) == 'Capability':
        return node.attr
    return None


def vocabulary(root: pathlib.Path) -> tuple[list[str], list[Overload]]:
    """The ``Capability`` members, and one ``Overload`` per ``@overload`` of ``require``."""
    tree = _parse(root, VOCABULARY)
    members: list[str] = []
    overloads: list[Overload] = []
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == 'Capability':
            members += [t.id for stmt in node.body if isinstance(stmt, ast.Assign)
                        for t in stmt.targets if isinstance(t, ast.Name)]
        elif (isinstance(node, ast.FunctionDef) and node.name == 'require'
              and any(_name(d) == 'overload' for d in node.decorator_list)):
            subject, capability = node.args.args[0].annotation, node.args.args[1].annotation
            overloads.append(Overload(_name(subject) or '?', _capability(capability) if capability else '?',
                                      _name(node.returns) or '?'))
    return members, overloads


def interfaces(root: pathlib.Path) -> dict[str, set[str]]:
    """Every ``Protocol`` class under ``INTERFACES``, by name, with the names of the members it declares."""
    found: dict[str, set[str]] = {}
    for path in sorted((root / INTERFACES).glob('*.py')):
        for node in _parse(root, str(path.relative_to(root))).body:
            if isinstance(node, ast.ClassDef) and any(_name(b) == 'Protocol' for b in node.bases):
                found[node.name] = {stmt.name for stmt in node.body
                                    if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef))}
    return found


def role(subject: str) -> str:
    """The facade role a subject interface names: ``AsyncAttemptContextInterface`` is ``AttemptContext``."""
    name = subject.removesuffix('Interface')
    return name.removeprefix('Async') if name.startswith('Async') and name[5:6].isupper() else name


def facades_by_role(facades: Iterable[tuple[str, ...]]) -> dict[str, tuple[str, ...]]:
    return {triple[0].rpartition('.')[2]: tuple(triple) for triple in facades}


def _class_source(root: pathlib.Path, qualname: str) -> tuple[str, ast.ClassDef] | None:
    """The file defining ``qualname`` and its top-level class node, or None when either is absent."""
    module, _, name = qualname.rpartition('.')
    base = module.replace('.', '/')
    for relative in (f'{base}.py', f'{base}/__init__.py'):
        if (root / relative).is_file():
            node = next((n for n in _parse(root, relative).body if isinstance(n, ast.ClassDef) and n.name == name),
                        None)
            return (relative, node) if node else None
    return None


def _is_self_impl(node: ast.expr) -> bool:
    return (isinstance(node, ast.Attribute) and node.attr == '_impl'
            and isinstance(node.value, ast.Name) and node.value.id == 'self')


def _methods(cls: ast.ClassDef) -> Iterator[ast.FunctionDef | ast.AsyncFunctionDef]:
    return (n for n in cls.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)))


def scan_facade(root: pathlib.Path, qualname: str, members: set[str]) -> tuple[list[Site], list[str]]:
    """The ``require()`` sites in one facade class, and each direct ``self._impl.<member>`` reach of
    one of ``members``, as a finding."""
    found = _class_source(root, qualname)
    if found is None:
        return [], []
    path, cls = found
    sites: list[Site] = []
    problems: list[str] = []
    for method in _methods(cls):
        for node in ast.walk(method):
            if isinstance(node, ast.Attribute) and node.attr in members and _is_self_impl(node.value):
                problems.append(f'{path}:{node.lineno}: {cls.name}.{method.name} reaches capability member '
                                f'{node.attr!r} as self._impl.{node.attr}; go through require()')
            elif (isinstance(node, ast.Call) and _name(node.func) == 'require' and len(node.args) == 2
                  and _is_self_impl(node.args[0])):
                capability = _capability(node.args[1]) or '?'
                sites.append(Site(path, node.lineno, qualname, method.name, capability))
    return sites, problems


def declarations(root: pathlib.Path) -> tuple[list[Declaration], list[str]]:
    """Each protocol package's ``CAPABILITIES`` and the interfaces its ``TYPE_CHECKING`` assignments check."""
    found: list[Declaration] = []
    problems: list[str] = []
    for package in sorted(p for p in (root / PROTOCOLS).iterdir() if (p / '__init__.py').is_file()):
        relative = f'{PROTOCOLS}/{package.name}/_capabilities.py'
        if not (root / relative).is_file():
            problems.append(f'{relative}: protocol {package.name!r} declares no capabilities; add the module, '
                            'with an empty CAPABILITIES if it has none')
            continue
        tree = _parse(root, relative)
        declared: set[str] | None = None
        checked: set[str] = set()
        for node in ast.walk(tree):
            if (isinstance(node, ast.Assign) and any(_name(t) == 'CAPABILITIES' for t in node.targets)
                    and isinstance(node.value, ast.Call) and node.value.args):
                elements = node.value.args[0]
                items = elements.elts if isinstance(elements, (ast.Set, ast.List, ast.Tuple)) else []
                declared = {_capability(e) or '?' for e in items}
            elif isinstance(node, ast.If) and _name(node.test) == 'TYPE_CHECKING':
                checked |= {_name(s.annotation) or '?' for s in node.body
                            if isinstance(s, ast.AnnAssign) and s.value is not None}
        if declared is None:
            problems.append(f'{relative}: no CAPABILITIES = frozenset({{...}}) assignment')
            continue
        found.append(Declaration(package.name, relative, frozenset(declared), frozenset(checked)))
    return found, problems


def check_overloads(members: list[str], overloads: list[Overload], protocols: dict[str, set[str]],
                    roles: dict[str, tuple[str, ...]]) -> list[str]:
    """Findings on the vocabulary: each overload names interfaces that exist and a facade role, and each
    member has an overload."""
    problems: list[str] = []
    for o in overloads:
        where = f'{VOCABULARY}: require({o.subject}, Capability.{o.capability}) -> {o.interface}'
        if o.capability not in members:
            problems.append(f'{where}: Capability has no member {o.capability!r}')
        problems += [f'{where}: {i} is not a Protocol in {INTERFACES}' for i in (o.subject, o.interface)
                     if i not in protocols]
        if o.subject in protocols and 'capabilities' not in protocols[o.subject]:
            problems.append(f'{where}: {o.subject} does not declare the capabilities require() reads')
        if role(o.subject) not in roles:
            problems.append(f'{where}: {o.subject} names the role {role(o.subject)!r}, which is not a facade in '
                            'check_api_parity.FACADES')
        overlap = protocols.get(o.subject, set()) & protocols.get(o.interface, set())
        if overlap:
            problems.append(f'{where}: {", ".join(sorted(overlap))} is both core and capability')
    problems += [f'{VOCABULARY}: Capability.{m} has no require() overload' for m in members
                 if not any(o.capability == m for o in overloads)]
    return problems


def scan_facades(root: pathlib.Path, overloads: list[Overload], protocols: dict[str, set[str]],
                 roles: dict[str, tuple[str, ...]]) -> tuple[list[Site], list[str]]:
    """Every ``require()`` site on the facades, and the findings on how each facade reaches a capability."""
    capability_members: dict[str, set[str]] = {}
    for o in overloads:
        capability_members.setdefault(role(o.subject), set()).update(protocols.get(o.interface, set()))
    subjects = {o.capability: role(o.subject) for o in overloads}
    sites: list[Site] = []
    problems: list[str] = []
    for name, triple in sorted(roles.items()):
        for qualname in triple:
            found, reaches = scan_facade(root, qualname, capability_members.get(name, set()))
            sites += found
            problems += reaches
            problems += [f'{s.path}:{s.line}: {name}.{s.method} requires Capability.{s.capability}, which is not '
                         f'a capability of the {name} role' for s in found if subjects.get(s.capability) != name]
    return sites, problems


def check_declarations(root: pathlib.Path, members: list[str], overloads: list[Overload]) -> list[str]:
    """Findings on each protocol's declaration: every capability it declares exists and is checked by mypy."""
    found, problems = declarations(root)
    for declaration in found:
        for capability in sorted(declaration.capabilities):
            if capability not in members:
                problems.append(f'{declaration.path}: declares Capability.{capability}, which does not exist')
                continue
            problems += [f'{declaration.path}: declares Capability.{capability} and assigns no impl to '
                         f'{o.interface} under TYPE_CHECKING, so mypy never checks it is complete'
                         for o in overloads if o.capability == capability and o.interface not in declaration.checked]
    return problems


def check(root: pathlib.Path, facades: Iterable[tuple[str, ...]]) -> tuple[list[Site], list[str]]:
    """Every ``require()`` site on the facades, and every finding."""
    members, overloads = vocabulary(root)
    protocols = interfaces(root)
    roles = facades_by_role(facades)
    problems = check_overloads(members, overloads, protocols, roles)
    sites, reaches = scan_facades(root, overloads, protocols, roles)
    problems += reaches
    problems += [f'{VOCABULARY}: Capability.{m} is required by no facade; remove it or reach it through require()'
                 for m in members if not any(s.capability == m for s in sites)]
    problems += check_declarations(root, members, overloads)
    return sites, problems


def main(argv: list[str]) -> int:
    sites, problems = check(ROOT, FACADES)
    if '--list' in argv:
        for site in sites:
            print(f'{site.path}:{site.line}: {site.facade}.{site.method} requires Capability.{site.capability}')
        return 0
    if problems:
        print(f'FAIL: {len(problems)} capability finding(s):', file=sys.stderr)
        for problem in problems:
            print(f'    {problem}', file=sys.stderr)
        return 1
    members, _ = vocabulary(ROOT)
    found_declarations, _ = declarations(ROOT)
    print(f'OK: {len(members)} capabilities, reached at {len(sites)} require() site(s) and declared by '
          f'{len(found_declarations)} protocol(s).')
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
