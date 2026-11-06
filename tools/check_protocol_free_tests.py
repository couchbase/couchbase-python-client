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

"""Fail when a shared test reaches into the classic protocol.

A test that runs on every protocol has to pass on couchbase2 without being edited, so it may not
depend on how classic is built.  Four kinds of site depend on it:

``classic``
    An import of a ``protocol.classic`` module, or a string naming one, as a patch target does.
``core``
    The same for ``pycbc_core``, the classic binding.
``private``
    An attribute read of ``_request``, ``_executor``, ``_stream``, ``_impl`` or ``_connection`` on
    anything but ``self``: a result's executor and stream, or an API object's implementation and
    connection, are what classic happens to put there.
``helper``
    An import of a test module that imports classic or ``pycbc_core`` itself, so an import is not
    hidden by moving it into a helper.  A helper's private reaches are not followed: each is a
    site in the helper already.

A site is exempt when it is in a test declared classic-only, by a module ``pytestmark`` or a
class's ``unsupported_protocol`` mark or ``UNSUPPORTED_PROTOCOLS`` naming ``COUCHBASE2``.  The
classic adapter's own unit tests are that, and stay.

The protocol views under ``tests/protocols/`` are not read either: each module there is one
protocol's answer to what the shared environments ask of a cluster, and reaching into that
protocol is its job.  The package itself is the way in.  An import of one protocol's module from
outside it is a ``helper`` site, so a test cannot name a protocol by importing its view.

Generated suites are not read: each is a copy of a ``couchbase/tests`` suite, which is.

Run directly, or via the ``check-protocol-free-tests`` pre-commit hook::

    python tools/check_protocol_free_tests.py

Prints every site by file and line, and exits 1 when there is one.  Tests::

    python -m pytest tools/tests -q
"""

from __future__ import annotations

import ast
import pathlib
import re
import subprocess
import sys
from collections import Counter
from collections.abc import Iterable, Iterator

TREES = ('tests', 'couchbase/tests', 'acouchbase/tests', 'txcouchbase/tests')
VIEWS = 'tests/protocols/'

PRIVATE = frozenset({'_request', '_executor', '_stream', '_impl', '_connection'})
_CLASSIC = re.compile(r'^(a|tx)?couchbase(\.\w+)*\.protocol\.classic(\.|$)')
_CORE = re.compile(r'^(a|tx)?couchbase(\.\w+)*\._?pycbc_core(\.|$)')
_DOTTED = re.compile(r'\w+(\.\w+)*')


def module_name(path: str) -> str:
    return path.removesuffix('.py').removesuffix('/__init__').replace('/', '.')


def _named_kind(name: str) -> str | None:
    if not _DOTTED.fullmatch(name):
        return None
    # The binding sits inside protocol.classic, so it has to be told apart first.
    if _CORE.match(name):
        return 'core'
    if _CLASSIC.match(name):
        return 'classic'
    return None


def _imported(node: ast.Import | ast.ImportFrom) -> list[str]:
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names]
    if node.module is None or node.level:
        return []
    # `from tests import a_helper` names the module in the alias, not in `module`.
    return [node.module] + [f'{node.module}.{alias.name}' for alias in node.names]


def _names_couchbase2(node: ast.expr) -> bool:
    return any(isinstance(n, ast.Attribute) and n.attr == 'COUCHBASE2' for n in ast.walk(node))


def _is_classic_only_mark(node: ast.expr) -> bool:
    return (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr == 'unsupported_protocol' and _names_couchbase2(node))


def _marks(value: ast.expr) -> list[ast.expr]:
    return list(value.elts) if isinstance(value, (ast.List, ast.Tuple)) else [value]


def is_classic_only_module(tree: ast.Module) -> bool:
    for stmt in tree.body:
        if (isinstance(stmt, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'pytestmark'
                                                 for t in stmt.targets)):
            return any(_is_classic_only_mark(m) for m in _marks(stmt.value))
    return False


def is_classic_only_class(node: ast.ClassDef) -> bool:
    if any(_is_classic_only_mark(d) for d in node.decorator_list):
        return True
    for stmt in node.body:
        target = (stmt.targets[0] if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1
                  else stmt.target if isinstance(stmt, ast.AnnAssign) else None)
        if (isinstance(target, ast.Name) and target.id in ('UNSUPPORTED_PROTOCOLS', 'pytestmark')
                and stmt.value is not None):
            if target.id == 'UNSUPPORTED_PROTOCOLS' and _names_couchbase2(stmt.value):
                return True
            if target.id == 'pytestmark' and any(_is_classic_only_mark(m) for m in _marks(stmt.value)):
                return True
    return False


def _shared_nodes(tree: ast.Module) -> Iterator[ast.AST]:
    """Every node outside the classes declared classic-only."""
    stack: list[ast.AST] = [tree]
    while stack:
        node = stack.pop()
        if isinstance(node, ast.ClassDef) and is_classic_only_class(node):
            continue
        yield node
        stack.extend(ast.iter_child_nodes(node))


def located_sites(source: str) -> list[tuple[str, int]]:
    """The classic, core and private sites in one module as (kind, line), outside what it declares
    classic-only."""
    tree = ast.parse(source)
    if is_classic_only_module(tree):
        return []
    sites: list[tuple[str, int]] = []
    for node in _shared_nodes(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            kinds = {_named_kind(name) for name in _imported(node)}
            sites.extend((k, node.lineno) for k in sorted(k for k in kinds if k))
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            kind = _named_kind(node.value)
            if kind:
                sites.append((kind, node.lineno))
        elif (isinstance(node, ast.Attribute) and node.attr in PRIVATE
              and not (isinstance(node.value, ast.Name) and node.value.id == 'self')):
            sites.append(('private', node.lineno))
    return sorted(sites, key=lambda site: site[1])


def own_sites(source: str) -> Counter[str]:
    return Counter(kind for kind, _ in located_sites(source))


def helper_lines(source: str, flagged: Iterable[str]) -> list[int]:
    """The lines importing a module in ``flagged``, outside what is classic-only."""
    tree = ast.parse(source)
    if is_classic_only_module(tree):
        return []
    flagged = set(flagged)
    return sorted(node.lineno for node in _shared_nodes(tree) if isinstance(node, (ast.Import, ast.ImportFrom))
                  and any(name in flagged for name in _imported(node)))


def is_protocol_view(path: str) -> bool:
    return path.startswith(VIEWS)


def is_read(path: str) -> bool:
    return path.endswith('.py') and '_generated' not in pathlib.PurePosixPath(path).parts


def files_read(root: pathlib.Path) -> list[str]:
    # Untracked files are included so a new suite is checked before it is staged.
    out = subprocess.run(['git', 'ls-files', '--cached', '--others', '--exclude-standard', '--', *TREES],
                         cwd=root, check=True, capture_output=True, text=True).stdout
    return sorted(p for p in set(out.splitlines()) if is_read(p) and (root / p).is_file())


def locate(sources: dict[str, str]) -> dict[str, list[tuple[str, int]]]:
    """Every site in ``sources`` as (kind, line), by path."""
    found = {path: [] if is_protocol_view(path) else located_sites(source) for path, source in sources.items()}
    flagged = [module_name(path) for path, sites in found.items()
               if any(kind in ('classic', 'core') for kind, _ in sites)]
    flagged += [module_name(path) for path in sources
                if is_protocol_view(path) and module_name(path) != module_name(VIEWS + '__init__.py')]
    for path, source in sources.items():
        if is_protocol_view(path):
            continue
        lines = helper_lines(source, [m for m in flagged if m != module_name(path)])
        found[path] = sorted(found[path] + [('helper', n) for n in lines], key=lambda site: site[1])
    return {path: sites for path, sites in found.items() if sites}


def main() -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    sources = {p: (root / p).read_text() for p in files_read(root)}
    found = locate(sources)
    if not found:
        print('OK: no shared test reaches into the classic protocol.')
        return 0
    print('FAIL: keep these tests protocol-neutral or declare them classic-only:', file=sys.stderr)
    for path, sites in found.items():
        lines = sources[path].splitlines()
        for kind, n in sites:
            print(f'    {path}:{n}: {kind:<7} {lines[n - 1].strip()}', file=sys.stderr)
    return 1


if __name__ == '__main__':
    sys.exit(main())
