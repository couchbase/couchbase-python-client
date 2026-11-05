"""Give autodoc the names the SDK imports only for type checkers, and link the ones it cannot.

The SDK's modules import much of what their annotations name under ``if TYPE_CHECKING:``, so at
runtime the annotations are strings naming objects the module does not hold.  Autodoc cannot evaluate
them, and a signature then shows each name as written, linking nowhere.  Before autodoc reads a module,
this runs the module's ``TYPE_CHECKING`` imports in its namespace, adding only names the module does not
already define, so the annotations evaluate to the objects they name.

A name that still has no target is resolved once more, when it is one of these:

* a name docstrings write unqualified, in ``ALIASES``, such as ``timedelta``;
* data that an annotation names as a class, such as ``typing_extensions.Unpack``;
* a keyword-argument ``TypedDict``, such as ``GetOptionsKwargs``, which links to the options class
  of the same name, ``GetOptions``, whose page describes the same keys.

Sphinx marks every name in an annotation with ``refspecific=False``, and searches for any node carrying
the attribute as though it were true, by suffix.  So ``type[GetResult]`` linked ``type`` to whichever
documented attribute is called ``type``.  The attribute is removed from annotations here, so a name
resolves as written.
"""

from __future__ import annotations

import ast
import importlib
import inspect
import sys
from collections.abc import Iterator
from types import ModuleType
from typing import Any

from docutils import nodes
from sphinx.addnodes import pending_xref
from sphinx.ext import intersphinx

PACKAGES = ('couchbase', 'acouchbase', 'txcouchbase')
ALIASES = {
    'Deferred': 'twisted.internet.defer.Deferred',
    'asyncio.events.AbstractEventLoop': 'asyncio.AbstractEventLoop',
    'couchbase.common.options.ClusterKwargs': 'couchbase.options.ClusterOptions',
    'couchbase.common.options.WaitUntilReadyKwargs': 'couchbase.options.WaitUntilReadyOptions',
    'timedelta': 'datetime.timedelta',
}


def _guarded_imports(module: ModuleType) -> list[ast.Import | ast.ImportFrom]:
    try:
        tree = ast.parse(inspect.getsource(module))
    except (OSError, TypeError):
        return []
    found: list[ast.Import | ast.ImportFrom] = []
    for node in tree.body:
        if isinstance(node, ast.If) and ast.unparse(node.test) in ('TYPE_CHECKING', 'typing.TYPE_CHECKING'):
            found += [n for n in node.body if isinstance(n, (ast.Import, ast.ImportFrom))]
    return found


def _bindings(node: ast.Import | ast.ImportFrom, package: str | None) -> Iterator[tuple[str, Any]]:
    """The names an import statement binds, and what it binds them to."""
    if isinstance(node, ast.Import):
        for alias in node.names:
            if alias.asname:
                yield alias.asname, importlib.import_module(alias.name)
            else:
                top = alias.name.partition('.')[0]
                yield top, importlib.import_module(top)
        return
    source = importlib.import_module('.' * node.level + (node.module or ''), package)
    for alias in node.names:
        value = getattr(source, alias.name, None)
        if value is None:
            value = importlib.import_module(f'{source.__name__}.{alias.name}')
        yield alias.asname or alias.name, value


def resolve(module: ModuleType) -> None:
    for node in _guarded_imports(module):
        try:
            bound = list(_bindings(node, module.__package__))
        except ImportError:
            continue
        for name, value in bound:
            module.__dict__.setdefault(name, value)


def _before_signature(app: Any, obj: Any, bound_method: bool) -> None:
    module = sys.modules.get(getattr(obj, '__module__', None) or '')
    if module is not None and module.__name__.split('.')[0] in PACKAGES and module not in _done:
        _done.add(module)
        resolve(module)


_done: set[ModuleType] = set()


def _missing_reference(app: Any, env: Any, node: pending_xref, contnode: nodes.TextElement) -> nodes.Node | None:
    if node.get('refdomain') != 'py':
        return None
    target = node['reftarget']
    target = ALIASES.get(target, target)
    if target.endswith('Kwargs'):
        target = target[:-len('Kwargs')]
    retry = node.deepcopy()
    retry['reftarget'] = target
    retry['reftype'] = 'obj'
    domain = env.get_domain('py')
    resolved = domain.resolve_xref(env, node['refdoc'], app.builder, 'obj', target, retry, contnode)
    return resolved or intersphinx.missing_reference(app, env, retry, contnode)


def _exact_annotations(app: Any, doctree: nodes.document) -> None:
    for node in doctree.findall(pending_xref):
        if node.get('refdomain') == 'py' and node.hasattr('refspecific') and not node['refspecific']:
            del node['refspecific']


def setup(app: Any) -> dict[str, Any]:
    app.connect('autodoc-before-process-signature', _before_signature)
    app.connect('doctree-read', _exact_annotations)
    # After intersphinx, which resolves what it can as written.
    app.connect('missing-reference', _missing_reference, priority=900)
    return {'parallel_read_safe': True}
