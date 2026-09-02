"""Generate one API variant of a sync suite, and write the tree.

The steps are ordered by what each one needs to see.  A sidecar is merged before the
import prune, so the prune reads the merged tests and keeps the imports they use; the
banner goes on after the prune, so the prune never has to reason about it.
"""

from __future__ import annotations

import ast
import importlib
import os
from collections.abc import Callable
from dataclasses import dataclass, field

from tools.testgen.apply import add_header, apply_edits
from tools.testgen.format import format_generated, line_length
from tools.testgen.resolver import Resolver
from tools.testgen.suites import ConfigError, SuiteTable
from tools.testgen.transform import (AsyncTransformer,
                                     Report,
                                     SuiteTransformer,
                                     TwistedTransformer)

ImportStmt = ast.Import | ast.ImportFrom
Method = ast.FunctionDef | ast.AsyncFunctionDef

TRANSFORMERS = {
    'acouchbase': AsyncTransformer,
    'txcouchbase': TwistedTransformer,
}


def generate(source: str, api: str, root: str, resolver: Resolver,
             report: Report, sidecar: str | None = None) -> str | None:
    src = open(os.path.join(root, source)).read()
    tr = TRANSFORMERS[api](src, source, resolver, report)
    tr.run()
    out = apply_edits(src, tr.edits)
    if not _parses(out, f'{source} -> {api}: generated file does not parse', report):
        return None

    out = _add_missing_imports(out, tr)

    # Sidecar before the import prune, so the prune sees what the merged tests actually
    # use and keeps those imports.
    if sidecar:
        out = merge_sidecar(out, sidecar, root, report)
        out = merge_sidecar_imports(out, sidecar, root)
        if not _parses(out, f'{source} -> {api}: sidecar merge broke the file', report):
            return None

    out = _prune_unused_imports(out)

    # The header goes on before the formatters rather than after, so what they are checked
    # against is the file as written.  A banner added afterwards would be the one part of
    # the output nothing had verified.
    out = add_header(out, source)
    where = f'{api}/{os.path.basename(source)}'
    out = format_generated(out, root, where, report)

    limit = line_length(root)
    for i, line in enumerate(out.splitlines(), 1):
        if len(line) > limit:
            report.long_lines.append(f'{where}:{i}: {len(line)} cols')

    check_imports(out, source, api, report)
    return out


def _parses(text: str, complaint: str, report: Report) -> bool:
    """True if `text` is still valid Python; otherwise `complaint` becomes a blocker."""
    try:
        ast.parse(text)
    except SyntaxError as ex:
        report.blockers.append(f'{complaint}: {ex}')
        return False
    return True


def _add_missing_imports(text: str, tr: SuiteTransformer) -> str:
    """Import what the rewritten suite now needs and the sync source never did."""
    if tr.needs_pytest_asyncio and 'import pytest_asyncio' not in text:
        text = text.replace('import pytest\n', 'import pytest\nimport pytest_asyncio\n', 1)
    if tr.needs_async_generator and 'import AsyncGenerator' not in text:
        # Add AsyncGenerator, keep Generator.  bucketmgmt_t has both a class-scoped
        # `drop_bucket` fixture that stays synchronous and returns Generator[None, None,
        # None], and an async cb_env fixture, so replacing the import rather than extending
        # it leaves `Generator` undefined at the first one.
        text = text.replace('from collections.abc import ',
                            'from collections.abc import AsyncGenerator, ', 1)
    if getattr(tr, 'needs_rit', False):
        text = _add_import_name(text, 'tests_v2.environments.twisted_environment',
                                'run_in_reactor_thread')
    return '\n'.join(_dedupe_abc_import(ln) for ln in text.splitlines()) + '\n'


def merge_sidecar(text: str, sidecar_rel: str, root: str, report: Report) -> str:
    """Merge a hand-written sidecar's methods into the generated classes of the same name.

    The sidecar is the mirror image of `testgen_exclude`.  Exclusion removes a test the
    sync suite has and an api cannot use; the sidecar adds a test an api needs and the sync
    suite cannot express.  Both converge on `TEST_MANIFEST`, which is why they have to be
    kept in step: `validate_test_manifest` compares the manifest against the concrete class
    with `symmetric_difference`, so adding a method without adding its name fails the class
    at setup just as loudly as the reverse.

    The sidecar class name *is* the merge target.  Naming only the file is not enough:
    query_t.py has two suite classes, and these two tests belong to exactly one of them.

    Nothing here is transformed.  The sidecar is already asynchronous and hand-written, so
    it is spliced in verbatim; running it through the sync-to-async transform would be
    meaningless for a test whose entire subject is asyncio.
    """
    return _SidecarMerge(text, sidecar_rel, root, report).merged()


class _SidecarMerge:
    """One sidecar file merged into one generated file.

    A class rather than a chain of calls because every step below needs the same four
    things: both files' lines, the sidecar's path for the messages, and the report.
    """

    def __init__(self, text: str, sidecar_rel: str, root: str, report: Report) -> None:
        side_src = open(os.path.join(root, sidecar_rel)).read()
        self.side = ast.parse(side_src)
        self.side_lines = side_src.splitlines()
        self.lines = text.splitlines()
        self.gen_classes = {c.name: c for c in ast.parse(text).body
                            if isinstance(c, ast.ClassDef)}
        self.rel = sidecar_rel
        self.report = report

    def merged(self) -> str:
        # (insert-at line, text) edits, applied bottom-up so earlier line numbers stay
        # valid.  Same discipline as the transformer's edit list.
        inserts: list[tuple[int, list[str]]] = []
        for cls in self.side.body:
            if isinstance(cls, ast.ClassDef):
                inserts.extend(self._class_inserts(cls))
        for at, body in sorted(inserts, key=lambda x: -x[0]):
            self.lines[at:at] = body
        return '\n'.join(self.lines) + '\n'

    def _class_inserts(self, cls: ast.ClassDef) -> list[tuple[int, list[str]]]:
        """Everything one sidecar class contributes, or nothing if it cannot be merged."""
        target = self.gen_classes.get(cls.name)
        if target is None:
            self.report.blockers.append(
                f'{self.rel}: no generated class named {cls.name} to merge into')
            return []

        methods = [n for n in cls.body
                   if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                   and n.name.startswith('test_')]
        refusal = _sidecar_refusal(cls, target, methods)
        if refusal is not None:
            self.report.blockers.append(f'{self.rel}:{cls.name}: {refusal}')
            return []

        # Append after the class's last statement rather than at its textual end, so a
        # trailing comment or blank line in the generated file stays where it is.
        last = target.body[-1]
        inserts = [((last.end_lineno or last.lineno), self._method_lines(methods))]
        inserts.extend(self._manifest_inserts(target, methods))
        return inserts

    def _method_lines(self, methods: list[Method]) -> list[str]:
        """The sidecar's own text for each method, decorators included."""
        body: list[str] = []
        for m in methods:
            start = min([d.lineno for d in m.decorator_list] + [m.lineno]) - 1
            body.append('')
            body.extend(self.side_lines[start:(m.end_lineno or m.lineno)])
        return body

    def _manifest_inserts(self, target: ast.ClassDef,
                          methods: list[Method]) -> list[tuple[int, list[str]]]:
        """Where each new name goes in the target's TEST_MANIFEST.

        Sorted position rather than file order, which is the manifests' house style.
        """
        inserts: list[tuple[int, list[str]]] = []
        for s in target.body:
            if not _is_manifest(s) or not isinstance(s.value, ast.List) or not s.value.elts:
                continue
            existing = [(e.value, e.lineno) for e in s.value.elts if isinstance(e, ast.Constant)]
            first = self.lines[existing[0][1] - 1]
            indent = ' ' * (len(first) - len(first.lstrip()))
            for m in methods:
                after = [ln for val, ln in existing if val < m.name]
                at = max(after) if after else existing[0][1] - 1
                inserts.append((at, [f"{indent}'{m.name}',"]))
            self.report.notes.append(
                f'{self.rel}: merged {len(methods)} test(s) into {target.name} '
                f'({len(existing)} -> {len(existing) + len(methods)} in TEST_MANIFEST)')
        return inserts


def _is_manifest(stmt: ast.stmt) -> bool:
    return isinstance(stmt, ast.Assign) and any(
        isinstance(t, ast.Name) and t.id == 'TEST_MANIFEST' for t in stmt.targets)


def _declared_manifest(cls: ast.ClassDef) -> set[str] | None:
    """The names a class's own TEST_MANIFEST lists, or None if it declares none."""
    declared = None
    for s in cls.body:
        if _is_manifest(s):
            declared = {e.value for e in s.value.elts if isinstance(e, ast.Constant)}
    return declared


def _sidecar_refusal(cls: ast.ClassDef, target: ast.ClassDef,
                     methods: list[Method]) -> str | None:
    """Why this sidecar class cannot be merged, or None.

    A sidecar adds tests and never redefines generated ones.  Python accepts the collision
    silently, keeping whichever definition came last in the merged class body, so the
    sidecar would quietly replace a generated test with the manifest count still adding up.
    Nothing downstream can see that, so it is checked here.

    The second refusal is cheaper and catches a different mistake: a sidecar whose own
    TEST_MANIFEST disagrees with what the file defines, which is what a method added and
    never listed looks like.
    """
    names = {m.name for m in methods}
    clash = names & {n.name for n in target.body
                     if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    if clash:
        return (f'would silently redefine generated test(s) {sorted(clash)}; '
                f'rename, or fix the sync source instead')

    declared = _declared_manifest(cls)
    if declared is not None and declared != names:
        return f'TEST_MANIFEST does not match its own methods: {sorted(declared ^ names)}'
    return None


def merge_sidecar_imports(text: str, sidecar_rel: str, root: str) -> str:
    """Add the sidecar's module-level imports that the generated file does not already have.

    Only the names are merged, not the statements: the generated file already imports
    pytest and the async environment, and re-importing them would be an F811 redefinition.
    Anything merged but unneeded is removed again by _prune_unused_imports downstream.
    """
    side = ast.parse(open(os.path.join(root, sidecar_rel)).read())
    gen = ast.parse(text)

    def bound(mod: ast.Module) -> set[str]:
        out = set()
        for n in mod.body:
            if isinstance(n, (ast.Import, ast.ImportFrom)):
                out |= {(a.asname or a.name).split('.')[0] for a in n.names}
        return out

    have = bound(gen)
    new: list[str] = []
    for n in side.body:
        if not isinstance(n, (ast.Import, ast.ImportFrom)):
            continue
        if isinstance(n, ast.ImportFrom) and n.module == '__future__':
            continue
        missing = [a for a in n.names if (a.asname or a.name).split('.')[0] not in have]
        if not missing:
            continue
        names = ', '.join(a.name + (f' as {a.asname}' if a.asname else '') for a in missing)
        if isinstance(n, ast.ImportFrom):
            new.append(f'from {"." * n.level}{n.module or ""} import {names}')
        else:
            new.append(f'import {names}')

    if not new:
        return text
    lines = text.splitlines()
    last_import = max((n.end_lineno or n.lineno) for n in gen.body
                      if isinstance(n, (ast.Import, ast.ImportFrom)))
    lines[last_import:last_import] = new
    return '\n'.join(lines) + '\n'


def _prune_unused_imports(text: str) -> str:
    """Drop module-level imports the transform left unreferenced.

    Two separate causes, one rule.  Every fixture returning Generator[...] becomes an async
    generator, so `Generator` goes unused in most suites; and testgen_exclude removes whole
    methods, which orphans every import only those methods used: dropping query_t's two
    thread tests leaves threading, Callable, Any, Cluster, QueryResult and Scope behind.
    autopep8 will not remove an unused import, correctly, since it cannot know the import
    is not there for its side effect.  The generator created these orphans, so the generator cleans them
    up; this is not the formatting job that belongs to pre-commit.

    Only module-level imports are touched.  A function-local import is left alone: query_t
    imports Cluster inside test_query_timeout, and that one is load-bearing.
    """
    tree = ast.parse(text)
    imported = _module_imports(tree)
    used = _referenced_names(tree, _LocalImports(tree))
    if all(name in used for name in imported):
        return text
    return _without_unused(text, imported, used)


def _module_imports(tree: ast.Module) -> dict[str, ImportStmt]:
    """Every name a module-level import binds, mapped to the statement binding it."""
    imported: dict[str, ImportStmt] = {}
    for node in tree.body:
        if not isinstance(node, (ast.Import, ast.ImportFrom)):
            continue
        if isinstance(node, ast.ImportFrom) and node.module == '__future__':
            continue
        for alias in node.names:
            imported[(alias.asname or alias.name).split('.')[0]] = node
    return imported


class _LocalImports:
    """Which references a function-local import supplies, rather than a module-level one.

    query_t is the case: Cluster is imported at module level, referenced only inside
    test_query_timeout, and that test imports Cluster itself.  Counting that reference
    against the module-level import keeps a name the local import already supplies, and
    pyflakes then reports both an unused import and a redefinition.
    """

    def __init__(self, tree: ast.Module) -> None:
        self.parent: dict[int, ast.AST] = {}
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                self.parent[id(child)] = node
        self.module_level = {id(n) for n in tree.body}

    def supplies(self, node: ast.AST, name: str) -> bool:
        cur = self.parent.get(id(node))
        while cur is not None:
            if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef)) and self._binds(cur, name):
                return True
            cur = self.parent.get(id(cur))
        return False

    def _binds(self, func: ast.AST, name: str) -> bool:
        for sub in ast.walk(func):
            if not isinstance(sub, (ast.Import, ast.ImportFrom)):
                continue
            if id(sub) in self.module_level:
                continue
            if any((a.asname or a.name).split('.')[0] == name for a in sub.names):
                return True
        return False


def _referenced_names(tree: ast.Module, local: _LocalImports) -> set[str]:
    """Every name the module still refers to, minus those a local import supplies.

    An attribute's root is a Name, so it is collected with the rest.  A name appearing only
    inside a string annotation counts too: over-counting keeps an import that is not
    needed, under-counting emits a file that raises NameError.
    """
    used: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and not local.supplies(node, node.id):
            used.add(node.id)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            used.add(node.value.split('[')[0].split('.')[0].strip("'\" "))
    return used


def _without_unused(text: str, imported: dict[str, ImportStmt], used: set[str]) -> str:
    """Rewrite `text` with every unused name gone from the statement that imports it.

    Keyed by start line, and spans are skipped wholesale: a source import may still be
    parenthesised across several lines, and rebuilding only its first line would leave the
    continuation lines behind as a syntax error.
    """
    starts = {n.lineno: n for n in imported.values()}
    lines = text.splitlines()
    out: list[str] = []
    removed = 0
    i = 1
    while i <= len(lines):
        node = starts.get(i)
        if node is None:
            out.append(lines[i - 1])
            i += 1
            continue
        end = node.end_lineno or node.lineno
        keep = [a for a in node.names if (a.asname or a.name).split('.')[0] in used]
        if len(keep) == len(node.names):
            out.extend(lines[i - 1:end])
        else:
            removed += 1
            if keep:
                out.append(_import_stmt(node, keep))
        i = end + 1
    return '\n'.join(out) + '\n' if removed else text


def _import_stmt(node: ImportStmt, keep: list[ast.alias]) -> str:
    names = ', '.join(a.name + (f' as {a.asname}' if a.asname else '') for a in keep)
    if isinstance(node, ast.ImportFrom):
        return f'from {"." * node.level}{node.module or ""} import {names}'
    return f'import {names}'


def _add_import_name(text: str, module: str, name: str) -> str:
    """Make sure `name` is imported from `module`, extending an existing statement if there
    already is one.

    Extending rather than appending matters: the generated txcouchbase suites already
    import several names from twisted_environment, and a second `from ... import` of the
    same module is an isort violation and reads as a mistake.  Only the module-level
    statements are considered, so a function-local import of the same module is left alone.
    """
    tree = ast.parse(text)
    lines = text.splitlines()
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom) or node.module != module:
            continue
        if any((a.asname or a.name) == name for a in node.names):
            return text
        names = sorted({a.name + (f' as {a.asname}' if a.asname else '')
                        for a in node.names} | {name})
        end = (node.end_lineno or node.lineno)
        lines[node.lineno - 1:end] = [f'from {module} import ' + ', '.join(names)]
        return '\n'.join(lines) + '\n'

    last = max((n.end_lineno or n.lineno) for n in tree.body
               if isinstance(n, (ast.Import, ast.ImportFrom)))
    lines[last:last] = [f'from {module} import {name}']
    return '\n'.join(lines) + '\n'


def _dedupe_abc_import(line: str) -> str:
    """`from collections.abc import AsyncGenerator, AsyncGenerator, Callable` is legal but
    silly.  isort collapses it downstream; emitting it in the first place is still wrong."""
    if not line.startswith('from collections.abc import '):
        return line
    names, seen = [], set()
    for n in line[len('from collections.abc import '):].split(', '):
        if n not in seen:
            seen.add(n)
            names.append(n)
    return 'from collections.abc import ' + ', '.join(sorted(names))


def check_imports(text: str, source: str, api: str, report: Report) -> None:
    """Resolve every `from X import a, b` in the generated file against the real package.

    Cheap, and it catches the failure mode a parse check cannot: a file that is valid
    Python, imports a module that exists, and names something that module does not export.
    That is what the environment-module rewrite produced on the first run.
    """
    tree = ast.parse(text)
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom) or node.module is None:
            continue
        try:
            mod = importlib.import_module(node.module)
        except Exception as ex:
            report.blockers.append(f'{api}/{os.path.basename(source)}:{node.lineno}: '
                                   f'cannot import module `{node.module}`: {ex}')
            continue
        for alias in node.names:
            if alias.name != '*' and not hasattr(mod, alias.name):
                report.blockers.append(
                    f'{api}/{os.path.basename(source)}:{node.lineno}: '
                    f'`{node.module}` has no attribute `{alias.name}`')


@dataclass
class RunResult:
    """Everything one pass over the suite table produced."""

    report: Report
    resolvers: dict[str, Resolver] = field(default_factory=dict)
    files: dict[tuple[str, str], str] = field(default_factory=dict)
    bridged: dict[str, int] = field(default_factory=dict)


def run(table: SuiteTable, root: str, apis: tuple[str, ...] | None = None,
        progress: Callable[[str, str], None] | None = None) -> RunResult:
    """Generate every suite in `table`, in memory.

    Each api gets its own Resolver.  They hold different class models and accumulate their
    own misses, so one shared between them would answer acouchbase questions about
    txcouchbase code.

    `progress` is called with the api and source about to be generated.  This loop is where
    every command spends its time, and a caller that says nothing until it ends is
    indistinguishable from one that has hung.
    """
    unknown = sorted(set(table.apis) - set(TRANSFORMERS))
    if unknown:
        raise ConfigError(f'no transformer for api {", ".join(unknown)}; '
                          f'this generator knows {", ".join(sorted(TRANSFORMERS))}')
    result = RunResult(report=Report(),
                       resolvers={api: Resolver(api) for api in table.apis})
    for suite in table.suites:
        for api in suite.targets:
            if apis is not None and api not in apis:
                continue
            if progress is not None:
                progress(api, suite.source)
            out = generate(suite.source, api, root, result.resolvers[api], result.report,
                           sidecar=suite.sidecar_for(api))
            if out is None:
                continue
            result.files[(api, suite.source)] = out
            # "Bridged" rather than "awaited": acouchbase prefixes `await `, txcouchbase
            # wraps the call in run_in_reactor_thread().  Both are the same decision.
            result.bridged[f'{api}/{os.path.basename(suite.source)}'] = (
                out.count('await ') + out.count('run_in_reactor_thread('))
    return result


PACKAGE_MARKER = """\
# !! AUTO-GENERATED by tools/testgen: DO NOT EDIT MANUALLY !!
# Regenerate with: python -m tools.testgen generate --write
#
# Present so that pytest can tell this tree's modules from the other api's.  Both trees
# generate a views_t.py; without a package to qualify them, importing the second raises
# `import file mismatch` and every module after it fails to collect.
"""


def expected_tree(result: RunResult, table: SuiteTable,
                  apis: tuple[str, ...]) -> dict[str, str]:
    """What the output directories should hold, keyed by path relative to the root."""
    tree = {}
    for api in apis:
        tree[os.path.join(table.output_dir(api), '__init__.py')] = PACKAGE_MARKER
    for (api, source), text in result.files.items():
        if api in apis:
            tree[os.path.join(table.output_dir(api), os.path.basename(source))] = text
    return tree


def check(result: RunResult, table: SuiteTable, root: str,
          apis: tuple[str, ...]) -> list[tuple[str, str]]:
    """Compare the output directories on disk against what `run` produced.

    Three ways they differ.  Two are the obvious ones, a file whose content has moved on
    from its source and a file that was never written.  The third is what a diff of the
    generated files alone cannot see: a file the generator no longer writes stays on disk
    and keeps being collected, against a source that may not exist any more.  Dropping a
    suite from the table is exactly the change that leaves one.
    """
    expected = expected_tree(result, table, apis)
    found = set()
    for api in apis:
        dest_dir = os.path.join(root, table.output_dir(api))
        if not os.path.isdir(dest_dir):
            continue
        for name in os.listdir(dest_dir):
            if name.endswith('.py'):
                found.add(os.path.join(table.output_dir(api), name))

    out = []
    for path, text in expected.items():
        if path not in found:
            out.append(('missing', path))
        elif open(os.path.join(root, path)).read() != text:
            out.append(('stale', path))
    out.extend(('orphaned', path) for path in found - set(expected))
    return sorted(out, key=lambda d: (d[1], d[0]))


def write(result: RunResult, table: SuiteTable, root: str) -> list[str]:
    """Write what `run` produced, and return the paths written, relative to `root`."""
    written = []
    for (api, source), text in sorted(result.files.items()):
        dest_dir = os.path.join(root, table.output_dir(api))
        os.makedirs(dest_dir, exist_ok=True)
        marker = os.path.join(dest_dir, '__init__.py')
        if not os.path.exists(marker):
            with open(marker, 'w') as fh:
                fh.write(PACKAGE_MARKER)
            written.append(os.path.relpath(marker, root))
        dest = os.path.join(dest_dir, os.path.basename(source))
        with open(dest, 'w') as fh:
            fh.write(text)
        written.append(os.path.relpath(dest, root))
    return written
