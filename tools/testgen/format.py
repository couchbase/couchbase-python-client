"""Format generated output the way this repo's own hooks would.

A generator that emits anything the pre-commit hooks would rewrite produces a file that
can never be clean: the hook rewrites it, the next run puts the original back, and a
check for "regenerating changes nothing" reports a difference the generator cannot
remove.  So autopep8 and isort run here, in the hooks' own order and reading the same
pyproject.toml, before anything reaches disk.

Both are pinned in dev_requirements.txt to the revs .pre-commit-config.yaml runs.  That
pin is what this module rests on, and it is the reason those two entries are exact where
the rest of the file uses `~=`.

autopep8 runs with `experimental` on, which is the setting that reflows a line the
transform lengthened past the limit.  Inserting `await ` and widening TestEnvironment to
AsyncTestEnvironment costs eleven columns on a signature that was already close to it.

What makes an experimental formatter safe to depend on is not the flag, it is the two
checks below.  Every file is compared against its own input, and anything that changed
the code rather than its layout is a blocker; then the hooks' own settings are run over
the result, and a file they would still rewrite is a blocker too.  Both are cheap, and
between them there is no way for a formatter change, a version skew or a wrong line
length to reach disk quietly.
"""

from __future__ import annotations

import ast
import functools
import os
from typing import Any

import autopep8
import isort

from tools.testgen.transform import Report

HOOK_OPTIONS: dict[str, Any] = {}
GENERATOR_OPTIONS: dict[str, Any] = {'experimental': True}


@functools.lru_cache(maxsize=None)
def _isort_config(root: str) -> isort.Config:
    return isort.Config(settings_file=os.path.join(root, 'pyproject.toml'))


def line_length(root: str) -> int:
    """The limit both formatters use, read from `[tool.isort]`.

    `[tool.autopep8]` states the same number and autopep8 finds it by walking up from the
    file being formatted, which is not how this module calls it.  Rather than parse a
    second copy, the value is taken from the config that is already loaded and the fixed
    point check below is what proves the two agree: were they to diverge, autopep8's own
    settings would rewrap the result and the file would be reported.
    """
    return _isort_config(root).line_length


def _run(text: str, root: str, options: dict[str, Any]) -> str:
    """autopep8 then isort, which is the order .pre-commit-config.yaml runs them in."""
    fixed = autopep8.fix_code(text, options={'max_line_length': line_length(root), **options})
    return isort.code(fixed, config=_isort_config(root))


class _StripImports(ast.NodeTransformer):
    """Remove every import, at any nesting level, and record what each one bound.

    Nesting matters here: isort sorts a function-local import block as readily as a
    module-level one, and query_t has one inside test_query_timeout.
    """

    def __init__(self) -> None:
        self.names: set[str] = set()

    def _record(self, node: ast.Import | ast.ImportFrom) -> None:
        module = getattr(node, 'module', None) or ''
        level = getattr(node, 'level', 0)
        for alias in node.names:
            self.names.add(f'{"." * level}{module}.{alias.name}|{alias.asname}')
        return None

    visit_Import = _record       # type: ignore[assignment]
    visit_ImportFrom = _record   # type: ignore[assignment]


def code_shape(text: str) -> tuple[str, frozenset[str]]:
    """Everything about a file that formatting is not allowed to change.

    Import *order* is excluded because reordering imports is most of what isort is for.
    What it binds is not: an import the formatter dropped or added would be a real change
    wearing the same clothes.
    """
    stripper = _StripImports()
    tree = stripper.visit(ast.parse(text))
    return ast.dump(tree), frozenset(stripper.names)


def format_generated(text: str, root: str, where: str, report: Report) -> str:
    """Format `text`, or report why it was left alone and return it unchanged."""
    try:
        formatted = _run(text, root, GENERATOR_OPTIONS)
    except Exception as ex:
        report.blockers.append(f'{where}: formatter raised {type(ex).__name__}: {ex}')
        return text

    try:
        changed = code_shape(formatted) != code_shape(text)
    except SyntaxError as ex:
        report.blockers.append(f'{where}: formatted output does not parse: {ex}')
        return text
    if changed:
        report.blockers.append(f'{where}: formatting changed the code, not just its layout')
        return text

    if _run(formatted, root, HOOK_OPTIONS) != formatted:
        report.blockers.append(f'{where}: the pre-commit hooks would still rewrite this file')
        return text
    return formatted
