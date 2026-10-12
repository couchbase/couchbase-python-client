# No __init__.py beside this file.  It would name the package `tests`, which the repo
# already has at the top level, and this module would be imported from there instead.
import pathlib

import pytest

from tools.audit_type_ignores import (MissingRoots,
                                      collect,
                                      violations)


def _tree(base: pathlib.Path) -> pathlib.Path:
    """A root holding one suppression, and a generated copy of it."""
    root = base / 'couchbase' / 'tests'
    (root / '_generated').mkdir(parents=True)
    (root / 'bucket_t.py').write_text('x = 1  # type: ignore[assignment]  # PYCBC-1877\n')
    (root / '_generated' / 'bucket_t.py').write_text(
        'x = 1  # type: ignore[assignment]  # PYCBC-1877\n')
    return root


def test_a_tree_that_does_not_exist_stops_the_audit(tmp_path: pathlib.Path) -> None:
    """The failure the cutover rename produces, and the one the exit code cannot carry.

    Walking past the tree audits the ones that are left and exits 0, which is also what a
    conforming tree does.  The suppressions in it stop being checked and the run that
    stopped checking them is the one that reports success.
    """
    present = _tree(tmp_path)
    renamed = tmp_path / 'couchbase' / 'tests_v3'

    with pytest.raises(MissingRoots) as caught:
        collect((str(present), str(renamed)))

    assert caught.value.missing == [str(renamed)]


def test_the_trees_that_are_there_are_audited(tmp_path: pathlib.Path) -> None:
    """That a root exists is a precondition and not a filter: nothing else narrows."""
    root = _tree(tmp_path)

    sites = collect((str(root),))

    assert [(s.line, s.codes, s.key) for s in sites] == [(1, ('assignment',), 'PYCBC-1877')]
    assert '_generated' not in sites[0].path


def test_a_list_of_codes_is_specific_and_each_code_needs_its_own_reason(
        tmp_path: pathlib.Path) -> None:
    """mypy accepts `[a, b]`, and reading only the first code would call it bare."""
    root = tmp_path / 'couchbase' / 'tests'
    root.mkdir(parents=True)
    (root / 'x_t.py').write_text('def f(self) -> None:  # type: ignore[override, arg-type]\n'
                                 'def g(self) -> None:  # type: ignore[override,arg-type]  # PYCBC-1877\n'
                                 'def h(self) -> None:  # type: ignore[override]\n')

    sites = collect((str(root),))

    assert [s.codes for s in sites] == [('override', 'arg-type')] * 2 + [('override',)]
    assert [site.line for site, _ in violations(sites)] == [1]
