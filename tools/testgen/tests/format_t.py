from pathlib import Path

from tools.testgen.format import code_shape, format_generated
from tools.testgen.transform import Report

PYPROJECT = '''
[tool.isort]
multi_line_output = 1
force_grid_wrap = 3
use_parentheses = true
ensure_newline_before_comments = true
line_length = 120
order_by_type = true

[tool.autopep8]
max_line_length = 120
'''


def _root(tmp_path: Path) -> str:
    """A directory holding just the formatter config, which is all this module reads."""
    (tmp_path / 'pyproject.toml').write_text(PYPROJECT)
    return str(tmp_path)


def test_a_long_import_is_wrapped_and_the_result_is_a_fixed_point(tmp_path: Path) -> None:
    """The whole reason the post-pass exists.

    A file the hooks would rewrite can never be clean: the hook rewrites it, the next run
    puts the original back, and nothing can settle the difference.
    """
    names = ', '.join(f'Name{i}' for i in range(20))
    report = Report()
    out = format_generated(f'from somewhere.deep import {names}\n', _root(tmp_path),
                           'acouchbase/x_t.py', report)

    assert report.blockers == []
    assert max(len(ln) for ln in out.splitlines()) <= 120
    assert format_generated(out, _root(tmp_path), 'acouchbase/x_t.py', report) == out


def test_reordering_imports_is_not_a_code_change() -> None:
    """Reordering imports is most of what isort is for, so the check has to allow it."""
    assert code_shape('import b\nimport a\nx = 1\n') == code_shape('import a\nimport b\nx = 1\n')


def test_dropping_an_import_is_a_code_change() -> None:
    assert code_shape('import a\nx = 1\n') != code_shape('x = 1\n')


def test_a_function_local_import_counts_too() -> None:
    """isort sorts a function-local import block as readily as a module-level one, so the
    check has to see inside functions or it would miss a change made there."""
    outer = 'def f():\n    import a\n    return a\n'
    assert code_shape(outer) != code_shape('def f():\n    import b\n    return b\n')


def test_a_formatter_that_rewrites_code_is_reported_and_ignored(tmp_path: Path,
                                                                monkeypatch) -> None:
    """What makes an experimental formatter safe to depend on.

    A reformat that changed the code rather than its layout has to lose to the unformatted
    file, because the generated tests are the thing being protected.
    """
    import tools.testgen.format as fmt
    monkeypatch.setattr(fmt, '_run', lambda text, root, options: 'x = 2\n')

    report = Report()
    src = 'x = 1\n'
    assert format_generated(src, _root(tmp_path), 'acouchbase/x_t.py', report) == src
    assert report.blockers == ['acouchbase/x_t.py: formatting changed the code, '
                               'not just its layout']


def test_output_the_hooks_would_still_rewrite_is_reported(tmp_path: Path,
                                                          monkeypatch) -> None:
    """The check that catches a wrong line length or a version skew, whatever caused it."""
    import tools.testgen.format as fmt
    monkeypatch.setattr(fmt, '_run',
                        lambda text, root, options: text if options else 'x = 1  # rewritten\n')

    report = Report()
    src = 'x = 1\n'
    assert format_generated(src, _root(tmp_path), 'acouchbase/x_t.py', report) == src
    assert report.blockers == ['acouchbase/x_t.py: the pre-commit hooks would still '
                               'rewrite this file']
