import importlib.machinery
import pathlib

import pytest

from tools.check_binding_free_imports import (BASELINE,
                                              Failure,
                                              compare,
                                              entry_into_classic,
                                              import_failure,
                                              is_checked,
                                              main,
                                              measure,
                                              module_name,
                                              read_baseline,
                                              render_baseline)


@pytest.mark.parametrize('path, expected', [
    ('couchbase/common/result.py', True),
    ('couchbase/__init__.py', True),
    ('couchbase/protocol/__init__.py', True),
    ('acouchbase/management/buckets.py', True),
    ('txcouchbase/cluster.py', True),
    ('couchbase/protocol/classic/_cluster_impl.py', False),
    ('acouchbase/protocol/classic/_collection_impl.py', False),
    ('couchbase/protocol/classic/_pycbc_core/__init__.py', False),
    ('couchbase/tests/binding_load_t.py', False),
    ('couchbase/tests/connection_t.py', False),
    ('acouchbase/tests/_generated/kv_t.py', False),
    ('tools/check_binding_free_imports.py', False),
    ('couchbase/common/README.md', False),
])
def test_is_checked(path: str, expected: bool) -> None:
    assert is_checked(path) is expected


def test_module_name() -> None:
    assert module_name('couchbase/common/result.py') == 'couchbase.common.result'
    assert module_name('acouchbase/management/__init__.py') == 'acouchbase.management'


def _failure(*missing: str) -> Failure:
    return {'error': 'ImportError: no', 'frames': [], 'missing': list(missing)}


def test_a_new_module_that_needs_the_binding_fails() -> None:
    problems = compare(set(), {'couchbase.result': _failure('couchbase.protocol.classic._pycbc_core._core')})
    assert len(problems) == 1
    assert problems[0].startswith('couchbase.result: cannot be imported without the native extension')


def test_a_listed_module_that_imports_fails_until_it_is_removed() -> None:
    problems = compare({'couchbase.cluster'}, {})
    assert problems == ['couchbase.cluster: imports without the native extension; '
                        'run with --update to remove it from the baseline']


def test_a_module_that_fails_with_nothing_refused_cannot_be_checked() -> None:
    problems = compare({'couchbase.otel_tracer'}, {'couchbase.otel_tracer': _failure()})
    assert problems[0].startswith('couchbase.otel_tracer: cannot be checked')
    assert len(problems) == 2


def test_the_listed_modules_failing_is_clean() -> None:
    assert compare({'couchbase.cluster'}, {'couchbase.cluster': _failure('x')}) == []


def test_the_baseline_round_trips(tmp_path: pathlib.Path) -> None:
    path = tmp_path / 'baseline.txt'
    path.write_text(render_baseline(['txcouchbase.scope', 'couchbase.cluster']))
    assert read_baseline(path) == {'couchbase.cluster', 'txcouchbase.scope'}


def test_entry_into_classic_names_the_last_frame_before_classic(tmp_path: pathlib.Path) -> None:
    frames = ['/usr/lib/python3.10/importlib/__init__.py:126',
              f'{tmp_path}/couchbase/cluster.py:30',
              f'{tmp_path}/couchbase/collection.py:46',
              f'{tmp_path}/couchbase/protocol/classic/_collection_impl.py:20',
              f'{tmp_path}/couchbase/protocol/classic/_pycbc_core/__init__.py:54']
    assert entry_into_classic(tmp_path, frames) == 'couchbase/collection.py:46'


@pytest.fixture
def fake_tree(tmp_path: pathlib.Path) -> pathlib.Path:
    """A ``couchbase`` package holding a file the import system takes for an extension module.  The
    finder refuses it by its loader, so it is never loaded and need not be one."""
    pkg = tmp_path / 'couchbase'
    pkg.mkdir()
    (pkg / '__init__.py').write_text('')
    (pkg / f'_ext{importlib.machinery.EXTENSION_SUFFIXES[0]}').write_bytes(b'not an extension')
    (pkg / 'plain.py').write_text('import json\n')
    (pkg / 'eager.py').write_text('from couchbase import _ext\n')
    (pkg / 'guarded.py').write_text('try:\n    from couchbase import _ext\nexcept ImportError:\n    _ext = None\n')
    (pkg / 'lacking.py').write_text('import not_a_dependency_anyone_has\n')
    return tmp_path


def test_a_module_that_loads_an_extension_is_refused(fake_tree: pathlib.Path) -> None:
    failure = import_failure(fake_tree, 'couchbase.eager')
    assert failure is not None
    assert failure['missing'] == ['couchbase._ext']


def test_a_module_that_degrades_without_the_extension_imports(fake_tree: pathlib.Path) -> None:
    assert import_failure(fake_tree, 'couchbase.guarded') is None


def test_an_extension_that_was_never_built_is_missing(fake_tree: pathlib.Path) -> None:
    (fake_tree / 'couchbase' / 'absent.py').write_text('import couchbase._never_built\n')
    failure = import_failure(fake_tree, 'couchbase.absent')
    assert failure is not None
    assert failure['missing'] == ['couchbase._never_built']


def test_measure_reports_only_the_modules_that_fail(fake_tree: pathlib.Path) -> None:
    failures = measure(fake_tree, ['couchbase.plain', 'couchbase.eager', 'couchbase.guarded', 'couchbase.lacking'])
    assert sorted(failures) == ['couchbase.eager', 'couchbase.lacking']
    assert failures['couchbase.lacking']['missing'] == []
    assert 'not_a_dependency_anyone_has' in failures['couchbase.lacking']['error']


def test_the_tree_matches_the_baseline() -> None:
    assert BASELINE.exists()
    assert main([]) == 0
