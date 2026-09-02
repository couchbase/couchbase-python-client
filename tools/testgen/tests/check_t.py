from pathlib import Path

from tools.testgen.generate import (PACKAGE_MARKER,
                                    RunResult,
                                    check)
from tools.testgen.suites import SuiteTable, load
from tools.testgen.transform import Report

CONFIG = '''
source_dir: src

output:
  acouchbase: acouchbase/out
  txcouchbase: txcouchbase/out

defaults:
  targets: [acouchbase, txcouchbase]

suites:
  - source: src/both_t.py

not_generated: {}
'''

BOTH = 'src/both_t.py'
APIS = ('acouchbase', 'txcouchbase')


def _tree(tmp_path: Path) -> tuple[SuiteTable, str, RunResult]:
    """A repo whose two output directories already hold what the generator would write."""
    (tmp_path / 'src').mkdir()
    (tmp_path / BOTH).write_text('')
    (tmp_path / 'testgen.yaml').write_text(CONFIG)
    table = load(str(tmp_path / 'testgen.yaml'), str(tmp_path))

    result = RunResult(report=Report(),
                       files={(api, BOTH): f'# {api}\n' for api in APIS})
    for api in APIS:
        out = tmp_path / table.output_dir(api)
        out.mkdir(parents=True)
        (out / '__init__.py').write_text(PACKAGE_MARKER)
        (out / 'both_t.py').write_text(f'# {api}\n')
    return table, str(tmp_path), result


def test_a_tree_that_matches_its_sources_reports_nothing(tmp_path: Path) -> None:
    table, root, result = _tree(tmp_path)
    assert check(result, table, root, APIS) == []


def test_a_hand_edited_file_is_stale(tmp_path: Path) -> None:
    table, root, result = _tree(tmp_path)
    (tmp_path / 'acouchbase/out/both_t.py').write_text('# edited\n')

    assert check(result, table, root, APIS) == [('stale', 'acouchbase/out/both_t.py')]


def test_a_file_the_generator_writes_and_the_tree_lacks_is_missing(tmp_path: Path) -> None:
    table, root, result = _tree(tmp_path)
    (tmp_path / 'txcouchbase/out/both_t.py').unlink()

    assert check(result, table, root, APIS) == [('missing', 'txcouchbase/out/both_t.py')]


def test_a_file_no_source_produces_is_orphaned(tmp_path: Path) -> None:
    """The one a diff of the generated files cannot find.

    Moving a suite to `not_generated` leaves its output where it was, and regenerating
    never removes it: pytest goes on collecting a file with no source behind it.
    """
    table, root, result = _tree(tmp_path)
    (tmp_path / 'acouchbase/out/dropped_t.py').write_text('# once generated\n')

    assert check(result, table, root, APIS) == [('orphaned', 'acouchbase/out/dropped_t.py')]


def test_the_package_marker_is_expected_and_is_compared(tmp_path: Path) -> None:
    """It is generated too, and without it the second tree's modules fail to import."""
    table, root, result = _tree(tmp_path)
    (tmp_path / 'txcouchbase/out/__init__.py').write_text('')

    assert check(result, table, root, APIS) == [('stale', 'txcouchbase/out/__init__.py')]


def test_limiting_to_one_api_does_not_orphan_the_other_tree(tmp_path: Path) -> None:
    """`run` with `--api` fills `files` for that api alone, so scanning both output
    directories would report every file of the other one as having no source."""
    table, root, _ = _tree(tmp_path)
    one = RunResult(report=Report(), files={('acouchbase', BOTH): '# acouchbase\n'})

    assert check(one, table, root, ('acouchbase',)) == []
