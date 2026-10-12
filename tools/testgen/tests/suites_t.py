from pathlib import Path

import pytest

from tools.testgen.suites import ConfigError, load

VALID = '''
source_dir: src

output:
  acouchbase: acouchbase/tests/_generated
  txcouchbase: txcouchbase/tests/_generated

defaults:
  targets: [acouchbase, txcouchbase]

suites:
  - source: src/both_t.py
  - source: src/async_only_t.py
    targets: [acouchbase]
  - source: src/with_sidecar_t.py
    sidecar:
      acouchbase: side/extra_t.py

not_generated:
  cluster_free:
    - src/no_cluster_t.py
'''


def _tree(tmp_path: Path, text: str) -> tuple[str, str]:
    """Write `text` as a config beside a fake repo holding every path it names."""
    for rel in ('src/both_t.py', 'src/async_only_t.py', 'src/with_sidecar_t.py',
                'src/no_cluster_t.py', 'side/extra_t.py'):
        target = tmp_path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text('')
    cfg = tmp_path / 'testgen.yaml'
    cfg.write_text(text)
    return str(cfg), str(tmp_path)


def test_loads_targets_output_and_sidecars(tmp_path: Path) -> None:
    cfg = load(*_tree(tmp_path, VALID))

    assert cfg.apis == ('acouchbase', 'txcouchbase')
    assert cfg.output_dir('acouchbase') == 'acouchbase/tests/_generated'
    both = [s.source for s in cfg.suites_for('txcouchbase')]
    assert both == ['src/both_t.py', 'src/with_sidecar_t.py']
    sidecar_suite = cfg.suites[2]
    assert sidecar_suite.sidecar_for('acouchbase') == 'side/extra_t.py'
    assert sidecar_suite.sidecar_for('txcouchbase') is None


def test_omitted_targets_fall_back_to_the_declared_default(tmp_path: Path) -> None:
    cfg = load(*_tree(tmp_path, VALID))
    assert cfg.suites[0].targets == ('acouchbase', 'txcouchbase')


def test_a_misspelled_key_is_an_error_rather_than_a_silent_default(tmp_path: Path) -> None:
    """The reason this file is validated at all.

    `target:` for `targets:` reads as "no override", so the suite generates for an api that
    cannot run it and nothing says so.
    """
    text = VALID.replace('    targets: [acouchbase]', '    target: [acouchbase]')
    with pytest.raises(ConfigError, match='unknown key'):
        load(*_tree(tmp_path, text))


def test_an_unknown_target_names_the_api(tmp_path: Path) -> None:
    text = VALID.replace('targets: [acouchbase]', 'targets: [twisted]')
    with pytest.raises(ConfigError, match='no `output` entry for twisted'):
        load(*_tree(tmp_path, text))


def test_a_sidecar_for_an_untargeted_api_is_an_error(tmp_path: Path) -> None:
    """A sidecar merges into a generated file, so one for an api with no generated file is
    silently inert."""
    text = VALID.replace('      acouchbase: side/extra_t.py',
                         '      acouchbase: side/extra_t.py\n      nosuch: side/extra_t.py')
    with pytest.raises(ConfigError, match='not one of its targets'):
        load(*_tree(tmp_path, text))


def test_a_duplicate_source_is_an_error(tmp_path: Path) -> None:
    text = VALID.replace('  - source: src/with_sidecar_t.py',
                         '  - source: src/both_t.py\n  - source: src/with_sidecar_t.py')
    with pytest.raises(ConfigError, match='listed more than once'):
        load(*_tree(tmp_path, text))


def test_a_missing_source_is_reported_up_front(tmp_path: Path) -> None:
    text = VALID.replace('src/both_t.py', 'src/moved_away_t.py')
    with pytest.raises(ConfigError, match='source not found'):
        load(*_tree(tmp_path, text))


def test_a_missing_sidecar_is_reported_up_front(tmp_path: Path) -> None:
    text = VALID.replace('side/extra_t.py', 'side/gone_t.py')
    with pytest.raises(ConfigError, match='sidecar not found'):
        load(*_tree(tmp_path, text))


def test_a_source_in_neither_list_is_reported(tmp_path: Path) -> None:
    """The check that catches a suite added to the tree and never wired up here.

    Nothing else would say so.  The entry is not malformed, it is absent, so the generator
    produces nothing for that suite and reports nothing about it.
    """
    cfg, root = _tree(tmp_path, VALID)
    (Path(root) / 'src' / 'forgotten_t.py').write_text('')
    with pytest.raises(ConfigError, match='in neither `suites` nor `not_generated`'):
        load(cfg, root)


def test_a_source_in_both_lists_is_reported(tmp_path: Path) -> None:
    text = VALID.replace('    - src/no_cluster_t.py',
                         '    - src/no_cluster_t.py\n    - src/both_t.py')
    with pytest.raises(ConfigError, match='is in both'):
        load(*_tree(tmp_path, text))


def test_a_missing_not_generated_source_is_reported_up_front(tmp_path: Path) -> None:
    text = VALID.replace('src/no_cluster_t.py', 'src/vanished_t.py')
    with pytest.raises(ConfigError, match='source not found'):
        load(*_tree(tmp_path, text))


def test_not_generated_keeps_the_reason_it_was_filed_under(tmp_path: Path) -> None:
    cfg = load(*_tree(tmp_path, VALID))
    assert cfg.not_generated == {'cluster_free': ('src/no_cluster_t.py',)}
