"""Load and validate the suite table in config/testgen.yaml.

Validation is the point rather than parsing, and the mistake it exists to catch is the quiet
one: `target:` for `targets:` reads as "no override", so the suite generates for an API that
cannot run it and nothing reports it.  Every check here turns that class of mistake into a
message naming the entry.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Mapping

import yaml

SUITE_KEYS = frozenset({'source', 'targets', 'sidecar'})
TOP_LEVEL_KEYS = frozenset({'source_dir', 'output', 'defaults', 'suites', 'not_generated'})


class ConfigError(Exception):
    """Raised for any malformed or unsatisfiable configuration."""


@dataclass(frozen=True)
class Suite:
    """One sync source and the APIs it generates for."""

    source: str
    targets: tuple[str, ...]
    sidecars: Mapping[str, str]

    def sidecar_for(self, api: str) -> str | None:
        return self.sidecars.get(api)


@dataclass(frozen=True)
class SuiteTable:
    """The whole suite table, plus where each API's output goes."""

    source_dir: str
    output: Mapping[str, str]
    suites: tuple[Suite, ...]
    not_generated: Mapping[str, tuple[str, ...]]

    @property
    def apis(self) -> tuple[str, ...]:
        return tuple(sorted(self.output))

    def suites_for(self, api: str) -> tuple[Suite, ...]:
        return tuple(s for s in self.suites if api in s.targets)

    def output_dir(self, api: str) -> str:
        return self.output[api]


def load(path: str, root: str) -> SuiteTable:
    """Read `path` and return a validated Config.

    `root` is the repository root every path in the file is relative to.  Paths are checked
    against it here rather than at generation time so a stale entry is reported once, up
    front, instead of as a missing-file traceback partway through a run.
    """
    with open(path) as fh:
        raw = yaml.safe_load(fh)
    if not isinstance(raw, dict):
        raise ConfigError(f'{path}: expected a mapping at the top level')
    _reject_unknown(raw, TOP_LEVEL_KEYS, path, 'top level')

    source_dir = _read_source_dir(raw, path)
    output = _read_output(raw, path)
    defaults = _read_defaults(raw, path, output)
    suites = tuple(_read_suite(entry, i, path, output, defaults)
                   for i, entry in enumerate(_read_list(raw, 'suites', path)))
    not_generated = _read_not_generated(raw, path)
    _reject_duplicate_sources(suites, path)
    _check_paths(suites, not_generated, root, path)
    _check_coverage(suites, not_generated, source_dir, root, path)
    return SuiteTable(source_dir=source_dir, output=output, suites=suites,
                      not_generated=not_generated)


def _reject_unknown(mapping: Mapping[str, Any], allowed: frozenset[str],
                    path: str, where: str) -> None:
    unknown = sorted(set(mapping) - allowed)
    if unknown:
        raise ConfigError(f'{path}: unknown key(s) at {where}: {", ".join(unknown)}; '
                          f'expected one of {", ".join(sorted(allowed))}')


def _read_list(raw: Mapping[str, Any], key: str, path: str) -> list[Any]:
    value = raw.get(key)
    if not isinstance(value, list) or not value:
        raise ConfigError(f'{path}: `{key}` must be a non-empty list')
    return value


def _read_source_dir(raw: Mapping[str, Any], path: str) -> str:
    source_dir = raw.get('source_dir')
    if not isinstance(source_dir, str) or not source_dir:
        raise ConfigError(f'{path}: `source_dir` is required and must be a string')
    return source_dir


def _read_not_generated(raw: Mapping[str, Any], path: str) -> Mapping[str, tuple[str, ...]]:
    section = raw.get('not_generated') or {}
    if not isinstance(section, dict):
        raise ConfigError(f'{path}: `not_generated` must be a mapping of reason to sources')
    reasons: dict[str, tuple[str, ...]] = {}
    for reason, sources in section.items():
        if not isinstance(sources, list) or not sources:
            raise ConfigError(f'{path}: `not_generated.{reason}` must be a non-empty list')
        if any(not isinstance(src, str) for src in sources):
            raise ConfigError(f'{path}: `not_generated.{reason}` entries must be strings')
        reasons[str(reason)] = tuple(sources)
    return reasons


def _read_output(raw: Mapping[str, Any], path: str) -> Mapping[str, str]:
    output = raw.get('output')
    if not isinstance(output, dict) or not output:
        raise ConfigError(f'{path}: `output` must be a non-empty mapping of api to directory')
    for api, dest in output.items():
        if not isinstance(dest, str):
            raise ConfigError(f'{path}: output[{api}] must be a string')
    return dict(output)


def _read_defaults(raw: Mapping[str, Any], path: str,
                   output: Mapping[str, str]) -> tuple[str, ...]:
    defaults = raw.get('defaults') or {}
    if not isinstance(defaults, dict):
        raise ConfigError(f'{path}: `defaults` must be a mapping')
    _reject_unknown(defaults, frozenset({'targets'}), path, '`defaults`')
    targets = defaults.get('targets', list(output))
    return _validate_targets(targets, output, path, '`defaults`')


def _validate_targets(targets: Any, output: Mapping[str, str],
                      path: str, where: str) -> tuple[str, ...]:
    if not isinstance(targets, list) or not targets:
        raise ConfigError(f'{path}: {where}: `targets` must be a non-empty list')
    unknown = sorted(set(targets) - set(output))
    if unknown:
        raise ConfigError(f'{path}: {where}: no `output` entry for {", ".join(unknown)}')
    return tuple(targets)


def _read_suite(entry: Any, index: int, path: str, output: Mapping[str, str],
                defaults: tuple[str, ...]) -> Suite:
    where = f'suites[{index}]'
    if not isinstance(entry, dict):
        raise ConfigError(f'{path}: {where} must be a mapping')
    _reject_unknown(entry, SUITE_KEYS, path, where)

    source = entry.get('source')
    if not isinstance(source, str) or not source:
        raise ConfigError(f'{path}: {where}: `source` is required and must be a string')

    targets = (_validate_targets(entry['targets'], output, path, f'{where} ({source})')
               if 'targets' in entry else defaults)
    return Suite(source=source, targets=targets,
                 sidecars=_read_sidecars(entry, source, path, where, targets))


def _read_sidecars(entry: Mapping[str, Any], source: str, path: str, where: str,
                   targets: tuple[str, ...]) -> Mapping[str, str]:
    sidecar = entry.get('sidecar') or {}
    if not isinstance(sidecar, dict):
        raise ConfigError(f'{path}: {where} ({source}): `sidecar` must be a mapping of api to file')
    for api, rel in sidecar.items():
        if not isinstance(rel, str):
            raise ConfigError(f'{path}: {where} ({source}): sidecar[{api}] must be a string')
        # A sidecar is merged into a generated file, so naming an api this suite does not
        # generate for produces nothing and reports nothing.  Catch it here instead.
        if api not in targets:
            raise ConfigError(f'{path}: {where} ({source}): sidecar names {api}, which is '
                              f'not one of its targets ({", ".join(targets)})')
    return dict(sidecar)


def _reject_duplicate_sources(suites: tuple[Suite, ...], path: str) -> None:
    seen: set[str] = set()
    for suite in suites:
        if suite.source in seen:
            raise ConfigError(f'{path}: `{suite.source}` is listed more than once')
        seen.add(suite.source)


def _check_paths(suites: tuple[Suite, ...], not_generated: Mapping[str, tuple[str, ...]],
                 root: str, path: str) -> None:
    for suite in suites:
        if not os.path.exists(os.path.join(root, suite.source)):
            raise ConfigError(f'{path}: source not found: {suite.source}')
        for api, rel in suite.sidecars.items():
            if not os.path.exists(os.path.join(root, rel)):
                raise ConfigError(f'{path}: sidecar not found for {api}: {rel}')
    for reason, sources in not_generated.items():
        for src in sources:
            if not os.path.exists(os.path.join(root, src)):
                raise ConfigError(f'{path}: `not_generated.{reason}`: source not found: {src}')


def _check_coverage(suites: tuple[Suite, ...], not_generated: Mapping[str, tuple[str, ...]],
                    source_dir: str, root: str, path: str) -> None:
    """Fail unless every test module under `source_dir` is accounted for exactly once.

    Without this a suite added to the tree and forgotten here is simply not generated, and
    nothing says so.  That is the same silence the key checks above exist to break, one level
    up: the entry is not malformed, it is absent.
    """
    listed = {suite.source: '`suites`' for suite in suites}
    for reason, sources in not_generated.items():
        for src in sources:
            if src in listed:
                raise ConfigError(f'{path}: `{src}` is in both {listed[src]} and '
                                  f'`not_generated.{reason}`')
            listed[src] = f'`not_generated.{reason}`'
    prefix = source_dir.rstrip('/') + '/'
    stray = sorted(src for src in listed if not src.startswith(prefix))
    if stray:
        raise ConfigError(f'{path}: outside `source_dir` ({source_dir}): {", ".join(stray)}')
    _check_complete(set(listed), source_dir, root, path)


def _check_complete(listed: set[str], source_dir: str, root: str, path: str) -> None:
    scanned = os.path.join(root, source_dir)
    if not os.path.isdir(scanned):
        raise ConfigError(f'{path}: `source_dir` not found: {source_dir}')
    on_disk = {f'{source_dir}/{name}' for name in os.listdir(scanned) if name.endswith('_t.py')}
    missing = sorted(on_disk - listed)
    if missing:
        raise ConfigError(f'{path}: in neither `suites` nor `not_generated`: '
                          f'{", ".join(missing)}; add each to one of them')
