"""Command-line entry point: python -m tools.testgen <command>."""

from __future__ import annotations

import os
import sys

import click

from tools.testgen.console import (console,
                                   plain_table,
                                   rule)
from tools.testgen.generate import (check,
                                    expected_tree,
                                    run,
                                    write)
from tools.testgen.report import print_report
from tools.testgen.suites import (ConfigError,
                                  SuiteTable,
                                  load)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), 'config', 'testgen.yaml')

_config_option = click.option(
    '--config', 'config_path', default=DEFAULT_CONFIG, show_default=False,
    type=click.Path(exists=True, dir_okay=False),
    help='Suite table to read (default: tools/testgen/config/testgen.yaml).')

_api_option = click.option(
    '--api', 'api', default=None, help='Limit to this api.')


@click.group()
@click.version_option(version='1.0.0', prog_name='testgen')
def cli() -> None:
    """Generate the acouchbase and txcouchbase test suites from their sync source.

    \b
    # Validate the suite table and show what it would generate
    python -m tools.testgen config
    # Report what the transform can and cannot resolve, writing nothing
    python -m tools.testgen report
    # Regenerate both trees
    python -m tools.testgen generate --write
    # Fail if the trees on disk have fallen behind their sources
    python -m tools.testgen check
    """


@cli.command(name='config')
@_config_option
@_api_option
def show_table(config_path: str, api: str | None) -> None:
    """Validate the suite table and print what each api generates."""
    cfg = _load(config_path)
    _check_api(cfg, api)
    _print_table(cfg, api)


@cli.command(name='report')
@_config_option
@_api_option
@click.option('-v', '--verbose', 'verbose', is_flag=True,
              help='Also name every test in the buckets that need no decision.')
def report_cmd(config_path: str, api: str | None, verbose: bool) -> None:
    """Generate in memory and report what resolved, writing nothing."""
    print_report(_run(config_path, api), verbose=verbose)


@cli.command(name='generate')
@_config_option
@_api_option
@click.option('--write', 'do_write', is_flag=True,
              help='Write the generated files.  Without it nothing reaches disk.')
def generate_cmd(config_path: str, api: str | None, do_write: bool) -> None:
    """Generate both api trees from the sync suites."""
    table = _load(config_path)
    result = _run(config_path, api, table=table)
    blockers = result.report.blockers
    if blockers:
        for line in sorted(set(blockers)):
            click.echo(f'blocker: {line}', err=True)
        raise click.ClickException(f'{len(set(blockers))} blocker(s); nothing written')
    if not do_write:
        click.echo(f'{len(result.files)} file(s) generated, none written; pass --write')
        return
    for path in write(result, table, ROOT):
        click.echo(path)


@cli.command(name='check')
@_config_option
@_api_option
def check_cmd(config_path: str, api: str | None) -> None:
    """Fail if the trees on disk are not what the current sources generate."""
    table = _load(config_path)
    result = _run(config_path, api, table=table)
    apis = _apis(table, api)
    blockers = sorted(set(result.report.blockers))
    if blockers:
        for line in blockers:
            click.echo(f'blocker: {line}', err=True)
        raise click.ClickException(
            f'{len(blockers)} blocker(s); the trees cannot be brought up to date')
    differences = check(result, table, ROOT, apis)
    if differences:
        for kind, path in differences:
            click.echo(f'{kind}: {path}', err=True)
        raise click.ClickException(
            f'{len(differences)} difference(s); {_remedy(differences)}')
    click.echo(f'{len(expected_tree(result, table, apis))} file(s) up to date')


def _remedy(differences: list[tuple[str, str]]) -> str:
    """What to actually do about what `check` found.

    Regenerating writes what the table names and removes nothing, so it is no answer to an
    orphan: a file the table has stopped naming stays where it is however many times the
    generator runs, and pytest keeps collecting it.
    """
    kinds = {kind for kind, _ in differences}
    regenerate = 'run `python -m tools.testgen generate --write` and commit what it writes'
    remove = 'delete the orphaned file(s), which no generator run will remove'
    if kinds == {'orphaned'}:
        return remove
    if 'orphaned' in kinds:
        return f'{regenerate}, then {remove}'
    return regenerate


def _load(config_path: str) -> SuiteTable:
    try:
        return load(config_path, ROOT)
    except ConfigError as ex:
        raise click.ClickException(str(ex))


def _run(config_path: str, api: str | None, table: SuiteTable | None = None):
    table = table if table is not None else _load(config_path)
    _check_api(table, api)
    counter = _Counter(table, _apis(table, api))
    try:
        return run(table, ROOT, apis=None if api is None else (api,), progress=counter.tick)
    except ConfigError as ex:
        raise click.ClickException(str(ex))
    finally:
        counter.clear()


class _Counter:
    """A single line on stderr naming the suite the generator is on.

    Every command here spends effectively all of its time in the per-suite loop inside
    `run`, and until this line existed there was nothing to distinguish a slow run from a
    hung one.  The line is rewritten in place and erased at the end, so it costs the output
    nothing; redirected, where rewriting a line only produces noise, it is not written at
    all.
    """

    def __init__(self, table: SuiteTable, apis: tuple[str, ...]) -> None:
        self.total = sum(1 for s in table.suites for a in s.targets if a in apis)
        self.done = 0
        self.enabled = sys.stderr.isatty()

    def tick(self, api: str, source: str) -> None:
        self.done += 1
        self._line(f'[{self.done}/{self.total}] {api} {os.path.basename(source)}')

    def clear(self) -> None:
        self._line('')

    def _line(self, text: str) -> None:
        if self.enabled:
            click.echo(f'\r\033[K{text}', nl=False, err=True)


def _apis(table: SuiteTable, api: str | None) -> tuple[str, ...]:
    return tuple(table.apis) if api is None else (api,)


def _check_api(table: SuiteTable, api: str | None) -> None:
    if api is not None and api not in table.apis:
        raise click.ClickException(
            f'unknown api {api!r}; the table declares {", ".join(table.apis)}')


def _print_table(table: SuiteTable, only: str | None) -> None:
    for api in table.apis:
        if only is not None and api != only:
            continue
        suites = table.suites_for(api)
        rule(f'{api} -> {table.output_dir(api)}  ({len(suites)} suites)')
        grid = plain_table(('source', {}),
                           ('sidecar merged in', {}))
        for suite in suites:
            grid.add_row(suite.source, suite.sidecar_for(api) or '')
        console.print(grid)
    skipped = [s for s in table.suites if len(s.targets) < len(table.apis)]
    rule(f'{len(table.suites)} suites, {len(skipped)} with a narrowed target list')
    grid = plain_table(('source', {}),
                       ('targets', {}))
    for suite in skipped:
        grid.add_row(suite.source, ', '.join(suite.targets))
    console.print(grid)
    total = sum(len(v) for v in table.not_generated.values())
    rule(f'{total} sources under {table.source_dir} generate nothing')
    grid = plain_table(('reason', {}),
                       ('sources', {'justify': 'right'}))
    for reason, sources in table.not_generated.items():
        grid.add_row(reason, str(len(sources)))
    console.print(grid)


if __name__ == '__main__':
    sys.exit(cli())
