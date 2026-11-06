#  Copyright 2016-2026. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Command-line entry point: python -m tools.docgen <command>."""

from __future__ import annotations

import pathlib
import subprocess
import sys
import tempfile

import click

from tools.check_public_api import (INVENTORY,
                                    read_counterparts,
                                    read_inventory)
from tools.docgen.layout import (LAYOUT,
                                 Layout,
                                 LayoutError,
                                 load,
                                 problems)
from tools.docgen.render import Reference

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent


@click.group()
def cli() -> None:
    """Generate the API reference pages from the public inventory and the docs layout.

    \b
    # Validate the layout and list the pages it generates
    python -m tools.docgen config
    # Write the pages, removing any the layout no longer generates
    python -m tools.docgen generate --write
    # Fail if the pages on disk are not what the layout generates
    python -m tools.docgen check
    # Build the HTML, failing on any warning
    python -m tools.docgen build
    """


@cli.command(name='config')
def config_cmd() -> None:
    """Validate the layout against the inventory and list the pages it generates."""
    _, expected = _expected()
    for path in sorted(expected):
        click.echo(path.relative_to(ROOT))


@cli.command(name='generate')
@click.option('--write', 'do_write', is_flag=True,
              help='Write the pages.  Without it nothing reaches disk.')
def generate_cmd(do_write: bool) -> None:
    """Generate every reference page."""
    layout, expected = _expected()
    stale, orphaned = _differences(layout, expected)
    if not do_write:
        click.echo(f'{len(expected)} page(s) generated, {len(stale)} differ from disk, '
                   f'{len(orphaned)} orphaned; pass --write')
        return
    for path in stale:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(expected[path])
        click.echo(f'wrote {path.relative_to(ROOT)}')
    for path in orphaned:
        path.unlink()
        click.echo(f'removed {path.relative_to(ROOT)}')


@cli.command(name='check')
def check_cmd() -> None:
    """Fail if the pages on disk are not what the layout generates."""
    layout, expected = _expected()
    stale, orphaned = _differences(layout, expected)
    for path in stale:
        click.echo(f'{"stale" if path.exists() else "missing"}: {path.relative_to(ROOT)}', err=True)
    for path in orphaned:
        click.echo(f'orphaned: {path.relative_to(ROOT)}', err=True)
    if stale or orphaned:
        raise click.ClickException(f'{len(stale) + len(orphaned)} difference(s); '
                                   'run `python -m tools.docgen generate --write` and commit what it changes')
    click.echo(f'{len(expected)} page(s) up to date')


@cli.command(name='build')
@click.option('--out', type=click.Path(file_okay=False, path_type=pathlib.Path),
              help='Where to write the HTML.  Defaults to a temporary directory, removed afterwards.')
def build_cmd(out: pathlib.Path | None) -> None:
    """Check the pages, then build the HTML with Sphinx, failing on any warning.

    The build reads the Python, Twisted, typing_extensions and OpenTelemetry inventories over the
    network, to link the names the SDK's signatures take from them.
    """
    check_cmd.callback()
    layout = load(LAYOUT)
    with tempfile.TemporaryDirectory() as scratch:
        target = out or pathlib.Path(scratch)
        argv = [sys.executable, '-m', 'sphinx', '-b', 'html', '-n', '-W', '--keep-going', '-E', '-q',
                '-d', str(pathlib.Path(scratch) / 'doctrees'), str(ROOT / layout.docs), str(target / 'html')]
        if subprocess.run(argv, cwd=ROOT).returncode:
            raise click.ClickException('the Sphinx build has warnings; each is printed above')
    click.echo(f'built {target / "html"}' if out else 'built cleanly')


def _expected() -> tuple[Layout, dict[pathlib.Path, str]]:
    try:
        layout = load(LAYOUT)
    except LayoutError as ex:
        raise click.ClickException(str(ex))
    inventory = read_inventory(INVENTORY)
    counterparts = read_counterparts(INVENTORY)
    found = problems(layout, inventory, counterparts)
    if found:
        for problem in found:
            click.echo(f'layout: {problem}', err=True)
        raise click.ClickException(f'{len(found)} disagreement(s) between {LAYOUT.name} and {INVENTORY.name}')
    reference = Reference(ROOT, layout, inventory, counterparts)
    found = reference.internal_problems()
    if found:
        for problem in found:
            click.echo(f'inventory: {problem}', err=True)
        raise click.ClickException(f'{len(found)} documented name(s) marked internal')
    return layout, {ROOT / layout.docs / path: text for path, text in reference.render().items()}


def _differences(layout: Layout, expected: dict[pathlib.Path, str]) -> tuple[list[pathlib.Path], list[pathlib.Path]]:
    """The pages that differ from disk, and the files under the output the layout does not generate.

    The output directory belongs to the generator, so anything else in it is an orphan.
    """
    stale = sorted(p for p, text in expected.items() if not p.is_file() or p.read_text() != text)
    output = ROOT / layout.docs / layout.output
    orphaned = sorted(p for p in output.rglob('*') if p.is_file() and p not in expected) if output.is_dir() else []
    return stale, orphaned
