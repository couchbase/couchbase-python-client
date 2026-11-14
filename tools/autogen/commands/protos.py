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

"""Protostellar generation commands."""

from pathlib import Path

import click

from tools.autogen.core.protos import (OUTPUT,
                                       ProtosError,
                                       drift,
                                       render,
                                       write)

_SOURCE = click.option('--source', type=click.Path(exists=True, file_okay=False, path_type=Path),
                       help='A Protostellar checkout at the pinned commit, rather than downloading it.')


@click.group(name='protos')
def protos_group():
    """The couchbase2 protocol's generated code and the schema it comes from."""
    pass


@protos_group.command(name='check')
@_SOURCE
@click.pass_context
def check(ctx, source) -> None:
    """Regenerate and fail when the result differs from the tree's."""
    root = Path(ctx.obj['root'])
    try:
        changes = drift(root, render(root, source))
    except ProtosError as error:
        raise click.ClickException(str(error))
    if changes:
        click.echo('\n'.join(changes), err=True)
        raise click.ClickException(f'{OUTPUT} is stale: run `python -m tools.autogen protos generate --write`')
    click.echo(f'{OUTPUT} up to date')


@protos_group.command(name='generate')
@click.option('--write', 'write_tree', is_flag=True, help='Replace the tree rather than list what would change.')
@_SOURCE
@click.pass_context
def generate(ctx, write_tree: bool, source) -> None:
    """Regenerate, listing the files that differ unless asked to write them."""
    root = Path(ctx.obj['root'])
    try:
        files = render(root, source)
        changes = write(root, files) if write_tree else drift(root, files)
    except ProtosError as error:
        raise click.ClickException(str(error))
    click.echo('\n'.join(changes) if changes else f'{OUTPUT}: nothing to change')
