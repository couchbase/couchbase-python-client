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

"""Operations table commands."""

from pathlib import Path

import click

from tools.autogen.core.operations import (OUTPUT,
                                           OperationsError,
                                           render,
                                           stale,
                                           write)


@click.group(name='operations')
def operations_group():
    """The `Op` enum and the table it is rendered from."""
    pass


@operations_group.command(name='check')
@click.pass_context
def check(ctx) -> None:
    """Load the table and fail when the module it renders differs from the tree's."""
    root = Path(ctx.obj['root'])
    try:
        if stale(root):
            raise click.ClickException(f'{OUTPUT} is stale: run `python -m tools.autogen operations generate --write`')
    except OperationsError as error:
        raise click.ClickException(str(error))
    click.echo(f'{OUTPUT} up to date')


@operations_group.command(name='generate')
@click.option('--write', 'write_module', is_flag=True, help='Write the module rather than print it.')
@click.pass_context
def generate(ctx, write_module: bool) -> None:
    """Render the table to source, on stdout unless asked to write."""
    root = Path(ctx.obj['root'])
    try:
        if write_module:
            click.echo(f'  wrote      {write(root)}')
            return
        click.echo(render(root), nl=False)
    except OperationsError as error:
        raise click.ClickException(str(error))
