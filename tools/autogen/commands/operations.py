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
