"""Main CLI entry point for the new autogen tools."""

from pathlib import Path

import click

from tools.autogen.commands.bindings import bindings_group


@click.group()
@click.version_option(version="1.0.0", prog_name="autogen")
@click.pass_context
def cli(ctx):
    """Couchbase Python SDK code generation tools.

    Generate SDK code from schema definitions including bindings,
    options classes, enums, and other structures.

    Examples:

    \b
    # Generate C++ bindings
    python -m tools.autogen bindings generate
    """
    # Ensure context object exists for subcommands
    ctx.ensure_object(dict)
    ctx.obj['root'] = Path(__file__).parent.parent.parent


# Register command groups

cli.add_command(bindings_group)


if __name__ == "__main__":
    cli()
