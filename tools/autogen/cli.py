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

"""Main CLI entry point for the new autogen tools."""

import importlib
from pathlib import Path

import click

from tools.autogen.commands.operations import operations_group


class _Lazy(click.Group):
    """A group that imports a deferred subcommand only when it is named, so a command does not
    pull in what it does not need: `bindings` needs libclang, and `operations` does not.
    """

    # Command name to the module holding it, for the groups that are not imported eagerly.
    deferred = {'bindings': 'tools.autogen.commands.bindings'}

    def list_commands(self, ctx):
        return sorted(set(super().list_commands(ctx)) | set(self.deferred))

    def get_command(self, ctx, name):
        found = super().get_command(ctx, name)
        if found is not None or name not in self.deferred:
            return found
        module = importlib.import_module(self.deferred[name])
        return getattr(module, '{}_group'.format(name))


@click.group(cls=_Lazy)
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


# Register command groups.

cli.add_command(operations_group)


if __name__ == "__main__":
    cli()
