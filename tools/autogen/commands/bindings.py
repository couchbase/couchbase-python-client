"""Binding generation commands."""

import json
from pathlib import Path
from typing import Optional, Tuple

import click

from tools.autogen.core.binding_builder import BindingBuilder
from tools.autogen.core.cpp_type_parser import CppTypeParser
from tools.autogen.core.generate_binding_files import BindingGenerator
from tools.autogen.core.post_process import restore_unchanged, run_pre_commit
from tools.autogen.core.type_inspector import inspect_types
from tools.autogen.core.validators import validate_llvm_compatibility


@click.group(name='bindings')
def bindings_group():
    """C++ binding generation commands."""
    pass


def llvm_options(command):
    """Attach the shared LLVM/Clang toolchain options to a command."""
    for option in reversed([
        click.option('--llvm-version',
                     help='Set LLVM/Clang version, or use command: llvm-config --version'),
        click.option('--llvm-includedir',
                     help='Set LLVM include directory, or use command: llvm-config --includedir'),
        click.option('--llvm-libdir',
                     help='Set LLVM library directory, or use command: llvm-config --libdir'),
        click.option('--system-headers',
                     help='Set system headers path, or use command: xcrun --show-sdk-path'),
    ]):
        command = option(command)
    return command


def _post_process(output_files, repo_root: Path, skip_format: bool, skip_restore: bool) -> None:
    """Format the generated files, then drop the ones that did not really change."""
    if not skip_format:
        click.echo(f"\n{'-'*60}")
        click.echo('Formatting generated files (pre-commit)...')
        ran, output = run_pre_commit(output_files, repo_root)
        if not ran:
            click.secho(f'Skipped formatting: {output}', fg='yellow', bold=True)
            click.secho('Generated files are unformatted; run pre-commit before committing.',
                        fg='yellow')
        elif output.strip():
            click.echo(output.strip())

    if skip_restore:
        return

    click.echo(f"\n{'-'*60}")
    restored, changed = restore_unchanged(output_files, repo_root)
    click.echo(f'{len(changed)} file(s) changed, {len(restored)} restored (metadata-only)')
    for path in changed:
        click.secho(f'  changed:  {path.relative_to(repo_root)}', fg='cyan')
    for path in restored:
        click.echo(f'  restored: {path.relative_to(repo_root)}')


@bindings_group.command()
@llvm_options
@click.option(
    '--config-path',
    type=click.Path(exists=True, path_type=Path),
    help='Path to bindings.yaml configuration'
)
@click.option(
    '--templates-path',
    type=click.Path(exists=True, path_type=Path),
    help='Path to Jinja2 templates directory'
)
@click.option(
    '--output-path',
    type=click.Path(exists=True, path_type=Path),
    help='Root directory for generated files'
)
@click.option(
    '--dry-run',
    is_flag=True,
    help='Show what would be generated without writing files'
)
@click.option(
    '--skip-version-check',
    is_flag=True,
    help='Skip the LLVM/Clang version compatibility check'
)
@click.option(
    '--no-format',
    is_flag=True,
    help='Skip the pre-commit pass over the generated files'
)
@click.option(
    '--no-restore-unchanged',
    is_flag=True,
    help='Keep generated files whose only delta is the Generated-On/Content-Hash lines'
)
@click.option(
    '--verbose',
    is_flag=True,
    help='Run in verbose mode'
)
def generate(llvm_version: Optional[str],  # noqa: C901
             llvm_includedir: Optional[str],
             llvm_libdir: Optional[str],
             system_headers: Optional[str],
             config_path: Optional[Path],
             templates_path: Optional[Path],
             output_path: Optional[Path],
             dry_run: bool,
             skip_version_check: bool,
             no_format: bool,
             no_restore_unchanged: bool,
             verbose: bool):
    """Generate C++ binding files from schema.

    Examples:

    \b
    # Generate bindings using default paths
    python -m tools.autogen bindings generate

    \b
    # Generate bindings with custom LLVM version
    python -m tools.autogen bindings generate --llvm-version 18

    \b
    # Preview what would be generated
    python -m tools.autogen bindings generate --dry-run
    """
    # Setup paths
    autogen_root = Path(__file__).parent.parent
    project_root = autogen_root.parent.parent

    if not config_path:
        config_path = autogen_root / 'config' / 'bindings.yaml'

    if not templates_path:
        templates_path = autogen_root / 'templates' / 'bindings'

    if not output_path:
        output_path = project_root

    # Perform version check
    if not skip_version_check:
        try:
            if not llvm_version:
                CppTypeParser.find_llvm()
                llvm_version = CppTypeParser.get_llvm_version()

            warning = validate_llvm_compatibility(llvm_version)
            if warning:
                click.echo(f"\n{'!'*60}")
                click.secho(warning, fg='yellow', bold=True)
                click.echo(f"{'!'*60}")
        except Exception as e:
            if verbose:
                click.secho(f"Note: Could not automatically verify LLVM version: {e}", fg='blue')

    click.echo(f"\n{'='*60}")
    click.echo("Starting C++ Binding Generation...")
    if dry_run:
        click.secho("MODE: DRY RUN", fg='yellow', bold=True)
    click.echo(f"{'='*60}")

    try:
        # Load schema
        schema = BindingGenerator.load_binding_schema_yaml(config_path)

        # Setup builder and generator
        builder = BindingBuilder({
            'llvm_clang_version': llvm_version,
            'llvm_libdir': llvm_libdir,
            'llvm_includedir': llvm_includedir,
            'system_headers': system_headers,
            'verbose': verbose
        })

        generator = BindingGenerator(
            schema=schema,
            binding_builder=builder,
            templates_dir=templates_path,
            output_root=output_path,
            dry_run=dry_run
        )

        # Run generation
        generator.run()

        if not dry_run:
            _post_process(generator.output_files,
                          project_root,
                          skip_format=no_format,
                          skip_restore=no_restore_unchanged)

        click.echo(f"\n{'='*60}")
        if dry_run:
            click.secho("✓ Dry run completed successfully", fg='green', bold=True)
        else:
            click.secho("✓ Binding generation completed successfully", fg='green', bold=True)
        click.echo(f"{'='*60}")

    except Exception as e:
        click.secho(f"\nError during generation: {e}", fg='red', bold=True)
        if verbose:
            import traceback
            click.echo(traceback.format_exc())
        raise click.Abort()


@bindings_group.command()
@llvm_options
@click.option(
    '--type', 'type_filters',
    multiple=True,
    help='Struct to inspect: fully-qualified, leaf name, or substring. Repeatable.'
)
@click.option(
    '--header', 'headers',
    multiple=True,
    help='Header to parse, relative to deps/couchbase-cxx-client. Repeatable.'
)
@click.option(
    '--as-json',
    is_flag=True,
    help='Emit records as JSON instead of a table'
)
@click.option(
    '--no-resolve',
    is_flag=True,
    help='Skip C++/Python type resolution and show only the parsed canonical types'
)
@click.option(
    '--verbose',
    is_flag=True,
    help='Run in verbose mode'
)
def inspect(llvm_version: Optional[str],  # noqa: C901
            llvm_includedir: Optional[str],
            llvm_libdir: Optional[str],
            system_headers: Optional[str],
            type_filters: Tuple[str, ...],
            headers: Tuple[str, ...],
            as_json: bool,
            no_resolve: bool,
            verbose: bool):
    """Inspect what the parser sees for individual C++ core structs.

    Renders nothing and writes no files - use it to debug a struct whose generated binding
    looks wrong or empty.

    Examples:

    \b
    # Everything a single header yields
    python -m tools.autogen bindings inspect --header core/operations/document_query.hxx

    \b
    # A single struct, header located automatically
    python -m tools.autogen bindings inspect --type couchbase::core::operations::query_request

    \b
    # Several structs at once, as JSON
    python -m tools.autogen bindings inspect --type query_request --type analytics_request --as-json
    """
    if not type_filters and not headers:
        raise click.UsageError('Provide at least one --type or --header.')

    try:
        builder = BindingBuilder({
            'llvm_clang_version': llvm_version,
            'llvm_libdir': llvm_libdir,
            'llvm_includedir': llvm_includedir,
            'system_headers': system_headers,
            'verbose': verbose
        })

        records, unresolved = inspect_types(builder,
                                            list(headers),
                                            list(type_filters),
                                            resolve_types=not no_resolve)
    except Exception as e:
        click.secho(f"\nError during inspection: {e}", fg='red', bold=True)
        if verbose:
            import traceback
            click.echo(traceback.format_exc())
        raise click.Abort()

    for type_filter in unresolved:
        click.secho(f'No defining header found for: {type_filter}', fg='yellow', bold=True)

    if as_json:
        click.echo(json.dumps(records, indent=2))
        return

    if not records:
        click.secho('No matching structs found.', fg='yellow', bold=True)
        return

    for record in records:
        click.echo('')
        click.secho(record['struct_name'], fg='cyan', bold=True)
        click.echo(f"  header: {record['header']}")
        if record['field_count'] == 0:
            click.secho('  0 fields - an empty tag/marker struct, or the parser could not read the body',
                        fg='red', bold=True)
            continue
        click.echo(f"  fields: {record['field_count']}")
        for field in record['fields']:
            click.echo(f"    {field['cpp_name']}")
            click.echo(f"      canonical: {field['canonical']}")
            if not no_resolve:
                click.echo(f"      cpp_type:  {field['cpp_type']}")
                click.echo(f"      py_type:   {field['py_type']}")


@bindings_group.command()
@click.option(
    '--config-path',
    type=click.Path(exists=True, path_type=Path),
    help='Path to bindings.yaml configuration'
)
@click.option(
    '--output-path',
    type=click.Path(exists=True, path_type=Path),
    help='Root directory for generated files'
)
@click.option(
    '--no-format',
    is_flag=True,
    help='Skip the pre-commit pass over the generated files'
)
def tidy(config_path: Optional[Path], output_path: Optional[Path], no_format: bool):
    """Format generated files and restore the ones that did not really change.

    This is the tail of `bindings generate`, runnable on its own for when generation and
    formatting were done by hand.

    \b
    python -m tools.autogen bindings tidy
    """
    autogen_root = Path(__file__).parent.parent
    project_root = autogen_root.parent.parent

    if not config_path:
        config_path = autogen_root / 'config' / 'bindings.yaml'
    if not output_path:
        output_path = project_root

    try:
        schema = BindingGenerator.load_binding_schema_yaml(config_path)
        generator = BindingGenerator(schema=schema,
                                     binding_builder=None,
                                     templates_dir=autogen_root / 'templates' / 'bindings',
                                     output_root=output_path)
        _post_process(generator.output_files, project_root, skip_format=no_format, skip_restore=False)
    except Exception as e:
        click.secho(f'\nError during tidy: {e}', fg='red', bold=True)
        raise click.Abort()
