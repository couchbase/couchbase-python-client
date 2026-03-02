"""Binding generation commands."""

from pathlib import Path
from typing import Optional

import click

from tools.autogen.core.binding_builder import BindingBuilder
from tools.autogen.core.cpp_type_parser import CppTypeParser
from tools.autogen.core.generate_binding_files import BindingGenerator
from tools.autogen.core.validators import validate_llvm_compatibility


@click.group(name='bindings')
def bindings_group():
    """C++ binding generation commands."""
    pass


@bindings_group.command()
@click.option(
    '--llvm-version',
    help='Set LLVM/Clang version, or use command: llvm-config --version'
)
@click.option(
    '--llvm-includedir',
    help='Set LLVM include directory, or use command: llvm-config --includedir'
)
@click.option(
    '--llvm-libdir',
    help='Set LLVM library directory, or use command: llvm-config --libdir'
)
@click.option(
    '--system-headers',
    help='Set system headers path, or use command: xcrun --show-sdk-path'
)
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
