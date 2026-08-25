# Couchbase Python SDK Autogen Tools

This package contains the next-generation code generation tools for the Couchbase Python SDK. It is designed to be extensible and modular, supporting the generation of C++ bindings, options classes, and more.

## Architecture

The package is organized into several key components:

- **`cli.py`**: The main entry point using [Click](https://click.palletsprojects.com/).
- **`commands/`**: Contains Click command groups. Each command group (e.g., `bindings`) has its own file.
- **`core/`**: Contains the heavy lifting logic. Modules here are designed to be used as libraries.
  - `cpp_type_parser.py`: Uses Clang's Python bindings to parse C++ core headers.
  - `binding_builder.py`: Maps C++ types to Python-friendly binding models.
  - `generate_binding_files.py`: Orchestrates the rendering of Jinja2 templates.
- **`config/`**: YAML configuration files defining the generation schema.
- **`templates/`**: Jinja2 templates for the generated C++ and Python files.

## C++ Binding Generation

The `bindings` command automates the alignment between the C++ core and the Python C-API. It parses the C++ request/response structures and generates:

1.  `operations_autogen.hxx`: Template specializations for `py_to_cbpp_t`.
2.  `cpp_core_types_autogen.hxx`: Core structure conversions.
3.  `cpp_core_enums_autogen.hxx`: Enum conversion macros.
4.  `pycbc_core.pyi`: Python type stubs for the core connection object.
5.  `binding_cpp_types.py`: Python `TypedDict` definitions mirroring C++ structs.

### Configuration (`config/bindings.yaml`)

The generation is driven by `bindings.yaml`. You can add new operations or types by modifying the appropriate sections:

- `key_value`: Operations in the KV namespace.
- `management`: Cluster, Bucket, and Collection management operations.
- `cpp_core_types`: Generic structs that need conversion logic.
- `cpp_core_enums`: Enums that need conversion logic.

## Post-Processing (formatting & churn)

`bindings generate` renders unformatted output, so the committed files are whatever the repo's
pre-commit fixers turn that output into. Generation therefore finishes with two extra steps:

1. **Format** - runs `pre-commit run --files <generated files>`. Scoped with `--files` so nothing
   outside the generated set is touched and unrelated lint failures cannot block generation.
2. **Restore unchanged** - every generated file whose only delta against `HEAD` is the
   `Generated-On` / `Content-Hash` lines is restored, so a regeneration that changes nothing
   leaves a clean `git status`.

The restore is safe by construction: a file is only restored when its content is byte-identical
to `HEAD` once those metadata lines are removed, so real changes - including hand-edits - can
never be discarded.

Opt out with `--no-format` and `--no-restore-unchanged`. If generation and formatting were done
by hand, run the same tail on its own:

```bash
python -m tools.autogen bindings tidy
```

## Debugging a Struct

When a generated binding looks wrong or empty, inspect exactly what the libclang pass sees for
one struct (or a list of them). This renders nothing and writes no files:

```bash
# everything a single header yields
python -m tools.autogen bindings inspect --header core/operations/document_query.hxx

# a single struct, defining header located automatically
python -m tools.autogen bindings inspect --type couchbase::core::operations::query_request

# several at once, as JSON
python -m tools.autogen bindings inspect --type query_request --type analytics_request --as-json
```

`--type` accepts a fully-qualified name, a leaf name, or a substring. Add `--no-resolve` to skip
C++/Python type mapping and see only the parsed canonical types.

A configured struct that parses to **zero fields** aborts generation rather than silently
emitting an empty binding. If the C++ struct genuinely has no fields (an empty tag type used as
a `std::variant` alternative, for example), set `allow_empty_fields: true` on its
`cpp_core_types` entry.

## Extensibility

To add a new generation command:

1.  Create a new logic module in `core/` (if needed).
2.  Create a new command file in `commands/`.
3.  Register the new command group in `cli.py`.

## Version Compatibility (System LLVM vs. Pip Clang)

The `bindings` command uses the `clang` Python package, which relies on the system's `libclang`.

**Important**: If your pip-installed `clang` package version is newer than your system's LLVM version, you may encounter `libclang` loading errors or parsing failures.

The tool performs an automatic check and will display a warning if a potential mismatch is detected. To resolve this:
- **Upgrade LLVM**: On macOS, run `brew upgrade llvm`.
- **Match Pip Package**: Install a version of the pip package that matches your system LLVM major version:
  ```bash
  pip install clang==$(llvm-config --version | cut -d. -f1).0.0
  ```

## Environment Configuration

The tool attempts to auto-locate LLVM and system headers on macOS. If your environment is non-standard, use the following flags:

- `--llvm-version`: Version string (e.g., `18`).
- `--llvm-includedir`: Path to LLVM includes.
- `--llvm-libdir`: Path to `libclang.dylib`.
- `--system-headers`: Path to system SDK (e.g., output of `xcrun --show-sdk-path`).
