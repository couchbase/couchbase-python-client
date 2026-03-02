# Quick Start: C++ Binding Autogen

This tool generates the C++ binding code and Python type stubs for the Couchbase Python SDK.

## Prerequisites

- Python 3.9+
- LLVM/Clang (installed via Homebrew on macOS: `brew install llvm`)
- Dependencies installed: `pip install -r tools/autogen/tools_requirements.txt`
- CPM cache has been populated (`PYCBC_SET_CPM_CACHE=ON PYCBC_USE_OPENSSL=OFF python setup.py configure_ext`; see [BUILDING doc](../../BUILDING.md))

## Basic Usage

Run the generator using default settings (suitable for most macOS installations):

```bash
python -m tools.autogen bindings generate
```

## Common Tasks

### Dry Run
Preview which files would be updated without modifying anything:

```bash
python -m tools.autogen bindings generate --dry-run
```

### Use a Specific LLVM Version
If you have multiple versions of LLVM installed:

```bash
python -m tools.autogen bindings generate --llvm-version 18
```

### Verbose Logging
Enable detailed logging for troubleshooting C++ parsing issues:

```bash
python -m tools.autogen bindings generate --verbose
```

## Help

For a full list of options:

```bash
python -m tools.autogen bindings generate --help
```
