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

"""Validation utilities for autogen tools."""

from __future__ import annotations

try:
    from importlib import metadata
except ImportError:
    # Fallback for Python < 3.8
    import importlib_metadata as metadata  # type: ignore


def get_pip_clang_version() -> str | None:
    """Get the version of the installed 'clang' pip package."""
    try:
        return metadata.version("clang")
    except metadata.PackageNotFoundError:
        return None


def parse_version(version_str: str) -> tuple[int, ...]:
    """Parse a version string into a tuple of integers."""
    try:
        # Handle cases like '18.1.1' or '18.0.0-rc1'
        parts = version_str.split("-")[0].split(".")
        return tuple(int(p) for p in parts if p.isdigit())
    except (ValueError, AttributeError):
        return (0,)


def validate_llvm_compatibility(system_llvm_version: str) -> str | None:
    """
    Check if the system LLVM version is compatible with the pip-installed clang.
    Returns a warning message if a potential incompatibility is detected.
    """
    pip_version_str = get_pip_clang_version()
    if not pip_version_str:
        return None

    system_ver = parse_version(system_llvm_version)
    pip_ver = parse_version(pip_version_str)

    # Compare major versions
    if system_ver[0] < pip_ver[0]:
        return f"""WARNING: Potential LLVM/Clang version mismatch detected!
  - System LLVM version: {system_llvm_version}
  - Pip 'clang' package version: {pip_version_str}

HINT: If you encounter 'libclang' loading errors or parsing issues, ensure your system LLVM version is >= your pip 'clang' package version.
You can either:
  1. Upgrade your system LLVM (e.g., 'brew upgrade llvm')
  2. Downgrade your pip package (e.g., 'pip install clang~={system_ver[0]}.0')"""  # noqa: E501

    return None
