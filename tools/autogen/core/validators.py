"""Validation utilities for autogen tools."""

from __future__ import annotations

from typing import Optional, Tuple

try:
    from importlib import metadata
except ImportError:
    # Fallback for Python < 3.8
    import importlib_metadata as metadata  # type: ignore


def get_pip_clang_version() -> Optional[str]:
    """Get the version of the installed 'clang' pip package."""
    try:
        return metadata.version("clang")
    except metadata.PackageNotFoundError:
        return None


def parse_version(version_str: str) -> Tuple[int, ...]:
    """Parse a version string into a tuple of integers."""
    try:
        # Handle cases like '18.1.1' or '18.0.0-rc1'
        parts = version_str.split("-")[0].split(".")
        return tuple(int(p) for p in parts if p.isdigit())
    except (ValueError, AttributeError):
        return (0,)


def validate_llvm_compatibility(system_llvm_version: str) -> Optional[str]:
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
