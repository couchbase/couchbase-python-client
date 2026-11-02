"""Refuse ``# type:`` comments in the SDK packages.

Annotations are the only form.  A type comment is invisible to anything that reads
``__annotations__`` or ``inspect.signature``, and mypy accepts one without complaint, so this
check is what keeps a new module from bringing them back.

``# type: ignore`` is not a type comment in this sense and is never counted; its rules are
``tools/audit_type_ignores.py``'s.

Run directly, or via the ``check-type-comments`` pre-commit hook::

    python tools/check_type_comments.py

Exits 1 when any module holds one.  Tests::

    python -m pytest tools/tests -q
"""

from __future__ import annotations

import io
import pathlib
import re
import subprocess
import sys
import tokenize
from collections.abc import Iterable

PACKAGES = ('couchbase', 'acouchbase', 'txcouchbase')

_TYPE_COMMENT = re.compile(r'#\s*type:(?!\s*ignore\b)')


def count(source: bytes) -> int:
    """Type comments in one module, read as tokens so a string holding the text is not one."""
    tokens = tokenize.tokenize(io.BytesIO(source).readline)
    return sum(1 for t in tokens if t.type == tokenize.COMMENT and _TYPE_COMMENT.match(t.string))


def is_counted(path: str) -> bool:
    parts = pathlib.PurePosixPath(path).parts
    return path.endswith('.py') and parts[0] in PACKAGES


def package_files(root: pathlib.Path) -> list[str]:
    # Untracked files are included so a new module is checked before it is staged; ignored
    # ones are not, which keeps build output and `_version.py` out.
    out = subprocess.run(['git', 'ls-files', '--cached', '--others', '--exclude-standard', '--', *PACKAGES],
                         cwd=root, check=True, capture_output=True, text=True).stdout
    return sorted(p for p in out.splitlines() if is_counted(p) and (root / p).is_file())


def measure(root: pathlib.Path, paths: Iterable[str]) -> dict[str, int]:
    counts = {p: count((root / p).read_bytes()) for p in paths}
    return {p: n for p, n in counts.items() if n}


def main() -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    found = measure(root, package_files(root))
    if not found:
        print('OK: no type comments.')
        return 0
    print(f'FAIL: {len(found)} module(s) hold type comments; write them as annotations:', file=sys.stderr)
    for path in sorted(found):
        print(f'    {path}: {found[path]}', file=sys.stderr)
    return 1


if __name__ == '__main__':
    sys.exit(main())
