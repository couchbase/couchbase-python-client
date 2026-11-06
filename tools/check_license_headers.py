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

"""Hold every source file to the Apache license header, and keep its year current.

Two rules, deliberately different in reach:

- Every source file opens with the header, word for word, reading ``Copyright 2016-<year>``.
  Any year from 2016 to the current one passes, so the turn of the year fails nothing.
- A file a commit changes carries the current year.  Only staged files are held to it, so a
  run over the whole tree, as CI does, never fails on the calendar.

A generated file is held to the first rule only.  Its header is its generator's, which takes the
year from its own input, ``license_year`` in the options schema, the testgen package marker or the
source a test is generated from, so a year written here would be written back by the next run.

Source files are Python, C++, shell, CMake and Dockerfiles.  Docs, configuration, data, the
protocol definitions copied from elsewhere and the templates are not; a template's output is
checked where it lands.  A tracked file the tree ignores is not part of the tree.

Run directly, or via the ``check-license-headers`` pre-commit hook, which fixes as it checks::

    python tools/check_license_headers.py [--fix] [--bump-all]

``--fix`` writes the header where it is missing or wrong, and the current year where a staged
file is stale.  ``--bump-all`` sets every file but a generated one to the current year, for a
sweep at the start of one.  Neither touches anything below the header.  That is checkable after the fact, which is
what makes a sweep reviewable::

    python tools/check_license_headers.py --headers-only <base> [<head>]

reports whether every file changed between the two revisions differs only in its header.

Exits 1 on any failure.  Tests::

    python -m pytest tools/tests -q
"""

from __future__ import annotations

import argparse
import datetime
import pathlib
import re
import subprocess
import sys
from collections.abc import Iterable
from dataclasses import dataclass

FIRST_YEAR = 2016

_BODY = (
    'All Rights Reserved.',
    '',
    'Licensed under the Apache License, Version 2.0 (the "License"{semi}',
    'you may not use this file except in compliance with the License.',
    'You may obtain a copy of the License at',
    '',
    '    http://www.apache.org/licenses/LICENSE-2.0',
    '',
    'Unless required by applicable law or agreed to in writing, software',
    'distributed under the License is distributed on an "AS IS" BASIS,',
    'WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.',
    'See the License for the specific language governing permissions and',
    'limitations under the License.',
)


@dataclass(frozen=True)
class Style:
    """How one language spells the header: the comment around each line, and its frame."""
    prefix: str
    semi: str
    opening: str = ''
    closing: str = ''

    def header(self, year: int) -> str:
        lines = [f'Copyright {FIRST_YEAR}-{year}. Couchbase, Inc.', *(b.format(semi=self.semi) for b in _BODY)]
        body = [(self.prefix + line).rstrip() for line in lines]
        framed = [self.opening, *body, self.closing] if self.opening else body
        return '\n'.join(framed) + '\n'


HASH = Style(prefix='#  ', semi=')')
C = Style(prefix=' *   ', semi=');', opening='/*', closing=' */')

_SUFFIXES = {'.py': HASH, '.pyi': HASH, '.sh': HASH, '.cxx': C, '.hxx': C}

# A generated file opens with this inside its first lines; anything further down is talking
# about the marker rather than carrying it.
_GENERATED = '!! AUTO-GENERATED'
_GENERATED_LINES = 20


def style_for(path: str) -> Style | None:
    """The header style for a source file, or None for a file outside the check."""
    p = pathlib.PurePosixPath(path)
    if p.name == 'CMakeLists.txt' or p.name == 'Dockerfile' or p.suffix == '.Dockerfile':
        return HASH
    return _SUFFIXES.get(p.suffix)


def _split(text: str) -> tuple[str, str, str]:
    """Split a file into its shebang, its header region and the rest.

    The header region is the leading blank lines, the license block and the blank lines after
    it.  A ``#`` block ends at its last license line, so a comment directly below it, such as a
    generated file's banner, stays in the rest.  A file opening with any other comment has an
    empty region, so a header is put in front of that comment rather than over it.
    """
    lines = text.splitlines(keepends=True)
    shebang = lines.pop(0) if lines and lines[0].startswith('#!') else ''
    i = 0
    while i < len(lines) and not lines[i].strip():
        i += 1
    j = i
    if j < len(lines) and lines[j].startswith('/*'):
        while j < len(lines) and '*/' not in lines[j]:
            j += 1
        j = min(j + 1, len(lines))
    else:
        while j < len(lines) and lines[j].startswith('#') and not lines[j].startswith('#!'):
            j += 1
            if 'limitations under the License.' in lines[j - 1]:
                break
    if 'Copyright' not in ''.join(lines[i:j]):
        j = 0
    while j < len(lines) and not lines[j].strip():
        j += 1
    return shebang, ''.join(lines[:j]), ''.join(lines[j:])


_YEAR = re.compile(rf'Copyright {FIRST_YEAR}-(\d{{4}})\. Couchbase, Inc\.')


def year_of(text: str, style: Style) -> int | None:
    """The header's closing year, or None when the file does not open with the header.

    The header is the one this module writes, spacing included, so a file passes exactly when
    rewriting it for the year it states would change nothing.
    """
    m = _YEAR.search(_split(text)[1])
    if m is None or with_header(text, style, int(m.group(1))) != text:
        return None
    return int(m.group(1))


def with_header(text: str, style: Style, year: int) -> str:
    """The file with its header region replaced by the header for ``year``.

    Shebang and everything below the region are kept byte for byte.  A shebang is followed by
    one blank line, the header by one more unless nothing comes after it.
    """
    shebang, _, rest = _split(text)
    header = style.header(year)
    return (shebang + '\n' if shebang else '') + header + ('\n' + rest if rest else '')


def unrecognised(text: str) -> bool:
    """Whether the header region is one this module will not rewrite.

    Another holder's notice is not ours to replace, and a ``#`` notice without the license's
    closing line has no end this module can find without guessing.
    """
    region = _split(text)[1]
    if 'Copyright' not in region:
        return False
    return 'Couchbase, Inc.' not in region or 'limitations under the License.' not in region


def is_generated(text: str) -> bool:
    return _GENERATED in ''.join(text.splitlines(keepends=True)[:_GENERATED_LINES])


def _git(root: pathlib.Path, *args: str) -> str:
    return subprocess.run(['git', *args], cwd=root, check=True, capture_output=True, text=True).stdout


def source_files(root: pathlib.Path) -> list[str]:
    # Tracked files only.  A checkout can hold untracked files the repository does not own, such
    # as a build's own scripts or interpreter, and --fix must never rewrite those.  A tracked file
    # that the ignore rules match is not part of the tree and is left out.
    listed = set(_git(root, 'ls-files', '--cached').splitlines())
    ignored = set(_git(root, 'ls-files', '--cached', '--ignored', '--exclude-standard').splitlines())
    return sorted(p for p in listed - ignored if style_for(p) and (root / p).is_file())


def staged_files(root: pathlib.Path) -> set[str]:
    return set(_git(root, 'diff', '--cached', '--name-only', '--diff-filter=ACMR').splitlines())


def audit(root: pathlib.Path,
          paths: Iterable[str],
          staged: set[str],
          year: int,
          fix: bool,
          bump_all: bool) -> list[str]:
    """Check each file, fixing what ``fix`` allows, and return a line per failure.

    A fixed file is still reported, as pre-commit expects of a hook that rewrites: the commit
    stops so the rewrite can be read and staged.
    """
    problems = []
    for path in paths:
        style = style_for(path)
        if style is None:
            continue
        file = root / path
        text = file.read_text()
        if unrecognised(text):
            problems.append(f'{path}: carries a copyright notice this check cannot rewrite; fix it by hand')
            continue
        found = year_of(text, style)
        if found is None or not FIRST_YEAR <= found <= year:
            reason = 'no license header, or not the standard one' if found is None else f'year {found}'
        elif found < year and (bump_all or path in staged) and not is_generated(text):
            reason = f'changed in this commit but its header says {found}'
        else:
            continue
        if fix:
            file.write_text(with_header(text, style, year))
            reason += f'; rewritten for {year}'
        problems.append(f'{path}: {reason}')
    return problems


def headers_only(root: pathlib.Path, base: str, head: str) -> list[str]:
    """Each file changed between two revisions whose change reaches below its header."""
    problems = []
    for line in _git(root, 'diff', '--name-status', '--no-renames', base, head).splitlines():
        status, path = line.split('\t', 1)
        if status != 'M':
            problems.append(f'{path}: status {status}; a header change only modifies files')
            continue
        before, after = (_git(root, 'cat-file', '-p', f'{rev}:{path}') for rev in (base, head))
        if _split(before)[::2] != _split(after)[::2]:
            problems.append(f'{path}: changed outside its license header')
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--fix', action='store_true', help='rewrite each header that fails')
    parser.add_argument('--bump-all', action='store_true', help='hold every file to the current year')
    parser.add_argument('--headers-only', nargs='+', metavar='REV',
                        help='check that BASE..HEAD (HEAD defaults to HEAD) changed only headers')
    args = parser.parse_args(argv)
    root = pathlib.Path(__file__).resolve().parent.parent

    if args.headers_only:
        base, head = (args.headers_only + ['HEAD'])[:2]
        changed = _git(root, 'diff', '--name-only', base, head).splitlines()
        problems = headers_only(root, base, head)
        if not problems:
            print(f'OK: all {len(changed)} files changed between {base} and {head} differ only in their header.')
            return 0
        print(f'FAIL: {len(problems)} of {len(changed)} changed files differ outside their header:', file=sys.stderr)
        for p in problems:
            print(f'    {p}', file=sys.stderr)
        return 1

    year = datetime.date.today().year
    paths = source_files(root)
    problems = audit(root, paths, staged_files(root), year, args.fix, args.bump_all)
    if not problems:
        print(f'OK: {len(paths)} source files carry the license header.')
        return 0
    print(f'FAIL: {len(problems)} file(s) need their license header seen to:', file=sys.stderr)
    for p in problems:
        print(f'    {p}', file=sys.stderr)
    return 1


if __name__ == '__main__':
    sys.exit(main())
