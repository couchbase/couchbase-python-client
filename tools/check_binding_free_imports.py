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

"""Hold the modules that cannot be imported without the native extension to a baseline that only falls.

couchbase2 runs with no native extension, and a user may remove it to make the install smaller, so
every module outside the classic protocol has to import without it.  Each module under
``couchbase``, ``acouchbase`` and ``txcouchbase`` is imported in an interpreter of its own, where a
meta path finder refuses any extension module those packages hold, which is what deleting the
``.so`` does.  ``protocol/classic`` is not checked: the binding is classic's.  Neither are the tests.

The modules that fail today are listed, not banned, so the check is a ratchet:

1. **A module not in the baseline imports.**
2. **A module in the baseline fails to import.**  One that imports fails the check too, until its
   line is removed, so the baseline is always the exact list of what is left.

The check also imports the binding package under the same finder and fails unless that import
fails, so a binding that moves cannot make every module pass by no longer being refused.

Run directly, or via the ``check-binding-free-imports`` pre-commit hook::

    python tools/check_binding_free_imports.py            # check
    python tools/check_binding_free_imports.py --update   # rewrite the baseline after a fix
    python tools/check_binding_free_imports.py --list     # every failing module, with why

``--update`` refuses to write a baseline that adds a module, and writes the first one from the
tree as it stands.  Exits 1 when the tree and the baseline disagree.  Tests::

    python -m pytest tools/tests -q
"""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from typing import TypedDict

PACKAGES = ('couchbase', 'acouchbase', 'txcouchbase')
BASELINE = pathlib.Path(__file__).with_name('binding_free_imports_baseline.txt')
BINDING = 'couchbase.protocol.classic._pycbc_core'
EXCLUDED_DIRS = frozenset({'tests', '_generated'})


class Failure(TypedDict):
    error: str
    frames: list[str]
    # The package modules the finder refused or the import system did not find.  An import that
    # fails with none missing failed for some other reason, such as a dependency the interpreter
    # lacks, and says nothing either way.
    missing: list[str]


# Refusing by location rather than by name also catches an extension a module loads under a name
# nobody listed.  The refusal is a ModuleNotFoundError, as a deleted file is, so a tree that was never
# built reports the same way: a package module not found anywhere in the exception chain is missing.
_CHILD = '''
import importlib
import importlib.abc
import importlib.machinery
import json
import sys
import traceback

PACKAGES = {packages!r}
refused = []


class RefuseExtensions(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path, target=None):
        if name.partition('.')[0] not in PACKAGES:
            return None
        spec = importlib.machinery.PathFinder.find_spec(name, path)
        if spec is not None and isinstance(spec.loader, importlib.machinery.ExtensionFileLoader):
            refused.append(name)
            raise ModuleNotFoundError(f'No module named {{name!r}}', name=name)
        return None


def not_found(exc):
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, ModuleNotFoundError) and (exc.name or '').partition('.')[0] in PACKAGES:
            yield exc.name
        exc = exc.__cause__ or exc.__context__


sys.meta_path.insert(0, RefuseExtensions())
try:
    importlib.import_module(sys.argv[1])
except BaseException as exc:
    frames = [f'{{f.filename}}:{{f.lineno}}' for f in traceback.extract_tb(exc.__traceback__)
              if not f.filename.startswith('<')]
    missing = sorted(set(refused) | set(not_found(exc)))
    print(json.dumps({{'error': f'{{type(exc).__name__}}: {{exc}}', 'frames': frames, 'missing': missing}}))
else:
    print(json.dumps(None))
'''


def module_name(path: str) -> str:
    return path.removesuffix('.py').removesuffix('/__init__').replace('/', '.')


def is_checked(path: str) -> bool:
    parts = pathlib.PurePosixPath(path).parts
    if not path.endswith('.py') or parts[0] not in PACKAGES or EXCLUDED_DIRS.intersection(parts):
        return False
    return parts[1:3] != ('protocol', 'classic')


def modules_checked(root: pathlib.Path) -> list[str]:
    # Untracked files are included so a new module is checked before it is staged.
    out = subprocess.run(['git', 'ls-files', '--cached', '--others', '--exclude-standard', '--', *PACKAGES],
                         cwd=root, check=True, capture_output=True, text=True).stdout
    return sorted(module_name(p) for p in set(out.splitlines()) if is_checked(p) and (root / p).is_file())


def import_failure(root: pathlib.Path, module: str) -> Failure | None:
    """Why ``module`` cannot be imported with the extensions refused, or None when it can."""
    env = {**os.environ, 'PYTHONPATH': os.pathsep.join(filter(None, [str(root), os.environ.get('PYTHONPATH')]))}
    proc = subprocess.run([sys.executable, '-c', _CHILD.format(packages=PACKAGES), module],
                          cwd=root, env=env, capture_output=True, text=True, timeout=300)
    lines = proc.stdout.strip().splitlines()
    if proc.returncode or not lines:
        return {'error': f'the interpreter exited {proc.returncode}: {proc.stderr.strip()[-500:]}',
                'frames': [],
                'missing': []}
    failure: Failure | None = json.loads(lines[-1])
    return failure


def measure(root: pathlib.Path, modules: Iterable[str]) -> dict[str, Failure]:
    """The modules that cannot be imported with the extensions refused, and why."""
    modules = list(modules)
    with ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as pool:
        failures = pool.map(lambda m: import_failure(root, m), modules)
        return {m: f for m, f in zip(modules, failures) if f is not None}


def entry_into_classic(root: pathlib.Path, frames: Iterable[str]) -> str | None:
    """The last frame outside classic before the import entered it: where the module reaches for the
    binding."""
    previous = None
    for frame in frames:
        filename, _, line = frame.rpartition(':')
        try:
            rel = pathlib.Path(filename).relative_to(root).as_posix()
        except ValueError:
            continue
        if is_checked(rel):
            previous = f'{rel}:{line}'
        elif previous is not None and rel.partition('/')[0] in PACKAGES:
            return previous
    return previous


def read_baseline(path: pathlib.Path) -> set[str]:
    return {line.strip() for line in path.read_text().splitlines() if line.strip() and not line.startswith('#')}


def render_baseline(modules: Iterable[str]) -> str:
    header = ('# Written by tools/check_binding_free_imports.py --update.  One line per module outside\n'
              '# the classic protocol that still cannot be imported without the native extension.\n'
              '# A line leaves when its module imports.\n')
    return header + ''.join(f'{m}\n' for m in sorted(modules))


def compare(baseline: set[str], failures: dict[str, Failure]) -> list[str]:
    current = {m for m, f in failures.items() if f['missing']}
    return ([f'{m}: cannot be checked, since it fails with no package module missing: {f["error"]}'
             for m, f in sorted(failures.items()) if not f['missing']]
            + [f'{m}: cannot be imported without the native extension, and is not in the baseline; '
               'load the binding where it is used, not at import' for m in sorted(current - baseline)]
            + [f'{m}: imports without the native extension; run with --update to remove it from the baseline'
               for m in sorted(baseline - current)])


def binding_is_refused(root: pathlib.Path) -> bool:
    failure = import_failure(root, BINDING)
    return failure is not None and bool(failure['missing'])


def main(argv: list[str]) -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    if not binding_is_refused(root):
        print(f'FAIL: {BINDING} imports with every extension refused, so nothing is checked.', file=sys.stderr)
        return 1
    modules = modules_checked(root)
    failures = measure(root, modules)
    if '--list' in argv:
        for module, failure in sorted(failures.items()):
            where = entry_into_classic(root, failure['frames'])
            print(f'{module}: {failure["error"]}' + (f' (from {where})' if where else ''))
        return 0
    baseline = read_baseline(BASELINE) if BASELINE.exists() else set()

    if '--update' in argv:
        unchecked = sorted(m for m, f in failures.items() if not f['missing'])
        added = sorted(m for m, f in failures.items() if f['missing'] and m not in baseline)
        if unchecked or (added and BASELINE.exists()):
            print('FAIL: refusing to write the baseline; these cannot be imported without the extension'
                  ' or cannot be checked:', file=sys.stderr)
            for module in (added if BASELINE.exists() else []) + unchecked:
                print(f'    {module}: {failures[module]["error"]}', file=sys.stderr)
            return 1
        BASELINE.write_text(render_baseline(failures))
        print(f'baseline written: {len(failures)} of {len(modules)} modules')
        return 0

    problems = compare(baseline, failures)
    if not problems:
        print(f'OK: {len(modules) - len(failures)} of {len(modules)} modules import without the native extension.')
        return 0
    print(f'FAIL: {len(problems)} module(s) disagree with {BASELINE.name}:', file=sys.stderr)
    for problem in problems:
        print(f'    {problem}', file=sys.stderr)
    return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
