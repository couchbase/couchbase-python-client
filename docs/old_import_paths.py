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

"""Warn, on an object's own entry, that the old paths it can still be imported from are deprecated.

``python -m tools.docgen generate`` writes the map this reads, from each documented object to the compat
names in ``tools/public_api.yaml`` it replaces.  The warning goes after
the docstring's summary, where a reader who imports the object from an old path sees it first.
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

MAP = 'reference/old_import_paths.json'
DOCUMENTED = ('class', 'exception', 'function', 'data')


def warning(name: str, old: list[str]) -> list[str]:
    module, _, short = name.rpartition('.')
    paths = [f'``{path}``' for path in old]
    said = paths[0] if len(paths) == 1 else f'{", ".join(paths[:-1])} and {paths[-1]}'
    verb = 'is' if len(paths) == 1 else 'are'
    return ['.. warning::',
            '',
            f'   {said} {verb} deprecated, and will be removed in a future release.  '
            f'Import ``{short}`` from ``{module}``.',
            '']


def _insert(lines: list[str], added: list[str]) -> None:
    """Put ``added`` after the summary, the docstring's first paragraph."""
    start = next((i for i, line in enumerate(lines) if line.strip()), len(lines))
    end = next((i for i in range(start, len(lines)) if not lines[i].strip()), len(lines))
    lines[end:end] = ['', *added]


def setup(app: Any) -> dict[str, Any]:
    old_paths: dict[str, list[str]] = json.loads((pathlib.Path(app.srcdir) / MAP).read_text())

    def process(app: Any, what: str, name: str, obj: object, options: Any, lines: list[str]) -> None:
        if what in DOCUMENTED and name in old_paths:
            _insert(lines, warning(name, old_paths[name]))

    app.connect('autodoc-process-docstring', process)
    return {'parallel_read_safe': True}
