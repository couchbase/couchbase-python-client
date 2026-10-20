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

"""The operations table: `operations.yaml` loaded, checked and rendered to the `Op` enum.

The rendered module is compared byte for byte with the one in the tree, so the template emits
source the repo's fixers leave alone, and `check` needs none of them to run.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import yaml
from jinja2 import (Environment,
                    FileSystemLoader,
                    StrictUndefined)

CONFIG = Path('tools/autogen/config/operations.yaml')
TEMPLATE = Path('tools/autogen/templates/operations/operations.py.jinja2')
OUTPUT = Path('couchbase/common/_operations.py')

_MAX_LINE = 120


class OperationsError(Exception):
    pass


@dataclass(frozen=True)
class Row:
    name: str
    value: str
    span_name: str | None
    kind: str
    service: str | None
    single: str | None


@dataclass(frozen=True)
class Table:
    kinds: dict[str, str]
    services: dict[str, str]
    rows: list[Row]


def snake(name: str) -> str:
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()


_ROW_KEYS = {'span', 'single'}
_GROUP_KEYS = {'kind', 'service', 'span', 'ops'}


def _row(name: str, spec: dict, group: dict, kinds: dict, services: dict) -> Row:
    unknown = set(spec) - _ROW_KEYS
    if unknown:
        raise OperationsError(f'{name}: unknown keys {sorted(unknown)}')
    kind = group['kind']
    if 'span' in spec:
        span = spec['span']
    elif group.get('span') == 'name':
        span = snake(name)
    else:
        raise OperationsError(f'{name}: no span, and its group does not name rows by name')
    single = spec.get('single')
    if (kind == 'KeyValueMulti') != (single is not None):
        raise OperationsError(f'{name}: `single` is set on exactly the KeyValueMulti rows')
    service = group.get('service')
    if service is not None and service not in services:
        raise OperationsError(f'{name}: no service {service}')
    if kind not in kinds:
        raise OperationsError(f'{name}: no kind {kind}')
    return Row(name, snake(name), span, kind, service, single)


def load(root: Path) -> Table:
    return parse(yaml.safe_load((root / CONFIG).read_text()))


def parse(data: dict) -> Table:
    kinds, services = data['kinds'], data['services']
    rows: list[Row] = []
    for group in data['groups']:
        unknown = set(group) - _GROUP_KEYS
        if unknown:
            raise OperationsError(f'group {group.get("kind")}: unknown keys {sorted(unknown)}')
        rows.extend(_row(name, spec, group, kinds, services) for name, spec in group['ops'].items())

    for label, names in (('row', [r.name for r in rows]),
                         ('span name', [r.span_name for r in rows if r.span_name is not None])):
        twice = sorted(n for n, count in Counter(names).items() if count > 1)
        if twice:
            raise OperationsError(f'{label} given twice: {twice}')
    by_name = {r.name: r for r in rows}
    position = {r.name: i for i, r in enumerate(rows)}
    for r in rows:
        if r.single is None:
            continue
        target = by_name.get(r.single)
        if target is None or target.kind != 'KeyValue':
            raise OperationsError(f'{r.name}: single {r.single} is not a KeyValue row')
        if position[r.single] > position[r.name]:
            raise OperationsError(f'{r.name}: its single {r.single} has to come first in the table')
    return Table(kinds, services, rows)


def _member(row: Row) -> str:
    """One member assignment, wrapped under its opening parenthesis when it will not fit."""
    parts = [repr(row.value),
             repr(row.span_name),
             f'OpKind.{row.kind}',
             f'ServiceType.{row.service}' if row.service else 'None']
    if row.single is not None:
        parts.append(repr(row.single))
    head = f'    {row.name} = ('
    line = head + ', '.join(parts) + ')'
    if len(line) <= _MAX_LINE:
        return line
    pad = ' ' * len(head)
    return head + (',\n' + pad).join(parts) + ')'


def render(root: Path) -> str:
    table = load(root)
    env = Environment(loader=FileSystemLoader(str(root / TEMPLATE.parent)),
                      undefined=StrictUndefined, keep_trailing_newline=True,
                      trim_blocks=True, lstrip_blocks=True)
    return env.get_template(TEMPLATE.name).render(
        kinds=table.kinds,
        services=table.services,
        members=[_member(r) for r in table.rows],
        snake=snake)


def stale(root: Path) -> bool:
    target = root / OUTPUT
    return not target.exists() or target.read_text() != render(root)


def write(root: Path) -> Path:
    (root / OUTPUT).write_text(render(root))
    return OUTPUT
