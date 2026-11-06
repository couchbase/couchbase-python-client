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

"""Focused inspection of individual C++ core structs.

Debug aid for the bindings generator: parse one header (or locate the header defining a
struct) and dump exactly what the libclang pass sees, without rendering or writing any
generated file.
"""

from __future__ import annotations

import os
import re
from typing import Any

from tools.autogen.core.binding_builder import BindingBuilder
from tools.autogen.core.cpp_type_parser import CXX_CLIENT_ROOT

# Directories under the C++ client that can define a bindable struct.
_SEARCH_ROOTS = ('core', 'couchbase')


def _definition_pattern(leaf_name: str) -> re.Pattern:
    # A definition opens a body or a base-clause; a trailing ';' is a forward declaration.
    return re.compile(rf'^\s*(?:struct|class)\s+{re.escape(leaf_name)}\s*(?:final\s*)?[:{{]', re.MULTILINE)


def find_headers_for_struct(struct_name: str) -> list[str]:
    """Return C++ client relative header paths that *define* ``struct_name``."""
    leaf_name = struct_name.split('::')[-1]
    pattern = _definition_pattern(leaf_name)

    matches = []
    for search_root in _SEARCH_ROOTS:
        root_dir = os.path.join(CXX_CLIENT_ROOT, search_root)
        if not os.path.isdir(root_dir):
            continue
        for dir_path, _, file_names in os.walk(root_dir):
            for file_name in file_names:
                if not file_name.endswith('.hxx'):
                    continue
                full_path = os.path.join(dir_path, file_name)
                try:
                    with open(full_path, 'r', errors='replace') as f:
                        contents = f.read()
                except OSError:
                    continue
                if pattern.search(contents):
                    matches.append(os.path.relpath(full_path, CXX_CLIENT_ROOT))

    return sorted(matches)


def _matches_filter(struct_name: str, type_filters: list[str]) -> bool:
    if not type_filters:
        return True
    leaf_name = struct_name.split('::')[-1]
    return any(tf == struct_name or tf == leaf_name or tf in struct_name for tf in type_filters)


def _describe_field(builder: BindingBuilder, field: dict[str, Any]) -> dict[str, str]:
    described = {'cpp_name': field['name'], 'canonical': field['cpp_type'].get('name', '')}
    # A resolution failure here is itself the interesting signal, so report it rather than raise.
    for key, resolve in (('cpp_type', builder.get_cpp_type), ('py_type', builder.get_py_type)):
        try:
            described[key] = resolve(field)
        except Exception as ex:
            described[key] = f'<unresolved: {ex}>'
    return described


def inspect_types(builder: BindingBuilder,
                  headers: list[str],
                  type_filters: list[str],
                  resolve_types: bool | None = True) -> tuple[list[dict[str, Any]], list[str]]:
    """Parse ``headers`` and return (records, unresolved) for structs matching ``type_filters``.

    When no headers are given, the defining header for each type filter is located by scanning
    the C++ client tree.  Filters with no defining header are returned as ``unresolved``.
    """
    resolved_headers = list(headers)
    unresolved = []
    if not headers:
        for type_filter in type_filters:
            found = find_headers_for_struct(type_filter)
            if not found:
                unresolved.append(type_filter)
            resolved_headers.extend(h for h in found if h not in resolved_headers)

    records = []
    for header in resolved_headers:
        parsed = builder.cpp_parser.parse_op(header)
        for cpp_type in parsed:
            if not _matches_filter(cpp_type['struct_name'], type_filters):
                continue
            fields = [
                _describe_field(builder, f) if resolve_types
                else {'cpp_name': f['name'], 'canonical': f['cpp_type'].get('name', '')}
                for f in cpp_type['fields']
            ]
            records.append({
                'struct_name': cpp_type['struct_name'],
                'header': header,
                'field_count': len(fields),
                'fields': fields,
            })

    return records, unresolved
