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

"""What a SQL++ or analytics caller's arguments mean, whichever protocol sends them."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from couchbase.common._option_spec import BLOCKS
from couchbase.common.options import AnalyticsOptions, QueryOptions


@dataclass(frozen=True)
class QueryArgs:
    """A query's arguments, sorted into its options and its parameters."""
    options: dict[str, Any] = field(default_factory=dict)
    positional: list[Any] = field(default_factory=list)
    # By name without the leading `$`, which each protocol adds or leaves to its server.
    named: dict[str, Any] = field(default_factory=dict)


def query_args(options_type: type[QueryOptions] | type[AnalyticsOptions],
               options: Sequence[object],
               kwargs: Mapping[str, Any]) -> QueryArgs:
    """Sort a statement call's arguments the way every query API reads them.

    The last `options_type` among `options` holds the options, and any other positional argument is
    a positional parameter.
    """
    stated: Mapping[str, Any] = {}
    positional = []
    for option in options:
        if isinstance(option, options_type):
            stated = option
        else:
            positional.append(option)
    return sort_query_args(options_type, stated, positional, kwargs)


def sort_query_args(options_type: type[QueryOptions] | type[AnalyticsOptions],
                    stated: Mapping[str, Any],
                    positional: Sequence[object],
                    kwargs: Mapping[str, Any]) -> QueryArgs:
    """Sort the options a caller stated, laid under its keywords, into options and parameters.

    Any `positional` replaces the options' `positional_parameters`.  A keyword that names no
    `options_type` option is a named parameter, added to the options' `named_parameters`.
    """
    merged = dict(stated)
    merged.update(kwargs)
    # Accepted and discarded, as on every protocol.
    merged.pop('num_workers', None)

    if positional:
        merged.pop('positional_parameters', None)
        params = list(positional)
    else:
        params = list(merged.pop('positional_parameters', None) or ())
    keys = BLOCKS[options_type.__name__].keys
    named = dict(merged.pop('named_parameters', None) or {})
    named.update({key: merged.pop(key) for key in list(merged) if key not in keys})
    return QueryArgs(options=merged,
                     positional=params,
                     named={name.removeprefix('$'): value for name, value in named.items()})
