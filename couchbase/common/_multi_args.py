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

"""What a multi operation caller's arguments mean, whichever protocol sends them."""

from __future__ import annotations

from typing import Any

from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.result import MultiGetResult, MultiMutationResult


def refuse_projection(final_args: dict[str, Any], per_key_args: dict[str, dict[str, Any]] | None) -> None:
    """Each key of a multi get goes out as a plain get, which carries no projection."""
    if any(args.get('project') is not None for args in (final_args, *(per_key_args or {}).values())):
        raise InvalidArgumentException('A multi get cannot project fields; use get for each key instead.')


def unlock_keys_and_cas(keys: object) -> dict[str, int]:
    """The CAS to unlock each key with, from a mapping of them or from the multi result that locked them."""
    if isinstance(keys, dict):
        if not all(map(lambda k: isinstance(k, str), keys.keys())):
            raise InvalidArgumentException('If providing keys of type dict, all values must be type int.')
        if not all(map(lambda v: isinstance(v, int), keys.values())):
            raise InvalidArgumentException('If providing keys of type dict, all values must be type int.')
        return dict(keys)
    if isinstance(keys, (MultiGetResult, MultiMutationResult)):
        return {k: v.cas for k, v in keys.results.items()}
    raise InvalidArgumentException('keys type must be MultiGetResult | MultiMutationResult | dict[str, int].')
