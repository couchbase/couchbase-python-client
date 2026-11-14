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

from __future__ import annotations

from typing import Any

from couchbase.common._utils import is_null_or_empty
from couchbase.common.exceptions import InvalidArgumentException


def validate_name(value: object, field: str) -> str:
    if not isinstance(value, str):
        if value is None:
            raise InvalidArgumentException(f'The {field} cannot be empty.')
        raise InvalidArgumentException(f'The {field} must be str.')
    if is_null_or_empty(value):
        raise InvalidArgumentException(f'The {field} cannot be empty.')
    return value


def single_options(method_name: str, rest: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    if len(rest) > 1:
        raise TypeError(f'{method_name}() takes one options object, {len(rest)} given')
    if 'options' in kwargs:
        if rest:
            raise TypeError(f"{method_name}() got multiple values for argument 'options'")
        return kwargs.pop('options')
    return rest[0] if rest else None
