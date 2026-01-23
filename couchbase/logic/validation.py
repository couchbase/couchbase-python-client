#  Copyright 2016-2023. Couchbase, Inc.
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

from typing import (Any,
                    Union,
                    get_args,
                    get_origin)

from couchbase.exceptions import InvalidArgumentException


def validate_bool(value: bool) -> bool:
    if not isinstance(value, bool):
        raise InvalidArgumentException(message='Expected value to be of type bool.')
    return value


def validate_int(value: int) -> int:
    if not isinstance(value, int):
        raise InvalidArgumentException(message='Expected value to be of type int.')
    return value


def validate_str(value: str) -> str:
    if not isinstance(value, str):
        raise InvalidArgumentException(message='Expected value to be of type str.')
    return value


def validate_type(val: Any, type_: Any) -> bool:
    type_origin = get_origin(type_)
    if type_origin is Union:
        type_args = get_args(type_)
        return any(validate_type(val, t_arg) for t_arg in type_args)
    elif type_origin is dict:
        if not isinstance(val, dict):
            return False
        key_types, val_types = get_args(type_)
        keys_okay = all(validate_type(k, key_types) for k in val.keys())
        vals_okay = all(validate_type(v, val_types) for v in val.values())
        return keys_okay and vals_okay
    elif type_origin is list:
        if not isinstance(val, list):
            return False
        type_args = get_args(type_)
        if not type_args:  # List without type parameter
            return True
        element_type = type_args[0]
        return all(validate_type(element, element_type) for element in val)

    if type_ is Any:
        return True

    return isinstance(val, type_)
