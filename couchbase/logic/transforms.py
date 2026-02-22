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

from datetime import timedelta
from enum import Enum, IntEnum
from typing import (Any,
                    Callable,
                    Optional,
                    Union)

from couchbase.exceptions import InvalidArgumentException


def enum_to_int(value: Enum, enum: Enum, conversion_fn: Optional[Callable[..., Any]] = None) -> int:
    if isinstance(value, int) and value in map(lambda x: x.value, enum):
        # TODO: use warning?
        # warn("Using deprecated string parameter {}".format(value))
        return value
    if not isinstance(value, enum):
        raise InvalidArgumentException(f"Argument must be of type {enum} but got {value}")
    if conversion_fn:
        try:
            return conversion_fn(value)
        except Exception:
            raise InvalidArgumentException(f"Unable to convert enum value {value} to str.")

    return value.value


def enum_to_str(value: Enum, enum: Enum, conversion_fn: Optional[Callable[..., Any]] = None) -> str:
    if isinstance(value, str) and value in map(lambda x: x.value, enum):
        # TODO: use warning?
        # warn("Using deprecated string parameter {}".format(value))
        return value
    if not isinstance(value, enum):
        raise InvalidArgumentException(f"Argument must be of type {enum} but got {value}")
    if conversion_fn:
        try:
            return conversion_fn(value)
        except Exception:
            raise InvalidArgumentException(f"Unable to convert enum value {value} to str.")

    return value.value


def int_to_enum(value: int, enum: IntEnum, conversion_fn: Optional[Callable[..., Any]] = None) -> int:
    if not isinstance(value, int):
        raise InvalidArgumentException(f"Argument must be of type int but got {type(value)}.")
    try:
        if conversion_fn:
            return conversion_fn(value)
        return enum(value)
    except Exception:
        raise InvalidArgumentException(f"Unable to convert {value} to enum of type {enum}.")


def seconds_to_timedelta(value: Union[float, int]) -> timedelta:
    if value and not isinstance(value, (float, int)):
        raise InvalidArgumentException(message=("Excepted value to be of type "
                                                f"Union[float, int] instead of {type(value)}"))
    return timedelta(seconds=value)


def str_to_enum(value: str, enum: Enum, conversion_fn: Optional[Callable[..., Any]] = None) -> str:
    if not isinstance(value, str):
        raise InvalidArgumentException(f"Argument must be of type str but got {type(value)}.")
    try:
        if conversion_fn:
            return conversion_fn(value)
        return enum(value)
    except Exception:
        raise InvalidArgumentException(f"Unable to convert {value} to enum of type {enum}.")


def timedelta_as_milliseconds(duration: timedelta) -> int:
    if duration and not isinstance(duration, timedelta):
        raise InvalidArgumentException(message=f'Expected value to be of type timedelta instead of {type(duration)}')
    return int(duration.total_seconds() * 1e3 if duration else 0)


def to_seconds(value: Union[timedelta, float, int]) -> int:
    if value and not isinstance(value, (timedelta, float, int)):
        raise InvalidArgumentException(message=("Excepted value to be of type "
                                                f"Union[timedelta, float, int] instead of {value}"))
    if not value:
        total_secs = 0
    elif isinstance(value, timedelta):
        total_secs = int(value.total_seconds())
    else:
        total_secs = int(value)

    return total_secs
