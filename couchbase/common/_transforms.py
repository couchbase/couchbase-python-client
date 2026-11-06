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

from collections.abc import Callable, Iterable
from datetime import timedelta
from enum import Enum
from time import time
from typing import TYPE_CHECKING, Any

from couchbase.common.durability import ServerDurability
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.option_enums import IpProtocol, TLSVerifyMode
from couchbase.common.serializer import DefaultJsonSerializer

if TYPE_CHECKING:
    from couchbase.common.mutation_state import MutationState
    from couchbase.common.result import _MutationTokenDict


def enum_to_int(value: Enum | int, enum: type[Enum], conversion_fn: Callable[..., Any] | None = None) -> int:
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


def enum_to_str(value: Enum | str, enum: type[Enum], conversion_fn: Callable[..., Any] | None = None) -> str:
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


def seconds_to_timedelta(value: float | int) -> timedelta:
    if value and not isinstance(value, (float, int)):
        raise InvalidArgumentException(message=("Expected value to be of type "
                                                f"float | int instead of {type(value)}"))
    return timedelta(seconds=value)


def timedelta_as_milliseconds(duration: timedelta) -> int:
    if duration and not isinstance(duration, timedelta):
        raise InvalidArgumentException(message=f'Expected timedelta instead of {type(duration)}')
    return int(duration.total_seconds() * 1e3 if duration else 0)


def to_seconds(value: timedelta | float | int) -> int:
    if value and not isinstance(value, (timedelta, float, int)):
        raise InvalidArgumentException(message=("Expected value to be of type "
                                                f"timedelta | float | int instead of {value}"))
    if not value:
        total_secs = 0
    elif isinstance(value, timedelta):
        total_secs = int(value.total_seconds())
    else:
        total_secs = int(value)

    return total_secs


def to_milliseconds(timeout: timedelta | float | int) -> int:
    if timeout and not isinstance(timeout, (timedelta, float, int)):
        raise InvalidArgumentException(message=("Expected timeout to be of type "
                                                f"timedelta | float | int instead of {timeout}"))
    if not timeout:
        total_us = 0
    elif isinstance(timeout, timedelta):
        total_us = int(timeout.total_seconds() * 1e3)
    else:
        total_us = int(timeout * 1e3)

    return total_us


THIRTY_DAYS_IN_SECONDS = 30 * 24 * 60 * 60
# The server stores an expiry as an unsigned 32-bit Unix timestamp, so the last second it can
# hold is 2106-02-07T06:28:15Z.
MAX_EXPIRY_TIMESTAMP = 0xFFFFFFFF


def timedelta_as_timestamp(duration: timedelta) -> int:
    if not isinstance(duration, timedelta):
        raise InvalidArgumentException(f'Expected timedelta instead of {duration}')

    # PYCBC-1177 remove deprecated heuristic from PYCBC-948:
    seconds = int(duration.total_seconds())
    if seconds < 0:
        raise InvalidArgumentException(f'Expected expiry seconds of zero (for no expiry) or greater, got {seconds}.')

    if seconds < THIRTY_DAYS_IN_SECONDS:
        return seconds

    timestamp = seconds + int(time())
    if timestamp > MAX_EXPIRY_TIMESTAMP:
        raise InvalidArgumentException(
            f'Expected an expiry ending no later than 2106-02-07T06:28:15Z, got {duration} from now.')
    return timestamp


def validate_int(value: int) -> int:
    if not isinstance(value, int):
        raise InvalidArgumentException(message='Expected value to be of type int.')
    return value


def validate_bool(value: bool) -> bool:
    if not isinstance(value, bool):
        raise InvalidArgumentException(message='Expected value to be of type bool.')
    return value


def validate_str(value: str) -> str:
    if not isinstance(value, str):
        raise InvalidArgumentException(message='Expected value to be of type str.')
    return value


def as_str(value: Any) -> str:
    return str(value)


def enum_value(value: Enum) -> Any:
    return value.value


def passthrough(value: Any) -> Any:
    return value


def timedelta_as_seconds(duration: timedelta) -> int:
    return int(duration.total_seconds())


def csv_or_list(value: str | list[str]) -> list[str]:
    return value.split(',') if isinstance(value, str) else value


def mutation_tokens_object(value: MutationState | None) -> dict[str, list[_MutationTokenDict]] | None:
    if not value:
        return None
    # Imported here because `couchbase.mutation_state` reaches `couchbase.result`, which reaches
    # the options modules through observability, and they import this one.
    from couchbase.common.mutation_state import MutationState
    if not (isinstance(value, MutationState) and len(value._sv) > 0):
        raise InvalidArgumentException('Passed empty or invalid mutation state')
    # The range scan orchestrator reads the state as an envelope, where the query and search
    # builders send the same tokens as a bare list.
    return {'tokens': [token.as_dict() for token in value._sv]}


def ip_protocol_as_str(value: IpProtocol | str) -> str:
    return IpProtocol.to_str(value)


def tls_verify_as_str(value: TLSVerifyMode | str) -> str:
    return TLSVerifyMode.to_str(value)


# The five below are read only by the transaction tables, and four of them are a unit or a
# rejection that no other block needs.

def transaction_duration(duration: timedelta) -> int | None:
    if not isinstance(duration, timedelta):
        raise InvalidArgumentException(
            message='Expected timedelta instead of {}'.format(type(duration)))
    # The core reads a transaction's three durations in microseconds, and no other duration.
    # Zero becomes absent because the two are opposite answers here: absent is the core's own
    # default and zero is "expire now", so `timedelta()` would give a transaction that cannot
    # commit.
    return int(duration.total_seconds() * 1e6) or None


def transaction_durability(durability: ServerDurability) -> int:
    if not isinstance(durability, ServerDurability):
        raise InvalidArgumentException(
            message='Expected ServerDurability instead of {}'.format(type(durability)))
    # One level applies to every operation in the transaction, so a client durability policy has
    # nowhere to go.  `parse_durability` would answer with a dict where the core reads an int.
    return durability.level.value


def transaction_scan_consistency(value: Enum) -> str:
    consistency = enum_value(value)
    # `AT_PLUS` reads against a named mutation state, and a transactional query has no state to
    # name before its transaction commits.
    if consistency == 'at_plus':
        raise InvalidArgumentException(
            message='QueryScanConsistency.AT_PLUS not valid for transactions')
    return consistency


def json_encoded_values(values: dict[str, Any]) -> dict[str, Any]:
    return {key: DefaultJsonSerializer().serialize(value) for key, value in values.items()}


def json_encoded_list(values: Iterable[Any]) -> list[Any]:
    return [DefaultJsonSerializer().serialize(value) for value in values]
