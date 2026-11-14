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

"""What a binary operation caller's arguments mean, whichever protocol sends them."""

from __future__ import annotations

from typing import Any

from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.options import DeltaValue, SignedInt64


def read_counter_args(final_args: dict[str, Any]) -> tuple[int, int | None]:
    """Pop `delta` and `initial`, and return the delta and the initial value, if any.

    An unset or zero delta is 1, and an unset initial value is 0.  A negative initial value means
    none: the counter must already exist.
    """
    delta = final_args.pop('delta', None) or DeltaValue(1)
    initial = final_args.pop('initial', None) or SignedInt64(0)
    if not isinstance(delta, DeltaValue):
        raise InvalidArgumentException('Argument is not valid DeltaValue')
    if not isinstance(initial, SignedInt64):
        raise InvalidArgumentException('Argument is not valid SignedInt64')
    return int(delta), int(initial) if int(initial) >= 0 else None


def binary_value(value: str | bytes | bytearray) -> bytes:
    if isinstance(value, str):
        return value.encode('utf-8')
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    raise ValueError('The value provided must of type str, bytes or bytearray.')
