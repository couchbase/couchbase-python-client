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

"""`asyncio.run` for a sync test, leaving the thread's current loop as it found it.

Every tree loads the twisted plugin, so txcouchbase is imported before any test runs, and that import
installs the asyncioreactor over the thread's current loop.  `asyncio.run` ends by setting the current
loop to None, which drops that loop: nothing closes it, and Python reports it as an unclosed event
loop when the run exits.  Running the coroutine on a loop that is never made current is not an
option, because grpc.aio binds its futures to the current loop and refuses one that differs.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any, TypeVar

T = TypeVar('T')


def run_on_own_loop(coroutine: Coroutine[Any, Any, T]) -> T:
    try:
        previous: asyncio.AbstractEventLoop | None = asyncio.get_event_loop()
    except RuntimeError:
        previous = None
    try:
        return asyncio.run(coroutine)
    finally:
        asyncio.set_event_loop(previous)
