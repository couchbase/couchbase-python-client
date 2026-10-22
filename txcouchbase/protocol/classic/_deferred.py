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

"""How a txcouchbase impl runs an acouchbase impl's coroutine as a `Deferred`."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import (TYPE_CHECKING,
                    Any,
                    TypeVar)

from twisted.internet.defer import Deferred
from twisted.python.failure import Failure

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop

    from couchbase.common._observability import ObservableRequestHandler

T = TypeVar('T')


def defer(loop: AbstractEventLoop,
          obs_handler: ObservableRequestHandler | None,
          start: Callable[[], Coroutine[Any, Any, T]]) -> Deferred[T]:
    """**INTERNAL**

    `start` builds the request and returns the coroutine that executes it.  It runs before the
    `Deferred` exists, so an invalid argument raises at the call rather than arriving as a failure.
    The span ends when the `Deferred` fires, whichever way it fires.
    """
    try:
        d: Deferred[T] = Deferred.fromFuture(asyncio.ensure_future(start(), loop=loop))
    except Exception as e:
        if obs_handler is not None:
            obs_handler.__exit__(type(e), e, e.__traceback__)
        raise
    if obs_handler is not None:
        d.addBoth(_finish_span, obs_handler)
    return d


def _finish_span(result: Any, obs_handler: ObservableRequestHandler) -> Any:
    if isinstance(result, Failure):
        exc = result.value
        obs_handler.__exit__(type(exc), exc, exc.__traceback__)
    else:
        obs_handler.__exit__(None, None, None)
    return result
