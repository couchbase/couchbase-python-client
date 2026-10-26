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

from asyncio import AbstractEventLoop
from typing import TYPE_CHECKING, Any

from twisted.internet.defer import Deferred

from acouchbase.protocol.classic._streaming import response_signal
from couchbase.common._streaming import (FinalMetadataT,
                                         RowIterator,
                                         RowT)
from couchbase.protocol.classic._streaming import ClassicStreamingExecutor, ClassicStreamSpec

if TYPE_CHECKING:
    from couchbase.common._observability import ObservableRequestHandler
    from couchbase.protocol.classic._pycbc_core import pycbc_connection


def submit_rows(connection: pycbc_connection,
                loop: AbstractEventLoop,
                spec: ClassicStreamSpec[RowT, FinalMetadataT],
                *,
                obs_handler: ObservableRequestHandler | None,
                streaming_timeout: Any) -> tuple[RowIterator[RowT, FinalMetadataT], Deferred[None]]:
    """Send a streaming request, and fire the Deferred once the core has buffered the response.

    Twisted hands out the result only then, so iterating its rows never blocks the reactor.
    """
    responded, on_response = response_signal(loop)
    rows = RowIterator(ClassicStreamingExecutor(connection,
                                                spec,
                                                obs_handler=obs_handler,
                                                streaming_timeout=streaming_timeout,
                                                callback=on_response))
    rows.start()
    return rows, Deferred.fromFuture(responded)
