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

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from couchbase.common._streaming import AsyncStreamingExecutor
from couchbase.common.result import ScanResult
from couchbase.protocol.classic._range_scan import RangeScanStream

if TYPE_CHECKING:
    from couchbase.protocol.classic._pycbc_core import pycbc_connection


class ClassicAsyncRangeScanExecutor(AsyncStreamingExecutor[ScanResult, None]):
    """Classic's asyncio range scan executor: the scan starts, and each read waits, in a worker thread.

    The binding offers no callback for the scan, so a read can only wait.  Starting the scan waits
    on the server too, and on the loop it would stop every other operation from completing until it
    returned.  The calls are sequential, so one worker serves the whole scan.
    """

    def __init__(self,
                 connection: pycbc_connection,
                 scan_args: dict[str, Any],
                 loop: asyncio.AbstractEventLoop) -> None:
        self._stream = RangeScanStream(connection, scan_args)
        self._loop = loop
        self._worker = ThreadPoolExecutor(max_workers=1)
        self._released = False

    async def submit(self) -> None:
        await self._loop.run_in_executor(self._worker, self._stream.submit)

    async def next_row(self) -> ScanResult:
        return await self._loop.run_in_executor(self._worker, self._stream.fetch, StopAsyncIteration)

    def final_metadata(self) -> None:
        return None

    def cancel_scan(self) -> None:
        self._stream.cancel()

    def _end_span(self, exc: BaseException | None) -> None:
        pass

    def _cancel(self) -> None:
        self._stream.cancel()

    def _release(self) -> None:
        # A worker still waiting on the core is released by the cancel, so nothing waits for it.
        if not self._released:
            self._released = True
            self._worker.shutdown(wait=False)
