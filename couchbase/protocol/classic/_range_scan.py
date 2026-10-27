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

"""Classic's range scan over the core's scan iterator, and its blocking executor.

A scan ends with a ``range_scan_completed`` error rather than an end marker, and so does a
cancelled scan on its next read.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from couchbase.common._streaming import StreamingExecutor
from couchbase.common.exceptions import InternalSDKException, RangeScanCompletedException
from couchbase.common.result import ScanResult
from couchbase.protocol.classic._error_mapping import ErrorMapper
from couchbase.protocol.classic._pycbc_core import pycbc_exception as PycbcCoreException
from couchbase.protocol.classic._result import _scan_item_from_core

if TYPE_CHECKING:
    from couchbase.protocol.classic._pycbc_core import pycbc_connection, pycbc_scan_iterator
    from couchbase.transcoder import Transcoder


class RangeScanStream:
    """One range scan: its call, the reads of its items, and its cancel.

    Each side's executor holds one and decides only how to wait for the core.
    """

    def __init__(self, connection: pycbc_connection, scan_args: dict[str, Any]) -> None:
        self._connection = connection
        self._scan_args = dict(scan_args)
        self._transcoder: Transcoder = self._scan_args.pop('transcoder')
        self._ids_only: bool = self._scan_args['orchestrator_options'].get('ids_only', False)
        self._scan_iterator: pycbc_scan_iterator[Any] | None = None

    def submit(self) -> None:
        self._scan_iterator = self._connection.pycbc_kv_range_scan(**self._scan_args)

    def fetch(self, stop: type[Exception]) -> ScanResult:
        """The next item, raising ``stop`` once the scan has completed."""
        if self._scan_iterator is None:
            raise InternalSDKException('The range scan was not submitted.')
        item = next(self._scan_iterator)
        if isinstance(item, PycbcCoreException):
            exc = ErrorMapper.build_exception(item)
            if isinstance(exc, RangeScanCompletedException):
                raise stop
            raise exc
        return ScanResult(_scan_item_from_core(item.raw_result), self._ids_only, self._transcoder)

    def cancel(self) -> None:
        """Stop the scan.  A read waiting on the core is released, and every later read ends it."""
        if self._scan_iterator is not None and not self._scan_iterator.is_cancelled():
            self._scan_iterator.cancel_scan()


class ClassicRangeScanExecutor(StreamingExecutor[ScanResult, None]):
    """Classic's blocking range scan executor: each read waits on the core, the GIL released."""

    def __init__(self, connection: pycbc_connection, scan_args: dict[str, Any]) -> None:
        self._stream = RangeScanStream(connection, scan_args)

    def submit(self) -> None:
        self._stream.submit()

    def next_row(self) -> ScanResult:
        return self._stream.fetch(StopIteration)

    def final_metadata(self) -> None:
        return None

    def cancel_scan(self) -> None:
        self._stream.cancel()

    def _end_span(self, exc: BaseException | None) -> None:
        pass

    def _cancel(self) -> None:
        self._stream.cancel()
