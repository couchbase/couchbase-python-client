# Copyright 2021, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Optional

from couchbase.exceptions import ErrorMapper, InvalidArgumentException
from couchbase.exceptions import exception as CouchbaseBaseException
from couchbase.logic.wrappers import decode_value
from couchbase.pycbc_core import kv_range_scan_operation
from couchbase.result import ScanResult

if TYPE_CHECKING:
    from couchbase.transcoder import Transcoder


class ScanTerm:
    """Represents a search term for a RangeScan
    """

    def __init__(self, term,  # type: str
                 exclusive=None  # type: Optional[bool]
                 ) -> None:

        self._term = None
        if isinstance(term, str):
            self._term = term
        else:
            raise InvalidArgumentException('Invalid term value provided.  Expected str.')

        self._exclusive = exclusive

    def to_dict(self):
        return {
            'term': self._term,
            'exclusive': self._exclusive
        }


class ScanType(ABC):
    """
    ** INTERNAL **
    """


class RangeScan(ScanType):
    """A RangeScan performs a scan on a range of keys with the range specified through a start and end ScanTerm.
    """

    def __init__(self,
                 start=None,  # type: Optional[ScanTerm]
                 end=None,  # type: Optional[ScanTerm]
                 ) -> None:
        self._start = start
        self._end = end

    @property
    def start(self) -> ScanTerm:
        return self._start

    @property
    def end(self) -> ScanTerm:
        return self._end


class PrefixScan(ScanType):
    """A PrefixScan performs a scan on a given prefix
    """

    def __init__(self,
                 prefix,  # type: str
                 ) -> None:
        self._prefix = prefix

    @property
    def prefix(self) -> str:
        return self._prefix


class SamplingScan(ScanType):
    """A SamplingScan performs a scan on a random sampling of keys with the sampling bounded by a limit.
    """

    def __init__(self, limit,  # type: int
                 seed=None,  # type: Optional[int]
                 ) -> None:
        self._limit = limit
        self._seed = seed

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def seed(self) -> Optional[int]:
        return self._seed


class RangeScanRequestLogic:
    """
    ** INTERNAL **
    """

    def __init__(self,
                 **kwargs
                 ):
        self._transcoder = kwargs.pop('transcoder', None)
        self._ids_only = kwargs['op_args'].get('ids_only', False)
        if not self._transcoder:
            raise InvalidArgumentException('No transcoder provided.')
        self._scan_args = kwargs
        self._scan_iterator = None
        self._started_streaming = False
        self._done_streaming = False

    @property
    def transcoder(self) -> Transcoder:
        return self._transcoder

    @property
    def started_streaming(self) -> bool:
        return self._started_streaming

    @property
    def done_streaming(self) -> bool:
        return self._done_streaming

    def cancel_scan(self) -> None:
        if self._scan_iterator.is_cancelled() is False:
            self._scan_iterator.cancel_scan()

    def _submit_scan(self):
        if self.done_streaming:
            return

        self._started_streaming = True
        self._scan_iterator = kv_range_scan_operation(**self._scan_args)

    def _get_next_row(self):
        if self.done_streaming is True:
            return

        resp = next(self._scan_iterator)
        if isinstance(resp, CouchbaseBaseException):
            raise ErrorMapper.build_exception(resp)

        value = resp.raw_result.get('value', None)
        flags = resp.raw_result.get('flags', None)
        if value:
            resp.raw_result['value'] = decode_value(self.transcoder, value, flags)

        return ScanResult(resp, self._ids_only)
