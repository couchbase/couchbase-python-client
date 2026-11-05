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

from __future__ import annotations

__all__ = [
    'PrefixScan',
    'RangeScan',
    'SamplingScan',
    'ScanTerm',
    'ScanType',
]

from abc import ABC
from typing import Any

from couchbase.common._utils import fields_repr
from couchbase.common.exceptions import InvalidArgumentException


class ScanTerm:
    """Represents a search term for a RangeScan
    """

    def __init__(self, term: str, exclusive: bool | None = None) -> None:

        self._term = None
        if isinstance(term, str):
            self._term = term
        else:
            raise InvalidArgumentException('Invalid term value provided.  Expected str.')

        self._exclusive = exclusive

    def to_dict(self) -> dict[str, Any]:
        return {
            'term': self._term,
            'exclusive': self._exclusive
        }

    def __repr__(self) -> str:
        return fields_repr(self, term=self._term, exclusive=self._exclusive)


class ScanType(ABC):
    """
    ** INTERNAL **
    """


class RangeScan(ScanType):
    """A RangeScan performs a scan on a range of keys with the range specified through a start and end ScanTerm.
    """

    def __init__(self, start: ScanTerm | None = None, end: ScanTerm | None = None) -> None:
        self._start = start
        self._end = end

    @property
    def start(self) -> ScanTerm | None:
        return self._start

    @property
    def end(self) -> ScanTerm | None:
        return self._end

    def __repr__(self) -> str:
        return fields_repr(self, start=self._start, end=self._end)


class PrefixScan(ScanType):
    """A PrefixScan performs a scan on a given prefix
    """

    def __init__(self, prefix: str) -> None:
        self._prefix = prefix

    @property
    def prefix(self) -> str:
        return self._prefix

    def __repr__(self) -> str:
        return fields_repr(self, prefix=self._prefix)


class SamplingScan(ScanType):
    """A SamplingScan performs a scan on a random sampling of keys with the sampling bounded by a limit.
    """

    def __init__(self, limit: int, seed: int | None = None) -> None:
        self._limit = limit
        self._seed = seed

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def seed(self) -> int | None:
        return self._seed

    def __repr__(self) -> str:
        return fields_repr(self, limit=self._limit, seed=self._seed)
