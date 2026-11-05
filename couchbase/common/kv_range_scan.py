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
    """One end of a :class:`RangeScan`.

    Args:
        term (str): The document ID the range starts or ends at.
        exclusive (bool, optional): True to leave the term itself out of the range. Defaults to None,
            which includes it.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If the term is not a str.
    """

    def __init__(self, term: str, exclusive: bool | None = None) -> None:

        self._term = None
        if isinstance(term, str):
            self._term = term
        else:
            raise InvalidArgumentException('Invalid term value provided.  Expected str.')

        self._exclusive = exclusive

    def to_dict(self) -> dict[str, Any]:
        """**INTERNAL** The term as the scan request takes it."""
        return {
            'term': self._term,
            'exclusive': self._exclusive
        }

    def __repr__(self) -> str:
        return fields_repr(self, term=self._term, exclusive=self._exclusive)


class ScanType(ABC):
    """The kind of scan :meth:`~couchbase.collection.Collection.scan` runs.

    Pass one of :class:`RangeScan`, :class:`PrefixScan` or :class:`SamplingScan`.
    """


class RangeScan(ScanType):
    """A scan of every document whose ID falls between two terms.

    Args:
        start (:class:`ScanTerm`, optional): Where the range starts. Defaults to None, which starts at the
            lowest ID.
        end (:class:`ScanTerm`, optional): Where the range ends. Defaults to None, which ends at the highest
            ID.
    """

    def __init__(self, start: ScanTerm | None = None, end: ScanTerm | None = None) -> None:
        self._start = start
        self._end = end

    @property
    def start(self) -> ScanTerm | None:
        """Where the range starts, or None to start at the lowest ID."""
        return self._start

    @property
    def end(self) -> ScanTerm | None:
        """Where the range ends, or None to end at the highest ID."""
        return self._end

    def __repr__(self) -> str:
        return fields_repr(self, start=self._start, end=self._end)


class PrefixScan(ScanType):
    """A scan of every document whose ID starts with a prefix.

    Args:
        prefix (str): The prefix the IDs share.
    """

    def __init__(self, prefix: str) -> None:
        self._prefix = prefix

    @property
    def prefix(self) -> str:
        """The prefix the IDs share."""
        return self._prefix

    def __repr__(self) -> str:
        return fields_repr(self, prefix=self._prefix)


class SamplingScan(ScanType):
    """A scan of a random sample of the collection's documents.

    Args:
        limit (int): The most documents the sample holds. Must be greater than 0.
        seed (int, optional): The seed the sample is drawn with. The same seed draws the same sample from
            the same data. Defaults to None, which draws a different sample each time.
    """

    def __init__(self, limit: int, seed: int | None = None) -> None:
        self._limit = limit
        self._seed = seed

    @property
    def limit(self) -> int:
        """The most documents the sample holds."""
        return self._limit

    @property
    def seed(self) -> int | None:
        """The seed the sample is drawn with, or None to draw a different sample each time."""
        return self._seed

    def __repr__(self) -> str:
        return fields_repr(self, limit=self._limit, seed=self._seed)
