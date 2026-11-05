#  Copyright 2016-2022. Couchbase, Inc.
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

__all__ = [
    'TransactionGetMultiMode',
    'TransactionGetMultiReplicasFromPreferredServerGroupMode',
    'TransactionGetMultiReplicasFromPreferredServerGroupResult',
    'TransactionGetMultiReplicasFromPreferredServerGroupSpec',
    'TransactionGetMultiResult',
    'TransactionGetMultiSpec',
    'TransactionMultiContentProxy',
]

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import (TYPE_CHECKING,
                    Any,
                    Protocol,
                    TypeVar,
                    cast)

from couchbase.common.exceptions import DocumentNotFoundException, InvalidIndexException
from couchbase.common.transcoder import Transcoder

if TYPE_CHECKING:
    from acouchbase.collection import Collection as AsyncCollection
    from couchbase.collection import Collection

_T = TypeVar('_T')


class _GetMultiResultSource(Protocol):
    """A protocol's result for a multi-document transactional read, one `(value, flags)` per spec."""

    @property
    def content(self) -> list[tuple[Any, int]]: ...


class TransactionGetMultiMode(Enum):
    """How hard a multi-document read inside a transaction works to avoid read skew.

    Read skew is seeing some documents from before another transaction committed and some from after.
    ``PRIORITISE_LATENCY`` makes a time-bounded effort to avoid it, ``PRIORITISE_READ_SKEW_DETECTION``
    makes a greater effort at a cost in latency, and ``DISABLE_READ_SKEW_DETECTION`` makes none.
    """

    PRIORITISE_LATENCY = 'prioritise_latency'
    DISABLE_READ_SKEW_DETECTION = 'disable_read_skew_detection'
    PRIORITISE_READ_SKEW_DETECTION = 'prioritise_read_skew_detection'


class TransactionGetMultiReplicasFromPreferredServerGroupMode(Enum):
    """How hard a multi-document read inside a transaction works to avoid read skew.

    Read skew is seeing some documents from before another transaction committed and some from after.
    ``PRIORITISE_LATENCY`` makes a time-bounded effort to avoid it, ``PRIORITISE_READ_SKEW_DETECTION``
    makes a greater effort at a cost in latency, and ``DISABLE_READ_SKEW_DETECTION`` makes none.
    """

    PRIORITISE_LATENCY = 'prioritise_latency'
    DISABLE_READ_SKEW_DETECTION = 'disable_read_skew_detection'
    PRIORITISE_READ_SKEW_DETECTION = 'prioritise_read_skew_detection'


@dataclass
class TransactionGetMultiSpec:
    """One document for a multi-document read inside a transaction.

    Attributes:
        collection: The collection the document is in.
        id: The document's ID.
        transcoder: The transcoder to decode the document with, if not the transaction's default.
    """

    collection: Collection | AsyncCollection
    id: str
    transcoder: Transcoder | None = None

    def _astuple(self) -> tuple[str, str, str, str]:
        return (self.collection._impl.bucket_name,
                self.collection._impl.scope_name,
                self.collection.name,
                self.id)


@dataclass
class TransactionGetMultiReplicasFromPreferredServerGroupSpec:
    """One document for a multi-document read inside a transaction from the preferred server group's replicas.

    Attributes:
        collection: The collection the document is in.
        id: The document's ID.
        transcoder: The transcoder to decode the document with, if not the transaction's default.
    """

    collection: Collection | AsyncCollection
    id: str
    transcoder: Transcoder | None = None

    def _astuple(self) -> tuple[str, str, str, str]:
        return (self.collection._impl.bucket_name,
                self.collection._impl.scope_name,
                self.collection.name,
                self.id)


class TransactionMultiContentProxy:
    """What a transactional multi-document read's ``content_as`` returns.

    ``content_as[dict](0)`` is the first document as a ``dict``.
    """

    def __init__(self, content_as_index: Callable[[int], Any]) -> None:
        self._content_as_index = content_as_index

    def __getitem__(self, type_: type[_T] | Callable[[Any], _T]) -> Callable[[int], _T]:
        """Get a function that takes a document's index and returns the document, converted by calling ``type_`` on it.

        Args:
            type_: The type, or any callable, to convert the document with.

        Returns:
            The function.
        """
        convert = cast(Callable[[Any], _T], type_)
        return lambda index: convert(self._content_as_index(index))


class TransactionGetMultiResult:
    """The documents a multi-document read inside a transaction returned, in the order of its specs."""

    def __init__(self,
                 res: _GetMultiResultSource,
                 transcoders: list[Transcoder | None],
                 default_transcoder: Transcoder) -> None:
        self._res = res
        self._transcoders = transcoders
        self._default_transcoder = default_transcoder
        self._decoded_values: dict[int, Any] = {}

    def exists(self, index: int) -> bool:
        """Check whether the document at an index exists.

        Args:
            index: The index of the document's spec.

        Returns:
            bool: ``True`` if the document exists.

        Raises:
            :class:`~couchbase.exceptions.InvalidIndexException`: If the index is out of range.
        """
        if index > len(self._res.content) - 1 or index < 0:
            raise InvalidIndexException(f'Provided index ({index}) is invalid.')

        return self._res.content[index] is not None

    @property
    def content_as(self) -> TransactionMultiContentProxy:
        """A proxy to return the document at the specified index.

        Get the first document as a dict::

            res = ctx.get_multi(specs)
            value = res.content_as[dict](0)

        Reading a document that does not exist raises
        :class:`~couchbase.exceptions.DocumentNotFoundException`.
        """

        def content_at_index(index: int) -> Any:
            if not self.exists(index):
                raise DocumentNotFoundException(f'Document does not exist at index={index}.')

            if index not in self._decoded_values:
                val, flags = self._res.content[index]
                tc = self._transcoders[index] or self._default_transcoder
                decoded_value = tc.decode_value(val, flags)
                self._decoded_values[index] = decoded_value

            return self._decoded_values[index]

        return TransactionMultiContentProxy(content_at_index)

    def __repr__(self) -> str:
        content: list[str | None] = []
        for idx in range(len(self._res.content)):
            if not self.exists(idx):
                content.append(None)
            else:
                content.append(self.content_as[str](idx))
        return f'{type(self).__name__}(content_list={content})'

    def __str__(self) -> str:
        return self.__repr__()


class TransactionGetMultiReplicasFromPreferredServerGroupResult:
    """The documents a multi-document read from the preferred server group returned, in the order of its specs."""

    def __init__(self,
                 res: _GetMultiResultSource,
                 transcoders: list[Transcoder | None],
                 default_transcoder: Transcoder) -> None:
        self._res = res
        self._transcoders = transcoders
        self._default_transcoder = default_transcoder
        self._decoded_values: dict[int, Any] = {}

    def exists(self, index: int) -> bool:
        """Check whether the document at an index exists.

        Args:
            index: The index of the document's spec.

        Returns:
            bool: ``True`` if the document exists.

        Raises:
            :class:`~couchbase.exceptions.InvalidIndexException`: If the index is out of range.
        """
        if index > len(self._res.content) - 1 or index < 0:
            raise InvalidIndexException(f'Provided index ({index}) is invalid.')

        return self._res.content[index] is not None

    @property
    def content_as(self) -> TransactionMultiContentProxy:
        """A proxy to return the document at the specified index.

        Get the first document as a dict::

            res = ctx.get_multi_replicas_from_preferred_server_group(specs)
            value = res.content_as[dict](0)

        Reading a document that does not exist raises
        :class:`~couchbase.exceptions.DocumentNotFoundException`.
        """

        def content_at_index(index: int) -> Any:
            if not self.exists(index):
                raise DocumentNotFoundException(f'Document does not exist at index={index}.')

            if index not in self._decoded_values:
                val, flags = self._res.content[index]
                tc = self._transcoders[index] or self._default_transcoder
                decoded_value = tc.decode_value(val, flags)
                self._decoded_values[index] = decoded_value

            return self._decoded_values[index]

        return TransactionMultiContentProxy(content_at_index)

    def __repr__(self) -> str:
        content: list[str | None] = []
        for idx in range(len(self._res.content)):
            if not self.exists(idx):
                content.append(None)
            else:
                content.append(self.content_as[str](idx))
        return f'{type(self).__name__}(content_list={content})'

    def __str__(self) -> str:
        return self.__repr__()
