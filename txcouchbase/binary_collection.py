
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
    'BinaryCollection',
]

from typing import TYPE_CHECKING, Any

from twisted.internet.defer import Deferred

from couchbase.result import CounterResult, MutationResult

if TYPE_CHECKING:
    from couchbase.common._interfaces._binary_collection import TxBinaryCollectionInterface
    from couchbase.options import (AppendOptions,
                                   DecrementOptions,
                                   IncrementOptions,
                                   PrependOptions)


class BinaryCollection:

    def __init__(self, impl: TxBinaryCollectionInterface) -> None:
        self._impl = impl

    def increment(self,
                  key: str,
                  opts: IncrementOptions | None = None,
                  **kwargs: Any) -> Deferred[CounterResult]:
        """Increments the ASCII value of the document, specified by the key, by the amount indicated in the delta
        option (defaults to 1).

        Args:
            key (str): The key of the document to increment.
            opts (:class:`~couchbase.options.IncrementOptions`): Optional parameters for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.IncrementOptions`

        Returns:
            Deferred[:class:`~couchbase.result.CounterResult`]: An instance of :class:`~couchbase.result.CounterResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        return self._impl.increment_deferred(key, opts, **kwargs)

    def decrement(self,
                  key: str,
                  opts: DecrementOptions | None = None,
                  **kwargs: Any) -> Deferred[CounterResult]:
        """Decrements the ASCII value of the document, specified by the key, by the amount indicated in the delta
        option (defaults to 1).

        Args:
            key (str): The key of the document to decrement.
            opts (:class:`~couchbase.options.DecrementOptions`): Optional parameters for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.DecrementOptions`

        Returns:
            Deferred[:class:`~couchbase.result.CounterResult`]: An instance of :class:`~couchbase.result.CounterResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        return self._impl.decrement_deferred(key, opts, **kwargs)

    def append(self,
               key: str,
               value: str | bytes | bytearray,
               opts: AppendOptions | None = None,
               **kwargs: Any) -> Deferred[MutationResult]:
        """Appends the specified value to the beginning of document of the specified key.

        Args:
            key (str): The key of the document to append to.
            value (str | bytes | bytearray): The value to append to the document.
            opts (:class:`~couchbase.options.AppendOptions`): Optional parameters for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.AppendOptions`

        Returns:
            Deferred[:class:`~couchbase.result.MutationResult`]: An instance of
            :class:`~couchbase.result.MutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        return self._impl.append_deferred(key, value, opts, **kwargs)

    def prepend(self,
                key: str,
                value: str | bytes | bytearray,
                opts: PrependOptions | None = None,
                **kwargs: Any) -> Deferred[MutationResult]:
        """Prepends the specified value to the beginning of document of the specified key.

        Args:
            key (str): The key of the document to prepend to.
            value (str | bytes | bytearray): The value to prepend to the document.
            opts (:class:`~couchbase.options.PrependOptions`): Optional parameters for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.PrependOptions`

        Returns:
            Deferred[:class:`~couchbase.result.MutationResult`]: An instance of
            :class:`~couchbase.result.MutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.
        """
        return self._impl.prepend_deferred(key, value, opts, **kwargs)
