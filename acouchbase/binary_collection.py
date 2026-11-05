
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

from typing import TYPE_CHECKING

from couchbase.result import CounterResult, MutationResult

if TYPE_CHECKING:
    from typing_extensions import Unpack

    from couchbase.common._interfaces._binary_collection import AsyncBinaryCollectionInterface
    from couchbase.common.options import (AppendOptionsKwargs,
                                          DecrementOptionsKwargs,
                                          IncrementOptionsKwargs,
                                          PrependOptionsKwargs)
    from couchbase.options import (AppendOptions,
                                   DecrementOptions,
                                   IncrementOptions,
                                   PrependOptions)


class BinaryCollection:
    """The operations on a collection's binary documents and counters.

    Get one from :meth:`~acouchbase.collection.AsyncCollection.binary`.
    """

    def __init__(self, impl: AsyncBinaryCollectionInterface) -> None:
        self._impl = impl

    async def increment(self,
                        key: str,
                        opts: IncrementOptions | None = None,
                        **kwargs: Unpack[IncrementOptionsKwargs]) -> CounterResult:
        """Increments the counter held in a document by the delta option, which defaults to 1.

        Args:
            key (str): The key of the document to increment.
            opts (:class:`~couchbase.options.IncrementOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.IncrementOptions`

        Returns:
            Awaitable[:class:`~couchbase.result.CounterResult`]: A future that contains an instance
            of :class:`~couchbase.result.CounterResult` if successful.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:
            Simple increment operation::

                from acouchbase.options import IncrementOptions, SignedInt64

                # ... other code ...

                collection = bucket.default_collection()
                res = await collection.binary().increment('counter-doc', IncrementOptions(initial=SignedInt64(100))
                print(f'Counter value: {res.content}')

            Simple increment operation, change default delta::

                from acouchbase.options import IncrementOptions, DeltaValue

                # ... other code ...

                collection = bucket.default_collection()
                res = await collection.binary().increment('counter-doc', IncrementOptions(delta=DeltaValue(5))
                print(f'Counter value: {res.content}')

        """
        return await self._impl.increment(key, opts, **kwargs)

    async def decrement(self,
                        key: str,
                        opts: DecrementOptions | None = None,
                        **kwargs: Unpack[DecrementOptionsKwargs]) -> CounterResult:
        """Decrements the counter held in a document by the delta option, which defaults to 1.

        Args:
            key (str): The key of the document to decrement.
            opts (:class:`~couchbase.options.DecrementOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.DecrementOptions`

        Returns:
            Awaitable[:class:`~couchbase.result.CounterResult`]: A future that contains an instance
            of :class:`~couchbase.result.CounterResult` if successful.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:
            Simple decrement operation::

                from acouchbase.options import DecrementOptions, SignedInt64

                # ... other code ...

                collection = bucket.default_collection()
                res = await collection.binary().decrement('counter-doc', DecrementOptions(initial=SignedInt64(100))
                print(f'Counter value: {res.content}')

            Simple decrement operation, change default delta::

                from acouchbase.options import DecrementOptions, DeltaValue

                # ... other code ...

                collection = bucket.default_collection()
                res = await collection.binary().decrement('counter-doc', DecrementOptions(delta=DeltaValue(5))
                print(f'Counter value: {res.content}')

        """
        return await self._impl.decrement(key, opts, **kwargs)

    async def append(self,
                     key: str,
                     value: str | bytes | bytearray,
                     opts: AppendOptions | None = None,
                     **kwargs: Unpack[AppendOptionsKwargs]) -> MutationResult:
        """Appends the specified value to the beginning of document of the specified key.

        Args:
            key (str): The key of the document to append to.
            value (str | bytes | bytearray): The value to append to the document.
            opts (:class:`~couchbase.options.AppendOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.AppendOptions`

        Returns:
            Awaitable[:class:`~couchbase.result.MutationResult`]: A future that contains an instance
            of :class:`~couchbase.result.MutationResult` if successful.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:
            Simple append string operation::

                # ... other code ...

                collection = bucket.default_collection()
                res = await collection.binary().append('string-doc', 'XYZ')

            Simple append binary operation::

                # ... other code ...

                collection = bucket.default_collection()
                res = await collection.binary().append('binary-doc', b'XYZ')

            Simple append operation with options::

                from datetime import timedelta

                from acouchbase.options import AppendOptions

                # ... other code ...

                collection = bucket.default_collection()
                res = await collection.binary().append('string-doc',
                                                        'XYZ',
                                                        AppendOptions(timeout=timedelta(seconds=2)))

        """
        return await self._impl.append(key, value, opts, **kwargs)

    async def prepend(self,
                      key: str,
                      value: str | bytes | bytearray,
                      opts: PrependOptions | None = None,
                      **kwargs: Unpack[PrependOptionsKwargs]) -> MutationResult:
        """Prepends the specified value to the beginning of document of the specified key.

        Args:
            key (str): The key of the document to prepend to.
            value (str | bytes | bytearray): The value to prepend to the document.
            opts (:class:`~couchbase.options.PrependOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.PrependOptions`

        Returns:
            Awaitable[:class:`~couchbase.result.MutationResult`]: A future that contains an instance
            of :class:`~couchbase.result.MutationResult` if successful.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:
            Simple prepend string operation::

                # ... other code ...

                collection = bucket.default_collection()
                res = await collection.binary().prepend('string-doc', 'ABC')

            Simple prepend binary operation::

                # ... other code ...

                collection = bucket.default_collection()
                res = await collection.binary().prepend('binary-doc', b'ABC')

            Simple prepend operation with options::

                from datetime import timedelta

                from acouchbase.options import PrependOptions

                # ... other code ...

                collection = bucket.default_collection()
                res = await collection.binary().prepend('string-doc',
                                                        'ABC',
                                                        PrependOptions(timeout=timedelta(seconds=2)))

        """
        return await self._impl.prepend(key, value, opts, **kwargs)
