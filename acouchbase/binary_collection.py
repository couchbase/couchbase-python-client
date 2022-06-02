
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

from typing import (TYPE_CHECKING,
                    Any,
                    Awaitable,
                    Union)

from couchbase.result import CounterResult, MutationResult

if TYPE_CHECKING:
    from couchbase.options import (AppendOptions,
                                   DecrementOptions,
                                   IncrementOptions,
                                   PrependOptions)


class BinaryCollection:

    def __init__(self, collection):
        self._collection = collection

    def increment(
        self,
        key,  # type: str
        *opts,  # type: IncrementOptions
        **kwargs,  # type: Any
    ) -> Awaitable[CounterResult]:
        """Increments the ASCII value of the document, specified by the key, by the amount indicated in the delta
        option (defaults to 1).

        Args:
            key (str): The key of the document to increment.
            opts (:class:`~couchbase.options.IncrementOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.IncrementOptions`

        Returns:
            Awaitable[:class:`~couchbase.result.CounterResult`]: A future that contains an instance
            of :class:`~couchbase.result.CounterResult` if successful.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:

            Simple increment operation::

                from couchbase.options import IncrementOptions, SignedInt64

                # ... other code ...

                collection = bucket.default_collection()
                res = await collection.binary().increment('counter-doc', IncrementOptions(initial=SignedInt64(100))
                print(f'Counter value: {res.content}')

            Simple increment operation, change default delta::

                from couchbase.options import IncrementOptions, DeltaValue

                # ... other code ...

                collection = bucket.default_collection()
                res = await collection.binary().increment('counter-doc', IncrementOptions(delta=DeltaValue(5))
                print(f'Counter value: {res.content}')

        """
        return self._collection._increment(key, *opts, **kwargs)

    def decrement(
        self,
        key,  # type: str
        *opts,  # type: DecrementOptions
        **kwargs,  # type: Any
    ) -> Awaitable[CounterResult]:
        """Decrements the ASCII value of the document, specified by the key, by the amount indicated in the delta
        option (defaults to 1).

        Args:
            key (str): The key of the document to decrement.
            opts (:class:`~couchbase.options.DecrementOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.DecrementOptions`

        Returns:
            Awaitable[:class:`~couchbase.result.CounterResult`]: A future that contains an instance
            of :class:`~couchbase.result.CounterResult` if successful.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:

            Simple decrement operation::

                from couchbase.options import DecrementOptions, SignedInt64

                # ... other code ...

                collection = bucket.default_collection()
                res = await collection.binary().decrement('counter-doc', DecrementOptions(initial=SignedInt64(100))
                print(f'Counter value: {res.content}')

            Simple decrement operation, change default delta::

                from couchbase.options import DecrementOptions, DeltaValue

                # ... other code ...

                collection = bucket.default_collection()
                res = await collection.binary().decrement('counter-doc', DecrementOptions(delta=DeltaValue(5))
                print(f'Counter value: {res.content}')

        """
        return self._collection._decrement(key, *opts, **kwargs)

    def append(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: AppendOptions
        **kwargs,  # type: Any
    ) -> Awaitable[MutationResult]:
        """Appends the specified value to the beginning of document of the specified key.

        Args:
            key (str): The key of the document to append to.
            value (Union[str, bytes, bytearray]): The value to append to the document.
            opts (:class:`~couchbase.options.AppendOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
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

                from couchbase.options import AppendOptions

                # ... other code ...

                collection = bucket.default_collection()
                res = await collection.binary().append('string-doc',
                                                        'XYZ',
                                                        AppendOptions(timeout=timedelta(seconds=2)))

        """
        return self._collection._append(key, value, *opts, **kwargs)

    def prepend(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: PrependOptions
        **kwargs,  # type: Any
    ) -> Awaitable[MutationResult]:
        """Prepends the specified value to the beginning of document of the specified key.

        Args:
            key (str): The key of the document to prepend to.
            value (Union[str, bytes, bytearray]): The value to prepend to the document.
            opts (:class:`~couchbase.options.PrependOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
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

                from couchbase.options import PrependOptions

                # ... other code ...

                collection = bucket.default_collection()
                res = await collection.binary().prepend('string-doc',
                                                        'ABC',
                                                        PrependOptions(timeout=timedelta(seconds=2)))

        """
        return self._collection._prepend(key, value, *opts, **kwargs)
