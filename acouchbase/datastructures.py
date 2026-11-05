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
    'CouchbaseList',
    'CouchbaseMap',
    'CouchbaseQueue',
    'CouchbaseSet',
]

import asyncio
import time
from collections.abc import (Awaitable,
                             Callable,
                             Generator,
                             Iterator)
from contextlib import contextmanager
from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Any,
                    NoReturn,
                    TypeVar)

from couchbase.common._observability import ObservableRequestHandler, parent_span_kwargs
from couchbase.common._operations import Op
from couchbase.exceptions import (CasMismatchException,
                                  DocumentExistsException,
                                  DocumentNotFoundException,
                                  InvalidArgumentException,
                                  PathExistsException,
                                  PathNotFoundException,
                                  QueueEmpty,
                                  UnAmbiguousTimeoutException)
from couchbase.subdocument import (array_addunique,
                                   array_append,
                                   array_prepend,
                                   count)
from couchbase.subdocument import exists as subdoc_exists
from couchbase.subdocument import get as subdoc_get
from couchbase.subdocument import (remove,
                                   replace,
                                   upsert)

if TYPE_CHECKING:
    from acouchbase.collection import AsyncCollection
    from couchbase.common._observability import WrappedSpan
    from couchbase.common._utils import JSONType
    from couchbase.result import GetResult, LookupInResult
    from couchbase.subdocument import Spec


_T = TypeVar('_T')


def _map_path(mapkey: str) -> str:
    # A map key is one field of the document, so it is quoted as a single path element: a dot or a
    # bracket in it would otherwise address a nested field.  A backtick is escaped by doubling it.
    return '`' + mapkey.replace('`', '``') + '`'


def _first_value(sd_res: LookupInResult) -> Any:
    """The decoded value of a one-spec lookup, or None where the spec failed.

    Read through `value` because `content_as` raises the failed spec's error instead.
    """
    fields = sd_res.value or []
    return fields[0].get('value', None)


class _Datastructure:
    """What the four datastructures share: a document, the collection it lives in, and a span per operation.

    Every read and write goes through the public `AsyncCollection`, with the datastructure operation's span passed
    as `parent_span`, so the key-value operations it performs are traced and measured as they would be if called
    directly.
    """

    def __init__(self, key: str, collection: AsyncCollection) -> None:
        self._key = key
        self._collection = collection

    def _empty(self) -> list[Any] | dict[str, Any]:
        return []

    @contextmanager
    def _span(self, op: Op) -> Iterator[WrappedSpan | None]:
        impl = self._collection._impl
        with ObservableRequestHandler(op, impl.observability_instruments) as obs_handler:
            obs_handler.create_kv_span(impl.keyspace)
            yield obs_handler.wrapped_span

    async def _create_if_missing(self, fn: Callable[[], Awaitable[_T]], parent_span: WrappedSpan | None) -> _T:
        try:
            return await fn()
        except DocumentNotFoundException:
            try:
                await self._collection.insert(self._key, self._empty(), **parent_span_kwargs(parent_span))
            except DocumentExistsException:
                pass
            return await fn()

    async def _get(self, parent_span: WrappedSpan | None = None) -> GetResult:
        return await self._create_if_missing(lambda: self._collection.get(self._key, **parent_span_kwargs(parent_span)),
                                             parent_span)

    async def _lookup_in(self, spec: Spec, parent_span: WrappedSpan | None) -> LookupInResult:
        return await self._create_if_missing(
            lambda: self._collection.lookup_in(self._key, (spec,), **parent_span_kwargs(parent_span)), parent_span)

    async def _mutate_in(self, spec: Spec, parent_span: WrappedSpan | None, create: bool = True) -> None:
        if create:
            await self._create_if_missing(
                lambda: self._collection.mutate_in(self._key, (spec,), **parent_span_kwargs(parent_span)), parent_span)
        else:
            await self._collection.mutate_in(self._key, (spec,), **parent_span_kwargs(parent_span))

    async def _clear(self, op: Op) -> None:
        with self._span(op) as span:
            try:
                await self._collection.remove(self._key, **parent_span_kwargs(span))
            except DocumentNotFoundException:
                pass


class CouchbaseList(_Datastructure):
    """CouchbaseList provides a simplified interface for storing a list within a Couchbase document.

    Args:
        key (str): Document key to use for the list.
        collection (:class:`~acouchbase.collection.Collection`): The :class:`~acouchbase.collection.Collection`
            where the list belongs.
    """

    def __init__(self, key: str, collection: AsyncCollection) -> None:
        super().__init__(key, collection)
        self._full_list: Iterator[Any] | None = None

    async def append(self, value: JSONType) -> None:
        """Add an item to the end of the list.

        Args:
            value (JSONType): The value to add.

        """
        with self._span(Op.ListAppend) as span:
            await self._mutate_in(array_append('', value), span)

    async def prepend(self, value: JSONType) -> None:
        """Add an item to the beginning of the list.

        Args:
            value (JSONType): The value to add.

        """
        with self._span(Op.ListPrepend) as span:
            await self._mutate_in(array_prepend('', value), span)

    async def set_at(self, index: int, value: JSONType) -> None:
        """Sets an item within a list at a specified index.

        Args:
            index (int): The index to retrieve.
            value (JSONType): The value to set.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index is out of range.

        """
        with self._span(Op.ListSetAt) as span:
            try:
                await self._mutate_in(replace(f'[{index}]', value), span, create=False)
            except PathNotFoundException:
                raise InvalidArgumentException(message=f'Index: {index} is out of range.') from None

    async def get_at(self, index: int) -> Any:
        """Retrieves the item at a specific index in the list.

        Args:
            index (int): The index to retrieve.

        Returns:
            Any: The value of the element at the specified index.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index is out of range.

        """
        with self._span(Op.ListGetAt) as span:
            try:
                sd_res = await self._lookup_in(subdoc_get(f'[{index}]'), span)
                return _first_value(sd_res)
            except PathNotFoundException:
                raise InvalidArgumentException(message=f'Index: {index} is out of range.') from None

    async def remove_at(self, index: int) -> None:
        """Removes an item at a specific index from the list.

        Args:
            index (int): The index to remove.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index is out of range.

        """
        with self._span(Op.ListRemoveAt) as span:
            try:
                await self._mutate_in(remove(f'[{index}]'), span, create=False)
            except PathNotFoundException:
                raise InvalidArgumentException(message=f'Index: {index} is out of range.') from None

    async def size(self) -> int:
        """Returns the number of items in the list.

        Returns:
            int: The number of items in the list.

        """
        with self._span(Op.ListSize) as span:
            sd_res = await self._lookup_in(count(''), span)
            return _first_value(sd_res)

    async def index_of(self, value: Any) -> int:
        """Returns the index of a specific value from the list.

        Args:
            value (JSONType): The value to search for.

        Returns:
            int: The index of the value in the list. Returns -1 if value is not found.

        """
        with self._span(Op.ListIndexOf) as span:
            for idx, val in enumerate((await self._get(span)).content_as[list]):
                if val == value:
                    return idx

            return -1

    async def get_all(self) -> list[Any]:
        """Returns the entire list of items in this list.

        Returns:
            list[Any]: The entire list.

        """
        with self._span(Op.ListGetAll) as span:
            return (await self._get(span)).content_as[list]

    async def clear(self) -> None:
        """Clears the list."""
        await self._clear(Op.ListClear)

    def __aiter__(self) -> CouchbaseList:
        return self

    async def __anext__(self) -> Any:
        if self._full_list is None:
            self._full_list = iter((await self._get()).content_as[list])

        try:
            val = next(self._full_list)
            # yield to the event loop
            await asyncio.sleep(0)
            return val
        except StopIteration:
            self._full_list = None
            raise StopAsyncIteration


class CouchbaseMap(_Datastructure):
    """CouchbaseMap provides a simplified interface for storing a map within a Couchbase document.

    Args:
        key (str): Document key to use for the map.
        collection (:class:`~acouchbase.collection.Collection`): The :class:`~acouchbase.collection.Collection`
            where the map belongs.
    """

    def _empty(self) -> dict[str, Any]:
        return {}

    async def add(self, mapkey: str, value: Any) -> None:
        """Sets a specific key to the specified value in the map.

        Args:
            mapkey (str): The key to set.
            value (JSONType): The value to set.

        """
        with self._span(Op.MapAdd) as span:
            await self._mutate_in(upsert(_map_path(mapkey), value), span)

    async def get(self, mapkey: str) -> Any:
        """Fetches a specific key from the map.

        Args:
            mapkey (str): The key to fetch.

        Returns:
            Any: The value of the specified key.

        """
        with self._span(Op.MapGet) as span:
            sd_res = await self._lookup_in(subdoc_get(_map_path(mapkey)), span)
            return _first_value(sd_res)

    async def remove(self, mapkey: str) -> None:
        """Removes a specific key from the map.

        Args:
            mapkey (str): The key in the map to remove.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the key is not in the map.

        """
        with self._span(Op.MapRemove) as span:
            try:
                await self._mutate_in(remove(_map_path(mapkey)), span, create=False)
            except PathNotFoundException:
                raise InvalidArgumentException(message=f'Key: {mapkey} is not in the map.') from None

    async def size(self) -> int:
        """Returns the number of items in the map.

        Returns:
            int: The number of items in the map.

        """
        with self._span(Op.MapSize) as span:
            sd_res = await self._lookup_in(count(''), span)
            return _first_value(sd_res)

    async def exists(self, key: str) -> bool:
        """Checks whether a specific key exists in the map.

        Args:
            key (str): The key to set.

        Returns:
            bool: True if the key exists in the map, False otherwise.

        """
        with self._span(Op.MapExists) as span:
            return (await self._lookup_in(subdoc_exists(_map_path(key)), span)).exists(0)

    async def keys(self) -> list[str]:
        """Returns a list of all the keys which exist in the map.

        Returns:
            list[str]: A list of all the keys that exist in the map.
        """
        with self._span(Op.MapKeys) as span:
            return list((await self._get(span)).content_as[dict].keys())

    async def values(self) -> list[Any]:
        """Returns a list of all the values which exist in the map.

        Returns:
            list[Any]: A list of all the values that exist in the map.
        """
        with self._span(Op.MapValues) as span:
            return list((await self._get(span)).content_as[dict].values())

    async def get_all(self) -> dict[str, Any]:
        """Retrieves the entire map.

        Returns:
            dict[str, Any]: The entire CouchbaseMap.
        """
        with self._span(Op.MapGetAll) as span:
            return (await self._get(span)).content_as[dict]

    async def clear(self) -> None:
        """Clears the map."""
        await self._clear(Op.MapClear)

    async def items(self) -> Generator[tuple[str, Any], None, None]:
        """Provides mechanism to loop over the entire map.

        Returns:
            Generator:  A generator expression for the map
        """
        with self._span(Op.MapItems) as span:
            map_ = (await self._get(span)).content_as[dict]
            return ((k, v) for k, v in map_.items())


class CouchbaseSet(_Datastructure):
    """CouchbaseSet provides a simplified interface for storing a set within a Couchbase document.

    Args:
        key (str): Document key to use for the set.
        collection (:class:`~acouchbase.collection.Collection`): The :class:`~acouchbase.collection.Collection`
            where the set belongs.
    """

    async def add(self, value: Any) -> bool:
        """Adds a value to the set, unless the set already holds it.

        Args:
            value (Any): The value to add.

        Returns:
            bool:  True if the value was added, False otherwise (meaning the value already
                exists in the set).

        """
        with self._span(Op.SetAdd) as span:
            try:
                await self._mutate_in(array_addunique('', value), span)
                return True
            except PathExistsException:
                return False

    async def remove(self, value: Any, timeout: timedelta | None = None) -> None:  # noqa: C901
        """Removes a specific value from the set.

        Args:
            value (Any): The value to remove
            timeout (timedelta, optional): Amount of time allowed when attempting
                to remove the value.  Defaults to 10 seconds.

        """
        with self._span(Op.SetRemove) as span:
            if timeout is None:
                timeout = timedelta(seconds=10)

            timeout_millis = timeout.total_seconds() * 1000

            interval_millis = float(50)
            start = time.perf_counter()
            time_left = timeout_millis
            while True:
                get_res = await self._get(span)
                list_ = get_res.content_as[list]
                val_idx = -1
                for idx, v in enumerate(list_):
                    if v == value:
                        val_idx = idx
                        break

                if val_idx < 0:
                    break

                try:
                    await self._collection.mutate_in(self._key,
                                                     (remove(f'[{val_idx}]'),),
                                                     cas=get_res.cas,
                                                     **parent_span_kwargs(span))
                    break
                except CasMismatchException:
                    pass

                interval_millis += 500
                if interval_millis > 1000:
                    interval_millis = 1000

                time_left = timeout_millis - ((time.perf_counter() - start) * 1000)
                if interval_millis > time_left:
                    interval_millis = time_left

                if time_left <= 0:
                    raise UnAmbiguousTimeoutException(message=f"Unable to remove {value} from the CouchbaseSet.")

                await asyncio.sleep(interval_millis / 1000)

    async def contains(self, value: Any) -> bool:
        """Returns whether a specific value already exists in the set.

        Args:
            value (Any): The value to check for existence.

        Returns:
            bool:  True if the specified value exists in the set.  False otherwise.

        """
        with self._span(Op.SetContains) as span:
            return value in (await self._get(span)).content_as[list]

    async def size(self) -> int:
        """Returns the number of items in the set.

        Returns:
            int: The number of items in the set.

        """
        with self._span(Op.SetSize) as span:
            sd_res = await self._lookup_in(count(''), span)
            return _first_value(sd_res)

    async def clear(self) -> None:
        """Clears the set."""
        await self._clear(Op.SetClear)

    async def values(self) -> list[Any]:
        """Returns a list of all the values which exist in the set.

        Returns:
            list[Any]: The values that exist in the set.
        """
        with self._span(Op.SetValues) as span:
            return (await self._get(span)).content_as[list]


class CouchbaseQueue(_Datastructure):
    """CouchbaseQueue provides a simplified interface for storing a queue within a Couchbase document.

    Args:
        key (str): Document key to use for the queue.
        collection (:class:`~acouchbase.collection.Collection`): The :class:`~acouchbase.collection.Collection`
            where the queue belongs.
    """

    def __init__(self, key: str, collection: AsyncCollection) -> None:
        super().__init__(key, collection)
        self._full_queue: Iterator[Any] | None = None

    async def push(self, value: JSONType) -> None:
        """Adds a new item to the back of the queue.

        Args:
            value (JSONType): The value to push onto the queue.

        """
        with self._span(Op.QueuePush) as span:
            await self._mutate_in(array_prepend('', value), span)

    async def pop(self, timeout: timedelta | None = None) -> Any:
        """Removes an item from the front of the queue.

        Args:
            timeout (timedelta, optional): Amount of time allowed when attempting
                to remove the value.  Defaults to 10 seconds.


        Returns:
            Any: The value that was removed from the front of the queue.
        """
        with self._span(Op.QueuePop) as span:
            if timeout is None:
                timeout = timedelta(seconds=10)

            timeout_millis = timeout.total_seconds() * 1000

            interval_millis = float(50)
            start = time.perf_counter()
            time_left = timeout_millis
            while True:
                try:
                    sd_res = await self._collection.lookup_in(self._key,
                                                              (subdoc_get('[-1]'),),
                                                              **parent_span_kwargs(span))
                    val = _first_value(sd_res)

                    try:
                        await self._collection.mutate_in(self._key,
                                                         (remove('[-1]'),),
                                                         cas=sd_res.cas,
                                                         **parent_span_kwargs(span))
                        return val
                    except CasMismatchException:
                        pass

                    interval_millis += 500
                    if interval_millis > 1000:
                        interval_millis = 1000

                    time_left = timeout_millis - ((time.perf_counter() - start) * 1000)
                    if interval_millis > time_left:
                        interval_millis = time_left

                    if time_left <= 0:
                        raise UnAmbiguousTimeoutException(message="Unable to pop from the CouchbaseQueue.")

                    await asyncio.sleep(interval_millis / 1000)
                except PathNotFoundException:
                    raise QueueEmpty('No items to remove from the queue')

    async def size(self) -> int:
        """Returns the number of items in the queue.

        Returns:
            int: The number of items in the queue.

        """
        with self._span(Op.QueueSize) as span:
            sd_res = await self._lookup_in(count(''), span)
            return _first_value(sd_res)

    async def clear(self) -> None:
        """Clears the queue."""
        await self._clear(Op.QueueClear)

    def __iter__(self) -> NoReturn:
        raise TypeError('CouchbaseQueue is not iterable.  Try using `async for`.')

    def __aiter__(self) -> CouchbaseQueue:
        return self

    async def __anext__(self) -> Any:
        if self._full_queue is None:
            self._full_queue = iter((await self._get()).content_as[list])

        try:
            val = next(self._full_queue)
            # yield to the event loop
            await asyncio.sleep(0)
            return val
        except StopIteration:
            self._full_queue = None
            raise StopAsyncIteration
