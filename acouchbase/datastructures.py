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

import asyncio
import time
from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Generator,
                    List,
                    Optional)

from acouchbase.logic.wrappers import AsyncWrapper
from couchbase.exceptions import (CasMismatchException,
                                  DocumentNotFoundException,
                                  InvalidArgumentException,
                                  PathExistsException,
                                  PathNotFoundException,
                                  QueueEmpty,
                                  UnAmbiguousTimeoutException)
from couchbase.options import MutateInOptions
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
    from acouchbase.collection import Collection
    from couchbase._utils import JSONType


class CouchbaseList:
    def __init__(self, key,  # type: str
                 collection  # type: Collection
                 ) -> None:
        self._key = key
        self._collection = collection
        self._iter = False
        self._full_list = None

    @AsyncWrapper.datastructure_op(create_type=list)
    async def _get(self) -> List:
        """
        Get the entire list.
        """

        return await self._collection.get(self._key)

    @AsyncWrapper.datastructure_op(create_type=list)
    async def append(self, value  # type: JSONType
                     ) -> None:
        """
        Add an item to the end of a list.

        :param value: The value to append
        :return: None
        :raise: :cb_exc:`DocumentNotFoundException` if the document does not exist.
            and `create` was not specified.

        example::

            cb.list_append('a_list', 'hello')
            cb.list_append('a_list', 'world')

        .. seealso:: :meth:`map_add`
        """
        op = array_append('', value)
        await self._collection.mutate_in(self._key, (op,))

    @AsyncWrapper.datastructure_op(create_type=list)
    async def prepend(self, value  # type: JSONType
                      ) -> None:
        """
        Add an item to the beginning of a list.

        :param value: Value to prepend
        :return: :class:`OperationResult`.
        :raise: :cb_exc:`DocumentNotFoundException` if the document does not exist.
            and `create` was not specified.

        This function is identical to :meth:`list_append`, except for prepending
        rather than appending the item

        .. seealso:: :meth:`list_append`, :meth:`map_add`
        """
        op = array_prepend('', value)
        await self._collection.mutate_in(self._key, (op,))

    async def set_at(self, index,  # type: int
                     value  # type: JSONType
                     ) -> None:
        """
        Sets an item within a list at a given position.

        :param index: The position to replace
        :param value: The value to be inserted
        :raise: :cb_exc:`DocumentNotFoundException` if the list does not exist
        :raise: :exc:`IndexError` if the index is out of bounds

        example::

            cb.upsert('a_list', ['hello', 'world'])
            cb.list_set('a_list', 1, 'good')
            cb.get('a_list').value # => ['hello', 'good']

        .. seealso:: :meth:`map_add`, :meth:`list_append`
        """
        try:
            op = replace(f'[{index}]', value)
            await self._collection.mutate_in(self._key, (op,))
        except PathNotFoundException:
            raise InvalidArgumentException(message=f'Index: {index} is out of range.') from None

    @AsyncWrapper.datastructure_op(create_type=list)
    async def get_at(self, index  # type: int
                     ) -> Any:
        """
        Get a specific element within a list.

        :param index: The index to retrieve
        :return: value for the element
        :raise: :exc:`IndexError` if the index does not exist
        :raise: :cb_exc:`DocumentNotFoundException` if the list does not exist
        """
        try:
            op = subdoc_get(f'[{index}]')
            sdres = await self._collection.lookup_in(self._key, (op,))
            return sdres.value[0].get("value", None)
        except PathNotFoundException:
            raise InvalidArgumentException(message=f'Index: {index} is out of range.') from None

    async def remove_at(self, index  # type: int
                        ) -> None:
        """
        Remove the element at a specific index from a list.

        :param index: The index to remove
        :param kwargs: Arguments to :meth:`mutate_in`
        :return: :class:`OperationResult`
        :raise: :exc:`IndexError` if the index does not exist
        :raise: :cb_exc:`DocumentNotFoundException` if the list does not exist
        """
        try:
            op = remove(f'[{index}]')
            await self._collection.mutate_in(self._key, (op,))
        except PathNotFoundException:
            raise InvalidArgumentException(message=f'Index: {index} is out of range.') from None

    @AsyncWrapper.datastructure_op(create_type=list)
    async def size(self) -> int:
        """
        Retrieve the number of elements in the list.

        :return: The number of elements within the list
        :raise: :cb_exc:`DocumentNotFoundException` if the list does not exist
        """
        op = count('')
        sdres = await self._collection.lookup_in(self._key, (op,))
        return sdres.value[0].get("value", None)

    @AsyncWrapper.datastructure_op(create_type=list)
    async def index_of(self, value  # type: Any
                       ) -> int:
        """
        Retrieve the index of the specified value in the list.

        :param value: the value to look-up
        :return: The index of the specified value, -1 if not found
        :raise: :cb_exc:`DocumentNotFoundException` if the list does not exist
        """

        list_ = await self._get()
        for idx, val in enumerate(list_.content_as[list]):
            if val == value:
                return idx

        return -1

    async def get_all(self) -> List[Any]:
        """
        Retrieves the entire list.

        :return: The entire CouchbaseList
        :raise: :cb_exc:`DocumentNotFoundException` if the list does not exist
        """

        list_ = await self._get()
        return list_.content_as[list]

    async def clear(self) -> None:
        """
        Clears the list.

        :return: clears the CouchbaseList
        :raise: :cb_exc:`DocumentNotFoundException` if the list does not exist
        """
        try:
            await self._collection.remove(self._key)
        except DocumentNotFoundException:
            pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._iter is False:
            list_ = await self._get()
            self._full_list = (v for v in list_.content_as[list])
            self._iter = True

        try:
            val = next(self._full_list)
            # yield to the event loop
            await asyncio.sleep(0)
            return val
        except StopIteration:
            self._iter = False
            raise StopAsyncIteration


class CouchbaseMap:
    def __init__(self, key,  # type: str
                 collection  # type: Collection
                 ) -> None:
        self._key = key
        self._collection = collection
        self._full_map = None

    @AsyncWrapper.datastructure_op(create_type=dict)
    async def _get(self) -> Dict:
        """
        Get the entire map.
        """
        return await self._collection.get(self._key)

    @AsyncWrapper.datastructure_op(create_type=dict)
    async def add(self, mapkey,  # type: str
                  value  # type: Any
                  ) -> None:
        """
        Set a value for a key in a map.

        These functions are all wrappers around the :meth:`mutate_in` or
        :meth:`lookup_in` methods.

        :param mapkey: The key in the map to set
        :param value: The value to use (anything serializable to JSON)
        :raise: :cb_exc:`Document.DocumentNotFoundException` if the document does not exist.
            and `create` was not specified

        .. Initialize a map and add a value

            cb.upsert('a_map', {})
            cb.map_add('a_map', 'some_key', 'some_value')
            cb.map_get('a_map', 'some_key').value  # => 'some_value'
            cb.get('a_map').value  # => {'some_key': 'some_value'}

        """
        op = upsert(mapkey, value)
        await self._collection.mutate_in(self._key, (op,))

    @AsyncWrapper.datastructure_op(create_type=dict)
    async def get(self, mapkey,  # type: str
                  ) -> Any:
        """
        Retrieve a value from a map.

        :param key: The document ID
        :param mapkey: Key within the map to retrieve
        :return: :class:`~.ValueResult`
        :raise: :exc:`IndexError` if the mapkey does not exist
        :raise: :cb_exc:`DocumentNotFoundException` if the document does not exist.

        .. seealso:: :meth:`map_add` for an example
        """
        op = subdoc_get(mapkey)
        sd_res = await self._collection.lookup_in(self._key, (op,))
        return sd_res.value[0].get("value", None)

    async def remove(self, mapkey  # type: str
                     ) -> None:
        """
        Remove an item from a map.

        :param key: The document ID
        :param mapkey: The key in the map
        :param See:meth:`mutate_in` for options
        :raise: :exc:`IndexError` if the mapkey does not exist
        :raise: :cb_exc:`DocumentNotFoundException` if the document does not exist.

        .. Remove a map key-value pair:

            cb.map_remove('a_map', 'some_key')

        .. seealso:: :meth:`map_add`
        """
        try:
            op = remove(mapkey)
            await self._collection.mutate_in(self._key, (op,))
        except PathNotFoundException:
            raise InvalidArgumentException(message=f'Key: {mapkey} is not in the map.') from None

    @AsyncWrapper.datastructure_op(create_type=dict)
    async def size(self) -> int:
        """
        Get the number of items in the map.

        :param key: The document ID of the map
        :return int: The number of items in the map
        :raise: :cb_exc:`DocumentNotFoundException` if the document does not exist.

        .. seealso:: :meth:`map_add`
        """
        op = count('')
        sd_res = await self._collection.lookup_in(self._key, (op,))
        return sd_res.value[0].get("value", None)

    @AsyncWrapper.datastructure_op(create_type=dict)
    async def exists(self, key  # type: Any
                     ) -> bool:
        """
        hecks whether a specific key exists in the map.

        :param key: The key to check
        :return bool: If the key exists in the map or not
        :raise: :cb_exc:`DocumentNotFoundException` if the document does not exist.

        .. seealso:: :meth:`map_add`
        """
        op = subdoc_exists(key)
        sd_res = await self._collection.lookup_in(self._key, (op,))
        return sd_res.exists(0)

    async def keys(self) -> List[str]:
        """
        Returns a list of all the keys which exist in the map.

        :return: The keys in CouchbaseMap
        :raise: :cb_exc:`DocumentNotFoundException` if the map does not exist
        """

        map_ = await self._get()
        return list(map_.content_as[dict].keys())

    async def values(self) -> List[str]:
        """
        Returns a list of all the values which exist in the map.

        :return: The keys in CouchbaseMap
        :raise: :cb_exc:`DocumentNotFoundException` if the map does not exist
        """

        map_ = await self._get()
        return list(map_.content_as[dict].values())

    async def get_all(self) -> List[Any]:
        """
        Retrieves the entire map.

        :return: The entire CouchbaseMap
        :raise: :cb_exc:`DocumentNotFoundException` if the list does not exist
        """

        map_ = await self._get()
        return map_.content_as[dict]

    async def clear(self) -> None:
        """
        Clears the map.

        :return: clears the CouchbaseMap
        :raise: :cb_exc:`DocumentNotFoundException` if the list does not exist
        """
        try:
            await self._collection.remove(self._key)
        except DocumentNotFoundException:
            pass

    async def items(self) -> Generator:
        """
        Provide mechanism to loop over the entire map.

        :return: Generator expression for CouchbaseMap
        :raise: :cb_exc:`DocumentNotFoundException` if the list does not exist
        """

        map_ = await self._get()
        return ((k, v) for k, v in map_.content_as[dict].items())


class CouchbaseSet:
    def __init__(self, key,  # type: str
                 collection  # type: Collection
                 ) -> None:
        self._key = key
        self._collection = collection

    @AsyncWrapper.datastructure_op(create_type=list)
    async def _get(self) -> List:
        """
        Get the entire set.
        """
        return await self._collection.get(self._key)

    @AsyncWrapper.datastructure_op(create_type=list)
    async def add(self, value  # type: Any
                  ) -> None:
        """
        Add an item to a set if the item does not yet exist.

        :param value: Value to add
        .. seealso:: :meth:`map_add`
        """
        try:
            op = array_addunique('', value)
            await self._collection.mutate_in(self._key, (op,))
            return True
        except PathExistsException:
            return False

    async def remove(self, value,  # type: Any  # noqa: C901
                     timeout=None  # type: Optional[timedelta]
                     ) -> None:
        """
        Remove an item from a set.

        :param value: Value to remove
        :param kwargs: Arguments to :meth:`mutate_in`
        :raise: :cb_exc:`DocumentNotFoundException` if the set does not exist.

        .. seealso:: :meth:`set_add`, :meth:`map_add`
        """

        if timeout is None:
            timeout = timedelta(seconds=10)

        timeout_millis = timeout.total_seconds() * 1000

        interval_millis = float(50)
        start = time.perf_counter()
        time_left = timeout_millis
        while True:
            sd_res = await self._get()
            list_ = sd_res.content_as[list]
            val_idx = -1
            for idx, v in enumerate(list_):
                if v == value:
                    val_idx = idx
                    break

            if val_idx >= 0:
                try:
                    op = remove(f'[{val_idx}]')
                    await self._collection.mutate_in(self._key, (op,), MutateInOptions(cas=sd_res.cas))
                    break
                except CasMismatchException:
                    pass
            else:
                break

            interval_millis += 500
            if interval_millis > 1000:
                interval_millis = 1000

            time_left = timeout_millis - ((time.perf_counter() - start) * 1000)
            if interval_millis > time_left:
                interval_millis = time_left

            if time_left <= 0:
                raise UnAmbiguousTimeoutException(message=f"Unable to remove {value} from the CouchbaseSet.")

            await asyncio.sleep(interval_millis / 1000)

    @AsyncWrapper.datastructure_op(create_type=list)
    async def contains(self, value  # type: Any
                       ) -> None:
        """
        Check whether or not the CouchbaseSet contains a value

        :param value: Value to remove
        :return: True if `value` exists in the set, False otherwise
        :raise: :cb_exc:`DocumentNotFoundException` if the set does not exist.

        .. seealso:: :meth:`set_add`, :meth:`map_add`
        """
        sd_res = await self._get()
        list_ = sd_res.content_as[list]
        return value in list_

    @AsyncWrapper.datastructure_op(create_type=list)
    async def size(self) -> int:
        """
        Get the number of items in the set.

        :return int: The number of items in the map
        :raise: :cb_exc:`DocumentNotFoundException` if the document does not exist.

        .. seealso:: :meth:`map_add`
        """
        op = count('')
        sd_res = await self._collection.lookup_in(self._key, (op,))
        return sd_res.value[0].get("value", None)

    async def clear(self) -> None:
        """
        Clears the set.

        :return: clears the CouchbaseSet
        """
        try:
            await self._collection.remove(self._key)
        except DocumentNotFoundException:
            pass

    @AsyncWrapper.datastructure_op(create_type=list)
    async def values(self) -> List[Any]:
        """
        Returns a list of all the values which exist in the set.

        :return: The keys in CouchbaseSet
        :raise: :cb_exc:`DocumentNotFoundException` if the map does not exist
        """

        list_ = await self._get()
        return list_.content_as[list]


class CouchbaseQueue:
    def __init__(self, key,  # type: str
                 collection  # type: Collection
                 ) -> None:
        self._key = key
        self._collection = collection
        self._full_queue = None
        self._iter = False

    @AsyncWrapper.datastructure_op(create_type=list)
    async def _get(self) -> List:
        """
        Get the entire queuee.
        """
        return await self._collection.get(self._key)

    @AsyncWrapper.datastructure_op(create_type=list)
    async def push(self, value  # type: JSONType
                   ) -> None:
        """
        Add an item to the queue.

        :param value: Value to push onto queue
        """
        op = array_prepend('', value)
        await self._collection.mutate_in(self._key, (op,))

    async def pop(self, timeout=None  # type: Optional[timedelta]
                  ) -> None:
        """
        Pop an item from the queue.

        :param value: Value to remove
        :raise: :cb_exc:`DocumentNotFoundException` if the set does not exist.

        .. seealso:: :meth:`set_add`, :meth:`map_add`
        """

        if timeout is None:
            timeout = timedelta(seconds=10)

        timeout_millis = timeout.total_seconds() * 1000

        interval_millis = float(50)
        start = time.perf_counter()
        time_left = timeout_millis
        while True:
            try:
                op = subdoc_get('[-1]')
                sd_res = await self._collection.lookup_in(self._key, (op,))
                val = sd_res.value[0].get("value", None)

                try:
                    op = remove('[-1]')
                    await self._collection.mutate_in(self._key, (op,), MutateInOptions(cas=sd_res.cas))
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

    @AsyncWrapper.datastructure_op(create_type=list)
    async def size(self) -> int:
        """
        Get the number of items in the queue.

        :return int: The number of items in the queue
        :raise: :cb_exc:`DocumentNotFoundException` if the document does not exist.

        .. seealso:: :meth:`map_add`
        """
        op = count('')
        sd_res = await self._collection.lookup_in(self._key, (op,))
        return sd_res.value[0].get("value", None)

    async def clear(self) -> None:
        """
        Clears the queue.

        :return: clears the CouchbaseQueue
        :raise: :cb_exc:`DocumentNotFoundException` if the list does not exist
        """
        try:
            await self._collection.remove(self._key)
        except DocumentNotFoundException:
            pass

    def __iter__(self):
        raise TypeError('CouchbaseQueue is not iterable.  Try using `async for`.')

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._iter is False:
            list_ = await self._get()
            self._full_queue = (v for v in list_.content_as[list])
            self._iter = True

        try:
            val = next(self._full_queue)
            # yield to the event loop
            await asyncio.sleep(0)
            return val
        except StopIteration:
            self._iter = False
            raise StopAsyncIteration
