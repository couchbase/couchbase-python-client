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

import asyncio
import time
from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Any,
                    Callable,
                    Coroutine,
                    Dict,
                    Generator,
                    List,
                    Optional,
                    Union)

from couchbase.exceptions import (CasMismatchException,
                                  DocumentExistsException,
                                  DocumentNotFoundException,
                                  InvalidArgumentException,
                                  PathExistsException,
                                  PathNotFoundException,
                                  QueueEmpty,
                                  UnAmbiguousTimeoutException)
from couchbase.logic.collection_types import (GetRequest,
                                              LookupInRequest,
                                              MutateInRequest)
from couchbase.options import MutateInOptions
from couchbase.result import LookupInResult
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
    from acouchbase.logic.collection_impl import AsyncCollectionImpl
    from couchbase._utils import JSONType

DataStructureRequest = Union[GetRequest, LookupInRequest, MutateInRequest]


class CouchbaseList:
    def __init__(self, key: str, collection_impl: AsyncCollectionImpl) -> None:
        self._key = key
        self._impl = collection_impl
        self._iter = False
        self._full_list = None

    async def _execute_op(self,
                          fn: Callable[[DataStructureRequest], Coroutine[Any, Any, Any]],
                          req: DataStructureRequest,
                          create_type: Optional[bool] = None) -> Any:
        try:
            return await fn(req)
        except DocumentNotFoundException:
            if create_type is True:
                try:
                    ins_req = self._impl.request_builder.build_insert_request(self._key, list())
                    await self._impl.insert(ins_req)
                except DocumentExistsException:
                    pass
                return await fn(req)
            else:
                raise

    async def _get(self) -> List:
        """
        Get the entire list.
        """
        req = self._impl.request_builder.build_get_request(self._key)
        return await self._execute_op(self._impl.get, req, create_type=True)

    async def append(self, value: JSONType) -> None:
        """
        Add an item to the end of a list.

        :param value: The value to append
        :return: None

        example::

            cb.list_append('a_list', 'hello')
            cb.list_append('a_list', 'world')

        .. seealso:: :meth:`map_add`
        """
        op = array_append('', value)
        req = self._impl.request_builder.build_mutate_in_request(self._key, (op,))
        await self._execute_op(self._impl.mutate_in, req, create_type=True)

    async def prepend(self, value: JSONType) -> None:
        """
        Add an item to the beginning of a list.

        :param value: Value to prepend
        :return: :class:`OperationResult`.

        This function is identical to :meth:`list_append`, except for prepending
        rather than appending the item

        .. seealso:: :meth:`list_append`, :meth:`map_add`
        """
        op = array_prepend('', value)
        req = self._impl.request_builder.build_mutate_in_request(self._key, (op,))
        await self._execute_op(self._impl.mutate_in, req, create_type=True)

    async def set_at(self, index: int, value: JSONType) -> None:
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
            req = self._impl.request_builder.build_mutate_in_request(self._key, (op,))
            await self._execute_op(self._impl.mutate_in, req)
        except PathNotFoundException:
            raise InvalidArgumentException(message=f'Index: {index} is out of range.') from None

    async def get_at(self, index: int) -> Any:
        """
        Get a specific element within a list.

        :param index: The index to retrieve
        :return: value for the element
        :raise: :exc:`IndexError` if the index does not exist
        """
        try:
            op = subdoc_get(f'[{index}]')
            req = self._impl.request_builder.build_lookup_in_request(self._key, (op,))
            sdres: LookupInResult = await self._execute_op(self._impl.lookup_in, req, create_type=True)
            return sdres.value[0].get('value', None)
        except PathNotFoundException:
            raise InvalidArgumentException(message=f'Index: {index} is out of range.') from None

    async def remove_at(self, index: int) -> None:
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
            req = self._impl.request_builder.build_mutate_in_request(self._key, (op,))
            await self._execute_op(self._impl.mutate_in, req)
        except PathNotFoundException:
            raise InvalidArgumentException(message=f'Index: {index} is out of range.') from None

    async def size(self) -> int:
        """
        Retrieve the number of elements in the list.

        :return: The number of elements within the list
        """
        op = count('')
        req = self._impl.request_builder.build_lookup_in_request(self._key, (op,))
        sdres: LookupInResult = await self._execute_op(self._impl.lookup_in, req, create_type=True)
        return sdres.value[0].get("value", None)

    async def index_of(self, value: Any) -> int:
        """
        Retrieve the index of the specified value in the list.

        :param value: the value to look-up
        :return: The index of the specified value, -1 if not found
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
        """

        list_ = await self._get()
        return list_.content_as[list]

    async def clear(self) -> None:
        """
        Clears the list.

        :return: clears the CouchbaseList
        """
        try:
            req = self._impl.request_builder.build_remove_request(self._key)
            await self._impl.remove(req)
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
    def __init__(self, key: str, collection_impl: AsyncCollectionImpl) -> None:
        self._key = key
        self._impl = collection_impl
        self._full_map = None

    async def _execute_op(self,
                          fn: Callable[[DataStructureRequest], Coroutine[Any, Any, Any]],
                          req: DataStructureRequest,
                          create_type: Optional[bool] = None) -> Any:
        try:
            return await fn(req)
        except DocumentNotFoundException:
            if create_type is True:
                try:
                    ins_req = self._impl.request_builder.build_insert_request(self._key, dict())
                    await self._impl.insert(ins_req)
                except DocumentExistsException:
                    pass
                return await fn(req)
            else:
                raise

    async def _get(self) -> Dict:
        """
        Get the entire map.
        """
        req = self._impl.request_builder.build_get_request(self._key)
        return await self._execute_op(self._impl.get, req, create_type=True)

    async def add(self, mapkey: str, value: Any) -> None:
        """
        Set a value for a key in a map.

        These functions are all wrappers around the :meth:`mutate_in` or
        :meth:`lookup_in` methods.

        :param mapkey: The key in the map to set
        :param value: The value to use (anything serializable to JSON)

        .. Initialize a map and add a value

            cb.upsert('a_map', {})
            cb.map_add('a_map', 'some_key', 'some_value')
            cb.map_get('a_map', 'some_key').value  # => 'some_value'
            cb.get('a_map').value  # => {'some_key': 'some_value'}

        """
        op = upsert(mapkey, value)
        req = self._impl.request_builder.build_mutate_in_request(self._key, (op,))
        return await self._execute_op(self._impl.mutate_in, req, create_type=True)

    async def get(self, mapkey: str) -> Any:
        """
        Retrieve a value from a map.

        :param key: The document ID
        :param mapkey: Key within the map to retrieve
        :return: :class:`~.ValueResult`

        .. seealso:: :meth:`map_add` for an example
        """
        op = subdoc_get(mapkey)
        req = self._impl.request_builder.build_lookup_in_request(self._key, (op,))
        sd_res = await self._execute_op(self._impl.lookup_in, req, create_type=True)
        return sd_res.value[0].get('value', None)

    async def remove(self, mapkey: str) -> None:
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
            req = self._impl.request_builder.build_mutate_in_request(self._key, (op,))
            await self._impl.mutate_in(req)
        except PathNotFoundException:
            raise InvalidArgumentException(message=f'Key: {mapkey} is not in the map.') from None

    async def size(self) -> int:
        """
        Get the number of items in the map.

        :param key: The document ID of the map
        :return int: The number of items in the map

        .. seealso:: :meth:`map_add`
        """
        op = count('')
        req = self._impl.request_builder.build_lookup_in_request(self._key, (op,))
        sd_res: LookupInResult = await self._execute_op(self._impl.lookup_in, req, create_type=True)
        return sd_res.value[0].get('value', None)

    async def exists(self, key: str) -> bool:
        """
        Checks whether a specific key exists in the map.

        :param key: The key to check
        :return bool: If the key exists in the map or not

        .. seealso:: :meth:`map_add`
        """
        op = subdoc_exists(key)
        req = self._impl.request_builder.build_lookup_in_request(self._key, (op,))
        sd_res: LookupInResult = await self._execute_op(self._impl.lookup_in, req, create_type=True)
        return sd_res.exists(0)

    async def keys(self) -> List[str]:
        """
        Returns a list of all the keys which exist in the map.

        :return: The keys in CouchbaseMap
        """

        map_ = await self._get()
        return list(map_.content_as[dict].keys())

    async def values(self) -> List[str]:
        """
        Returns a list of all the values which exist in the map.

        :return: The keys in CouchbaseMap
        """

        map_ = await self._get()
        return list(map_.content_as[dict].values())

    async def get_all(self) -> List[Any]:
        """
        Retrieves the entire map.

        :return: The entire CouchbaseMap
        """

        map_ = await self._get()
        return map_.content_as[dict]

    async def clear(self) -> None:
        """
        Clears the map.

        :return: clears the CouchbaseMap
        """
        try:
            req = self._impl.request_builder.build_remove_request(self._key)
            await self._impl.remove(req)
        except DocumentNotFoundException:
            pass

    async def items(self) -> Generator:
        """
        Provide mechanism to loop over the entire map.

        :return: Generator expression for CouchbaseMap
        """

        map_ = await self._get()
        return ((k, v) for k, v in map_.content_as[dict].items())


class CouchbaseSet:
    def __init__(self, key: str, collection_impl: AsyncCollectionImpl) -> None:
        self._key = key
        self._impl = collection_impl

    async def _execute_op(self,
                          fn: Callable[[DataStructureRequest], Coroutine[Any, Any, Any]],
                          req: DataStructureRequest,
                          create_type: Optional[bool] = None) -> Any:
        try:
            return await fn(req)
        except DocumentNotFoundException:
            if create_type is True:
                try:
                    ins_req = self._impl.request_builder.build_insert_request(self._key, list())
                    await self._impl.insert(ins_req)
                except DocumentExistsException:
                    pass
                return await fn(req)
            else:
                raise

    async def _get(self) -> List:
        """
        Get the entire set.
        """
        req = self._impl.request_builder.build_get_request(self._key)
        return await self._execute_op(self._impl.get, req, create_type=True)

    async def add(self, value: Any) -> None:
        """
        Add an item to a set if the item does not yet exist.

        :param value: Value to add
        .. seealso:: :meth:`map_add`
        """
        try:
            op = array_addunique('', value)
            req = self._impl.request_builder.build_mutate_in_request(self._key, (op,))
            await self._execute_op(self._impl.mutate_in, req, create_type=True)
            return True
        except PathExistsException:
            return False

    async def remove(self, value: Any, timeout: Optional[timedelta] = None) -> None:  # noqa: C901
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
                    req = self._impl.request_builder.build_mutate_in_request(self._key,
                                                                             (op,),
                                                                             MutateInOptions(cas=sd_res.cas))
                    await self._impl.mutate_in(req)
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

    async def contains(self, value: Any) -> None:
        """
        Check whether or not the CouchbaseSet contains a value

        :param value: Value to remove
        :return: True if `value` exists in the set, False otherwise

        .. seealso:: :meth:`set_add`, :meth:`map_add`
        """
        sd_res = await self._get()
        list_ = sd_res.content_as[list]
        return value in list_

    async def size(self) -> int:
        """
        Get the number of items in the set.

        :return int: The number of items in the map

        .. seealso:: :meth:`map_add`
        """
        op = count('')
        req = self._impl.request_builder.build_lookup_in_request(self._key, (op,))
        sd_res: LookupInResult = await self._execute_op(self._impl.lookup_in, req, create_type=True)
        return sd_res.value[0].get('value', None)

    async def clear(self) -> None:
        """
        Clears the set.

        :return: clears the CouchbaseSet
        """
        try:
            req = self._impl.request_builder.build_remove_request(self._key)
            await self._impl.remove(req)
        except DocumentNotFoundException:
            pass

    async def values(self) -> List[Any]:
        """
        Returns a list of all the values which exist in the set.

        :return: The keys in CouchbaseSet
        """

        list_ = await self._get()
        return list_.content_as[list]


class CouchbaseQueue:
    def __init__(self, key: str, collection_impl: AsyncCollectionImpl) -> None:
        self._key = key
        self._impl = collection_impl
        self._full_queue = None
        self._iter = False

    async def _execute_op(self,
                          fn: Callable[[DataStructureRequest], Coroutine[Any, Any, Any]],
                          req: DataStructureRequest,
                          create_type: Optional[bool] = None) -> Any:
        try:
            return await fn(req)
        except DocumentNotFoundException:
            if create_type is True:
                try:
                    ins_req = self._impl.request_builder.build_insert_request(self._key, list())
                    await self._impl.insert(ins_req)
                except DocumentExistsException:
                    pass
                return await fn(req)
            else:
                raise

    async def _get(self) -> List:
        """
        Get the entire queuee.
        """
        req = self._impl.request_builder.build_get_request(self._key)
        return await self._execute_op(self._impl.get, req, create_type=True)

    async def push(self, value: JSONType) -> None:
        """
        Add an item to the queue.

        :param value: Value to push onto queue
        """
        op = array_prepend('', value)
        req = self._impl.request_builder.build_mutate_in_request(self._key, (op,))
        await self._execute_op(self._impl.mutate_in, req, create_type=True)

    async def pop(self, timeout: Optional[timedelta] = None) -> None:
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
                lookup_in_req = self._impl.request_builder.build_lookup_in_request(self._key, (op,))
                sd_res = await self._impl.lookup_in(lookup_in_req)
                val = sd_res.value[0].get('value', None)

                try:
                    op = remove('[-1]')
                    mutate_in_opts = MutateInOptions(cas=sd_res.cas)
                    mutate_in_req = self._impl.request_builder.build_mutate_in_request(self._key,
                                                                                       (op,),
                                                                                       mutate_in_opts)
                    await self._impl.mutate_in(mutate_in_req)
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
        """
        Get the number of items in the queue.

        :return int: The number of items in the queue

        .. seealso:: :meth:`map_add`
        """
        op = count('')
        req = self._impl.request_builder.build_lookup_in_request(self._key, (op,))
        sd_res: LookupInResult = await self._execute_op(self._impl.lookup_in, req, create_type=True)
        return sd_res.value[0].get('value', None)

    async def clear(self) -> None:
        """
        Clears the queue.

        :return: clears the CouchbaseQueue
        :raise: :cb_exc:`DocumentNotFoundException` if the list does not exist
        """
        try:
            req = self._impl.request_builder.build_remove_request(self._key)
            await self._impl.remove(req)
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
