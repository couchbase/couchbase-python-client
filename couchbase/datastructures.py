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

import time
from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Generator,
                    List,
                    Optional)

from couchbase.exceptions import (CasMismatchException,
                                  DocumentNotFoundException,
                                  InvalidArgumentException,
                                  PathExistsException,
                                  PathNotFoundException,
                                  QueueEmpty,
                                  UnAmbiguousTimeoutException)
from couchbase.logic.wrappers import BlockingWrapper
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
    from couchbase._utils import JSONType
    from couchbase.collection import Collection


class CouchbaseList:
    """
    CouchbaseList provides a simplified interface for storing lists within a Couchbase document.

    Args:
        key (str): Document key to use for the list.
        collection (:class:`~.collection.Collection`): The :class:`~.collection.Collection` where the
            list belongs

    """

    def __init__(self, key,  # type: str
                 collection  # type: Collection
                 ) -> None:
        self._key = key
        self._collection = collection
        self._full_list = None

    @BlockingWrapper.datastructure_op(create_type=list)
    def _get(self) -> List:
        """
        Get the entire list.
        """

        return self._collection.get(self._key)

    @BlockingWrapper.datastructure_op(create_type=list)
    def append(self, value  # type: JSONType
               ) -> None:
        """Add an item to the end of the list.

        Args:
            value (JSONType): The value to add.

        """
        op = array_append('', value)
        self._collection.mutate_in(self._key, (op,))

    @BlockingWrapper.datastructure_op(create_type=list)
    def prepend(self, value  # type: JSONType
                ) -> None:
        """Add an item to the beginning of the list.

        Args:
            value (JSONType): The value to add.

        """
        op = array_prepend('', value)
        self._collection.mutate_in(self._key, (op,))

    def set_at(self, index,  # type: int
               value  # type: JSONType
               ) -> None:
        """Sets an item within a list at a specified index.

        Args:
            index (int): The index to retrieve.
            value (JSONType): The value to set.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index is out of range.

        """
        try:
            op = replace(f'[{index}]', value)
            self._collection.mutate_in(self._key, (op,))
        except PathNotFoundException:
            raise InvalidArgumentException(message=f'Index: {index} is out of range.') from None

    @BlockingWrapper.datastructure_op(create_type=list)
    def get_at(self, index  # type: int
               ) -> Any:
        """Retrieves the item at a specific index in the list.

        Args:
            index (int): The index to retrieve.

        Returns:
            Any: The value of the element at the specified index.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index is out of range.

        """
        try:
            op = subdoc_get(f'[{index}]')
            sdres = self._collection.lookup_in(self._key, (op,))
            return sdres.value[0].get("value", None)
        except PathNotFoundException:
            raise InvalidArgumentException(message=f'Index: {index} is out of range.') from None

    def remove_at(self, index  # type: int
                  ) -> None:
        """Removes an item at a specific index from the list.

        Args:
            index (int): The index to remove.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index is out of range.

        """
        try:
            op = remove(f'[{index}]')
            self._collection.mutate_in(self._key, (op,))
        except PathNotFoundException:
            raise InvalidArgumentException(message=f'Index: {index} is out of range.') from None

    @BlockingWrapper.datastructure_op(create_type=list)
    def size(self) -> int:
        """Returns the number of items in the list.

        Returns:
            int: The number of items in the list.

        """
        op = count('')
        sdres = self._collection.lookup_in(self._key, (op,))
        return sdres.value[0].get("value", None)

    @BlockingWrapper.datastructure_op(create_type=list)
    def index_of(self, value  # type: JSONType
                 ) -> int:
        """Returns the index of a specific value from the list.

        Args:
            value (JSONType): The value to search for.

        Returns:
            int: The index of the value in the list. Returns -1 if value is not found.

        """
        list_ = self._get()
        for idx, val in enumerate(list_.content_as[list]):
            if val == value:
                return idx

        return -1

    def get_all(self) -> List[Any]:
        """Returns the entire list of items in this list.

        Returns:
            int: The entire list.

        """

        list_ = self._get()
        return list_.content_as[list]

    def clear(self) -> None:
        """Clears the list.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the list does not already exist.
        """
        try:
            self._collection.remove(self._key)
        except DocumentNotFoundException:
            pass

    def __iter__(self):
        list_ = self._get()
        self._full_list = (v for v in list_.content_as[list])
        return self

    def __next__(self):
        return next(self._full_list)


class CouchbaseMap:
    """
    CouchbaseMap provides a simplified interface for storing a map within a Couchbase document.

    Args:
        key (str): Document key to use for the map.
        collection (:class:`~.collection.Collection`): The :class:`~.collection.Collection` where the
            map belongs

    """

    def __init__(self, key,  # type: str
                 collection  # type: Collection
                 ) -> None:
        self._key = key
        self._collection = collection
        self._full_map = None

    @BlockingWrapper.datastructure_op(create_type=dict)
    def _get(self) -> Dict[str, Any]:
        """
        Get the entire map.
        """
        return self._collection.get(self._key)

    @BlockingWrapper.datastructure_op(create_type=dict)
    def add(self,
            mapkey,  # type: str
            value  # type: Any
            ) -> None:
        """Sets a specific key to the specified value in the map.

        Args:
            mapkey (str): The key to set.
            value (JSONType): The value to set.

        """
        op = upsert(mapkey, value)
        self._collection.mutate_in(self._key, (op,))

    @BlockingWrapper.datastructure_op(create_type=dict)
    def get(self,
            mapkey,  # type: str
            ) -> Any:
        """Fetches a specific key from the map.

        Args:
            mapkey (str): The key to fetch.

        Returns:
            Any: The value of the specified key.

        """
        op = subdoc_get(mapkey)
        sd_res = self._collection.lookup_in(self._key, (op,))
        return sd_res.value[0].get("value", None)

    def remove(self,
               mapkey  # type: str
               ) -> None:
        """Removes a specific key from the map.

        Args:
            mapkey (str): The key in the map to remove.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the key is not in the map.

        """
        try:
            op = remove(mapkey)
            self._collection.mutate_in(self._key, (op,))
        except PathNotFoundException:
            raise InvalidArgumentException(message=f'Key: {mapkey} is not in the map.') from None

    @BlockingWrapper.datastructure_op(create_type=dict)
    def size(self) -> int:
        """Returns the number of items in the map.

        Returns:
            int: The number of items in the map.

        """
        op = count('')
        sd_res = self._collection.lookup_in(self._key, (op,))
        return sd_res.value[0].get("value", None)

    @BlockingWrapper.datastructure_op(create_type=dict)
    def exists(self,
               key  # type: str
               ) -> bool:
        """Checks whether a specific key exists in the map.

        Args:
            key (str): The key to set.

        Returns:
            bool: True if the key exists in the map, False otherwise.

        """
        op = subdoc_exists(key)
        sd_res = self._collection.lookup_in(self._key, (op,))
        return sd_res.exists(0)

    def keys(self) -> List[str]:
        """Returns a list of all the keys which exist in the map.

        Returns:
            List[str]: A list of all the keys that exist in the map.
        """

        map_ = self._get()
        return list(map_.content_as[dict].keys())

    def values(self) -> List[Any]:
        """Returns a list of all the values which exist in the map.

        Returns:
            List[Any]: A list of all the values that exist in the map.
        """

        map_ = self._get()
        return list(map_.content_as[dict].values())

    def get_all(self) -> Dict[str, Any]:
        """Retrieves the entire map.

        Returns:
            Dict[str, Any]: The entire CouchbaseMap.
        """
        map_ = self._get()
        return map_.content_as[dict]

    def clear(self) -> None:
        """Clears the map.
        """
        try:
            self._collection.remove(self._key)
        except DocumentNotFoundException:
            pass

    def items(self) -> Generator:
        """Provides mechanism to loop over the entire map.

        Returns:
            Generator:  A generator expression for the map
        """

        map_ = self._get()
        return ((k, v) for k, v in map_.content_as[dict].items())


class CouchbaseSet:
    """
    CouchbaseSet provides a simplified interface for storing a set within a Couchbase document.

    Args:
        key (str): Document key to use for the set.
        collection (:class:`~.collection.Collection`): The :class:`~.collection.Collection` where the
            set belongs.

    """

    def __init__(self,
                 key,  # type: str
                 collection  # type: Collection
                 ) -> None:
        self._key = key
        self._collection = collection

    @BlockingWrapper.datastructure_op(create_type=list)
    def _get(self) -> List:
        """
        Get the entire set.
        """
        return self._collection.get(self._key)

    @BlockingWrapper.datastructure_op(create_type=list)
    def add(self,
            value  # type: Any
            ) -> bool:
        """Adds a new item to the set. Returning whether the item already existed in the set or not.

        Args:
            value (Any):

        Returns:
            bool:  True if the value was added, False otherwise (meaning the value already
                exists in the set).

        """
        try:
            op = array_addunique('', value)
            self._collection.mutate_in(self._key, (op,))
            return True
        except PathExistsException:
            return False

    def remove(self,   # noqa: C901
               value,  # type: Any
               timeout=None  # type: Optional[timedelta]
               ) -> None:
        """Removes a specific value from the set.

        Args:
            value (Any): The value to remove
            timeout (timedelta, optional): Amount of time allowed when attempting
                to remove the value.  Defaults to 10 seconds.

        """

        if timeout is None:
            timeout = timedelta(seconds=10)

        timeout_millis = timeout.total_seconds() * 1000

        interval_millis = float(50)
        start = time.perf_counter()
        time_left = timeout_millis
        while True:
            sd_res = self._get()
            list_ = sd_res.content_as[list]
            val_idx = -1
            for idx, v in enumerate(list_):
                if v == value:
                    val_idx = idx
                    break

            if val_idx >= 0:
                try:
                    op = remove(f'[{val_idx}]')
                    self._collection.mutate_in(self._key, (op,), MutateInOptions(cas=sd_res.cas))
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

            time.sleep(interval_millis / 1000)

    @BlockingWrapper.datastructure_op(create_type=list)
    def contains(self,
                 value  # type: Any
                 ) -> bool:
        """Returns whether a specific value already exists in the set.

        Args:
            value (Any): The value to check for existence.

        Returns:
            bool:  True if the specified value exists in the set.  False otherwise.

        """
        list_ = self._get().content_as[list]
        return value in list_

    @BlockingWrapper.datastructure_op(create_type=list)
    def size(self) -> int:
        """Returns the number of items in the set.

        Returns:
            int: The number of items in the set.

        """
        op = count('')
        sd_res = self._collection.lookup_in(self._key, (op,))
        return sd_res.value[0].get("value", None)

    def clear(self) -> None:
        """Clears the set.
        """
        try:
            self._collection.remove(self._key)
        except DocumentNotFoundException:
            pass

    @BlockingWrapper.datastructure_op(create_type=list)
    def values(self) -> List[Any]:
        """Returns a list of all the values which exist in the set.

        Returns:
            List[Any]: The values that exist in the set.
        """

        list_ = self._get()
        return list_.content_as[list]


class CouchbaseQueue:
    """
    CouchbaseQueue provides a simplified interface for storing a queue within a Couchbase document.

    Args:
        key (str): Document key to use for the queue.
        collection (:class:`~.collection.Collection`): The :class:`~.collection.Collection` where the
            queue belongs.

    """

    def __init__(self,
                 key,  # type: str
                 collection  # type: Collection
                 ) -> None:
        self._key = key
        self._collection = collection
        self._full_queue = None

    @BlockingWrapper.datastructure_op(create_type=list)
    def _get(self) -> List:
        """
        Get the entire queuee.
        """
        return self._collection.get(self._key)

    @BlockingWrapper.datastructure_op(create_type=list)
    def push(self,
             value  # type: JSONType
             ) -> None:
        """Adds a new item to the back of the queue.

        Args:
            value (JSONType): The value to push onto the queue.

        """
        op = array_prepend('', value)
        self._collection.mutate_in(self._key, (op,))

    def pop(self,
            timeout=None  # type: Optional[timedelta]
            ) -> Any:
        """Removes an item from the front of the queue.

        Args:
            timeout (timedelta, optional): Amount of time allowed when attempting
                to remove the value.  Defaults to 10 seconds.


        Returns:
            Any: The value that was removed from the front of the queue.
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
                sd_res = self._collection.lookup_in(self._key, (op,))
                val = sd_res.value[0].get("value", None)

                try:
                    op = remove('[-1]')
                    self._collection.mutate_in(self._key, (op,), MutateInOptions(cas=sd_res.cas))
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

                time.sleep(interval_millis / 1000)
            except PathNotFoundException:
                raise QueueEmpty('No items to remove from the queue')

    @BlockingWrapper.datastructure_op(create_type=list)
    def size(self) -> int:
        """Returns the number of items in the queue.

        Returns:
            int: The number of items in the queue.

        """
        op = count('')
        sd_res = self._collection.lookup_in(self._key, (op,))
        return sd_res.value[0].get("value", None)

    def clear(self) -> None:
        """Clears the queue.
        """
        try:
            self._collection.remove(self._key)
        except DocumentNotFoundException:
            pass

    def __iter__(self):
        list_ = self._get()
        self._full_queue = (v for v in list_.content_as[list])
        return self

    def __next__(self):
        return next(self._full_queue)
