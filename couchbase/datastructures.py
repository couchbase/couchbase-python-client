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

import time
from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Any,
                    Callable,
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
from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import DatastructureOperationType, KeyValueOperationType
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
    from couchbase.logic.collection_impl import CollectionImpl
    from couchbase.logic.observability import WrappedSpan
    from couchbase.result import LookupInResult

DataStructureRequest = Union[GetRequest, LookupInRequest, MutateInRequest]


class CouchbaseList:
    """
    CouchbaseList provides a simplified interface for storing lists within a Couchbase document.

    Args:
        key (str): Document key to use for the list.
        collection (:class:`~.collection.Collection`): The :class:`~.collection.Collection` where the
            list belongs

    """

    def __init__(self, key: str, collection_impl: CollectionImpl) -> None:
        self._key = key
        self._impl = collection_impl
        self._full_list = None

    def _execute_op(self,
                    fn: Callable[[DataStructureRequest], Any],
                    req: DataStructureRequest,
                    obs_handler: ObservableRequestHandler,
                    parent_span: WrappedSpan,
                    create_type: Optional[bool] = None) -> Any:
        try:
            return fn(req, obs_handler)
        except DocumentNotFoundException:
            if create_type is True:
                orig_opt_type = obs_handler.op_type
                obs_handler.reset(KeyValueOperationType.Insert, with_error=True)
                try:
                    ins_req = self._impl.request_builder.build_insert_request(self._key,
                                                                              list(),
                                                                              obs_handler,
                                                                              parent_span=parent_span)
                    self._impl.insert(ins_req, obs_handler)
                except DocumentExistsException:
                    pass
                obs_handler.reset(orig_opt_type)
                obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict(),
                                           parent_span=parent_span)
                return fn(req, obs_handler)
            else:
                raise

    def _get(self, parent_span: Optional[WrappedSpan] = None) -> List[Any]:
        """
        Get the entire list.
        """
        op_type = KeyValueOperationType.Get
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_request(self._key, obs_handler, parent_span=parent_span)
            return self._execute_op(self._impl.get,
                                    req,
                                    obs_handler,
                                    parent_span=parent_span,
                                    create_type=True)

    def append(self, value: JSONType) -> None:
        """Add an item to the end of the list.

        Args:
            value (JSONType): The value to add.

        """
        op_type = DatastructureOperationType.ListAppend
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            kv_op_type = KeyValueOperationType.MutateIn
            with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                op = array_append('', value)
                req = self._impl.request_builder.build_mutate_in_request(self._key,
                                                                         (op,),
                                                                         obs_handler,
                                                                         parent_span=ds_obs_handler.wrapped_span)
                self._execute_op(self._impl.mutate_in,
                                 req,
                                 obs_handler,
                                 ds_obs_handler.wrapped_span,
                                 create_type=True)

    def prepend(self, value: JSONType) -> None:
        """Add an item to the beginning of the list.

        Args:
            value (JSONType): The value to add.

        """
        op_type = DatastructureOperationType.ListPrepend
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            kv_op_type = KeyValueOperationType.MutateIn
            with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                op = array_prepend('', value)
                req = self._impl.request_builder.build_mutate_in_request(self._key,
                                                                         (op,),
                                                                         obs_handler,
                                                                         parent_span=ds_obs_handler.wrapped_span)
                self._execute_op(self._impl.mutate_in,
                                 req,
                                 obs_handler,
                                 ds_obs_handler.wrapped_span,
                                 create_type=True)

    def set_at(self, index: int, value: JSONType) -> None:
        """Sets an item within a list at a specified index.

        Args:
            index (int): The index to retrieve.
            value (JSONType): The value to set.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index is out of range.

        """
        op_type = DatastructureOperationType.ListSetAt
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            kv_op_type = KeyValueOperationType.MutateIn
            with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                try:
                    op = replace(f'[{index}]', value)
                    req = self._impl.request_builder.build_mutate_in_request(self._key,
                                                                             (op,),
                                                                             obs_handler,
                                                                             parent_span=ds_obs_handler.wrapped_span)
                    self._execute_op(self._impl.mutate_in,
                                     req,
                                     obs_handler,
                                     ds_obs_handler.wrapped_span)
                except PathNotFoundException:
                    raise InvalidArgumentException(message=f'Index: {index} is out of range.') from None

    def get_at(self, index: int) -> Any:
        """Retrieves the item at a specific index in the list.

        Args:
            index (int): The index to retrieve.

        Returns:
            Any: The value of the element at the specified index.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index is out of range.

        """
        op_type = DatastructureOperationType.ListGetAt
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            kv_op_type = KeyValueOperationType.LookupIn
            with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                try:
                    op = subdoc_get(f'[{index}]')
                    req = self._impl.request_builder.build_lookup_in_request(self._key,
                                                                             (op,),
                                                                             obs_handler,
                                                                             parent_span=ds_obs_handler.wrapped_span)
                    sdres: LookupInResult = self._execute_op(self._impl.lookup_in,
                                                             req,
                                                             obs_handler,
                                                             ds_obs_handler.wrapped_span,
                                                             create_type=True)
                    return sdres.value[0].get('value', None)
                except PathNotFoundException:
                    raise InvalidArgumentException(message=f'Index: {index} is out of range.') from None

    def remove_at(self, index: int) -> None:
        """Removes an item at a specific index from the list.

        Args:
            index (int): The index to remove.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index is out of range.

        """
        op_type = DatastructureOperationType.ListRemoveAt
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            kv_op_type = KeyValueOperationType.MutateIn
            with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                try:
                    op = remove(f'[{index}]')
                    req = self._impl.request_builder.build_mutate_in_request(self._key,
                                                                             (op,),
                                                                             obs_handler,
                                                                             parent_span=ds_obs_handler.wrapped_span)
                    self._execute_op(self._impl.mutate_in,
                                     req,
                                     obs_handler,
                                     ds_obs_handler.wrapped_span)
                except PathNotFoundException:
                    raise InvalidArgumentException(message=f'Index: {index} is out of range.') from None

    def size(self) -> int:
        """Returns the number of items in the list.

        Returns:
            int: The number of items in the list.

        """
        op_type = DatastructureOperationType.ListSize
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            kv_op_type = KeyValueOperationType.LookupIn
            with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                op = count('')
                req = self._impl.request_builder.build_lookup_in_request(self._key,
                                                                         (op,),
                                                                         obs_handler,
                                                                         parent_span=ds_obs_handler.wrapped_span)
                sdres: LookupInResult = self._execute_op(self._impl.lookup_in,
                                                         req,
                                                         obs_handler,
                                                         ds_obs_handler.wrapped_span,
                                                         create_type=True)
                return sdres.value[0].get('value', None)

    def index_of(self, value: JSONType) -> int:
        """Returns the index of a specific value from the list.

        Args:
            value (JSONType): The value to search for.

        Returns:
            int: The index of the value in the list. Returns -1 if value is not found.

        """
        op_type = DatastructureOperationType.ListIndexOf
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            list_ = self._get(parent_span=ds_obs_handler.wrapped_span)
            for idx, val in enumerate(list_.content_as[list]):
                if val == value:
                    return idx

            return -1

    def get_all(self) -> List[Any]:
        """Returns the entire list of items in this list.

        Returns:
            int: The entire list.

        """
        op_type = DatastructureOperationType.ListGetAll
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            list_ = self._get(parent_span=ds_obs_handler.wrapped_span)
            return list_.content_as[list]

    def clear(self) -> None:
        """Clears the list.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the list does not already exist.
        """
        op_type = DatastructureOperationType.ListClear
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            kv_op_type = KeyValueOperationType.Remove
            with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                try:
                    req = self._impl.request_builder.build_remove_request(self._key,
                                                                          obs_handler,
                                                                          parent_span=ds_obs_handler.wrapped_span)
                    self._impl.remove(req, obs_handler)
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

    def __init__(self, key: str, collection_impl: CollectionImpl) -> None:
        self._key = key
        self._impl = collection_impl
        self._full_map = None

    def _execute_op(self,
                    fn: Callable[[DataStructureRequest], Any],
                    req: DataStructureRequest,
                    obs_handler: ObservableRequestHandler,
                    parent_span: WrappedSpan,
                    create_type: Optional[bool] = None) -> Any:
        try:
            return fn(req, obs_handler)
        except DocumentNotFoundException:
            if create_type is True:
                orig_opt_type = obs_handler.op_type
                obs_handler.reset(KeyValueOperationType.Insert, with_error=True)
                try:
                    ins_req = self._impl.request_builder.build_insert_request(self._key,
                                                                              dict(),
                                                                              obs_handler,
                                                                              parent_span=parent_span)
                    self._impl.insert(ins_req, obs_handler)
                except DocumentExistsException:
                    pass
                obs_handler.reset(orig_opt_type)
                obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict(),
                                           parent_span=parent_span)
                return fn(req, obs_handler)
            else:
                raise

    def _get(self, parent_span: Optional[WrappedSpan] = None) -> Dict[str, Any]:
        """
        Get the entire map.
        """
        op_type = KeyValueOperationType.Get
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_request(self._key, obs_handler, parent_span=parent_span)
            return self._execute_op(self._impl.get,
                                    req,
                                    obs_handler,
                                    parent_span=parent_span,
                                    create_type=True)

    def add(self, mapkey: str, value: Any) -> None:
        """Sets a specific key to the specified value in the map.

        Args:
            mapkey (str): The key to set.
            value (JSONType): The value to set.

        """
        op_type = DatastructureOperationType.MapAdd
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            kv_op_type = KeyValueOperationType.MutateIn
            with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                op = upsert(mapkey, value)
                req = self._impl.request_builder.build_mutate_in_request(self._key,
                                                                         (op,),
                                                                         obs_handler,
                                                                         parent_span=ds_obs_handler.wrapped_span)
                return self._execute_op(self._impl.mutate_in,
                                        req,
                                        obs_handler,
                                        ds_obs_handler.wrapped_span,
                                        create_type=True)

    def get(self, mapkey: str) -> Any:
        """Fetches a specific key from the map.

        Args:
            mapkey (str): The key to fetch.

        Returns:
            Any: The value of the specified key.

        """
        op_type = DatastructureOperationType.MapGet
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            kv_op_type = KeyValueOperationType.LookupIn
            with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                op = subdoc_get(mapkey)
                req = self._impl.request_builder.build_lookup_in_request(self._key,
                                                                         (op,),
                                                                         obs_handler,
                                                                         parent_span=ds_obs_handler.wrapped_span)
                sd_res = self._execute_op(self._impl.lookup_in,
                                          req,
                                          obs_handler,
                                          ds_obs_handler.wrapped_span,
                                          create_type=True)
                return sd_res.value[0].get('value', None)

    def remove(self, mapkey: str) -> None:
        """Removes a specific key from the map.

        Args:
            mapkey (str): The key in the map to remove.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the key is not in the map.

        """
        op_type = DatastructureOperationType.MapRemove
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            kv_op_type = KeyValueOperationType.MutateIn
            with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                try:
                    op = remove(mapkey)
                    req = self._impl.request_builder.build_mutate_in_request(self._key,
                                                                             (op,),
                                                                             obs_handler,
                                                                             parent_span=ds_obs_handler.wrapped_span)
                    self._impl.mutate_in(req, obs_handler)
                except PathNotFoundException:
                    raise InvalidArgumentException(message=f'Key: {mapkey} is not in the map.') from None

    def size(self) -> int:
        """Returns the number of items in the map.

        Returns:
            int: The number of items in the map.

        """
        op_type = DatastructureOperationType.MapSize
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            kv_op_type = KeyValueOperationType.LookupIn
            with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                op = count('')
                req = self._impl.request_builder.build_lookup_in_request(self._key,
                                                                         (op,),
                                                                         obs_handler,
                                                                         parent_span=ds_obs_handler.wrapped_span)
                sd_res: LookupInResult = self._execute_op(self._impl.lookup_in,
                                                          req,
                                                          obs_handler,
                                                          ds_obs_handler.wrapped_span,
                                                          create_type=True)
                return sd_res.value[0].get('value', None)

    def exists(self, key: str) -> bool:
        """Checks whether a specific key exists in the map.

        Args:
            key (str): The key to set.

        Returns:
            bool: True if the key exists in the map, False otherwise.

        """
        op_type = DatastructureOperationType.MapExists
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            kv_op_type = KeyValueOperationType.LookupIn
            with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                op = subdoc_exists(key)
                req = self._impl.request_builder.build_lookup_in_request(self._key,
                                                                         (op,),
                                                                         obs_handler,
                                                                         parent_span=ds_obs_handler.wrapped_span)
                sd_res: LookupInResult = self._execute_op(self._impl.lookup_in,
                                                          req,
                                                          obs_handler,
                                                          ds_obs_handler.wrapped_span,
                                                          create_type=True)
                return sd_res.exists(0)

    def keys(self) -> List[str]:
        """Returns a list of all the keys which exist in the map.

        Returns:
            List[str]: A list of all the keys that exist in the map.
        """
        op_type = DatastructureOperationType.MapKeys
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            map_ = self._get(parent_span=ds_obs_handler.wrapped_span)
            return list(map_.content_as[dict].keys())

    def values(self) -> List[Any]:
        """Returns a list of all the values which exist in the map.

        Returns:
            List[Any]: A list of all the values that exist in the map.
        """
        op_type = DatastructureOperationType.MapValues
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            map_ = self._get(parent_span=ds_obs_handler.wrapped_span)
            return list(map_.content_as[dict].values())

    def get_all(self) -> Dict[str, Any]:
        """Retrieves the entire map.

        Returns:
            Dict[str, Any]: The entire CouchbaseMap.
        """
        op_type = DatastructureOperationType.MapGetAll
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            map_ = self._get(parent_span=ds_obs_handler.wrapped_span)
            return map_.content_as[dict]

    def clear(self) -> None:
        """Clears the map.
        """
        op_type = DatastructureOperationType.MapClear
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            kv_op_type = KeyValueOperationType.Remove
            with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                try:
                    req = self._impl.request_builder.build_remove_request(self._key,
                                                                          obs_handler,
                                                                          parent_span=ds_obs_handler.wrapped_span)
                    self._impl.remove(req, obs_handler)
                except DocumentNotFoundException:
                    pass

    def items(self) -> Generator:
        """Provides mechanism to loop over the entire map.

        Returns:
            Generator:  A generator expression for the map
        """
        op_type = DatastructureOperationType.MapItems
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            map_ = self._get(parent_span=ds_obs_handler.wrapped_span)
            return ((k, v) for k, v in map_.content_as[dict].items())


class CouchbaseSet:
    """
    CouchbaseSet provides a simplified interface for storing a set within a Couchbase document.

    Args:
        key (str): Document key to use for the set.
        collection (:class:`~.collection.Collection`): The :class:`~.collection.Collection` where the
            set belongs.

    """

    def __init__(self, key: str, collection_impl: CollectionImpl) -> None:
        self._key = key
        self._impl = collection_impl

    def _execute_op(self,
                    fn: Callable[[DataStructureRequest], Any],
                    req: DataStructureRequest,
                    obs_handler: ObservableRequestHandler,
                    parent_span: WrappedSpan,
                    create_type: Optional[bool] = None) -> Any:
        try:
            return fn(req, obs_handler)
        except DocumentNotFoundException:
            if create_type is True:
                orig_opt_type = obs_handler.op_type
                obs_handler.reset(KeyValueOperationType.Insert, with_error=True)
                try:
                    ins_req = self._impl.request_builder.build_insert_request(self._key,
                                                                              list(),
                                                                              obs_handler,
                                                                              parent_span=parent_span)
                    self._impl.insert(ins_req, obs_handler)
                except DocumentExistsException:
                    pass
                obs_handler.reset(orig_opt_type)
                obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict(),
                                           parent_span=parent_span)
                return fn(req, obs_handler)
            else:
                raise

    def _get(self, parent_span: Optional[WrappedSpan] = None) -> List[Any]:
        """
        Get the entire set.
        """
        op_type = KeyValueOperationType.Get
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_request(self._key, obs_handler, parent_span=parent_span)
            return self._execute_op(self._impl.get,
                                    req,
                                    obs_handler,
                                    parent_span=parent_span,
                                    create_type=True)

    def add(self, value: Any) -> bool:
        """Adds a new item to the set. Returning whether the item already existed in the set or not.

        Args:
            value (Any):

        Returns:
            bool:  True if the value was added, False otherwise (meaning the value already
                exists in the set).

        """
        op_type = DatastructureOperationType.SetAdd
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            kv_op_type = KeyValueOperationType.MutateIn
            with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                try:
                    op = array_addunique('', value)
                    req = self._impl.request_builder.build_mutate_in_request(self._key,
                                                                             (op,),
                                                                             obs_handler,
                                                                             parent_span=ds_obs_handler.wrapped_span)
                    self._execute_op(self._impl.mutate_in,
                                     req,
                                     obs_handler,
                                     ds_obs_handler.wrapped_span,
                                     create_type=True)
                    return True
                except PathExistsException:
                    return False

    def remove(self, value: Any, timeout: Optional[timedelta] = None) -> None:  # noqa: C901
        """Removes a specific value from the set.

        Args:
            value (Any): The value to remove
            timeout (timedelta, optional): Amount of time allowed when attempting
                to remove the value.  Defaults to 10 seconds.

        """
        op_type = DatastructureOperationType.SetRemove
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())

            if timeout is None:
                timeout = timedelta(seconds=10)

            timeout_millis = timeout.total_seconds() * 1000

            interval_millis = float(50)
            start = time.perf_counter()
            time_left = timeout_millis
            while True:
                sd_res = self._get(parent_span=ds_obs_handler.wrapped_span)
                list_ = sd_res.content_as[list]
                val_idx = -1
                for idx, v in enumerate(list_):
                    if v == value:
                        val_idx = idx
                        break

                if val_idx >= 0:
                    kv_op_type = KeyValueOperationType.MutateIn
                    with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                        try:
                            mut_opts = MutateInOptions(cas=sd_res.cas, parent_span=ds_obs_handler.wrapped_span)
                            op = remove(f'[{val_idx}]')
                            req = self._impl.request_builder.build_mutate_in_request(self._key,
                                                                                     (op,),
                                                                                     obs_handler,
                                                                                     mut_opts)
                            self._impl.mutate_in(req, obs_handler)
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

    def contains(self, value: Any) -> bool:
        """Returns whether a specific value already exists in the set.

        Args:
            value (Any): The value to check for existence.

        Returns:
            bool:  True if the specified value exists in the set.  False otherwise.

        """
        op_type = DatastructureOperationType.SetContains
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            list_ = self._get(parent_span=ds_obs_handler.wrapped_span).content_as[list]
            return value in list_

    def size(self) -> int:
        """Returns the number of items in the set.

        Returns:
            int: The number of items in the set.

        """
        op_type = DatastructureOperationType.SetSize
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            kv_op_type = KeyValueOperationType.LookupIn
            with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                op = count('')
                req = self._impl.request_builder.build_lookup_in_request(self._key,
                                                                         (op,),
                                                                         obs_handler,
                                                                         parent_span=ds_obs_handler.wrapped_span)
                sd_res: LookupInResult = self._execute_op(self._impl.lookup_in,
                                                          req,
                                                          obs_handler,
                                                          ds_obs_handler.wrapped_span,
                                                          create_type=True)
                return sd_res.value[0].get('value', None)

    def clear(self) -> None:
        """Clears the set.
        """
        op_type = DatastructureOperationType.SetClear
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            kv_op_type = KeyValueOperationType.Remove
            with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                try:
                    req = self._impl.request_builder.build_remove_request(self._key,
                                                                          obs_handler,
                                                                          parent_span=ds_obs_handler.wrapped_span)
                    self._impl.remove(req, obs_handler)
                except DocumentNotFoundException:
                    pass

    def values(self) -> List[Any]:
        """Returns a list of all the values which exist in the set.

        Returns:
            List[Any]: The values that exist in the set.
        """
        op_type = DatastructureOperationType.SetValues
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            list_ = self._get(parent_span=ds_obs_handler.wrapped_span)
            return list_.content_as[list]


class CouchbaseQueue:
    """
    CouchbaseQueue provides a simplified interface for storing a queue within a Couchbase document.

    Args:
        key (str): Document key to use for the queue.
        collection (:class:`~.collection.Collection`): The :class:`~.collection.Collection` where the
            queue belongs.

    """

    def __init__(self, key: str, collection_impl: CollectionImpl) -> None:
        self._key = key
        self._impl = collection_impl
        self._full_queue = None

    def _execute_op(self,
                    fn: Callable[[DataStructureRequest], Any],
                    req: DataStructureRequest,
                    obs_handler: ObservableRequestHandler,
                    parent_span: WrappedSpan,
                    create_type: Optional[bool] = None) -> Any:
        try:
            return fn(req, obs_handler)
        except DocumentNotFoundException:
            if create_type is True:
                orig_opt_type = obs_handler.op_type
                obs_handler.reset(KeyValueOperationType.Insert, with_error=True)
                try:
                    ins_req = self._impl.request_builder.build_insert_request(self._key,
                                                                              list(),
                                                                              obs_handler,
                                                                              parent_span=parent_span)
                    self._impl.insert(ins_req, obs_handler)
                except DocumentExistsException:
                    pass
                obs_handler.reset(orig_opt_type)
                obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict(),
                                           parent_span=parent_span)
                return fn(req, obs_handler)
            else:
                raise

    def _get(self, parent_span: Optional[WrappedSpan] = None) -> List:
        """
        Get the entire queuee.
        """
        op_type = KeyValueOperationType.Get
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_request(self._key, obs_handler, parent_span=parent_span)
            return self._execute_op(self._impl.get,
                                    req,
                                    obs_handler,
                                    parent_span=parent_span,
                                    create_type=True)

    def push(self, value: JSONType) -> None:
        """Adds a new item to the back of the queue.

        Args:
            value (JSONType): The value to push onto the queue.

        """
        op_type = DatastructureOperationType.QueuePush
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            kv_op_type = KeyValueOperationType.MutateIn
            with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                op = array_prepend('', value)
                req = self._impl.request_builder.build_mutate_in_request(self._key,
                                                                         (op,),
                                                                         obs_handler,
                                                                         parent_span=ds_obs_handler.wrapped_span)
                self._execute_op(self._impl.mutate_in,
                                 req,
                                 obs_handler,
                                 ds_obs_handler.wrapped_span,
                                 create_type=True)

    def pop(self, timeout: Optional[timedelta] = None) -> Any:
        """Removes an item from the front of the queue.

        Args:
            timeout (timedelta, optional): Amount of time allowed when attempting
                to remove the value.  Defaults to 10 seconds.


        Returns:
            Any: The value that was removed from the front of the queue.
        """
        op_type = DatastructureOperationType.QueuePop
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            if timeout is None:
                timeout = timedelta(seconds=10)

            timeout_millis = timeout.total_seconds() * 1000

            interval_millis = float(50)
            start = time.perf_counter()
            time_left = timeout_millis
            parent_span = ds_obs_handler.wrapped_span
            while True:
                try:
                    kv_op_type = KeyValueOperationType.LookupIn
                    with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                        op = subdoc_get('[-1]')
                        lookup_in_req = self._impl.request_builder.build_lookup_in_request(self._key,
                                                                                           (op,),
                                                                                           obs_handler,
                                                                                           parent_span=parent_span)
                        sd_res = self._impl.lookup_in(lookup_in_req, obs_handler)
                        val = sd_res.value[0].get('value', None)

                    kv_op_type = KeyValueOperationType.MutateIn
                    with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                        try:
                            op = remove('[-1]')
                            mutate_in_opts = MutateInOptions(cas=sd_res.cas, parent_span=parent_span)
                            mutate_in_req = self._impl.request_builder.build_mutate_in_request(self._key,
                                                                                               (op,),
                                                                                               obs_handler,
                                                                                               mutate_in_opts)
                            self._impl.mutate_in(mutate_in_req, obs_handler)
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

    def size(self) -> int:
        """Returns the number of items in the queue.

        Returns:
            int: The number of items in the queue.

        """
        op_type = DatastructureOperationType.QueueSize
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            kv_op_type = KeyValueOperationType.LookupIn
            with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                op = count('')
                req = self._impl.request_builder.build_lookup_in_request(self._key,
                                                                         (op,),
                                                                         obs_handler,
                                                                         parent_span=ds_obs_handler.wrapped_span)
                sd_res: LookupInResult = self._execute_op(self._impl.lookup_in,
                                                          req,
                                                          obs_handler,
                                                          ds_obs_handler.wrapped_span,
                                                          create_type=True)
                return sd_res.value[0].get('value', None)

    def clear(self) -> None:
        """Clears the queue.
        """
        op_type = DatastructureOperationType.QueueClear
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as ds_obs_handler:
            ds_obs_handler.create_kv_span(self._impl._request_builder._collection_dtls.get_details_as_dict())
            kv_op_type = KeyValueOperationType.Remove
            with ObservableRequestHandler(kv_op_type, self._impl.observability_instruments) as obs_handler:
                try:
                    req = self._impl.request_builder.build_remove_request(self._key,
                                                                          obs_handler,
                                                                          parent_span=ds_obs_handler.wrapped_span)
                    self._impl.remove(req, obs_handler)
                except DocumentNotFoundException:
                    pass

    def __iter__(self):
        list_ = self._get()
        self._full_queue = (v for v in list_.content_as[list])
        return self

    def __next__(self):
        return next(self._full_queue)
