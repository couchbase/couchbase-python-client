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

from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable)

from acouchbase.management.logic.query_index_mgmt_impl import AsyncQueryIndexMgmtImpl
from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import MgmtOperationType, QueryIndexMgmtOperationType
from couchbase.management.logic.query_index_mgmt_req_types import QueryIndex

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.logic.observability import ObservabilityInstruments
    from couchbase.management.options import (BuildDeferredQueryIndexOptions,
                                              CreatePrimaryQueryIndexOptions,
                                              CreateQueryIndexOptions,
                                              DropPrimaryQueryIndexOptions,
                                              DropQueryIndexOptions,
                                              GetAllQueryIndexOptions,
                                              WatchQueryIndexOptions)


class QueryIndexManager:
    def __init__(self,
                 client_adapter: AsyncClientAdapter,
                 observability_instruments: ObservabilityInstruments) -> None:
        self._impl = AsyncQueryIndexMgmtImpl(client_adapter, observability_instruments)
        self._collection_ctx = None

    async def create_index(self,
                           bucket_name,   # type: str
                           index_name,    # type: str
                           keys,        # type: Iterable[str]
                           *options,      # type: CreateQueryIndexOptions
                           **kwargs
                           ) -> None:
        """Creates a new query index.

        Args:
            bucket_name (str): The name of the bucket this index is for.
            index_name (str): The name of the index.
            keys (Iterable[str]): The keys which this index should cover.
            options (:class:`~couchbase.management.options.CreateQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name, index_name or keys
                are invalid types.
            :class:`~couchbase.exceptions.QueryIndexAlreadyExistsException`: If the index already exists.
        """
        op_type = QueryIndexMgmtOperationType.QueryIndexCreate
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_create_index_request(bucket_name,
                                                                        index_name,
                                                                        keys,
                                                                        obs_handler,
                                                                        self._collection_ctx,
                                                                        *options,
                                                                        **kwargs)
            await self._impl.create_index(req, obs_handler)

    async def create_primary_index(self,
                                   bucket_name,   # type: str
                                   *options,      # type: CreatePrimaryQueryIndexOptions
                                   **kwargs
                                   ) -> None:
        """Creates a new primary query index.

        Args:
            bucket_name (str): The name of the bucket this index is for.
            options (:class:`~couchbase.management.options.CreatePrimaryQueryIndexOptions`): Optional parameters for
                this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name is an invalid type.
            :class:`~couchbase.exceptions.QueryIndexAlreadyExistsException`: If the index already exists.
        """
        op_type = QueryIndexMgmtOperationType.QueryIndexCreate
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_create_primary_index_request(bucket_name,
                                                                                obs_handler,
                                                                                self._collection_ctx,
                                                                                *options,
                                                                                **kwargs)
            await self._impl.create_primary_index(req, obs_handler)

    async def drop_index(self,
                         bucket_name,     # type: str
                         index_name,      # type: str
                         *options,        # type: DropQueryIndexOptions
                         **kwargs) -> None:
        """Drops an existing query index.

        Args:
            bucket_name (str): The name of the bucket containing the index to drop.
            index_name (str): The name of the index to drop.
            options (:class:`~couchbase.management.options.DropQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name or index_name are
                invalid types.
            :class:`~couchbase.exceptions.QueryIndexNotFoundException`: If the index does not exists.
        """
        op_type = QueryIndexMgmtOperationType.QueryIndexDrop
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_drop_index_request(bucket_name,
                                                                      index_name,
                                                                      obs_handler,
                                                                      self._collection_ctx,
                                                                      *options,
                                                                      **kwargs)
            await self._impl.drop_index(req, obs_handler)

    async def drop_primary_index(self,
                                 bucket_name,     # type: str
                                 *options,        # type: DropPrimaryQueryIndexOptions
                                 **kwargs) -> None:
        """Drops an existing primary query index.

        Args:
            bucket_name (str): The name of the bucket this index to drop.
            options (:class:`~couchbase.management.options.DropPrimaryQueryIndexOptions`): Optional parameters for
                this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name is an invalid type.
            :class:`~couchbase.exceptions.QueryIndexNotFoundException`: If the index does not exists.
        """
        op_type = QueryIndexMgmtOperationType.QueryIndexDrop
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_drop_primary_index_request(bucket_name,
                                                                              obs_handler,
                                                                              self._collection_ctx,
                                                                              *options,
                                                                              **kwargs)
            await self._impl.drop_primary_index(req, obs_handler)

    async def get_all_indexes(self,
                              bucket_name,    # type: str
                              *options,       # type: GetAllQueryIndexOptions
                              **kwargs        # type: Dict[str,Any]
                              ) -> Iterable[QueryIndex]:
        """Returns a list of indexes for a specific bucket.

        Args:
            bucket_name (str): The name of the bucket to fetch indexes for.
            options (:class:`~couchbase.management.options.GetAllQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Awaitable[Iterable[:class:`.QueryIndex`]]: A list of indexes for a specific bucket.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name is an invalid type.
        """
        op_type = QueryIndexMgmtOperationType.QueryIndexGetAll
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_all_indexes_request(bucket_name,
                                                                           obs_handler,
                                                                           self._collection_ctx,
                                                                           *options,
                                                                           **kwargs)
            return await self._impl.get_all_indexes(req, obs_handler)

    async def build_deferred_indexes(self,
                                     bucket_name,     # type: str
                                     *options,        # type: BuildDeferredQueryIndexOptions
                                     **kwargs
                                     ) -> None:
        """Starts building any indexes which were previously created with ``deferred=True``.

        Args:
            bucket_name (str): The name of the bucket to perform build on.
            options (:class:`~couchbase.management.options.BuildDeferredQueryIndexOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name is an invalid type.
        """
        op_type = QueryIndexMgmtOperationType.QueryIndexBuildDeferred
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_build_deferred_indexes_request(bucket_name,
                                                                                  obs_handler,
                                                                                  self._collection_ctx,
                                                                                  *options,
                                                                                  **kwargs)
            await self._impl.build_deferred_indexes(req, obs_handler)

    async def watch_indexes(self,   # noqa: C901
                            bucket_name,  # type: str
                            index_names,  # type: Iterable[str]
                            *options,     # type: WatchQueryIndexOptions
                            **kwargs      # type: Dict[str,Any]
                            ) -> None:
        """Waits for a number of indexes to finish creation and be ready to use.

        Args:
            bucket_name (str): The name of the bucket to watch for indexes on.
            index_names (Iterable[str]): The names of the indexes to watch.
            options (:class:`~couchbase.management.options.WatchQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name or index_names are
                invalid types.
            :class:`~couchbase.exceptions.WatchQueryIndexTimeoutException`: If the specified timeout is reached
                before all the specified indexes are ready to use.
        """
        op_type = MgmtOperationType.QueryIndexWatchIndexes
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_watch_indexes_request(bucket_name,
                                                                         index_names,
                                                                         obs_handler,
                                                                         self._collection_ctx,
                                                                         *options,
                                                                         **kwargs)
            await self._impl.watch_indexes(req, obs_handler)


class CollectionQueryIndexManager:
    def __init__(self,
                 client_adapter: AsyncClientAdapter,
                 bucket_name: str,
                 scope_name: str,
                 collection_name: str,
                 observability_instruments: ObservabilityInstruments) -> None:
        self._bucket_name = bucket_name
        self._impl = AsyncQueryIndexMgmtImpl(client_adapter, observability_instruments)
        self._collection_ctx = (collection_name, scope_name)

    async def create_index(self,
                           index_name,    # type: str
                           keys,        # type: Iterable[str]
                           *options,      # type: CreateQueryIndexOptions
                           **kwargs
                           ) -> None:
        """Creates a new query index.

        Args:
            index_name (str): The name of the index.
            keys (Iterable[str]): The keys which this index should cover.
            options (:class:`~couchbase.management.options.CreateQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index_name or keys are invalid types.
            :class:`~couchbase.exceptions.QueryIndexAlreadyExistsException`: If the index already exists.
        """
        op_type = QueryIndexMgmtOperationType.QueryIndexCreate
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_create_index_request(self._bucket_name,
                                                                        index_name,
                                                                        keys,
                                                                        obs_handler,
                                                                        self._collection_ctx,
                                                                        *options,
                                                                        **kwargs)
            await self._impl.create_index(req, obs_handler)

    async def create_primary_index(self,
                                   *options,      # type: CreatePrimaryQueryIndexOptions
                                   **kwargs
                                   ) -> None:
        """Creates a new primary query index.

        Args:
            options (:class:`~couchbase.management.options.CreatePrimaryQueryIndexOptions`): Optional parameters for
                this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.QueryIndexAlreadyExistsException`: If the index already exists.
        """
        op_type = QueryIndexMgmtOperationType.QueryIndexCreate
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_create_primary_index_request(self._bucket_name,
                                                                                obs_handler,
                                                                                self._collection_ctx,
                                                                                *options,
                                                                                **kwargs)
            await self._impl.create_primary_index(req, obs_handler)

    async def drop_index(self,
                         index_name,      # type: str
                         *options,        # type: DropQueryIndexOptions
                         **kwargs) -> None:
        """Drops an existing query index.

        Args:
            index_name (str): The name of the index to drop.
            options (:class:`~couchbase.management.options.DropQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index_name is an invalid type.
            :class:`~couchbase.exceptions.QueryIndexNotFoundException`: If the index does not exists.
        """
        op_type = QueryIndexMgmtOperationType.QueryIndexDrop
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_drop_index_request(self._bucket_name,
                                                                      index_name,
                                                                      obs_handler,
                                                                      self._collection_ctx,
                                                                      *options,
                                                                      **kwargs)
            await self._impl.drop_index(req, obs_handler)

    async def drop_primary_index(self,
                                 *options,        # type: DropPrimaryQueryIndexOptions
                                 **kwargs) -> None:
        """Drops an existing primary query index.

        Args:
            options (:class:`~couchbase.management.options.DropPrimaryQueryIndexOptions`): Optional parameters for
                this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.QueryIndexNotFoundException`: If the index does not exists.
        """
        op_type = QueryIndexMgmtOperationType.QueryIndexDrop
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_drop_primary_index_request(self._bucket_name,
                                                                              obs_handler,
                                                                              self._collection_ctx,
                                                                              *options,
                                                                              **kwargs)
            await self._impl.drop_primary_index(req, obs_handler)

    async def get_all_indexes(self,
                              *options,       # type: GetAllQueryIndexOptions
                              **kwargs        # type: Dict[str, Any]
                              ) -> Iterable[QueryIndex]:
        """Returns a list of indexes for a specific collection.

        Args:
            bucket_name (str): The name of the bucket to fetch indexes for.
            options (:class:`~couchbase.management.options.GetAllQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Awaitable[Iterable[:class:`.QueryIndex`]]: A list of indexes.
        """
        op_type = QueryIndexMgmtOperationType.QueryIndexGetAll
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_all_indexes_request(self._bucket_name,
                                                                           obs_handler,
                                                                           self._collection_ctx,
                                                                           *options,
                                                                           **kwargs)
            return await self._impl.get_all_indexes(req, obs_handler)

    async def build_deferred_indexes(self,
                                     *options,        # type: BuildDeferredQueryIndexOptions
                                     **kwargs
                                     ) -> None:
        """Starts building any indexes which were previously created with ``deferred=True``.

        Args:
            options (:class:`~couchbase.management.options.BuildDeferredQueryIndexOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        """
        op_type = QueryIndexMgmtOperationType.QueryIndexBuildDeferred
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_build_deferred_indexes_request(self._bucket_name,
                                                                                  obs_handler,
                                                                                  self._collection_ctx,
                                                                                  *options,
                                                                                  **kwargs)
            await self._impl.build_deferred_indexes(req, obs_handler)

    async def watch_indexes(self,
                            index_names,  # type: Iterable[str]
                            *options,     # type: WatchQueryIndexOptions
                            **kwargs      # type: Dict[str,Any]
                            ) -> None:
        """Waits for a number of indexes to finish creation and be ready to use.

        Args:
            index_names (Iterable[str]): The names of the indexes to watch.
            options (:class:`~couchbase.management.options.WatchQueryIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the bucket_name or index_names are
                invalid types.
            :class:`~couchbase.exceptions.WatchQueryIndexTimeoutException`: If the specified timeout is reached
                before all the specified indexes are ready to use.
        """
        op_type = MgmtOperationType.QueryIndexWatchIndexes
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_watch_indexes_request(self._bucket_name,
                                                                         index_names,
                                                                         obs_handler,
                                                                         self._collection_ctx,
                                                                         *options,
                                                                         **kwargs)
            await self._impl.watch_indexes(req, obs_handler)
