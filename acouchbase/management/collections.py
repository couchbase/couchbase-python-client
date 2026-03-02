#  Copyright 2016-2023. Couchbase, Inc.
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

import sys
from typing import (TYPE_CHECKING,
                    Any,
                    Iterable,
                    Optional,
                    overload)

if sys.version_info >= (3, 13):
    from warnings import deprecated
else:
    from typing_extensions import deprecated

from acouchbase.management.logic.collection_mgmt_impl import AsyncCollectionMgmtImpl
from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import CollectionMgmtOperationType
from couchbase.management.logic.collection_mgmt_impl import CollectionSpec, ScopeSpec
from couchbase.management.logic.collection_mgmt_req_types import CreateCollectionSettings, UpdateCollectionSettings
from couchbase.management.options import (CreateCollectionOptions,
                                          CreateScopeOptions,
                                          DropCollectionOptions,
                                          DropScopeOptions,
                                          GetAllScopesOptions,
                                          UpdateCollectionOptions)

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.logic.observability import ObservabilityInstruments


class CollectionManager:

    def __init__(self,
                 client_adapter: AsyncClientAdapter,
                 bucket_name: str,
                 observability_instruments: ObservabilityInstruments) -> None:
        self._bucket_name = bucket_name
        self._impl = AsyncCollectionMgmtImpl(client_adapter, observability_instruments)

    async def create_scope(self,
                           scope_name: str,
                           *options: CreateScopeOptions,
                           **kwargs: Any
                           ) -> None:
        """Creates a new scope.

        Args:
            scope_name (str): The name of the scope.
            options (:class:`~couchbase.management.options.CreateScopeOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.ScopeAlreadyExistsException`: If the scope already exists.
        """
        op_type = CollectionMgmtOperationType.ScopeCreate
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_create_scope_request(self._bucket_name,
                                                                        scope_name,
                                                                        obs_handler,
                                                                        *options,
                                                                        **kwargs)
            await self._impl.create_scope(req, obs_handler)

    async def drop_scope(self,
                         scope_name: str,
                         *options: DropScopeOptions,
                         **kwargs: Any
                         ) -> None:
        """Drops an existing scope.

        Args:
            scope_name (str): The name of the scope.
            options (:class:`~couchbase.management.options.DropScopeOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.


        Raises:
            :class:`~couchbase.exceptions.ScopeNotFoundException`: If the scope does not exist.
        """
        op_type = CollectionMgmtOperationType.ScopeDrop
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_drop_scope_request(self._bucket_name,
                                                                      scope_name,
                                                                      obs_handler,
                                                                      *options,
                                                                      **kwargs)
            await self._impl.drop_scope(req, obs_handler)

    async def get_all_scopes(self,
                             *options: GetAllScopesOptions,
                             **kwargs: Any
                             ) -> Iterable[ScopeSpec]:
        """Returns all configured scopes along with their collections.

        Args:
            scope_name (str): The name of the scope.
            options (:class:`~couchbase.management.options.GetAllScopesOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Iterable[:class:`.ScopeSpec`]: A list of all configured scopes.
        """
        op_type = CollectionMgmtOperationType.ScopeGetAll
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_all_scopes_request(self._bucket_name,
                                                                          obs_handler,
                                                                          *options,
                                                                          **kwargs)
            return await self._impl.get_all_scopes(req, obs_handler)

    @overload
    @deprecated("Use ``create_collection(scope_name, collection_name, settings=None, *options, **kwargs)`` instead.")
    async def create_collection(self,
                                collection: CollectionSpec,
                                *options: CreateCollectionOptions,
                                **kwargs: Any
                                ) -> None:
        ...

    @overload
    async def create_collection(self,
                                scope_name: str,
                                collection_name: str,
                                settings: Optional[CreateCollectionSettings] = None,
                                *options: CreateCollectionOptions,
                                **kwargs: Any
                                ) -> None:
        ...

    async def create_collection(self, *args: object, **kwargs: object) -> None:
        """Creates a new collection in a specified scope.

        .. note::
            The overloaded create_collection method that takes a CollectionSpec is deprecated as of v4.1.9
            and will be removed in a future version.

        Args:
            scope_name (str): The name of the scope the collection will be created in.
            collection_name (str): The name of the collection to be created
            settings (:class:`~.CreateCollectionSettings`, optional): Settings to apply for the collection
            options (:class:`~couchbase.management.options.CreateCollectionOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.CollectionAlreadyExistsException`: If the collection already exists.
            :class:`~couchbase.exceptions.ScopeNotFoundException`: If the scope does not exist.
        """
        op_type = CollectionMgmtOperationType.CollectionCreate
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            kwargs['obs_handler'] = obs_handler
            req = self._impl.request_builder.build_create_collection_request(self._bucket_name, *args, **kwargs)
            await self._impl.create_collection(req, obs_handler)

    async def update_collection(self,
                                scope_name: str,
                                collection_name: str,
                                settings: UpdateCollectionSettings,
                                *options: UpdateCollectionOptions,
                                **kwargs: Any
                                ) -> None:
        """Updates a collection in a specified scope.

        Args:
            scope_name (str): The name of the scope the collection is in.
            collection_name (str): The name of the collection that will be updated
            settings (:class:`~.UpdateCollectionSettings`, optional): Settings to apply for the collection
            options (:class:`~couchbase.management.options.UpdateCollectionOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.CollectionNotFoundException`: If the collection does not exist.
            :class:`~couchbase.exceptions.ScopeNotFoundException`: If the scope does not exist.
        """  # noqa: E501
        op_type = CollectionMgmtOperationType.CollectionUpdate
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_update_collection_request(self._bucket_name,
                                                                             scope_name,
                                                                             collection_name,
                                                                             settings,
                                                                             obs_handler,
                                                                             *options,
                                                                             **kwargs)
            await self._impl.update_collection(req, obs_handler)

    @overload
    @deprecated("Use ``drop_collection(scope_name, collection_name, *options, **kwargs)`` instead.")
    async def drop_collection(self,
                              collection: CollectionSpec,
                              *options: DropCollectionOptions,
                              **kwargs: Any
                              ) -> None:
        ...

    @overload
    async def drop_collection(self,
                              scope_name: str,
                              collection_name: str,
                              *options: DropCollectionOptions,
                              **kwargs: Any) -> None:
        ...

    async def drop_collection(self, *args: object, **kwargs: object) -> None:
        """Drops a collection from a scope.

        .. note::
            The overloaded drop_collection method that takes a CollectionSpec is deprecated as of v4.1.9
            and will be removed in a future version.

        Args:
            scope_name (str): The name of the scope the collection is in.
            collection_name (str): The name of the collection to be dropped.
            options (:class:`~couchbase.management.options.DropCollectionOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.CollectionNotFoundException`: If the collection does not exist.
        """
        op_type = CollectionMgmtOperationType.CollectionDrop
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            kwargs['obs_handler'] = obs_handler
            req = self._impl.request_builder.build_drop_collection_request(self._bucket_name, *args, **kwargs)
            await self._impl.drop_collection(req, obs_handler)
