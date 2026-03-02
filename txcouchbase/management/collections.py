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

from twisted.internet.defer import Deferred

from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import CollectionMgmtOperationType
from couchbase.management.logic.collection_mgmt_req_types import (CollectionSpec,
                                                                  CreateCollectionSettings,
                                                                  ScopeSpec,
                                                                  UpdateCollectionSettings)
from couchbase.management.options import (CreateCollectionOptions,
                                          CreateScopeOptions,
                                          DropCollectionOptions,
                                          DropScopeOptions,
                                          GetAllScopesOptions,
                                          UpdateCollectionOptions)
from txcouchbase.management.logic.collection_mgmt_impl import TxCollectionMgmtImpl

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.logic.observability import ObservabilityInstruments


class CollectionManager:

    def __init__(self,
                 client_adapter: AsyncClientAdapter,
                 bucket_name: str,
                 observability_instruments: ObservabilityInstruments) -> None:
        self._bucket_name = bucket_name
        self._impl = TxCollectionMgmtImpl(client_adapter, observability_instruments)

    def create_scope(self, scope_name: str, *options: CreateScopeOptions, **kwargs: Any) -> Deferred[None]:
        """Creates a new scope.

        Args:
            scope_name (str): The name of the scope.
            options (:class:`~couchbase.management.options.CreateScopeOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            `Deferred`: An empty `Deferred` instance.

        Raises:
            :class:`~couchbase.exceptions.ScopeAlreadyExistsException`: If the scope already exists.
        """
        op_type = CollectionMgmtOperationType.ScopeCreate
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_create_scope_request(self._bucket_name,
                                                                        scope_name,
                                                                        obs_handler,
                                                                        *options,
                                                                        **kwargs)
            d = self._impl.create_scope_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def drop_scope(self, scope_name: str, *options: DropScopeOptions, **kwargs: Any) -> Deferred[None]:
        """Drops an existing scope.

        Args:
            scope_name (str): The name of the scope.
            options (:class:`~couchbase.management.options.DropScopeOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            `Deferred`: An empty `Deferred` instance.

        Raises:
            :class:`~couchbase.exceptions.ScopeNotFoundException`: If the scope does not exist.
        """
        op_type = CollectionMgmtOperationType.ScopeDrop
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_drop_scope_request(self._bucket_name,
                                                                      scope_name,
                                                                      obs_handler,
                                                                      *options,
                                                                      **kwargs)
            d = self._impl.drop_scope_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_all_scopes(self, *options: GetAllScopesOptions, **kwargs: Any) -> Deferred[Iterable[ScopeSpec]]:
        """Returns all configured scopes along with their collections.

        Args:
            scope_name (str): The name of the scope.
            options (:class:`~couchbase.management.options.GetAllScopesOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Deferred[Iterable[:class:`.ScopeSpec`]]: A list of all configured scopes.
        """
        op_type = CollectionMgmtOperationType.ScopeGetAll
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_all_scopes_request(self._bucket_name,
                                                                          obs_handler,
                                                                          *options,
                                                                          **kwargs)
            d = self._impl.get_all_scopes_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    @overload
    @deprecated("Use ``create_collection(scope_name, collection_name, settings=None, *options, **kwargs)`` instead.")
    def create_collection(self,
                          collection: CollectionSpec,
                          *options: CreateCollectionOptions,
                          **kwargs: Any
                          ) -> Deferred[None]:
        ...

    @overload
    def create_collection(self,
                          scope_name: str,
                          collection_name: str,
                          settings: Optional[CreateCollectionSettings] = None,
                          *options: CreateCollectionOptions,
                          **kwargs: Any
                          ) -> Deferred[None]:
        ...

    def create_collection(self, *args: object, **kwargs: object) -> Deferred[None]:
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

        Returns:
            `Deferred`: An empty `Deferred` instance.

        Raises:
            :class:`~couchbase.exceptions.CollectionAlreadyExistsException`: If the collection already exists.
            :class:`~couchbase.exceptions.ScopeNotFoundException`: If the scope does not exist.
        """
        op_type = CollectionMgmtOperationType.CollectionCreate
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            kwargs['obs_handler'] = obs_handler
            req = self._impl.request_builder.build_create_collection_request(self._bucket_name, *args, **kwargs)
            d = self._impl.create_collection_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def update_collection(self,
                          scope_name: str,
                          collection_name: str,
                          settings: UpdateCollectionSettings,
                          *options: UpdateCollectionOptions,
                          **kwargs: Any
                          ) -> Deferred[None]:
        """Updates a collection in a specified scope.

        Args:
            scope_name (str): The name of the scope the collection is in.
            collection_name (str): The name of the collection that will be updated
            settings (:class:`~.UpdateCollectionSettings`, optional): Settings to apply for the collection
            options (:class:`~couchbase.management.options.UpdateCollectionOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            `Deferred`: An empty `Deferred` instance.

        Raises:
            :class:`~couchbase.exceptions.CollectionNotFoundException`: If the collection does not exist.
            :class:`~couchbase.exceptions.ScopeNotFoundException`: If the scope does not exist.
        """  # noqa: E501
        op_type = CollectionMgmtOperationType.CollectionUpdate
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_update_collection_request(self._bucket_name,
                                                                             scope_name,
                                                                             collection_name,
                                                                             settings,
                                                                             obs_handler,
                                                                             *options,
                                                                             **kwargs)
            d = self._impl.update_collection_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    @overload
    @deprecated("Use ``drop_collection(scope_name, collection_name, *options, **kwargs)`` instead.")
    def drop_collection(self,
                        collection: CollectionSpec,
                        *options: DropCollectionOptions,
                        **kwargs: Any
                        ) -> Deferred[None]:
        ...

    @overload
    def drop_collection(self,
                        scope_name: str,
                        collection_name: str,
                        *options: DropCollectionOptions,
                        **kwargs: Any) -> None:
        ...

    def drop_collection(self, *args: object, **kwargs: object) -> None:
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

        Returns:
            `Deferred`: An empty `Deferred` instance.

        Raises:
            :class:`~couchbase.exceptions.CollectionNotFoundException`: If the collection does not exist.
        """
        op_type = CollectionMgmtOperationType.CollectionDrop
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            kwargs['obs_handler'] = obs_handler
            req = self._impl.request_builder.build_drop_collection_request(self._bucket_name, *args, **kwargs)
            d = self._impl.drop_collection_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise
