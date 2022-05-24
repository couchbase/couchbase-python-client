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
                    Dict,
                    Iterable)

from twisted.internet.defer import Deferred

from couchbase.management.logic import ManagementType
from couchbase.management.logic.collections_logic import (CollectionManagerLogic,
                                                          CollectionSpec,
                                                          ScopeSpec)
from txcouchbase.management.logic.wrappers import TxMgmtWrapper

if TYPE_CHECKING:
    from couchbase.management.options import (CreateCollectionOptions,
                                              CreateScopeOptions,
                                              DropCollectionOptions,
                                              DropScopeOptions,
                                              GetAllScopesOptions)


class CollectionManager(CollectionManagerLogic):

    def __init__(self, connection, loop, bucket_name):
        super().__init__(connection, bucket_name)
        self._loop = loop

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._loop

    @TxMgmtWrapper.inject_callbacks(None, ManagementType.CollectionMgmt, CollectionManagerLogic._ERROR_MAPPING)
    def create_scope(self,
                     scope_name,      # type: str
                     *options,        # type: CreateScopeOptions
                     **kwargs         # type: Dict[str, Any]
                     ) -> Deferred[None]:
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
        super().create_scope(scope_name, *options, **kwargs)

    @TxMgmtWrapper.inject_callbacks(None, ManagementType.CollectionMgmt, CollectionManagerLogic._ERROR_MAPPING)
    def drop_scope(self,
                   scope_name,      # type: str
                   *options,        # type: DropScopeOptions
                   **kwargs         # type: Dict[str, Any]
                   ) -> Deferred[None]:
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
        super().drop_scope(scope_name, *options, **kwargs)

    @TxMgmtWrapper.inject_callbacks((ScopeSpec, CollectionSpec),
                                    ManagementType.CollectionMgmt,
                                    CollectionManagerLogic._ERROR_MAPPING)
    def get_all_scopes(self,
                       *options,        # type: GetAllScopesOptions
                       **kwargs         # type: Dict[str, Any]
                       ) -> Deferred[Iterable[ScopeSpec]]:
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
        super().get_all_scopes(*options, **kwargs)

    @TxMgmtWrapper.inject_callbacks(None, ManagementType.CollectionMgmt, CollectionManagerLogic._ERROR_MAPPING)
    def create_collection(self,
                          collection,     # type: CollectionSpec
                          *options,       # type: CreateCollectionOptions
                          **kwargs        # type: Dict[str, Any]
                          ) -> Deferred[None]:
        """Creates a new collection in a specified scope.

        Args:
            collection (:class:`.CollectionSpec`): The collection details.
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
        super().create_collection(collection, *options, **kwargs)

    @TxMgmtWrapper.inject_callbacks(None, ManagementType.CollectionMgmt, CollectionManagerLogic._ERROR_MAPPING)
    def drop_collection(self,
                        collection,     # type: CollectionSpec
                        *options,       # type: DropCollectionOptions
                        **kwargs        # type: Dict[str, Any]
                        ) -> Deferred[None]:
        """Drops a collection from a scope.

        Args:
            collection (:class:`.CollectionSpec`): The collection details.
            options (:class:`~couchbase.management.options.DropCollectionOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            `Deferred`: An empty `Deferred` instance.

        Raises:
            :class:`~couchbase.exceptions.CollectionNotFoundException`: If the collection does not exist.
        """
        super().drop_collection(collection, *options, **kwargs)
