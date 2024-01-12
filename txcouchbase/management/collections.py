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

from inspect import Parameter, Signature
from typing import (Any,
                    Dict,
                    Iterable,
                    Optional)

from twisted.internet.defer import Deferred

from couchbase._utils import OverloadType
from couchbase.logic.supportability import Supportability
from couchbase.management.logic import ManagementType
from couchbase.management.logic.collections_logic import (CollectionManagerLogic,
                                                          CollectionSpec,
                                                          CreateCollectionSettings,
                                                          ScopeSpec,
                                                          UpdateCollectionSettings)
from couchbase.management.options import (CreateCollectionOptions,
                                          CreateScopeOptions,
                                          DropCollectionOptions,
                                          DropScopeOptions,
                                          GetAllScopesOptions,
                                          UpdateCollectionOptions)
from txcouchbase.management.logic.wrappers import TxMgmtWrapper


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

    @TxMgmtWrapper.inject_callbacks(None,
                                    ManagementType.CollectionMgmt,
                                    CollectionManagerLogic._ERROR_MAPPING,
                                    OverloadType.SECONDARY)
    def create_collection(self,  # noqa: F811
                          collection: CollectionSpec,
                          *options: CreateCollectionOptions,
                          **kwargs: Dict[str, Any],
                          ) -> Deferred[None]:
        Supportability.method_signature_deprecated(
            'create_collection',
            Signature(
                parameters=[
                    Parameter('collection', Parameter.POSITIONAL_OR_KEYWORD, annotation=CollectionSpec),
                    Parameter('options', Parameter.VAR_POSITIONAL, annotation=CreateCollectionOptions),
                    Parameter('kwargs', Parameter.VAR_KEYWORD, annotation=Dict[str, Any]),
                ],
                return_annotation=Deferred[None]
            ),
            Signature(
                parameters=[
                    Parameter('scope_name', Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
                    Parameter('collection_name', Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
                    Parameter('settings', Parameter.POSITIONAL_OR_KEYWORD,
                              annotation=Optional[CreateCollectionSettings]),
                    Parameter('options', Parameter.VAR_POSITIONAL, annotation=CreateCollectionOptions),
                    Parameter('kwargs', Parameter.VAR_KEYWORD, annotation=Dict[str, Any]),
                ],
                return_annotation=Deferred[None]
            )
        )
        settings = None
        if collection.max_expiry is not None:
            settings = CreateCollectionSettings(max_expiry=collection.max_expiry)
        super().create_collection(collection.scope_name, collection.name, settings, *options, **kwargs)

    @TxMgmtWrapper.inject_callbacks(None,
                                    ManagementType.CollectionMgmt,
                                    CollectionManagerLogic._ERROR_MAPPING,
                                    OverloadType.DEFAULT)
    def create_collection(self,  # noqa: F811
                          scope_name: str,
                          collection_name: str,
                          settings: Optional[CreateCollectionSettings] = None,
                          *options: CreateCollectionOptions,
                          **kwargs: Dict[str, Any]
                          ):
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
        super().create_collection(scope_name, collection_name, settings, *options, **kwargs)

    @TxMgmtWrapper.inject_callbacks(None, ManagementType.CollectionMgmt, CollectionManagerLogic._ERROR_MAPPING)
    def update_collection(self,
                          scope_name: str,
                          collection_name: str,
                          settings: UpdateCollectionSettings,
                          *options: UpdateCollectionOptions,
                          **kwargs: Dict[str, Any]
                          ) -> Deferred[None]:
        super().update_collection(scope_name, collection_name, settings, *options, **kwargs)

    @TxMgmtWrapper.inject_callbacks(None,
                                    ManagementType.CollectionMgmt,
                                    CollectionManagerLogic._ERROR_MAPPING,
                                    OverloadType.SECONDARY)
    def drop_collection(self,  # noqa: F811
                        collection: CollectionSpec,
                        *options: DropCollectionOptions,
                        **kwargs: Dict[str, Any]
                        ) -> Deferred[None]:
        Supportability.method_signature_deprecated(
            'drop_collection',
            Signature(
                parameters=[
                    Parameter('self', Parameter.POSITIONAL_OR_KEYWORD),
                    Parameter('collection', Parameter.POSITIONAL_OR_KEYWORD, annotation=CollectionSpec),
                    Parameter('options', Parameter.VAR_POSITIONAL, annotation=DropCollectionOptions),
                    Parameter('kwargs', Parameter.VAR_KEYWORD, annotation=Dict[str, Any]),
                ],
                return_annotation=Deferred[None],
            ),
            Signature(
                parameters=[
                    Parameter('self', Parameter.POSITIONAL_OR_KEYWORD),
                    Parameter('scope_name', Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
                    Parameter('collection_name', Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
                    Parameter('options', Parameter.VAR_POSITIONAL, annotation=DropCollectionOptions),
                    Parameter('kwargs', Parameter.VAR_KEYWORD, annotation=Dict[str, Any]),
                ],
                return_annotation=Deferred[None]
            )
        )
        super().drop_collection(collection.scope_name, collection.name, *options, **kwargs)

    @TxMgmtWrapper.inject_callbacks(None,
                                    ManagementType.CollectionMgmt,
                                    CollectionManagerLogic._ERROR_MAPPING,
                                    OverloadType.DEFAULT)
    def drop_collection(self,  # noqa: F811
                        scope_name: str,
                        collection_name: str,
                        *options: DropCollectionOptions,
                        **kwargs: Dict[str, Any]
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
        super().drop_collection(scope_name, collection_name, *options, **kwargs)
