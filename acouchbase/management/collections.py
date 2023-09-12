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

from inspect import Parameter, Signature
from typing import (Any,
                    Awaitable,
                    Dict,
                    Iterable,
                    Optional)

from acouchbase.management.logic.wrappers import AsyncMgmtWrapper
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

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.CollectionMgmt, CollectionManagerLogic._ERROR_MAPPING)
    def create_scope(self,
                     scope_name: str,
                     *options: CreateScopeOptions,
                     **kwargs: Dict[str, Any]
                     ) -> Awaitable[None]:
        super().create_scope(scope_name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.CollectionMgmt, CollectionManagerLogic._ERROR_MAPPING)
    def drop_scope(self,
                   scope_name: str,
                   *options: DropScopeOptions,
                   **kwargs: Dict[str, Any]
                   ) -> Awaitable[None]:
        super().drop_scope(scope_name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks((ScopeSpec, CollectionSpec), ManagementType.CollectionMgmt,
                                       CollectionManagerLogic._ERROR_MAPPING)
    def get_all_scopes(self,
                       *options: GetAllScopesOptions,
                       **kwargs: Dict[str, Any]
                       ) -> Awaitable[Iterable[ScopeSpec]]:
        super().get_all_scopes(*options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None,
                                       ManagementType.CollectionMgmt,
                                       CollectionManagerLogic._ERROR_MAPPING,
                                       OverloadType.DEFAULT)
    def create_collection(self,
                          scope_name: str,
                          collection_name: str,
                          settings: Optional[CreateCollectionSettings] = None,
                          *options: CreateCollectionOptions,
                          **kwargs: Dict[str, Any]
                          ):
        super().create_collection(scope_name, collection_name, settings, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None,
                                       ManagementType.CollectionMgmt,
                                       CollectionManagerLogic._ERROR_MAPPING,
                                       OverloadType.SECONDARY)
    def create_collection(self,  # noqa: F811
                          collection: CollectionSpec,
                          *options: CreateCollectionOptions,
                          **kwargs: Dict[str, Any],
                          ) -> Awaitable[None]:
        Supportability.method_signature_deprecated(
            'create_collection',
            Signature(
                parameters=[
                    Parameter('collection', Parameter.POSITIONAL_OR_KEYWORD, annotation=CollectionSpec),
                    Parameter('options', Parameter.VAR_POSITIONAL, annotation=CreateCollectionOptions),
                    Parameter('kwargs', Parameter.VAR_KEYWORD, annotation=Dict[str, Any]),
                ],
                return_annotation=Awaitable[None]
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
                return_annotation=Awaitable[None]
            )
        )
        settings = None
        if collection.max_expiry is not None:
            settings = CreateCollectionSettings(max_expiry=collection.max_expiry)
        super().create_collection(collection.scope_name, collection.name, settings, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.CollectionMgmt, CollectionManagerLogic._ERROR_MAPPING)
    def update_collection(self,
                          scope_name: str,
                          collection_name: str,
                          settings: UpdateCollectionSettings,
                          *options: UpdateCollectionOptions,
                          **kwargs: Dict[str, Any]
                          ) -> Awaitable[None]:
        super().update_collection(scope_name, collection_name, settings, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None,
                                       ManagementType.CollectionMgmt,
                                       CollectionManagerLogic._ERROR_MAPPING,
                                       OverloadType.DEFAULT)
    def drop_collection(self,
                        scope_name: str,
                        collection_name: str,
                        *options: DropCollectionOptions,
                        **kwargs: Dict[str, Any]
                        ) -> Awaitable[None]:
        super().drop_collection(scope_name, collection_name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None,
                                       ManagementType.CollectionMgmt,
                                       CollectionManagerLogic._ERROR_MAPPING,
                                       OverloadType.SECONDARY)
    def drop_collection(self,  # noqa: F811
                        collection: CollectionSpec,
                        *options: DropCollectionOptions,
                        **kwargs: Dict[str, Any]
                        ) -> Awaitable[None]:
        Supportability.method_signature_deprecated(
            'drop_collection',
            Signature(
                parameters=[
                    Parameter('self', Parameter.POSITIONAL_OR_KEYWORD),
                    Parameter('collection', Parameter.POSITIONAL_OR_KEYWORD, annotation=CollectionSpec),
                    Parameter('options', Parameter.VAR_POSITIONAL, annotation=DropCollectionOptions),
                    Parameter('kwargs', Parameter.VAR_KEYWORD, annotation=Dict[str, Any]),
                ],
                return_annotation=Awaitable[None],
            ),
            Signature(
                parameters=[
                    Parameter('self', Parameter.POSITIONAL_OR_KEYWORD),
                    Parameter('scope_name', Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
                    Parameter('collection_name', Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
                    Parameter('options', Parameter.VAR_POSITIONAL, annotation=DropCollectionOptions),
                    Parameter('kwargs', Parameter.VAR_KEYWORD, annotation=Dict[str, Any]),
                ],
                return_annotation=Awaitable[None]
            )
        )
        super().drop_collection(collection.scope_name, collection.name, *options, **kwargs)
