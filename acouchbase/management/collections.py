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
                    Awaitable,
                    Dict,
                    Iterable)

from acouchbase.management.logic.wrappers import AsyncMgmtWrapper
from couchbase.management.logic import ManagementType
from couchbase.management.logic.collections_logic import (CollectionManagerLogic,
                                                          CollectionSpec,
                                                          ScopeSpec)

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

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.CollectionMgmt, CollectionManagerLogic._ERROR_MAPPING)
    def create_scope(self,
                     scope_name,      # type: str
                     *options,        # type: CreateScopeOptions
                     **kwargs         # type: Dict[str, Any]
                     ) -> Awaitable[None]:
        super().create_scope(scope_name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.CollectionMgmt, CollectionManagerLogic._ERROR_MAPPING)
    def drop_scope(self,
                   scope_name,      # type: str
                   *options,        # type: DropScopeOptions
                   **kwargs         # type: Dict[str, Any]
                   ) -> Awaitable[None]:
        super().drop_scope(scope_name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks((ScopeSpec, CollectionSpec), ManagementType.CollectionMgmt,
                                       CollectionManagerLogic._ERROR_MAPPING)
    def get_all_scopes(self,
                       *options,        # type: GetAllScopesOptions
                       **kwargs         # type: Dict[str, Any]
                       ) -> Awaitable[Iterable[ScopeSpec]]:
        super().get_all_scopes(*options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.CollectionMgmt, CollectionManagerLogic._ERROR_MAPPING)
    def create_collection(self,
                          collection,     # type: CollectionSpec
                          *options,       # type: CreateCollectionOptions
                          **kwargs        # type: Dict[str, Any]
                          ) -> Awaitable[None]:
        super().create_collection(collection, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.CollectionMgmt, CollectionManagerLogic._ERROR_MAPPING)
    def drop_collection(self,
                        collection,     # type: CollectionSpec
                        *options,       # type: DropCollectionOptions
                        **kwargs        # type: Dict[str, Any]
                        ) -> Awaitable[None]:
        super().drop_collection(collection, *options, **kwargs)
