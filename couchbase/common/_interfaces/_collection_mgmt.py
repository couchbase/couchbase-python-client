#  Copyright 2016-2026. Couchbase, Inc.
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

"""What a protocol's collection management impl must provide to the `CollectionManager` facades.

The impl is built by its bucket impl for one bucket, so no operation takes the bucket name.
`create_collection` and `drop_collection` take `*args` because their two public overloads, the
deprecated `CollectionSpec` form and the names form, reach the impl unparsed and the request builder
tells them apart.  The SDK imports this module only under `TYPE_CHECKING`.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import (TYPE_CHECKING,
                    Any,
                    Protocol)

if TYPE_CHECKING:
    from twisted.internet.defer import Deferred

    from couchbase.common.management.collections import ScopeSpec, UpdateCollectionSettings
    from couchbase.common.management.options import (CreateScopeOptions,
                                                     DropScopeOptions,
                                                     GetAllScopesOptions,
                                                     UpdateCollectionOptions)


class CollectionMgmtInterface(Protocol):
    def create_scope(self,
                     scope_name: str,
                     options: CreateScopeOptions | None = None,
                     **kwargs: Any) -> None: ...

    def drop_scope(self,
                   scope_name: str,
                   options: DropScopeOptions | None = None,
                   **kwargs: Any) -> None: ...

    def get_all_scopes(self,
                       options: GetAllScopesOptions | None = None,
                       **kwargs: Any) -> Iterable[ScopeSpec]: ...

    def update_collection(self,
                          scope_name: str,
                          collection_name: str,
                          settings: UpdateCollectionSettings,
                          options: UpdateCollectionOptions | None = None,
                          **kwargs: Any) -> None: ...

    def create_collection(self, *args: Any, **kwargs: Any) -> None: ...

    def drop_collection(self, *args: Any, **kwargs: Any) -> None: ...


class AsyncCollectionMgmtInterface(Protocol):
    async def create_scope(self,
                           scope_name: str,
                           options: CreateScopeOptions | None = None,
                           **kwargs: Any) -> None: ...

    async def drop_scope(self,
                         scope_name: str,
                         options: DropScopeOptions | None = None,
                         **kwargs: Any) -> None: ...

    async def get_all_scopes(self,
                             options: GetAllScopesOptions | None = None,
                             **kwargs: Any) -> Iterable[ScopeSpec]: ...

    async def update_collection(self,
                                scope_name: str,
                                collection_name: str,
                                settings: UpdateCollectionSettings,
                                options: UpdateCollectionOptions | None = None,
                                **kwargs: Any) -> None: ...

    async def create_collection(self, *args: Any, **kwargs: Any) -> None: ...

    async def drop_collection(self, *args: Any, **kwargs: Any) -> None: ...


class TxCollectionMgmtInterface(Protocol):
    def create_scope_deferred(self,
                              scope_name: str,
                              options: CreateScopeOptions | None = None,
                              **kwargs: Any) -> Deferred[None]: ...

    def drop_scope_deferred(self,
                            scope_name: str,
                            options: DropScopeOptions | None = None,
                            **kwargs: Any) -> Deferred[None]: ...

    def get_all_scopes_deferred(self,
                                options: GetAllScopesOptions | None = None,
                                **kwargs: Any) -> Deferred[Iterable[ScopeSpec]]: ...

    def update_collection_deferred(self,
                                   scope_name: str,
                                   collection_name: str,
                                   settings: UpdateCollectionSettings,
                                   options: UpdateCollectionOptions | None = None,
                                   **kwargs: Any) -> Deferred[None]: ...

    def create_collection_deferred(self, *args: Any, **kwargs: Any) -> Deferred[None]: ...

    def drop_collection_deferred(self, *args: Any, **kwargs: Any) -> Deferred[None]: ...
