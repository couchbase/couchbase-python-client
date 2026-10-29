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

"""What a protocol's query index management impl must provide to the query index manager facades.

One impl serves both facades, `QueryIndexManager` and `CollectionQueryIndexManager`: the cluster impl builds it with no
collection context and the collection impl with its collection and scope names, and `CollectionQueryIndexManager` passes
its bucket name on each call.  txcouchbase offers no collection-level manager.  The SDK imports this module only under
`TYPE_CHECKING`.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import (TYPE_CHECKING,
                    Any,
                    Protocol)

if TYPE_CHECKING:
    from twisted.internet.defer import Deferred

    from couchbase.common.management.options import (BuildDeferredQueryIndexOptions,
                                                     CreatePrimaryQueryIndexOptions,
                                                     CreateQueryIndexOptions,
                                                     DropPrimaryQueryIndexOptions,
                                                     DropQueryIndexOptions,
                                                     GetAllQueryIndexOptions,
                                                     WatchQueryIndexOptions)
    from couchbase.common.management.queries import QueryIndex


class QueryIndexMgmtInterface(Protocol):
    def create_index(self,
                     bucket_name: str,
                     index_name: str,
                     keys: Iterable[str],
                     options: CreateQueryIndexOptions | None = None,
                     **kwargs: Any) -> None: ...

    def create_primary_index(self,
                             bucket_name: str,
                             options: CreatePrimaryQueryIndexOptions | None = None,
                             **kwargs: Any) -> None: ...

    def drop_index(self,
                   bucket_name: str,
                   index_name: str,
                   options: DropQueryIndexOptions | None = None,
                   **kwargs: Any) -> None: ...

    def drop_primary_index(self,
                           bucket_name: str,
                           options: DropPrimaryQueryIndexOptions | None = None,
                           **kwargs: Any) -> None: ...

    def get_all_indexes(self,
                        bucket_name: str,
                        options: GetAllQueryIndexOptions | None = None,
                        **kwargs: Any) -> Iterable[QueryIndex]: ...

    def build_deferred_indexes(self,
                               bucket_name: str,
                               options: BuildDeferredQueryIndexOptions | None = None,
                               **kwargs: Any) -> None: ...

    def watch_indexes(self,
                      bucket_name: str,
                      index_names: Iterable[str],
                      options: WatchQueryIndexOptions | None = None,
                      **kwargs: Any) -> None: ...


class AsyncQueryIndexMgmtInterface(Protocol):
    async def create_index(self,
                           bucket_name: str,
                           index_name: str,
                           keys: Iterable[str],
                           options: CreateQueryIndexOptions | None = None,
                           **kwargs: Any) -> None: ...

    async def create_primary_index(self,
                                   bucket_name: str,
                                   options: CreatePrimaryQueryIndexOptions | None = None,
                                   **kwargs: Any) -> None: ...

    async def drop_index(self,
                         bucket_name: str,
                         index_name: str,
                         options: DropQueryIndexOptions | None = None,
                         **kwargs: Any) -> None: ...

    async def drop_primary_index(self,
                                 bucket_name: str,
                                 options: DropPrimaryQueryIndexOptions | None = None,
                                 **kwargs: Any) -> None: ...

    async def get_all_indexes(self,
                              bucket_name: str,
                              options: GetAllQueryIndexOptions | None = None,
                              **kwargs: Any) -> Iterable[QueryIndex]: ...

    async def build_deferred_indexes(self,
                                     bucket_name: str,
                                     options: BuildDeferredQueryIndexOptions | None = None,
                                     **kwargs: Any) -> None: ...

    async def watch_indexes(self,
                            bucket_name: str,
                            index_names: Iterable[str],
                            options: WatchQueryIndexOptions | None = None,
                            **kwargs: Any) -> None: ...


class TxQueryIndexMgmtInterface(Protocol):
    def create_index_deferred(self,
                              bucket_name: str,
                              index_name: str,
                              keys: Iterable[str],
                              options: CreateQueryIndexOptions | None = None,
                              **kwargs: Any) -> Deferred[None]: ...

    def create_primary_index_deferred(self,
                                      bucket_name: str,
                                      options: CreatePrimaryQueryIndexOptions | None = None,
                                      **kwargs: Any) -> Deferred[None]: ...

    def drop_index_deferred(self,
                            bucket_name: str,
                            index_name: str,
                            options: DropQueryIndexOptions | None = None,
                            **kwargs: Any) -> Deferred[None]: ...

    def drop_primary_index_deferred(self,
                                    bucket_name: str,
                                    options: DropPrimaryQueryIndexOptions | None = None,
                                    **kwargs: Any) -> Deferred[None]: ...

    def get_all_indexes_deferred(self,
                                 bucket_name: str,
                                 options: GetAllQueryIndexOptions | None = None,
                                 **kwargs: Any) -> Deferred[Iterable[QueryIndex]]: ...

    def build_deferred_indexes_deferred(self,
                                        bucket_name: str,
                                        options: BuildDeferredQueryIndexOptions | None = None,
                                        **kwargs: Any) -> Deferred[None]: ...

    def watch_indexes_deferred(self,
                               bucket_name: str,
                               index_names: Iterable[str],
                               options: WatchQueryIndexOptions | None = None,
                               **kwargs: Any) -> Deferred[None]: ...
