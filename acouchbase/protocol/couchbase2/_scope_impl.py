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

"""An asyncio scope over the gateway: its query and search run in the scope, and its collections are built here."""

from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    NoReturn)

from couchbase.protocol.couchbase2._core import unsupported

if TYPE_CHECKING:
    from acouchbase.protocol.couchbase2._bucket_impl import AsyncBucketImpl
    from acouchbase.protocol.couchbase2._cluster_impl import AsyncClusterImpl
    from acouchbase.result import AsyncAnalyticsResult
    from couchbase.common._utils import JSONType
    from couchbase.common.options import (AnalyticsOptions,
                                          QueryOptions,
                                          SearchOptions)
    from couchbase.common.search_queries import SearchQuery
    from couchbase.common.search_request import SearchRequest


class AsyncScopeImpl:

    def __init__(self, scope_name: str, bucket_impl: AsyncBucketImpl) -> None:
        self._scope_name = scope_name
        self._bucket_name = bucket_impl.bucket_name
        self._cluster_impl: AsyncClusterImpl = bucket_impl.cluster_impl

    @property
    def name(self) -> str:
        return self._scope_name

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    @property
    def cluster_impl(self) -> AsyncClusterImpl:
        """**INTERNAL**"""
        return self._cluster_impl

    @property
    def connection(self) -> object:
        raise unsupported('AsyncScope.connection')

    def collection(self, name: str) -> NoReturn:
        raise unsupported('Collections')

    def query(self,
              statement: str,
              *options: QueryOptions | JSONType,
              **kwargs: Any) -> NoReturn:
        raise unsupported('query')

    def analytics_query(self,
                        statement: str,
                        *options: AnalyticsOptions | JSONType,
                        **kwargs: Any) -> AsyncAnalyticsResult:
        raise unsupported('analytics_query')

    def search_query(self,
                     index: str,
                     query: SearchQuery,
                     options: SearchOptions | None = None,
                     **kwargs: Any) -> NoReturn:
        raise unsupported('search_query')

    def search(self,
               index: str,
               request: SearchRequest | SearchQuery,
               options: SearchOptions | None = None,
               **kwargs: Any) -> NoReturn:
        raise unsupported('search')

    def search_index_mgmt(self) -> NoReturn:
        raise unsupported('Search index management')

    def eventing_function_mgmt(self) -> NoReturn:
        raise unsupported('Eventing function management')
