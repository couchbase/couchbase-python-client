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

"""What a protocol's scope impl must provide to the scope facades.

`ScopeInterface` and its async and Twisted twins state the scope's queries and searches in the public shape, with
what the facade reads besides: the names, and the impls behind `collection()` and the scope-level managers.  `query`
and `analytics_query` keep their variadic, because it carries positional query parameters as well as the options
object.  The SDK imports this module only under `TYPE_CHECKING`.
"""

from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    Protocol)

if TYPE_CHECKING:
    from twisted.internet.defer import Deferred

    from acouchbase.result import (AsyncAnalyticsResult,
                                   AsyncQueryResult,
                                   AsyncSearchResult)
    from couchbase.common._interfaces._collection import (AsyncCollectionInterface,
                                                          CollectionInterface,
                                                          TxCollectionInterface)
    from couchbase.common._interfaces._eventing_function_mgmt import (AsyncEventingFunctionMgmtInterface,
                                                                      EventingFunctionMgmtInterface)
    from couchbase.common._interfaces._search_index_mgmt import (AsyncSearchIndexMgmtInterface,
                                                                 SearchIndexMgmtInterface,
                                                                 TxSearchIndexMgmtInterface)
    from couchbase.common._utils import JSONType
    from couchbase.common.options import (AnalyticsOptions,
                                          QueryOptions,
                                          SearchOptions)
    from couchbase.common.search_queries import SearchQuery
    from couchbase.common.search_request import SearchRequest
    from couchbase.result import (AnalyticsResult,
                                  QueryResult,
                                  SearchResult)


class ScopeInterface(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def bucket_name(self) -> str: ...

    def collection(self, name: str) -> CollectionInterface: ...

    def query(self,
              statement: str,
              *options: QueryOptions | JSONType,
              **kwargs: Any) -> QueryResult: ...

    def analytics_query(self,
                        statement: str,
                        *options: AnalyticsOptions | JSONType,
                        **kwargs: Any) -> AnalyticsResult: ...

    def search_query(self,
                     index: str,
                     query: SearchQuery,
                     options: SearchOptions | None = None,
                     **kwargs: Any) -> SearchResult: ...

    def search(self,
               index: str,
               request: SearchRequest | SearchQuery,
               options: SearchOptions | None = None,
               **kwargs: Any) -> SearchResult: ...

    def search_index_mgmt(self) -> SearchIndexMgmtInterface: ...

    def eventing_function_mgmt(self) -> EventingFunctionMgmtInterface: ...


class AsyncScopeInterface(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def bucket_name(self) -> str: ...

    @property
    def connection(self) -> object: ...

    def collection(self, name: str) -> AsyncCollectionInterface: ...

    def query(self,
              statement: str,
              *options: QueryOptions | JSONType,
              **kwargs: Any) -> AsyncQueryResult: ...

    def analytics_query(self,
                        statement: str,
                        *options: AnalyticsOptions | JSONType,
                        **kwargs: Any) -> AsyncAnalyticsResult: ...

    def search_query(self,
                     index: str,
                     query: SearchQuery,
                     options: SearchOptions | None = None,
                     **kwargs: Any) -> AsyncSearchResult: ...

    def search(self,
               index: str,
               request: SearchRequest | SearchQuery,
               options: SearchOptions | None = None,
               **kwargs: Any) -> AsyncSearchResult: ...

    def search_index_mgmt(self) -> AsyncSearchIndexMgmtInterface: ...

    def eventing_function_mgmt(self) -> AsyncEventingFunctionMgmtInterface: ...


class TxScopeInterface(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def bucket_name(self) -> str: ...

    def collection(self, name: str) -> TxCollectionInterface: ...

    def query_deferred(self,
                       statement: str,
                       *options: QueryOptions | JSONType,
                       **kwargs: Any) -> Deferred[QueryResult]: ...

    def analytics_query_deferred(self,
                                 statement: str,
                                 *options: AnalyticsOptions | JSONType,
                                 **kwargs: Any) -> Deferred[AnalyticsResult]: ...

    def search_query_deferred(self,
                              index: str,
                              query: SearchQuery,
                              options: SearchOptions | None = None,
                              **kwargs: Any) -> Deferred[SearchResult]: ...

    def search_deferred(self,
                        index: str,
                        request: SearchRequest | SearchQuery,
                        options: SearchOptions | None = None,
                        **kwargs: Any) -> Deferred[SearchResult]: ...

    def search_index_mgmt(self) -> TxSearchIndexMgmtInterface: ...
