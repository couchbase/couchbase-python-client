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

"""What a protocol's cluster impl must provide to the cluster facades.

`ClusterInterface` and its async and Twisted twins state the cluster's operations in the public shape, with what
the facade reads besides: the constructor `resolve()` hands back, closing and connecting, transactions, and the
impls behind `bucket()` and the cluster-level managers.  `query` and `analytics_query` keep their variadic, because
it carries positional query parameters as well as the options object.  The SDK imports this module only under
`TYPE_CHECKING`.
"""

from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    Protocol)

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop
    from datetime import timedelta

    from twisted.internet.defer import Deferred

    from acouchbase.result import (AsyncAnalyticsResult,
                                   AsyncQueryResult,
                                   AsyncSearchResult)
    from acouchbase.transactions import Transactions as AsyncTransactions
    from couchbase.common._interfaces._analytics_mgmt import (AnalyticsMgmtInterface,
                                                              AsyncAnalyticsMgmtInterface,
                                                              TxAnalyticsMgmtInterface)
    from couchbase.common._interfaces._bucket import (AsyncBucketInterface,
                                                      BucketInterface,
                                                      TxBucketInterface)
    from couchbase.common._interfaces._bucket_mgmt import (AsyncBucketMgmtInterface,
                                                           BucketMgmtInterface,
                                                           TxBucketMgmtInterface)
    from couchbase.common._interfaces._eventing_function_mgmt import (AsyncEventingFunctionMgmtInterface,
                                                                      EventingFunctionMgmtInterface)
    from couchbase.common._interfaces._query_index_mgmt import (AsyncQueryIndexMgmtInterface,
                                                                QueryIndexMgmtInterface,
                                                                TxQueryIndexMgmtInterface)
    from couchbase.common._interfaces._search_index_mgmt import (AsyncSearchIndexMgmtInterface,
                                                                 SearchIndexMgmtInterface,
                                                                 TxSearchIndexMgmtInterface)
    from couchbase.common._interfaces._user_mgmt import (AsyncUserMgmtInterface,
                                                         TxUserMgmtInterface,
                                                         UserMgmtInterface)
    from couchbase.common._utils import JSONType
    from couchbase.common.auth import (CertificateAuthenticator,
                                       JwtAuthenticator,
                                       PasswordAuthenticator)
    from couchbase.common.options import (AnalyticsOptions,
                                          ClusterOptions,
                                          DiagnosticsOptions,
                                          PingOptions,
                                          QueryOptions,
                                          SearchOptions,
                                          WaitUntilReadyOptions)
    from couchbase.common.result import (ClusterInfoResult,
                                         DiagnosticsResult,
                                         PingResult)
    from couchbase.common.search_queries import SearchQuery
    from couchbase.common.search_request import SearchRequest
    from couchbase.result import (AnalyticsResult,
                                  QueryResult,
                                  SearchResult)
    from couchbase.transactions import Transactions


class ClusterInterface(Protocol):
    def __init__(self, connstr: str, *options: ClusterOptions, **kwargs: Any) -> None: ...

    @property
    def transactions(self) -> Transactions: ...

    def close_connection(self) -> None: ...

    def bucket(self, name: str) -> BucketInterface: ...

    def get_cluster_info(self) -> ClusterInfoResult: ...

    def ping(self,
             opts: PingOptions | None = None,
             **kwargs: Any) -> PingResult: ...

    def diagnostics(self,
                    opts: DiagnosticsOptions | None = None,
                    **kwargs: Any) -> DiagnosticsResult: ...

    def update_credentials(self,
                           authenticator: CertificateAuthenticator | JwtAuthenticator | PasswordAuthenticator
                           ) -> None: ...

    def wait_until_ready(self,
                         timeout: timedelta,
                         opts: WaitUntilReadyOptions | None = None,
                         **kwargs: Any) -> None: ...

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

    def bucket_mgmt(self) -> BucketMgmtInterface: ...

    def user_mgmt(self) -> UserMgmtInterface: ...

    def query_index_mgmt(self) -> QueryIndexMgmtInterface: ...

    def analytics_mgmt(self) -> AnalyticsMgmtInterface: ...

    def search_index_mgmt(self) -> SearchIndexMgmtInterface: ...

    def eventing_function_mgmt(self) -> EventingFunctionMgmtInterface: ...


class AsyncClusterInterface(Protocol):
    def __init__(self,
                 connstr: str,
                 *options: ClusterOptions,
                 loop: AbstractEventLoop | None = None,
                 **kwargs: Any) -> None: ...

    @property
    def loop(self) -> AbstractEventLoop: ...

    @property
    def transactions(self) -> AsyncTransactions: ...

    async def close_connection(self) -> None: ...

    async def wait_until_connected(self) -> None: ...

    def bucket(self, name: str) -> AsyncBucketInterface: ...

    async def get_cluster_info(self) -> ClusterInfoResult: ...

    async def ping(self,
                   opts: PingOptions | None = None,
                   **kwargs: Any) -> PingResult: ...

    async def diagnostics(self,
                          opts: DiagnosticsOptions | None = None,
                          **kwargs: Any) -> DiagnosticsResult: ...

    def update_credentials(self,
                           authenticator: CertificateAuthenticator | JwtAuthenticator | PasswordAuthenticator
                           ) -> None: ...

    async def wait_until_ready(self,
                               timeout: timedelta,
                               opts: WaitUntilReadyOptions | None = None,
                               **kwargs: Any) -> None: ...

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

    def bucket_mgmt(self) -> AsyncBucketMgmtInterface: ...

    def user_mgmt(self) -> AsyncUserMgmtInterface: ...

    def query_index_mgmt(self) -> AsyncQueryIndexMgmtInterface: ...

    def analytics_mgmt(self) -> AsyncAnalyticsMgmtInterface: ...

    def search_index_mgmt(self) -> AsyncSearchIndexMgmtInterface: ...

    def eventing_function_mgmt(self) -> AsyncEventingFunctionMgmtInterface: ...


class TxClusterInterface(Protocol):
    def __init__(self, connstr: str, *options: ClusterOptions, **kwargs: Any) -> None: ...

    @property
    def loop(self) -> AbstractEventLoop: ...

    def close_connection_deferred(self) -> Deferred[None]: ...

    def wait_until_connected_deferred(self) -> Deferred[None]: ...

    def bucket(self, name: str) -> TxBucketInterface: ...

    def get_cluster_info_deferred(self) -> Deferred[ClusterInfoResult]: ...

    def ping_deferred(self,
                      opts: PingOptions | None = None,
                      **kwargs: Any) -> Deferred[PingResult]: ...

    def diagnostics_deferred(self,
                             opts: DiagnosticsOptions | None = None,
                             **kwargs: Any) -> Deferred[DiagnosticsResult]: ...

    def wait_until_ready_deferred(self,
                                  timeout: timedelta,
                                  opts: WaitUntilReadyOptions | None = None,
                                  **kwargs: Any) -> Deferred[None]: ...

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

    def bucket_mgmt(self) -> TxBucketMgmtInterface: ...

    def user_mgmt(self) -> TxUserMgmtInterface: ...

    def query_index_mgmt(self) -> TxQueryIndexMgmtInterface: ...

    def analytics_mgmt(self) -> TxAnalyticsMgmtInterface: ...

    def search_index_mgmt(self) -> TxSearchIndexMgmtInterface: ...
