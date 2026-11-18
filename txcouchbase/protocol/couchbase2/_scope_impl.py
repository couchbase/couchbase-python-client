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

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from acouchbase.protocol.couchbase2._scope_impl import AsyncScopeImpl
from couchbase.protocol.couchbase2._core import unsupported
from txcouchbase.protocol.couchbase2._collection_impl import TxCollectionImpl
from txcouchbase.protocol.couchbase2._management._search_index_mgmt_impl import TxSearchIndexMgmtImpl

if TYPE_CHECKING:
    from twisted.internet.defer import Deferred

    from couchbase.common._utils import JSONType
    from couchbase.common.options import (AnalyticsOptions,
                                          QueryOptions,
                                          SearchOptions)
    from couchbase.common.search_queries import SearchQuery
    from couchbase.common.search_request import SearchRequest
    from couchbase.result import (AnalyticsResult,
                                  QueryResult,
                                  SearchResult)
    from txcouchbase.protocol.couchbase2._bucket_impl import TxBucketImpl
    from txcouchbase.protocol.couchbase2._cluster_impl import TxClusterImpl


class TxScopeImpl(AsyncScopeImpl):

    def __init__(self, scope_name: str, bucket_impl: TxBucketImpl) -> None:
        super().__init__(scope_name, bucket_impl)
        self._tx_cluster_impl = bucket_impl.cluster_impl

    @property
    def cluster_impl(self) -> TxClusterImpl:
        """**INTERNAL**"""
        return self._tx_cluster_impl

    def collection(self, name: str) -> TxCollectionImpl:
        """**INTERNAL**"""
        return TxCollectionImpl(name, self)

    def search_index_mgmt(self) -> TxSearchIndexMgmtImpl:
        """**INTERNAL**"""
        return TxSearchIndexMgmtImpl(self._tx_cluster_impl, (self._bucket_name, self._scope_name))

    def query_deferred(self,
                       statement: str,
                       *options: QueryOptions | JSONType,
                       **kwargs: Any) -> Deferred[QueryResult]:
        return self._tx_cluster_impl.query_deferred_from(lambda: self.prepare_query(statement, options, kwargs))

    def analytics_query_deferred(self,
                                 statement: str,
                                 *options: AnalyticsOptions | JSONType,
                                 **kwargs: Any) -> Deferred[AnalyticsResult]:
        raise unsupported('analytics_query')

    def search_query_deferred(self,
                              index: str,
                              query: SearchQuery,
                              options: SearchOptions | None = None,
                              **kwargs: Any) -> Deferred[SearchResult]:
        return self._tx_cluster_impl.search_deferred_from(
            lambda: self.prepare_search_query(index, query, options, kwargs))

    def search_deferred(self,
                        index: str,
                        request: SearchRequest | SearchQuery,
                        options: SearchOptions | None = None,
                        **kwargs: Any) -> Deferred[SearchResult]:
        return self._tx_cluster_impl.search_deferred_from(lambda: self.prepare_search(index, request, options, kwargs))
