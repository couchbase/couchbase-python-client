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

"""A sync scope over the gateway: its query and search run in the scope, and its collections are built here."""

from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    NoReturn)

from couchbase.protocol.couchbase2._collection_impl import CollectionImpl
from couchbase.protocol.couchbase2._core import unsupported
from couchbase.protocol.couchbase2._management._search_index_mgmt_impl import SearchIndexMgmtImpl
from couchbase.protocol.couchbase2._scope_req_builder import ScopeRequestBuilder

if TYPE_CHECKING:
    from couchbase.common._utils import JSONType
    from couchbase.common.options import (AnalyticsOptions,
                                          QueryOptions,
                                          SearchOptions)
    from couchbase.common.search_queries import SearchQuery
    from couchbase.common.search_request import SearchRequest
    from couchbase.protocol.couchbase2._bucket_impl import BucketImpl
    from couchbase.protocol.couchbase2._cluster_impl import ClusterImpl
    from couchbase.result import (AnalyticsResult,
                                  QueryResult,
                                  SearchResult)


class ScopeImpl:

    def __init__(self, scope_name: str, bucket_impl: BucketImpl) -> None:
        self._scope_name = scope_name
        self._bucket_name = bucket_impl.bucket_name
        self._cluster_impl: ClusterImpl = bucket_impl.cluster_impl
        self._request_builder = ScopeRequestBuilder(self._cluster_impl.core, self._bucket_name, scope_name)

    @property
    def name(self) -> str:
        return self._scope_name

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    @property
    def cluster_impl(self) -> ClusterImpl:
        """**INTERNAL**"""
        return self._cluster_impl

    def collection(self, name: str) -> CollectionImpl:
        """**INTERNAL**"""
        return CollectionImpl(name, self)

    def query(self,
              statement: str,
              *options: QueryOptions | JSONType,
              **kwargs: Any) -> QueryResult:
        self._cluster_impl.core.ensure_open()
        return self._cluster_impl.run_query(self._request_builder.build_query_request(statement, *options, **kwargs))

    def analytics_query(self,
                        statement: str,
                        *options: AnalyticsOptions | JSONType,
                        **kwargs: Any) -> AnalyticsResult:
        raise unsupported('analytics_query')

    def search_query(self,
                     index: str,
                     query: SearchQuery,
                     options: SearchOptions | None = None,
                     **kwargs: Any) -> SearchResult:
        self._cluster_impl.core.ensure_open()
        return self._cluster_impl.run_search(
            self._request_builder.build_search_query_request(index, query, options, **kwargs))

    def search(self,
               index: str,
               request: SearchRequest | SearchQuery,
               options: SearchOptions | None = None,
               **kwargs: Any) -> SearchResult:
        self._cluster_impl.core.ensure_open()
        return self._cluster_impl.run_search(
            self._request_builder.build_search_request(index, request, options, **kwargs))

    def search_index_mgmt(self) -> SearchIndexMgmtImpl:
        """**INTERNAL**"""
        return SearchIndexMgmtImpl(self._cluster_impl, (self._bucket_name, self._scope_name))

    def eventing_function_mgmt(self) -> NoReturn:
        raise unsupported('Eventing function management')
