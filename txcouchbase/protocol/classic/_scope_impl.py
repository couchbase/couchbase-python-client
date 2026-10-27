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

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from twisted.internet.defer import Deferred

from acouchbase.protocol.classic._scope_impl import AsyncScopeImpl
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.protocol.classic._streaming import (AnalyticsStreamSpec,
                                                   QueryStreamSpec,
                                                   SearchStreamSpec)
from couchbase.result import (AnalyticsResult,
                              QueryResult,
                              SearchResult)
from txcouchbase.protocol.classic._collection_impl import TxCollectionImpl
from txcouchbase.protocol.classic._streaming import submit_rows

if TYPE_CHECKING:
    from couchbase.common._utils import JSONType
    from couchbase.common.options import (AnalyticsOptions,
                                          QueryOptions,
                                          SearchOptions)
    from couchbase.common.search_queries import SearchQuery
    from couchbase.common.search_request import SearchRequest
    from couchbase.protocol.classic._scope_types import SearchQueryRequest
    from txcouchbase.protocol.classic._bucket_impl import TxBucketImpl


class TxScopeImpl(AsyncScopeImpl):

    def __init__(self, scope_name: str, bucket_impl: TxBucketImpl) -> None:
        super().__init__(scope_name, bucket_impl)

    def collection(self, name: str) -> TxCollectionImpl:
        """**INTERNAL**"""
        return TxCollectionImpl(name, self)

    def analytics_query_deferred(self,
                                 statement: str,
                                 *options: AnalyticsOptions | JSONType,
                                 **kwargs: Any) -> Deferred[AnalyticsResult]:
        obs_handler = ObservableRequestHandler(Op.AnalyticsQuery, self.observability_instruments)
        req = self._request_builder.build_analytics_query_request(statement, obs_handler, *options, **kwargs)
        if not self.connected:
            raise RuntimeError('Cannot attempt to execute an analytics query prior to establishing a connection.')
        # If the analytics_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the analytics_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's analytics_timeout (set here). If the cluster
        # also does not specify an analytics_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::analytics_timeout when the streaming object is created in the bindings.
        streaming_timeout = self.streaming_timeouts.get('analytics_timeout', None)
        rows, responded = submit_rows(self.connection,
                                      self.loop,
                                      AnalyticsStreamSpec(req.analytics_query.params, self.default_serializer),
                                      obs_handler=req.obs_handler,
                                      streaming_timeout=streaming_timeout)
        return responded.addCallback(lambda _: AnalyticsResult(rows))

    def query_deferred(self,
                       statement: str,
                       *options: QueryOptions | JSONType,
                       **kwargs: Any) -> Deferred[QueryResult]:
        obs_handler = ObservableRequestHandler(Op.Query, self.observability_instruments)
        req = self._request_builder.build_query_request(statement, obs_handler, *options, **kwargs)
        if not self.connected:
            raise RuntimeError('Cannot attempt to execute a query prior to establishing a connection.')

        # If the n1ql_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the n1ql_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's query_timeout (set here). If the cluster
        # also does not specify a query_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::query_timeout when the streaming object is created in the bindings.
        streaming_timeout = self.streaming_timeouts.get('query_timeout', None)
        rows, responded = submit_rows(self.connection,
                                      self.loop,
                                      QueryStreamSpec(req.n1ql_query.params, self.default_serializer),
                                      obs_handler=req.obs_handler,
                                      streaming_timeout=streaming_timeout)
        return responded.addCallback(lambda _: QueryResult(rows))

    def search_query_deferred(self,
                              index: str,
                              query: SearchQuery,
                              options: SearchOptions | None = None,
                              **kwargs: Any) -> Deferred[SearchResult]:
        obs_handler = ObservableRequestHandler(Op.SearchQuery, self.observability_instruments)
        req = self._request_builder.build_search_query_request(index, query, obs_handler, options, **kwargs)
        return self._search_deferred(req)

    def search_deferred(self,
                        index: str,
                        request: SearchRequest | SearchQuery,
                        options: SearchOptions | None = None,
                        **kwargs: Any) -> Deferred[SearchResult]:
        obs_handler = ObservableRequestHandler(Op.SearchQuery, self.observability_instruments)
        req = self._request_builder.build_search_request(index, request, obs_handler, options, **kwargs)
        return self._search_deferred(req)

    def _search_deferred(self, req: SearchQueryRequest) -> Deferred[SearchResult]:
        if not self.connected:
            raise RuntimeError('Cannot attempt to execute a search prior to establishing a connection.')
        # If the search_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the search_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's search_timeout (set here). If the cluster
        # also does not specify a search_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::search_timeout when the streaming object is created in the bindings.
        streaming_timeout = self.streaming_timeouts.get('search_timeout', None)
        rows, responded = submit_rows(self.connection,
                                      self.loop,
                                      SearchStreamSpec(req.query_builder.as_encodable(),
                                                       self.default_serializer,
                                                       bucket_name=req.bucket_name,
                                                       scope_name=req.scope_name),
                                      obs_handler=req.obs_handler,
                                      streaming_timeout=streaming_timeout)
        return responded.addCallback(lambda _: SearchResult(rows))

    def wait_until_bucket_connected_deferred(self) -> Deferred[None]:
        coro = super().wait_until_bucket_connected()
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d
