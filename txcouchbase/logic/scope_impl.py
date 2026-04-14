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
from typing import TYPE_CHECKING

from twisted.internet.defer import Deferred

from acouchbase.logic.scope_impl import AsyncScopeImpl
from couchbase.result import (AnalyticsResult,
                              QueryResult,
                              SearchResult)
from txcouchbase.analytics import AnalyticsRequest
from txcouchbase.n1ql import N1QLRequest
from txcouchbase.search import FullTextSearchRequest

if TYPE_CHECKING:
    from couchbase.logic.cluster_types import (AnalyticsQueryRequest,
                                               QueryRequest,
                                               SearchQueryRequest)
    from txcouchbase.bucket import TxBucket


class TxScopeImpl(AsyncScopeImpl):

    def __init__(self, scope_name: str, bucket: TxBucket) -> None:
        super().__init__(scope_name, bucket)

    def analytics_query_deferred(self, req: AnalyticsQueryRequest) -> Deferred[AnalyticsResult]:
        if not self.connected:
            raise RuntimeError('Cannot attempt to execute an analytics query prior to establishing a connection.')
        # If the analytics_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the analytics_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's analytics_timeout (set here). If the cluster
        # also does not specify an analytics_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::analytics_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._cluster_settings.streaming_timeouts.get('analytics_timeout', None)
        q_req = AnalyticsRequest.generate_analytics_request(self.connection,
                                                            self.loop,
                                                            req.analytics_query.params,
                                                            default_serializer=self.default_serializer,
                                                            streaming_timeout=streaming_timeout,
                                                            obs_handler=req.obs_handler)
        d = Deferred()

        def _on_ok(_):
            d.callback(AnalyticsResult(q_req))

        def _on_err(exc):
            d.errback(exc)

        query_d = q_req.execute_analytics_query()
        query_d.addCallback(_on_ok)
        query_d.addErrback(_on_err)
        return d

    def query_deferred(self, req: QueryRequest) -> Deferred[QueryResult]:
        if not self.connected:
            raise RuntimeError('Cannot attempt to execute a query prior to establishing a connection.')

        # If the n1ql_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the n1ql_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's query_timeout (set here). If the cluster
        # also does not specify a query_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::query_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._cluster_settings.streaming_timeouts.get('query_timeout', None)
        q_req = N1QLRequest.generate_n1ql_request(self.connection,
                                                  self.loop,
                                                  req.n1ql_query.params,
                                                  default_serializer=self.default_serializer,
                                                  streaming_timeout=streaming_timeout,
                                                  obs_handler=req.obs_handler)

        d = Deferred()

        def _on_ok(_):
            d.callback(QueryResult(q_req))

        def _on_err(exc):
            d.errback(exc)

        query_d = q_req.execute_query()
        query_d.addCallback(_on_ok)
        query_d.addErrback(_on_err)
        return d

    def search_deferred(self, req: SearchQueryRequest) -> Deferred[SearchResult]:
        if not self.connected:
            raise RuntimeError('Cannot attempt to execute a search prior to establishing a connection.')
        # If the search_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the search_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's search_timeout (set here). If the cluster
        # also does not specify a search_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::search_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._cluster_settings.streaming_timeouts.get('search_timeout', None)
        q_req = FullTextSearchRequest.generate_search_request(self.connection,
                                                              self.loop,
                                                              req.query_builder.as_encodable(),
                                                              default_serializer=self.default_serializer,
                                                              streaming_timeout=streaming_timeout,
                                                              obs_handler=req.obs_handler,
                                                              bucket_name=req.bucket_name,
                                                              scope_name=req.scope_name)
        d = Deferred()

        def _on_ok(_):
            d.callback(SearchResult(q_req))

        def _on_err(exc):
            d.errback(exc)

        query_d = q_req.execute_search_query()
        query_d.addCallback(_on_ok)
        query_d.addErrback(_on_err)
        return d

    def wait_until_bucket_connected_deferred(self) -> Deferred[None]:
        coro = super().wait_until_bucket_connected()
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d
