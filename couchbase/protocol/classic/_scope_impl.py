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

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from couchbase.common._cluster_settings import ClusterSettings
from couchbase.common._observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common._streaming import RowIterator
from couchbase.protocol.classic._cluster_types import StreamingTimeouts
from couchbase.protocol.classic._collection_impl import CollectionImpl
from couchbase.protocol.classic._management._eventing_function_mgmt_impl import EventingFunctionMgmtImpl
from couchbase.protocol.classic._management._search_index_mgmt_impl import SearchIndexMgmtImpl
from couchbase.protocol.classic._pycbc_core import pycbc_connection
from couchbase.protocol.classic._scope_req_builder import ScopeRequestBuilder
from couchbase.protocol.classic._streaming import (AnalyticsStreamSpec,
                                                   ClassicStreamingExecutor,
                                                   QueryStreamSpec,
                                                   SearchStreamSpec)
from couchbase.result import (AnalyticsResult,
                              QueryResult,
                              SearchResult)
from couchbase.serializer import Serializer
from couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from couchbase.common._utils import JSONType
    from couchbase.common.options import (AnalyticsOptions,
                                          QueryOptions,
                                          SearchOptions)
    from couchbase.common.protocol import CouchbaseProtocol
    from couchbase.common.search_queries import SearchQuery
    from couchbase.common.search_request import SearchRequest
    from couchbase.protocol.classic._bucket_impl import BucketImpl
    from couchbase.protocol.classic._client_adapter import ClientAdapter
    from couchbase.protocol.classic._scope_types import SearchQueryRequest


class ScopeImpl:

    def __init__(self, scope_name: str, bucket_impl: BucketImpl) -> None:
        self._scope_name = scope_name
        self._bucket_impl = bucket_impl
        # needed for the CollectionImpl
        self._client_adapter: ClientAdapter = bucket_impl._client_adapter
        self._request_builder = ScopeRequestBuilder(self._bucket_impl.bucket_name, self._scope_name,
                                                    self._client_adapter.option_validation)

    @property
    def couchbase_protocol(self) -> CouchbaseProtocol:
        """**INTERNAL**"""
        return self._client_adapter.couchbase_protocol

    @property
    def bucket_name(self) -> str:
        return self._bucket_impl.bucket_name

    @property
    def cluster_settings(self) -> ClusterSettings:
        return self._bucket_impl.cluster_settings

    @property
    def connected(self) -> bool:
        return self._bucket_impl.connected

    @property
    def connection(self) -> pycbc_connection:
        """
        **INTERNAL**
        """
        return self._client_adapter.connection

    @property
    def default_serializer(self) -> Serializer:
        return self.cluster_settings.default_serializer

    @property
    def default_transcoder(self) -> Transcoder:
        return self.cluster_settings.default_transcoder

    @property
    def request_builder(self) -> ScopeRequestBuilder:
        return self._request_builder

    @property
    def streaming_timeouts(self) -> StreamingTimeouts:
        """
        **INTERNAL**
        """
        return self._bucket_impl.streaming_timeouts

    @property
    def kv_durable_timeout(self) -> int | None:
        """**INTERNAL**"""
        return self._bucket_impl.kv_durable_timeout

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        """**INTERNAL**"""
        return self.cluster_settings.observability_instruments

    @property
    def name(self) -> str:
        return self._scope_name

    def collection(self, name: str) -> CollectionImpl:
        """**INTERNAL**"""
        return CollectionImpl(name, self)

    def query(self,
              statement: str,
              *options: QueryOptions | JSONType,
              **kwargs: Any) -> QueryResult:
        obs_handler = ObservableRequestHandler(Op.Query, self.observability_instruments)
        req = self._request_builder.build_query_request(statement, obs_handler, *options, **kwargs)
        self._client_adapter._ensure_not_closed()
        self._client_adapter._ensure_connected()
        # If the n1ql_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the n1ql_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's query_timeout (set here). If the cluster
        # also does not specify a query_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::query_timeout when the streaming object is created in the bindings.
        streaming_timeout = self.streaming_timeouts.get('query_timeout', None)
        return QueryResult(RowIterator(ClassicStreamingExecutor(self.connection,
                                                                QueryStreamSpec(req.n1ql_query.params,
                                                                                self.default_serializer),
                                                                obs_handler=req.obs_handler,
                                                                streaming_timeout=streaming_timeout)))

    def analytics_query(self,
                        statement: str,
                        *options: AnalyticsOptions | JSONType,
                        **kwargs: Any) -> AnalyticsResult:
        obs_handler = ObservableRequestHandler(Op.AnalyticsQuery, self.observability_instruments)
        req = self._request_builder.build_analytics_query_request(statement, obs_handler, *options, **kwargs)
        self._client_adapter._ensure_not_closed()
        self._client_adapter._ensure_connected()
        # If the analytics_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the analytics_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's analytics_timeout (set here). If the cluster
        # also does not specify an analytics_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::analytics_timeout when the streaming object is created in the bindings.
        streaming_timeout = self.streaming_timeouts.get('analytics_timeout', None)
        return AnalyticsResult(RowIterator(ClassicStreamingExecutor(self.connection,
                                                                    AnalyticsStreamSpec(req.analytics_query.params,
                                                                                        self.default_serializer),
                                                                    obs_handler=req.obs_handler,
                                                                    streaming_timeout=streaming_timeout)))

    def search_query(self,
                     index: str,
                     query: SearchQuery,
                     options: SearchOptions | None = None,
                     **kwargs: Any) -> SearchResult:
        obs_handler = ObservableRequestHandler(Op.SearchQuery, self.observability_instruments)
        req = self._request_builder.build_search_query_request(index, query, obs_handler, options, **kwargs)
        return self._search(req)

    def search(self,
               index: str,
               request: SearchRequest | SearchQuery,
               options: SearchOptions | None = None,
               **kwargs: Any) -> SearchResult:
        obs_handler = ObservableRequestHandler(Op.SearchQuery, self.observability_instruments)
        req = self._request_builder.build_search_request(index, request, obs_handler, options, **kwargs)
        return self._search(req)

    def eventing_function_mgmt(self) -> EventingFunctionMgmtImpl:
        """**INTERNAL**"""
        return EventingFunctionMgmtImpl(self._client_adapter,
                                        self.observability_instruments,
                                        scope_context=(self.bucket_name, self._scope_name))

    def search_index_mgmt(self) -> SearchIndexMgmtImpl:
        """**INTERNAL**"""
        return SearchIndexMgmtImpl(self._client_adapter,
                                   self.observability_instruments,
                                   scope_context=(self.bucket_name, self._scope_name))

    def _search(self, req: SearchQueryRequest) -> SearchResult:
        self._client_adapter._ensure_not_closed()
        self._client_adapter._ensure_connected()
        # If the search_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the search_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's search_timeout (set here). If the cluster
        # also does not specify a search_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::search_timeout when the streaming object is created in the bindings.
        streaming_timeout = self.streaming_timeouts.get('search_timeout', None)
        return SearchResult(RowIterator(ClassicStreamingExecutor(self.connection,
                                                                 SearchStreamSpec(req.query_builder.as_encodable(),
                                                                                  self.default_serializer,
                                                                                  bucket_name=req.bucket_name,
                                                                                  scope_name=req.scope_name),
                                                                 obs_handler=req.obs_handler,
                                                                 streaming_timeout=streaming_timeout)))
