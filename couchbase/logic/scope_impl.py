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

from typing import TYPE_CHECKING, Optional

from couchbase.analytics import AnalyticsRequest
from couchbase.logic.cluster_impl import ClusterSettings
from couchbase.logic.cluster_settings import StreamingTimeouts
from couchbase.logic.scope_req_builder import ScopeRequestBuilder
from couchbase.logic.top_level_types import PyCapsuleType
from couchbase.n1ql import N1QLRequest
from couchbase.result import (AnalyticsResult,
                              QueryResult,
                              SearchResult)
from couchbase.search import FullTextSearchRequest
from couchbase.serializer import Serializer
from couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from couchbase.bucket import Bucket
    from couchbase.logic.scope_types import (AnalyticsQueryRequest,
                                             QueryRequest,
                                             SearchQueryRequest)


class ScopeImpl:

    def __init__(self, scope_name: str, bucket: Bucket) -> None:
        self._scope_name = scope_name
        self._bucket_impl = bucket._impl
        # needed for the CollectionImpl
        self._client_adapter = bucket._impl._client_adapter
        self._request_builder = ScopeRequestBuilder(self._bucket_impl.bucket_name, self._scope_name)

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
    def connection(self) -> Optional[PyCapsuleType]:
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
        return self.cluster_settings.streaming_timeouts

    @property
    def name(self) -> str:
        return self._scope_name

    def analytics_query(self, req: AnalyticsQueryRequest) -> AnalyticsResult:
        self._client_adapter._ensure_not_closed()
        self._client_adapter._ensure_connected()
        # If the analytics_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the analytics_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's analytics_timeout (set here). If the cluster
        # also does not specify an analytics_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::analytics_timeout when the streaming object is created in the bindings.
        streaming_timeout = self.streaming_timeouts.get('analytics_timeout', None)
        return AnalyticsResult(AnalyticsRequest.generate_analytics_request(self.connection,
                                                                           req.analytics_query.params,
                                                                           default_serializer=self.default_serializer,
                                                                           streaming_timeout=streaming_timeout))

    def query(self, req: QueryRequest) -> QueryResult:
        self._client_adapter._ensure_not_closed()
        self._client_adapter._ensure_connected()
        # If the n1ql_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the n1ql_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's query_timeout (set here). If the cluster
        # also does not specify a query_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::query_timeout when the streaming object is created in the bindings.
        streaming_timeout = self.streaming_timeouts.get('query_timeout', None)
        return QueryResult(N1QLRequest.generate_n1ql_request(self.connection,
                                                             req.n1ql_query.params,
                                                             default_serializer=self.default_serializer,
                                                             streaming_timeout=streaming_timeout))

    def search(self, req: SearchQueryRequest) -> SearchResult:
        self._client_adapter._ensure_not_closed()
        self._client_adapter._ensure_connected()
        # If the search_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the search_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's search_timeout (set here). If the cluster
        # also does not specify a search_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::search_timeout when the streaming object is created in the bindings.
        streaming_timeout = self.streaming_timeouts.get('search_timeout', None)
        return SearchResult(FullTextSearchRequest.generate_search_request(self.connection,
                                                                          req.query_builder.as_encodable(),
                                                                          default_serializer=self.default_serializer,
                                                                          streaming_timeout=streaming_timeout))
