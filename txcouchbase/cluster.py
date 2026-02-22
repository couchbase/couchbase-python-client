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

# used to allow for unquoted (i.e. forward reference, Python >= 3.7, PEP563)
from __future__ import annotations

from asyncio import AbstractEventLoop
from typing import (TYPE_CHECKING,
                    Any,
                    Dict)

from twisted.internet.defer import Deferred

from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import StreamingOperationType
from couchbase.options import (DiagnosticsOptions,
                               PingOptions,
                               WaitUntilReadyOptions)
from couchbase.result import (AnalyticsResult,
                              ClusterInfoResult,
                              DiagnosticsResult,
                              PingResult,
                              QueryResult,
                              SearchResult)
from txcouchbase.bucket import Bucket
from txcouchbase.logic.cluster_impl import TxClusterImpl
from txcouchbase.management.analytics import AnalyticsIndexManager
from txcouchbase.management.buckets import BucketManager
from txcouchbase.management.queries import QueryIndexManager
from txcouchbase.management.search import SearchIndexManager
from txcouchbase.management.users import UserManager

if TYPE_CHECKING:
    from datetime import timedelta

    from couchbase.options import (AnalyticsOptions,
                                   ClusterOptions,
                                   QueryOptions,
                                   SearchOptions)
    from couchbase.search import SearchQuery, SearchRequest


class Cluster:

    def __init__(self,
                 connstr,  # type: str
                 *options,  # type: ClusterOptions
                 **kwargs,  # type: Dict[str, Any]
                 ) -> None:
        self._impl = TxClusterImpl(connstr, *options, **kwargs)

    @property
    def loop(self) -> AbstractEventLoop:
        """
        **INTERNAL**
        """
        return self._impl.loop

    def on_connect(self) -> Deferred[None]:
        return self._impl.wait_until_connected_deferred()

    def close(self) -> Deferred[None]:
        return self._impl.close_connection_deferred()

    def bucket(self, bucket_name):
        return Bucket(self, bucket_name)

    def cluster_info(self) -> Deferred[ClusterInfoResult]:
        req = self._impl.request_builder.build_cluster_info_request()
        return self._impl.get_cluster_info_deferred(req)

    def ping(self,
             *opts,  # type: PingOptions
             **kwargs  # type: Dict[str, Any]
             ) -> Deferred[PingResult]:
        req = self._impl.request_builder.build_ping_request(*opts, **kwargs)
        return self._impl.ping_deferred(req)

    def diagnostics(self,
                    *opts,  # type: DiagnosticsOptions
                    **kwargs  # type: Dict[str, Any]
                    ) -> Deferred[DiagnosticsResult]:

        req = self._impl.request_builder.build_diagnostics_request(*opts, **kwargs)
        return self._impl.diagnostics_deferred(req)

    def wait_until_ready(self,
                         timeout,  # type: timedelta
                         *opts,  # type: WaitUntilReadyOptions
                         **kwargs  # type: Dict[str, Any]
                         ) -> Deferred[None]:
        req = self._impl.request_builder.build_wait_until_ready_request(timeout, *opts, **kwargs)
        return self._impl.wait_until_ready_deferred(req)

    def query(
        self,
        statement,  # type: str
        *options,  # type: QueryOptions
        **kwargs  # type: Dict[str, Any]
    ) -> Deferred[QueryResult]:
        op_type = StreamingOperationType.Query
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        req = self._impl.request_builder.build_query_request(statement, obs_handler, *options, **kwargs)
        return self._impl.query_deferred(req)

    def analytics_query(
        self,
        statement,  # type: str
        *options,  # type: AnalyticsOptions
        **kwargs  # type: Dict[str, Any]
    ) -> Deferred[AnalyticsResult]:
        op_type = StreamingOperationType.AnalyticsQuery
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        req = self._impl.request_builder.build_analytics_query_request(statement, obs_handler, *options, **kwargs)
        return self._impl.analytics_query_deferred(req)

    def search_query(self,
                     index,  # type: str
                     query,  # type: SearchQuery
                     *options,  # type: SearchOptions
                     **kwargs  # type: Dict[str, Any]
                     ) -> Deferred[SearchResult]:
        op_type = StreamingOperationType.SearchQuery
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        req = self._impl.request_builder.build_search_request(index, query, obs_handler, *options, **kwargs)
        return self._impl.search_deferred(req)

    def search(self,
               index,  # type: str
               request,  # type: SearchRequest
               *options,  # type: SearchOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> Deferred[SearchResult]:
        op_type = StreamingOperationType.SearchQuery
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        req = self._impl.request_builder.build_search_request(index, request, obs_handler, *options, **kwargs)
        return self._impl.search_deferred(req)

    def buckets(self) -> BucketManager:
        """
        Get the BucketManager.

        :return: A :class:`~.management.BucketManager` with which you can create or
              modify buckets on the cluster.
        """
        return BucketManager(self._impl._client_adapter, self._impl.observability_instruments)

    def users(self) -> UserManager:
        """
        Get the UserManager.

        :return: A :class:`~.management.UserManager` with which you can create or update cluster users and roles.
        """
        return UserManager(self._impl._client_adapter, self._impl.observability_instruments)

    def query_indexes(self) -> QueryIndexManager:
        """
        Get the QueryIndexManager.

        :return:  A :class:`~.management.queries.QueryIndexManager` with which you can
              create or modify query indexes on the cluster.
        """
        return QueryIndexManager(self._impl._client_adapter, self._impl.observability_instruments)

    def analytics_indexes(self) -> AnalyticsIndexManager:
        """
        Get the AnalyticsIndexManager.

        :return:  A :class:`~.management.AnalyticsIndexManager` with which you can create or modify analytics datasets,
            dataverses, etc.. on the cluster.
        """
        return AnalyticsIndexManager(self._impl.client_adapter, self._impl.observability_instruments)

    def search_indexes(self) -> SearchIndexManager:
        """
        Get the SearchIndexManager.

        :return:  A :class:`~.management.SearchIndexManager` with which you can create or modify analytics datasets,
            dataverses, etc.. on the cluster.
        """
        return SearchIndexManager(self._impl._client_adapter, self._impl.observability_instruments)


TxCluster = Cluster
