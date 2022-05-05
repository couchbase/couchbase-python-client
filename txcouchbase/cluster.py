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
from time import perf_counter
from typing import (TYPE_CHECKING,
                    Any,
                    Dict)

from twisted.internet import task
from twisted.internet.defer import Deferred, inlineCallbacks

from acouchbase import get_event_loop
from couchbase.diagnostics import ClusterState, ServiceType
from couchbase.exceptions import UnAmbiguousTimeoutException
from couchbase.logic.analytics import AnalyticsQuery
from couchbase.logic.cluster import ClusterLogic
from couchbase.logic.n1ql import N1QLQuery
from couchbase.logic.search import SearchQueryBuilder
from couchbase.options import (DiagnosticsOptions,
                               PingOptions,
                               WaitUntilReadyOptions,
                               forward_args)
from couchbase.result import (AnalyticsResult,
                              ClusterInfoResult,
                              DiagnosticsResult,
                              PingResult,
                              QueryResult,
                              SearchResult)
from txcouchbase.analytics import AnalyticsRequest
from txcouchbase.bucket import Bucket
from txcouchbase.logic import TxWrapper
from txcouchbase.management.analytics import AnalyticsIndexManager
from txcouchbase.management.buckets import BucketManager
from txcouchbase.management.queries import QueryIndexManager
from txcouchbase.management.search import SearchIndexManager
from txcouchbase.management.users import UserManager
from txcouchbase.n1ql import N1QLRequest
from txcouchbase.search import SearchRequest

if TYPE_CHECKING:
    from datetime import timedelta

    from couchbase.options import (AnalyticsOptions,
                                   ClusterOptions,
                                   QueryOptions,
                                   SearchOptions)
    from couchbase.search import SearchQuery


class Cluster(ClusterLogic):

    def __init__(self,
                 connstr,  # type: str
                 *options,  # type: ClusterOptions
                 **kwargs,  # type: Dict[str, Any]
                 ) -> Cluster:

        self._loop = self._get_loop(kwargs.pop("loop", None))
        super().__init__(connstr, *options, **kwargs)

        self._close_d = None
        self._twisted_loop = None
        self._wur_state = {}
        self._connect_d = self._connect()

    @property
    def loop(self) -> AbstractEventLoop:
        """
        **INTERNAL**
        """
        return self._loop

    def _get_loop(self, loop=None) -> AbstractEventLoop:
        # no need to check if the loop is running, that will
        # be controlled by the reactor
        if not loop:
            loop = get_event_loop()

        return loop

    @TxWrapper.inject_connection_callbacks()
    def _connect(self, **kwargs) -> Deferred:
        """
        **INTERNAL**
        """
        super()._connect_cluster(**kwargs)

    def on_connect(self) -> Deferred:
        if not (self._connect_d or self.connected):
            self._connect_d = self._connect()
            self._close_d = None

        return self._connect_d

    def close(self) -> Deferred[None]:
        if self.connected and not self._close_d:
            self._close_d = self._close()
            self._connect_d = None

        d = Deferred()

        def _on_okay(_):
            super()._destroy_connection()
            d.callback(None)

        def _on_err(exc):
            d.errback(exc)

        self._close_d.addCallback(_on_okay)
        self._close_d.addErrback(_on_err)

        return d

    @TxWrapper.inject_close_callbacks()
    def _close(self, **kwargs) -> Deferred:
        """
        **INTERNAL**
        """
        super()._close_cluster(**kwargs)

    def bucket(self, bucket_name):
        return Bucket(self, bucket_name)

    def cluster_info(self) -> Deferred[ClusterInfoResult]:
        if not self.connected:
            # @TODO(jc):  chain??
            raise RuntimeError(
                "Cluster is not connected, cannot get cluster info. "
                "Use await cluster.on_connect() to connect a cluster.")

        return self._get_cluster_info()

    @TxWrapper.inject_cluster_callbacks(ClusterInfoResult, set_cluster_info=True)
    def _get_cluster_info(self, **kwargs) -> Deferred[ClusterInfoResult]:
        """**INTERNAL**

        use cluster_info()

        Returns:
            Deferred: _description_
        """
        super()._get_cluster_info(**kwargs)

    @TxWrapper.inject_cluster_callbacks(PingResult, chain_connection=True)
    def ping(self,
             *opts,  # type: PingOptions
             **kwargs  # type: Dict[str, Any]
             ) -> Deferred[PingResult]:
        return super().ping(*opts, **kwargs)

    @TxWrapper.inject_cluster_callbacks(DiagnosticsResult, chain_connection=True)
    def diagnostics(self,
                    *opts,  # type: DiagnosticsOptions
                    **kwargs  # type: Dict[str, Any]
                    ) -> Deferred[DiagnosticsResult]:

        return super().diagnostics(*opts, **kwargs)

    @inlineCallbacks
    def _wait_until_ready(self, service_types, desired_state):
        diag_res = yield self.diagnostics()
        endpoint_svc_types = set(map(lambda st: st.value, diag_res.endpoints.keys()))
        if not endpoint_svc_types.issuperset(service_types):
            yield self.ping(PingOptions(service_types=list(service_types)))
            diag_res = yield self.diagnostics()

        if diag_res.state == desired_state:
            self._twisted_loop.stop()

        self._wur_state["interval_millis"] += 500
        if self._wur_state["interval_millis"] > 1000:
            self._wur_state["interval_millis"] = 1000

        time_left = self._wur_state["timeout_millis"] - ((perf_counter() - self._wur_state["start"]) * 1000)
        if self._wur_state["interval_millis"] > time_left:
            self._wur_state["interval_millis"] = time_left

        if time_left <= 0:
            raise UnAmbiguousTimeoutException(message="Desired state not found.")

    def wait_until_ready(self,
                         timeout,  # type: timedelta
                         *opts,  # type: WaitUntilReadyOptions
                         **kwargs  # type: Dict[str, Any]
                         ) -> Deferred[None]:
        final_args = forward_args(kwargs, *opts)
        service_types = final_args.get("service_types", None)
        if not service_types:
            service_types = [ServiceType(st.value) for st in ServiceType]

        desired_state = final_args.get("desired_state", ClusterState.Online)
        service_types_set = set(map(lambda st: st.value if isinstance(st, ServiceType) else st, service_types))
        self._wur_state = {}

        # @TODO: handle units
        self._wur_state["timeout_millis"] = timeout.total_seconds() * 1000

        self._wur_state["interval_millis"] = float(500)
        self._wur_state["start"] = perf_counter()

        d = Deferred()
        self._twisted_loop = task.LoopingCall(self._wait_until_ready, service_types_set, desired_state)
        wur_d = self._twisted_loop.start(self._wur_state["interval_millis"] / 1000, now=True)

        def _on_okay(_):
            d.callback(True)

        def _on_err(exc):
            d.errback(exc)

        wur_d.addCallback(_on_okay)
        wur_d.addErrback(_on_err)
        return d

    def query(
        self,
        statement,  # type: str
        *options,  # type: QueryOptions
        **kwargs  # type: Dict[str, Any]
    ) -> Deferred[QueryResult]:

        query = N1QLQuery.create_query_object(
            statement, *options, **kwargs)
        request = N1QLRequest.generate_n1ql_request(self.connection,
                                                    self.loop,
                                                    query.params,
                                                    default_serializer=self.default_serializer)
        d = Deferred()

        def _on_ok(_):
            d.callback(QueryResult(request))

        def _on_err(exc):
            d.errback(exc)

        query_d = request.execute_query()
        query_d.addCallback(_on_ok)
        query_d.addErrback(_on_err)
        return d

    def analytics_query(
        self,
        statement,  # type: str
        *options,  # type: AnalyticsOptions
        **kwargs  # type: Dict[str, Any]
    ) -> Deferred[AnalyticsResult]:

        query = AnalyticsQuery.create_query_object(
            statement, *options, **kwargs)
        request = AnalyticsRequest.generate_analytics_request(self.connection,
                                                              self.loop,
                                                              query.params,
                                                              default_serializer=self.default_serializer)
        d = Deferred()

        def _on_ok(_):
            d.callback(AnalyticsResult(request))

        def _on_err(exc):
            d.errback(exc)

        query_d = request.execute_analytics_query()
        query_d.addCallback(_on_ok)
        query_d.addErrback(_on_err)
        return d

    def search_query(
        self,
        index,  # type: str
        query,  # type: SearchQuery
        *options,  # type: SearchOptions
        **kwargs  # type: Dict[str, Any]
    ) -> Deferred[SearchResult]:
        query = SearchQueryBuilder.create_search_query_object(
            index, query, *options, **kwargs
        )
        request = SearchRequest.generate_search_request(self.connection,
                                                        self.loop,
                                                        query.as_encodable(),
                                                        default_serializer=self.default_serializer)
        d = Deferred()

        def _on_ok(_):
            d.callback(SearchResult(request))

        def _on_err(exc):
            d.errback(exc)

        query_d = request.execute_search_query()
        query_d.addCallback(_on_ok)
        query_d.addErrback(_on_err)
        return d

    def buckets(self) -> BucketManager:
        """
        Get the BucketManager.

        :return: A :class:`~.management.BucketManager` with which you can create or
              modify buckets on the cluster.
        """
        # TODO:  AlreadyShutdownException?
        return BucketManager(self.connection, self.loop)

    def users(self) -> UserManager:
        """
        Get the UserManager.

        :return: A :class:`~.management.UserManager` with which you can create or update cluster users and roles.
        """
        # TODO:  AlreadyShutdownException?
        return UserManager(self.connection, self.loop)

    def query_indexes(self) -> QueryIndexManager:
        """
        Get the QueryIndexManager.

        :return:  A :class:`~.management.queries.QueryIndexManager` with which you can
              create or modify query indexes on the cluster.
        """
        # TODO:  AlreadyShutdownException?
        return QueryIndexManager(self.connection, self.loop)

    def analytics_indexes(self) -> AnalyticsIndexManager:
        """
        Get the AnalyticsIndexManager.

        :return:  A :class:`~.management.AnalyticsIndexManager` with which you can create or modify analytics datasets,
            dataverses, etc.. on the cluster.
        """
        # TODO:  AlreadyShutdownException?
        return AnalyticsIndexManager(self.connection, self.loop)

    def search_indexes(self) -> SearchIndexManager:
        """
        Get the SearchIndexManager.

        :return:  A :class:`~.management.SearchIndexManager` with which you can create or modify analytics datasets,
            dataverses, etc.. on the cluster.
        """
        # TODO:  AlreadyShutdownException?
        return SearchIndexManager(self.connection, self.loop)


TxCluster = Cluster
