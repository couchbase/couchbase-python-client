from __future__ import annotations

import time
from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Any,
                    Dict)

from couchbase.analytics import AnalyticsQuery, AnalyticsRequest
from couchbase.bucket import Bucket
from couchbase.diagnostics import ClusterState, ServiceType
from couchbase.exceptions import UnAmbiguousTimeoutException
from couchbase.logic import BlockingWrapper
from couchbase.logic.cluster import ClusterLogic
from couchbase.management.analytics import AnalyticsIndexManager
from couchbase.management.buckets import BucketManager
from couchbase.management.eventing import EventingFunctionManager
from couchbase.management.queries import QueryIndexManager
from couchbase.management.search import SearchIndexManager
from couchbase.management.users import UserManager
from couchbase.n1ql import N1QLQuery, N1QLRequest
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
from couchbase.search import SearchQueryBuilder, SearchRequest
from couchbase.transactions import Transactions

if TYPE_CHECKING:
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

        super().__init__(connstr, *options, **kwargs)
        self._connect()

    @BlockingWrapper.block(True)
    def _connect(self, **kwargs):
        conn = super()._connect_cluster(**kwargs)
        self._set_connection(conn)

    @BlockingWrapper.block(True)
    def _close_cluster(self):
        res = super()._close_cluster()
        if res is not None:
            super()._destroy_connection()

    @property
    def transactions(self) -> Transactions:
        if not self._transactions:
            self._transactions = Transactions(self, self._transaction_config)
        return self._transactions

    def close(self):
        if self.connected:
            self._close_cluster()

    def bucket(self, bucket_name):
        if not self.connected:
            raise RuntimeError("Cluster not yet connected.")

        return Bucket(self, bucket_name)

    def cluster_info(self) -> ClusterInfoResult:
        if not self.connected:
            # @TODO(jc):  chain??
            raise RuntimeError(
                "Cluster is not connected, cannot get cluster info.")

        cluster_info = self._get_cluster_info()
        self._cluster_info = cluster_info
        return cluster_info

    @BlockingWrapper.block(ClusterInfoResult)
    def _get_cluster_info(self):
        return super()._get_cluster_info()

    @BlockingWrapper.block(PingResult)
    def ping(self,
             *opts,  # type: PingOptions
             **kwargs  # type: Any
             ) -> PingResult:
        return super().ping(*opts, **kwargs)

    @BlockingWrapper.block(DiagnosticsResult)
    def diagnostics(self,
                    *opts,  # type: DiagnosticsOptions
                    **kwargs  # type: Dict[str, Any]
                    ) -> DiagnosticsResult:

        return super().diagnostics(*opts, **kwargs)

    def wait_until_ready(self,
                         timeout,  # type: timedelta
                         *opts,  # type: WaitUntilReadyOptions
                         **kwargs  # type: Dict[str, Any]
                         ) -> None:
        final_args = forward_args(kwargs, *opts)
        service_types = final_args.get("service_types", None)
        if not service_types:
            service_types = [ServiceType(st.value) for st in ServiceType]

        desired_state = final_args.get("desired_state", ClusterState.Online)
        service_types_set = set(map(lambda st: st.value if isinstance(st, ServiceType) else st, service_types))

        # @TODO: handle units
        timeout_millis = timeout.total_seconds() * 1000

        interval_millis = float(50)
        start = time.perf_counter()
        time_left = timeout_millis
        while True:

            diag_res = self.diagnostics()
            endpoint_svc_types = set(map(lambda st: st.value, diag_res.endpoints.keys()))
            if not endpoint_svc_types.issuperset(service_types_set):
                self.ping(PingOptions(service_types=service_types))
                diag_res = self.diagnostics()

            if diag_res.state == desired_state:
                break

            interval_millis += 500
            if interval_millis > 1000:
                interval_millis = 1000

            time_left = timeout_millis - ((time.perf_counter() - start) * 1000)
            if interval_millis > time_left:
                interval_millis = time_left

            if time_left <= 0:
                raise UnAmbiguousTimeoutException(message="Desired state not found.")

            time.sleep(interval_millis / 1000)

    def query(
        self,
        statement,  # type: str
        *options,  # type: QueryOptions
        **kwargs  # type: Any
    ) -> QueryResult:

        query = N1QLQuery.create_query_object(
            statement, *options, **kwargs)
        return QueryResult(N1QLRequest.generate_n1ql_request(self.connection, query.params))

    def analytics_query(
        self,  # type: Cluster
        statement,  # type: str
        *options,  # type: AnalyticsOptions
        **kwargs
    ) -> AnalyticsResult:

        query = AnalyticsQuery.create_query_object(
            statement, *options, **kwargs)
        return AnalyticsResult(AnalyticsRequest.generate_analytics_request(self.connection,
                                                                           query.params))

    def search_query(
        self,
        index,  # type: str
        query,  # type: SearchQuery
        *options,  # type: SearchOptions
        **kwargs
    ) -> SearchResult:
        query = SearchQueryBuilder.create_search_query_object(
            index, query, *options, **kwargs
        )
        return SearchResult(SearchRequest.generate_search_request(self.connection,
                                                                  query.as_encodable()))

    def buckets(self) -> BucketManager:
        """
        Get the BucketManager.

        :return: A :class:`~.management.BucketManager` with which you can create or modify buckets on the cluster.
        """
        # TODO:  AlreadyShutdownException?
        return BucketManager(self.connection)

    def users(self) -> UserManager:
        """
        Get the UserManager.

        :return: A :class:`~.management.UserManager` with which you can create or update cluster users and roles.
        """
        # TODO:  AlreadyShutdownException?
        return UserManager(self.connection)

    def query_indexes(self) -> QueryIndexManager:
        """
        Get the QueryIndexManager.

        :return:  A :class:`~.management.queries.QueryIndexManager` with which you can create or modify query indexes on
            the cluster.
        """
        # TODO:  AlreadyShutdownException?
        return QueryIndexManager(self.connection)

    def analytics_indexes(self) -> AnalyticsIndexManager:
        """
        Get the AnalyticsIndexManager.

        :return:  A :class:`~.management.AnalyticsIndexManager` with which you can create or modify analytics datasets,
            dataverses, etc.. on the cluster.
        """
        # TODO:  AlreadyShutdownException?
        return AnalyticsIndexManager(self.connection)

    def search_indexes(self) -> SearchIndexManager:
        """
        Get the SearchIndexManager.

        :return:  A :class:`~.management.SearchIndexManager` with which you can create or modify analytics datasets,
            dataverses, etc.. on the cluster.
        """
        # TODO:  AlreadyShutdownException?
        return SearchIndexManager(self.connection)

    def eventing_functions(self) -> EventingFunctionManager:
        """
        Get the EventingFunctionManager.

        :return:  A :class:`~.management.EventingFunctionManager` with which you can create or modify eventing
            functions.
        """
        # TODO:  AlreadyShutdownException?
        return EventingFunctionManager(self.connection)

    @staticmethod
    def connect(connstr,  # type: str
                *options,  # type: ClusterOptions
                **kwargs,  # type: Dict[str, Any]
                ) -> "Cluster":
        cluster = Cluster(connstr, *options, **kwargs)
        return cluster
