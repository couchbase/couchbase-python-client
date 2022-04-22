from __future__ import annotations

import asyncio
from asyncio import AbstractEventLoop
from datetime import timedelta
from time import perf_counter
from typing import (TYPE_CHECKING,
                    Any,
                    Awaitable,
                    Dict)

from acouchbase import get_event_loop
from acouchbase.analytics import AnalyticsQuery, AsyncAnalyticsRequest
from acouchbase.bucket import AsyncBucket
from acouchbase.logic import AsyncWrapper
from acouchbase.management.analytics import AnalyticsIndexManager
from acouchbase.management.buckets import BucketManager
from acouchbase.management.eventing import EventingFunctionManager
from acouchbase.management.queries import QueryIndexManager
from acouchbase.management.search import SearchIndexManager
from acouchbase.management.users import UserManager
from acouchbase.n1ql import AsyncN1QLRequest, N1QLQuery
from acouchbase.search import AsyncSearchRequest, SearchQueryBuilder
from acouchbase.transactions import Transactions
from couchbase.diagnostics import ClusterState, ServiceType
from couchbase.exceptions import UnAmbiguousTimeoutException
from couchbase.logic.cluster import ClusterLogic
from couchbase.options import PingOptions, forward_args
from couchbase.result import (AnalyticsResult,
                              ClusterInfoResult,
                              DiagnosticsResult,
                              PingResult,
                              QueryResult,
                              SearchResult)

if TYPE_CHECKING:
    from acouchbase.search import SearchQuery
    from couchbase.options import (AnalyticsOptions,
                                   ClusterOptions,
                                   DiagnosticsOptions,
                                   QueryOptions,
                                   SearchOptions,
                                   WaitUntilReadyOptions)


class AsyncCluster(ClusterLogic):

    def __init__(self,
                 connstr,  # type: str
                 *options,  # type: ClusterOptions
                 **kwargs,  # type: Dict[str, Any]
                 ) -> AsyncCluster:

        self._loop = self._get_loop(kwargs.pop("loop", None))
        super().__init__(connstr, *options, **kwargs)

        self._close_ftr = None
        self._connect_ftr = self._connect()

    @property
    def loop(self) -> AbstractEventLoop:
        """
        **INTERNAL**
        """
        return self._loop

    def _get_loop(self, loop=None) -> AbstractEventLoop:
        if not loop:
            loop = get_event_loop()

        if not loop.is_running():
            raise RuntimeError("Event loop is not running.")

        return loop

    @property
    def transactions(self) -> Transactions:
        if not self._transactions:
            self._transactions = Transactions(self, self._transaction_config)
        return self._transactions

    @AsyncWrapper.inject_connection_callbacks()
    def _connect(self, **kwargs) -> Awaitable:
        """
        **INTERNAL**
        """
        super()._connect_cluster(**kwargs)

    def on_connect(self) -> Awaitable:
        if not (self._connect_ftr or self.connected):
            self._connect_ftr = self._connect()
            self._close_ftr = None

        return self._connect_ftr

    @AsyncWrapper.inject_close_callbacks()
    def _close(self, **kwargs) -> Awaitable:
        """
        **INTERNAL**
        """
        super()._close_cluster(**kwargs)

    async def close(self) -> None:
        if self.connected and not self._close_ftr:
            self._close_ftr = self._close()
            self._connect_ftr = None

        await self._close_ftr
        super()._destroy_connection()

    def bucket(self, bucket_name) -> AsyncBucket:
        return AsyncBucket(self, bucket_name)

    def cluster_info(self) -> Awaitable[ClusterInfoResult]:
        if not self.connected:
            # @TODO(jc):  chain??
            raise RuntimeError(
                "Cluster is not connected, cannot get cluster info. "
                "Use await cluster.on_connect() to connect a cluster.")

        return self._get_cluster_info()

    @AsyncWrapper.inject_cluster_callbacks(ClusterInfoResult, set_cluster_info=True)
    def _get_cluster_info(self, **kwargs) -> Awaitable[ClusterInfoResult]:
        """**INTERNAL**

        use cluster_info()

        Returns:
            Awaitable: _description_
        """
        super()._get_cluster_info(**kwargs)

    @AsyncWrapper.inject_cluster_callbacks(PingResult, chain_connection=True)
    def ping(self,
             *opts,  # type: PingOptions
             **kwargs  # type: Any
             ) -> Awaitable[PingResult]:
        return super().ping(*opts, **kwargs)

    @AsyncWrapper.inject_cluster_callbacks(DiagnosticsResult, chain_connection=True)
    def diagnostics(self,
                    *opts,  # type: DiagnosticsOptions
                    **kwargs  # type: Dict[str, Any]
                    ) -> Awaitable[DiagnosticsResult]:

        return super().diagnostics(*opts, **kwargs)

    async def wait_until_ready(self,
                               timeout,  # type: timedelta
                               *opts,  # type: WaitUntilReadyOptions
                               **kwargs  # type: Dict[str, Any]
                               ) -> Awaitable[None]:
        final_args = forward_args(kwargs, *opts)
        service_types = final_args.get("service_types", None)
        if not service_types:
            service_types = [ServiceType(st.value) for st in ServiceType]

        desired_state = final_args.get("desired_state", ClusterState.Online)
        service_types_set = set(map(lambda st: st.value if isinstance(st, ServiceType) else st, service_types))

        # @TODO: handle units
        timeout_millis = timeout.total_seconds() * 1000

        interval_millis = float(50)
        start = perf_counter()
        time_left = timeout_millis
        while True:

            diag_res = await self.diagnostics()
            endpoint_svc_types = set(map(lambda st: st.value, diag_res.endpoints.keys()))
            if not endpoint_svc_types.issuperset(service_types_set):
                await self.ping(PingOptions(service_types=service_types))
                diag_res = await self.diagnostics()

            if diag_res.state == desired_state:
                break

            interval_millis += 500
            if interval_millis > 1000:
                interval_millis = 1000

            time_left = timeout_millis - ((perf_counter() - start) * 1000)
            if interval_millis > time_left:
                interval_millis = time_left

            if time_left <= 0:
                raise UnAmbiguousTimeoutException(message="Desired state not found.")

            await asyncio.sleep(interval_millis / 1000)

    def query(
        self,
        statement,  # type: str
        *options,  # type: QueryOptions
        **kwargs  # type: Any
    ) -> QueryResult:

        query = N1QLQuery.create_query_object(
            statement, *options, **kwargs)
        return QueryResult(AsyncN1QLRequest.generate_n1ql_request(self.connection,
                                                                  self.loop,
                                                                  query.params,
                                                                  default_serializer=self.default_serializer))

    def analytics_query(
        self,  # type: Cluster
        statement,  # type: str
        *options,  # type: AnalyticsOptions
        **kwargs
    ) -> AnalyticsResult:
        query = AnalyticsQuery.create_query_object(
            statement, *options, **kwargs)
        return AnalyticsResult(AsyncAnalyticsRequest.generate_analytics_request(
            self.connection,
            self.loop,
            query.params,
            default_serializer=self.default_serializer))

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
        return SearchResult(AsyncSearchRequest.generate_search_request(self.connection,
                                                                       self.loop,
                                                                       query.as_encodable(),
                                                                       default_serializer=self.default_serializer))

    def buckets(self) -> BucketManager:
        """
        Get the BucketManager.

        :return: A :class:`~.management.BucketManager` with which you can create or modify buckets on the cluster.
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

        :return:  A :class:`~.management.queries.QueryIndexManager` with which you can create or modify query indexes on
            the cluster.
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

        :return:  A :class:`~.management.SearchIndexManager` with which you can create or modify search indexes
            on the cluster.
        """
        # TODO:  AlreadyShutdownException?
        return SearchIndexManager(self.connection, self.loop)

    def eventing_functions(self) -> EventingFunctionManager:
        """
        Get the EventingFunctionManager.

        :return:  A :class:`~.management.EventingFunctionManager` with which you can create or modify eventing
            functions
        """
        # TODO:  AlreadyShutdownException?
        return EventingFunctionManager(self.connection, self.loop)

    @staticmethod
    async def connect(connstr,  # type: str
                      *options,  # type: ClusterOptions
                      **kwargs,  # type: Dict[str, Any]
                      ) -> AsyncCluster:
        cluster = AsyncCluster(connstr, *options, **kwargs)
        await cluster.on_connect()
        return cluster


Cluster = AsyncCluster
