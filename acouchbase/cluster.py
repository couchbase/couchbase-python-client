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
    """Create a Couchbase Cluster instance.

    The cluster instance exposes the operations which are available to be performed against a cluster.

    .. note::
        Although creating an instance of :class:`.AsyncCluster` is allowed, it is recommended to
        use the AsyncCluster's static :meth:`.AsyncCluster.connect` method. See :meth:`.AsyncCluster.connect` for
        connect for examples.

    Args:
        connstr (str):
            The connection string to use for connecting to the cluster.
            This is a URI-like string allowing specifying multiple hosts.

            The format of the connection string is the *scheme*
            (``couchbase`` for normal connections, ``couchbases`` for
            SSL enabled connections); a list of one or more *hostnames*
            delimited by commas
        options (:class:`~couchbase.options.ClusterOptions`): Global options to set for the cluster.
            Some operations allow the global options to be overriden by passing in options to the
            operation.
        **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
            overrride provided :class:`~couchbase.options.ClusterOptions`

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If no :class:`~couchbase.auth.Authenticator`
            is provided.  Also raised if an invalid `ClusterOption` is provided.
        :class:`~couchbase.exceptions.AuthenticationException`: If provided :class:`~couchbase.auth.Authenticator`
            has incorrect credentials.

    """

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
        """
            :class:`~acouchbase.transactions.Transactions`: A Transactions instance which can be used to
                perform transactions on this cluster.
        """
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
        """Returns an awaitable future that indicates connecting to the Couchbase cluster has completed.

        .. note::
            It is recommended to use the AsyncCluster's static :meth:`.AsyncCluster.connect` method.
            See :meth:`.AsyncCluster.connect` for connect for examples.

        Returns:
            Awaitable: An empty future.  If a result is provided, connecting to the Couchbase cluster is complete.
                Otherwise an exception is raised.

        Raises:
            :class:`~couchbase.exceptions.UnAmbiguousTimeoutException`: If an error occured while trying to connect.
        """
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
        """Shuts down this cluster instance. Cleaning up all resources associated with it.

        .. warning::
            Use of this method is almost *always* unnecessary.  Cluster resources should be cleaned
            up once the cluster instance falls out of scope.  However, in some applications tuning resources
            is necessary and in those types of applications, this method might be beneficial.

        """
        if self.connected and not self._close_ftr:
            self._close_ftr = self._close()
            self._connect_ftr = None

        await self._close_ftr
        super()._destroy_connection()

    def bucket(self, bucket_name) -> AsyncBucket:
        """Creates a Bucket instance to a specific bucket.

        .. seealso::
            :class:`.bucket.AsyncBucket`

        Args:
            bucket_name (str): Name of the bucket to reference

        Returns:
            :class:`~acouchbase.bucket.AsyncBucket`: A bucket instance

        Raises:
            :class:`~couchbase.exceptions.BucketNotFoundException`: If provided `bucket_name` cannot
                be found.

        """
        return AsyncBucket(self, bucket_name)

    def cluster_info(self) -> Awaitable[ClusterInfoResult]:
        """Retrieve the Couchbase cluster information

        .. note::
            If using Couchbase Server version < 6.6, a bucket *must* be opened prior to calling
            `cluster.cluster_info()`.  If a bucket is not opened a
            :class:`~couchbase.exceptions.ServiceUnavailableException` will be raised.


        Returns:
            Awaitable[:class:`~couchbase.result.ClusterInfoResult`]: Information about the connected cluster.

        Raises:
            RuntimeError:  If called prior to the cluster being connected.
            :class:`~couchbase.exceptions.ServiceUnavailableException`: If called prior to connecting
                to a bucket if using server version < 6.6.

        """
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
        """Performs a ping operation against the cluster.

        The ping operation pings the services which are specified
        (or all services if none are specified). Returns a report which describes the outcome of
        the ping operations which were performed.

        Args:
            opts (:class:`~couchbase.options.PingOptions`): Optional parameters for this operation.

        Returns:
            Awaitable[:class:`~couchbase.result.PingResult`]: A report which describes the outcome of the
            ping operations which were performed.

        """
        return super().ping(*opts, **kwargs)

    @AsyncWrapper.inject_cluster_callbacks(DiagnosticsResult, chain_connection=True)
    def diagnostics(self,
                    *opts,  # type: DiagnosticsOptions
                    **kwargs  # type: Dict[str, Any]
                    ) -> Awaitable[DiagnosticsResult]:
        """Performs a diagnostic operation against the cluster.

        The diagnostic operations returns a report about the current active connections with the cluster.
        Includes information about remote and local addresses, last activity, and other diagnostics information.

        Args:
            opts (:class:`~couchbase.options.DiagnosticsOptions`): Optional parameters for this operation.

        Returns:
            Awaitable[:class:`~couchbase.result.DiagnosticsResult`]: A report which describes current active
            connections with the cluster.

        """
        return super().diagnostics(*opts, **kwargs)

    async def wait_until_ready(self,
                               timeout,  # type: timedelta
                               *opts,  # type: WaitUntilReadyOptions
                               **kwargs  # type: Dict[str, Any]
                               ) -> Awaitable[None]:
        """Wait until the cluster is ready for use.

            Check the current connections to see if the desired state has been reached.  If not,
            perform a ping against the specified services. The ping operation will be performed
            repeatedly with a slight delay in between until the specified timeout has been reached
            or the cluster is ready for use, whichever comes first.

            .. seealso::
                * :class:`~couchbase.diagnostics.ServiceType`
                * :class:`~couchbase.diagnostics.ClusterState`

        Args:
            timeout (timedelta): Amount of time to wait for cluster to be ready before a
                :class:`~couchbase.exceptions.UnAmbiguousTimeoutException` is raised.
            opts (:class:`~couchbase.options.WaitUntilReadyOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.WaitUntilReadyOptions`

        Raises:
            :class:`~couchbase.exceptions.UnAmbiguousTimeoutException`: If the specified timeout is reached prior to
                the cluster being ready for use.

        Example:

            Wait until the cluster is ready to use KV and query services::

                from acouchbase.cluster import Cluster
                from couchbase.auth import PasswordAuthenticator
                from couchbase.diagnostics import ServiceType
                from couchbase.options import WaitUntilReadyOptions

                auth = PasswordAuthenticator('username', 'password')
                cluster = Cluster.connect('couchbase://localhost', ClusterOptions(auth))

                await cluster.wait_until_ready(timedelta(seconds=3),
                         WaitUntilReadyOptions(service_types=[ServiceType.KeyValue, ServiceType.Query]))

        """
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
        """Executes a N1QL query against the cluster.

        .. note::
            The query is executed lazily in that it is executed once iteration over the
            :class:`~couchbase.result.QueryResult` begins.

        .. seealso::
            * :class:`~acouchbase.management.queries.QueryIndexManager`: for how to manage query indexes
            * :meth:`~acouchbase.scope.AsyncScope.query`: For how to execute scope-level queries.

        Args:
            statement (str): The N1QL statement to execute.
            options (:class:`~couchbase.options.QueryOptions`): Optional parameters for the query operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.QueryOptions`

        Returns:
            :class:`~couchbase.result.QueryResult`: An instance of a :class:`~couchbase.result.QueryResult` which
            provides access to iterate over the query results and access metadata and metrics about the query.

        Examples:
            Simple query::

                q_res = cluster.query('SELECT * FROM `travel-sample` WHERE country LIKE 'United%' LIMIT 2;')
                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple query with positional parameters::

                from couchbase.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $1 LIMIT $2;'
                q_res = cluster.query(q_str, QueryOptions(positional_parameters=['United%', 5]))
                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple query with named parameters::

                from couchbase.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $country LIMIT $lim;'
                q_res = cluster.query(q_str, QueryOptions(named_parameters={'country': 'United%', 'lim':2}))
                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Retrieve metadata and/or metrics from query::

                from couchbase.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $country LIMIT $lim;'
                q_res = cluster.query(q_str, QueryOptions(metrics=True))
                async for row in q_res.rows():
                    print(f'Found row: {row}')

                print(f'Query metadata: {q_res.metadata()}')
                print(f'Query metrics: {q_res.metadata().metrics()}')

        """

        request_args = dict()
        num_workers = kwargs.pop('num_workers', None)
        if num_workers:
            request_args['num_workers'] = num_workers
        query = N1QLQuery.create_query_object(
            statement, *options, **kwargs)
        return QueryResult(AsyncN1QLRequest.generate_n1ql_request(self.connection,
                                                                  self.loop,
                                                                  query.params,
                                                                  **request_args))

    def analytics_query(
        self,  # type: Cluster
        statement,  # type: str
        *options,  # type: AnalyticsOptions
        **kwargs
    ) -> AnalyticsResult:
        """Executes an analaytics query against the cluster.

        .. note::
            The analytics query is executed lazily in that it is executed once iteration over the
            :class:`~couchbase.result.AnalyticsResult` begins.

        .. seealso::
            * :class:`~acouchbase.management.analytics.AnalyticsIndexManager`: for how to manage analytics dataverses, datasets, indexes and links.
            * :meth:`~acouchbase.scope.AsyncScope.analytics_query`: for how to execute scope-level analytics queries

        Args:
            statement (str): The analytics SQL++ statement to execute.
            options (:class:`~couchbase.options.AnalyticsOptions`): Optional parameters for the analytics query
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.AnalyticsOptions`

        Returns:
            :class:`~couchbase.result.AnalyticsResult`: An instance of a
            :class:`~couchbase.result.AnalyticsResult` which provides access to iterate over the analytics
            query results and access metadata and metrics about the analytics query.

        Examples:
            .. note::
                Be sure to setup the necessary dataverse(s), dataset(s) for your analytics queries.
                See :analytics_intro:`Analytics Introduction <>` in Couchbase Server docs.

            Simple analytics query::

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $1 LIMIT $2;'
                q_res = cluster.analytics_query(q_str)
                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple analytics query with positional parameters::

                from couchbase.options import AnalyticsOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $1 LIMIT $2;'
                q_res = cluster.analytics_query(q_str, AnalyticsOptions(positional_parameters=['United%', 5]))
                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple analytics query with named parameters::

                from couchbase.options import AnalyticsOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $country LIMIT $lim;'
                q_res = cluster.analytics_query(q_str,
                                                AnalyticsOptions(named_parameters={'country': 'United%', 'lim':2}))
                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Retrieve metadata and/or metrics from analytics query::

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $country LIMIT $lim;'
                q_res = cluster.analytics_query(q_str)
                async for row in q_res.rows():
                    print(f'Found row: {row}')

                print(f'Analytics query metadata: {q_res.metadata()}')
                print(f'Analytics query metrics: {q_res.metadata().metrics()}')

        """  # noqa: E501

        request_args = dict()
        num_workers = kwargs.pop('num_workers', None)
        if num_workers:
            request_args['num_workers'] = num_workers

        query = AnalyticsQuery.create_query_object(
            statement, *options, **kwargs)
        return AnalyticsResult(AsyncAnalyticsRequest.generate_analytics_request(self.connection,
                                                                                self.loop,
                                                                                query.params,
                                                                                **request_args))

    def search_query(
        self,
        index,  # type: str
        query,  # type: SearchQuery
        *options,  # type: SearchOptions
        **kwargs
    ) -> SearchResult:
        """Executes an search query against the cluster.

        .. note::
            The search query is executed lazily in that it is executed once iteration over the
            :class:`~couchbase.result.SearchResult` begins.

        .. seealso::
            * :class:`~acouchbase.management.search.SearchIndexManager`: for how to manage search indexes.
            * :meth:`~acouchbase.scope.AsyncScope.search_query`: for how to execute scope-level search queries

        Args:
            index (str): Name of the search query to use.
            query (:class:`~couchbase.search.SearchQuery`): Type of search query to perform.
            options (:class:`~couchbase.options.SearchOptions`): Optional parameters for the search query
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.SearchOptions`

        Returns:
            :class:`~couchbase.result.SearchResult`: An instance of a
            :class:`~couchbase.result.SearchResult` which provides access to iterate over the search
            query results and access metadata and metrics about the search query.

        Examples:

            .. note::
                Be sure to create a search index prior to executing search queries.  Also, if an application
                desires to utilize search row locations, highlighting, etc. make sure the search index is
                setup appropriately.  See :search_create_idx:`Creating Indexes <>` in Couchbase Server docs.

            Simple search query::

                import couchbase.search as search
                from couchbase.options import SearchOptions

                # ... other code ...

                query = search.TermQuery('home')
                q_res = cluster.search_query('travel-sample-index',
                                            query,
                                            SearchOptions(limit=10))

                async for row in q_res.rows():
                    print(f'Found row: {row}')


            Simple search query with facets::

                import couchbase.search as search
                from couchbase.options import SearchOptions

                # ... other code ...

                facet_name = 'activity'
                facet = search.TermFacet('activity')
                query = search.TermQuery('home')
                q_res = cluster.search_query('travel-sample-index',
                                            query,
                                            SearchOptions(limit=10, facets={facet_name: facet}))

                async for row in q_res.rows():
                    print(f'Found row: {row}')

                print(f'facets: {q_res.facets()}')


            Simple search query with fields and locations::

                import couchbase.search as search
                from couchbase.options import SearchOptions

                # ... other code ...

                search_fields = ['name', 'activity']
                query = search.TermQuery('home')
                q_res = cluster.search_query('travel-sample-index',
                                            query,
                                            SearchOptions(limit=10,
                                                        include_locations=True,
                                                        fields=search_fields))

                async for row in q_res.rows():
                    print(f'Found row: {row}')
                    print(f'Fields: {row.fields}')
                    print(f'Locations: {row.locations}')

        """
        request_args = dict()
        num_workers = kwargs.pop('num_workers', None)
        if num_workers:
            request_args['num_workers'] = num_workers
        query = SearchQueryBuilder.create_search_query_object(
            index, query, *options, **kwargs
        )
        return SearchResult(AsyncSearchRequest.generate_search_request(self.connection,
                                                                       self.loop,
                                                                       query.as_encodable(),
                                                                       **request_args))

    def buckets(self) -> BucketManager:
        """
        Get a :class:`~acouchbase.management.buckets.BucketManager` which can be used to manage the buckets
        of this cluster.

        Returns:
            :class:`~acouchbase.management.buckets.BucketManager`: A :class:`~acouchbase.management.buckets.BucketManager` instance.
        """  # noqa: E501
        # TODO:  AlreadyShutdownException?
        return BucketManager(self.connection, self.loop)

    def users(self) -> UserManager:
        """
        Get a :class:`~acouchbase.management.users.UserManager` which can be used to manage the users
        of this cluster.

        Returns:
            :class:`~acouchbase.management.users.UserManager`: A :class:`~couchbase.management.users.UserManager` instance.
        """  # noqa: E501
        # TODO:  AlreadyShutdownException?
        return UserManager(self.connection, self.loop)

    def query_indexes(self) -> QueryIndexManager:
        """
        Get a :class:`~acouchbase.management.queries.QueryIndexManager` which can be used to manage the query
        indexes of this cluster.

        Returns:
            :class:`~acouchbase.management.queries.QueryIndexManager`: A :class:`~acouchbase.management.queries.QueryIndexManager` instance.
        """  # noqa: E501
        # TODO:  AlreadyShutdownException?
        return QueryIndexManager(self.connection, self.loop)

    def analytics_indexes(self) -> AnalyticsIndexManager:
        """
        Get a :class:`~acouchbase.management.analytics.AnalyticsIndexManager` which can be used to manage the analytics
        dataverses, dataset, indexes and links of this cluster.

        Returns:
            :class:`~acouchbase.management.analytics.AnalyticsIndexManager`: An :class:`~acouchbase.management.analytics.AnalyticsIndexManager` instance.
        """  # noqa: E501
        # TODO:  AlreadyShutdownException?
        return AnalyticsIndexManager(self.connection, self.loop)

    def search_indexes(self) -> SearchIndexManager:
        """
        Get a :class:`~acouchbase.management.search.SearchIndexManager` which can be used to manage the search
        indexes of this cluster.

        Returns:
            :class:`~acouchbase.management.search.SearchIndexManager`: A :class:`~acouchbase.management.search.SearchIndexManager` instance.

        """  # noqa: E501
        # TODO:  AlreadyShutdownException?
        return SearchIndexManager(self.connection, self.loop)

    def eventing_functions(self) -> EventingFunctionManager:
        """
        Get a :class:`~acouchbase.management.eventing.EventingFunctionManager` which can be used to manage the
        eventing functions of this cluster.

        .. note::
            Eventing function management is an **uncommitted** API that is unlikely to change,
            but may still change as final consensus on its behavior has not yet been reached.

        Returns:
            :class:`~acouchbase.management.eventing.EventingFunctionManager`: An :class:`~acouchbase.management.eventing.EventingFunctionManager` instance.

        """  # noqa: E501
        # TODO:  AlreadyShutdownException?
        return EventingFunctionManager(self.connection, self.loop)

    @staticmethod
    async def connect(connstr,  # type: str
                      *options,  # type: ClusterOptions
                      **kwargs,  # type: Dict[str, Any]
                      ) -> AsyncCluster:
        """Create a Couchbase Cluster and connect

        Args:
            connstr (str):
                The connection string to use for connecting to the cluster.
                This is a URI-like string allowing specifying multiple hosts.

                The format of the connection string is the *scheme*
                (``couchbase`` for normal connections, ``couchbases`` for
                SSL enabled connections); a list of one or more *hostnames*
                delimited by commas
            options (:class:`~couchbase.options.ClusterOptions`): Global options to set for the cluster.
                Some operations allow the global options to be overriden by passing in options to the
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                overrride provided :class:`~couchbase.options.ClusterOptions`

        Returns:
            :class:`.AsyncCluster`: If successful, a connect Couchbase Cluster instance.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If no :class:`~couchbase.auth.Authenticator`
                is provided.  Also raised if an invalid `ClusterOption` is provided.
            :class:`~couchbase.exceptions.AuthenticationException`: If provided :class:`~couchbase.auth.Authenticator`
                has incorrect credentials.


        Examples:
            Initialize cluster using default options::

                from acouchbase.cluster import Cluster
                from couchbase.auth import PasswordAuthenticator
                from couchbase.options import ClusterOptions

                auth = PasswordAuthenticator('username', 'password')
                cluster = await Cluster.connect('couchbase://localhost', ClusterOptions(auth))

            Connect using SSL::

                from acouchbase.cluster import Cluster
                from couchbase.auth import PasswordAuthenticator
                from couchbase.options import ClusterOptions

                auth = PasswordAuthenticator('username', 'password', cert_path='/path/to/cert')
                cluster = await Cluster.connect('couchbases://localhost', ClusterOptions(auth))

            Initialize cluster using with global timeout options::

                from datetime import timedelta

                from acouchbase.cluster import Cluster
                from couchbase.auth import PasswordAuthenticator
                from couchbase.options import ClusterOptions, ClusterTimeoutOptions

                auth = PasswordAuthenticator('username', 'password')
                timeout_opts = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=10),
                                                    query_timeout=timedelta(seconds=120))
                cluster = await Cluster.connect('couchbase://localhost',
                                                ClusterOptions(auth, timeout_options=timeout_opts))

        """
        cluster = AsyncCluster(connstr, *options, **kwargs)
        await cluster.on_connect()
        return cluster


Cluster = AsyncCluster
