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

# used to allow for unquoted (i.e. forward reference, Python >= 3.7, PEP563)
from __future__ import annotations

__all__ = [
    'Cluster',
    'TxCluster',
]

from asyncio import AbstractEventLoop
from typing import TYPE_CHECKING, Any

from twisted.internet.defer import Deferred

from couchbase.common._registry import TX_CLUSTER, resolve
from couchbase.common.protocol import CouchbaseProtocol
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
from txcouchbase.management.analytics import AnalyticsIndexManager
from txcouchbase.management.buckets import BucketManager
from txcouchbase.management.queries import QueryIndexManager
from txcouchbase.management.search import SearchIndexManager
from txcouchbase.management.users import UserManager

if TYPE_CHECKING:
    from datetime import timedelta

    from typing_extensions import Unpack

    from couchbase.common._interfaces._cluster import TxClusterInterface
    from couchbase.common._utils import JSONType
    from couchbase.common.options import (ClusterKwargs,
                                          DiagnosticsOptionsKwargs,
                                          PingOptionsKwargs,
                                          SearchOptionsKwargs,
                                          WaitUntilReadyKwargs)
    from couchbase.options import (AnalyticsOptions,
                                   ClusterOptions,
                                   QueryOptions,
                                   SearchOptions)
    from couchbase.search import SearchQuery, SearchRequest


class Cluster:
    """A connection to a Couchbase cluster, for use with Twisted.

    Its operations return a Deferred. Create it on a running reactor and wait on :meth:`on_connect` before the first
    operation.

    Args:
        connstr (str): The connection string for the cluster.
        *options (:class:`~couchbase.options.ClusterOptions`): Options for the cluster, including its authenticator.
        **kwargs: keyword arguments that can be used in place or to
            override provided :class:`~couchbase.options.ClusterOptions`
    """

    def __init__(self, connstr: str, *options: ClusterOptions, **kwargs: Unpack[ClusterKwargs]) -> None:
        impl = resolve(TX_CLUSTER, CouchbaseProtocol.from_connstr(connstr))
        self._impl: TxClusterInterface = impl(connstr, *options, **kwargs)

    @property
    def loop(self) -> AbstractEventLoop:
        """**INTERNAL** The event loop the cluster runs on."""
        return self._impl.loop

    def on_connect(self) -> Deferred[None]:
        """Wait until the cluster is connected.

        Returns:
            Deferred: Fires once the cluster is connected, or fails if it cannot connect.
        """
        return self._impl.wait_until_connected_deferred()

    def close(self) -> Deferred[None]:
        """Shuts down this cluster instance. Cleaning up all resources associated with it.

        .. warning::
            Use of this method is almost *always* unnecessary.  Cluster resources should be cleaned
            up once the cluster instance falls out of scope.  However, in some applications tuning resources
            is necessary and in those types of applications, this method might be beneficial.
        """
        return self._impl.close_connection_deferred()

    def bucket(self, bucket_name: str) -> Bucket:
        """Creates a Bucket instance to a specific bucket.

        .. seealso::
            :class:`~txcouchbase.bucket.Bucket`

        Args:
            bucket_name (str): Name of the bucket to reference

        Returns:
            :class:`~txcouchbase.bucket.Bucket`: A bucket instance

        The bucket opens on the reactor once the cluster is connected, so this call does not raise.  A bucket
        that cannot be opened, such as one that does not exist, fails the first operation on it, and the
        Deferred from :meth:`~txcouchbase.bucket.Bucket.on_connect`.
        """
        return Bucket(self, bucket_name)

    def cluster_info(self) -> Deferred[ClusterInfoResult]:
        """Retrieve the Couchbase cluster information.

        .. note::
            If using Couchbase Server version < 6.6, a bucket *must* be opened prior to calling
            `cluster.cluster_info()`.  If a bucket is not opened a
            :class:`~couchbase.exceptions.ServiceUnavailableException` will be raised.


        Returns:
            Deferred[:class:`~couchbase.result.ClusterInfoResult`]: Information about the connected cluster.

        Raises:
            :class:`~couchbase.exceptions.ServiceUnavailableException`: If called prior to connecting
                to a bucket if using server version < 6.6.
        """
        return self._impl.get_cluster_info_deferred()

    def ping(self, opts: PingOptions | None = None, **kwargs: Unpack[PingOptionsKwargs]) -> Deferred[PingResult]:
        """Performs a ping operation against the cluster.

        The ping operation pings the services which are specified
        (or all services if none are specified). Returns a report which describes the outcome of
        the ping operations which were performed.

        Args:
            opts (:class:`~couchbase.options.PingOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.PingOptions`

        Returns:
            Deferred[:class:`~couchbase.result.PingResult`]: A report which describes the outcome of the ping operations
            which were performed.
        """
        return self._impl.ping_deferred(opts, **kwargs)

    def diagnostics(self,
                    opts: DiagnosticsOptions | None = None,
                    **kwargs: Unpack[DiagnosticsOptionsKwargs]) -> Deferred[DiagnosticsResult]:
        """Performs a diagnostic operation against the cluster.

        The diagnostic operations returns a report about the current active connections with the cluster.
        Includes information about remote and local addresses, last activity, and other diagnostics information.

        Args:
            opts (:class:`~couchbase.options.DiagnosticsOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.DiagnosticsOptions`

        Returns:
            Deferred[:class:`~couchbase.result.DiagnosticsResult`]: A report which describes current active connections
            with the cluster.
        """
        return self._impl.diagnostics_deferred(opts, **kwargs)

    def wait_until_ready(self,
                         timeout: timedelta,
                         opts: WaitUntilReadyOptions | None = None,
                         **kwargs: Unpack[WaitUntilReadyKwargs]) -> Deferred[None]:
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
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.WaitUntilReadyOptions`

        Raises:
            :class:`~couchbase.exceptions.UnAmbiguousTimeoutException`: If the specified timeout is reached prior to
                the cluster being ready for use.
        """
        return self._impl.wait_until_ready_deferred(timeout, opts, **kwargs)

    def query(self, statement: str, *options: QueryOptions | JSONType, **kwargs: Any) -> Deferred[QueryResult]:
        """Executes a N1QL query against the cluster.

        .. note::
            The query is sent when this is called, and the Deferred fires once the core has buffered
            the response, so iterating the :class:`~couchbase.result.QueryResult` does not block the reactor.

        .. seealso::
            * :class:`~txcouchbase.management.queries.QueryIndexManager`: for how to manage query indexes
            * :meth:`~txcouchbase.scope.Scope.query`: For how to execute scope-level queries.

        Args:
            statement (str): The N1QL statement to execute.
            options (:class:`~couchbase.options.QueryOptions`): Optional parameters for the query operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.QueryOptions`.  A keyword that is not one of its
                options is sent as a named parameter; prefer ``QueryOptions(named_parameters=...)``, which
                cannot collide with an option name.

        Returns:
            Deferred[:class:`~couchbase.result.QueryResult`]: An instance of a :class:`~couchbase.result.QueryResult`
            which provides access to iterate over the query results and access metadata and metrics about the query.
        """
        return self._impl.query_deferred(statement, *options, **kwargs)

    def analytics_query(self,
                        statement: str,
                        *options: AnalyticsOptions | JSONType,
                        **kwargs: Any) -> Deferred[AnalyticsResult]:
        """Executes an analaytics query against the cluster.

        .. note::
            The analytics query is sent when this is called, and the Deferred fires once the core has buffered
            the response, so iterating the :class:`~couchbase.result.AnalyticsResult` does not block the reactor.

        .. seealso::

            * :class:`~txcouchbase.management.analytics.AnalyticsIndexManager`: for how to manage analytics dataverses,
              datasets, indexes and links.
            * :meth:`~txcouchbase.scope.Scope.analytics_query`: for how to execute scope-level analytics queries

        Args:
            statement (str): The analytics SQL++ statement to execute.
            options (:class:`~couchbase.options.AnalyticsOptions`): Optional parameters for the analytics query
                operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.AnalyticsOptions`.  A keyword that is not one of its
                options is sent as a named parameter; prefer ``AnalyticsOptions(named_parameters=...)``, which
                cannot collide with an option name.

        Returns:
            Deferred[:class:`~couchbase.result.AnalyticsResult`]: An instance of a
            :class:`~couchbase.result.AnalyticsResult` which provides access to iterate over the analytics
            query results and access metadata and metrics about the analytics query.
        """
        return self._impl.analytics_query_deferred(statement, *options, **kwargs)

    def search_query(self,
                     index: str,
                     query: SearchQuery,
                     options: SearchOptions | None = None,
                     **kwargs: Unpack[SearchOptionsKwargs]) -> Deferred[SearchResult]:
        """Executes an search query against the cluster.

        .. deprecated:: 4.1.11
            Use :meth:`~txcouchbase.cluster.Cluster.search` instead.

        .. note::
            The search query is sent when this is called, and the Deferred fires once the core has buffered
            the response, so iterating the :class:`~couchbase.result.SearchResult` does not block the reactor.

        .. seealso::
            * :class:`~txcouchbase.management.search.SearchIndexManager`: for how to manage search indexes.

        Args:
            index (str): Name of the search query to use.
            query (:class:`~couchbase.search.SearchQuery`): Type of search query to perform.
            options (:class:`~couchbase.options.SearchOptions`): Optional parameters for the search query
                operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.SearchOptions`

        Returns:
            Deferred[:class:`~couchbase.result.SearchResult`]: An instance of a
            :class:`~couchbase.result.SearchResult` which provides access to iterate over the search
            query results and access metadata and metrics about the search query.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If a
                :class:`~couchbase.search.SearchRequest` is provided.  Use
                :meth:`~txcouchbase.cluster.Cluster.search` for a search request.
        """
        return self._impl.search_query_deferred(index, query, options, **kwargs)

    def search(self,
               index: str,
               request: SearchRequest | SearchQuery,
               options: SearchOptions | None = None,
               **kwargs: Unpack[SearchOptionsKwargs]) -> Deferred[SearchResult]:
        """Executes an search against the cluster.

        .. note::
            The search is sent when this is called, and the Deferred fires once the core has buffered
            the response, so iterating the :class:`~couchbase.result.SearchResult` does not block the reactor.

        .. seealso::
            * :class:`~txcouchbase.management.search.SearchIndexManager`: for how to manage search indexes.
            * :meth:`~txcouchbase.scope.Scope.search`: for how to execute scope-level search

        Args:
            index (str): Name of the search index to use.
            request (:class:`~couchbase.search.SearchRequest` | :class:`~couchbase.search.SearchQuery`): Type of
                search request to perform.

                .. deprecated:: 4.7.0
                    Passing a :class:`~couchbase.search.SearchQuery` is deprecated; wrap it with
                    :meth:`~couchbase.search.SearchRequest.create` instead.
            options (:class:`~couchbase.options.SearchOptions`): Optional parameters for the search query operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.SearchOptions`

        Returns:
            Deferred[:class:`~couchbase.result.SearchResult`]: An instance of a
            :class:`~couchbase.result.SearchResult` which provides access to iterate over the search
            query results and access metadata and metrics about the search query.
        """
        return self._impl.search_deferred(index, request, options, **kwargs)

    def buckets(self) -> BucketManager:
        """Get a manager for the buckets of this cluster.

        Returns:
            :class:`~txcouchbase.management.buckets.BucketManager`: A
            :class:`~txcouchbase.management.buckets.BucketManager` instance.
        """
        return BucketManager(self._impl.bucket_mgmt())

    def users(self) -> UserManager:
        """Get a manager for the users of this cluster.

        Returns:
            :class:`~txcouchbase.management.users.UserManager`: A :class:`~txcouchbase.management.users.UserManager`
            instance.
        """
        return UserManager(self._impl.user_mgmt())

    def query_indexes(self) -> QueryIndexManager:
        """Get a manager for the query indexes of this cluster.

        Returns:
            :class:`~txcouchbase.management.queries.QueryIndexManager`: A
            :class:`~txcouchbase.management.queries.QueryIndexManager` instance.
        """
        return QueryIndexManager(self._impl.query_index_mgmt())

    def analytics_indexes(self) -> AnalyticsIndexManager:
        """Get a manager for the analytics dataverses, datasets, indexes and links of this cluster.

        Returns:
            :class:`~txcouchbase.management.analytics.AnalyticsIndexManager`: An
            :class:`~txcouchbase.management.analytics.AnalyticsIndexManager` instance.
        """
        return AnalyticsIndexManager(self._impl.analytics_mgmt())

    def search_indexes(self) -> SearchIndexManager:
        """Get a manager for the search indexes of this cluster.

        Returns:
            :class:`~txcouchbase.management.search.SearchIndexManager`: A
            :class:`~txcouchbase.management.search.SearchIndexManager` instance.
        """
        return SearchIndexManager(self._impl.search_index_mgmt())


TxCluster = Cluster
