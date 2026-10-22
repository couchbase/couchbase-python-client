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

__all__ = [
    'Cluster',
    'TxCluster',
]

from asyncio import AbstractEventLoop
from typing import (TYPE_CHECKING,
                    Any,
                    Union)

from twisted.internet.defer import Deferred

from couchbase.common._registry import TX_CLUSTER, resolve
from couchbase.common.protocol import CouchbaseProtocol
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
from txcouchbase.management.analytics import AnalyticsIndexManager
from txcouchbase.management.buckets import BucketManager
from txcouchbase.management.queries import QueryIndexManager
from txcouchbase.management.search import SearchIndexManager
from txcouchbase.management.users import UserManager

if TYPE_CHECKING:
    from datetime import timedelta

    from couchbase.common._interfaces._cluster import TxClusterInterface
    from couchbase.common._utils import JSONType
    from couchbase.options import (AnalyticsOptions,
                                   ClusterOptions,
                                   QueryOptions,
                                   SearchOptions)
    from couchbase.search import SearchQuery, SearchRequest


class Cluster:

    def __init__(self, connstr: str, *options: ClusterOptions, **kwargs: Any) -> None:
        impl = resolve(TX_CLUSTER, CouchbaseProtocol.from_connstr(connstr))
        self._impl: TxClusterInterface = impl(connstr, *options, **kwargs)

    @property
    def loop(self) -> AbstractEventLoop:
        """
        **INTERNAL**
        """
        return self._impl.loop

    def on_connect(self) -> Deferred[None]:
        return self._impl.wait_until_connected_deferred()

    def close(self) -> Deferred[None]:
        """Shuts down this cluster instance. Cleaning up all resources associated with it.

        .. warning::
            Use of this method is almost *always* unnecessary.  Cluster resources should be cleaned
            up once the cluster instance falls out of scope.  However, in some applications tuning resources
            is necessary and in those types of applications, this method might be beneficial.
        """
        return self._impl.close_connection_deferred()

    def bucket(self, bucket_name):
        """Creates a Bucket instance to a specific bucket.

        .. seealso::
            :class:`.bucket.Bucket`

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
        """Retrieve the Couchbase cluster information

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

    def ping(self, opts: PingOptions | None = None, **kwargs: Any) -> Deferred[PingResult]:
        """Performs a ping operation against the cluster.

        The ping operation pings the services which are specified
        (or all services if none are specified). Returns a report which describes the outcome of
        the ping operations which were performed.

        Args:
            opts (:class:`~couchbase.options.PingOptions`): Optional parameters for this operation.

        Returns:
            Deferred[:class:`~couchbase.result.PingResult`]: A report which describes the outcome of the ping operations
            which were performed.
        """
        return self._impl.ping_deferred(opts, **kwargs)

    def diagnostics(self, opts: DiagnosticsOptions | None = None, **kwargs: Any) -> Deferred[DiagnosticsResult]:
        """Performs a diagnostic operation against the cluster.

        The diagnostic operations returns a report about the current active connections with the cluster.
        Includes information about remote and local addresses, last activity, and other diagnostics information.

        Args:
            opts (:class:`~couchbase.options.DiagnosticsOptions`): Optional parameters for this operation.

        Returns:
            Deferred[:class:`~couchbase.result.DiagnosticsResult`]: A report which describes current active connections
            with the cluster.
        """
        return self._impl.diagnostics_deferred(opts, **kwargs)

    def wait_until_ready(self,
                         timeout: timedelta,
                         opts: WaitUntilReadyOptions | None = None,
                         **kwargs: Any) -> Deferred[None]:
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
            **kwargs (dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.WaitUntilReadyOptions`

        Raises:
            :class:`~couchbase.exceptions.UnAmbiguousTimeoutException`: If the specified timeout is reached prior to
                the cluster being ready for use.
        """
        return self._impl.wait_until_ready_deferred(timeout, opts, **kwargs)

    def query(
        self,
        statement,  # type: str
        *options,  # type: Union[QueryOptions, JSONType]
        **kwargs  # type: Any
    ) -> Deferred[QueryResult]:
        op_type = StreamingOperationType.Query
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        req = self._impl.request_builder.build_query_request(statement, obs_handler, *options, **kwargs)
        return self._impl.query_deferred(req)

    def analytics_query(
        self,
        statement,  # type: str
        *options,  # type: Union[AnalyticsOptions, JSONType]
        **kwargs  # type: Any
    ) -> Deferred[AnalyticsResult]:
        op_type = StreamingOperationType.AnalyticsQuery
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        req = self._impl.request_builder.build_analytics_query_request(statement, obs_handler, *options, **kwargs)
        return self._impl.analytics_query_deferred(req)

    def search_query(self,
                     index,  # type: str
                     query,  # type: SearchQuery
                     *options,  # type: SearchOptions
                     **kwargs  # type: Any
                     ) -> Deferred[SearchResult]:
        op_type = StreamingOperationType.SearchQuery
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        req = self._impl.request_builder.build_search_query_request(index, query, obs_handler, *options, **kwargs)
        return self._impl.search_deferred(req)

    def search(self,
               index,  # type: str
               request,  # type: Union[SearchRequest, SearchQuery]
               *options,  # type: SearchOptions
               **kwargs,  # type: Any
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
