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

from typing import (TYPE_CHECKING,
                    Any,
                    Awaitable,
                    Dict)

from acouchbase.collection import Collection
from acouchbase.logic import AsyncWrapper
from acouchbase.management.collections import CollectionManager
from acouchbase.management.views import ViewIndexManager
from acouchbase.scope import Scope
from acouchbase.views import AsyncViewRequest, ViewQuery
from couchbase.logic.bucket import BucketLogic
from couchbase.result import PingResult, ViewResult

if TYPE_CHECKING:
    from acouchbase.cluster import Cluster
    from couchbase.options import PingOptions, ViewOptions


class AsyncBucket(BucketLogic):
    """Create a Couchbase Bucket instance.

    Exposes the operations which are available to be performed against a bucket. Namely the ability to
    access to Collections as well as performing management operations against the bucket.

    Args:
        cluster (:class:`~acouchbase.cluster.Cluster`): A :class:`~acouchbase.cluster.Cluster` instance.
        bucket_name (str): Name of the bucket.

    Raises:
        :class:`~couchbase.exceptions.BucketNotFoundException`: If provided `bucket_name` cannot be found.

    """

    def __init__(self, cluster,  # type: Cluster
                 bucket_name  # type: str
                 ):
        super().__init__(cluster, bucket_name)
        self._close_ftr = None
        self._connect_ftr = self._open_bucket()

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._cluster.loop

    # def _connect_bucket(self):
    #     """
    #     **INTERNAL**
    #     Used w/in wrappers if a collection op is called when not connected
    #     """
    #     if self._connect_ftr is not None:
    #         return

    #     if not self._cluster.connected:
    #         self._connect_ftr = self.loop.create_future()
    #         ft = self._cluster.on_connect()
    #         ft.add_done_callback(partial(AsyncWrapper.chain_connect_callbacks, self))
    #     else:
    #         self._connect_ftr = super()._open_or_close_bucket(open_bucket=True)

    @AsyncWrapper.inject_bucket_open_callbacks()
    def _open_bucket(self, **kwargs) -> Awaitable:
        super()._open_or_close_bucket(open_bucket=True, **kwargs)

    @AsyncWrapper.inject_close_callbacks()
    def _close_bucket(self, **kwargs):
        super()._open_or_close_bucket(open_bucket=False, **kwargs)

    def on_connect(self) -> Awaitable:
        """Returns an awaitable future that indicates connecting to the Couchbase bucket has completed.

        Returns:
            Awaitable: An empty future.  If a result is provided, connecting to the Couchbase bucket is complete.
                Otherwise an exception is raised.

        Raises:
            :class:`~couchbase.exceptions.UnAmbiguousTimeoutException`: If an error occured while trying to connect.
        """
        if not (self._connect_ftr or self.connected):
            self._connect_ftr = self._open_bucket()
            self._close_ftr = None

        return self._connect_ftr

    async def close(self) -> None:
        """Shuts down this bucket instance. Cleaning up all resources associated with it.

        .. warning::
            Use of this method is almost *always* unnecessary.  Bucket resources should be cleaned
            up once the bucket instance falls out of scope.  However, in some applications tuning resources
            is necessary and in those types of applications, this method might be beneficial.

        """
        if self.connected and not self._close_ftr:
            self._close_ftr = self._close_bucket()
            self._connect_ftr = None

        await self._close_ftr
        super()._destroy_connection()

    def default_scope(self
                      ) -> Scope:
        """Creates a :class:`~acouchbase.scope.Scope` instance of the default scope.

        Returns:
            :class:`~acouchbase.scope.Scope`: A :class:`~acouchbase.scope.Scope` instance of the default scope.

        """
        return self.scope(Scope.default_name())

    def scope(self, name  # type: str
              ) -> Scope:
        """Creates a :class:`~acouchbase.scope.Scope` instance of the specified scope.

        Args:
            name (str): Name of the scope to reference.

        Returns:
            :class:`~acouchbase.scope.Scope`: A :class:`~couchbase.scope.Scope` instance of the specified scope.

        """
        return Scope(self, name)

    def collection(self, collection_name):
        """Creates a :class:`~acouchbase.collection.Collection` instance of the specified collection.

        Args:
            collection_name (str): Name of the collection to reference.

        Returns:
            :class:`~acouchbase.collection.Collection`: A :class:`~acouchbase.collection.Collection` instance of the specified collection.

        """  # noqa: E501
        scope = self.default_scope()
        return scope.collection(collection_name)

    def default_collection(self):
        """Creates a :class:`~acouchbase.collection.Collection` instance of the default collection.

        Returns:
            :class:`~acouchbase.collection.Collection`: A :class:`~acouchbase.collection.Collection` instance of the default collection.
        """  # noqa: E501
        scope = self.default_scope()
        return scope.collection(Collection.default_name())

    @AsyncWrapper.inject_cluster_callbacks(PingResult)
    def ping(self,
             *opts,  # type: PingOptions
             **kwargs  # type: Dict[str, Any]
             ) -> Awaitable[PingResult]:
        """Performs a ping operation against the bucket.

        The ping operation pings the services which are specified
        (or all services if none are specified). Returns a report which describes the outcome of
        the ping operations which were performed.

        Args:
            opts (:class:`~couchbase.options.PingOptions`): Optional parameters for this operation.

        Returns:
            Awaitable[:class:`~couchbase.result.PingResult`]: A report which describes the outcome of the ping
            operations which were performed.

        """
        return super().ping(*opts, **kwargs)

    def view_query(self,
                   design_doc,      # type: str
                   view_name,       # type: str
                   *view_options,   # type: ViewOptions
                   **kwargs         # type: Dict[str, Any]
                   ) -> ViewResult:
        """Executes a View query against the bucket.

        .. note::

            The query is executed lazily in that it is executed once iteration over the
            :class:`~.result.ViewResult` begins.

        .. seealso::
            * :class:`~.management.ViewIndexManager`: for how to manage query indexes

        Args:
            design_doc (str): The name of the design document containing the view to execute.
            view_name (str): The name of the view to execute.
            view_options (:class:`~.options.ViewOptions`): Optional parameters for the view query operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~.options.ViewOptions`

        Returns:
            :class:`~.result.ViewResult`: An instance of a :class:`~.result.ViewResult` which
            provides access to iterate over the query results and access metadata about the query.

        Examples:
            Simple view query::

                from couchbase.management.views import DesignDocumentNamespace

                # ... other code ...

                view_result = bucket.view_query('ddoc-name',
                                                'view-name',
                                                limit=10,
                                                namespace=DesignDocumentNamespace.DEVELOPMENT)

                async for row in view_result.rows():
                    print(f'Found row: {row}')

        """
        request_args = dict(default_serialize=self.default_serializer,
                            streaming_timeout=self.streaming_timeouts.get('view_timeout', None))
        num_workers = kwargs.pop('num_workers', None)
        if num_workers:
            request_args['num_workers'] = num_workers

        query = ViewQuery.create_view_query_object(self.name, design_doc, view_name, *view_options, **kwargs)
        return ViewResult(AsyncViewRequest.generate_view_request(self.connection,
                                                                 self.loop,
                                                                 query.as_encodable(),
                                                                 **request_args))

    def collections(self) -> CollectionManager:
        """
        Get a :class:`~acouchbase.management.collections.CollectionManager` which can be used to manage the scopes and collections
        of this bucket.

        Returns:
            :class:`~acouchbase.management.collections.CollectionManager`: A :class:`~couchbase.management.collections.CollectionManager` instance.
        """  # noqa: E501
        return CollectionManager(self.connection, self.loop, self.name)

    def view_indexes(self) -> ViewIndexManager:
        """
        Get a :class:`~acouchbase.management.views.ViewIndexManager` which can be used to manage the view design documents
        and views of this bucket.

        Returns:
            :class:`~acouchbase.management.views.ViewIndexManager`: A :class:`~couchbase.management.views.ViewIndexManager` instance.
        """  # noqa: E501
        return ViewIndexManager(self.connection, self.loop, self.name)


Bucket = AsyncBucket
