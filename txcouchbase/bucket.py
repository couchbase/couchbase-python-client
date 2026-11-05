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

__all__ = [
    'Bucket',
    'TxBucket',
]

from typing import TYPE_CHECKING

from twisted.internet.defer import Deferred

from couchbase.result import PingResult, ViewResult
from txcouchbase.collection import Collection
from txcouchbase.management.collections import CollectionManager
from txcouchbase.management.views import ViewIndexManager
from txcouchbase.scope import Scope

if TYPE_CHECKING:
    from typing_extensions import Unpack

    from couchbase.common._interfaces._bucket import TxBucketInterface
    from couchbase.common.options import PingOptionsKwargs, ViewOptionsKwargs
    from couchbase.options import PingOptions, ViewOptions
    from txcouchbase.cluster import TxCluster


class Bucket:
    """A bucket in a cluster: its scopes and collections, and the operations run against it.

    Get one from :meth:`~txcouchbase.cluster.Cluster.bucket`.

    Args:
        cluster (:class:`~txcouchbase.cluster.Cluster`): The cluster the bucket belongs to.
        bucket_name (str): The name of the bucket.
    """

    def __init__(self, cluster: TxCluster, bucket_name: str) -> None:
        self._impl: TxBucketInterface = cluster._impl.bucket(bucket_name)

    @property
    def name(self) -> str:
        """The name of this bucket."""
        return self._impl.bucket_name

    def on_connect(self) -> Deferred[None]:
        """Wait until the bucket is open.

        Returns:
            Deferred: Fires once the bucket is open, or fails if it cannot be opened.
        """
        return self._impl.wait_until_bucket_connected_deferred()

    def close(self) -> Deferred:
        """Shuts down this bucket instance. Cleaning up all resources associated with it.

        .. warning::
            Use of this method is almost *always* unnecessary.  Bucket resources should be cleaned
            up once the bucket instance falls out of scope.  However, in some applications tuning resources
            is necessary and in those types of applications, this method might be beneficial.
        """
        return self._impl.close_bucket_deferred()

    def default_scope(self) -> Scope:
        """Creates a :class:`~.scope.Scope` instance of the default scope.

        Returns:
            :class:`~.scope.Scope`: A :class:`~.scope.Scope` instance of the default scope.
        """
        return self.scope(Scope.default_name())

    def scope(self, name: str) -> Scope:
        """Creates a :class:`~txcouchbase.scope.Scope` instance of the specified scope.

        Args:
            name (str): Name of the scope to reference.

        Returns:
            :class:`~txcouchbase.scope.Scope`: A :class:`~txcouchbase.scope.Scope` instance of the specified scope.
        """
        return Scope(self, name)

    def collection(self, collection_name: str) -> Collection:
        """Creates a :class:`~txcouchbase.collection.Collection` instance of the specified collection.

        Args:
            collection_name (str): Name of the collection to reference.

        Returns:
            :class:`~txcouchbase.collection.Collection`: A :class:`~txcouchbase.collection.Collection` instance of the
            specified collection.
        """
        scope = self.default_scope()
        return scope.collection(collection_name)

    def default_collection(self) -> Collection:
        """Creates a :class:`~txcouchbase.collection.Collection` instance of the default collection.

        Returns:
            :class:`~txcouchbase.collection.Collection`: A :class:`~txcouchbase.collection.Collection` instance of the
            default collection.
        """
        scope = self.default_scope()
        return scope.collection(Collection.default_name())

    def ping(self, opts: PingOptions | None = None, **kwargs: Unpack[PingOptionsKwargs]) -> Deferred[PingResult]:
        """Performs a ping operation against the bucket.

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

    def view_query(self,
                   design_doc: str,
                   view_name: str,
                   view_options: ViewOptions | None = None,
                   **kwargs: Unpack[ViewOptionsKwargs]) -> Deferred[ViewResult]:
        """Executes a View query against the bucket.

        .. deprecated:: 4.6.0

            Views are deprecated in Couchbase Server 7.0+, and will be removed from a future server version.
            Views are not compatible with the Magma storage engine. Instead of views, use indexes and queries using the
            Index Service (GSI) and the Query Service (SQL++).

        .. note::

            The query is sent when this is called, and the Deferred fires once the core has buffered
            the response, so iterating the :class:`~couchbase.result.ViewResult` does not block the reactor.

        .. seealso::
            * :class:`~.management.ViewIndexManager`: for how to manage query indexes

        Args:
            design_doc (str): The name of the design document containing the view to execute.
            view_name (str): The name of the view to execute.
            view_options (:class:`~.options.ViewOptions`): Optional parameters for the view query operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~.options.ViewOptions`

        Returns:
            Deferred[:class:`~.result.ViewResult`]: An instance of a :class:`~.result.ViewResult` which
            provides access to iterate over the query results and access metadata about the query.
        """
        return self._impl.view_query_deferred(design_doc, view_name, view_options, **kwargs)

    def collections(self) -> CollectionManager:
        """Get a manager for the scopes and collections of this bucket.

        Returns:
            :class:`~txcouchbase.management.collections.CollectionManager`: A
            :class:`~txcouchbase.management.collections.CollectionManager` instance.
        """
        return CollectionManager(self._impl.collection_mgmt())

    def view_indexes(self) -> ViewIndexManager:
        """Get a manager for the view design documents and views of this bucket.

        .. deprecated:: 4.6.0

            Views are deprecated in Couchbase Server 7.0+, and will be removed from a future server version.
            Views are not compatible with the Magma storage engine. Instead of views, use indexes and queries using the
            Index Service (GSI) and the Query Service (SQL++).

        Returns:
            :class:`~txcouchbase.management.views.ViewIndexManager`: A
            :class:`~txcouchbase.management.views.ViewIndexManager` instance.
        """
        return ViewIndexManager(self._impl.view_index_mgmt())


TxBucket = Bucket
