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

from __future__ import annotations

__all__ = [
    'Bucket',
]

from typing import TYPE_CHECKING

from couchbase.collection import Collection
from couchbase.common import options
from couchbase.common._supportability import Supportability
from couchbase.management.collections import CollectionManager
from couchbase.management.views import ViewIndexManager
from couchbase.result import PingResult, ViewResult
from couchbase.scope import Scope

if TYPE_CHECKING:
    from typing_extensions import Unpack

    from couchbase.cluster import Cluster
    from couchbase.common._interfaces._bucket import BucketInterface


class Bucket:
    """Create a Couchbase Bucket instance.

    Exposes the operations which are available to be performed against a bucket. Namely the ability to
    access to Collections as well as performing management operations against the bucket.

    Args:
        cluster (:class:`~couchbase.cluster.Cluster`): A :class:`~couchbase.cluster.Cluster` instance.
        bucket_name (str): Name of the bucket.

    Raises:
        :class:`~couchbase.exceptions.BucketNotFoundException`: If provided `bucket_name` cannot be found.

    """

    def __init__(self, cluster: Cluster, bucket_name: str) -> None:
        self._impl: BucketInterface = cluster._impl.bucket(bucket_name)

    @property
    def name(self) -> str:
        """The name of this bucket."""
        return self._impl.bucket_name

    def close(self) -> None:
        """Shuts down this bucket instance. Cleaning up all resources associated with it.

        .. warning::
            Use of this method is almost *always* unnecessary.  Bucket resources should be cleaned
            up once the bucket instance falls out of scope.  However, in some applications tuning resources
            is necessary and in those types of applications, this method might be beneficial.

        """
        self._impl.close_bucket()

    def default_scope(self) -> Scope:
        """Creates a :class:`~couchbase.scope.Scope` instance of the default scope.

        Returns:
            :class:`~couchbase.scope.Scope`: A :class:`~couchbase.scope.Scope` instance of the default scope.

        """
        return self.scope(Scope.default_name())

    def scope(self, name: str) -> Scope:
        """Creates a :class:`~couchbase.scope.Scope` instance of the specified scope.

        Args:
            name (str): Name of the scope to reference.

        Returns:
            :class:`~couchbase.scope.Scope`: A :class:`~couchbase.scope.Scope` instance of the specified scope.

        """
        return Scope(self, name)

    def collection(self, collection_name: str) -> Collection:
        """Creates a :class:`~couchbase.collection.Collection` instance of the specified collection.

        Args:
            collection_name (str): Name of the collection to reference.

        Returns:
            :class:`~couchbase.collection.Collection`: A :class:`~couchbase.collection.Collection` instance of the
            specified collection.

        """
        scope = self.default_scope()
        return scope.collection(collection_name)

    def default_collection(self) -> Collection:
        """Creates a :class:`~couchbase.collection.Collection` instance of the default collection.

        Returns:
            :class:`~couchbase.collection.Collection`: A :class:`~couchbase.collection.Collection` instance of the
            default collection.
        """
        scope = self.default_scope()
        return scope.collection(Collection.default_name())

    def ping(self, opts: options.PingOptions | None = None, **kwargs: Unpack[options.PingOptionsKwargs]) -> PingResult:
        """Performs a ping operation against the bucket.

        The ping operation pings the services which are specified
        (or all services if none are specified). Returns a report which describes the outcome of
        the ping operations which were performed.

        Args:
            opts (:class:`~couchbase.options.PingOptions`): Optional parameters for this operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.PingOptions`

        Returns:
            :class:`~couchbase.result.PingResult`: A report which describes the outcome of the ping operations
            which were performed.

        """
        return self._impl.ping(opts, **kwargs)

    def view_query(self,
                   design_doc: str,
                   view_name: str,
                   view_options: options.ViewOptions | None = None,
                   **kwargs: Unpack[options.ViewOptionsKwargs]) -> ViewResult:
        """Executes a View query against the bucket.

        .. deprecated:: 4.6.0

            Views are deprecated in Couchbase Server 7.0+, and will be removed from a future server version.
            Views are not compatible with the Magma storage engine. Instead of views, use indexes and queries using the
            Index Service (GSI) and the Query Service (SQL++).

        .. note::

            The query is executed lazily in that it is executed once iteration over the
            :class:`~.result.ViewResult` begins.

        .. seealso::
            * :class:`~couchbase.management.views.ViewIndexManager`: for how to manage design documents

        Args:
            design_doc (str): The name of the design document containing the view to execute.
            view_name (str): The name of the view to execute.
            view_options (:class:`~.options.ViewOptions`): Optional parameters for the view query operation.
            **kwargs: keyword arguments that can be used in place or to
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

                for row in view_result.rows():
                    print(f'Found row: {row}')

        """
        return self._impl.view_query(design_doc, view_name, view_options, **kwargs)

    def collections(self) -> CollectionManager:
        """Get a manager for the scopes and collections of this bucket.

        Returns:
            :class:`~couchbase.management.collections.CollectionManager`: A
            :class:`~couchbase.management.collections.CollectionManager` instance.
        """
        return CollectionManager(self._impl.collection_mgmt())

    def view_indexes(self) -> ViewIndexManager:
        """Get a manager for the view design documents and views of this bucket.

        .. deprecated:: 4.6.0

            Views are deprecated in Couchbase Server 7.0+, and will be removed from a future server version.
            Views are not compatible with the Magma storage engine. Instead of views, use indexes and queries using the
            Index Service (GSI) and the Query Service (SQL++).

        Returns:
            :class:`~couchbase.management.views.ViewIndexManager`: A
            :class:`~couchbase.management.views.ViewIndexManager` instance.
        """
        return ViewIndexManager(self._impl.view_index_mgmt())


if not TYPE_CHECKING:
    __getattr__ = Supportability.deprecated_paths(__name__)
