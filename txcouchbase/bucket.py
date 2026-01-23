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

from typing import (TYPE_CHECKING,
                    Any,
                    Dict)

from twisted.internet.defer import Deferred

from couchbase.result import PingResult, ViewResult
from txcouchbase.collection import Collection
from txcouchbase.logic.bucket_impl import TxBucketImpl
from txcouchbase.management.collections import CollectionManager
from txcouchbase.scope import Scope

if TYPE_CHECKING:
    from couchbase.options import PingOptions, ViewOptions
    from txcouchbase.cluster import TxCluster


class Bucket:

    def __init__(self, cluster: TxCluster, bucket_name: str) -> None:
        self._impl = TxBucketImpl(bucket_name, cluster)

    @property
    def name(self) -> str:
        return self._impl.bucket_name

    def on_connect(self) -> Deferred[None]:
        return self._impl.wait_until_bucket_connected_deferred()

    def close(self) -> Deferred:
        return self._impl.close_bucket_deferred()

    def default_scope(self) -> Scope:
        return self.scope(Scope.default_name())

    def scope(self, name: str) -> Scope:
        return Scope(self, name)

    def collection(self, collection_name: str) -> Collection:
        scope = self.default_scope()
        return scope.collection(collection_name)

    def default_collection(self) -> Collection:
        scope = self.default_scope()
        return scope.collection(Collection.default_name())

    def ping(self,
             *opts,  # type: PingOptions
             **kwargs  # type: Dict[str, Any]
             ) -> Deferred[PingResult]:
        req = self._impl.request_builder.build_ping_request(*opts, **kwargs)
        return self._impl.ping_deferred(req)

    def view_query(self,
                   design_doc,      # type: str
                   view_name,       # type: str
                   *view_options,   # type: ViewOptions
                   **kwargs
                   ) -> Deferred[ViewResult]:
        """Executes a View query against the bucket.

        .. deprecated:: 4.6.0

            Views are deprecated in Couchbase Server 7.0+, and will be removed from a future server version.
            Views are not compatible with the Magma storage engine. Instead of views, use indexes and queries using the
            Index Service (GSI) and the Query Service (SQL++).

        """
        req = self._impl.request_builder.build_view_query_request(design_doc, view_name, *view_options, **kwargs)
        return self._impl.view_query_deferred(req)

    def collections(self) -> CollectionManager:
        """
        Get the CollectionManager.

        :return: the :class:`.management.collections.CollectionManager` for this bucket.
        """
        return CollectionManager(self._impl._client_adapter, self.name)


TxBucket = Bucket
