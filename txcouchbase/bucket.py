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
                    Dict)

from twisted.internet.defer import Deferred

from couchbase.logic.bucket import BucketLogic
from couchbase.logic.views import ViewQuery
from couchbase.result import PingResult, ViewResult
from txcouchbase.collection import Collection
from txcouchbase.logic import TxWrapper
from txcouchbase.management.collections import CollectionManager
from txcouchbase.scope import Scope
from txcouchbase.views import ViewRequest

if TYPE_CHECKING:
    from couchbase.options import PingOptions, ViewOptions


class Bucket(BucketLogic):

    def __init__(self, cluster, bucket_name):
        super().__init__(cluster, bucket_name)
        self._close_d = None
        self._connect_d = self._open_bucket()

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._cluster.loop

    # def _connect_bucket(self):
    #     """
    #     **INTERNAL**
    #     """

    #     def cb(_, self):
    #         self._connection = self._cluster.connection
    #         return Deferred.fromFuture(super(Bucket, self)._open_or_close_bucket(open_bucket=True))

    #     if not self._cluster.connected:
    #         d = self._cluster.on_connect()
    #         d.addCallback(cb, self)
    #         self._connect_d = d
    #     else:
    #         self._connect_d = Deferred.fromFuture(super()._open_or_close_bucket(open_bucket=True))

    @TxWrapper.inject_bucket_open_callbacks()
    def _open_bucket(self, **kwargs) -> Deferred:
        super()._open_or_close_bucket(open_bucket=True, **kwargs)

    @TxWrapper.inject_close_callbacks()
    def _close_bucket(self, **kwargs) -> Deferred:
        super()._open_or_close_bucket(open_bucket=False, **kwargs)

    def on_connect(self) -> Deferred:
        # only open if the connect deferred doesn't exist and we are not connected
        if not self._connect_d and not self.connected:
            self._connect_d = self._open_bucket()
            self._close_d = None

        return self._connect_d

    def close(self) -> Deferred:
        # only close if we are connected
        if self.connected and not self._close_d:
            self._close_d = self._close_bucket()
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

    def default_scope(self
                      ) -> Scope:
        return self.scope(Scope.default_name())

    def scope(self, name  # type: str
              ) -> Scope:
        return Scope(self, name)

    def collection(self, collection_name):
        scope = self.default_scope()
        return scope.collection(collection_name)

    def default_collection(self):
        scope = self.default_scope()
        return scope.collection(Collection.default_name())

    @TxWrapper.inject_cluster_callbacks(PingResult)
    def ping(self,
             *opts,  # type: PingOptions
             **kwargs  # type: Dict[str, Any]
             ) -> Deferred[PingResult]:
        super().ping(*opts, **kwargs)

    def view_query(self,
                   design_doc,      # type: str
                   view_name,       # type: str
                   *view_options,   # type: ViewOptions
                   **kwargs
                   ) -> Deferred[ViewResult]:

        query = ViewQuery.create_view_query_object(
            self.name, design_doc, view_name, *view_options, **kwargs
        )
        request = ViewRequest.generate_view_request(self.connection,
                                                    self.loop,
                                                    query.as_encodable(),
                                                    default_serializer=self.default_serializer)

        d = Deferred()

        def _on_ok(_):
            d.callback(ViewResult(request))

        def _on_err(exc):
            d.errback(exc)

        query_d = request.execute_view_query()
        query_d.addCallback(_on_ok)
        query_d.addErrback(_on_err)
        return d

    def collections(self) -> CollectionManager:
        """
        Get the CollectionManager.

        :return: the :class:`.management.collections.CollectionManager` for this bucket.
        """
        return CollectionManager(self.connection, self.loop, self.name)


TxBucket = Bucket
