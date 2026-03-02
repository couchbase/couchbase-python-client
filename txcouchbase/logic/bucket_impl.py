#  Copyright 2016-2023. Couchbase, Inc.
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
from typing import TYPE_CHECKING

from twisted.internet.defer import Deferred

from acouchbase.logic.bucket_impl import AsyncBucketImpl
from couchbase.result import PingResult, ViewResult
from txcouchbase.views import ViewRequest

if TYPE_CHECKING:
    from couchbase.logic.bucket_types import PingRequest, ViewQueryRequest
    from txcouchbase.cluster import TxCluster


class TxBucketImpl(AsyncBucketImpl):

    def __init__(self, bucket_name: str, cluster: TxCluster) -> None:
        super().__init__(bucket_name, cluster)

    def close_bucket_deferred(self) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().close_bucket()
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def ping_deferred(self, req: PingRequest) -> Deferred[PingResult]:
        """**INTERNAL**"""
        coro = super().ping(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def view_query_deferred(self, req: ViewQueryRequest) -> ViewResult:
        """**INTERNAL**"""
        if not self.connected or not self.bucket_connected:
            raise RuntimeError('Cannot attempt to execute a view query to establishing a connection.')
        # If the view_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the view_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's view_timeout (set here). If the cluster
        # also does not specify a view_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::view_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._cluster_settings.streaming_timeouts.get('view_timeout', None)
        q_req = ViewRequest.generate_view_request(self.connection,
                                                  self.loop,
                                                  req.view_query.as_encodable(),
                                                  default_serializer=self.default_serializer,
                                                  streaming_timeout=streaming_timeout,
                                                  obs_handler=req.obs_handler)
        d = Deferred()

        def _on_ok(_):
            d.callback(ViewResult(q_req))

        def _on_err(exc):
            d.errback(exc)

        query_d = q_req.execute_view_query()
        query_d.addCallback(_on_ok)
        query_d.addErrback(_on_err)
        return d

    def wait_until_bucket_connected_deferred(self) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().wait_until_bucket_connected()
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d
