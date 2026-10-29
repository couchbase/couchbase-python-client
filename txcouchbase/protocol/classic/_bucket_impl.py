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
from typing import TYPE_CHECKING, Any

from twisted.internet.defer import Deferred

from acouchbase.protocol.classic._bucket_impl import AsyncBucketImpl
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common.result import PingResult
from couchbase.protocol.classic._streaming import ViewStreamSpec
from couchbase.result import ViewResult
from txcouchbase.protocol.classic._deferred import defer
from txcouchbase.protocol.classic._management._collection_mgmt_impl import TxCollectionMgmtImpl
from txcouchbase.protocol.classic._management._view_index_mgmt_impl import TxViewIndexMgmtImpl
from txcouchbase.protocol.classic._scope_impl import TxScopeImpl
from txcouchbase.protocol.classic._streaming import submit_rows

if TYPE_CHECKING:
    from couchbase.common.options import PingOptions, ViewOptions
    from txcouchbase.protocol.classic._cluster_impl import TxClusterImpl


class TxBucketImpl(AsyncBucketImpl):

    def __init__(self, bucket_name: str, cluster_impl: TxClusterImpl) -> None:
        super().__init__(bucket_name, cluster_impl)

    def close_bucket_deferred(self) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().close_bucket()
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def scope(self, name: str) -> TxScopeImpl:
        """**INTERNAL**"""
        return TxScopeImpl(name, self)

    def ping_deferred(self, opts: PingOptions | None = None, **kwargs: Any) -> Deferred[PingResult]:
        return defer(self.loop, None, lambda: self._ping(self._request_builder.build_ping_request(opts, **kwargs)))

    def collection_mgmt(self) -> TxCollectionMgmtImpl:
        """**INTERNAL**"""
        return TxCollectionMgmtImpl(self._client_adapter, self._bucket_name, self.observability_instruments)

    def view_index_mgmt(self) -> TxViewIndexMgmtImpl:
        """**INTERNAL**"""
        return TxViewIndexMgmtImpl(self._client_adapter, self._bucket_name, self.observability_instruments)

    def view_query_deferred(self,
                            design_doc: str,
                            view_name: str,
                            view_options: ViewOptions | None = None,
                            **kwargs: Any) -> Deferred[ViewResult]:
        obs_handler = ObservableRequestHandler(Op.ViewQuery, self.observability_instruments)
        req = self._request_builder.build_view_query_request(design_doc,
                                                             view_name,
                                                             obs_handler,
                                                             view_options,
                                                             **kwargs)
        if not self.connected or not self.bucket_connected:
            raise RuntimeError('Cannot attempt to execute a view query to establishing a connection.')
        # If the view_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the view_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's view_timeout (set here). If the cluster
        # also does not specify a view_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::view_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._streaming_timeouts.get('view_timeout', None)
        rows, responded = submit_rows(self.connection,
                                      self.loop,
                                      ViewStreamSpec(req.view_query.as_encodable(), self.default_serializer),
                                      obs_handler=req.obs_handler,
                                      streaming_timeout=streaming_timeout)
        return responded.addCallback(lambda _: ViewResult(rows))

    def wait_until_bucket_connected_deferred(self) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().wait_until_bucket_connected()
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d
