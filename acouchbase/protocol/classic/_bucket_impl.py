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

from asyncio import AbstractEventLoop, Future
from typing import TYPE_CHECKING, Any

from acouchbase.protocol.classic._management._collection_mgmt_impl import AsyncCollectionMgmtImpl
from acouchbase.protocol.classic._management._view_index_mgmt_impl import AsyncViewIndexMgmtImpl
from acouchbase.protocol.classic._scope_impl import AsyncScopeImpl
from acouchbase.protocol.classic._streaming import ClassicAsyncStreamingExecutor
from acouchbase.result import AsyncViewResult
from couchbase.common._cluster_settings import ClusterSettings
from couchbase.common._observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common._streaming import AsyncRowIterator
from couchbase.common.result import PingResult
from couchbase.protocol.classic._bucket_req_builder import BucketRequestBuilder
from couchbase.protocol.classic._cluster_types import StreamingTimeouts
from couchbase.protocol.classic._pycbc_core import pycbc_connection
from couchbase.protocol.classic._result import _ping_from_core
from couchbase.protocol.classic._streaming import ViewStreamSpec

if TYPE_CHECKING:
    from acouchbase.protocol.classic._client_adapter import AsyncClientAdapter
    from acouchbase.protocol.classic._cluster_impl import AsyncClusterImpl
    from couchbase.common.options import PingOptions, ViewOptions
    from couchbase.common.protocol import CouchbaseProtocol
    from couchbase.protocol.classic._bucket_types import PingRequest
    from couchbase.serializer import Serializer
    from couchbase.transcoder import Transcoder


class AsyncBucketImpl:

    def __init__(self, bucket_name: str, cluster_impl: AsyncClusterImpl) -> None:
        self._bucket_name = bucket_name
        self._client_adapter: AsyncClientAdapter = cluster_impl._client_adapter
        self._cluster_settings = cluster_impl._cluster_settings
        self._streaming_timeouts = cluster_impl.streaming_timeouts
        self._kv_durable_timeout = cluster_impl.kv_durable_timeout
        self._request_builder = BucketRequestBuilder(bucket_name,
                                                     self._client_adapter.option_validation)
        # Opening a bucket is an async operation that we cannot await b/c the call needs to happen when we initialize
        # a bucket. We await the bucket connection future in whichever operation comes next.
        # NOTE:  We chain the bucket connection with the client connection future if the client connection future was
        #        not previously awaited.
        self._connect_ft = self._client_adapter.execute_connect_bucket_request(bucket_name)
        self._bucket_connected = False

    @property
    def couchbase_protocol(self) -> CouchbaseProtocol:
        """**INTERNAL**"""
        return self._client_adapter.couchbase_protocol

    @property
    def bucket_connected(self) -> bool:
        """**INTERNAL**"""
        return self._bucket_connected

    @property
    def bucket_connect_ft(self) -> Future[None]:
        """**INTERNAL**"""
        return self._connect_ft

    @property
    def bucket_name(self) -> str:
        """**INTERNAL**"""
        return self._bucket_name

    @property
    def connected(self) -> bool:
        """**INTERNAL**"""
        return self._client_adapter.connected

    @property
    def connection(self) -> pycbc_connection:
        """**INTERNAL**"""
        return self._client_adapter.connection

    @property
    def cluster_settings(self) -> ClusterSettings:
        """**INTERNAL**"""
        return self._cluster_settings

    @property
    def default_serializer(self) -> Serializer:
        """
        **INTERNAL**
        """
        return self._cluster_settings.default_serializer

    @property
    def default_transcoder(self) -> Transcoder:
        """**INTERNAL**"""
        return self._cluster_settings.default_transcoder

    @property
    def loop(self) -> AbstractEventLoop:
        """**INTERNAL**"""
        return self._client_adapter.loop

    @property
    def request_builder(self) -> BucketRequestBuilder:
        """**INTERNAL**"""
        return self._request_builder

    @property
    def streaming_timeouts(self) -> StreamingTimeouts:
        """**INTERNAL**"""
        return self._streaming_timeouts

    @property
    def kv_durable_timeout(self) -> int | None:
        """**INTERNAL**"""
        return self._kv_durable_timeout

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        """**INTERNAL**"""
        return self._cluster_settings.observability_instruments

    async def close_bucket(self) -> None:
        """**INTERNAL**"""
        try:
            await self.wait_until_bucket_connected()
        except Exception:
            # The bucket never opened, so there is nothing to tear down, and the caller cannot
            # act on an open error raised out of close.
            return
        await self._client_adapter.execute_close_bucket_request(self._bucket_name)

    def scope(self, name: str) -> AsyncScopeImpl:
        """**INTERNAL**"""
        return AsyncScopeImpl(name, self)

    async def ping(self, opts: PingOptions | None = None, **kwargs: Any) -> PingResult:
        return await self._ping(self._request_builder.build_ping_request(opts, **kwargs))

    # Split from ping so TxBucketImpl can build the request inside defer()'s start callable, where a bad
    # argument raises at the call rather than arriving as a failed Deferred.
    async def _ping(self, req: PingRequest) -> PingResult:
        await self.wait_until_bucket_connected()
        res = await self._client_adapter.execute_bucket_request(req)
        return PingResult(_ping_from_core(res.raw_result))

    def collection_mgmt(self) -> AsyncCollectionMgmtImpl:
        """**INTERNAL**"""
        return AsyncCollectionMgmtImpl(self._client_adapter, self._bucket_name, self.observability_instruments)

    def view_index_mgmt(self) -> AsyncViewIndexMgmtImpl:
        """**INTERNAL**"""
        return AsyncViewIndexMgmtImpl(self._client_adapter, self._bucket_name, self.observability_instruments)

    def view_query(self,
                   design_doc: str,
                   view_name: str,
                   view_options: ViewOptions | None = None,
                   **kwargs: Any) -> AsyncViewResult:
        obs_handler = ObservableRequestHandler(Op.ViewQuery, self.observability_instruments)
        req = self._request_builder.build_view_query_request(design_doc,
                                                             view_name,
                                                             obs_handler,
                                                             view_options,
                                                             **kwargs)
        self._client_adapter._ensure_not_closed()
        if not self.connected or not self.bucket_connected:
            raise RuntimeError('Cannot perform operations without first establishing a connection.')
        # If the view_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the view_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's view_timeout (set here). If the cluster
        # also does not specify a view_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::view_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._streaming_timeouts.get('view_timeout', None)
        executor = ClassicAsyncStreamingExecutor(self._client_adapter.connection,
                                                 ViewStreamSpec(req.view_query.as_encodable(), self.default_serializer),
                                                 self.loop,
                                                 obs_handler=req.obs_handler,
                                                 streaming_timeout=streaming_timeout)
        return AsyncViewResult(AsyncRowIterator(executor))

    async def wait_until_bucket_connected(self) -> None:
        """**INTERNAL**"""
        if self.bucket_connected:
            return
        await self._connect_ft
        self._bucket_connected = True
