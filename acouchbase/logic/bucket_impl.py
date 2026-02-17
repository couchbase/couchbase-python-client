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
from typing import (TYPE_CHECKING,
                    Optional,
                    Union)

from acouchbase.views import AsyncViewRequest
from couchbase.logic.bucket_req_builder import BucketRequestBuilder
from couchbase.logic.client_adapter import PyCapsuleType
from couchbase.logic.cluster_settings import ClusterSettings, StreamingTimeouts
from couchbase.result import PingResult, ViewResult

if TYPE_CHECKING:
    from acouchbase.cluster import AsyncCluster
    from couchbase.logic.bucket_types import PingRequest, ViewQueryRequest
    from couchbase.serializer import Serializer
    from couchbase.transcoder import Transcoder
    from txcouchbase.cluster import TxCluster


class AsyncBucketImpl:
    def __init__(self, bucket_name: str, cluster: Union[AsyncCluster, TxCluster]) -> None:
        self._bucket_name = bucket_name
        self._client_adapter = cluster._impl._client_adapter
        self._cluster_settings = cluster._impl._cluster_settings
        self._request_builder = BucketRequestBuilder(bucket_name)
        # Opening a bucket is an async operation that we cannot await b/c the call needs to happen when we initialize
        # a bucket. We await the bucket connection future in whichever operation comes next.
        # NOTE:  We chain the bucket connection with the client connection future if the client connection future was
        #        not previously awaited.
        self._connect_ft = self._client_adapter.execute_connect_bucket_request(bucket_name)
        self._bucket_connected = False

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
    def connection(self) -> Optional[PyCapsuleType]:
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
        return self._cluster_settings.streaming_timeouts

    async def close_bucket(self) -> None:
        """**INTERNAL**"""
        await self.wait_until_bucket_connected()
        await self._client_adapter.execute_close_bucket_request(self._bucket_name)

    async def ping(self, req: PingRequest) -> PingResult:
        """**INTERNAL**"""
        await self.wait_until_bucket_connected()
        res = await self._client_adapter.execute_bucket_request(req)
        return PingResult(res)

    def view_query(self, req: ViewQueryRequest) -> ViewResult:
        """**INTERNAL**"""
        self._client_adapter._ensure_not_closed()
        if not self.connected or not self.bucket_connected:
            raise RuntimeError('Cannot perform operations without first establishing a connection.')
        # If the view_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the view_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's view_timeout (set here). If the cluster
        # also does not specify a view_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::view_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._cluster_settings.streaming_timeouts.get('view_timeout', None)
        return ViewResult(AsyncViewRequest.generate_view_request(self._client_adapter.connection,
                                                                 self.loop,
                                                                 req.view_query.as_encodable(),
                                                                 default_serializer=self.default_serializer,
                                                                 streaming_timeout=streaming_timeout,
                                                                 num_workers=req.num_workers))

    async def wait_until_bucket_connected(self) -> None:
        """**INTERNAL**"""
        if self.bucket_connected:
            return
        await self._connect_ft
        self._bucket_connected = True
