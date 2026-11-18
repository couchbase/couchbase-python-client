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

"""An asyncio bucket manager over the gateway's bucket admin service.

Each operation's request is built, then sent, so Twisted can build at the call and send in a coroutine.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.protocol.couchbase2._core import unsupported
from couchbase.protocol.couchbase2._driver import call_unary_async
from couchbase.protocol.couchbase2._management._bucket_mgmt_req_builder import BucketMgmtRequestBuilder
from couchbase.protocol.couchbase2._management._bucket_mgmt_types import (BucketMgmtRequest,
                                                                          GetBucketRequest,
                                                                          UpdateBucketRequest,
                                                                          bucket_mgmt_errors,
                                                                          bucket_settings,
                                                                          check_no_durability_to_clear,
                                                                          named_bucket_settings)

if TYPE_CHECKING:
    from acouchbase.protocol.couchbase2._cluster_impl import AsyncClusterImpl
    from couchbase.common.management.buckets import (BucketDescribeResult,
                                                     BucketSettings,
                                                     CreateBucketSettings)
    from couchbase.common.management.options import (BucketDescribeOptions,
                                                     CreateBucketOptions,
                                                     DropBucketOptions,
                                                     FlushBucketOptions,
                                                     GetAllBucketOptions,
                                                     GetBucketOptions,
                                                     UpdateBucketOptions)
    from couchbase.protocol.couchbase2._generated.admin.bucket.v1 import bucket_pb2


class AsyncBucketMgmtImpl:

    def __init__(self, cluster_impl: AsyncClusterImpl) -> None:
        self._cluster_impl = cluster_impl
        self._request_builder = BucketMgmtRequestBuilder(cluster_impl.core)

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        return self._cluster_impl.observability_instruments

    def request_builder(self) -> BucketMgmtRequestBuilder:
        """**INTERNAL** The builder, once the cluster is known to be open."""
        self._cluster_impl.core.ensure_open()
        return self._request_builder

    async def create_bucket(self,
                            settings: CreateBucketSettings,
                            options: CreateBucketOptions | None = None,
                            **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.BucketCreate, self.observability_instruments) as obs_handler:
            await self.send_create_bucket(
                self.request_builder().build_create_bucket_request(settings, obs_handler, options, **kwargs))

    async def update_bucket(self,
                            settings: BucketSettings,
                            options: UpdateBucketOptions | None = None,
                            **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.BucketUpdate, self.observability_instruments) as obs_handler:
            await self.send_update_bucket(
                self.request_builder().build_update_bucket_request(settings, obs_handler, options, **kwargs))

    async def drop_bucket(self,
                          bucket_name: str,
                          options: DropBucketOptions | None = None,
                          **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.BucketDrop, self.observability_instruments) as obs_handler:
            await self.send_drop_bucket(
                self.request_builder().build_drop_bucket_request(bucket_name, obs_handler, options, **kwargs))

    async def get_bucket(self,
                         bucket_name: str,
                         options: GetBucketOptions | None = None,
                         **kwargs: Any) -> BucketSettings:
        async with ObservableRequestHandler.create(Op.BucketGet, self.observability_instruments) as obs_handler:
            return await self.send_get_bucket(
                self.request_builder().build_get_bucket_request(bucket_name, obs_handler, options, **kwargs))

    async def get_all_buckets(self,
                              options: GetAllBucketOptions | None = None,
                              **kwargs: Any) -> list[BucketSettings]:
        async with ObservableRequestHandler.create(Op.BucketGetAll, self.observability_instruments) as obs_handler:
            return await self.send_get_all_buckets(
                self.request_builder().build_get_all_buckets_request(obs_handler, options, **kwargs))

    async def flush_bucket(self,
                           bucket_name: str,
                           options: FlushBucketOptions | None = None,
                           **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.BucketFlush, self.observability_instruments) as obs_handler:
            await self.send_flush_bucket(
                self.request_builder().build_flush_bucket_request(bucket_name, obs_handler, options, **kwargs))

    async def bucket_describe(self,
                              bucket_name: str,
                              options: BucketDescribeOptions | None = None,
                              **kwargs: Any) -> BucketDescribeResult:
        raise unsupported('bucket_describe')

    async def send_create_bucket(self, req: BucketMgmtRequest[bucket_pb2.CreateBucketRequest]) -> None:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        stub = await self._cluster_impl.bucket_admin()
        ctx = core.context(Op.BucketCreate, req.timeout, obs_handler=req.obs_handler)
        with bucket_mgmt_errors():
            await call_unary_async(stub.CreateBucket, req.message, ctx, core.metadata)

    async def send_update_bucket(self, req: UpdateBucketRequest) -> None:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        stub = await self._cluster_impl.bucket_admin()
        ctx = core.context(Op.BucketUpdate, req.timeout, obs_handler=req.obs_handler)
        if req.listing is not None:
            listed = await call_unary_async(stub.ListBuckets, req.listing, ctx, core.metadata)
            check_no_durability_to_clear(listed, req.message.bucket_name)
        with bucket_mgmt_errors():
            await call_unary_async(stub.UpdateBucket, req.message, ctx, core.metadata)

    async def send_drop_bucket(self, req: BucketMgmtRequest[bucket_pb2.DeleteBucketRequest]) -> None:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        stub = await self._cluster_impl.bucket_admin()
        ctx = core.context(Op.BucketDrop, req.timeout, obs_handler=req.obs_handler)
        with bucket_mgmt_errors():
            await call_unary_async(stub.DeleteBucket, req.message, ctx, core.metadata)

    async def send_get_bucket(self, req: GetBucketRequest) -> BucketSettings:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        stub = await self._cluster_impl.bucket_admin()
        ctx = core.context(Op.BucketGet, req.timeout, obs_handler=req.obs_handler)
        response = await call_unary_async(stub.ListBuckets, req.message, ctx, core.metadata)
        return named_bucket_settings(response, req.bucket_name)

    async def send_get_all_buckets(self,
                                   req: BucketMgmtRequest[bucket_pb2.ListBucketsRequest]) -> list[BucketSettings]:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        stub = await self._cluster_impl.bucket_admin()
        ctx = core.context(Op.BucketGetAll, req.timeout, obs_handler=req.obs_handler)
        return bucket_settings(await call_unary_async(stub.ListBuckets, req.message, ctx, core.metadata))

    async def send_flush_bucket(self, req: BucketMgmtRequest[bucket_pb2.FlushBucketRequest]) -> None:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        stub = await self._cluster_impl.bucket_admin()
        ctx = core.context(Op.BucketFlush, req.timeout, obs_handler=req.obs_handler)
        with bucket_mgmt_errors():
            await call_unary_async(stub.FlushBucket, req.message, ctx, core.metadata)
