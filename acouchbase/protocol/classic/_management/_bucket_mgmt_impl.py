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

from asyncio import AbstractEventLoop
from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common.management.buckets import BucketDescribeResult, BucketSettings
from couchbase.protocol.classic._management._bucket_mgmt_req_builder import BucketMgmtRequestBuilder

if TYPE_CHECKING:
    from acouchbase.protocol.classic._client_adapter import AsyncClientAdapter
    from couchbase.common._observability import ObservabilityInstruments
    from couchbase.common.management.buckets import CreateBucketSettings
    from couchbase.common.management.options import (BucketDescribeOptions,
                                                     CreateBucketOptions,
                                                     DropBucketOptions,
                                                     FlushBucketOptions,
                                                     GetAllBucketOptions,
                                                     GetBucketOptions,
                                                     UpdateBucketOptions)
    from couchbase.protocol.classic._management._bucket_mgmt_types import (BucketDescribeRequest,
                                                                           CreateBucketRequest,
                                                                           DropBucketRequest,
                                                                           FlushBucketRequest,
                                                                           GetAllBucketsRequest,
                                                                           GetBucketRequest,
                                                                           UpdateBucketRequest)


class AsyncBucketMgmtImpl:
    def __init__(self,
                 client_adapter: AsyncClientAdapter,
                 observability_instruments: ObservabilityInstruments) -> None:
        self._client_adapter = client_adapter
        self._request_builder = BucketMgmtRequestBuilder(client_adapter.option_validation)
        self._observability_instruments = observability_instruments

    @property
    def loop(self) -> AbstractEventLoop:
        """**INTERNAL** The loop the txcouchbase impl runs these coroutines on."""
        return self._client_adapter.loop

    async def create_bucket(self,
                            settings: CreateBucketSettings,
                            options: CreateBucketOptions | None = None,
                            **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.BucketCreate, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_create_bucket_request(settings, obs_handler, options, **kwargs)
            await self._create_bucket(req, obs_handler)

    async def update_bucket(self,
                            settings: BucketSettings,
                            options: UpdateBucketOptions | None = None,
                            **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.BucketUpdate, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_update_bucket_request(settings, obs_handler, options, **kwargs)
            await self._update_bucket(req, obs_handler)

    async def drop_bucket(self,
                          bucket_name: str,
                          options: DropBucketOptions | None = None,
                          **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.BucketDrop, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_bucket_request(bucket_name, obs_handler, options, **kwargs)
            await self._drop_bucket(req, obs_handler)

    async def get_bucket(self,
                         bucket_name: str,
                         options: GetBucketOptions | None = None,
                         **kwargs: Any) -> BucketSettings:
        async with ObservableRequestHandler(Op.BucketGet, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_bucket_request(bucket_name, obs_handler, options, **kwargs)
            return await self._get_bucket(req, obs_handler)

    async def get_all_buckets(self,
                              options: GetAllBucketOptions | None = None,
                              **kwargs: Any) -> list[BucketSettings]:
        async with ObservableRequestHandler(Op.BucketGetAll, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_buckets_request(obs_handler, options, **kwargs)
            return await self._get_all_buckets(req, obs_handler)

    async def flush_bucket(self,
                           bucket_name: str,
                           options: FlushBucketOptions | None = None,
                           **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.BucketFlush, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_flush_bucket_request(bucket_name, obs_handler, options, **kwargs)
            await self._flush_bucket(req, obs_handler)

    async def bucket_describe(self,
                              bucket_name: str,
                              options: BucketDescribeOptions | None = None,
                              **kwargs: Any) -> BucketDescribeResult:
        async with ObservableRequestHandler(Op.BucketDescribe, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_bucket_describe_request(bucket_name, obs_handler, options, **kwargs)
            return await self._bucket_describe(req, obs_handler)

    # The execute half of each operation.  The txcouchbase impl builds the request itself, inside
    # the `start()` it hands to `defer()`, and returns one of these as the coroutine to run.
    async def _bucket_describe(self,
                               req: BucketDescribeRequest,
                               obs_handler: ObservableRequestHandler) -> BucketDescribeResult:
        res = await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        bucket_info = res.raw_result['info']
        return BucketDescribeResult(**bucket_info)

    async def _create_bucket(self, req: CreateBucketRequest, obs_handler: ObservableRequestHandler) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def _drop_bucket(self, req: DropBucketRequest, obs_handler: ObservableRequestHandler) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def _flush_bucket(self, req: FlushBucketRequest, obs_handler: ObservableRequestHandler) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def _get_all_buckets(self,
                               req: GetAllBucketsRequest,
                               obs_handler: ObservableRequestHandler) -> list[BucketSettings]:
        res = await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_buckets = res.raw_result['buckets']
        buckets = []
        for b in raw_buckets:
            bucket_settings = BucketSettings.bucket_settings_from_server(b)
            buckets.append(bucket_settings)

        return buckets

    async def _get_bucket(self, req: GetBucketRequest, obs_handler: ObservableRequestHandler) -> BucketSettings:
        res = await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_settings = res.raw_result['bucket']
        return BucketSettings.bucket_settings_from_server(raw_settings)

    async def _update_bucket(self, req: UpdateBucketRequest, obs_handler: ObservableRequestHandler) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
