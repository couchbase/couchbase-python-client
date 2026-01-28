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

from asyncio import AbstractEventLoop
from typing import (TYPE_CHECKING,
                    List,
                    Optional)

from couchbase.management.logic.bucket_mgmt_req_builder import BucketMgmtRequestBuilder
from couchbase.management.logic.bucket_mgmt_types import BucketDescribeResult, BucketSettings

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.management.logic.bucket_mgmt_types import (BucketDescribeRequest,
                                                              CreateBucketRequest,
                                                              DropBucketRequest,
                                                              FlushBucketRequest,
                                                              GetAllBucketsRequest,
                                                              GetBucketRequest,
                                                              UpdateBucketRequest)


class AsyncBucketMgmtImpl:
    def __init__(self, client_adapter: AsyncClientAdapter) -> None:
        self._client_adapter = client_adapter
        self._request_builder = BucketMgmtRequestBuilder()

    @property
    def loop(self) -> AbstractEventLoop:
        """**INTERNAL**"""
        return self._client_adapter.loop

    @property
    def request_builder(self) -> BucketMgmtRequestBuilder:
        """**INTERNAL**"""
        return self._request_builder

    async def bucket_describe(self, req: BucketDescribeRequest) -> Optional[BucketDescribeResult]:
        """**INTERNAL**"""
        res = await self._client_adapter.execute_mgmt_request(req)
        bucket_info = res.raw_result.get('bucket_info', None)
        if bucket_info:
            return BucketDescribeResult(**bucket_info)
        return None

    async def create_bucket(self, req: CreateBucketRequest) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req)

    async def drop_bucket(self, req: DropBucketRequest) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req)

    async def flush_bucket(self, req: FlushBucketRequest) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req)

    async def get_all_buckets(self, req: GetAllBucketsRequest) -> List[BucketSettings]:
        """**INTERNAL**"""
        res = await self._client_adapter.execute_mgmt_request(req)
        raw_buckets = res.raw_result.get('buckets', None)
        buckets = []
        if raw_buckets:
            for b in raw_buckets:
                bucket_settings = BucketSettings.bucket_settings_from_server(b)
                buckets.append(bucket_settings)

        return buckets

    async def get_bucket(self, req: GetBucketRequest) -> BucketSettings:
        """**INTERNAL**"""
        res = await self._client_adapter.execute_mgmt_request(req)
        raw_settings = res.raw_result.get('bucket_settings', None)
        bucket_settings = None
        if raw_settings:
            bucket_settings = BucketSettings.bucket_settings_from_server(raw_settings)

        return bucket_settings

    async def update_bucket(self, req: UpdateBucketRequest) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req)
