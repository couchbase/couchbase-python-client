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

from typing import TYPE_CHECKING, List

from couchbase.management.logic.bucket_mgmt_req_builder import BucketMgmtRequestBuilder
from couchbase.management.logic.bucket_mgmt_types import BucketDescribeResult, BucketSettings

if TYPE_CHECKING:
    from couchbase.logic.client_adapter import ClientAdapter
    from couchbase.management.logic.bucket_mgmt_types import (BucketDescribeRequest,
                                                              CreateBucketRequest,
                                                              DropBucketRequest,
                                                              FlushBucketRequest,
                                                              GetAllBucketsRequest,
                                                              GetBucketRequest,
                                                              UpdateBucketRequest)


class BucketMgmtImpl:
    def __init__(self, client_adapter: ClientAdapter) -> None:
        self._client_adapter = client_adapter
        self._request_builder = BucketMgmtRequestBuilder()

    @property
    def request_builder(self) -> BucketMgmtRequestBuilder:
        """**INTERNAL**"""
        return self._request_builder

    def bucket_describe(self, req: BucketDescribeRequest) -> BucketDescribeResult:
        """**INTERNAL**"""
        res = self._client_adapter.execute_mgmt_request(req)
        bucket_info = res.raw_result['bucket_info']
        return BucketDescribeResult(**bucket_info)

    def create_bucket(self, req: CreateBucketRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def drop_bucket(self, req: DropBucketRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def flush_bucket(self, req: FlushBucketRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def get_all_buckets(self, req: GetAllBucketsRequest) -> List[BucketSettings]:
        """**INTERNAL**"""
        res = self._client_adapter.execute_mgmt_request(req)
        raw_buckets = res.raw_result['buckets']
        buckets = []
        for b in raw_buckets:
            bucket_settings = BucketSettings.bucket_settings_from_server(b)
            buckets.append(bucket_settings)

        return buckets

    def get_bucket(self, req: GetBucketRequest) -> BucketSettings:
        """**INTERNAL**"""
        res = self._client_adapter.execute_mgmt_request(req)
        raw_settings = res.raw_result['bucket_settings']
        return BucketSettings.bucket_settings_from_server(raw_settings)

    def update_bucket(self, req: UpdateBucketRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)
