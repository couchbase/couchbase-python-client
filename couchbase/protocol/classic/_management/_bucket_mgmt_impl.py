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

from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common.management.buckets import BucketDescribeResult, BucketSettings
from couchbase.protocol.classic._management._bucket_mgmt_req_builder import BucketMgmtRequestBuilder

if TYPE_CHECKING:
    from couchbase.common._observability import ObservabilityInstruments
    from couchbase.common.management.buckets import CreateBucketSettings
    from couchbase.common.management.options import (BucketDescribeOptions,
                                                     CreateBucketOptions,
                                                     DropBucketOptions,
                                                     FlushBucketOptions,
                                                     GetAllBucketOptions,
                                                     GetBucketOptions,
                                                     UpdateBucketOptions)
    from couchbase.protocol.classic._client_adapter import ClientAdapter


class BucketMgmtImpl:
    def __init__(self, client_adapter: ClientAdapter, observability_instruments: ObservabilityInstruments) -> None:
        self._client_adapter = client_adapter
        self._request_builder = BucketMgmtRequestBuilder(client_adapter.option_validation)
        self._observability_instruments = observability_instruments

    def create_bucket(self,
                      settings: CreateBucketSettings,
                      options: CreateBucketOptions | None = None,
                      **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.BucketCreate, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_create_bucket_request(settings, obs_handler, options, **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def update_bucket(self,
                      settings: BucketSettings,
                      options: UpdateBucketOptions | None = None,
                      **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.BucketUpdate, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_update_bucket_request(settings, obs_handler, options, **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def drop_bucket(self,
                    bucket_name: str,
                    options: DropBucketOptions | None = None,
                    **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.BucketDrop, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_bucket_request(bucket_name, obs_handler, options, **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def get_bucket(self,
                   bucket_name: str,
                   options: GetBucketOptions | None = None,
                   **kwargs: Any) -> BucketSettings:
        with ObservableRequestHandler(Op.BucketGet, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_bucket_request(bucket_name, obs_handler, options, **kwargs)
            res = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            raw_settings = res.raw_result['bucket']
            return BucketSettings.bucket_settings_from_server(raw_settings)

    def get_all_buckets(self,
                        options: GetAllBucketOptions | None = None,
                        **kwargs: Any) -> list[BucketSettings]:
        with ObservableRequestHandler(Op.BucketGetAll, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_buckets_request(obs_handler, options, **kwargs)
            res = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            raw_buckets = res.raw_result['buckets']
            buckets = []
            for b in raw_buckets:
                bucket_settings = BucketSettings.bucket_settings_from_server(b)
                buckets.append(bucket_settings)

            return buckets

    def flush_bucket(self,
                     bucket_name: str,
                     options: FlushBucketOptions | None = None,
                     **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.BucketFlush, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_flush_bucket_request(bucket_name, obs_handler, options, **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def bucket_describe(self,
                        bucket_name: str,
                        options: BucketDescribeOptions | None = None,
                        **kwargs: Any) -> BucketDescribeResult:
        with ObservableRequestHandler(Op.BucketDescribe, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_bucket_describe_request(bucket_name, obs_handler, options, **kwargs)
            res = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            bucket_info = res.raw_result['info']
            return BucketDescribeResult(**bucket_info)
