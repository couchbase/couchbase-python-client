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

from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any

from acouchbase.protocol.classic._management._bucket_mgmt_impl import AsyncBucketMgmtImpl
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from txcouchbase.protocol.classic._deferred import defer

if TYPE_CHECKING:
    from twisted.internet.defer import Deferred

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


class TxBucketMgmtImpl(AsyncBucketMgmtImpl):

    def create_bucket_deferred(self,
                               settings: CreateBucketSettings,
                               options: CreateBucketOptions | None = None,
                               **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.BucketCreate, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_create_bucket_request(settings, obs_handler, options, **kwargs)
            return self._create_bucket(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def update_bucket_deferred(self,
                               settings: BucketSettings,
                               options: UpdateBucketOptions | None = None,
                               **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.BucketUpdate, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_update_bucket_request(settings, obs_handler, options, **kwargs)
            return self._update_bucket(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def drop_bucket_deferred(self,
                             bucket_name: str,
                             options: DropBucketOptions | None = None,
                             **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.BucketDrop, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_drop_bucket_request(bucket_name, obs_handler, options, **kwargs)
            return self._drop_bucket(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_bucket_deferred(self,
                            bucket_name: str,
                            options: GetBucketOptions | None = None,
                            **kwargs: Any) -> Deferred[BucketSettings]:
        obs_handler = ObservableRequestHandler(Op.BucketGet, self._observability_instruments)

        def start() -> Coroutine[Any, Any, BucketSettings]:
            req = self._request_builder.build_get_bucket_request(bucket_name, obs_handler, options, **kwargs)
            return self._get_bucket(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_all_buckets_deferred(self,
                                 options: GetAllBucketOptions | None = None,
                                 **kwargs: Any) -> Deferred[list[BucketSettings]]:
        obs_handler = ObservableRequestHandler(Op.BucketGetAll, self._observability_instruments)

        def start() -> Coroutine[Any, Any, list[BucketSettings]]:
            req = self._request_builder.build_get_all_buckets_request(obs_handler, options, **kwargs)
            return self._get_all_buckets(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def flush_bucket_deferred(self,
                              bucket_name: str,
                              options: FlushBucketOptions | None = None,
                              **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.BucketFlush, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_flush_bucket_request(bucket_name, obs_handler, options, **kwargs)
            return self._flush_bucket(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def bucket_describe_deferred(self,
                                 bucket_name: str,
                                 options: BucketDescribeOptions | None = None,
                                 **kwargs: Any) -> Deferred[BucketDescribeResult]:
        obs_handler = ObservableRequestHandler(Op.BucketDescribe, self._observability_instruments)

        def start() -> Coroutine[Any, Any, BucketDescribeResult]:
            req = self._request_builder.build_bucket_describe_request(bucket_name, obs_handler, options, **kwargs)
            return self._bucket_describe(req, obs_handler)

        return defer(self.loop, obs_handler, start)
