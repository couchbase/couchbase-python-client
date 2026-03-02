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
from typing import (TYPE_CHECKING,
                    List,
                    Optional)

from twisted.internet.defer import Deferred
from twisted.python.failure import Failure

from acouchbase.management.logic.bucket_mgmt_impl import AsyncBucketMgmtImpl
from couchbase.logic.observability import ObservabilityInstruments, ObservableRequestHandler
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


class TxBucketMgmtImpl(AsyncBucketMgmtImpl):
    def __init__(self,
                 client_adapter: AsyncClientAdapter,
                 observability_instruments: ObservabilityInstruments) -> None:
        super().__init__(client_adapter, observability_instruments)

    def _finish_span(self, result, obs_handler):
        """Callback to properly end the span on success or failure."""
        if isinstance(result, Failure):
            exc = result.value
            obs_handler.__exit__(type(exc), exc, exc.__traceback__)
            return result
        else:
            obs_handler.__exit__(None, None, None)
            return result

    def bucket_describe_deferred(self,
                                 req: BucketDescribeRequest,
                                 obs_handler: ObservableRequestHandler) -> Deferred[Optional[BucketDescribeResult]]:
        """**INTERNAL**"""
        coro = super().bucket_describe(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def create_bucket_deferred(self,
                               req: CreateBucketRequest,
                               obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().create_bucket(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_bucket_deferred(self, req: DropBucketRequest, obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_bucket(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def flush_bucket_deferred(self, req: FlushBucketRequest, obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().flush_bucket(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_buckets_deferred(self,
                                 req: GetAllBucketsRequest,
                                 obs_handler: ObservableRequestHandler) -> Deferred[List[BucketSettings]]:
        """**INTERNAL**"""
        coro = super().get_all_buckets(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_bucket_deferred(self,
                            req: GetBucketRequest,
                            obs_handler: ObservableRequestHandler) -> Deferred[BucketSettings]:
        """**INTERNAL**"""
        coro = super().get_bucket(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def update_bucket_deferred(self, req: UpdateBucketRequest, obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().update_bucket(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d
