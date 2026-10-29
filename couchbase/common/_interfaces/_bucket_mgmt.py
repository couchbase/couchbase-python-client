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

"""What a protocol's bucket management impl must provide to the `BucketManager` facades.

The impl is built by its cluster impl.  The SDK imports this module only under `TYPE_CHECKING`.
"""

from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    Protocol)

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


class BucketMgmtInterface(Protocol):
    def create_bucket(self,
                      settings: CreateBucketSettings,
                      options: CreateBucketOptions | None = None,
                      **kwargs: Any) -> None: ...

    def update_bucket(self,
                      settings: BucketSettings,
                      options: UpdateBucketOptions | None = None,
                      **kwargs: Any) -> None: ...

    def drop_bucket(self,
                    bucket_name: str,
                    options: DropBucketOptions | None = None,
                    **kwargs: Any) -> None: ...

    def get_bucket(self,
                   bucket_name: str,
                   options: GetBucketOptions | None = None,
                   **kwargs: Any) -> BucketSettings: ...

    def get_all_buckets(self,
                        options: GetAllBucketOptions | None = None,
                        **kwargs: Any) -> list[BucketSettings]: ...

    def flush_bucket(self,
                     bucket_name: str,
                     options: FlushBucketOptions | None = None,
                     **kwargs: Any) -> None: ...

    def bucket_describe(self,
                        bucket_name: str,
                        options: BucketDescribeOptions | None = None,
                        **kwargs: Any) -> BucketDescribeResult: ...


class AsyncBucketMgmtInterface(Protocol):
    async def create_bucket(self,
                            settings: CreateBucketSettings,
                            options: CreateBucketOptions | None = None,
                            **kwargs: Any) -> None: ...

    async def update_bucket(self,
                            settings: BucketSettings,
                            options: UpdateBucketOptions | None = None,
                            **kwargs: Any) -> None: ...

    async def drop_bucket(self,
                          bucket_name: str,
                          options: DropBucketOptions | None = None,
                          **kwargs: Any) -> None: ...

    async def get_bucket(self,
                         bucket_name: str,
                         options: GetBucketOptions | None = None,
                         **kwargs: Any) -> BucketSettings: ...

    async def get_all_buckets(self,
                              options: GetAllBucketOptions | None = None,
                              **kwargs: Any) -> list[BucketSettings]: ...

    async def flush_bucket(self,
                           bucket_name: str,
                           options: FlushBucketOptions | None = None,
                           **kwargs: Any) -> None: ...

    async def bucket_describe(self,
                              bucket_name: str,
                              options: BucketDescribeOptions | None = None,
                              **kwargs: Any) -> BucketDescribeResult: ...


class TxBucketMgmtInterface(Protocol):
    def create_bucket_deferred(self,
                               settings: CreateBucketSettings,
                               options: CreateBucketOptions | None = None,
                               **kwargs: Any) -> Deferred[None]: ...

    def update_bucket_deferred(self,
                               settings: BucketSettings,
                               options: UpdateBucketOptions | None = None,
                               **kwargs: Any) -> Deferred[None]: ...

    def drop_bucket_deferred(self,
                             bucket_name: str,
                             options: DropBucketOptions | None = None,
                             **kwargs: Any) -> Deferred[None]: ...

    def get_bucket_deferred(self,
                            bucket_name: str,
                            options: GetBucketOptions | None = None,
                            **kwargs: Any) -> Deferred[BucketSettings]: ...

    def get_all_buckets_deferred(self,
                                 options: GetAllBucketOptions | None = None,
                                 **kwargs: Any) -> Deferred[list[BucketSettings]]: ...

    def flush_bucket_deferred(self,
                              bucket_name: str,
                              options: FlushBucketOptions | None = None,
                              **kwargs: Any) -> Deferred[None]: ...

    def bucket_describe_deferred(self,
                                 bucket_name: str,
                                 options: BucketDescribeOptions | None = None,
                                 **kwargs: Any) -> Deferred[BucketDescribeResult]: ...
