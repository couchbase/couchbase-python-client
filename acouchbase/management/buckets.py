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

from typing import (TYPE_CHECKING,
                    Any,
                    Awaitable,
                    Dict,
                    List)

from acouchbase.management.logic.bucket_mgmt_impl import AsyncBucketMgmtImpl
from couchbase.management.logic.bucket_mgmt_types import (BucketDescribeResult,
                                                          BucketSettings,
                                                          CreateBucketSettings)

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.management.options import (BucketDescribeOptions,
                                              CreateBucketOptions,
                                              DropBucketOptions,
                                              FlushBucketOptions,
                                              GetAllBucketOptions,
                                              GetBucketOptions,
                                              UpdateBucketOptions)


class BucketManager:
    def __init__(self, client_adapter: AsyncClientAdapter) -> None:
        self._impl = AsyncBucketMgmtImpl(client_adapter)

    async def create_bucket(self,
                            settings,  # type: CreateBucketSettings
                            *options,  # type: CreateBucketOptions
                            **kwargs   # type: Any
                            ) -> None:
        """
        Creates a new bucket.

        :param: CreateBucketSettings settings: settings for the bucket.
        :param: CreateBucketOptions options: options for setting the bucket.
        :param: Any kwargs: override corresponding values in the options.

        :raises: BucketAlreadyExistsException
        :raises: InvalidArgumentsException
        """
        req = self._impl.request_builder.build_create_bucket_request(settings, *options, **kwargs)
        await self._impl.create_bucket(req)

    async def update_bucket(self,
                            settings,  # type: BucketSettings
                            *options,  # type: UpdateBucketOptions
                            **kwargs  # type: Dict[str, Any]
                            ) -> None:
        req = self._impl.request_builder.build_update_bucket_request(settings, *options, **kwargs)
        await self._impl.update_bucket(req)

    async def drop_bucket(self,
                          bucket_name,  # type: str
                          *options,     # type: DropBucketOptions
                          **kwargs      # type: Dict[str, Any]
                          ) -> None:
        req = self._impl.request_builder.build_drop_bucket_request(bucket_name, *options, **kwargs)
        await self._impl.drop_bucket(req)

    async def get_bucket(self,
                         bucket_name,   # type: str
                         *options,      # type: GetBucketOptions
                         **kwargs       # type: Dict[str, Any]
                         ) -> Awaitable[BucketSettings]:
        req = self._impl.request_builder.build_get_bucket_request(bucket_name, *options, **kwargs)
        return await self._impl.get_bucket(req)

    async def get_all_buckets(self,
                              *options,  # type: GetAllBucketOptions
                              **kwargs  # type: Dict[str, Any]
                              ) -> Awaitable[List[BucketSettings]]:
        req = self._impl.request_builder.build_get_all_buckets_request(*options, **kwargs)
        return await self._impl.get_all_buckets(req)

    async def flush_bucket(self,
                           bucket_name,   # type: str
                           *options,      # type: FlushBucketOptions
                           **kwargs       # type: Dict[str, Any]
                           ) -> Awaitable[None]:
        req = self._impl.request_builder.build_flush_bucket_request(bucket_name, *options, **kwargs)
        await self._impl.flush_bucket(req)

    async def bucket_describe(self,
                              bucket_name,   # type: str
                              *options,      # type: BucketDescribeOptions
                              **kwargs       # type: Dict[str, Any]
                              ) -> BucketDescribeResult:
        """Provides details on provided the bucket.

        Args:
            bucket_name (str): The name of the bucket to flush.
            options (:class:`~couchbase.management.options.BucketDescribeOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`.BucketDescribeResult`: Key-value pair details describing the bucket.

        Raises:
            :class:`~couchbase.exceptions.BucketDoesNotExistException`: If the bucket does not exist.
        """
        req = self._impl.request_builder.build_bucket_describe_request(bucket_name, *options, **kwargs)
        return await self._impl.bucket_describe(req)
