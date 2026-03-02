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
from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import BucketMgmtOperationType
from couchbase.management.logic.bucket_mgmt_types import (BucketDescribeResult,
                                                          BucketSettings,
                                                          CreateBucketSettings)

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.logic.observability import ObservabilityInstruments
    from couchbase.management.options import (BucketDescribeOptions,
                                              CreateBucketOptions,
                                              DropBucketOptions,
                                              FlushBucketOptions,
                                              GetAllBucketOptions,
                                              GetBucketOptions,
                                              UpdateBucketOptions)


class BucketManager:
    def __init__(self,
                 client_adapter: AsyncClientAdapter,
                 observability_instruments: ObservabilityInstruments) -> None:
        self._impl = AsyncBucketMgmtImpl(client_adapter, observability_instruments)

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
        op_type = BucketMgmtOperationType.BucketCreate
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_create_bucket_request(settings, obs_handler, *options, **kwargs)
            await self._impl.create_bucket(req, obs_handler)

    async def update_bucket(self,
                            settings,  # type: BucketSettings
                            *options,  # type: UpdateBucketOptions
                            **kwargs  # type: Dict[str, Any]
                            ) -> None:
        op_type = BucketMgmtOperationType.BucketUpdate
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_update_bucket_request(settings, obs_handler, *options, **kwargs)
            await self._impl.update_bucket(req, obs_handler)

    async def drop_bucket(self,
                          bucket_name,  # type: str
                          *options,     # type: DropBucketOptions
                          **kwargs      # type: Dict[str, Any]
                          ) -> None:
        op_type = BucketMgmtOperationType.BucketDrop
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_drop_bucket_request(bucket_name, obs_handler, *options, **kwargs)
            await self._impl.drop_bucket(req, obs_handler)

    async def get_bucket(self,
                         bucket_name,   # type: str
                         *options,      # type: GetBucketOptions
                         **kwargs       # type: Dict[str, Any]
                         ) -> Awaitable[BucketSettings]:
        op_type = BucketMgmtOperationType.BucketGet
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_bucket_request(bucket_name, obs_handler, *options, **kwargs)
            return await self._impl.get_bucket(req, obs_handler)

    async def get_all_buckets(self,
                              *options,  # type: GetAllBucketOptions
                              **kwargs  # type: Dict[str, Any]
                              ) -> Awaitable[List[BucketSettings]]:
        op_type = BucketMgmtOperationType.BucketGetAll
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_all_buckets_request(obs_handler, *options, **kwargs)
            return await self._impl.get_all_buckets(req, obs_handler)

    async def flush_bucket(self,
                           bucket_name,   # type: str
                           *options,      # type: FlushBucketOptions
                           **kwargs       # type: Dict[str, Any]
                           ) -> Awaitable[None]:
        op_type = BucketMgmtOperationType.BucketFlush
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_flush_bucket_request(bucket_name, obs_handler, *options, **kwargs)
            await self._impl.flush_bucket(req, obs_handler)

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
        op_type = BucketMgmtOperationType.BucketDescribe
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_bucket_describe_request(bucket_name, obs_handler, *options, **kwargs)
            return await self._impl.bucket_describe(req, obs_handler)
