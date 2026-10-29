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

__all__ = [
    'BucketDescribeResult',
    'BucketManager',
    'BucketSettings',
    'BucketType',
    'CompressionMode',
    'ConflictResolutionType',
    'CreateBucketSettings',
    'EjectionMethod',
    'EvictionPolicyType',
    'StorageBackend',
]

from typing import TYPE_CHECKING, Any

from couchbase.common.management.buckets import (BucketDescribeResult,
                                                 BucketSettings,
                                                 BucketType,
                                                 CompressionMode,
                                                 ConflictResolutionType,
                                                 CreateBucketSettings,
                                                 EjectionMethod,
                                                 EvictionPolicyType,
                                                 StorageBackend)

if TYPE_CHECKING:
    from couchbase.common._interfaces._bucket_mgmt import AsyncBucketMgmtInterface
    from couchbase.management.options import (BucketDescribeOptions,
                                              CreateBucketOptions,
                                              DropBucketOptions,
                                              FlushBucketOptions,
                                              GetAllBucketOptions,
                                              GetBucketOptions,
                                              UpdateBucketOptions)


class BucketManager:
    def __init__(self, impl: AsyncBucketMgmtInterface) -> None:
        self._impl = impl

    async def create_bucket(self,
                            settings: CreateBucketSettings,
                            options: CreateBucketOptions | None = None,
                            **kwargs: Any) -> None:
        """Creates a new bucket.

        Args:
            settings (:class:`.CreateBucketSettings`): The settings to use for the new bucket.
            options (:class:`~couchbase.management.options.CreateBucketOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.BucketAlreadyExistsException`: If the bucket already exists.
            :class:`~couchbase.exceptions.InvalidArgumentException`: If an invalid type or value is provided for the
                settings argument.
        """
        await self._impl.create_bucket(settings, options, **kwargs)

    async def update_bucket(self,
                            settings: BucketSettings,
                            options: UpdateBucketOptions | None = None,
                            **kwargs: Any) -> None:
        """Update the settings for an existing bucket.

        Args:
            settings (:class:`.BucketSettings`): The settings to use for the new bucket.
            options (:class:`~couchbase.management.options.UpdateBucketOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If an invalid type or value is provided for the
                settings argument.
        """
        await self._impl.update_bucket(settings, options, **kwargs)

    async def drop_bucket(self,
                          bucket_name: str,
                          options: DropBucketOptions | None = None,
                          **kwargs: Any) -> None:
        """Drops an existing bucket.

        Args:
            bucket_name (str): The name of the bucket to drop.
            options (:class:`~couchbase.management.options.DropBucketOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.BucketDoesNotExistException`: If the bucket does not exist.
        """
        await self._impl.drop_bucket(bucket_name, options, **kwargs)

    async def get_bucket(self,
                         bucket_name: str,
                         options: GetBucketOptions | None = None,
                         **kwargs: Any) -> BucketSettings:
        """Fetches the settings in use for a specified bucket.

        Args:
            bucket_name (str): The name of the bucket to fetch.
            options (:class:`~couchbase.management.options.GetBucketOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`.BucketSettings`: The settings of the specified bucket.

        Raises:
            :class:`~couchbase.exceptions.BucketDoesNotExistException`: If the bucket does not exist.
        """
        return await self._impl.get_bucket(bucket_name, options, **kwargs)

    async def get_all_buckets(self,
                              options: GetAllBucketOptions | None = None,
                              **kwargs: Any) -> list[BucketSettings]:
        """Returns a list of existing buckets in the cluster.

        Args:
            options (:class:`~couchbase.management.options.GetAllBucketOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            list[:class:`.BucketSettings`]: A list of existing buckets in the cluster.
        """
        return await self._impl.get_all_buckets(options, **kwargs)

    async def flush_bucket(self,
                           bucket_name: str,
                           options: FlushBucketOptions | None = None,
                           **kwargs: Any) -> None:
        """Flushes the bucket, deleting all the existing data that is stored in it.

        Args:
            bucket_name (str): The name of the bucket to flush.
            options (:class:`~couchbase.management.options.FlushBucketOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.BucketDoesNotExistException`: If the bucket does not exist.
            :class:`~couchbase.exceptions.BucketNotFlushableException`: If the bucket's settings have
                flushing disabled.
        """
        await self._impl.flush_bucket(bucket_name, options, **kwargs)

    async def bucket_describe(self,
                              bucket_name: str,
                              options: BucketDescribeOptions | None = None,
                              **kwargs: Any) -> BucketDescribeResult:
        """Provides details on provided the bucket.

        Args:
            bucket_name (str): The name of the bucket to describe.
            options (:class:`~couchbase.management.options.BucketDescribeOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`.BucketDescribeResult`: Key-value pair details describing the bucket.

        Raises:
            :class:`~couchbase.exceptions.BucketDoesNotExistException`: If the bucket does not exist.
        """
        return await self._impl.bucket_describe(bucket_name, options, **kwargs)
