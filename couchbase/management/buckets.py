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

from typing import (Any,
                    Dict,
                    List)

from couchbase.management.logic.buckets_logic import BucketType  # noqa: F401
from couchbase.management.logic.buckets_logic import CompressionMode  # noqa: F401
from couchbase.management.logic.buckets_logic import ConflictResolutionType  # noqa: F401
from couchbase.management.logic.buckets_logic import EjectionMethod  # noqa: F401
from couchbase.management.logic.buckets_logic import EvictionPolicyType  # noqa: F401
from couchbase.management.logic.buckets_logic import StorageBackend  # noqa: F401
from couchbase.management.logic.buckets_logic import (BucketDescribeResult,
                                                      BucketManagerLogic,
                                                      BucketSettings,
                                                      CreateBucketSettings)
from couchbase.management.logic.wrappers import BlockingMgmtWrapper, ManagementType

# @TODO:  lets deprecate import of options from couchbase.management.buckets
from couchbase.management.options import (BucketDescribeOptions,
                                          CreateBucketOptions,
                                          DropBucketOptions,
                                          FlushBucketOptions,
                                          GetAllBucketOptions,
                                          GetBucketOptions,
                                          UpdateBucketOptions)


class BucketManager(BucketManagerLogic):
    def __init__(self, connection):
        super().__init__(connection)

    @BlockingMgmtWrapper.block(None, ManagementType.BucketMgmt, BucketManagerLogic._ERROR_MAPPING)
    def create_bucket(self,
                      settings,  # type: CreateBucketSettings
                      *options,  # type: CreateBucketOptions
                      **kwargs   # type: Dict[str, Any]
                      ) -> None:
        """Creates a new bucket.

        Args:
            settings (:class:`.CreateBucketSettings`): The settings to use for the new bucket.
            options (:class:`~couchbase.management.options.CreateBucketOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.BucketAlreadyExistsException`: If the bucket already exists.
            :class:`~couchbase.exceptions.InvalidArgumentsException`: If an invalid type or value is provided for the
                settings argument.
        """
        return super().create_bucket(settings, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.BucketMgmt, BucketManagerLogic._ERROR_MAPPING)
    def update_bucket(self,
                      settings,  # type: BucketSettings
                      *options,  # type: UpdateBucketOptions
                      **kwargs  # type: Dict[str, Any]
                      ) -> None:
        """Update the settings for an existing bucket.

        Args:
            settings (:class:`.BucketSettings`): The settings to use for the new bucket.
            options (:class:`~couchbase.management.options.UpdateBucketOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentsException`: If an invalid type or value is provided for the
                settings argument.
        """
        return super().update_bucket(settings, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.BucketMgmt, BucketManagerLogic._ERROR_MAPPING)
    def drop_bucket(self,
                    bucket_name,  # type: str
                    *options,     # type: DropBucketOptions
                    **kwargs      # type: Dict[str, Any]
                    ) -> None:
        """Drops an existing bucket.

        Args:
            bucket_name (str): The name of the bucket to drop.
            options (:class:`~couchbase.management.options.DropBucketOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.BucketDoesNotExistException`: If the bucket does not exist.
        """
        return super().drop_bucket(bucket_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(BucketSettings, ManagementType.BucketMgmt, BucketManagerLogic._ERROR_MAPPING)
    def get_bucket(self,
                   bucket_name,   # type: str
                   *options,      # type: GetBucketOptions
                   **kwargs       # type: Dict[str, Any]
                   ) -> BucketSettings:
        """Fetches the settings in use for a specified bucket.

        Args:
            bucket_name (str): The name of the bucket to fetch.
            options (:class:`~couchbase.management.options.GetBucketOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`.BucketSettings`: The settings of the specified bucket.

        Raises:
            :class:`~couchbase.exceptions.BucketDoesNotExistException`: If the bucket does not exist.
        """
        return super().get_bucket(bucket_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(BucketSettings, ManagementType.BucketMgmt, BucketManagerLogic._ERROR_MAPPING)
    def get_all_buckets(self,
                        *options,  # type: GetAllBucketOptions
                        **kwargs  # type: Dict[str, Any]
                        ) -> List[BucketSettings]:
        """Returns a list of existing buckets in the cluster.

        Args:
            options (:class:`~couchbase.management.options.GetAllBucketOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            List[:class:`.BucketSettings`]: A list of existing buckets in the cluster.
        """
        return super().get_all_buckets(*options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.BucketMgmt, BucketManagerLogic._ERROR_MAPPING)
    def flush_bucket(self,
                     bucket_name,   # type: str
                     *options,      # type: FlushBucketOptions
                     **kwargs       # type: Dict[str, Any]
                     ) -> None:
        """Flushes the bucket, deleting all the existing data that is stored in it.

        Args:
            bucket_name (str): The name of the bucket to flush.
            options (:class:`~couchbase.management.options.FlushBucketOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.BucketDoesNotExistException`: If the bucket does not exist.
            :class:`~couchbase.exceptions.BucketNotFlushableException`: If the bucket's settings have
                flushing disabled.
        """
        return super().flush_bucket(bucket_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(BucketDescribeResult, ManagementType.BucketMgmt, BucketManagerLogic._ERROR_MAPPING)
    def bucket_describe(self,
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
        return super().bucket_describe(bucket_name, *options, **kwargs)
