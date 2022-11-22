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

from typing import (TYPE_CHECKING,
                    Any,
                    Awaitable,
                    Dict,
                    List)

from acouchbase.management.logic.wrappers import AsyncMgmtWrapper
from couchbase.management.logic import ManagementType
from couchbase.management.logic.buckets_logic import (BucketDescribeResult,
                                                      BucketManagerLogic,
                                                      BucketSettings,
                                                      CreateBucketSettings)

if TYPE_CHECKING:
    from couchbase.management.options import (BucketDescribeOptions,
                                              CreateBucketOptions,
                                              DropBucketOptions,
                                              FlushBucketOptions,
                                              GetAllBucketOptions,
                                              GetBucketOptions,
                                              UpdateBucketOptions)


class BucketManager(BucketManagerLogic):
    def __init__(self, connection, loop):
        super().__init__(connection)
        self._loop = loop

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._loop

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.BucketMgmt, BucketManagerLogic._ERROR_MAPPING)
    def create_bucket(self,
                      settings,  # type: CreateBucketSettings
                      *options,  # type: CreateBucketOptions
                      **kwargs   # type: Any
                      ) -> Awaitable[None]:
        """
        Creates a new bucket.

        :param: CreateBucketSettings settings: settings for the bucket.
        :param: CreateBucketOptions options: options for setting the bucket.
        :param: Any kwargs: override corresponding values in the options.

        :raises: BucketAlreadyExistsException
        :raises: InvalidArgumentsException
        """
        super().create_bucket(settings, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.BucketMgmt, BucketManagerLogic._ERROR_MAPPING)
    def update_bucket(self,
                      settings,  # type: BucketSettings
                      *options,  # type: UpdateBucketOptions
                      **kwargs  # type: Dict[str, Any]
                      ) -> Awaitable[None]:

        super().update_bucket(settings, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.BucketMgmt, BucketManagerLogic._ERROR_MAPPING)
    def drop_bucket(self,
                    bucket_name,  # type: str
                    *options,     # type: DropBucketOptions
                    **kwargs      # type: Dict[str, Any]
                    ) -> Awaitable[None]:

        super().drop_bucket(bucket_name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(BucketSettings, ManagementType.BucketMgmt, BucketManagerLogic._ERROR_MAPPING)
    def get_bucket(self,
                   bucket_name,   # type: str
                   *options,      # type: GetBucketOptions
                   **kwargs       # type: Dict[str, Any]
                   ) -> Awaitable[BucketSettings]:

        super().get_bucket(bucket_name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(BucketSettings, ManagementType.BucketMgmt, BucketManagerLogic._ERROR_MAPPING)
    def get_all_buckets(self,
                        *options,  # type: GetAllBucketOptions
                        **kwargs  # type: Dict[str, Any]
                        ) -> Awaitable[List[BucketSettings]]:

        super().get_all_buckets(*options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.BucketMgmt, BucketManagerLogic._ERROR_MAPPING)
    def flush_bucket(self,
                     bucket_name,   # type: str
                     *options,      # type: FlushBucketOptions
                     **kwargs       # type: Dict[str, Any]
                     ) -> Awaitable[None]:

        super().flush_bucket(bucket_name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(BucketDescribeResult,
                                       ManagementType.BucketMgmt,
                                       BucketManagerLogic._ERROR_MAPPING)
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
        super().bucket_describe(bucket_name, *options, **kwargs)
