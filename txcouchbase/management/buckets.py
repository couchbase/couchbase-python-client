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
                    List)

from twisted.internet.defer import Deferred

from couchbase.management.logic.buckets_logic import (BucketManagerLogic,
                                                      BucketSettings,
                                                      CreateBucketSettings)

if TYPE_CHECKING:
    from couchbase.management.options import (CreateBucketOptions,
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

    def create_bucket(self,
                      settings,  # type: CreateBucketSettings
                      *options,  # type: CreateBucketOptions
                      **kwargs   # type: Any
                      ) -> Deferred[None]:
        """
        Creates a new bucket.

        :param: CreateBucketSettings settings: settings for the bucket.
        :param: CreateBucketOptions options: options for setting the bucket.
        :param: Any kwargs: override corresponding values in the options.

        :raises: BucketAlreadyExistsException
        :raises: InvalidArgumentsException
        """
        return Deferred.fromFuture(super().create_bucket(settings, *options, **kwargs))

    def update_bucket(self,
                      settings,  # type: BucketSettings
                      *options,  # type: UpdateBucketOptions
                      **kwargs  # type: Any
                      ) -> Deferred[None]:

        return Deferred.fromFuture(super().update_bucket(settings, *options, **kwargs))

    def drop_bucket(self,
                    bucket_name,  # type: str
                    *options,     # type: DropBucketOptions
                    **kwargs      # type: Any
                    ) -> Deferred[None]:

        return Deferred.fromFuture(super().drop_bucket(bucket_name, *options, **kwargs))

    def get_bucket(self,
                   bucket_name,   # type: str
                   *options,      # type: GetBucketOptions
                   **kwargs       # type: Any
                   ) -> Deferred[BucketSettings]:

        return Deferred.fromFuture(super().get_bucket(bucket_name, *options, **kwargs))

    def get_all_buckets(self,
                        *options,  # type: GetAllBucketOptions
                        **kwargs  # type: Any
                        ) -> Deferred[List[BucketSettings]]:

        return Deferred.fromFuture(super().get_all_buckets(*options, **kwargs))

    def flush_bucket(self,
                     bucket_name,   # type: str
                     *options,      # type: FlushBucketOptions
                     **kwargs       # type: Any
                     ) -> Deferred[None]:

        return Deferred.fromFuture(super().flush_bucket(bucket_name, *options, **kwargs))
