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
                    Dict,
                    List)

from twisted.internet.defer import Deferred

from couchbase.management.logic.bucket_mgmt_types import BucketSettings, CreateBucketSettings
from txcouchbase.management.logic.bucket_mgmt_impl import TxBucketMgmtImpl

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.management.options import (CreateBucketOptions,
                                              DropBucketOptions,
                                              FlushBucketOptions,
                                              GetAllBucketOptions,
                                              GetBucketOptions,
                                              UpdateBucketOptions)


class BucketManager:
    def __init__(self, client_adapter: AsyncClientAdapter) -> None:
        self._impl = TxBucketMgmtImpl(client_adapter)

    def create_bucket(self,
                      settings,  # type: CreateBucketSettings
                      *options,  # type: CreateBucketOptions
                      **kwargs   # type: Dict[str, Any]
                      ) -> Deferred[None]:
        """Creates a new bucket.

        Args:
            settings (:class:`.CreateBucketSettings`): The settings to use for the new bucket.
            options (:class:`~couchbase.management.options.CreateBucketOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            `Deferred`: An empty `Deferred` instance.

        Raises:
            :class:`~couchbase.exceptions.BucketAlreadyExistsException`: If the bucket already exists.
            :class:`~couchbase.exceptions.InvalidArgumentsException`: If an invalid type or value is provided for the
                settings argument.
        """
        req = self._impl.request_builder.build_create_bucket_request(settings, *options, **kwargs)
        return self._impl.create_bucket_deferred(req)

    def update_bucket(self,
                      settings,  # type: BucketSettings
                      *options,  # type: UpdateBucketOptions
                      **kwargs  # type: Dict[str, Any]
                      ) -> Deferred[None]:
        """Update the settings for an existing bucket.

        Args:
            settings (:class:`.BucketSettings`): The settings to use for the new bucket.
            options (:class:`~couchbase.management.options.UpdateBucketOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            `Deferred`: An empty `Deferred` instance.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentsException`: If an invalid type or value is provided for the
                settings argument.
        """
        req = self._impl.request_builder.build_update_bucket_request(settings, *options, **kwargs)
        return self._impl.update_bucket_deferred(req)

    def drop_bucket(self,
                    bucket_name,  # type: str
                    *options,     # type: DropBucketOptions
                    **kwargs      # type: Dict[str, Any]
                    ) -> Deferred[None]:
        """Drops an existing bucket.

        Args:
            bucket_name (str): The name of the bucket to drop.
            options (:class:`~couchbase.management.options.DropBucketOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            `Deferred`: An empty `Deferred` instance.

        Raises:
            :class:`~couchbase.exceptions.BucketDoesNotExistException`: If the bucket does not exist.
        """
        req = self._impl.request_builder.build_drop_bucket_request(bucket_name, *options, **kwargs)
        return self._impl.drop_bucket_deferred(req)

    def get_bucket(self,
                   bucket_name,   # type: str
                   *options,      # type: GetBucketOptions
                   **kwargs       # type: Dict[str, Any]
                   ) -> Deferred[BucketSettings]:
        """Fetches the settings in use for a specified bucket.

        Args:
            bucket_name (str): The name of the bucket to fetch.
            options (:class:`~couchbase.management.options.GetBucketOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Deferred[:class:`.BucketSettings`]: The settings of the specified bucket.

        Raises:
            :class:`~couchbase.exceptions.BucketDoesNotExistException`: If the bucket does not exist.
        """
        req = self._impl.request_builder.build_get_bucket_request(bucket_name, *options, **kwargs)
        return self._impl.get_bucket_deferred(req)

    def get_all_buckets(self,
                        *options,  # type: GetAllBucketOptions
                        **kwargs  # type: Dict[str, Any]
                        ) -> Deferred[List[BucketSettings]]:
        """Returns a list of existing buckets in the cluster.

        Args:
            options (:class:`~couchbase.management.options.GetAllBucketOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Deferred[List[:class:`.BucketSettings`]]: A list of existing buckets in the cluster.
        """
        req = self._impl.request_builder.build_get_all_buckets_request(*options, **kwargs)
        return self._impl.get_all_buckets_deferred(req)

    def flush_bucket(self,
                     bucket_name,   # type: str
                     *options,      # type: FlushBucketOptions
                     **kwargs       # type: Dict[str, Any]
                     ) -> Deferred[None]:
        """Flushes the bucket, deleting all the existing data that is stored in it.

        Args:
            bucket_name (str): The name of the bucket to flush.
            options (:class:`~couchbase.management.options.FlushBucketOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            `Deferred`: An empty `Deferred` instance.

        Raises:
            :class:`~couchbase.exceptions.BucketDoesNotExistException`: If the bucket does not exist.
            :class:`~couchbase.exceptions.BucketNotFlushableException`: If the bucket's settings have
                flushing disabled.
        """
        req = self._impl.request_builder.build_flush_bucket_request(bucket_name, *options, **kwargs)
        return self._impl.flush_bucket_deferred(req)
