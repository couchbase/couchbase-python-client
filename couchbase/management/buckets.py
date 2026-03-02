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

from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import BucketMgmtOperationType
from couchbase.management.logic.bucket_mgmt_impl import BucketMgmtImpl
from couchbase.management.logic.bucket_mgmt_types import BucketType  # noqa: F401
from couchbase.management.logic.bucket_mgmt_types import CompressionMode  # noqa: F401
from couchbase.management.logic.bucket_mgmt_types import ConflictResolutionType  # noqa: F401
from couchbase.management.logic.bucket_mgmt_types import EjectionMethod  # noqa: F401
from couchbase.management.logic.bucket_mgmt_types import EvictionPolicyType  # noqa: F401
from couchbase.management.logic.bucket_mgmt_types import StorageBackend  # noqa: F401
from couchbase.management.logic.bucket_mgmt_types import (BucketDescribeResult,
                                                          BucketSettings,
                                                          CreateBucketSettings)

# @TODO:  lets deprecate import of options from couchbase.management.buckets
from couchbase.management.options import (BucketDescribeOptions,
                                          CreateBucketOptions,
                                          DropBucketOptions,
                                          FlushBucketOptions,
                                          GetAllBucketOptions,
                                          GetBucketOptions,
                                          UpdateBucketOptions)

if TYPE_CHECKING:
    from couchbase.logic.client_adapter import ClientAdapter
    from couchbase.logic.observability import ObservabilityInstruments


class BucketManager:

    def __init__(self, client_adapter: ClientAdapter, observability_instruments: ObservabilityInstruments) -> None:
        self._impl = BucketMgmtImpl(client_adapter, observability_instruments)

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
        op_type = BucketMgmtOperationType.BucketCreate
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_create_bucket_request(settings, obs_handler, *options, **kwargs)
            self._impl.create_bucket(req, obs_handler)

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
        op_type = BucketMgmtOperationType.BucketUpdate
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_update_bucket_request(settings, obs_handler, *options, **kwargs)
            self._impl.update_bucket(req, obs_handler)

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
        op_type = BucketMgmtOperationType.BucketDrop
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_drop_bucket_request(bucket_name, obs_handler, *options, **kwargs)
            self._impl.drop_bucket(req, obs_handler)

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
        op_type = BucketMgmtOperationType.BucketGet
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_bucket_request(bucket_name, obs_handler, *options, **kwargs)
            return self._impl.get_bucket(req, obs_handler)

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
        op_type = BucketMgmtOperationType.BucketGetAll
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_all_buckets_request(obs_handler, *options, **kwargs)
            return self._impl.get_all_buckets(req, obs_handler)

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
        op_type = BucketMgmtOperationType.BucketFlush
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_flush_bucket_request(bucket_name, obs_handler, *options, **kwargs)
            self._impl.flush_bucket(req, obs_handler)

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
        op_type = BucketMgmtOperationType.BucketDescribe
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_bucket_describe_request(bucket_name, obs_handler, *options, **kwargs)
            return self._impl.bucket_describe(req, obs_handler)
