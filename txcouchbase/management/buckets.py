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

from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import BucketMgmtOperationType
from couchbase.management.logic.bucket_mgmt_types import (BucketDescribeResult,
                                                          BucketSettings,
                                                          CreateBucketSettings)
from txcouchbase.management.logic.bucket_mgmt_impl import TxBucketMgmtImpl

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
        self._impl = TxBucketMgmtImpl(client_adapter, observability_instruments)

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
        op_type = BucketMgmtOperationType.BucketCreate
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_create_bucket_request(settings, obs_handler, *options, **kwargs)
            d = self._impl.create_bucket_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

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
        op_type = BucketMgmtOperationType.BucketUpdate
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_update_bucket_request(settings, obs_handler, *options, **kwargs)
            d = self._impl.update_bucket_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def drop_bucket(self,
                    bucket_name,  # type: str
                    *options,     # type: DropBucketOptions
                    **kwargs      # type: Dict[str, Any]
                    ) -> Deferred[None]:
        """Drops an existing bucket.

        Args:
            bucket_name(str): The name of the bucket to drop.
            options(: class: `~couchbase.management.options.DropBucketOptions`): Optional parameters for this
            operation.
            **kwargs(Dict[str, Any]): keyword arguments that can be used as optional parameters
            for this operation.

        Returns:
            `Deferred`: An empty `Deferred` instance.

        Raises:
            : class: `~couchbase.exceptions.BucketDoesNotExistException`: If the bucket does not exist.
        """
        op_type = BucketMgmtOperationType.BucketDrop
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_drop_bucket_request(bucket_name, obs_handler, *options, **kwargs)
            d = self._impl.drop_bucket_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_bucket(self,
                   bucket_name,   # type: str
                   *options,      # type: GetBucketOptions
                   **kwargs       # type: Dict[str, Any]
                   ) -> Deferred[BucketSettings]:
        """Fetches the settings in use for a specified bucket.

        Args:
            bucket_name(str): The name of the bucket to fetch.
            options(: class: `~couchbase.management.options.GetBucketOptions`): Optional parameters for this
            operation.
            **kwargs(Dict[str, Any]): keyword arguments that can be used as optional parameters
            for this operation.

        Returns:
            Deferred[:class:`.BucketSettings`]: The settings of the specified bucket.

        Raises:
            : class: `~couchbase.exceptions.BucketDoesNotExistException`: If the bucket does not exist.
        """
        op_type = BucketMgmtOperationType.BucketGet
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_bucket_request(bucket_name, obs_handler, *options, **kwargs)
            d = self._impl.get_bucket_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_all_buckets(self,
                        *options,  # type: GetAllBucketOptions
                        **kwargs  # type: Dict[str, Any]
                        ) -> Deferred[List[BucketSettings]]:
        """Returns a list of existing buckets in the cluster.

        Args:
            options(: class: `~couchbase.management.options.GetAllBucketOptions`): Optional parameters for this
            operation.
            **kwargs(Dict[str, Any]): keyword arguments that can be used as optional parameters
            for this operation.

        Returns:
            Deferred[List[:class:`.BucketSettings`]]: A list of existing buckets in the cluster.
        """
        op_type = BucketMgmtOperationType.BucketGetAll
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_all_buckets_request(obs_handler, *options, **kwargs)
            d = self._impl.get_all_buckets_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def flush_bucket(self,
                     bucket_name,   # type: str
                     *options,      # type: FlushBucketOptions
                     **kwargs       # type: Dict[str, Any]
                     ) -> Deferred[None]:
        """Flushes the bucket, deleting all the existing data that is stored in it.

        Args:
            bucket_name(str): The name of the bucket to flush.
            options(: class: `~couchbase.management.options.FlushBucketOptions`): Optional parameters for this
            operation.
            **kwargs(Dict[str, Any]): keyword arguments that can be used as optional parameters
            for this operation.

        Returns:
            `Deferred`: An empty `Deferred` instance.

        Raises:
            : class: `~couchbase.exceptions.BucketDoesNotExistException`: If the bucket does not exist.
            : class: `~couchbase.exceptions.BucketNotFlushableException`: If the bucket's settings have
            flushing disabled.
        """
        op_type = BucketMgmtOperationType.BucketFlush
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_flush_bucket_request(bucket_name, obs_handler, *options, **kwargs)
            d = self._impl.flush_bucket_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def bucket_describe(self,
                        bucket_name,   # type: str
                        *options,      # type: BucketDescribeOptions
                        **kwargs       # type: Dict[str, Any]
                        ) -> Deferred[BucketDescribeResult]:
        """Provides details on provided the bucket.

        Args:
            bucket_name(str): The name of the bucket to describe.
            options(: class: `~couchbase.management.options.BucketDescribeOptions`): Optional parameters for this
            operation.
            **kwargs(Dict[str, Any]): keyword arguments that can be used as optional parameters
            for this operation.

        Returns:
            Deferred[:class:`.BucketDescribeResult`]: Key-value pair details describing the bucket.

        Raises:
            : class: `~couchbase.exceptions.BucketDoesNotExistException`: If the bucket does not exist.
        """
        op_type = BucketMgmtOperationType.BucketDescribe
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_bucket_describe_request(bucket_name, obs_handler, *options, **kwargs)
            d = self._impl.bucket_describe_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise
