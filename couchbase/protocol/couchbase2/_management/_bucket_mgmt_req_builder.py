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

"""A bucket manager's requests to the gateway, shared by the sync, asyncio and Twisted impls."""

from __future__ import annotations

from collections.abc import Mapping
from typing import (TYPE_CHECKING,
                    Any,
                    TypeVar)

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._option_args import merge
from couchbase.common.durability import DurabilityLevel
from couchbase.common.exceptions import FeatureUnavailableException, InvalidArgumentException
from couchbase.common.management._bucket_mgmt_args import BucketSettingsFields, settings_fields
from couchbase.common.management._mgmt_args import validate_name
from couchbase.common.management.buckets import BucketType, StorageBackend
from couchbase.protocol.couchbase2._core import timeout_seconds
from couchbase.protocol.couchbase2._generated.admin.bucket.v1 import bucket_pb2
from couchbase.protocol.couchbase2._management._bucket_mgmt_types import (BUCKET_TYPES,
                                                                          COMPRESSION_MODES,
                                                                          CONFLICT_RESOLUTION_TYPES,
                                                                          DURABILITY_LEVELS,
                                                                          EVICTION_POLICIES,
                                                                          STORAGE_BACKENDS,
                                                                          BucketMgmtOptionsBlock,
                                                                          BucketMgmtRequest,
                                                                          GetBucketRequest,
                                                                          UpdateBucketRequest)

if TYPE_CHECKING:
    from enum import Enum

    from couchbase.common._option_diagnostics import OptionsBlock
    from couchbase.common.management.buckets import BucketSettings, CreateBucketSettings
    from couchbase.protocol.couchbase2._core import ClusterCore

_Message = TypeVar('_Message', bucket_pb2.CreateBucketRequest, bucket_pb2.UpdateBucketRequest)
E = TypeVar('E', bound='Enum')
W = TypeVar('W')


class BucketMgmtRequestBuilder:

    def __init__(self, core: ClusterCore) -> None:
        self._core = core

    def _final_args(self,
                    block: OptionsBlock,
                    options: Any,
                    kwargs: dict[str, Any],
                    obs_handler: ObservableRequestHandler | None,
                    bucket_name: str | None = None) -> dict[str, Any]:
        final_args = merge(kwargs, options)
        self._core.validate_options(block, final_args)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        if obs_handler:
            obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name)
        return final_args

    def _timeout(self, final_args: dict[str, Any]) -> float:
        return timeout_seconds(final_args, self._core.management_timeout)

    def build_create_bucket_request(self,
                                    settings: CreateBucketSettings,
                                    obs_handler: ObservableRequestHandler | None,
                                    options: Any = None,
                                    **kwargs: Any) -> BucketMgmtRequest[bucket_pb2.CreateBucketRequest]:
        """A bucket type of memcached and a number of vBuckets have no field in the gateway's request."""
        final_args = self._final_args(BucketMgmtOptionsBlock.CreateBucket,
                                      options,
                                      kwargs,
                                      obs_handler,
                                      settings.get('name') if isinstance(settings, dict) else None)
        fields = settings_fields(settings, create=True)
        if fields.bucket_type == BucketType.MEMCACHED:
            raise FeatureUnavailableException('A memcached bucket cannot be created over the couchbase2 protocol.')
        if fields.num_vbuckets is not None:
            raise FeatureUnavailableException(
                "A bucket's number of vBuckets cannot be set over the couchbase2 protocol.")
        message = _common_fields(bucket_pb2.CreateBucketRequest(bucket_name=fields.name), fields)
        if fields.bucket_type is not None:
            message.bucket_type = _wire(BUCKET_TYPES, fields.bucket_type, 'bucket_type')
        if fields.replica_index is not None:
            message.replica_indexes = fields.replica_index
        if fields.storage_backend not in (None, StorageBackend.UNDEFINED):
            message.storage_backend = _wire(STORAGE_BACKENDS, fields.storage_backend, 'storage_backend')
        if fields.conflict_resolution_type is not None:
            message.conflict_resolution_type = _wire(CONFLICT_RESOLUTION_TYPES,
                                                     fields.conflict_resolution_type,
                                                     'conflict_resolution_type')
        return BucketMgmtRequest(message, self._timeout(final_args), obs_handler=obs_handler)

    def build_update_bucket_request(self,
                                    settings: BucketSettings,
                                    obs_handler: ObservableRequestHandler | None,
                                    options: Any = None,
                                    **kwargs: Any) -> UpdateBucketRequest:
        """The settings fixed when a bucket is created have no field in the gateway's update, and are left out.

        So are a bucket's storage backend, which the server can change from 7.6, and a minimum durability level
        of none, which the gateway cannot spell.  Each is left out rather than refused because the settings
        :meth:`get_bucket` returns carry them, and updating those is the usual way to change one setting.  A
        level of none carries a listing, sent first to check the bucket has no level the update would keep.
        """
        final_args = self._final_args(BucketMgmtOptionsBlock.UpdateBucket,
                                      options,
                                      kwargs,
                                      obs_handler,
                                      settings.get('name') if isinstance(settings, dict) else None)
        fields = settings_fields(settings, create=False)
        message = _common_fields(bucket_pb2.UpdateBucketRequest(bucket_name=fields.name), fields)
        listing = (bucket_pb2.ListBucketsRequest()
                   if fields.minimum_durability_level == DurabilityLevel.NONE else None)
        return UpdateBucketRequest(message, self._timeout(final_args), obs_handler=obs_handler, listing=listing)

    def build_drop_bucket_request(self,
                                  bucket_name: str,
                                  obs_handler: ObservableRequestHandler | None,
                                  options: Any = None,
                                  **kwargs: Any) -> BucketMgmtRequest[bucket_pb2.DeleteBucketRequest]:
        final_args = self._final_args(BucketMgmtOptionsBlock.DropBucket, options, kwargs, obs_handler, bucket_name)
        message = bucket_pb2.DeleteBucketRequest(bucket_name=validate_name(bucket_name, 'bucket_name'))
        return BucketMgmtRequest(message, self._timeout(final_args), obs_handler=obs_handler)

    def build_flush_bucket_request(self,
                                   bucket_name: str,
                                   obs_handler: ObservableRequestHandler | None,
                                   options: Any = None,
                                   **kwargs: Any) -> BucketMgmtRequest[bucket_pb2.FlushBucketRequest]:
        final_args = self._final_args(BucketMgmtOptionsBlock.FlushBucket, options, kwargs, obs_handler, bucket_name)
        message = bucket_pb2.FlushBucketRequest(bucket_name=validate_name(bucket_name, 'bucket_name'))
        return BucketMgmtRequest(message, self._timeout(final_args), obs_handler=obs_handler)

    def build_get_all_buckets_request(self,
                                      obs_handler: ObservableRequestHandler | None,
                                      options: Any = None,
                                      **kwargs: Any) -> BucketMgmtRequest[bucket_pb2.ListBucketsRequest]:
        final_args = self._final_args(BucketMgmtOptionsBlock.GetAllBucket, options, kwargs, obs_handler)
        return BucketMgmtRequest(bucket_pb2.ListBucketsRequest(), self._timeout(final_args), obs_handler=obs_handler)

    def build_get_bucket_request(self,
                                 bucket_name: str,
                                 obs_handler: ObservableRequestHandler | None,
                                 options: Any = None,
                                 **kwargs: Any) -> GetBucketRequest:
        final_args = self._final_args(BucketMgmtOptionsBlock.GetBucket, options, kwargs, obs_handler, bucket_name)
        return GetBucketRequest(bucket_pb2.ListBucketsRequest(),
                                self._timeout(final_args),
                                validate_name(bucket_name, 'bucket_name'),
                                obs_handler=obs_handler)


def _common_fields(message: _Message, fields: BucketSettingsFields) -> _Message:
    """The settings both a create and an update carry."""
    for wire_name, value in (('ram_quota_mb', fields.ram_quota_mb),
                             ('num_replicas', fields.num_replicas),
                             ('flush_enabled', fields.flush_enabled),
                             ('max_expiry_secs', fields.max_expiry),
                             ('history_retention_collection_default', fields.history_retention_collection_default),
                             ('history_retention_bytes', fields.history_retention_bytes),
                             ('history_retention_duration_secs', fields.history_retention_duration)):
        if value is not None:
            setattr(message, wire_name, value)
    if fields.eviction_policy is not None:
        message.eviction_mode = _wire(EVICTION_POLICIES, fields.eviction_policy, 'eviction_policy')
    if fields.compression_mode is not None:
        message.compression_mode = _wire(COMPRESSION_MODES, fields.compression_mode, 'compression_mode')
    if fields.minimum_durability_level not in (None, DurabilityLevel.NONE):
        message.minimum_durability_level = _wire(DURABILITY_LEVELS,
                                                 fields.minimum_durability_level,
                                                 'minimum_durability_level')
    return message


def _wire(members: Mapping[E, W], member: E, setting: str) -> W:
    if member not in members:
        raise InvalidArgumentException(f'{member} is not a {setting} that can be sent.')
    return members[member]
