#  Copyright 2016-2023. Couchbase, Inc.
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

from typing import (Any,
                    Dict,
                    Union)

from couchbase._utils import is_null_or_empty
from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.transforms import enum_to_str, to_seconds
from couchbase.logic.validation import validate_bool, validate_int
from couchbase.management.logic.bucket_mgmt_types import (BUCKET_MGMT_ERROR_MAP,
                                                          BucketDescribeRequest,
                                                          BucketSettings,
                                                          BucketType,
                                                          CompressionMode,
                                                          ConflictResolutionType,
                                                          CreateBucketRequest,
                                                          CreateBucketSettings,
                                                          DropBucketRequest,
                                                          EvictionPolicyType,
                                                          FlushBucketRequest,
                                                          GetAllBucketsRequest,
                                                          GetBucketRequest,
                                                          StorageBackend,
                                                          UpdateBucketRequest)
from couchbase.options import forward_args


class BucketMgmtRequestBuilder:

    def __init__(self) -> None:
        self._error_map = BUCKET_MGMT_ERROR_MAP

    def _bucket_settings_to_server(self,  # noqa: C901
                                   settings: Union[CreateBucketRequest, BucketSettings]) -> Dict[str, Any]:
        """**INTERNAL**"""
        output = {'name': settings['name']}
        bucket_type = settings.get('bucket_type', None)
        if bucket_type:
            output['bucket_type'] = enum_to_str(bucket_type,
                                                BucketType,
                                                BucketType.to_server_str)
        compression_mode = settings.get('compression_mode', None)
        if compression_mode:
            output['compression_mode'] = enum_to_str(compression_mode, CompressionMode)
        conflict_resolution_type = settings.get('conflict_resolution_type', None)
        if conflict_resolution_type:
            output['conflict_resolution_type'] = enum_to_str(conflict_resolution_type,
                                                             ConflictResolutionType,
                                                             ConflictResolutionType.to_server_str)
        eviction_policy = settings.get('eviction_policy', None)
        if eviction_policy:
            output['eviction_policy'] = enum_to_str(eviction_policy,
                                                    EvictionPolicyType,
                                                    EvictionPolicyType.to_server_str)
        flush_enabled = settings.get('flush_enabled', None)
        if flush_enabled is not None:
            output['flush_enabled'] = flush_enabled
        history_retention_collection_default = settings.get('history_retention_collection_default', None)
        if history_retention_collection_default is not None:
            output['history_retention_collection_default'] = validate_bool(history_retention_collection_default)
        history_retention_bytes = settings.get('history_retention_bytes', None)
        if history_retention_bytes is not None:
            output['history_retention_bytes'] = validate_int(history_retention_bytes)
        history_retention_duration = settings.get('history_retention_duration', None)
        if history_retention_duration is not None:
            output['history_retention_duration'] = to_seconds(history_retention_duration)

        # max_ttl not a thing in C++ core; if provided, convert to max_expiry and send to
        # server, but max_expiry takes precedence
        max_ttl = settings.get('max_ttl', None)
        if max_ttl is not None:
            output['max_expiry'] = validate_int(max_ttl)
        max_expiry = settings.get('max_expiry', None)
        if max_expiry is not None:
            output['max_expiry'] = to_seconds(max_expiry)
        minimum_durability_level = settings.get('minimum_durability_level', None)
        if minimum_durability_level is not None:
            output['minimum_durability_level'] = minimum_durability_level.value

        num_replicas = settings.get('num_replicas', None)
        if num_replicas is not None:
            output['num_replicas'] = validate_int(num_replicas)
        ram_quota_mb = settings.get('ram_quota_mb', None)
        if ram_quota_mb is not None:
            output['ram_quota_mb'] = validate_int(ram_quota_mb)
        replica_index = settings.get('replica_index', None)
        if replica_index is not None:
            output['replica_indexes'] = validate_bool(replica_index)
        storage_backend = settings.get('storage_backend', None)
        if storage_backend:
            output['storage_backend'] = enum_to_str(storage_backend, StorageBackend)
        num_vbuckets = settings.get('num_vbuckets', None)
        if num_vbuckets is not None:
            output['num_vbuckets'] = validate_int(num_vbuckets)

        return output

    def _validate_bucket_name(self, bucket_name: str) -> None:
        if is_null_or_empty(bucket_name):
            raise InvalidArgumentException('The bucket_name cannot be empty.')

        if not isinstance(bucket_name, str):
            raise InvalidArgumentException('The bucket_name must be str.')

    def build_bucket_describe_request(self,
                                      bucket_name: str,
                                      *options: object,
                                      **kwargs: object) -> BucketDescribeRequest:
        self._validate_bucket_name(bucket_name)
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = BucketDescribeRequest(self._error_map, bucket_name)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_create_bucket_request(self,
                                    settings: CreateBucketSettings,
                                    *options: object,
                                    **kwargs: object) -> CreateBucketRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        bucket_settings = self._bucket_settings_to_server(settings)
        req = CreateBucketRequest(self._error_map, bucket_settings)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_drop_bucket_request(self, bucket_name: str, *options: object, **kwargs: object) -> DropBucketRequest:
        self._validate_bucket_name(bucket_name)
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = DropBucketRequest(self._error_map, bucket_name)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_flush_bucket_request(self, bucket_name: str, *options: object, **kwargs: object) -> FlushBucketRequest:
        self._validate_bucket_name(bucket_name)
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = FlushBucketRequest(self._error_map, bucket_name)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_all_buckets_request(self, *options: object, **kwargs: object) -> GetAllBucketsRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = GetAllBucketsRequest(self._error_map)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_bucket_request(self, bucket_name: str, *options: object, **kwargs: object) -> GetBucketRequest:
        self._validate_bucket_name(bucket_name)
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = GetBucketRequest(self._error_map, bucket_name)

        if timeout is not None:
            req.timeout = timeout

        return req

    def build_update_bucket_request(self,
                                    settings: CreateBucketSettings,
                                    *options: object,
                                    **kwargs: object) -> UpdateBucketRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        bucket_settings = self._bucket_settings_to_server(settings)
        req = UpdateBucketRequest(self._error_map, bucket_settings)
        if timeout is not None:
            req.timeout = timeout

        return req
