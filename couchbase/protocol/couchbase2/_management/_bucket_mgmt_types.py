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

"""The requests a bucket manager's builder returns, the gateway's spelling of each setting, and the listing read back.

The gateway has no request for one bucket, so ``get_bucket`` reads it from the listing.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import (TYPE_CHECKING,
                    Any,
                    Generic,
                    TypeVar)

from couchbase.common._option_diagnostics import OptionsBlock
from couchbase.common.durability import DurabilityLevel
from couchbase.common.exceptions import (BucketDoesNotExistException,
                                         BucketNotFoundException,
                                         FeatureUnavailableException)
from couchbase.common.management.buckets import (BucketSettings,
                                                 BucketType,
                                                 CompressionMode,
                                                 ConflictResolutionType,
                                                 EvictionPolicyType,
                                                 StorageBackend)
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.protocol.couchbase2._generated.admin.bucket.v1 import bucket_pb2
from couchbase.protocol.couchbase2._generated.kv.v1 import kv_pb2
from couchbase.protocol.couchbase2._management._mgmt_errors import unavailable_when

if TYPE_CHECKING:
    from couchbase.common._observability import ObservableRequestHandler

M = TypeVar('M')
E = TypeVar('E', bound=Enum)
U = TypeVar('U')

BUCKET_TYPES = {
    BucketType.COUCHBASE: bucket_pb2.BUCKET_TYPE_COUCHBASE,
    BucketType.EPHEMERAL: bucket_pb2.BUCKET_TYPE_EPHEMERAL,
}
COMPRESSION_MODES = {
    CompressionMode.OFF: bucket_pb2.COMPRESSION_MODE_OFF,
    CompressionMode.PASSIVE: bucket_pb2.COMPRESSION_MODE_PASSIVE,
    CompressionMode.ACTIVE: bucket_pb2.COMPRESSION_MODE_ACTIVE,
}
CONFLICT_RESOLUTION_TYPES = {
    ConflictResolutionType.TIMESTAMP: bucket_pb2.CONFLICT_RESOLUTION_TYPE_TIMESTAMP,
    ConflictResolutionType.SEQUENCE_NUMBER: bucket_pb2.CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER,
    ConflictResolutionType.CUSTOM: bucket_pb2.CONFLICT_RESOLUTION_TYPE_CUSTOM,
}
EVICTION_POLICIES = {
    EvictionPolicyType.FULL: bucket_pb2.EVICTION_MODE_FULL,
    EvictionPolicyType.NOT_RECENTLY_USED: bucket_pb2.EVICTION_MODE_NOT_RECENTLY_USED,
    EvictionPolicyType.VALUE_ONLY: bucket_pb2.EVICTION_MODE_VALUE_ONLY,
    EvictionPolicyType.NO_EVICTION: bucket_pb2.EVICTION_MODE_NONE,
}
# The gateway has no level for none: a bucket without a minimum leaves the field out.
DURABILITY_LEVELS = {
    DurabilityLevel.MAJORITY: kv_pb2.DURABILITY_LEVEL_MAJORITY,
    DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE: kv_pb2.DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE,
    DurabilityLevel.PERSIST_TO_MAJORITY: kv_pb2.DURABILITY_LEVEL_PERSIST_TO_MAJORITY,
}
STORAGE_BACKENDS = {
    StorageBackend.COUCHSTORE: bucket_pb2.STORAGE_BACKEND_COUCHSTORE,
    StorageBackend.MAGMA: bucket_pb2.STORAGE_BACKEND_MAGMA,
}


class BucketMgmtOptionsBlock(OptionsBlock, protocol=CouchbaseProtocol.COUCHBASE2):
    BucketDescribe = 'BucketDescribeOptions'
    CreateBucket = 'CreateBucketOptions'
    DropBucket = 'DropBucketOptions'
    FlushBucket = 'FlushBucketOptions'
    GetAllBucket = 'GetAllBucketOptions'
    GetBucket = 'GetBucketOptions'
    UpdateBucket = 'UpdateBucketOptions'


@dataclass(frozen=True)
class BucketMgmtRequest(Generic[M]):
    message: M
    timeout: float
    obs_handler: ObservableRequestHandler | None


@dataclass(frozen=True)
class UpdateBucketRequest:
    message: bucket_pb2.UpdateBucketRequest
    timeout: float
    obs_handler: ObservableRequestHandler | None
    # Sent first when the update asks for no minimum durability level, to check there is none to clear.
    listing: bucket_pb2.ListBucketsRequest | None = None


@dataclass(frozen=True)
class GetBucketRequest:
    message: bucket_pb2.ListBucketsRequest
    timeout: float
    bucket_name: str
    obs_handler: ObservableRequestHandler | None


@contextmanager
def bucket_mgmt_errors() -> Iterator[None]:
    """The exceptions a bucket manager documents, for the gateway's.

    A missing bucket is :class:`BucketDoesNotExistException`.  A setting the server takes only with
    developer preview enabled comes back as an invalid argument, and is the feature being unavailable.
    """
    try:
        with unavailable_when('is supported only with developer preview enabled'):
            yield
    except BucketNotFoundException as e:
        raise BucketDoesNotExistException(message=e.message, context=e.error_context) from e


def check_no_durability_to_clear(response: bucket_pb2.ListBucketsResponse, bucket_name: str) -> None:
    """Refuses an update asking for no minimum durability level of a bucket that has one.

    The gateway has no level for none and keeps the bucket's level when the update leaves it out
    (ING-2862), so the update would silently leave it in place.  A bucket with no level, or one not
    listed, is left for the update itself to answer.
    """
    for bucket in response.buckets:
        if bucket.bucket_name == bucket_name and bucket.HasField('minimum_durability_level'):
            raise FeatureUnavailableException(
                "Clearing a bucket's minimum durability level is not available over the couchbase2 protocol: "
                'the gateway keeps the current level.')


def bucket_settings(response: bucket_pb2.ListBucketsResponse) -> list[BucketSettings]:
    return [_bucket_settings(b) for b in response.buckets]


def named_bucket_settings(response: bucket_pb2.ListBucketsResponse, bucket_name: str) -> BucketSettings:
    for bucket in response.buckets:
        if bucket.bucket_name == bucket_name:
            return _bucket_settings(bucket)
    raise BucketDoesNotExistException(message=f'Bucket {bucket_name!r} does not exist.')


def _bucket_settings(bucket: bucket_pb2.ListBucketsResponse.Bucket) -> BucketSettings:
    settings: dict[str, Any] = {
        'name': bucket.bucket_name,
        'bucket_type': _member(BUCKET_TYPES, bucket.bucket_type, BucketType.UNKNOWN),
        'compression_mode': _member(COMPRESSION_MODES, bucket.compression_mode, CompressionMode.UNKNOWN),
        'conflict_resolution_type': _member(CONFLICT_RESOLUTION_TYPES,
                                            bucket.conflict_resolution_type,
                                            ConflictResolutionType.UNKNOWN),
        'eviction_policy': _member(EVICTION_POLICIES, bucket.eviction_mode, EvictionPolicyType.UNKNOWN),
        'flush_enabled': bucket.flush_enabled,
        'max_expiry': timedelta(seconds=bucket.max_expiry_secs),
        'minimum_durability_level': (_member(DURABILITY_LEVELS, bucket.minimum_durability_level, None)
                                     if bucket.HasField('minimum_durability_level') else DurabilityLevel.NONE),
        'num_replicas': bucket.num_replicas,
        'ram_quota_mb': bucket.ram_quota_mb,
        'replica_index': bucket.replica_indexes,
        'storage_backend': (_member(STORAGE_BACKENDS, bucket.storage_backend, StorageBackend.UNDEFINED)
                            if bucket.HasField('storage_backend') else StorageBackend.UNDEFINED),
    }
    if bucket.HasField('history_retention_collection_default'):
        settings['history_retention_collection_default'] = bucket.history_retention_collection_default
    if bucket.HasField('history_retention_bytes'):
        settings['history_retention_bytes'] = bucket.history_retention_bytes
    if bucket.HasField('history_retention_duration_secs'):
        settings['history_retention_duration'] = timedelta(seconds=bucket.history_retention_duration_secs)
    return BucketSettings(**settings)


def _member(members: Mapping[E, int], wire: int, unknown: U) -> E | U:
    """The SDK's member for a value the gateway sent, or ``unknown`` for one newer than the SDK."""
    return next((member for member, value in members.items() if value == wire), unknown)
