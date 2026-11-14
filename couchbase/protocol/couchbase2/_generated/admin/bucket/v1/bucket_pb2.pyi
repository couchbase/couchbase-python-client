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

# !! AUTO-GENERATED from tools/autogen/config/protos.yaml: DO NOT EDIT MANUALLY !!
# Regenerate with `python -m tools.autogen protos generate --write`.

from couchbase.protocol.couchbase2._generated.kv.v1 import kv_pb2 as _kv_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BucketType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    BUCKET_TYPE_COUCHBASE: _ClassVar[BucketType]
    BUCKET_TYPE_EPHEMERAL: _ClassVar[BucketType]

class EvictionMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EVICTION_MODE_FULL: _ClassVar[EvictionMode]
    EVICTION_MODE_NOT_RECENTLY_USED: _ClassVar[EvictionMode]
    EVICTION_MODE_VALUE_ONLY: _ClassVar[EvictionMode]
    EVICTION_MODE_NONE: _ClassVar[EvictionMode]

class CompressionMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COMPRESSION_MODE_OFF: _ClassVar[CompressionMode]
    COMPRESSION_MODE_PASSIVE: _ClassVar[CompressionMode]
    COMPRESSION_MODE_ACTIVE: _ClassVar[CompressionMode]

class StorageBackend(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STORAGE_BACKEND_COUCHSTORE: _ClassVar[StorageBackend]
    STORAGE_BACKEND_MAGMA: _ClassVar[StorageBackend]

class ConflictResolutionType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONFLICT_RESOLUTION_TYPE_TIMESTAMP: _ClassVar[ConflictResolutionType]
    CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER: _ClassVar[ConflictResolutionType]
    CONFLICT_RESOLUTION_TYPE_CUSTOM: _ClassVar[ConflictResolutionType]
BUCKET_TYPE_COUCHBASE: BucketType
BUCKET_TYPE_EPHEMERAL: BucketType
EVICTION_MODE_FULL: EvictionMode
EVICTION_MODE_NOT_RECENTLY_USED: EvictionMode
EVICTION_MODE_VALUE_ONLY: EvictionMode
EVICTION_MODE_NONE: EvictionMode
COMPRESSION_MODE_OFF: CompressionMode
COMPRESSION_MODE_PASSIVE: CompressionMode
COMPRESSION_MODE_ACTIVE: CompressionMode
STORAGE_BACKEND_COUCHSTORE: StorageBackend
STORAGE_BACKEND_MAGMA: StorageBackend
CONFLICT_RESOLUTION_TYPE_TIMESTAMP: ConflictResolutionType
CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER: ConflictResolutionType
CONFLICT_RESOLUTION_TYPE_CUSTOM: ConflictResolutionType

class ListBucketsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListBucketsResponse(_message.Message):
    __slots__ = ("buckets",)
    class Bucket(_message.Message):
        __slots__ = ("bucket_name", "flush_enabled", "ram_quota_mb", "num_replicas", "replica_indexes", "bucket_type", "eviction_mode", "max_expiry_secs", "compression_mode", "minimum_durability_level", "storage_backend", "conflict_resolution_type", "history_retention_collection_default", "history_retention_bytes", "history_retention_duration_secs")
        BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
        FLUSH_ENABLED_FIELD_NUMBER: _ClassVar[int]
        RAM_QUOTA_MB_FIELD_NUMBER: _ClassVar[int]
        NUM_REPLICAS_FIELD_NUMBER: _ClassVar[int]
        REPLICA_INDEXES_FIELD_NUMBER: _ClassVar[int]
        BUCKET_TYPE_FIELD_NUMBER: _ClassVar[int]
        EVICTION_MODE_FIELD_NUMBER: _ClassVar[int]
        MAX_EXPIRY_SECS_FIELD_NUMBER: _ClassVar[int]
        COMPRESSION_MODE_FIELD_NUMBER: _ClassVar[int]
        MINIMUM_DURABILITY_LEVEL_FIELD_NUMBER: _ClassVar[int]
        STORAGE_BACKEND_FIELD_NUMBER: _ClassVar[int]
        CONFLICT_RESOLUTION_TYPE_FIELD_NUMBER: _ClassVar[int]
        HISTORY_RETENTION_COLLECTION_DEFAULT_FIELD_NUMBER: _ClassVar[int]
        HISTORY_RETENTION_BYTES_FIELD_NUMBER: _ClassVar[int]
        HISTORY_RETENTION_DURATION_SECS_FIELD_NUMBER: _ClassVar[int]
        bucket_name: str
        flush_enabled: bool
        ram_quota_mb: int
        num_replicas: int
        replica_indexes: bool
        bucket_type: BucketType
        eviction_mode: EvictionMode
        max_expiry_secs: int
        compression_mode: CompressionMode
        minimum_durability_level: _kv_pb2.DurabilityLevel
        storage_backend: StorageBackend
        conflict_resolution_type: ConflictResolutionType
        history_retention_collection_default: bool
        history_retention_bytes: int
        history_retention_duration_secs: int
        def __init__(self, bucket_name: _Optional[str] = ..., flush_enabled: bool = ..., ram_quota_mb: _Optional[int] = ..., num_replicas: _Optional[int] = ..., replica_indexes: bool = ..., bucket_type: _Optional[_Union[BucketType, str]] = ..., eviction_mode: _Optional[_Union[EvictionMode, str]] = ..., max_expiry_secs: _Optional[int] = ..., compression_mode: _Optional[_Union[CompressionMode, str]] = ..., minimum_durability_level: _Optional[_Union[_kv_pb2.DurabilityLevel, str]] = ..., storage_backend: _Optional[_Union[StorageBackend, str]] = ..., conflict_resolution_type: _Optional[_Union[ConflictResolutionType, str]] = ..., history_retention_collection_default: bool = ..., history_retention_bytes: _Optional[int] = ..., history_retention_duration_secs: _Optional[int] = ...) -> None: ...
    BUCKETS_FIELD_NUMBER: _ClassVar[int]
    buckets: _containers.RepeatedCompositeFieldContainer[ListBucketsResponse.Bucket]
    def __init__(self, buckets: _Optional[_Iterable[_Union[ListBucketsResponse.Bucket, _Mapping]]] = ...) -> None: ...

class CreateBucketRequest(_message.Message):
    __slots__ = ("bucket_name", "bucket_type", "ram_quota_mb", "num_replicas", "flush_enabled", "replica_indexes", "eviction_mode", "max_expiry_secs", "compression_mode", "minimum_durability_level", "storage_backend", "conflict_resolution_type", "history_retention_collection_default", "history_retention_bytes", "history_retention_duration_secs")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    BUCKET_TYPE_FIELD_NUMBER: _ClassVar[int]
    RAM_QUOTA_MB_FIELD_NUMBER: _ClassVar[int]
    NUM_REPLICAS_FIELD_NUMBER: _ClassVar[int]
    FLUSH_ENABLED_FIELD_NUMBER: _ClassVar[int]
    REPLICA_INDEXES_FIELD_NUMBER: _ClassVar[int]
    EVICTION_MODE_FIELD_NUMBER: _ClassVar[int]
    MAX_EXPIRY_SECS_FIELD_NUMBER: _ClassVar[int]
    COMPRESSION_MODE_FIELD_NUMBER: _ClassVar[int]
    MINIMUM_DURABILITY_LEVEL_FIELD_NUMBER: _ClassVar[int]
    STORAGE_BACKEND_FIELD_NUMBER: _ClassVar[int]
    CONFLICT_RESOLUTION_TYPE_FIELD_NUMBER: _ClassVar[int]
    HISTORY_RETENTION_COLLECTION_DEFAULT_FIELD_NUMBER: _ClassVar[int]
    HISTORY_RETENTION_BYTES_FIELD_NUMBER: _ClassVar[int]
    HISTORY_RETENTION_DURATION_SECS_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    bucket_type: BucketType
    ram_quota_mb: int
    num_replicas: int
    flush_enabled: bool
    replica_indexes: bool
    eviction_mode: EvictionMode
    max_expiry_secs: int
    compression_mode: CompressionMode
    minimum_durability_level: _kv_pb2.DurabilityLevel
    storage_backend: StorageBackend
    conflict_resolution_type: ConflictResolutionType
    history_retention_collection_default: bool
    history_retention_bytes: int
    history_retention_duration_secs: int
    def __init__(self, bucket_name: _Optional[str] = ..., bucket_type: _Optional[_Union[BucketType, str]] = ..., ram_quota_mb: _Optional[int] = ..., num_replicas: _Optional[int] = ..., flush_enabled: bool = ..., replica_indexes: bool = ..., eviction_mode: _Optional[_Union[EvictionMode, str]] = ..., max_expiry_secs: _Optional[int] = ..., compression_mode: _Optional[_Union[CompressionMode, str]] = ..., minimum_durability_level: _Optional[_Union[_kv_pb2.DurabilityLevel, str]] = ..., storage_backend: _Optional[_Union[StorageBackend, str]] = ..., conflict_resolution_type: _Optional[_Union[ConflictResolutionType, str]] = ..., history_retention_collection_default: bool = ..., history_retention_bytes: _Optional[int] = ..., history_retention_duration_secs: _Optional[int] = ...) -> None: ...

class CreateBucketResponse(_message.Message):
    __slots__ = ("bucket_uuid",)
    BUCKET_UUID_FIELD_NUMBER: _ClassVar[int]
    bucket_uuid: str
    def __init__(self, bucket_uuid: _Optional[str] = ...) -> None: ...

class UpdateBucketRequest(_message.Message):
    __slots__ = ("bucket_name", "ram_quota_mb", "num_replicas", "flush_enabled", "eviction_mode", "max_expiry_secs", "compression_mode", "minimum_durability_level", "history_retention_collection_default", "history_retention_bytes", "history_retention_duration_secs")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    RAM_QUOTA_MB_FIELD_NUMBER: _ClassVar[int]
    NUM_REPLICAS_FIELD_NUMBER: _ClassVar[int]
    FLUSH_ENABLED_FIELD_NUMBER: _ClassVar[int]
    EVICTION_MODE_FIELD_NUMBER: _ClassVar[int]
    MAX_EXPIRY_SECS_FIELD_NUMBER: _ClassVar[int]
    COMPRESSION_MODE_FIELD_NUMBER: _ClassVar[int]
    MINIMUM_DURABILITY_LEVEL_FIELD_NUMBER: _ClassVar[int]
    HISTORY_RETENTION_COLLECTION_DEFAULT_FIELD_NUMBER: _ClassVar[int]
    HISTORY_RETENTION_BYTES_FIELD_NUMBER: _ClassVar[int]
    HISTORY_RETENTION_DURATION_SECS_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    ram_quota_mb: int
    num_replicas: int
    flush_enabled: bool
    eviction_mode: EvictionMode
    max_expiry_secs: int
    compression_mode: CompressionMode
    minimum_durability_level: _kv_pb2.DurabilityLevel
    history_retention_collection_default: bool
    history_retention_bytes: int
    history_retention_duration_secs: int
    def __init__(self, bucket_name: _Optional[str] = ..., ram_quota_mb: _Optional[int] = ..., num_replicas: _Optional[int] = ..., flush_enabled: bool = ..., eviction_mode: _Optional[_Union[EvictionMode, str]] = ..., max_expiry_secs: _Optional[int] = ..., compression_mode: _Optional[_Union[CompressionMode, str]] = ..., minimum_durability_level: _Optional[_Union[_kv_pb2.DurabilityLevel, str]] = ..., history_retention_collection_default: bool = ..., history_retention_bytes: _Optional[int] = ..., history_retention_duration_secs: _Optional[int] = ...) -> None: ...

class UpdateBucketResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DeleteBucketRequest(_message.Message):
    __slots__ = ("bucket_name",)
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    def __init__(self, bucket_name: _Optional[str] = ...) -> None: ...

class DeleteBucketResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class FlushBucketRequest(_message.Message):
    __slots__ = ("bucket_name",)
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    def __init__(self, bucket_name: _Optional[str] = ...) -> None: ...

class FlushBucketResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
