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

from dataclasses import dataclass, fields
from datetime import timedelta
from enum import Enum
from typing import (Any,
                    Callable,
                    Dict,
                    List,
                    Optional,
                    Union,
                    overload)

from couchbase._utils import is_null_or_empty
from couchbase.durability import DurabilityLevel
from couchbase.exceptions import (BucketAlreadyExistsException,
                                  BucketDoesNotExistException,
                                  BucketNotFlushableException,
                                  FeatureUnavailableException,
                                  InvalidArgumentException,
                                  RateLimitedException)
from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import BucketMgmtOperationType
from couchbase.logic.transforms import (int_to_enum,
                                        seconds_to_timedelta,
                                        str_to_enum)
from couchbase.management.logic.mgmt_req import MgmtRequest


class EvictionPolicyType(Enum):
    NOT_RECENTLY_USED = "nruEviction"
    NO_EVICTION = "noEviction"
    FULL = "fullEviction"
    VALUE_ONLY = "valueOnly"

    @classmethod
    def from_server_str(cls, value):
        if value == 'not_recently_used':
            return cls.NOT_RECENTLY_USED
        elif value == 'no_eviction':
            return cls.NO_EVICTION
        elif value == 'value_only':
            return cls.VALUE_ONLY
        elif value == 'full':
            return cls.FULL
        else:
            return cls.UNKNOWN

    @classmethod
    def to_server_str(cls, value):
        if value == cls.NOT_RECENTLY_USED:
            return 'not_recently_used'
        elif value == cls.NO_EVICTION:
            return 'no_eviction'
        elif value == cls.VALUE_ONLY:
            return 'value_only'
        elif value == cls.FULL:
            return 'full'
        else:
            return 'unknown'


class EjectionMethod(Enum):
    FULL_EVICTION = "fullEviction"
    VALUE_ONLY = "valueOnly"


class BucketType(Enum):
    COUCHBASE = "membase"
    MEMCACHED = "memcached"
    EPHEMERAL = "ephemeral"
    UNKNOWN = "unknown"

    @classmethod
    def from_server_str(cls, value):
        if value == 'couchbase':
            return cls.COUCHBASE
        elif value == 'membase':
            return cls.COUCHBASE
        elif value == 'memcached':
            return cls.MEMCACHED
        elif value == 'ephemeral':
            return cls.EPHEMERAL
        else:
            return cls.UNKNOWN

    @classmethod
    def to_server_str(cls, value):
        if value == cls.COUCHBASE:
            return 'couchbase'
        elif value == cls.MEMCACHED:
            return 'memcached'
        elif value == cls.EPHEMERAL:
            return 'ephemeral'
        else:
            return 'unknown'


class CompressionMode(Enum):
    OFF = "off"
    PASSIVE = "passive"
    ACTIVE = "active"
    UNKNOWN = "unknown"


class ConflictResolutionType(Enum):
    """
    Specifies the conflict resolution type to use for the bucket.

    Members:
    TIMESTAMP: specifies to use timestamp conflict resolution
    SEQUENCE_NUMBER: specifies to use sequence number conflict resolution
    CUSTOM: specifies to use a custom conflict resolution

    """
    TIMESTAMP = "lww"
    SEQUENCE_NUMBER = "seqno"
    CUSTOM = "custom"
    UNKNOWN = "unknown"

    @classmethod
    def from_server_str(cls, value):
        if value == 'timestamp':
            return cls.TIMESTAMP
        elif value == 'sequence_number':
            return cls.SEQUENCE_NUMBER
        elif value == 'custom':
            return cls.CUSTOM
        else:
            return cls.UNKNOWN

    @classmethod
    def to_server_str(cls, value):
        if value == cls.TIMESTAMP:
            return 'timestamp'
        elif value == cls.SEQUENCE_NUMBER:
            return 'sequence_number'
        elif value == cls.CUSTOM:
            return 'custom'
        else:
            return 'unknown'


class StorageBackend(Enum):
    """
    Specifies the storage type to use for the bucket.
    """
    UNDEFINED = "undefined"
    COUCHSTORE = "couchstore"
    MAGMA = "magma"


class BucketSettings(dict):
    @overload
    def __init__(self,
                 name: str,
                 bucket_type: Optional[BucketType] = None,
                 compression_mode: Optional[CompressionMode] = None,
                 eviction_policy: Optional[EvictionPolicyType] = None,
                 flush_enabled: Optional[bool] = None,
                 history_retention_collection_default: Optional[bool] = None,
                 history_retention_bytes: Optional[int] = None,
                 history_retention_duration: Optional[timedelta] = None,
                 max_expiry: Optional[Union[timedelta, float, int]] = None,
                 max_ttl: Optional[Union[timedelta, float, int]] = None,
                 minimum_durability_level: Optional[DurabilityLevel] = None,
                 num_replicas: Optional[int] = None,
                 ram_quota_mb: Optional[int] = None,
                 replica_index: Optional[bool] = None,
                 storage_backend: Optional[StorageBackend] = None,
                 num_vbuckets: Optional[int] = None,
                 ) -> None:
        pass

    def __init__(self, **kwargs: object) -> None:
        """BucketSettings provides a means of mapping bucket settings into an object.
        """
        bucket_name = kwargs.get('name', None)
        if is_null_or_empty(bucket_name):
            raise InvalidArgumentException('Must provide a non-empty bucket name.')
        if kwargs.get('bucket_type', None) == 'couchbase':
            kwargs['bucket_type'] = BucketType.COUCHBASE
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @property
    def bucket_type(self) -> Optional[BucketType]:
        """BucketType {couchbase (sent on wire as membase), memcached, ephemeral}
        The type of the bucket. Default to couchbase."""
        return self.get('bucket_type', None)

    @property
    def compression_mode(self) -> Optional[CompressionMode]:
        """{off | passive | active} - The compression mode to use."""
        return self.get('compression_mode', None)

    @property
    def eviction_policy(self) -> Optional[EvictionPolicyType]:
        """{fullEviction | valueOnly}. The eviction policy to use."""
        return self.get('eviction_policy', None)

    @property
    def flush_enabled(self) -> Optional[bool]:
        """True if flush enabled on bucket, False otherwise"""
        return self.get('flush_enabled', None)

    @property
    def history_retention_collection_default(self) -> Optional[bool]:
        """
        Whether history retention on collections is enabled by default
        """
        return self.get('history_retention_collection_default', None)

    @property
    def history_retention_bytes(self) -> Optional[int]:
        """
        The maximum size, in bytes, of the change history that is written to disk for all collections in this bucket
        """
        return self.get('history_retention_bytes', None)

    @property
    def history_retention_duration(self) -> Optional[timedelta]:
        """
        The maximum duration to be covered by the change history that is written to disk for all collections in this
        bucket
        """
        return self.get('history_retention_duration', None)

    @property
    def max_expiry(self) -> Optional[Union[timedelta, float, int]]:
        """
           Value for the max expiry of new documents created without an expiry.
        """
        return self.get('max_expiry', None)

    @property
    def max_ttl(self) -> Optional[Union[timedelta, float, int]]:
        """
         **DEPRECATED** use max_expiry
            Value for the maxTTL of new documents created without a ttl.
        """
        return self.get('max_expiry', None)

    @property
    def minimum_durability_level(self) -> Optional[DurabilityLevel]:
        """The durability level to use for the bucket."""
        return self.get('minimum_durability_level', None)

    @property
    def name(self) -> str:
        """ Bucket name"""
        return self.get('name')

    @property
    def num_replicas(self) -> Optional[int]:
        """NumReplicas (int) - The number of replicas for documents."""
        return self.get('num_replicas', None)

    @property
    def num_vbuckets(self) -> Optional[int]:
        """num_vbuckets (int) - The number of vbuckets for the bucket."""
        return self.get('num_vbuckets', None)

    @property
    def ram_quota_mb(self) -> Optional[int]:
        """ram_quota_mb (int) - The RAM quota for the bucket."""
        return self.get('ram_quota_mb', None)

    @property
    def replica_index(self) -> Optional[bool]:
        """ Whether replica indexes should be enabled for the bucket."""
        return self.get('replica_index', None)

    @property
    def storage_backend(self) -> Optional[StorageBackend]:
        """
        {couchstore | magma | undefined} - The storage backend to use.
        """
        return self.get('storage_backend', None)

    @classmethod
    def bucket_settings_from_server(cls, settings: Dict[str, Any]) -> BucketSettings:  # noqa: C901
        """**INTERNAL**"""
        output = dict()
        bucket_type = settings.get('bucket_type', None)
        if bucket_type:
            output['bucket_type'] = str_to_enum(bucket_type,
                                                BucketType,
                                                BucketType.from_server_str)
        compression_mode = settings.get('compression_mode', None)
        if compression_mode:
            output['compression_mode'] = str_to_enum(compression_mode,
                                                     CompressionMode)
        conflict_resolution_type = settings.get('conflict_resolution_type', None)
        if conflict_resolution_type:
            output['conflict_resolution_type'] = str_to_enum(conflict_resolution_type,
                                                             ConflictResolutionType,
                                                             ConflictResolutionType.from_server_str)
        eviction_policy = settings.get('eviction_policy', None)
        if eviction_policy:
            output['eviction_policy'] = str_to_enum(eviction_policy,
                                                    EvictionPolicyType,
                                                    EvictionPolicyType.from_server_str)
        output['flush_enabled'] = settings.get('flush_enabled', None)
        history_retention_collection_default = settings.get('history_retention_collection_default', None)
        if history_retention_collection_default is not None:
            output['history_retention_collection_default'] = history_retention_collection_default
        history_retention_bytes = settings.get('history_retention_bytes', None)
        if history_retention_bytes is not None:
            output['history_retention_bytes'] = history_retention_bytes
        history_retention_duration = settings.get('history_retention_duration', None)
        if history_retention_duration is not None:
            output['history_retention_duration'] = seconds_to_timedelta(history_retention_duration)
        # maxTTL not a thing in C++ core; only when writing to server will we send max_expiry with the maxTTL value
        max_expiry = settings.get('max_expiry', None)
        if max_expiry is not None:
            output['max_expiry'] = seconds_to_timedelta(max_expiry)
        minimum_durability_level = settings.get('minimum_durability_level', None)
        if minimum_durability_level is not None:
            output['minimum_durability_level'] = int_to_enum(minimum_durability_level, DurabilityLevel)
        output['name'] = settings.get('name', None)
        num_replicas = settings.get('num_replicas', None)
        if num_replicas is not None:
            output['num_replicas'] = num_replicas
        num_vbuckets = settings.get('num_vbuckets', None)
        if num_vbuckets is not None:
            output['num_vbuckets'] = num_vbuckets
        ram_quota_mb = settings.get('ram_quota_mb', None)
        if ram_quota_mb is not None:
            output['ram_quota_mb'] = ram_quota_mb
        # replica_indexes in C++ core struct
        replica_index = settings.get('replica_indexes', None)
        if replica_index is not None:
            output['replica_index'] = replica_index
        storage_backend = settings.get('storage_backend', None)
        if storage_backend:
            if storage_backend == 'unknown':
                storage_backend = 'undefined'
            output['storage_backend'] = str_to_enum(storage_backend, StorageBackend)

        return cls(**output)

    def __repr__(self):
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self):
        return self.__repr__()


class CreateBucketSettings(BucketSettings):
    @overload
    def __init__(self,
                 name: str,
                 bucket_type: Optional[BucketType] = None,
                 compression_mode: Optional[CompressionMode] = None,
                 conflict_resolution_type: Optional[ConflictResolutionType] = None,
                 eviction_policy: Optional[EvictionPolicyType] = None,
                 flush_enabled: Optional[bool] = None,
                 history_retention_collection_default: Optional[bool] = None,
                 history_retention_bytes: Optional[int] = None,
                 history_retention_duration: Optional[timedelta] = None,
                 max_expiry: Optional[Union[timedelta, float, int]] = None,
                 max_ttl: Optional[Union[timedelta, float, int]] = None,
                 minimum_durability_level: Optional[DurabilityLevel] = None,
                 num_replicas: Optional[int] = None,
                 ram_quota_mb: Optional[int] = None,
                 replica_index: Optional[bool] = None,
                 storage_backend: Optional[StorageBackend] = None,
                 num_vbuckets: Optional[int] = None,
                 ) -> None:
        pass

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)

    @property
    def conflict_resolution_type(self) -> Optional[ConflictResolutionType]:
        return self.get('conflict_resolution_type', None)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()


@dataclass
class BucketDescribeResult:
    name: str = None
    uuid: str = None
    number_of_nodes: int = None
    number_of_replicas: int = None
    bucket_capabilities: List[str] = None
    storage_backend: str = None
    server_groups: Dict[str, Any] = None


# we have these params on the top-level pycbc_core request
OPARG_SKIP_LIST = ['error_map']


@dataclass
class BucketMgmtRequest(MgmtRequest):

    def req_to_dict(self,
                    obs_handler: Optional[ObservableRequestHandler] = None,
                    callback: Optional[Callable[..., None]] = None,
                    errback: Optional[Callable[..., None]] = None) -> Dict[str, Any]:

        mgmt_kwargs = {
            field.name: getattr(self, field.name)
            for field in fields(self)
            if field.name not in OPARG_SKIP_LIST and getattr(self, field.name) is not None
        }

        if callback is not None:
            mgmt_kwargs['callback'] = callback

        if errback is not None:
            mgmt_kwargs['errback'] = errback

        if obs_handler:
            # TODO(PYCBC-1746): Update once legacy tracing logic is removed
            if obs_handler.is_legacy_tracer:
                legacy_request_span = obs_handler.legacy_request_span
                if legacy_request_span:
                    mgmt_kwargs['parent_span'] = legacy_request_span
            else:
                mgmt_kwargs['wrapper_span_name'] = obs_handler.wrapper_span_name

        return mgmt_kwargs


@dataclass
class BucketDescribeRequest(BucketMgmtRequest):
    name: str
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return BucketMgmtOperationType.BucketDescribe.value


@dataclass
class CreateBucketRequest(BucketMgmtRequest):
    bucket: Dict[str, Any]
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return BucketMgmtOperationType.BucketCreate.value


@dataclass
class DropBucketRequest(BucketMgmtRequest):
    name: str
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return BucketMgmtOperationType.BucketDrop.value


@dataclass
class FlushBucketRequest(BucketMgmtRequest):
    name: str
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return BucketMgmtOperationType.BucketFlush.value


@dataclass
class GetAllBucketsRequest(BucketMgmtRequest):
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return BucketMgmtOperationType.BucketGetAll.value


@dataclass
class GetBucketRequest(BucketMgmtRequest):
    name: str
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return BucketMgmtOperationType.BucketGet.value


@dataclass
class UpdateBucketRequest(BucketMgmtRequest):
    bucket: Dict[str, Any]
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return BucketMgmtOperationType.BucketUpdate.value


BUCKET_MGMT_ERROR_MAP: Dict[str, Exception] = {
    'Bucket with given name (already|still) exists': BucketAlreadyExistsException,
    'Requested resource not found': BucketDoesNotExistException,
    r'.*non existent bucket.*': BucketDoesNotExistException,
    r'.*Flush is disabled for the bucket.*': BucketNotFlushableException,
    r'.*Limit\(s\) exceeded\s+\[.*\].*': RateLimitedException,
    r'.*is supported only with developer preview enabled.*': FeatureUnavailableException}
