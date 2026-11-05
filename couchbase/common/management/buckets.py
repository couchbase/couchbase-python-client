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

__all__ = [
    'BucketDescribeResult',
    'BucketSettings',
    'BucketSettingsKwargs',
    'BucketType',
    'CompressionMode',
    'ConflictResolutionType',
    'CreateBucketSettings',
    'CreateBucketSettingsKwargs',
    'EjectionMethod',
    'EvictionPolicyType',
    'StorageBackend',
]

from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import (TYPE_CHECKING,
                    Any,
                    TypedDict)

from couchbase.common._supportability import Supportability
from couchbase.common._transforms import seconds_to_timedelta
from couchbase.common._utils import fields_repr, is_null_or_empty
from couchbase.common.durability import DurabilityLevel
from couchbase.common.exceptions import InvalidArgumentException

if TYPE_CHECKING:
    from typing_extensions import Required, Unpack


class EvictionPolicyType(Enum):
    """How a bucket removes data from memory when it reaches its RAM quota.

    ``FULL`` and ``VALUE_ONLY`` apply to a Couchbase bucket, which keeps its data on disk: ``FULL`` removes a whole
    document, while ``VALUE_ONLY`` keeps its key and metadata in memory. ``NOT_RECENTLY_USED`` and ``NO_EVICTION`` apply
    to an ephemeral bucket, which has nowhere else to keep it: ``NOT_RECENTLY_USED`` removes the documents least
    recently used, while ``NO_EVICTION`` refuses new writes instead.
    """

    NOT_RECENTLY_USED = "nruEviction"
    NO_EVICTION = "noEviction"
    FULL = "fullEviction"
    VALUE_ONLY = "valueOnly"
    UNKNOWN = "unknown"

    @classmethod
    def from_server_str(cls, value: object) -> EvictionPolicyType:
        """**INTERNAL** The member for the core's name of a policy, or ``UNKNOWN`` for a name it does not know."""
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
    def to_server_str(cls, value: EvictionPolicyType) -> str:
        """**INTERNAL** The core's name of a policy."""
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
    """How a Couchbase bucket removes data from memory: the whole document, or only its value."""

    FULL_EVICTION = "fullEviction"
    VALUE_ONLY = "valueOnly"


class BucketType(Enum):
    """The type of a bucket.

    ``COUCHBASE`` keeps its data in memory and on disk, ``EPHEMERAL`` only in memory, and ``MEMCACHED`` is a memory-only
    cache.
    """

    COUCHBASE = "membase"
    MEMCACHED = "memcached"
    EPHEMERAL = "ephemeral"
    UNKNOWN = "unknown"

    @classmethod
    def from_server_str(cls, value: object) -> BucketType:
        """**INTERNAL** The member for the core's name of a type, or ``UNKNOWN`` for a name it does not know."""
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
    def to_server_str(cls, value: BucketType) -> str:
        """**INTERNAL** The core's name of a type."""
        if value == cls.COUCHBASE:
            return 'couchbase'
        elif value == cls.MEMCACHED:
            return 'memcached'
        elif value == cls.EPHEMERAL:
            return 'ephemeral'
        else:
            return 'unknown'


class CompressionMode(Enum):
    """How a bucket compresses the documents it holds.

    ``OFF`` stores documents as they arrive. ``PASSIVE`` keeps a document compressed when a client sends it compressed,
    and ``ACTIVE`` also compresses documents itself.
    """

    OFF = "off"
    PASSIVE = "passive"
    ACTIVE = "active"
    UNKNOWN = "unknown"

    @classmethod
    def from_server_str(cls, value: object) -> CompressionMode:
        """**INTERNAL** The member for the core's name of a mode, or ``UNKNOWN`` for a name it does not know."""
        try:
            return cls(value)
        except ValueError:
            return cls.UNKNOWN


class ConflictResolutionType(Enum):
    """How a bucket resolves a conflict between two versions of a document under XDCR.

    ``TIMESTAMP`` keeps the version written last, ``SEQUENCE_NUMBER`` the version changed most often, and ``CUSTOM``
    leaves the conflict to the application.
    """
    TIMESTAMP = "lww"
    SEQUENCE_NUMBER = "seqno"
    CUSTOM = "custom"
    UNKNOWN = "unknown"

    @classmethod
    def from_server_str(cls, value: object) -> ConflictResolutionType:
        """**INTERNAL** The member for the core's name of a type, or ``UNKNOWN`` for a name it does not know."""
        if value == 'timestamp':
            return cls.TIMESTAMP
        elif value == 'sequence_number':
            return cls.SEQUENCE_NUMBER
        elif value == 'custom':
            return cls.CUSTOM
        else:
            return cls.UNKNOWN

    @classmethod
    def to_server_str(cls, value: ConflictResolutionType) -> str:
        """**INTERNAL** The core's name of a type."""
        if value == cls.TIMESTAMP:
            return 'timestamp'
        elif value == cls.SEQUENCE_NUMBER:
            return 'sequence_number'
        elif value == cls.CUSTOM:
            return 'custom'
        else:
            return 'unknown'


class StorageBackend(Enum):
    """The storage engine a bucket keeps its data in."""
    UNDEFINED = "undefined"
    COUCHSTORE = "couchstore"
    MAGMA = "magma"

    @classmethod
    def from_server_str(cls, value: object) -> StorageBackend:
        """**INTERNAL** The member for the core's name of an engine, or ``UNDEFINED`` for a name it does not know."""
        try:
            return cls(value)
        except ValueError:
            return cls.UNDEFINED


class BucketSettingsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`.BucketSettings`."""

    name: Required[str]
    bucket_type: BucketType | None
    compression_mode: CompressionMode | None
    eviction_policy: EvictionPolicyType | None
    flush_enabled: bool | None
    history_retention_collection_default: bool | None
    history_retention_bytes: int | None
    history_retention_duration: timedelta | None
    max_expiry: timedelta | float | int | None
    max_ttl: timedelta | float | int | None
    minimum_durability_level: DurabilityLevel | None
    num_replicas: int | None
    ram_quota_mb: int | None
    replica_index: bool | None
    storage_backend: StorageBackend | None
    num_vbuckets: int | None


class BucketSettings(dict):
    """The settings of an existing bucket.

    :meth:`.BucketManager.get_bucket` returns them and :meth:`.BucketManager.update_bucket` takes them. The settings are
    a dict, and a setting given as ``None`` is left out of it, so each property reads one key, or ``None`` if it is
    absent.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If ``name`` is missing or empty.
    """

    def __init__(self, **kwargs: Unpack[BucketSettingsKwargs]) -> None:
        super().__init__()
        self._fill(dict(kwargs))

    def _fill(self, settings: dict[str, Any]) -> None:
        # Shared with CreateBucketSettings, whose conflict_resolution_type this constructor does not take.
        name = settings.get('name', None)
        if name is not None and not isinstance(name, str):
            raise InvalidArgumentException('The bucket name must be str.')
        if is_null_or_empty(name):
            raise InvalidArgumentException('Must provide a non-empty bucket name.')
        if settings.get('bucket_type', None) == 'couchbase':
            settings['bucket_type'] = BucketType.COUCHBASE
        self.update((k, v) for k, v in settings.items() if v is not None)

    def __repr__(self) -> str:
        return fields_repr(self, **self)

    @property
    def bucket_type(self) -> BucketType | None:
        """The type of the bucket."""
        return self.get('bucket_type', None)

    @property
    def compression_mode(self) -> CompressionMode | None:
        """How the bucket compresses the documents it holds."""
        return self.get('compression_mode', None)

    @property
    def conflict_resolution_type(self) -> ConflictResolutionType | None:
        """How the bucket resolves a conflict between two versions of a document under XDCR.

        It is fixed when the bucket is created, so it is sent to the server only from a :class:`.CreateBucketSettings`;
        :meth:`.BucketManager.update_bucket` never sends it.
        """
        return self.get('conflict_resolution_type', None)

    @property
    def eviction_policy(self) -> EvictionPolicyType | None:
        """How the bucket removes data from memory when it reaches its RAM quota."""
        return self.get('eviction_policy', None)

    @property
    def flush_enabled(self) -> bool | None:
        """Whether the bucket can be flushed."""
        return self.get('flush_enabled', None)

    @property
    def history_retention_collection_default(self) -> bool | None:
        """Whether the bucket's collections keep a change history unless a collection is set otherwise."""
        return self.get('history_retention_collection_default', None)

    @property
    def history_retention_bytes(self) -> int | None:
        """The largest the change history on disk can grow, in bytes, across all the bucket's collections."""
        return self.get('history_retention_bytes', None)

    @property
    def history_retention_duration(self) -> timedelta | None:
        """The longest span of time the change history on disk covers, across all the bucket's collections."""
        return self.get('history_retention_duration', None)

    @property
    def max_expiry(self) -> timedelta | float | int | None:
        """The longest expiry a document in the bucket can have, which a document written without one is given."""
        return self.get('max_expiry', None)

    @property
    def max_ttl(self) -> timedelta | float | int | None:
        """The longest expiry a document in the bucket can have, which a document written without one is given.

        .. deprecated:: 4.0.0
            Use :attr:`max_expiry` instead.
        """
        Supportability.class_property_deprecated('BucketSettings.max_ttl', 'BucketSettings.max_expiry')
        return self.get('max_expiry', None)

    @property
    def minimum_durability_level(self) -> DurabilityLevel | None:
        """The durability level the bucket applies to a mutation that asks for a lower one."""
        return self.get('minimum_durability_level', None)

    @property
    def name(self) -> str:
        """The name of the bucket."""
        name: str = self['name']
        return name

    @property
    def num_replicas(self) -> int | None:
        """The number of replicas the bucket keeps of each document."""
        return self.get('num_replicas', None)

    @property
    def num_vbuckets(self) -> int | None:
        """The number of vBuckets the bucket is split into."""
        return self.get('num_vbuckets', None)

    @property
    def ram_quota_mb(self) -> int | None:
        """The bucket's RAM quota on each node, in megabytes."""
        return self.get('ram_quota_mb', None)

    @property
    def replica_index(self) -> bool | None:
        """Whether the bucket's view indexes are replicated."""
        return self.get('replica_index', None)

    @property
    def storage_backend(self) -> StorageBackend | None:
        """The storage engine the bucket keeps its data in."""
        return self.get('storage_backend', None)

    @classmethod
    def bucket_settings_from_server(cls, settings: dict[str, Any]) -> BucketSettings:  # noqa: C901
        """**INTERNAL**."""
        output: dict[str, Any] = {}
        bucket_type = settings.get('bucket_type', None)
        if bucket_type:
            output['bucket_type'] = BucketType.from_server_str(bucket_type)
        compression_mode = settings.get('compression_mode', None)
        if compression_mode:
            output['compression_mode'] = CompressionMode.from_server_str(compression_mode)
        conflict_resolution_type = settings.get('conflict_resolution_type', None)
        if conflict_resolution_type:
            output['conflict_resolution_type'] = ConflictResolutionType.from_server_str(conflict_resolution_type)
        eviction_policy = settings.get('eviction_policy', None)
        if eviction_policy:
            output['eviction_policy'] = EvictionPolicyType.from_server_str(eviction_policy)
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
            output['minimum_durability_level'] = DurabilityLevel(minimum_durability_level)
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
            output['storage_backend'] = StorageBackend.from_server_str(storage_backend)

        return cls(**output)


class CreateBucketSettingsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`.CreateBucketSettings`."""

    name: Required[str]
    bucket_type: BucketType | None
    compression_mode: CompressionMode | None
    conflict_resolution_type: ConflictResolutionType | None
    eviction_policy: EvictionPolicyType | None
    flush_enabled: bool | None
    history_retention_collection_default: bool | None
    history_retention_bytes: int | None
    history_retention_duration: timedelta | None
    max_expiry: timedelta | float | int | None
    max_ttl: timedelta | float | int | None
    minimum_durability_level: DurabilityLevel | None
    num_replicas: int | None
    ram_quota_mb: int | None
    replica_index: bool | None
    storage_backend: StorageBackend | None
    num_vbuckets: int | None


class CreateBucketSettings(BucketSettings):
    """The settings of a bucket for :meth:`.BucketManager.create_bucket` to create.

    They are :class:`.BucketSettings` that can also set ``conflict_resolution_type``, which is fixed once the bucket
    exists.
    """

    def __init__(self, **kwargs: Unpack[CreateBucketSettingsKwargs]) -> None:
        self._fill(dict(kwargs))


@dataclass
class BucketDescribeResult:
    """A description of a bucket and where its data is, from :meth:`.BucketManager.bucket_describe`.

    Attributes:
        name (str | None): The name of the bucket.
        uuid (str | None): The bucket's UUID, which changes when a bucket is dropped and created again.
        number_of_nodes (int | None): The number of nodes the bucket's data is on.
        number_of_replicas (int | None): The number of replicas the bucket keeps of each document.
        bucket_capabilities (list[str] | None): The capabilities the bucket supports.
        storage_backend (str | None): The storage engine the bucket keeps its data in.
        server_groups (dict[str, Any] | None): The server groups the bucket's nodes are in.
    """

    name: str | None = None
    uuid: str | None = None
    number_of_nodes: int | None = None
    number_of_replicas: int | None = None
    bucket_capabilities: list[str] | None = None
    storage_backend: str | None = None
    server_groups: dict[str, Any] | None = None
