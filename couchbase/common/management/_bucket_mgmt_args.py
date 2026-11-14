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

"""A bucket's settings as the caller gave them, checked and read the same way whichever protocol sends them.

Classic's builder does not use this yet: its wire passes an enum given as one of its value strings
through as given, which reading it into its member here would change.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any, TypeVar

from couchbase.common._transforms import (to_seconds,
                                          validate_bool,
                                          validate_int)
from couchbase.common.durability import DurabilityLevel
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.management._mgmt_args import validate_name
from couchbase.common.management.buckets import (BucketSettings,
                                                 BucketType,
                                                 CompressionMode,
                                                 ConflictResolutionType,
                                                 EvictionPolicyType,
                                                 StorageBackend)

E = TypeVar('E', bound=Enum)
T = TypeVar('T')


@dataclass(frozen=True)
class BucketSettingsFields:
    """The settings, each ``None`` when not given.  Durations are whole seconds."""

    name: str
    bucket_type: BucketType | None = None
    compression_mode: CompressionMode | None = None
    conflict_resolution_type: ConflictResolutionType | None = None
    eviction_policy: EvictionPolicyType | None = None
    flush_enabled: bool | None = None
    history_retention_collection_default: bool | None = None
    history_retention_bytes: int | None = None
    history_retention_duration: int | None = None
    max_expiry: int | None = None
    minimum_durability_level: DurabilityLevel | None = None
    num_replicas: int | None = None
    num_vbuckets: int | None = None
    ram_quota_mb: int | None = None
    replica_index: bool | None = None
    storage_backend: StorageBackend | None = None


def settings_fields(settings: BucketSettings, *, create: bool) -> BucketSettingsFields:
    """``conflict_resolution_type`` is read only for a create, since it is fixed once the bucket exists.

    ``max_ttl`` is the deprecated spelling of ``max_expiry``, which wins when both are given.
    """
    if not isinstance(settings, BucketSettings):
        raise InvalidArgumentException('Expected the settings to be BucketSettings.')
    max_expiry = settings.get('max_expiry')
    if max_expiry is not None:
        max_expiry = to_seconds(max_expiry)
    elif settings.get('max_ttl') is not None:
        max_expiry = validate_int(settings['max_ttl'])
    history_retention_duration = settings.get('history_retention_duration')
    return BucketSettingsFields(
        name=validate_name(settings.get('name'), 'bucket name'),
        bucket_type=_member(settings.get('bucket_type'), BucketType),
        compression_mode=_member(settings.get('compression_mode'), CompressionMode),
        conflict_resolution_type=(_member(settings.get('conflict_resolution_type'), ConflictResolutionType)
                                  if create else None),
        eviction_policy=_member(settings.get('eviction_policy'), EvictionPolicyType),
        flush_enabled=_optional(settings.get('flush_enabled'), validate_bool),
        history_retention_collection_default=_optional(settings.get('history_retention_collection_default'),
                                                       validate_bool),
        history_retention_bytes=_optional(settings.get('history_retention_bytes'), validate_int),
        history_retention_duration=(to_seconds(history_retention_duration)
                                    if history_retention_duration is not None else None),
        max_expiry=max_expiry,
        minimum_durability_level=_member(settings.get('minimum_durability_level'), DurabilityLevel),
        num_replicas=_optional(settings.get('num_replicas'), validate_int),
        num_vbuckets=_optional(settings.get('num_vbuckets'), validate_int),
        ram_quota_mb=_optional(settings.get('ram_quota_mb'), validate_int),
        replica_index=_optional(settings.get('replica_index'), validate_bool),
        storage_backend=_member(settings.get('storage_backend'), StorageBackend),
    )


def _optional(value: object, validate: Callable[[Any], T]) -> T | None:
    return None if value is None else validate(value)


def _member(value: object, enum: type[E]) -> E | None:
    """``value`` as a member of ``enum``, which it may also name by its value."""
    if value is None or isinstance(value, enum):
        return value
    if isinstance(value, str):
        try:
            return enum(value)
        except ValueError:
            pass
    raise InvalidArgumentException(f'Argument must be of type {enum} but got {value}')
