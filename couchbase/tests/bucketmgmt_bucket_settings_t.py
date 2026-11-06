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

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from couchbase.common.management.buckets import (BucketSettings,
                                                 BucketType,
                                                 CompressionMode,
                                                 ConflictResolutionType,
                                                 CreateBucketSettings,
                                                 EvictionPolicyType,
                                                 StorageBackend)
from couchbase.common.warnings import CouchbaseDeprecationWarning
from couchbase.durability import DurabilityLevel
from couchbase.exceptions import InvalidArgumentException


def _optional_property_names() -> list[str]:
    # Read off the class rather than enumerated, so a property added later is asserted
    # without anyone remembering to extend a list.  `CreateBucketSettings` adds no property
    # of its own, so one list serves both.  `max_ttl` is left out because it warns on read; it
    # is covered by LegacyBucketSettingsUnitTests.
    return sorted(name for name in dir(BucketSettings)
                  if name not in ('name', 'max_ttl') and isinstance(getattr(BucketSettings, name, None), property))


class BucketSettingsUnitTests:
    # No cb_env and no TEST_MANIFEST: every test either builds a `BucketSettings` and reads it
    # back, or feeds a server-shaped dict to `BucketSettings.bucket_settings_from_server`.  What
    # classic's request builder makes of one is in bucket_settings_req_builder_t.  The
    # deprecated `max_ttl` alias is covered by LegacyBucketSettingsUnitTests below.

    @staticmethod
    def _assert_optional_fields_none(settings: BucketSettings, exclude: list[str] | None = None) -> None:
        excluded = exclude or []
        for field in _optional_property_names():
            if field in excluded:
                continue
            value = getattr(settings, field)
            assert value is None, f'{field} should be None but got {value}'

    # --- Minimal object state ---

    def test_bucket_settings_minimal_state(self) -> None:
        settings = BucketSettings(name='test-bucket')

        assert len(settings) == 1
        assert settings['name'] == 'test-bucket'
        assert settings.name == 'test-bucket'
        self._assert_optional_fields_none(settings)

    def test_create_bucket_settings_minimal_state(self) -> None:
        settings = CreateBucketSettings(name='test-bucket')

        assert len(settings) == 1
        assert settings['name'] == 'test-bucket'
        assert settings.name == 'test-bucket'
        self._assert_optional_fields_none(settings)

    # --- Zero and False are values, not absences ---

    def test_bucket_settings_num_replicas_zero(self) -> None:
        settings = BucketSettings(name='test', num_replicas=0)

        assert 'num_replicas' in settings
        assert settings['num_replicas'] == 0
        assert settings.num_replicas == 0
        assert len(settings) == 2

    def test_bucket_settings_flush_enabled_false(self) -> None:
        settings = BucketSettings(name='test', flush_enabled=False)

        assert 'flush_enabled' in settings
        assert settings['flush_enabled'] is False
        assert settings.flush_enabled is False
        assert len(settings) == 2

    def test_bucket_settings_replica_index_false(self) -> None:
        settings = BucketSettings(name='test', replica_index=False)

        assert 'replica_index' in settings
        assert settings['replica_index'] is False
        assert settings.replica_index is False
        assert len(settings) == 2

    def test_bucket_settings_history_retention_zeros(self) -> None:
        settings = BucketSettings(name='test',
                                  history_retention_collection_default=False,
                                  history_retention_bytes=0,
                                  history_retention_duration=timedelta(0))

        assert 'history_retention_collection_default' in settings
        assert 'history_retention_bytes' in settings
        assert 'history_retention_duration' in settings
        assert settings.history_retention_collection_default is False
        assert settings.history_retention_bytes == 0
        assert settings.history_retention_duration == timedelta(0)
        assert len(settings) == 4

    # --- Translation from the server payload ---

    def test_minimal_settings_from_server(self) -> None:
        settings = BucketSettings.bucket_settings_from_server({'name': 'test-bucket'})

        assert settings.name == 'test-bucket'
        self._assert_optional_fields_none(settings)

    def test_all_fields_from_server_translation(self) -> None:
        server_dict = {'name': 'test',
                       'bucket_type': 'couchbase',
                       'compression_mode': 'active',
                       'conflict_resolution_type': 'sequence_number',
                       'eviction_policy': 'full',
                       'flush_enabled': True,
                       'history_retention_collection_default': True,
                       'history_retention_bytes': 2048,
                       'history_retention_duration': 86400,
                       'max_expiry': 43200,
                       'minimum_durability_level': 1,
                       'num_replicas': 2,
                       'ram_quota_mb': 256,
                       'replica_indexes': True,
                       'storage_backend': 'magma',
                       'num_vbuckets': 128}

        settings = BucketSettings.bucket_settings_from_server(server_dict)

        assert settings.name == 'test'
        assert settings.bucket_type == BucketType.COUCHBASE
        assert settings.compression_mode == CompressionMode.ACTIVE
        assert settings.conflict_resolution_type == ConflictResolutionType.SEQUENCE_NUMBER
        assert settings.eviction_policy == EvictionPolicyType.FULL
        assert settings.flush_enabled is True
        assert settings.history_retention_collection_default is True
        assert settings.history_retention_bytes == 2048
        assert settings.history_retention_duration == timedelta(days=1)
        assert settings.max_expiry == timedelta(hours=12)
        assert settings.minimum_durability_level == DurabilityLevel.MAJORITY
        assert settings.num_replicas == 2
        assert settings.ram_quota_mb == 256
        assert settings.replica_index is True
        assert settings.storage_backend == StorageBackend.MAGMA
        assert settings.num_vbuckets == 128

    def test_zero_and_false_values_from_server(self) -> None:
        server_dict = {'name': 'test',
                       'num_replicas': 0,
                       'flush_enabled': False,
                       'replica_indexes': False,
                       'history_retention_bytes': 0,
                       'history_retention_duration': 0,
                       'max_expiry': 0,
                       'minimum_durability_level': 0}

        settings = BucketSettings.bucket_settings_from_server(server_dict)

        assert settings.num_replicas == 0
        assert settings.flush_enabled is False
        assert settings.replica_index is False
        assert settings.history_retention_bytes == 0
        assert settings.history_retention_duration == timedelta(0)
        assert settings.max_expiry == timedelta(0)
        assert settings.minimum_durability_level == DurabilityLevel.NONE

    def test_membase_bucket_type_from_server(self) -> None:
        # `BucketType.COUCHBASE.value` is `membase` while `to_server_str` writes `couchbase`,
        # so the enum round-trips through two strings and every other test here uses only
        # the second.  `from_server_str` accepts both; nothing proved it.
        settings = BucketSettings.bucket_settings_from_server({'name': 'test', 'bucket_type': 'membase'})

        assert settings.bucket_type == BucketType.COUCHBASE

    def test_unknown_storage_backend_from_server(self) -> None:
        # The core reports a bucket with no backend set as `unknown`, which `StorageBackend` has no
        # member for.
        settings = BucketSettings.bucket_settings_from_server({'name': 'test', 'storage_backend': 'unknown'})

        assert settings.storage_backend == StorageBackend.UNDEFINED

    @pytest.mark.parametrize('field, unknown', [('bucket_type', BucketType.UNKNOWN),
                                                ('compression_mode', CompressionMode.UNKNOWN),
                                                ('conflict_resolution_type', ConflictResolutionType.UNKNOWN),
                                                ('eviction_policy', EvictionPolicyType.UNKNOWN),
                                                ('storage_backend', StorageBackend.UNDEFINED)])
    def test_a_value_the_sdk_cannot_name_reads_as_unknown(self, field: str, unknown: object) -> None:
        # A server release can add a value, and reading the bucket must not fail on it.
        settings = BucketSettings.bucket_settings_from_server({'name': 'test', field: 'not-a-real-value'})

        assert getattr(settings, field) is unknown

    # --- Error cases ---

    def test_bucket_settings_empty_name_error(self) -> None:
        with pytest.raises(InvalidArgumentException) as exc_info:
            BucketSettings(name='')

        assert 'non-empty bucket name' in str(exc_info.value)

    def test_bucket_settings_none_name_error(self) -> None:
        # None is not a name and the checker is right to say so; what is under test is that
        # the constructor rejects it at runtime.  A cast would assert the opposite.
        with pytest.raises(InvalidArgumentException) as exc_info:
            BucketSettings(name=None)  # type: ignore[arg-type]

        assert 'non-empty bucket name' in str(exc_info.value)

    @pytest.mark.parametrize('settings_type', [BucketSettings, CreateBucketSettings])
    @pytest.mark.parametrize('name', [7, b'test', ['test']])
    def test_bucket_settings_non_str_name_error(self, settings_type: type[BucketSettings], name: Any) -> None:
        with pytest.raises(InvalidArgumentException) as exc_info:
            settings_type(name=name)

        assert exc_info.value.message == 'The bucket name must be str.'


class LegacyBucketSettingsUnitTests:
    # Kept apart from BucketSettingsUnitTests so that what the alias does is legible on its
    # own and leaves with it.

    def test_max_ttl_property_reports_max_expiry_and_not_max_ttl(self) -> None:
        # BucketSettings.max_ttl reads the 'max_expiry' key, so a settings object built with
        # max_ttl reports None from both accessors while still sending the value to the server.
        # Pinned: the property is public, correcting it changes what an existing caller sees,
        # and folding max_ttl into max_expiry was declined in PYCBC-1908.
        by_alias = BucketSettings(name='test', max_ttl=3600)
        assert by_alias['max_ttl'] == 3600
        assert by_alias.max_ttl is None
        assert by_alias.max_expiry is None

        by_current_name = BucketSettings(name='test', max_expiry=timedelta(hours=1))
        assert by_current_name.max_ttl == timedelta(hours=1)

    def test_max_ttl_property_warns(self) -> None:
        settings = BucketSettings(name='test', max_expiry=timedelta(hours=1))
        with pytest.warns(CouchbaseDeprecationWarning, match=r'BucketSettings\.max_ttl is deprecated'):
            _ = settings.max_ttl
