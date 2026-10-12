#  Copyright 2016-2024. Couchbase, Inc.
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

from couchbase.durability import DurabilityLevel
from couchbase.exceptions import InvalidArgumentException
from couchbase.management.logic.bucket_mgmt_req_builder import BucketMgmtRequestBuilder
from couchbase.management.logic.bucket_mgmt_types import (BucketSettings,
                                                          BucketType,
                                                          CompressionMode,
                                                          ConflictResolutionType,
                                                          CreateBucketSettings,
                                                          EvictionPolicyType,
                                                          StorageBackend)


def _optional_property_names() -> list[str]:
    # Read off the class rather than enumerated, so a property added later is asserted
    # without anyone remembering to extend a list.  `CreateBucketSettings` adds no property
    # of its own, so one list serves both.  PYCBC-1908 will make `max_ttl` warn on read, at
    # which point exclude it here rather than moving these tests.
    return sorted(name for name in dir(BucketSettings)
                  if name != 'name' and isinstance(getattr(BucketSettings, name, None), property))


class BucketSettingsUnitTests:
    # No cb_env and no TEST_MANIFEST: every test either builds a `BucketSettings` and reads
    # the dict it would hand the core, or feeds a server-shaped dict to
    # `BucketSettings.bucket_settings_from_server`.  Not generated either: this file never
    # gets as far as an api, since it imports the request builder and instantiates it
    # directly, so a generated copy would be the same file with nothing to await.  The
    # deprecated `max_ttl` alias is covered by LegacyBucketSettingsUnitTests below.

    @pytest.fixture(scope='class', name='request_builder')
    def bucket_mgmt_request_builder(self) -> BucketMgmtRequestBuilder:
        return BucketMgmtRequestBuilder()

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

    # --- Translation to the server payload ---

    def test_minimal_settings_to_server(self, request_builder: BucketMgmtRequestBuilder) -> None:
        settings = CreateBucketSettings(name='test-bucket')
        result = request_builder._bucket_settings_to_server(settings)

        assert result == {'name': 'test-bucket'}
        assert len(result) == 1

    def test_zero_replicas_to_server(self, request_builder: BucketMgmtRequestBuilder) -> None:
        settings = CreateBucketSettings(name='test', num_replicas=0)
        result = request_builder._bucket_settings_to_server(settings)

        assert result == {'name': 'test', 'num_replicas': 0}

    def test_false_flush_enabled_to_server(self, request_builder: BucketMgmtRequestBuilder) -> None:
        settings = CreateBucketSettings(name='test', flush_enabled=False)
        result = request_builder._bucket_settings_to_server(settings)

        assert result == {'name': 'test', 'flush_enabled': False}

    def test_false_replica_index_to_server(self, request_builder: BucketMgmtRequestBuilder) -> None:
        settings = CreateBucketSettings(name='test', replica_index=False)
        result = request_builder._bucket_settings_to_server(settings)

        # The wire name is plural and differs from the attribute name, which is why this is a
        # test of its own rather than a line in the exhaustive one below.
        assert result == {'name': 'test', 'replica_indexes': False}

    def test_zero_and_false_history_retention_to_server(self, request_builder: BucketMgmtRequestBuilder) -> None:
        settings = CreateBucketSettings(name='test',
                                        history_retention_collection_default=False,
                                        history_retention_bytes=0,
                                        history_retention_duration=timedelta(0))
        result = request_builder._bucket_settings_to_server(settings)

        assert result == {'name': 'test',
                          'history_retention_collection_default': False,
                          'history_retention_bytes': 0,
                          'history_retention_duration': 0}

    def test_durability_level_none_to_server(self, request_builder: BucketMgmtRequestBuilder) -> None:
        settings = CreateBucketSettings(name='test', minimum_durability_level=DurabilityLevel.NONE)
        result = request_builder._bucket_settings_to_server(settings)

        assert result == {'name': 'test', 'minimum_durability_level': 0}

    def test_max_expiry_zero_to_server(self, request_builder: BucketMgmtRequestBuilder) -> None:
        settings = CreateBucketSettings(name='test', max_expiry=timedelta(0))
        result = request_builder._bucket_settings_to_server(settings)

        assert result == {'name': 'test', 'max_expiry': 0}

    def test_all_fields_to_server_translation(self, request_builder: BucketMgmtRequestBuilder) -> None:
        settings = CreateBucketSettings(name='test',
                                        bucket_type=BucketType.COUCHBASE,
                                        compression_mode=CompressionMode.ACTIVE,
                                        conflict_resolution_type=ConflictResolutionType.SEQUENCE_NUMBER,
                                        eviction_policy=EvictionPolicyType.FULL,
                                        flush_enabled=True,
                                        history_retention_collection_default=True,
                                        history_retention_bytes=2048,
                                        history_retention_duration=timedelta(days=1),
                                        max_expiry=timedelta(hours=12),
                                        minimum_durability_level=DurabilityLevel.MAJORITY,
                                        num_replicas=2,
                                        ram_quota_mb=256,
                                        replica_index=True,
                                        storage_backend=StorageBackend.MAGMA,
                                        num_vbuckets=128)

        result = request_builder._create_bucket_settings_to_server(settings)

        assert result == {'name': 'test',
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

    def test_conflict_resolution_type_not_sent_on_update(self,
                                                         request_builder: BucketMgmtRequestBuilder) -> None:
        settings = CreateBucketSettings(name='test',
                                        conflict_resolution_type=ConflictResolutionType.CUSTOM)

        create_result = request_builder._create_bucket_settings_to_server(settings)
        update_result = request_builder._bucket_settings_to_server(settings)

        # A bucket's conflict resolution type is fixed at creation, so the update payload has
        # to drop it rather than resend it unchanged.
        assert create_result['conflict_resolution_type'] == 'custom'
        assert 'conflict_resolution_type' not in update_result

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
        # `StorageBackend` has no `unknown` member, so without the rewrite this raises rather
        # than reporting a backend the SDK does not recognise.
        settings = BucketSettings.bucket_settings_from_server({'name': 'test', 'storage_backend': 'unknown'})

        assert settings.storage_backend == StorageBackend.UNDEFINED

    # --- Round trips ---

    def test_round_trip_minimal(self, request_builder: BucketMgmtRequestBuilder) -> None:
        original_server_dict = {'name': 'test'}

        settings = BucketSettings.bucket_settings_from_server(original_server_dict)
        result_server_dict = request_builder._bucket_settings_to_server(settings)

        assert result_server_dict == original_server_dict

    def test_round_trip_full(self, request_builder: BucketMgmtRequestBuilder) -> None:
        original_server_dict = {'name': 'test',
                                'bucket_type': 'couchbase',
                                'compression_mode': 'active',
                                'eviction_policy': 'full',
                                'flush_enabled': True,
                                'num_replicas': 2,
                                'ram_quota_mb': 256,
                                'replica_indexes': True,
                                'storage_backend': 'magma',
                                'minimum_durability_level': 1,
                                'max_expiry': 3600}

        settings = BucketSettings.bucket_settings_from_server(original_server_dict)
        result_server_dict = request_builder._bucket_settings_to_server(settings)

        assert result_server_dict == original_server_dict

    def test_round_trip_edge_cases(self, request_builder: BucketMgmtRequestBuilder) -> None:
        original_server_dict = {'name': 'test',
                                'num_replicas': 0,
                                'flush_enabled': False,
                                'replica_indexes': False,
                                'history_retention_bytes': 0,
                                'history_retention_duration': 0,
                                'max_expiry': 0,
                                'minimum_durability_level': 0}

        settings = BucketSettings.bucket_settings_from_server(original_server_dict)
        result_server_dict = request_builder._bucket_settings_to_server(settings)

        assert result_server_dict == original_server_dict

    # --- Enum conversions ---

    @pytest.mark.parametrize('enum_field,enum_value,server_key,expected_value', [
        ('bucket_type', BucketType.COUCHBASE, 'bucket_type', 'couchbase'),
        ('bucket_type', BucketType.MEMCACHED, 'bucket_type', 'memcached'),
        ('bucket_type', BucketType.EPHEMERAL, 'bucket_type', 'ephemeral'),
        ('compression_mode', CompressionMode.OFF, 'compression_mode', 'off'),
        ('compression_mode', CompressionMode.PASSIVE, 'compression_mode', 'passive'),
        ('compression_mode', CompressionMode.ACTIVE, 'compression_mode', 'active'),
        ('conflict_resolution_type', ConflictResolutionType.TIMESTAMP, 'conflict_resolution_type', 'timestamp'),
        ('conflict_resolution_type', ConflictResolutionType.SEQUENCE_NUMBER,
         'conflict_resolution_type', 'sequence_number'),
        ('conflict_resolution_type', ConflictResolutionType.CUSTOM, 'conflict_resolution_type', 'custom'),
        ('eviction_policy', EvictionPolicyType.FULL, 'eviction_policy', 'full'),
        ('eviction_policy', EvictionPolicyType.VALUE_ONLY, 'eviction_policy', 'value_only'),
        ('eviction_policy', EvictionPolicyType.NOT_RECENTLY_USED, 'eviction_policy', 'not_recently_used'),
        ('eviction_policy', EvictionPolicyType.NO_EVICTION, 'eviction_policy', 'no_eviction'),
        ('storage_backend', StorageBackend.COUCHSTORE, 'storage_backend', 'couchstore'),
        ('storage_backend', StorageBackend.MAGMA, 'storage_backend', 'magma'),
        ('storage_backend', StorageBackend.UNDEFINED, 'storage_backend', 'undefined'),
        ('minimum_durability_level', DurabilityLevel.NONE, 'minimum_durability_level', 0),
        ('minimum_durability_level', DurabilityLevel.MAJORITY, 'minimum_durability_level', 1),
        ('minimum_durability_level', DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE,
         'minimum_durability_level', 2),
        ('minimum_durability_level', DurabilityLevel.PERSIST_TO_MAJORITY,
         'minimum_durability_level', 3),
    ])
    def test_enum_conversions_to_server(self,
                                        request_builder: BucketMgmtRequestBuilder,
                                        enum_field: str,
                                        enum_value: Any,
                                        server_key: str,
                                        expected_value: Any) -> None:
        settings = CreateBucketSettings(name='test', **{enum_field: enum_value})
        result = request_builder._create_bucket_settings_to_server(settings)

        assert server_key in result
        assert result[server_key] == expected_value

    # --- Error cases ---

    def test_bucket_settings_empty_name_error(self) -> None:
        with pytest.raises(InvalidArgumentException) as exc_info:
            BucketSettings(name='')

        assert 'non-empty bucket name' in str(exc_info.value)

    def test_bucket_settings_none_name_error(self) -> None:
        # None is not a name and the checker is right to say so; what is under test is that
        # the constructor rejects it at runtime.  A cast would assert the opposite.
        with pytest.raises(InvalidArgumentException) as exc_info:
            BucketSettings(name=None)  # type: ignore[call-overload]

        assert 'non-empty bucket name' in str(exc_info.value)

    def test_invalid_enum_type_error(self, request_builder: BucketMgmtRequestBuilder) -> None:
        settings = CreateBucketSettings(name='test')
        settings['bucket_type'] = 'invalid_string'

        with pytest.raises(InvalidArgumentException) as exc_info:
            request_builder._bucket_settings_to_server(settings)

        assert 'BucketType' in str(exc_info.value)
        assert 'invalid_string' in str(exc_info.value)

    def test_invalid_int_type_error(self, request_builder: BucketMgmtRequestBuilder) -> None:
        settings = CreateBucketSettings(name='test')
        settings['ram_quota_mb'] = 'not_an_int'

        with pytest.raises(InvalidArgumentException) as exc_info:
            request_builder._bucket_settings_to_server(settings)

        assert 'type int' in str(exc_info.value)

    def test_invalid_timedelta_type_error(self, request_builder: BucketMgmtRequestBuilder) -> None:
        settings = CreateBucketSettings(name='test')
        settings['max_expiry'] = 'not_a_timedelta'

        with pytest.raises(InvalidArgumentException) as exc_info:
            request_builder._bucket_settings_to_server(settings)

        # The accepted-types clause, not the verb: the message opens with `Excepted`, and
        # asserting around that typo takes an `or` over two substrings.
        assert 'Union[timedelta, float, int]' in str(exc_info.value)
        assert 'not_a_timedelta' in str(exc_info.value)


class LegacyBucketSettingsUnitTests:
    # Kept apart from BucketSettingsUnitTests so that what the alias does is legible on its
    # own and leaves with it.  Passing `max_ttl` alongside `max_expiry` in an exhaustive
    # to-server test reads as though the field were dropped; it is overwritten, not dropped.

    @pytest.fixture(scope='class', name='request_builder')
    def bucket_mgmt_request_builder(self) -> BucketMgmtRequestBuilder:
        return BucketMgmtRequestBuilder()

    def test_max_ttl_alone_is_sent_as_max_expiry(self, request_builder: BucketMgmtRequestBuilder) -> None:
        settings = CreateBucketSettings(name='test', max_ttl=3600)
        result = request_builder._bucket_settings_to_server(settings)

        # There is no maxTTL in the core, so the alias reaches the wire under the current name.
        assert result == {'name': 'test', 'max_expiry': 3600}

    def test_max_expiry_takes_precedence_over_max_ttl(self, request_builder: BucketMgmtRequestBuilder) -> None:
        settings = CreateBucketSettings(name='test', max_ttl=3600, max_expiry=timedelta(hours=12))
        result = request_builder._bucket_settings_to_server(settings)

        assert result == {'name': 'test', 'max_expiry': 43200}

    def test_max_ttl_property_reports_max_expiry_and_not_max_ttl(self) -> None:
        # @FINDING(PYCBC-1908): BucketSettings.max_ttl reads the 'max_expiry' key, so a settings
        # object built with max_ttl reports None from both accessors while still sending the
        # value to the server.  Reading back what was just written is the obvious way to
        # confirm a deprecated field was accepted, and it says the field was ignored.  Pinned
        # rather than fixed: the property is public, and correcting it changes what an
        # existing caller sees.
        by_alias = BucketSettings(name='test', max_ttl=3600)
        assert by_alias['max_ttl'] == 3600
        assert by_alias.max_ttl is None
        assert by_alias.max_expiry is None

        by_current_name = BucketSettings(name='test', max_expiry=timedelta(hours=1))
        assert by_current_name.max_ttl == timedelta(hours=1)

    def test_max_ttl_rejects_timedelta(self, request_builder: BucketMgmtRequestBuilder) -> None:
        # @FINDING(PYCBC-1908): max_ttl is annotated Optional[Union[timedelta, float, int]] on both
        # settings classes, matching max_expiry, but the builder sends it through an int
        # validator rather than the seconds conversion max_expiry gets.  A timedelta is what
        # the signature invites and it raises.
        settings = CreateBucketSettings(name='test', max_ttl=timedelta(hours=1))

        with pytest.raises(InvalidArgumentException) as exc_info:
            request_builder._bucket_settings_to_server(settings)

        assert 'type int' in str(exc_info.value)
