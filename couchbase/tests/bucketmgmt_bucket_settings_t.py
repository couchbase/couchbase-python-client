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

from datetime import timedelta

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


class BucketSettingsTestSuite:
    TEST_MANIFEST = [
        # Category 1: Minimal Object State (2 tests)
        'test_bucket_settings_minimal_state',
        'test_create_bucket_settings_minimal_state',

        # Category 2: Edge Cases - Zero and False Values (4 tests)
        'test_bucket_settings_num_replicas_zero',
        'test_bucket_settings_flush_enabled_false',
        'test_bucket_settings_replica_index_false',
        'test_bucket_settings_history_retention_zeros',

        # Category 3: Translation to Server - Minimal & Edge Cases (7 tests)
        'test_minimal_settings_to_server',
        'test_zero_replicas_to_server',
        'test_false_flush_enabled_to_server',
        'test_false_replica_index_to_server',
        'test_zero_and_false_history_retention_to_server',
        'test_durability_level_none_to_server',
        'test_max_expiry_zero_to_server',

        # Category 4: Translation to Server - Exhaustive (1 test)
        'test_all_fields_to_server_translation',

        # Category 5: Translation from Server (3 tests)
        'test_minimal_settings_from_server',
        'test_all_fields_from_server_translation',
        'test_zero_and_false_values_from_server',

        # Category 6: Round-Trip Translation (3 tests)
        'test_round_trip_minimal',
        'test_round_trip_full',
        'test_round_trip_edge_cases',

        # Category 7: Enum Conversions (1 parametrized test)
        'test_enum_conversions_to_server',

        # Category 8: Error Cases (5 tests)
        'test_bucket_settings_empty_name_error',
        'test_bucket_settings_none_name_error',
        'test_invalid_enum_type_error',
        'test_invalid_int_type_error',
        'test_invalid_timedelta_type_error',
    ]

    @pytest.fixture(scope='class')
    def request_builder(self):
        return BucketMgmtRequestBuilder()

    def assert_all_optional_fields_none(self, settings, exclude=None):
        exclude = exclude or []
        optional_fields = [
            'bucket_type', 'compression_mode', 'eviction_policy',
            'flush_enabled', 'history_retention_collection_default',
            'history_retention_bytes', 'history_retention_duration',
            'max_expiry', 'max_ttl', 'minimum_durability_level',
            'num_replicas', 'ram_quota_mb', 'replica_index',
            'storage_backend', 'num_vbuckets'
        ]
        for field in optional_fields:
            if field not in exclude:
                assert getattr(settings, field) is None, f"{field} should be None but got {getattr(settings, field)}"

    def assert_dict_subset(self, expected, actual):
        for key, value in expected.items():
            assert key in actual, f"Key '{key}' not found in actual dict"
            assert actual[key] == value, f"Key '{key}': expected {value}, got {actual[key]}"

    def test_bucket_settings_minimal_state(self):
        settings = BucketSettings(name="test-bucket")

        assert len(settings) == 1
        assert settings['name'] == "test-bucket"
        assert settings.name == "test-bucket"
        self.assert_all_optional_fields_none(settings)

    def test_create_bucket_settings_minimal_state(self):
        settings = CreateBucketSettings(name="test-bucket")

        assert len(settings) == 1
        assert settings['name'] == "test-bucket"
        assert settings.name == "test-bucket"
        self.assert_all_optional_fields_none(settings)
        assert settings.conflict_resolution_type is None

    def test_bucket_settings_num_replicas_zero(self):
        settings = BucketSettings(name="test", num_replicas=0)

        assert 'num_replicas' in settings
        assert settings['num_replicas'] == 0
        assert settings.num_replicas == 0
        assert len(settings) == 2

    def test_bucket_settings_flush_enabled_false(self):
        settings = BucketSettings(name="test", flush_enabled=False)

        assert 'flush_enabled' in settings
        assert settings['flush_enabled'] is False
        assert settings.flush_enabled is False
        assert len(settings) == 2

    def test_bucket_settings_replica_index_false(self):
        settings = BucketSettings(name="test", replica_index=False)

        assert 'replica_index' in settings
        assert settings['replica_index'] is False
        assert settings.replica_index is False
        assert len(settings) == 2

    def test_bucket_settings_history_retention_zeros(self):
        settings = BucketSettings(
            name="test",
            history_retention_collection_default=False,
            history_retention_bytes=0,
            history_retention_duration=timedelta(0)
        )

        assert 'history_retention_collection_default' in settings
        assert 'history_retention_bytes' in settings
        assert 'history_retention_duration' in settings
        assert settings.history_retention_collection_default is False
        assert settings.history_retention_bytes == 0
        assert settings.history_retention_duration == timedelta(0)
        assert len(settings) == 4

    def test_minimal_settings_to_server(self, request_builder):
        settings = CreateBucketSettings(name="test-bucket")
        result = request_builder._bucket_settings_to_server(settings)

        assert result == {'name': 'test-bucket'}
        assert len(result) == 1

    def test_zero_replicas_to_server(self, request_builder):
        settings = CreateBucketSettings(name="test", num_replicas=0)
        result = request_builder._bucket_settings_to_server(settings)

        assert result == {'name': 'test', 'num_replicas': 0}

    def test_false_flush_enabled_to_server(self, request_builder):
        settings = CreateBucketSettings(name="test", flush_enabled=False)
        result = request_builder._bucket_settings_to_server(settings)

        assert result == {'name': 'test', 'flush_enabled': False}

    def test_false_replica_index_to_server(self, request_builder):
        settings = CreateBucketSettings(name="test", replica_index=False)
        result = request_builder._bucket_settings_to_server(settings)

        assert result == {'name': 'test', 'replica_indexes': False}

    def test_zero_and_false_history_retention_to_server(self, request_builder):
        settings = CreateBucketSettings(
            name="test",
            history_retention_collection_default=False,
            history_retention_bytes=0,
            history_retention_duration=timedelta(0)
        )
        result = request_builder._bucket_settings_to_server(settings)

        expected = {
            'name': 'test',
            'history_retention_collection_default': False,
            'history_retention_bytes': 0,
            'history_retention_duration': 0
        }
        assert result == expected

    def test_durability_level_none_to_server(self, request_builder):
        settings = CreateBucketSettings(name="test", minimum_durability_level=DurabilityLevel.NONE)
        result = request_builder._bucket_settings_to_server(settings)

        assert result == {'name': 'test', 'minimum_durability_level': 0}

    def test_max_expiry_zero_to_server(self, request_builder):
        settings = CreateBucketSettings(name="test", max_expiry=timedelta(0))
        result = request_builder._bucket_settings_to_server(settings)

        assert result == {'name': 'test', 'max_expiry': 0}

    def test_all_fields_to_server_translation(self, request_builder):
        settings = CreateBucketSettings(
            name="test",
            bucket_type=BucketType.COUCHBASE,
            compression_mode=CompressionMode.ACTIVE,
            conflict_resolution_type=ConflictResolutionType.SEQUENCE_NUMBER,
            eviction_policy=EvictionPolicyType.FULL,
            flush_enabled=True,
            history_retention_collection_default=True,
            history_retention_bytes=2048,
            history_retention_duration=timedelta(days=1),
            max_expiry=timedelta(hours=12),
            max_ttl=3600,
            minimum_durability_level=DurabilityLevel.MAJORITY,
            num_replicas=2,
            ram_quota_mb=256,
            replica_index=True,
            storage_backend=StorageBackend.MAGMA,
            num_vbuckets=128
        )

        result = request_builder._bucket_settings_to_server(settings)

        expected = {
            'name': 'test',
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
            'num_vbuckets': 128
        }

        assert result == expected

    def test_minimal_settings_from_server(self):
        server_dict = {'name': 'test-bucket'}
        settings = BucketSettings.bucket_settings_from_server(server_dict)

        assert settings.name == 'test-bucket'
        assert settings.bucket_type is None
        assert settings.compression_mode is None
        assert settings.ram_quota_mb is None
        assert settings.minimum_durability_level is None

    def test_all_fields_from_server_translation(self):
        server_dict = {
            'name': 'test',
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
            'num_vbuckets': 128
        }

        settings = BucketSettings.bucket_settings_from_server(server_dict)

        assert settings.name == 'test'
        assert settings.bucket_type == BucketType.COUCHBASE
        assert settings.compression_mode == CompressionMode.ACTIVE
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

    def test_zero_and_false_values_from_server(self):
        server_dict = {
            'name': 'test',
            'num_replicas': 0,
            'flush_enabled': False,
            'replica_indexes': False,
            'history_retention_bytes': 0,
            'history_retention_duration': 0,
            'max_expiry': 0,
            'minimum_durability_level': 0
        }

        settings = BucketSettings.bucket_settings_from_server(server_dict)

        assert settings.num_replicas == 0
        assert settings.flush_enabled is False
        assert settings.replica_index is False
        assert settings.history_retention_bytes == 0
        assert settings.history_retention_duration == timedelta(0)
        assert settings.max_expiry == timedelta(0)
        assert settings.minimum_durability_level == DurabilityLevel.NONE

    def test_round_trip_minimal(self, request_builder):
        original_server_dict = {'name': 'test'}

        settings = BucketSettings.bucket_settings_from_server(original_server_dict)
        result_server_dict = request_builder._bucket_settings_to_server(settings)

        assert result_server_dict == original_server_dict

    def test_round_trip_full(self, request_builder):
        original_server_dict = {
            'name': 'test',
            'bucket_type': 'couchbase',
            'compression_mode': 'active',
            'eviction_policy': 'full',
            'flush_enabled': True,
            'num_replicas': 2,
            'ram_quota_mb': 256,
            'replica_indexes': True,
            'storage_backend': 'magma',
            'minimum_durability_level': 1,
            'max_expiry': 3600,
        }

        settings = BucketSettings.bucket_settings_from_server(original_server_dict)
        result_server_dict = request_builder._bucket_settings_to_server(settings)

        assert result_server_dict == original_server_dict

    def test_round_trip_edge_cases(self, request_builder):
        original_server_dict = {
            'name': 'test',
            'num_replicas': 0,
            'flush_enabled': False,
            'replica_indexes': False,
            'history_retention_bytes': 0,
            'history_retention_duration': 0,
            'max_expiry': 0,
            'minimum_durability_level': 0
        }

        settings = BucketSettings.bucket_settings_from_server(original_server_dict)
        result_server_dict = request_builder._bucket_settings_to_server(settings)

        assert result_server_dict == original_server_dict

    @pytest.mark.parametrize("enum_field,enum_value,server_key,expected_str", [
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
    def test_enum_conversions_to_server(self, request_builder, enum_field, enum_value, server_key, expected_str):
        settings = CreateBucketSettings(name="test", **{enum_field: enum_value})
        result = request_builder._bucket_settings_to_server(settings)

        assert server_key in result
        assert result[server_key] == expected_str

    def test_bucket_settings_empty_name_error(self):
        with pytest.raises(InvalidArgumentException) as exc_info:
            BucketSettings(name="")

        assert "non-empty bucket name" in str(exc_info.value)

    def test_bucket_settings_none_name_error(self):
        with pytest.raises(InvalidArgumentException) as exc_info:
            BucketSettings(name=None)

        assert "non-empty bucket name" in str(exc_info.value)

    def test_invalid_enum_type_error(self, request_builder):
        settings = CreateBucketSettings(name="test")
        settings['bucket_type'] = "invalid_string"

        with pytest.raises(InvalidArgumentException) as exc_info:
            request_builder._bucket_settings_to_server(settings)

        assert "BucketType" in str(exc_info.value)

    def test_invalid_int_type_error(self, request_builder):
        settings = CreateBucketSettings(name="test")
        settings['ram_quota_mb'] = "not_an_int"

        with pytest.raises(InvalidArgumentException) as exc_info:
            request_builder._bucket_settings_to_server(settings)

        assert "int" in str(exc_info.value).lower()

    def test_invalid_timedelta_type_error(self, request_builder):
        settings = CreateBucketSettings(name="test")
        settings['max_expiry'] = "not_a_timedelta"

        with pytest.raises(InvalidArgumentException) as exc_info:
            request_builder._bucket_settings_to_server(settings)

        assert "timedelta" in str(exc_info.value).lower() or "Union" in str(exc_info.value)


class ClassicBucketSettingsTests(BucketSettingsTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicBucketSettingsTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicBucketSettingsTests) if valid_test_method(meth)]
        compare = set(BucketSettingsTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', autouse=True)
    def check_test_manifest(self, test_manifest_validated):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated. Missing tests: {test_manifest_validated}.')
