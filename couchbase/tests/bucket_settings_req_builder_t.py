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
from couchbase.durability import DurabilityLevel
from couchbase.exceptions import InvalidArgumentException
from couchbase.protocol.classic._management._bucket_mgmt_req_builder import BucketMgmtRequestBuilder
from tests.test_features import TestProtocol

# What is checked here is classic's request builder, which writes what its binding reads.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)


class BucketSettingsToServerUnitTests:
    # No cb_env and no TEST_MANIFEST: every test hands a `BucketSettings` to the request builder
    # and reads the dict it would hand the core.  The settings themselves, and what they make of
    # a server payload, are in bucketmgmt_bucket_settings_t.

    @pytest.fixture(scope='class', name='request_builder')
    def bucket_mgmt_request_builder(self) -> BucketMgmtRequestBuilder:
        return BucketMgmtRequestBuilder()

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
        assert 'timedelta | float | int' in str(exc_info.value)
        assert 'not_a_timedelta' in str(exc_info.value)


class LegacyBucketSettingsToServerUnitTests:
    # Passing `max_ttl` alongside `max_expiry` in an exhaustive to-server test reads as though the
    # field were dropped; it is overwritten, not dropped.

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

    def test_max_ttl_rejects_timedelta(self, request_builder: BucketMgmtRequestBuilder) -> None:
        # max_ttl is annotated timedelta | float | int | None on both settings classes,
        # matching max_expiry, but the builder sends it through an int validator rather than the
        # seconds conversion max_expiry gets, so a timedelta raises.  Pinned: folding max_ttl
        # into max_expiry was declined in PYCBC-1908.
        settings = CreateBucketSettings(name='test', max_ttl=timedelta(hours=1))

        with pytest.raises(InvalidArgumentException) as exc_info:
            request_builder._bucket_settings_to_server(settings)

        assert 'type int' in str(exc_info.value)
