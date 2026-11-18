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

"""The couchbase2 bucket manager: its requests, its reading of the listing, and both impls end to end."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import timedelta
from typing import Any, cast

import pytest

from acouchbase.protocol.couchbase2._cluster_impl import AsyncClusterImpl
from couchbase.auth import PasswordAuthenticator
from couchbase.durability import DurabilityLevel
from couchbase.exceptions import (BucketAlreadyExistsException,
                                  BucketDoesNotExistException,
                                  FeatureUnavailableException,
                                  InvalidArgumentException)
from couchbase.management.buckets import (BucketSettings,
                                          BucketType,
                                          CompressionMode,
                                          ConflictResolutionType,
                                          CreateBucketSettings,
                                          EvictionPolicyType,
                                          StorageBackend)
from couchbase.options import ClusterOptions
from couchbase.protocol.couchbase2._cluster_impl import ClusterImpl
from couchbase.protocol.couchbase2._core import ClusterCore
from couchbase.protocol.couchbase2._generated.admin.bucket.v1 import bucket_pb2
from couchbase.protocol.couchbase2._generated.kv.v1 import kv_pb2
from couchbase.protocol.couchbase2._management._bucket_mgmt_impl import BucketMgmtImpl
from couchbase.protocol.couchbase2._management._bucket_mgmt_req_builder import BucketMgmtRequestBuilder
from couchbase.protocol.couchbase2._management._bucket_mgmt_types import (bucket_mgmt_errors,
                                                                          bucket_settings,
                                                                          named_bucket_settings)
from tests.couchbase2_gateway import (Certificates,
                                      FakeGateway,
                                      make_certificates,
                                      new_gateway,
                                      serving)
from tests.event_loop import run_on_own_loop

ListedBucket = bucket_pb2.ListBucketsResponse.Bucket


@pytest.fixture(scope='module')
def core() -> Iterator[ClusterCore]:
    built = ClusterCore('couchbase2://localhost', ClusterOptions(PasswordAuthenticator('Administrator', 'password')))
    try:
        yield built
    finally:
        built.close()


@pytest.fixture
def builder(core: ClusterCore) -> BucketMgmtRequestBuilder:
    return BucketMgmtRequestBuilder(core)


def _set_fields(message: Any) -> set[str]:
    return {descriptor.name for descriptor, _ in message.ListFields()}


class RequestUnitTests:

    def test_a_create_carries_only_what_is_given(self, builder: BucketMgmtRequestBuilder) -> None:
        message = builder.build_create_bucket_request(CreateBucketSettings(name='travel', ram_quota_mb=100),
                                                      None).message
        assert _set_fields(message) == {'bucket_name', 'ram_quota_mb'}

    def test_every_setting_of_a_create(self, builder: BucketMgmtRequestBuilder) -> None:
        settings = CreateBucketSettings(name='travel',
                                        bucket_type=BucketType.EPHEMERAL,
                                        compression_mode=CompressionMode.ACTIVE,
                                        conflict_resolution_type=ConflictResolutionType.SEQUENCE_NUMBER,
                                        eviction_policy=EvictionPolicyType.NO_EVICTION,
                                        flush_enabled=False,
                                        history_retention_collection_default=False,
                                        history_retention_bytes=0,
                                        history_retention_duration=timedelta(minutes=10),
                                        max_expiry=timedelta(seconds=0),
                                        minimum_durability_level=DurabilityLevel.MAJORITY,
                                        num_replicas=0,
                                        ram_quota_mb=100,
                                        replica_index=False,
                                        storage_backend=StorageBackend.MAGMA)
        message = builder.build_create_bucket_request(settings, None).message
        assert message == bucket_pb2.CreateBucketRequest(
            bucket_name='travel',
            bucket_type=bucket_pb2.BUCKET_TYPE_EPHEMERAL,
            compression_mode=bucket_pb2.COMPRESSION_MODE_ACTIVE,
            conflict_resolution_type=bucket_pb2.CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER,
            eviction_mode=bucket_pb2.EVICTION_MODE_NONE,
            flush_enabled=False,
            history_retention_collection_default=False,
            history_retention_bytes=0,
            history_retention_duration_secs=600,
            max_expiry_secs=0,
            minimum_durability_level=kv_pb2.DURABILITY_LEVEL_MAJORITY,
            num_replicas=0,
            ram_quota_mb=100,
            replica_indexes=False,
            storage_backend=bucket_pb2.STORAGE_BACKEND_MAGMA)
        assert {'flush_enabled', 'history_retention_bytes', 'max_expiry_secs', 'num_replicas',
                'replica_indexes'} <= _set_fields(message)

    def test_an_enum_named_by_its_value(self, builder: BucketMgmtRequestBuilder) -> None:
        by_value: Any = {'name': 'travel', 'eviction_policy': 'valueOnly'}
        settings = CreateBucketSettings(**by_value)
        message = builder.build_create_bucket_request(settings, None).message
        assert message.eviction_mode == bucket_pb2.EVICTION_MODE_VALUE_ONLY

    def test_settings_meaning_the_default_are_left_out(self, builder: BucketMgmtRequestBuilder) -> None:
        settings = CreateBucketSettings(name='travel',
                                        minimum_durability_level=DurabilityLevel.NONE,
                                        storage_backend=StorageBackend.UNDEFINED)
        assert _set_fields(builder.build_create_bucket_request(settings, None).message) == {'bucket_name'}

    @pytest.mark.parametrize('settings', [
        CreateBucketSettings(name='travel', bucket_type=BucketType.MEMCACHED),
        CreateBucketSettings(name='travel', num_vbuckets=128),
    ])
    def test_what_the_gateway_cannot_create(self, builder: BucketMgmtRequestBuilder,
                                            settings: CreateBucketSettings) -> None:
        with pytest.raises(FeatureUnavailableException, match='couchbase2'):
            builder.build_create_bucket_request(settings, None)

    def test_an_unknown_member_is_refused(self, builder: BucketMgmtRequestBuilder) -> None:
        with pytest.raises(InvalidArgumentException, match='compression_mode'):
            builder.build_create_bucket_request(
                CreateBucketSettings(name='travel', compression_mode=CompressionMode.UNKNOWN), None)

    def test_an_update_leaves_out_what_is_fixed_at_creation(self, builder: BucketMgmtRequestBuilder) -> None:
        settings = BucketSettings(name='travel',
                                  bucket_type=BucketType.COUCHBASE,
                                  storage_backend=StorageBackend.MAGMA,
                                  replica_index=True,
                                  minimum_durability_level=DurabilityLevel.NONE,
                                  max_expiry=timedelta(0),
                                  ram_quota_mb=256)
        message = builder.build_update_bucket_request(settings, None).message
        assert _set_fields(message) == {'bucket_name', 'max_expiry_secs', 'ram_quota_mb'}
        assert message.HasField('max_expiry_secs') and message.max_expiry_secs == 0

    @pytest.mark.parametrize('level, listed', [(DurabilityLevel.NONE, True),
                                               (DurabilityLevel.MAJORITY, False),
                                               (None, False)])
    def test_only_an_update_to_no_durability_lists_first(self, builder: BucketMgmtRequestBuilder,
                                                         level: DurabilityLevel | None, listed: bool) -> None:
        settings = BucketSettings(name='travel', minimum_durability_level=level)
        assert (builder.build_update_bucket_request(settings, None).listing is not None) is listed

    def test_max_ttl_is_read_as_max_expiry(self, builder: BucketMgmtRequestBuilder) -> None:
        alone = builder.build_update_bucket_request(BucketSettings(name='travel', max_ttl=30), None).message
        both = builder.build_update_bucket_request(
            BucketSettings(name='travel', max_ttl=30, max_expiry=timedelta(seconds=60)), None).message
        assert (alone.max_expiry_secs, both.max_expiry_secs) == (30, 60)

    @pytest.mark.parametrize('bucket_name', [None, '', ' ', 7])
    def test_a_bucket_name_that_is_not_a_name_is_refused(self, builder: BucketMgmtRequestBuilder,
                                                         bucket_name: Any) -> None:
        with pytest.raises(InvalidArgumentException, match='bucket_name'):
            builder.build_drop_bucket_request(bucket_name, None)

    def test_the_timeout_defaults_to_seventy_five_seconds(self, builder: BucketMgmtRequestBuilder) -> None:
        assert builder.build_get_all_buckets_request(None).timeout == 75.0


class BucketSettingsUnitTests:

    def _read(self, bucket: ListedBucket) -> BucketSettings:
        [settings] = bucket_settings(bucket_pb2.ListBucketsResponse(buckets=[bucket]))
        return settings

    def test_every_setting_of_a_listed_bucket(self) -> None:
        settings = self._read(ListedBucket(bucket_name='travel',
                                           flush_enabled=True,
                                           ram_quota_mb=100,
                                           num_replicas=1,
                                           replica_indexes=True,
                                           bucket_type=bucket_pb2.BUCKET_TYPE_COUCHBASE,
                                           eviction_mode=bucket_pb2.EVICTION_MODE_FULL,
                                           max_expiry_secs=500,
                                           compression_mode=bucket_pb2.COMPRESSION_MODE_PASSIVE,
                                           minimum_durability_level=kv_pb2.DURABILITY_LEVEL_PERSIST_TO_MAJORITY,
                                           storage_backend=bucket_pb2.STORAGE_BACKEND_MAGMA,
                                           conflict_resolution_type=bucket_pb2.CONFLICT_RESOLUTION_TYPE_TIMESTAMP,
                                           history_retention_collection_default=True,
                                           history_retention_bytes=2**31,
                                           history_retention_duration_secs=86400))
        assert dict(settings) == {'name': 'travel',
                                  'flush_enabled': True,
                                  'ram_quota_mb': 100,
                                  'num_replicas': 1,
                                  'replica_index': True,
                                  'bucket_type': BucketType.COUCHBASE,
                                  'eviction_policy': EvictionPolicyType.FULL,
                                  'max_expiry': timedelta(seconds=500),
                                  'compression_mode': CompressionMode.PASSIVE,
                                  'minimum_durability_level': DurabilityLevel.PERSIST_TO_MAJORITY,
                                  'storage_backend': StorageBackend.MAGMA,
                                  'conflict_resolution_type': ConflictResolutionType.TIMESTAMP,
                                  'history_retention_collection_default': True,
                                  'history_retention_bytes': 2**31,
                                  'history_retention_duration': timedelta(days=1)}

    def test_what_the_gateway_leaves_out(self) -> None:
        settings = self._read(ListedBucket(bucket_name='travel', bucket_type=bucket_pb2.BUCKET_TYPE_EPHEMERAL))
        assert settings.minimum_durability_level == DurabilityLevel.NONE
        assert settings.storage_backend == StorageBackend.UNDEFINED
        assert settings.history_retention_collection_default is None
        assert settings.history_retention_bytes is None
        assert settings.history_retention_duration is None

    def test_a_value_newer_than_the_sdk(self) -> None:
        settings = self._read(ListedBucket(bucket_name='travel',
                                           bucket_type=cast('bucket_pb2.BucketType', 99),
                                           storage_backend=cast('bucket_pb2.StorageBackend', 99)))
        assert (settings.bucket_type, settings.storage_backend) == (BucketType.UNKNOWN, StorageBackend.UNDEFINED)

    def test_a_bucket_not_listed(self) -> None:
        with pytest.raises(BucketDoesNotExistException):
            named_bucket_settings(bucket_pb2.ListBucketsResponse(buckets=[ListedBucket(bucket_name='a')]), 'b')


class BucketMgmtErrorsUnitTests:

    def test_a_setting_needing_developer_preview_is_unavailable(self) -> None:
        message = "conflict resolution type 'custom' is supported only with developer preview enabled"
        with pytest.raises(FeatureUnavailableException, match='developer preview'):
            with bucket_mgmt_errors():
                raise InvalidArgumentException(message=message)

    def test_any_other_invalid_argument_is_left_alone(self) -> None:
        with pytest.raises(InvalidArgumentException, match='ram quota'):
            with bucket_mgmt_errors():
                raise InvalidArgumentException(message='ram quota too small')


@pytest.fixture(scope='module')
def certificates(tmp_path_factory: pytest.TempPathFactory) -> Certificates:
    made = make_certificates(tmp_path_factory.mktemp('pki'))
    if made is None:
        pytest.skip('openssl is not on PATH, so no certificates can be made for a TLS server')
    return made


@pytest.fixture
def gateway() -> FakeGateway:
    return new_gateway()


@pytest.fixture
def connect(gateway: FakeGateway, certificates: Certificates) -> Iterator[Callable[[], tuple[str, ClusterOptions]]]:
    with serving(gateway, certificates) as port:
        yield lambda: (f'couchbase2://localhost:{port}',
                       ClusterOptions(PasswordAuthenticator('Administrator', 'password'),
                                      trust_store_path=str(certificates.ca)))


@pytest.fixture
def manager(connect: Callable[[], tuple[str, ClusterOptions]]) -> Iterator[BucketMgmtImpl]:
    impl = ClusterImpl(*connect())
    try:
        yield impl.bucket_mgmt()
    finally:
        impl.close_connection()


class BucketMgmtImplUnitTests:

    def test_a_created_bucket_is_listed(self, manager: BucketMgmtImpl) -> None:
        manager.create_bucket(CreateBucketSettings(name='travel', ram_quota_mb=100, flush_enabled=True))
        assert [b.name for b in manager.get_all_buckets()] == ['travel']
        settings = manager.get_bucket('travel')
        assert (settings.ram_quota_mb, settings.flush_enabled) == (100, True)

    def test_an_update_a_flush_and_a_drop(self, manager: BucketMgmtImpl, gateway: FakeGateway) -> None:
        manager.create_bucket(CreateBucketSettings(name='travel', ram_quota_mb=100))
        manager.update_bucket(BucketSettings(name='travel', max_expiry=timedelta(seconds=500)))
        assert manager.get_bucket('travel').max_expiry == timedelta(seconds=500)
        manager.flush_bucket('travel')
        manager.drop_bucket('travel')
        assert manager.get_all_buckets() == []
        assert [c.method for c in gateway.calls].count('FlushBucket') == 1

    def test_clearing_a_durability_level_is_unavailable(self, manager: BucketMgmtImpl, gateway: FakeGateway) -> None:
        manager.create_bucket(CreateBucketSettings(name='travel', minimum_durability_level=DurabilityLevel.MAJORITY))
        with pytest.raises(FeatureUnavailableException, match='minimum durability level'):
            manager.update_bucket(BucketSettings(name='travel', minimum_durability_level=DurabilityLevel.NONE))
        assert 'UpdateBucket' not in [c.method for c in gateway.calls]

    def test_no_durability_level_on_a_bucket_without_one_updates(self, manager: BucketMgmtImpl,
                                                                 gateway: FakeGateway) -> None:
        manager.create_bucket(CreateBucketSettings(name='travel', ram_quota_mb=100))
        manager.update_bucket(BucketSettings(name='travel', ram_quota_mb=200,
                                             minimum_durability_level=DurabilityLevel.NONE))
        assert manager.get_bucket('travel').ram_quota_mb == 200
        assert [c.method for c in gateway.calls][-3:-1] == ['ListBuckets', 'UpdateBucket']

    def test_the_timeout_defaults_to_seventy_five_seconds(self, manager: BucketMgmtImpl,
                                                          gateway: FakeGateway) -> None:
        manager.get_all_buckets()
        assert 74.0 < gateway.calls[-1].remaining <= 75.0

    def test_a_bucket_that_exists(self, manager: BucketMgmtImpl) -> None:
        manager.create_bucket(CreateBucketSettings(name='travel'))
        with pytest.raises(BucketAlreadyExistsException):
            manager.create_bucket(CreateBucketSettings(name='travel'))

    @pytest.mark.parametrize('call', [
        lambda m: m.get_bucket('missing'),
        lambda m: m.update_bucket(BucketSettings(name='missing', ram_quota_mb=100)),
        lambda m: m.drop_bucket('missing'),
        lambda m: m.flush_bucket('missing'),
    ])
    def test_a_bucket_that_does_not_exist(self, manager: BucketMgmtImpl,
                                          call: Callable[[BucketMgmtImpl], object]) -> None:
        with pytest.raises(BucketDoesNotExistException):
            call(manager)

    def test_describing_a_bucket_is_unavailable(self, manager: BucketMgmtImpl) -> None:
        with pytest.raises(FeatureUnavailableException):
            manager.bucket_describe('travel')


class AsyncBucketMgmtImplUnitTests:

    def test_a_created_bucket_is_read(self, connect: Callable[[], tuple[str, ClusterOptions]]) -> None:
        async def run() -> BucketSettings:
            impl = AsyncClusterImpl(*connect())
            try:
                manager = impl.bucket_mgmt()
                await manager.create_bucket(CreateBucketSettings(name='travel', ram_quota_mb=100))
                return await manager.get_bucket('travel')
            finally:
                await impl.close_connection()

        assert run_on_own_loop(run()).ram_quota_mb == 100

    def test_clearing_a_durability_level_is_unavailable(self,
                                                        connect: Callable[[], tuple[str, ClusterOptions]]) -> None:
        async def run() -> None:
            impl = AsyncClusterImpl(*connect())
            try:
                manager = impl.bucket_mgmt()
                await manager.create_bucket(CreateBucketSettings(name='travel',
                                                                 minimum_durability_level=DurabilityLevel.MAJORITY))
                await manager.update_bucket(BucketSettings(name='travel',
                                                           minimum_durability_level=DurabilityLevel.NONE))
            finally:
                await impl.close_connection()

        with pytest.raises(FeatureUnavailableException, match='minimum durability level'):
            run_on_own_loop(run())

    def test_a_bucket_that_does_not_exist(self, connect: Callable[[], tuple[str, ClusterOptions]]) -> None:
        async def run() -> None:
            impl = AsyncClusterImpl(*connect())
            try:
                await impl.bucket_mgmt().drop_bucket('missing')
            finally:
                await impl.close_connection()

        with pytest.raises(BucketDoesNotExistException):
            run_on_own_loop(run())
