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

from collections.abc import Generator
from datetime import timedelta

import pytest

from couchbase.bucket import Bucket
from couchbase.durability import DurabilityLevel
from couchbase.exceptions import (BucketAlreadyExistsException,
                                  BucketDoesNotExistException,
                                  BucketNotFlushableException,
                                  FeatureUnavailableException,
                                  InvalidArgumentException)
from couchbase.management.buckets import (BucketSettings,
                                          BucketType,
                                          ConflictResolutionType,
                                          CreateBucketSettings,
                                          StorageBackend)
from tests.assert_helpers import cb_assert_eq
from tests.environments.managers import TestEnvironmentBucketManager
from tests.environments.test_environment import (CollectionStrategy,
                                                 SyncTestEnvironment,
                                                 TestEnvironment,
                                                 build_environment,
                                                 teardown_environment)
from tests.manifest import validate_test_manifest
from tests.suite_fixtures import TopologySuite
from tests.test_features import EnvironmentFeatures, ServerFeatures


class BucketManagementTestSuite(TopologySuite):
    MANAGERS = [TestEnvironmentBucketManager]
    COLLECTION_STRATEGY = CollectionStrategy.DEFAULT

    TEST_MANIFEST = [
        'test_bucket_backend_default',
        'test_bucket_backend_ephemeral',
        'test_bucket_backend_magma',
        'test_bucket_backend_couchstore',
        'test_bucket_create',
        'test_bucket_create_durability',
        'test_bucket_create_fail',
        'test_bucket_create_replica_index_false',
        'test_bucket_create_replica_index_true',
        'test_bucket_create_zero_replicas',
        'test_bucket_custom_conflict_resolution',
        'test_bucket_create_history_retention',
        'test_bucket_create_history_retention_unsupported',
        'test_bucket_update_history_retention',
        'test_bucket_update_history_retention_unsupported',
        'test_bucket_drop_fail',
        'test_bucket_flush',
        'test_bucket_flush_fail',
        'test_bucket_list',
        'test_change_expiry',
        'test_cluster_sees_bucket',
    ]

    @pytest.fixture(scope='class')
    def check_bucket_mgmt_supported(self, cb_env: SyncTestEnvironment) -> None:
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.BucketManagement, cb_env.server_context)

    @pytest.fixture(scope='class')
    def check_bucket_min_durability_supported(self, cb_env: SyncTestEnvironment) -> None:
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.BucketMinDurability, cb_env.server_context)

    @pytest.fixture(scope='class')
    def check_bucket_storage_backend_supported(self, cb_env: SyncTestEnvironment) -> None:
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.BucketStorageBackend, cb_env.server_context)

    @pytest.fixture(scope='class')
    def check_custom_conflict_resolution_supported(self, cb_env: SyncTestEnvironment) -> None:
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.CustomConflictResolution, cb_env.server_context)

    @pytest.fixture(scope="class")
    def check_non_deduped_history_supported(self, cb_env: SyncTestEnvironment) -> None:
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.NonDedupedHistory, cb_env.server_context)

    def supports_magma_128_buckets(self, cb_env: SyncTestEnvironment) -> bool:
        return EnvironmentFeatures.supports_feature(ServerFeatures.Magma128Buckets, cb_env.server_context) is None

    @pytest.fixture()
    def drop_bucket(self, cb_env: SyncTestEnvironment) -> Generator[None, None, None]:
        yield
        cb_env.drop_bucket()

    @pytest.mark.usefixtures('check_bucket_storage_backend_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_backend_default(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()
        bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                ram_quota_mb=100,
                flush_enabled=False))
        cb_env.consistency.wait_until_bucket_present(bucket_name)
        bucket = bm.get_bucket(bucket_name)
        if self.supports_magma_128_buckets(cb_env):
            assert bucket.storage_backend == StorageBackend.MAGMA
        else:
            assert bucket.storage_backend == StorageBackend.COUCHSTORE

    @pytest.mark.usefixtures('check_bucket_storage_backend_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_backend_ephemeral(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()
        bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                ram_quota_mb=100,
                bucket_type=BucketType.EPHEMERAL,
                flush_enabled=False))
        cb_env.consistency.wait_until_bucket_present(bucket_name)
        bucket = bm.get_bucket(bucket_name)
        assert bucket.storage_backend == StorageBackend.UNDEFINED

    @pytest.mark.usefixtures('check_bucket_storage_backend_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_backend_magma(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()
        bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                ram_quota_mb=1024,
                flush_enabled=False,
                storage_backend=StorageBackend.MAGMA))
        cb_env.consistency.wait_until_bucket_present(bucket_name)
        bucket = bm.get_bucket(bucket_name)
        assert bucket.storage_backend == StorageBackend.MAGMA

    @pytest.mark.usefixtures('check_bucket_storage_backend_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_backend_couchstore(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()
        bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                ram_quota_mb=100,
                flush_enabled=False,
                storage_backend=StorageBackend.COUCHSTORE))
        cb_env.consistency.wait_until_bucket_present(bucket_name)
        bucket = bm.get_bucket(bucket_name)
        assert bucket.storage_backend == StorageBackend.COUCHSTORE

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_create(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()
        bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                bucket_type=BucketType.COUCHBASE,
                ram_quota_mb=100))
        cb_env.consistency.wait_until_bucket_present(bucket_name)
        bucket = bm.get_bucket(bucket_name)
        assert cb_env.server_version_short is not None
        if cb_env.server_version_short >= 6.6:
            assert bucket['minimum_durability_level'] == DurabilityLevel.NONE

    @pytest.mark.usefixtures('check_bucket_min_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_create_durability(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()
        min_durability = DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE
        bm.create_bucket(CreateBucketSettings(name=bucket_name,
                                              bucket_type=BucketType.COUCHBASE,
                                              ram_quota_mb=100,
                                              minimum_durability_level=min_durability))
        cb_env.consistency.wait_until_bucket_present(bucket_name)
        bucket = bm.get_bucket(bucket_name)
        assert bucket["minimum_durability_level"] == min_durability

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_create_fail(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()
        settings = CreateBucketSettings(
            name=bucket_name,
            bucket_type=BucketType.COUCHBASE,
            ram_quota_mb=100)
        bm.create_bucket(settings)
        cb_env.consistency.wait_until_bucket_present(bucket_name)
        with pytest.raises(BucketAlreadyExistsException):
            bm.create_bucket(settings)

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_create_replica_index_false(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()
        bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                bucket_type=BucketType.COUCHBASE,
                storage_backend=StorageBackend.COUCHSTORE,  # Views only supported on CouchStore
                ram_quota_mb=100,
                replica_index=False))
        cb_env.consistency.wait_until_bucket_present(bucket_name)
        bucket = bm.get_bucket(bucket_name)
        assert bucket.replica_index is False

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_create_replica_index_true(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()
        bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                bucket_type=BucketType.COUCHBASE,
                storage_backend=StorageBackend.COUCHSTORE,  # Views only supported on CouchStore
                ram_quota_mb=100,
                replica_index=True))
        cb_env.consistency.wait_until_bucket_present(bucket_name)
        bucket = bm.get_bucket(bucket_name)
        assert bucket.replica_index is True

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_create_zero_replicas(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()
        bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                bucket_type=BucketType.COUCHBASE,
                ram_quota_mb=100,
                num_replicas=0))
        cb_env.consistency.wait_until_bucket_present(bucket_name)
        bucket = bm.get_bucket(bucket_name)
        assert bucket.get('num_replicas') == 0

    @pytest.mark.usefixtures('check_custom_conflict_resolution_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_custom_conflict_resolution(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()
        if cb_env.is_developer_preview:
            bm.create_bucket(
                CreateBucketSettings(
                    name=bucket_name,
                    ram_quota_mb=100,
                    conflict_resolution_type=ConflictResolutionType.CUSTOM,
                    flush_enabled=False))
            cb_env.consistency.wait_until_bucket_present(bucket_name)
            bucket = bm.get_bucket(bucket_name)
            assert bucket.conflict_resolution_type == ConflictResolutionType.CUSTOM
        else:
            with pytest.raises(FeatureUnavailableException):
                bm.create_bucket(
                    CreateBucketSettings(
                        name=bucket_name,
                        ram_quota_mb=100,
                        conflict_resolution_type=ConflictResolutionType.CUSTOM,
                        flush_enabled=False))

    @pytest.mark.usefixtures('check_non_deduped_history_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_create_history_retention(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()
        bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                bucket_type=BucketType.COUCHBASE,
                storage_backend=StorageBackend.MAGMA,
                ram_quota_mb=1024,
                history_retention_collection_default=True,
                history_retention_bytes=2**31,
                history_retention_duration=timedelta(days=1)
            )
        )
        cb_env.consistency.wait_until_bucket_present(bucket_name)
        bucket = bm.get_bucket(bucket_name)
        assert bucket.history_retention_collection_default
        assert bucket.history_retention_bytes == 2**31
        assert bucket.history_retention_duration == timedelta(days=1)

    @pytest.mark.usefixtures('check_non_deduped_history_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_create_history_retention_unsupported(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()

        # History retention not supported for CouchStore
        with pytest.raises(InvalidArgumentException):
            bm.create_bucket(
                CreateBucketSettings(
                    name=bucket_name,
                    bucket_type=BucketType.COUCHBASE,
                    storage_backend=StorageBackend.COUCHSTORE,
                    ram_quota_mb=1024,
                    history_retention_collection_default=True,
                    history_retention_bytes=2**31,
                    history_retention_duration=timedelta(days=1)
                )
            )

    @pytest.mark.usefixtures('check_non_deduped_history_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_update_history_retention(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()

        bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                bucket_type=BucketType.COUCHBASE,
                storage_backend=StorageBackend.MAGMA,
                ram_quota_mb=1024,
            )
        )
        cb_env.consistency.wait_until_bucket_present(bucket_name)
        bucket = bm.get_bucket(bucket_name)
        assert bucket is not None
        assert bucket.history_retention_collection_default
        assert bucket.history_retention_bytes == 0
        assert bucket.history_retention_duration == timedelta(0)

        bm.update_bucket(
            BucketSettings(
                name=bucket_name,
                bucket_type=BucketType.COUCHBASE,
                storage_backend=StorageBackend.MAGMA,
                ram_quota_mb=1024,
                history_retention_collection_default=True,
                history_retention_bytes=2**31,
                history_retention_duration=timedelta(minutes=10)
            )
        )
        bucket = bm.get_bucket(bucket_name)
        assert bucket.history_retention_collection_default
        assert bucket.history_retention_bytes == 2**31
        assert bucket.history_retention_duration == timedelta(minutes=10)

    @pytest.mark.usefixtures('check_non_deduped_history_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_update_history_retention_unsupported(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()

        bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                bucket_type=BucketType.COUCHBASE,
                storage_backend=StorageBackend.COUCHSTORE,
                ram_quota_mb=256,
            )
        )
        cb_env.consistency.wait_until_bucket_present(bucket_name)
        bucket = bm.get_bucket(bucket_name)
        assert bucket is not None
        assert bucket.history_retention_collection_default is None
        assert bucket.history_retention_bytes is None
        assert bucket.history_retention_duration is None

        with pytest.raises(InvalidArgumentException):
            bm.update_bucket(
                BucketSettings(
                    name=bucket_name,
                    bucket_type=BucketType.COUCHBASE,
                    storage_backend=StorageBackend.COUCHSTORE,
                    ram_quota_mb=256,
                    history_retention_collection_default=True,
                    history_retention_bytes=2**31,
                    history_retention_duration=timedelta(minutes=10)
                )
            )

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('check_multi_node')
    def test_bucket_drop_fail(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()
        settings = CreateBucketSettings(
            name=bucket_name,
            bucket_type=BucketType.COUCHBASE,
            ram_quota_mb=100)
        bm.create_bucket(settings)
        cb_env.consistency.wait_until_bucket_present(bucket_name)
        bm.drop_bucket(bucket_name)
        cb_env.consistency.wait_until_bucket_dropped(bucket_name)
        with pytest.raises(BucketDoesNotExistException):
            bm.drop_bucket(bucket_name)

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_flush(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()
        bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                ram_quota_mb=100,
                flush_enabled=True))
        cb_env.consistency.wait_until_bucket_present(bucket_name)
        bucket = bm.get_bucket(bucket_name)
        assert bucket.flush_enabled is True
        TestEnvironment.try_n_times(10, 3, bm.flush_bucket, bucket.name)

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_flush_fail(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()
        bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                ram_quota_mb=100,
                flush_enabled=False))
        cb_env.consistency.wait_until_bucket_present(bucket_name)
        bucket = bm.get_bucket(bucket_name)
        assert bucket.flush_enabled is False

        with pytest.raises(BucketNotFlushableException):
            bm.flush_bucket(bucket_name)

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_list(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_names = cb_env.get_bucket_names()
        for bucket_name in bucket_names:
            bm.create_bucket(
                CreateBucketSettings(
                    name=bucket_name,
                    ram_quota_mb=100))
            cb_env.consistency.wait_until_bucket_present(bucket_name)

        buckets = bm.get_all_buckets()
        assert set() == set(bucket_names).difference(set(map(lambda b: b.name, buckets)))

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('drop_bucket')
    def test_change_expiry(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()
        bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                ram_quota_mb=100))
        cb_env.consistency.wait_until_bucket_present(bucket_name)

        TestEnvironment.try_n_times(10, 3, bm.update_bucket, BucketSettings(
            name=bucket_name, max_expiry=timedelta(seconds=500)))

        def get_bucket_expiry_equal(name: str, expiry: timedelta) -> None:
            # The poll is the assertion: try_n_times re-raises the final failure, so a
            # bucket whose expiry never changes fails the test rather than passing quietly.
            b = bm.get_bucket(name)
            cb_assert_eq(b.max_expiry, expiry, what='bucket max_expiry')

        TestEnvironment.try_n_times(10, 3, get_bucket_expiry_equal, bucket_name, timedelta(seconds=500))

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('drop_bucket')
    def test_cluster_sees_bucket(self, cb_env: SyncTestEnvironment) -> None:
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bucket_name = cb_env.get_bucket_name()
        bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                ram_quota_mb=100))
        cb_env.consistency.wait_until_bucket_present(bucket_name)
        # cluster should be able to return it (though, not right away)
        b = TestEnvironment.try_n_times(10, 2, cb_env.cluster.bucket, bucket_name)
        assert b is not None
        assert isinstance(b, Bucket)


@pytest.mark.xdist_group('bucket_lifecycle')
@pytest.mark.flaky(reruns=3, reruns_delay=1)
class BucketManagementTests(BucketManagementTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(BucketManagementTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, BucketManagementTestSuite)
        env.init_bucket_pool()
        yield env
        env.teardown_bucket_pool()
        teardown_environment(env, BucketManagementTestSuite)
