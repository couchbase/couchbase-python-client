#  Copyright 2016-2022. Couchbase, Inc.
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

from couchbase.bucket import Bucket
from couchbase.durability import DurabilityLevel
from couchbase.exceptions import (BucketAlreadyExistsException,
                                  BucketDoesNotExistException,
                                  BucketNotFlushableException,
                                  FeatureUnavailableException)
from couchbase.management.buckets import (BucketSettings,
                                          BucketType,
                                          ConflictResolutionType,
                                          CreateBucketSettings,
                                          StorageBackend)
from tests.environments import CollectionType
from tests.environments.bucket_mgmt_environment import BucketManagementTestEnvironment
from tests.environments.test_environment import TestEnvironment
from tests.test_features import EnvironmentFeatures


class BucketManagementTestSuite:
    TEST_MANIFEST = [
        'test_bucket_backend_default',
        'test_bucket_backend_ephemeral',
        'test_bucket_backend_magma',
        'test_bucket_create',
        'test_bucket_create_durability',
        'test_bucket_create_fail',
        'test_bucket_create_replica_index_false',
        'test_bucket_create_replica_index_true',
        'test_bucket_custom_conflict_resolution',
        'test_bucket_drop_fail',
        'test_bucket_flush',
        'test_bucket_flush_fail',
        'test_bucket_list',
        'test_change_expiry',
        'test_cluster_sees_bucket',
    ]

    @pytest.fixture(scope='class')
    def check_bucket_mgmt_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('bucket_mgmt',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture(scope='class')
    def check_bucket_min_durability_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('bucket_min_durability',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture(scope='class')
    def check_bucket_storage_backend_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('bucket_storage_backend',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture(scope='class')
    def check_custom_conflict_resolution_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('custom_conflict_resolution',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture()
    def drop_bucket(self, cb_env):
        yield
        cb_env.drop_bucket()

    @pytest.mark.usefixtures('check_bucket_storage_backend_supported')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_backend_default(self, cb_env):
        bucket_name = cb_env.get_bucket_name()
        # Create the bucket
        cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                ram_quota_mb=100,
                flush_enabled=False))
        bucket = TestEnvironment.try_n_times(10, 3, cb_env.bm.get_bucket, bucket_name)
        assert bucket.storage_backend == StorageBackend.COUCHSTORE

    @pytest.mark.usefixtures('check_bucket_storage_backend_supported')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_backend_ephemeral(self, cb_env):
        bucket_name = cb_env.get_bucket_name()
        # Create the bucket
        cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                ram_quota_mb=100,
                bucket_type=BucketType.EPHEMERAL,
                flush_enabled=False))
        bucket = TestEnvironment.try_n_times(10, 3, cb_env.bm.get_bucket, bucket_name)
        assert bucket.storage_backend == StorageBackend.UNDEFINED

    @pytest.mark.usefixtures('check_bucket_storage_backend_supported')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_backend_magma(self, cb_env):
        bucket_name = cb_env.get_bucket_name()
        # Create the bucket
        cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                ram_quota_mb=1024,
                flush_enabled=False,
                storage_backend=StorageBackend.MAGMA))
        bucket = TestEnvironment.try_n_times(10, 3, cb_env.bm.get_bucket, bucket_name)
        assert bucket.storage_backend == StorageBackend.MAGMA

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_create(self, cb_env):
        bucket_name = cb_env.get_bucket_name()
        cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                bucket_type=BucketType.COUCHBASE,
                ram_quota_mb=100))
        bucket = TestEnvironment.try_n_times(10, 1, cb_env.bm.get_bucket, bucket_name)
        if cb_env.server_version_short >= 6.6:
            assert bucket['minimum_durability_level'] == DurabilityLevel.NONE

    @pytest.mark.usefixtures('check_bucket_min_durability_supported')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_create_durability(self, cb_env):
        bucket_name = cb_env.get_bucket_name()
        min_durability = DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE
        cb_env.bm.create_bucket(CreateBucketSettings(name=bucket_name,
                                                     bucket_type=BucketType.COUCHBASE,
                                                     ram_quota_mb=100,
                                                     minimum_durability_level=min_durability))
        bucket = TestEnvironment.try_n_times(10, 1, cb_env.bm.get_bucket, bucket_name)
        assert bucket["minimum_durability_level"] == min_durability

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_create_fail(self, cb_env):
        bucket_name = cb_env.get_bucket_name()
        settings = CreateBucketSettings(
            name=bucket_name,
            bucket_type=BucketType.COUCHBASE,
            ram_quota_mb=100)
        cb_env.bm.create_bucket(settings)
        with pytest.raises(BucketAlreadyExistsException):
            cb_env.bm.create_bucket(settings)

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_create_replica_index_false(self, cb_env):
        bucket_name = cb_env.get_bucket_name()
        cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                bucket_type=BucketType.COUCHBASE,
                ram_quota_mb=100,
                replica_index=False))
        bucket = TestEnvironment.try_n_times(10, 1, cb_env.bm.get_bucket, bucket_name)
        assert bucket.replica_index is False

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_create_replica_index_true(self, cb_env):
        bucket_name = cb_env.get_bucket_name()
        cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                bucket_type=BucketType.COUCHBASE,
                ram_quota_mb=100,
                replica_index=True))
        bucket = TestEnvironment.try_n_times(10, 1, cb_env.bm.get_bucket, bucket_name)
        assert bucket.replica_index is True

    @pytest.mark.usefixtures('check_custom_conflict_resolution_supported')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_custom_conflict_resolution(self, cb_env):
        bucket_name = cb_env.get_bucket_name()
        if cb_env.is_developer_preview:
            # Create the bucket
            cb_env.bm.create_bucket(
                CreateBucketSettings(
                    name=bucket_name,
                    ram_quota_mb=100,
                    conflict_resolution_type=ConflictResolutionType.CUSTOM,
                    flush_enabled=False))
            bucket = TestEnvironment.try_n_times(10, 3, cb_env.bm.get_bucket, bucket_name)
            assert bucket.conflict_resolution_type == ConflictResolutionType.CUSTOM
        else:
            with pytest.raises(FeatureUnavailableException):
                cb_env.bm.create_bucket(
                    CreateBucketSettings(
                        name=bucket_name,
                        ram_quota_mb=100,
                        conflict_resolution_type=ConflictResolutionType.CUSTOM,
                        flush_enabled=False))

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    def test_bucket_drop_fail(self, cb_env):
        bucket_name = cb_env.get_bucket_name()
        settings = CreateBucketSettings(
            name=bucket_name,
            bucket_type=BucketType.COUCHBASE,
            ram_quota_mb=100)
        cb_env.bm.create_bucket(settings)
        TestEnvironment.try_n_times(10, 1, cb_env.bm.drop_bucket, bucket_name)
        with pytest.raises(BucketDoesNotExistException):
            cb_env.bm.drop_bucket(bucket_name)

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_flush(self, cb_env):
        bucket_name = cb_env.get_bucket_name()
        # Create the bucket
        cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                ram_quota_mb=100,
                flush_enabled=True))
        bucket = TestEnvironment.try_n_times(10, 3, cb_env.bm.get_bucket, bucket_name)
        assert bucket.flush_enabled is True
        # flush the bucket
        TestEnvironment.try_n_times(10, 3, cb_env.bm.flush_bucket, bucket.name)

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_flush_fail(self, cb_env):
        bucket_name = cb_env.get_bucket_name()
        # Create the bucket
        cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                ram_quota_mb=100,
                flush_enabled=False))
        bucket = TestEnvironment.try_n_times(10, 3, cb_env.bm.get_bucket, bucket_name)
        assert bucket.flush_enabled is False

        with pytest.raises(BucketNotFlushableException):
            cb_env.bm.flush_bucket(bucket_name)

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('drop_bucket')
    def test_bucket_list(self, cb_env):
        bucket_names = cb_env.get_bucket_names()
        for bucket_name in bucket_names:
            cb_env.bm.create_bucket(
                CreateBucketSettings(
                    name=bucket_name,
                    ram_quota_mb=100))
            TestEnvironment.try_n_times(10, 1, cb_env.bm.get_bucket, bucket_name)

        buckets = cb_env.bm.get_all_buckets()
        assert set() == set(bucket_names).difference(set(map(lambda b: b.name, buckets)))

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('drop_bucket')
    def test_change_expiry(self, cb_env):
        bucket_name = cb_env.get_bucket_name()
        # Create the bucket
        cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                ram_quota_mb=100))
        TestEnvironment.try_n_times(10, 3, cb_env.bm.get_bucket, bucket_name)

        # change bucket TTL
        TestEnvironment.try_n_times(10, 3, cb_env.bm.update_bucket, BucketSettings(
            name=bucket_name, max_expiry=timedelta(seconds=500)))

        def get_bucket_expiry_equal(name, expiry):
            b = cb_env.bm.get_bucket(name)

            if not expiry == b.max_expiry:
                raise Exception("not equal")

        TestEnvironment.try_n_times(10, 3, get_bucket_expiry_equal, bucket_name, timedelta(seconds=500))

    @pytest.mark.usefixtures('check_bucket_mgmt_supported')
    @pytest.mark.usefixtures('drop_bucket')
    def test_cluster_sees_bucket(self, cb_env):
        pytest.skip('Skip until seg fault root cause determined.')
        bucket_name = cb_env.get_bucket_name()
        # Create the bucket
        cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                ram_quota_mb=100))
        TestEnvironment.try_n_times(10, 1, cb_env.bm.get_bucket, bucket_name)
        # cluster should be able to return it (though, not right away)
        b = TestEnvironment.try_n_times(10, 2, cb_env.cluster.bucket, bucket_name)
        assert b is not None
        assert isinstance(b, Bucket)


@pytest.mark.flaky(reruns=3, reruns_delay=1)
class ClassicBucketManagementTests(BucketManagementTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicBucketManagementTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicBucketManagementTests) if valid_test_method(meth)]
        compare = set(BucketManagementTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = BucketManagementTestEnvironment.from_environment(cb_base_env)
        cb_env.enable_bucket_mgmt()
        cb_env.setup()
        yield cb_env
        cb_env.teardown()
