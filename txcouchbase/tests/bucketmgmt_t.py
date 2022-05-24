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
from random import choice

import pytest

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

from ._test_utils import TestEnvironment, run_in_reactor_thread


@pytest.mark.flaky(reruns=5, reruns_delay=1)
class BucketManagementTests:

    @pytest.fixture(scope="class")
    def test_buckets(self):
        return [f"test-bucket-{i}" for i in range(5)]

    @pytest.fixture(scope="class", name="cb_env")
    def couchbase_test_environment(self, couchbase_config, test_buckets):
        cb_env = TestEnvironment.get_environment(__name__, couchbase_config, manage_buckets=True)
        yield cb_env
        cb_env.purge_buckets(test_buckets)

    @pytest.fixture(scope="class")
    def check_bucket_mgmt_supported(self, cb_env):
        cb_env.check_if_feature_supported('bucket_mgmt')

    @pytest.fixture(scope="class")
    def check_bucket_min_durability_supported(self, cb_env):
        cb_env.check_if_feature_supported('bucket_min_durability')

    @pytest.fixture(scope="class")
    def check_bucket_storage_backend_supported(self, cb_env):
        cb_env.check_if_feature_supported('bucket_storage_backend')

    @pytest.fixture(scope="class")
    def check_custom_conflict_resolution_supported(self, cb_env):
        cb_env.check_if_feature_supported('custom_conflict_resolution')

    @pytest.fixture()
    def test_bucket(self, test_buckets):
        return choice(test_buckets)

    @pytest.fixture()
    def purge_buckets(self, cb_env, test_buckets):
        yield
        cb_env.purge_buckets(test_buckets)

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    @pytest.mark.usefixtures("purge_buckets")
    def test_bucket_create(self, cb_env, test_bucket):
        run_in_reactor_thread(cb_env.bm.create_bucket,
                              CreateBucketSettings(
                                  name=test_bucket,
                                  bucket_type=BucketType.COUCHBASE,
                                  ram_quota_mb=100))
        bucket = cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, test_bucket)
        if cb_env.server_version_short >= 6.6:
            assert bucket["minimum_durability_level"] == DurabilityLevel.NONE

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    @pytest.mark.usefixtures("purge_buckets")
    def test_bucket_create_replica_index_true(self, cb_env, test_bucket):
        run_in_reactor_thread(cb_env.bm.create_bucket,
                              CreateBucketSettings(
                                  name=test_bucket,
                                  bucket_type=BucketType.COUCHBASE,
                                  ram_quota_mb=100,
                                  replica_index=True))
        bucket = cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, test_bucket)
        assert bucket.replica_index is True

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    @pytest.mark.usefixtures("purge_buckets")
    def test_bucket_create_replica_index_false(self, cb_env, test_bucket):
        run_in_reactor_thread(cb_env.bm.create_bucket,
                              CreateBucketSettings(
                                  name=test_bucket,
                                  bucket_type=BucketType.COUCHBASE,
                                  ram_quota_mb=100,
                                  replica_index=False))
        bucket = cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, test_bucket)
        assert bucket.replica_index is False

    @pytest.mark.usefixtures("check_bucket_min_durability_supported")
    @pytest.mark.usefixtures("purge_buckets")
    def test_bucket_create_durability(self, cb_env, test_bucket):

        min_durability = DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE
        run_in_reactor_thread(cb_env.bm.create_bucket,
                              CreateBucketSettings(name=test_bucket,
                                                   bucket_type=BucketType.COUCHBASE,
                                                   ram_quota_mb=100,
                                                   minimum_durability_level=min_durability))
        bucket = cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, test_bucket)
        assert bucket["minimum_durability_level"] == min_durability

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    @pytest.mark.usefixtures("purge_buckets")
    def test_bucket_create_fail(self, cb_env, test_bucket):
        settings = CreateBucketSettings(
            name=test_bucket,
            bucket_type=BucketType.COUCHBASE,
            ram_quota_mb=100)
        run_in_reactor_thread(cb_env.bm.create_bucket, settings)
        with pytest.raises(BucketAlreadyExistsException):
            run_in_reactor_thread(cb_env.bm.create_bucket, settings)

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    def test_bucket_drop_fail(self, cb_env, test_bucket):
        settings = CreateBucketSettings(
            name=test_bucket,
            bucket_type=BucketType.COUCHBASE,
            ram_quota_mb=100)
        run_in_reactor_thread(cb_env.bm.create_bucket, settings)
        cb_env.try_n_times(10, 1, cb_env.bm.drop_bucket, test_bucket)
        with pytest.raises(BucketDoesNotExistException):
            run_in_reactor_thread(cb_env.bm.drop_bucket, test_bucket)

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    @pytest.mark.usefixtures("purge_buckets")
    def test_bucket_list(self, cb_env, test_buckets):
        for bucket in test_buckets:
            run_in_reactor_thread(cb_env.bm.create_bucket,
                                  CreateBucketSettings(
                                      name=bucket,
                                      ram_quota_mb=100))
            cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, bucket)

        buckets = run_in_reactor_thread(cb_env.bm.get_all_buckets)
        assert set() == set(test_buckets).difference(
            set(map(lambda b: b.name, buckets)))

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    @pytest.mark.usefixtures("purge_buckets")
    def test_cluster_sees_bucket(self, cb_env, test_bucket):
        # Create the bucket
        run_in_reactor_thread(cb_env.bm.create_bucket,
                              CreateBucketSettings(
                                  name=test_bucket,
                                  ram_quota_mb=100))
        cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, test_bucket)
        # cluster should be able to return it (though, not right away)
        cb_env.try_n_times(10, 2, cb_env.cluster.bucket, test_bucket)
        b = cb_env.cluster.bucket(test_bucket)
        assert b is not None

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    @pytest.mark.usefixtures("purge_buckets")
    def test_change_expiry(self, cb_env, test_bucket):
        # Create the bucket
        run_in_reactor_thread(cb_env.bm.create_bucket,
                              CreateBucketSettings(
                                  name=test_bucket,
                                  ram_quota_mb=100))
        cb_env.try_n_times(10, 3, cb_env.bm.get_bucket, test_bucket)

        # change bucket TTL
        cb_env.try_n_times(10, 3, cb_env.bm.update_bucket, BucketSettings(
            name=test_bucket, max_expiry=timedelta(seconds=500)))

        def get_bucket_expiry_equal(name, expiry):
            b = run_in_reactor_thread(cb_env.bm.get_bucket, name)

            if not expiry == b.max_expiry:
                raise Exception("not equal")

        cb_env.try_n_times(10, 3, get_bucket_expiry_equal, test_bucket, timedelta(seconds=500), is_deferred=False)

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    @pytest.mark.usefixtures("purge_buckets")
    def test_bucket_flush(self, cb_env, test_bucket):
        # Create the bucket
        run_in_reactor_thread(cb_env.bm.create_bucket,
                              CreateBucketSettings(
                                  name=test_bucket,
                                  ram_quota_mb=100,
                                  flush_enabled=True))
        bucket = cb_env.try_n_times(10, 3, cb_env.bm.get_bucket, test_bucket)
        assert bucket.flush_enabled is True
        # flush the bucket
        cb_env.try_n_times(10, 3, cb_env.bm.flush_bucket, bucket.name)

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    @pytest.mark.usefixtures("purge_buckets")
    def test_bucket_flush_fail(self, cb_env, test_bucket):
        # Create the bucket
        run_in_reactor_thread(cb_env.bm.create_bucket,
                              CreateBucketSettings(
                                  name=test_bucket,
                                  ram_quota_mb=100,
                                  flush_enabled=False))
        bucket = cb_env.try_n_times(10, 3, cb_env.bm.get_bucket, test_bucket)
        assert bucket.flush_enabled is False

        with pytest.raises(BucketNotFlushableException):
            run_in_reactor_thread(cb_env.bm.flush_bucket, test_bucket)

    @pytest.mark.usefixtures("check_bucket_storage_backend_supported")
    @pytest.mark.usefixtures("purge_buckets")
    def test_bucket_backend_default(self, cb_env, test_bucket):
        # Create the bucket
        run_in_reactor_thread(cb_env.bm.create_bucket,
                              CreateBucketSettings(
                                  name=test_bucket,
                                  ram_quota_mb=100,
                                  flush_enabled=False))
        bucket = cb_env.try_n_times(10, 3, cb_env.bm.get_bucket, test_bucket)
        assert bucket.storage_backend == StorageBackend.COUCHSTORE

    @pytest.mark.usefixtures("check_bucket_storage_backend_supported")
    @pytest.mark.usefixtures("purge_buckets")
    def test_bucket_backend_magma(self, cb_env, test_bucket):
        # Create the bucket
        run_in_reactor_thread(cb_env.bm.create_bucket,
                              CreateBucketSettings(
                                  name=test_bucket,
                                  ram_quota_mb=1024,
                                  flush_enabled=False,
                                  storage_backend=StorageBackend.MAGMA))
        bucket = cb_env.try_n_times(10, 3, cb_env.bm.get_bucket, test_bucket)
        assert bucket.storage_backend == StorageBackend.MAGMA

    @pytest.mark.usefixtures("check_bucket_storage_backend_supported")
    @pytest.mark.usefixtures("purge_buckets")
    def test_bucket_backend_ephemeral(self, cb_env, test_bucket):
        # Create the bucket
        run_in_reactor_thread(cb_env.bm.create_bucket,
                              CreateBucketSettings(
                                  name=test_bucket,
                                  ram_quota_mb=100,
                                  bucket_type=BucketType.EPHEMERAL,
                                  flush_enabled=False))
        bucket = cb_env.try_n_times(10, 3, cb_env.bm.get_bucket, test_bucket)
        assert bucket.storage_backend == StorageBackend.UNDEFINED

    @pytest.mark.usefixtures("check_custom_conflict_resolution_supported")
    @pytest.mark.usefixtures("purge_buckets")
    def test_bucket_custom_conflict_resolution(self, cb_env, test_bucket):

        if cb_env.is_developer_preview:
            # Create the bucket
            run_in_reactor_thread(cb_env.bm.create_bucket,
                                  CreateBucketSettings(
                                      name=test_bucket,
                                      ram_quota_mb=100,
                                      conflict_resolution_type=ConflictResolutionType.CUSTOM,
                                      flush_enabled=False))
            bucket = cb_env.try_n_times(10, 3, cb_env.bm.get_bucket, test_bucket)
            assert bucket.conflict_resolution_type == ConflictResolutionType.CUSTOM
        else:
            with pytest.raises(FeatureUnavailableException):
                run_in_reactor_thread(cb_env.bm.create_bucket,
                                      CreateBucketSettings(
                                          name=test_bucket,
                                          ram_quota_mb=100,
                                          conflict_resolution_type=ConflictResolutionType.CUSTOM,
                                          flush_enabled=False))
