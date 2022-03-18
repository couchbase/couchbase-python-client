from datetime import timedelta
from random import choice

import pytest
import pytest_asyncio

from acouchbase.cluster import Cluster, get_event_loop
from couchbase.auth import PasswordAuthenticator
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
from couchbase.options import ClusterOptions

from ._test_utils import TestEnvironment


class BucketManagementTests:

    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest.fixture(scope="class")
    def test_buckets(self):
        return [f"test-bucket-{i}" for i in range(5)]

    @pytest_asyncio.fixture(scope="class", name="cb_env")
    async def couchbase_test_environment(self, couchbase_config, test_buckets):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        opts = ClusterOptions(PasswordAuthenticator(username, pw))
        cluster = Cluster(
            conn_string, opts)
        await cluster.on_connect()
        await cluster.cluster_info()
        b = cluster.bucket(f"{couchbase_config.bucket_name}")
        await b.on_connect()
        coll = b.default_collection()
        cb_env = TestEnvironment(
            cluster, b, coll, couchbase_config, manage_buckets=True)

        yield cb_env
        if cb_env.is_feature_supported('bucket_mgmt'):
            await cb_env.purge_buckets(test_buckets)
        await cluster.close()

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

    # TODO:  more efficient, but cannot seem to get consistent results
    #           multiple tests fail with BucketAlreadyExistsException
    # @pytest_asyncio.fixture()
    # async def purge_bucket(self, cb_env,  # type: TestEnvironment
    #                        test_bucket  # type: str
    #                        ):
    #     yield
    #     await cb_env.purge_buckets(test_bucket)

    @pytest_asyncio.fixture()
    async def purge_buckets(self, cb_env, test_buckets):
        yield
        await cb_env.purge_buckets(test_buckets)

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    @pytest.mark.usefixtures("purge_buckets")
    @pytest.mark.asyncio
    async def test_bucket_create(self, cb_env, test_bucket):
        print(f"test_bucket_create bucket: {test_bucket}")
        await cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=test_bucket,
                bucket_type=BucketType.COUCHBASE,
                ram_quota_mb=100))
        bucket = await cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, test_bucket)
        if cb_env.server_version_short >= 6.6:
            assert bucket["minimum_durability_level"] == DurabilityLevel.NONE

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    @pytest.mark.usefixtures("purge_buckets")
    @pytest.mark.asyncio
    async def test_bucket_create_replica_index_true(self, cb_env, test_bucket):
        await cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=test_bucket,
                bucket_type=BucketType.COUCHBASE,
                ram_quota_mb=100,
                replica_index=True))
        bucket = await cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, test_bucket)
        assert bucket.replica_index is True

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    @pytest.mark.usefixtures("purge_buckets")
    @pytest.mark.asyncio
    async def test_bucket_create_replica_index_false(self, cb_env, test_bucket):
        print(f"test_bucket_create_replica_index_false bucket: {test_bucket}")
        await cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=test_bucket,
                bucket_type=BucketType.COUCHBASE,
                ram_quota_mb=100,
                replica_index=False))
        bucket = await cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, test_bucket)
        assert bucket.replica_index is False

    @pytest.mark.usefixtures("check_bucket_min_durability_supported")
    @pytest.mark.usefixtures("purge_buckets")
    @pytest.mark.asyncio
    async def test_bucket_create_durability(self, cb_env, test_bucket):
        # if cb_env.server_version_short < 6.6:
        #     pytest.skip(
        #         "Bucket minimum durability not available on server version < 6.6")

        min_durability = DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE
        await cb_env.bm.create_bucket(CreateBucketSettings(name=test_bucket,
                                                           bucket_type=BucketType.COUCHBASE,
                                                           ram_quota_mb=100,
                                                           minimum_durability_level=min_durability))
        bucket = await cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, test_bucket)
        assert bucket["minimum_durability_level"] == min_durability

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    @pytest.mark.usefixtures("purge_buckets")
    @pytest.mark.asyncio
    async def test_bucket_create_fail(self, cb_env, test_bucket):
        settings = CreateBucketSettings(
            name=test_bucket,
            bucket_type=BucketType.COUCHBASE,
            ram_quota_mb=100)
        await cb_env.bm.create_bucket(settings)
        with pytest.raises(BucketAlreadyExistsException):
            await cb_env.bm.create_bucket(settings)

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    @pytest.mark.asyncio
    async def test_bucket_drop_fail(self, cb_env, test_bucket):
        settings = CreateBucketSettings(
            name=test_bucket,
            bucket_type=BucketType.COUCHBASE,
            ram_quota_mb=100)
        await cb_env.bm.create_bucket(settings)
        await cb_env.try_n_times(10, 1, cb_env.bm.drop_bucket, test_bucket)
        with pytest.raises(BucketDoesNotExistException):
            await cb_env.bm.drop_bucket(test_bucket)

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    @pytest.mark.usefixtures("purge_buckets")
    @pytest.mark.asyncio
    async def test_bucket_list(self, cb_env, test_buckets):
        for bucket in test_buckets:
            await cb_env.bm.create_bucket(
                CreateBucketSettings(
                    name=bucket,
                    ram_quota_mb=100))
            await cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, bucket)

        buckets = await cb_env.bm.get_all_buckets()
        assert set() == set(test_buckets).difference(
            set(map(lambda b: b.name, buckets)))

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    @pytest.mark.usefixtures("purge_buckets")
    @pytest.mark.asyncio
    async def test_cluster_sees_bucket(self, cb_env, test_bucket):
        # Create the bucket
        await cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=test_bucket,
                ram_quota_mb=100))
        await cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, test_bucket)
        # cluster should be able to return it (though, not right away)
        b = cb_env.cluster.bucket(test_bucket)
        await cb_env.try_n_times(10, 1, b.on_connect)

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    @pytest.mark.usefixtures("purge_buckets")
    @pytest.mark.asyncio
    async def test_change_expiry(self, cb_env, test_bucket):
        # Create the bucket
        await cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=test_bucket,
                ram_quota_mb=100))
        await cb_env.try_n_times(10, 3, cb_env.bm.get_bucket, test_bucket)

        # change bucket TTL
        await cb_env.try_n_times(10, 3, cb_env.bm.update_bucket, BucketSettings(
            name=test_bucket, max_expiry=timedelta(seconds=500)))

        async def get_bucket_expiry_equal(name, expiry):
            b = await cb_env.bm.get_bucket(name)

            if not expiry == b.max_expiry:
                raise Exception("not equal")

        await cb_env.try_n_times(10, 3, get_bucket_expiry_equal, test_bucket, timedelta(seconds=500))

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    @pytest.mark.usefixtures("purge_buckets")
    @pytest.mark.asyncio
    async def test_bucket_flush(self, cb_env, test_bucket):
        # Create the bucket
        await cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=test_bucket,
                ram_quota_mb=100,
                flush_enabled=True))
        bucket = await cb_env.try_n_times(10, 3, cb_env.bm.get_bucket, test_bucket)
        assert bucket.flush_enabled is True
        # flush the bucket
        await cb_env.try_n_times(10, 3, cb_env.bm.flush_bucket, bucket.name)

    @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    @pytest.mark.usefixtures("purge_buckets")
    @pytest.mark.asyncio
    async def test_bucket_flush_fail(self, cb_env, test_bucket):
        # Create the bucket
        await cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=test_bucket,
                ram_quota_mb=100,
                flush_enabled=False))
        bucket = await cb_env.try_n_times(10, 3, cb_env.bm.get_bucket, test_bucket)
        assert bucket.flush_enabled is False

        with pytest.raises(BucketNotFlushableException):
            await cb_env.bm.flush_bucket(test_bucket)

    @pytest.mark.usefixtures("check_bucket_storage_backend_supported")
    @pytest.mark.usefixtures("purge_buckets")
    @pytest.mark.asyncio
    async def test_bucket_backend_default(self, cb_env, test_bucket):
        # if cb_env.server_version_short < 7.1:
        #     pytest.skip(
        #         "Bucket storage backend testing only available on server versions >= 7.1")

        # Create the bucket
        await cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=test_bucket,
                ram_quota_mb=100,
                flush_enabled=False))
        bucket = await cb_env.try_n_times(10, 3, cb_env.bm.get_bucket, test_bucket)
        assert bucket.storage_backend == StorageBackend.COUCHSTORE

    @pytest.mark.usefixtures("check_bucket_storage_backend_supported")
    @pytest.mark.usefixtures("purge_buckets")
    @pytest.mark.asyncio
    async def test_bucket_backend_magma(self, cb_env, test_bucket):
        # if cb_env.server_version_short < 7.1:
        #     pytest.skip(
        #         "Bucket storage backend testing only available on server versions >= 7.1")

        # @TODO:  couchbase++ raises Ram quota for magma must be at least 1024 MiB, this correct?
        with pytest.raises(InvalidArgumentException):
            await cb_env.bm.create_bucket(
                CreateBucketSettings(
                    name=test_bucket,
                    ram_quota_mb=256,
                    flush_enabled=False,
                    storage_backend=StorageBackend.MAGMA))
            bucket = await cb_env.try_n_times(10, 3, cb_env.bm.get_bucket, test_bucket)
            assert bucket.storage_backend == StorageBackend.MAGMA

    @pytest.mark.usefixtures("check_bucket_storage_backend_supported")
    @pytest.mark.usefixtures("purge_buckets")
    @pytest.mark.asyncio
    async def test_bucket_backend_ephemeral(self, cb_env, test_bucket):
        # if cb_env.server_version_short < 7.1:
        #     pytest.skip(
        #         "Bucket storage backend testing only available on server versions >= 7.1")

        # Create the bucket
        await cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=test_bucket,
                ram_quota_mb=100,
                bucket_type=BucketType.EPHEMERAL,
                flush_enabled=False))
        bucket = await cb_env.try_n_times(10, 3, cb_env.bm.get_bucket, test_bucket)
        assert bucket.storage_backend == StorageBackend.UNDEFINED

    @pytest.mark.usefixtures("check_custom_conflict_resolution_supported")
    @pytest.mark.usefixtures("purge_buckets")
    @pytest.mark.asyncio
    async def test_bucket_custom_conflict_resolution(self, cb_env, test_bucket):
        # if cb_env.server_version_short < 7.1:
        #     pytest.skip(
        #         "Custom conflict resolution testing only available on server "
        #         "versions >= 7.1 with developer preview enabled")

        if cb_env.is_developer_preview:
            # Create the bucket
            await cb_env.bm.create_bucket(
                CreateBucketSettings(
                    name=test_bucket,
                    ram_quota_mb=100,
                    conflict_resolution_type=ConflictResolutionType.CUSTOM,
                    flush_enabled=False))
            bucket = await cb_env.try_n_times(10, 3, cb_env.bm.get_bucket, test_bucket)
            assert bucket.conflict_resolution_type == ConflictResolutionType.CUSTOM
        else:
            with pytest.raises(FeatureUnavailableException):
                await cb_env.bm.create_bucket(
                    CreateBucketSettings(
                        name=test_bucket,
                        ram_quota_mb=100,
                        conflict_resolution_type=ConflictResolutionType.CUSTOM,
                        flush_enabled=False))
