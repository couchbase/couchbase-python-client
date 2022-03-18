import os
import datetime
from unittest import SkipTest

from flaky import flaky

from couchbase.exceptions import (BucketDoesNotExistException,
                                  BucketAlreadyExistsException,
                                  BucketNotFlushableException)
from couchbase.management.buckets import (ConflictResolutionType, CreateBucketSettings, BucketSettings,
                                          BucketType, StorageBackend)
from couchbase_tests.base import CollectionTestCase
from couchbase_core.durability import Durability


class BucketManagementTests(CollectionTestCase):

    BUCKETS_TO_ADD = {'fred': {}, 'jane': {}, 'sally': {}}

    def setUp(self, *args, **kwargs):
        super(BucketManagementTests, self).setUp(*args, **kwargs)
        self.bm = self.cluster.buckets()
        if not self.is_realserver:
            raise SkipTest('Real server must be used for admin tests')

        self.purge_buckets()

    def tearDown(self):
        self.purge_buckets()

    def purge_buckets(self):
        for bucket, kwargs in BucketManagementTests.BUCKETS_TO_ADD.items():
            try:
                self.bm.drop_bucket(bucket)
            except BucketDoesNotExistException:
                pass
            except Exception as e:
                raise
                # now be sure it is really gone
            self.try_n_times_till_exception(10, 3, self.bm.get_bucket, bucket)

    @flaky(5, 1)
    def test_bucket_create(self):
        self.bm.create_bucket(
            CreateBucketSettings(
                name="fred",
                bucket_type="couchbase",
                ram_quota_mb=100))
        bucket = self.try_n_times(10, 1, self.bm.get_bucket, "fred")
        if float(self.cluster_version[0:3]) >= 6.6:
            self.assertEqual(
                bucket['minimum_durability_level'],
                Durability.NONE)

    @flaky(5, 1)
    def test_bucket_create_replica_index_true(self):
        self.bm.create_bucket(
            CreateBucketSettings(
                name="fred",
                bucket_type="couchbase",
                ram_quota_mb=100,
                replica_index=True))
        bucket = self.try_n_times(10, 1, self.bm.get_bucket, "fred")
        self.assertTrue(bucket.replica_index)

    @flaky(5, 1)
    def test_bucket_create_replica_index_false(self):
        self.bm.create_bucket(
            CreateBucketSettings(
                name="fred",
                bucket_type="couchbase",
                ram_quota_mb=100,
                replica_index=False))
        bucket = self.try_n_times(10, 1, self.bm.get_bucket, "fred")
        self.assertFalse(bucket.replica_index)

    @flaky(5, 1)
    def test_bucket_create_durability(self):
        if float(self.cluster_version[0:3]) < 6.6:
            raise SkipTest(
                "Bucket minimum durability not available on server version < 6.6")
        min_durability = Durability.MAJORITY_AND_PERSIST_TO_ACTIVE
        self.bm.create_bucket(CreateBucketSettings(name="fred",
                                                   bucket_type="couchbase",
                                                   ram_quota_mb=100,
                                                   minimum_durability_level=min_durability))
        bucket = self.try_n_times(10, 1, self.bm.get_bucket, "fred")
        self.assertEqual(bucket['minimum_durability_level'], min_durability)

    @flaky(5, 1)
    def test_bucket_create_fail(self):
        settings = CreateBucketSettings(
            name='fred',
            bucket_type=BucketType.COUCHBASE,
            ram_quota_mb=100)
        self.bm.create_bucket(settings)
        self.assertRaises(
            BucketAlreadyExistsException,
            self.bm.create_bucket,
            settings)

    @flaky(5, 1)
    def test_bucket_drop_fail(self):
        settings = CreateBucketSettings(
            name='fred',
            bucket_type=BucketType.COUCHBASE,
            ram_quota_mb=100)
        self.bm.create_bucket(settings)
        self.try_n_times(10, 1, self.bm.drop_bucket, 'fred')
        self.assertRaises(
            BucketDoesNotExistException,
            self.bm.drop_bucket,
            'fred')

    @flaky(5, 1)
    def test_bucket_list(self):
        for bucket, kwargs in BucketManagementTests.BUCKETS_TO_ADD.items():
            self.bm.create_bucket(
                CreateBucketSettings(
                    name=bucket,
                    ram_quota_mb=100,
                    **kwargs))
            self.try_n_times(10, 1, self.bm.get_bucket, bucket)

        self.assertEqual(set(), {"fred", "jane", "sally"}.difference(
            set(map(lambda x: x.name, self.bm.get_all_buckets()))))

    @flaky(5, 1)
    def test_cluster_sees_bucket(self):
        # Create the bucket
        self.bm.create_bucket(
            CreateBucketSettings(
                name='fred',
                ram_quota_mb=100))
        # cluster should be able to return it (though, not right away)
        self.try_n_times(10, 3, self.cluster.bucket, 'fred')

    @flaky(5, 1)
    def test_change_ttl(self):
        # Create the bucket
        self.bm.create_bucket(
            CreateBucketSettings(
                name='fred',
                ram_quota_mb=100))
        self.try_n_times(10, 3, self.bm.get_bucket, 'fred')

        # change bucket TTL
        self.try_n_times(10, 3, self.bm.update_bucket, BucketSettings(
            name='fred', max_ttl=datetime.timedelta(seconds=500)))

        def get_bucket_ttl_equal(name, ttl):
            if not ttl == self.bm.get_bucket(name).max_ttl:
                raise Exception("not equal")

        self.try_n_times(
            10,
            3,
            get_bucket_ttl_equal,
            'fred',
            datetime.timedelta(
                seconds=500))

    @flaky(5, 1)
    def test_bucket_flush(self):
        # Create the bucket
        self.bm.create_bucket(
            CreateBucketSettings(
                name='fred',
                ram_quota_mb=100,
                flush_enabled=True))
        bucket = self.try_n_times(10, 3, self.bm.get_bucket, 'fred')
        self.assertTrue(bucket.flush_enabled)
        # flush the bucket
        self.try_n_times(10, 3, self.bm.flush_bucket, bucket.name)

    @flaky(5, 1)
    def test_bucket_flush_fail(self):
        # Create the bucket
        self.bm.create_bucket(
            CreateBucketSettings(
                name='fred',
                ram_quota_mb=100,
                flush_enabled=False))
        bucket = self.try_n_times(10, 3, self.bm.get_bucket, 'fred')
        self.assertFalse(bucket.flush_enabled)

        # verify appropriate Exception when flush attempted
        self.assertRaises(
            BucketNotFlushableException,
            self.bm.flush_bucket,
            'fred')

    @flaky(5, 1)
    def test_bucket_backend_default(self):
        version = self.cluster.get_server_version()
        if version.short_version < 7.1:
            raise SkipTest(
                "Bucket storage backend testing only available on server versions >= 7.1")

        # Create the bucket
        self.bm.create_bucket(
            CreateBucketSettings(
                name='fred',
                ram_quota_mb=100,
                flush_enabled=False))
        bucket = self.try_n_times(10, 3, self.bm.get_bucket, 'fred')
        self.assertEqual(bucket.storage_backend, StorageBackend.COUCHSTORE)

    @flaky(5, 1)
    def test_bucket_backend_magma(self):
        version = self.cluster.get_server_version()
        if version.short_version < 7.1:
            raise SkipTest(
                "Bucket storage backend testing only available on server versions >= 7.1")

        # Create the bucket
        self.bm.create_bucket(
            CreateBucketSettings(
                name='fred',
                ram_quota_mb=256,
                flush_enabled=False,
                storage_backend=StorageBackend.MAGMA))
        bucket = self.try_n_times(10, 3, self.bm.get_bucket, 'fred')
        self.assertEqual(bucket.storage_backend, StorageBackend.MAGMA)

    @flaky(5, 1)
    def test_bucket_backend_ephemeral(self):
        version = self.cluster.get_server_version()
        if version.short_version < 7.1:
            raise SkipTest(
                "Bucket storage backend testing only available on server versions >= 7.1")

        # Create the bucket
        self.bm.create_bucket(
            CreateBucketSettings(
                name='fred',
                ram_quota_mb=100,
                bucket_type=BucketType.EPHEMERAL,
                flush_enabled=False))
        bucket = self.try_n_times(10, 3, self.bm.get_bucket, 'fred')
        self.assertEqual(bucket.storage_backend, StorageBackend.UNDEFINED)

    @flaky(5, 1)
    def test_bucket_custom_conflict_resolution(self):
        version = self.cluster.get_server_version()
        if version.short_version < 7.1 or not version.is_dp:
            raise SkipTest(
                "Custom conflict resolution testing only available on server versions >= 7.1 with developer preview enabled")

        # Create the bucket
        self.bm.create_bucket(
            CreateBucketSettings(
                name='fred',
                ram_quota_mb=100,
                conflict_resolution_type=ConflictResolutionType.CUSTOM,
                flush_enabled=False))
        bucket = self.try_n_times(10, 3, self.bm.get_bucket, 'fred')
        self.assertEqual(bucket['conflict_resolution_type'],
                         ConflictResolutionType.CUSTOM)
