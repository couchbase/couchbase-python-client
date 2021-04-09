import os
import datetime
from unittest import SkipTest

from flaky import flaky

from couchbase.exceptions import BucketDoesNotExistException, BucketAlreadyExistsException, BucketNotFlushableException
from couchbase.management.buckets import CreateBucketSettings, BucketSettings, BucketType
from couchbase_tests.base import CollectionTestCase
from couchbase_core.durability import Durability


@flaky(10, 1)
class BucketManagementTests(CollectionTestCase):
    def setUp(self, *args, **kwargs):
        super(BucketManagementTests, self).setUp(*args, **kwargs)
        self.bm = self.cluster.buckets()
        if not self.is_realserver:
            raise SkipTest('Real server must be used for admin tests')

        self.purge_buckets()
    buckets_to_add = {'fred': {}, 'jane': {}, 'sally': {}}

    def tearDown(self):
        self.purge_buckets()

    def purge_buckets(self):
        for bucket, kwargs in BucketManagementTests.buckets_to_add.items():
            try:
                self.bm.drop_bucket(bucket)
            except BucketDoesNotExistException:
                pass
            except Exception as e:
                raise
                # now be sure it is really gone
            self.try_n_times_till_exception(10, 3, self.bm.get_bucket, bucket)

    def test_bucket_create(self):
        self.bm.create_bucket(CreateBucketSettings(name="fred", bucket_type="couchbase", ram_quota_mb=100))
        bucket = self.try_n_times(10, 1, self.bm.get_bucket, "fred")
        if float(self.cluster_version[0:3]) >= 6.6:
            self.assertEqual(bucket['minimum_durability_level'], Durability.NONE)

    def test_bucket_create_replica_index(self):
        self.bm.create_bucket(CreateBucketSettings(name="fred", bucket_type="couchbase", ram_quota_mb=100, replica_index=True))
        bucket = self.try_n_times(10, 1, self.bm.get_bucket, "fred")
        self.assertTrue(bucket.replica_index)
        self.try_n_times(10, 1, self.bm.drop_bucket, 'fred')
        self.assertRaises(BucketDoesNotExistException, self.bm.drop_bucket, 'fred')
        self.bm.create_bucket(CreateBucketSettings(name="fred", bucket_type="couchbase", ram_quota_mb=100, replica_index=False))
        bucket = self.try_n_times(10, 1, self.bm.get_bucket, "fred")
        self.assertFalse(bucket.replica_index)

    def test_bucket_create_durability(self):
        if float(self.cluster_version[0:3]) < 6.6:
            raise SkipTest("Bucket minimum durability not available on server version < 6.6")
        min_durability = Durability.MAJORITY_AND_PERSIST_TO_ACTIVE
        self.bm.create_bucket(CreateBucketSettings(name="fred", 
                                    bucket_type="couchbase", 
                                    ram_quota_mb=100, 
                                    minimum_durability_level=min_durability))
        bucket = self.try_n_times(10, 1, self.bm.get_bucket, "fred")
        self.assertEqual(bucket['minimum_durability_level'], min_durability)

    def test_bucket_create_fail(self):
        settings = CreateBucketSettings(name='fred', bucket_type=BucketType.COUCHBASE, ram_quota_mb=100)
        self.bm.create_bucket(settings)
        self.assertRaises(BucketAlreadyExistsException, self.bm.create_bucket, settings)

    def test_bucket_drop_fail(self):
        settings = CreateBucketSettings(name='fred', bucket_type=BucketType.COUCHBASE, ram_quota_mb=100)
        self.bm.create_bucket(settings)
        self.try_n_times(10, 1, self.bm.drop_bucket, 'fred')
        self.assertRaises(BucketDoesNotExistException, self.bm.drop_bucket, 'fred')

    def test_bucket_list(self):
        for bucket, kwargs in BucketManagementTests.buckets_to_add.items():
            self.bm.create_bucket(CreateBucketSettings(name=bucket, ram_quota_mb=100, **kwargs))
            self.try_n_times(10, 1, self.bm.get_bucket, bucket)

        self.assertEqual(set(), {"fred", "jane", "sally"}.difference(
            set(map(lambda x: x.name, self.bm.get_all_buckets()))))

    def test_cluster_sees_bucket(self):
        # Create the bucket
        self.bm.create_bucket(CreateBucketSettings(name='fred', ram_quota_mb=100))
        # cluster should be able to return it (though, not right away)
        self.try_n_times(10, 3, self.cluster.bucket, 'fred')

    def test_change_ttl(self):
        # Create the bucket
        self.bm.create_bucket(CreateBucketSettings(name='fred', ram_quota_mb=100))
        self.try_n_times(10, 3, self.bm.get_bucket, 'fred')

        # change bucket TTL
        self.try_n_times(10, 3, self.bm.update_bucket, BucketSettings(name='fred', max_ttl=datetime.timedelta(seconds=500)))

        def get_bucket_ttl_equal(name, ttl):
            if not ttl == self.bm.get_bucket(name).max_ttl:
                raise Exception("not equal")

        self.try_n_times(10, 3, get_bucket_ttl_equal, 'fred', datetime.timedelta(seconds=500))

    def test_bucket_flush(self):
        # Create the bucket
        self.bm.create_bucket(CreateBucketSettings(name='fred', ram_quota_mb=100, flush_enabled=True))
        bucket = self.try_n_times(10, 3, self.bm.get_bucket, 'fred')
        self.assertTrue(bucket.flush_enabled)
        # flush the bucket
        self.try_n_times(10, 3, self.bm.flush_bucket, bucket.name)

        # disable bucket flush
        self.bm.update_bucket(BucketSettings(name='fred', flush_enabled=False))
        bucket = self.try_n_times(10, 3, self.bm.get_bucket, 'fred')
        self.assertFalse(bucket.flush_enabled)

        # verify appropriate Exception when flush attempted
        self.assertRaises(BucketNotFlushableException, self.bm.flush_bucket, 'fred')
