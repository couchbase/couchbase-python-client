import os
from unittest import SkipTest

from flaky import flaky

from couchbase.exceptions import BucketDoesNotExistException, BucketAlreadyExistsException
from couchbase.management.buckets import CreateBucketSettings, BucketSettings
from couchbase_tests.base import CollectionTestCase


@flaky(10, 10)
class BucketManagementTests(CollectionTestCase):
    def setUp(self, *args, **kwargs):
        super(BucketManagementTests, self).setUp(*args, **kwargs)
        self.bm = self.cluster.buckets()
        if not self.is_realserver:
            raise SkipTest('Real server must be used for admin tests')

        self.try_n_times_till_exception(10, 3, self.bm.drop_bucket, 'fred')

        # be sure fred is not there anymore...
        self.try_n_times_till_exception(10, 3, self.bm.get_bucket, 'fred')

    def test_bucket_create(self):
        self.bm.create_bucket(CreateBucketSettings(name="fred", bucket_type="couchbase", ram_quota_mb=100))
        self.try_n_times(10, 1, self.bm.get_bucket, "fred")

    def test_bucket_create_fail(self):
        settings = CreateBucketSettings(name='fred', bucket_type='couchbase', ram_quota_mb=100)
        self.bm.create_bucket(settings)
        self.assertRaises(BucketAlreadyExistsException, self.bm.create_bucket, settings)

    def test_bucket_drop_fail(self):
        settings = CreateBucketSettings(name='fred', bucket_type='couchbase', ram_quota_mb=100)
        self.bm.create_bucket(settings)
        self.try_n_times(10, 1, self.bm.drop_bucket, 'fred')
        self.assertRaises(BucketDoesNotExistException, self.bm.drop_bucket, 'fred')

    def test_bucket_list(self):
        buckets_to_add = {'fred': {}, 'jane': {}, 'sally': {}}
        try:
            for bucket, kwargs in buckets_to_add.items():
                self.bm.create_bucket(CreateBucketSettings(name=bucket, ram_quota_mb=100, **kwargs))
                self.try_n_times(10, 1, self.bm.get_bucket, bucket)

            self.assertEqual(set(), {"fred", "jane", "sally"}.difference(
                set(map(lambda x: x.name, self.bm.get_all_buckets()))))
        finally:
            for bucket, kwargs in buckets_to_add.items():
                try:
                    self.bm.drop_bucket(bucket)
                except BucketDoesNotExistException:
                    pass
                # now be sure it is really gone
                self.try_n_times_till_exception(10, 3, self.bm.get_bucket, bucket)

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
        self.try_n_times(10, 3, self.bm.update_bucket, BucketSettings(name='fred', max_ttl=500))

        def get_bucket_ttl_equal(name, ttl):
            if not ttl == self.bm.get_bucket(name).max_ttl:
                raise Exception("not equal")

        self.try_n_times(10, 3, get_bucket_ttl_equal, 'fred', 500)
