import os
from unittest import SkipTest

from couchbase import CBCollection
from couchbase.exceptions import CouchbaseError
from couchbase_core.connstr import ConnectionString
from couchbase.management.buckets import CreateBucketSettings, BucketSettings
from couchbase_tests.base import CollectionTestCase


class BucketManagementTests(CollectionTestCase):
    def setUp(self, *args, **kwargs):
        super(BucketManagementTests, self).setUp(*args, **kwargs)
        self.bm = self.cluster.buckets()
        if not self.is_realserver:
            raise SkipTest('Real server must be used for admin tests')

        #if not os.environ.get('PYCBC_TEST_ADMIN'):
        #    raise SkipTest('PYCBC_TEST_ADMIN must be set in the environment')

    def test_bucket_create(self):
        try:
            self.bm.create_bucket(CreateBucketSettings(name="fred", bucket_type="couchbase", ram_quota_mb=100))
        finally:
            self.bm.drop_bucket('fred')

    def test_bucket_list(self):
        buckets_to_add = {'fred': {}, 'jane': {}, 'sally': {}}
        try:
            for bucket, kwargs in buckets_to_add.items():
                self.bm.create_bucket(CreateBucketSettings(name=bucket, ram_quota_mb=100, **kwargs))

            self.assertEqual(set(), {"fred", "jane", "sally"}.difference(
                set(map(lambda x: x.name, self.bm.get_all_buckets()))))
        finally:
            for bucket, kwargs in buckets_to_add.items():
                try:
                    self.bm.drop_bucket(bucket)
                except:
                    pass

    def test_actions(self):

        try:
            # Remove the bucket, if it exists
            self.bm.drop_bucket('dummy')
        except CouchbaseError:
            pass

        # Need to explicitly enable admin tests..
        # Create the bucket
        self.bm.create_bucket(CreateBucketSettings(name='dummy',
                                                   ram_quota_mb=100, bucket_password='letmein'))
        self.bm._admin_bucket.wait_ready('dummy', timeout=15.0)

        # All should be OK, ensure we can connect:
        connstr = ConnectionString.parse(
            self.make_connargs()['connection_string'])

        dummy_bucket = 'dummy'
        connstr = connstr.encode()
        args=self.make_connargs()
        args.pop('connection_string',None)
        args['bucket'] = dummy_bucket
        self.factory(connstr, **args)
        # OK, it exists
        self.assertRaises(CouchbaseError, self.factory, connstr)

        # Change the password

        self.bm.update_bucket(
                              BucketSettings(name=dummy_bucket, max_ttl=500))
        self.bm._admin_bucket.wait_ready(dummy_bucket, 10)
        dummy=self.bm.get_bucket('dummy')
        self.assertEqual(500,dummy.max_ttl)
        # Remove the bucket
        self.bm.drop_bucket('dummy')
        self.assertRaises(CouchbaseError, self.factory, connstr)
