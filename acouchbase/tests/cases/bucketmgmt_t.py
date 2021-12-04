from unittest import SkipTest
from flaky import flaky
import datetime

from acouchbase.cluster import (Cluster, get_event_loop,
                                close_event_loop)
from couchbase_tests.async_base import AsyncioTestCase, async_test
from couchbase.exceptions import (BucketDoesNotExistException, BucketAlreadyExistsException,
                                  BucketNotFlushableException)
from couchbase.management.buckets import (CreateBucketSettings, BucketSettings,
                                          BucketType, StorageBackend)
from couchbase_core.durability import Durability


@flaky(10, 1)
class AcouchbaseBucketManagerTests(AsyncioTestCase):

    _BUCKETS_TO_ADD = {"fred": {}, "jane": {}, "sally": {}}

    @classmethod
    def setUpClass(cls) -> None:
        super(AcouchbaseBucketManagerTests, cls).setUpClass(
            get_event_loop(), cluster_class=Cluster)

    @classmethod
    def tearDownClass(cls) -> None:
        super(AcouchbaseBucketManagerTests, cls).tearDownClass()
        close_event_loop()

    def setUp(self):
        super(AcouchbaseBucketManagerTests, self).setUp()

        self.bm = self.cluster.buckets()
        if not self.is_realserver:
            raise SkipTest("Real server must be used for admin tests")

        self.loop.run_until_complete(self._purge_buckets())

    def tearDown(self):
        self.loop.run_until_complete(self._purge_buckets())

    async def _purge_buckets(self):
        for bucket, _ in self._BUCKETS_TO_ADD.items():
            try:
                await self.bm.drop_bucket(bucket)
            except BucketDoesNotExistException:
                pass
            except Exception as e:
                raise
                # now be sure it is really gone
            await self.try_n_times_till_exception_async(10, 3, self.bm.get_bucket, bucket)

    @async_test
    async def test_bucket_create(self):
        await self.bm.create_bucket(
            CreateBucketSettings(
                name="fred",
                bucket_type="couchbase",
                ram_quota_mb=100))
        bucket = await self.try_n_times_async(10, 1, self.bm.get_bucket, "fred")
        if float(self.cluster_version[0:3]) >= 6.6:
            self.assertEqual(
                bucket["minimum_durability_level"],
                Durability.NONE)

    @async_test
    async def test_bucket_create_replica_index_true(self):
        await self.bm.create_bucket(
            CreateBucketSettings(
                name="fred",
                bucket_type="couchbase",
                ram_quota_mb=100,
                replica_index=True))
        bucket = await self.try_n_times_async(10, 1, self.bm.get_bucket, "fred")
        self.assertTrue(bucket.replica_index)

    @async_test
    async def test_bucket_create_replica_index_false(self):
        await self.bm.create_bucket(
            CreateBucketSettings(
                name="fred",
                bucket_type="couchbase",
                ram_quota_mb=100,
                replica_index=False))
        bucket = await self.try_n_times_async(10, 1, self.bm.get_bucket, "fred")
        self.assertFalse(bucket.replica_index)

    @async_test
    async def test_bucket_create_durability(self):
        if float(self.cluster_version[0:3]) < 6.6:
            raise SkipTest(
                "Bucket minimum durability not available on server version < 6.6")
        min_durability = Durability.MAJORITY_AND_PERSIST_TO_ACTIVE
        await self.bm.create_bucket(CreateBucketSettings(name="fred",
                                                         bucket_type="couchbase",
                                                         ram_quota_mb=100,
                                                         minimum_durability_level=min_durability))
        bucket = await self.try_n_times_async(10, 1, self.bm.get_bucket, "fred")
        self.assertEqual(bucket["minimum_durability_level"], min_durability)

    @async_test
    async def test_bucket_create_fail(self):
        settings = CreateBucketSettings(
            name="fred",
            bucket_type=BucketType.COUCHBASE,
            ram_quota_mb=100)
        await self.bm.create_bucket(settings)
        with self.assertRaises(BucketAlreadyExistsException):
            await self.bm.create_bucket(settings)

    @async_test
    async def test_bucket_drop_fail(self):
        settings = CreateBucketSettings(
            name="fred",
            bucket_type=BucketType.COUCHBASE,
            ram_quota_mb=100)
        await self.bm.create_bucket(settings)
        await self.try_n_times_async(10, 1, self.bm.drop_bucket, "fred")
        with self.assertRaises(BucketDoesNotExistException):
            await self.bm.drop_bucket("fred")

    @async_test
    async def test_bucket_list(self):
        for bucket, kwargs in self._BUCKETS_TO_ADD.items():
            await self.bm.create_bucket(
                CreateBucketSettings(
                    name=bucket,
                    ram_quota_mb=100,
                    **kwargs))
            await self.try_n_times_async(10, 1, self.bm.get_bucket, bucket)

        buckets = await self.bm.get_all_buckets()
        self.assertEqual(set(), {"fred", "jane", "sally"}.difference(
            set(map(lambda x: x.name, buckets))))

    @async_test
    async def test_cluster_sees_bucket(self):
        # Create the bucket
        await self.bm.create_bucket(
            CreateBucketSettings(
                name="fred",
                ram_quota_mb=100))
        # cluster should be able to return it (though, not right away)

        async def open_bucket(bucket):
            b = self.cluster.bucket(bucket)
            await b.on_connect()

        await self.try_n_times_async(10, 3, open_bucket, "fred")

    @async_test
    async def test_change_ttl(self):
        # Create the bucket
        await self.bm.create_bucket(
            CreateBucketSettings(
                name="fred",
                ram_quota_mb=100))
        await self.try_n_times_async(10, 3, self.bm.get_bucket, "fred")

        # change bucket TTL
        await self.try_n_times_async(10, 3, self.bm.update_bucket, BucketSettings(
            name="fred", max_ttl=datetime.timedelta(seconds=500)))

        async def get_bucket_ttl_equal(name, ttl):
            bucket = await self.bm.get_bucket(name)
            if not ttl == bucket.max_ttl:
                raise Exception("not equal")

        await self.try_n_times_async(
            10,
            3,
            get_bucket_ttl_equal,
            "fred",
            datetime.timedelta(
                seconds=500))

    @async_test
    async def test_bucket_flush(self):
        # Create the bucket
        await self.bm.create_bucket(
            CreateBucketSettings(
                name="fred",
                ram_quota_mb=100,
                flush_enabled=True))
        bucket = await self.try_n_times_async(10, 3, self.bm.get_bucket, "fred")
        self.assertTrue(bucket.flush_enabled)
        # flush the bucket
        await self.try_n_times_async(10, 3, self.bm.flush_bucket, bucket.name)

    @async_test
    async def test_bucket_flush_fail(self):
        # Create the bucket
        await self.bm.create_bucket(
            CreateBucketSettings(
                name="fred",
                ram_quota_mb=100,
                flush_enabled=False))
        bucket = await self.try_n_times_async(10, 3, self.bm.get_bucket, "fred")
        self.assertFalse(bucket.flush_enabled)

        # verify appropriate Exception when flush attempted
        with self.assertRaises(BucketNotFlushableException):
            await self.bm.flush_bucket("fred")

    @async_test
    async def test_bucket_backend_default(self):
        version = self.cluster.get_server_version()
        if version.short_version < 7.1:
            raise SkipTest(
                "Bucket storage backend testing only available on server versions >= 7.1")

        # Create the bucket
        await self.bm.create_bucket(
            CreateBucketSettings(
                name='fred',
                ram_quota_mb=100,
                flush_enabled=False))
        bucket = await self.try_n_times_async(10, 3, self.bm.get_bucket, 'fred')
        self.assertEqual(bucket.storage_backend, StorageBackend.COUCHSTORE)

    @async_test
    async def test_bucket_backend_magma(self):
        version = self.cluster.get_server_version()
        if version.short_version < 7.1:
            raise SkipTest(
                "Bucket storage backend testing only available on server versions >= 7.1")

        # Create the bucket
        await self.bm.create_bucket(
            CreateBucketSettings(
                name='fred',
                ram_quota_mb=256,
                flush_enabled=False,
                storage_backend=StorageBackend.MAGMA))
        bucket = await self.try_n_times_async(10, 3, self.bm.get_bucket, 'fred')
        self.assertEqual(bucket.storage_backend, StorageBackend.MAGMA)

    @async_test
    async def test_bucket_backend_ephemeral(self):
        version = self.cluster.get_server_version()
        if version.short_version < 7.1:
            raise SkipTest(
                "Bucket storage backend testing only available on server versions >= 7.1")

        # Create the bucket
        await self.bm.create_bucket(
            CreateBucketSettings(
                name='fred',
                ram_quota_mb=100,
                bucket_type=BucketType.EPHEMERAL,
                flush_enabled=False))
        bucket = await self.try_n_times_async(10, 3, self.bm.get_bucket, 'fred')
        self.assertEqual(bucket.storage_backend, StorageBackend.UNDEFINED)
