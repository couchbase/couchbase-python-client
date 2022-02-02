import asyncio
from unittest import SkipTest
from datetime import timedelta
from nose.plugins.attrib import attr

from acouchbase.cluster import (Cluster, get_event_loop,
                                close_event_loop)
from couchbase_tests.async_base import AsyncioTestCase, async_test
from couchbase.management.queries import CreateQueryIndexOptions, WatchQueryIndexOptions
from couchbase.exceptions import (CouchbaseException, QueryIndexNotFoundException,
                                  QueryIndexAlreadyExistsException, WatchQueryIndexTimeoutException)


@attr("index")
class AcouchbaseQueryIndexManagerTests(AsyncioTestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super(AcouchbaseQueryIndexManagerTests, cls).setUpClass(
            get_event_loop(), cluster_class=Cluster)

    @classmethod
    def tearDownClass(cls) -> None:
        super(AcouchbaseQueryIndexManagerTests, cls).tearDownClass()
        close_event_loop()

    def setUp(self):
        super(AcouchbaseQueryIndexManagerTests, self).setUp()

        self.mgr = self.cluster.query_indexes()
        if self.is_mock:
            raise SkipTest("Real server must be used for admin tests")
        if self.cluster._is_dev_preview():
            raise SkipTest(
                "dev preview is on, that means index creation will fail")

        self.loop.run_until_complete(self._clear_indexes())

    def tearDown(self):
        self.loop.run_until_complete(self._clear_indexes())
        super(AcouchbaseQueryIndexManagerTests, self).tearDown()

    async def _clear_indexes(self):
        # Drop all indexes!
        indexes = await self.try_n_times_async(10, 3, self.mgr.get_all_indexes, self.cluster_info.bucket_name)
        for index in indexes:
            await self.mgr.drop_index(self.cluster_info.bucket_name, index.name)
        for _ in range(10):
            count = await self.try_n_times_async(10, 3, self.mgr.get_all_indexes, self.cluster_info.bucket_name)
            if 0 == len(count):
                return
            asyncio.sleep(3)
        self.fail(
            "indexes were not dropped after {} waits of {} seconds each".format(10, 3))

    @async_test
    async def test_create_primary(self):
        bucket_name = self.cluster_info.bucket_name
        await self.mgr.create_primary_index(
            bucket_name, timeout=timedelta(seconds=60))

        # Ensure we can issue a query
        qstr = 'select * from {0} limit 1'.format(
            self.cluster_info.bucket_name)
        q_iter = self.cluster.query(qstr)
        _ = [r async for r in q_iter]
        # if we go this far the test passed
        # querying would fail if an index didn't exist

    @async_test
    async def test_create_named_primary(self):
        bucket_name = self.cluster_info.bucket_name
        ixname = 'namedPrimary'
        qstr = 'select * from {0} limit 1'.format(
            self.cluster_info.bucket_name)
        # Try to create a _named_ primary index
        await self.mgr.create_index(bucket_name, ixname, [], primary=True)
        q_iter = self.cluster.query(qstr)
        _ = [r async for r in q_iter]
        # if we go this far the test passed
        # querying would fail if an index didn't exist

    @async_test
    async def test_create_primary_ignore_if_exists(self):
        bucket_name = self.cluster_info.bucket_name
        await self.mgr.create_primary_index(bucket_name)
        await self.mgr.create_primary_index(bucket_name, ignore_if_exists=True)

        with self.assertRaises(QueryIndexAlreadyExistsException):
            await self.mgr.create_primary_index(bucket_name)

    @async_test
    async def test_drop_primary_ignore_if_not_exists(self):
        bucket_name = self.cluster_info.bucket_name
        await self.mgr.drop_primary_index(bucket_name, ignore_if_not_exists=True)
        with self.assertRaises(QueryIndexNotFoundException):
            await self.mgr.drop_primary_index(bucket_name)

    @async_test
    async def test_drop_named_primary_ignore_if_exists(self):
        ixname = 'namedPrimary'
        bucket_name = self.cluster_info.bucket_name
        await self.mgr.create_index(bucket_name, ixname, [], primary=True)
        await self.mgr.drop_index(bucket_name, ixname)
        await self.mgr.drop_index(bucket_name, ixname, ignore_missing=True)
        with self.assertRaises(QueryIndexNotFoundException):
            await self.mgr.drop_index(bucket_name, ixname)

    @async_test
    async def test_create_secondary_indexes(self):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        bucket_name = self.cluster_info.bucket_name
        self.mgr._admin_bucket.timeout = 10000
        await self.mgr.create_index(bucket_name, ixname,
                                    fields=fields, timeout=timedelta(days=1))
        qq = 'select {1}, {2} from `{0}` where {1}=1 and {2}=2 limit 1'\
            .format(bucket_name, *fields)
        q_iter = self.cluster.query(qq)
        _ = [r async for r in q_iter]

    @async_test
    async def test_create_secondary_indexes_condition(self):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        bucket_name = self.cluster_info.bucket_name
        self.mgr._admin_bucket.timeout = 10000

        await self.try_n_times_till_exception_async(10, 5, self.mgr.drop_index, bucket_name, ixname,
                                                    expected_exceptions=(QueryIndexNotFoundException,))
        CONDITION = '((`fld1` = 1) and (`fld2` = 2))'
        await self.mgr.create_index(bucket_name, ixname, fields,
                                    CreateQueryIndexOptions(timeout=timedelta(days=1), condition=CONDITION))

        async def check_index():
            indexes = await self.mgr.get_all_indexes(bucket_name)
            result = next(
                iter(x for x in indexes if x.name == ixname), None)
            self.assertIsNotNone(result)
            return result

        result = await self.try_n_times_async(10, 5, check_index)
        self.assertEqual(CONDITION, result.condition)

    @async_test
    async def test_drop_secondary_indexes(self):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        bucket_name = self.cluster_info.bucket_name
        await self.mgr.create_index(bucket_name, ixname,
                                    fields=fields, timeout=timedelta(days=1))

        qq = 'select {1}, {2} from `{0}` where {1}=1 and {2}=2 limit 1' \
            .format(bucket_name, *fields)

        q_iter = self.cluster.query(qq)
        _ = [r async for r in q_iter]
        # Drop the index
        await self.mgr.drop_index(bucket_name, ixname)
        # Issue the query again
        with self.assertRaises(CouchbaseException):
            q_iter = self.cluster.query(qq)
            _ = [r async for r in q_iter]

    @async_test
    async def test_create_index_no_fields(self):
        bucket_name = self.cluster_info.bucket_name
        with self.assertRaises((ValueError, TypeError,)):
            await self.mgr.create_index(bucket_name, 'withoutFields')

    @async_test
    async def test_create_secondary_indexes_ignore_if_exists(self):
        ixname = 'ix2'
        bucket_name = self.cluster_info.bucket_name
        await self.mgr.create_index(bucket_name, ixname, fields=['hello'])
        await self.mgr.create_index(bucket_name, ixname, fields=[
            'hello'], ignore_if_exists=True)
        with self.assertRaises(QueryIndexAlreadyExistsException):
            await self.mgr.create_index(bucket_name, ixname, fields=['hello'])

    @async_test
    async def test_drop_secondary_indexes_ignore_if_not_exists(self):
        # Create it
        ixname = 'ix2'
        bucket_name = self.cluster_info.bucket_name
        await self.mgr.create_index(bucket_name, ixname, fields=['hello'])
        # Drop it
        await self.mgr.drop_index(bucket_name, ixname)
        await self.mgr.drop_index(bucket_name, ixname, ignore_if_not_exists=True)
        with self.assertRaises(QueryIndexNotFoundException):
            await self.mgr.drop_index(bucket_name, ixname)

    @async_test
    async def test_list_indexes(self):
        # start with no indexes

        ixs = list(await self.mgr.get_all_indexes(self.cluster_info.bucket_name))
        self.assertEqual(0, len(ixs))

        # Create the primary index
        await self.mgr.create_primary_index(self.cluster_info.bucket_name)
        # type: List[QueryIndex]
        ixs = list(await self.mgr.get_all_indexes(self.cluster_info.bucket_name))
        self.assertEqual(1, len(ixs))
        self.assertTrue(ixs[0].is_primary)
        self.assertEqual('#primary', ixs[0].name)
        self.assertEqual(self.cluster_info.bucket_name, ixs[0].keyspace)

    @async_test
    async def test_index_partition_info(self):
        bucket_name = self.cluster_info.bucket_name
        # use query to create index w/ partition, cannot do that via manager
        # ATM
        qstr = 'CREATE INDEX idx_fld1 ON `{0}`(fld1) PARTITION BY HASH(fld1)'.format(
            bucket_name)
        q_iter = self.cluster.query(qstr)
        [r async for r in q_iter]

        ixs = list(await self.mgr.get_all_indexes(bucket_name))
        idx = next((ix for ix in ixs if ix.name == "idx_fld1"), None)
        self.assertIsNotNone(idx)
        self.assertIsNotNone(idx.partition)
        self.assertEqual(idx.partition, "HASH(`fld1`)")

    @async_test
    async def test_deferred(self):
        # Create primary index
        await self.mgr.create_primary_index(
            self.cluster_info.bucket_name, deferred=True)
        ix = await self.mgr.get_all_indexes(self.cluster_info.bucket_name)
        self.assertEqual('deferred', next(iter(ix)).state)

        # Create a bunch of other indexes
        for n in range(5):
            await self.mgr.create_index(self.cluster_info.bucket_name,
                                        'ix{0}'.format(n), fields=['fld{0}'.format(n)], deferred=True)

        ix = await self.mgr.get_all_indexes(self.cluster_info.bucket_name)
        self.assertEqual(6, len(ix))
        ix_names = list(map(lambda i: i.name, ix))

        await self.mgr.build_deferred_indexes(
            self.cluster_info.bucket_name)
        await self.mgr.watch_indexes(
            self.cluster_info.bucket_name, ix_names, WatchQueryIndexOptions(timeout=timedelta(seconds=30)))  # Should be OK
        await self.mgr.watch_indexes(self.cluster_info.bucket_name,
                                     ix_names, WatchQueryIndexOptions(timeout=timedelta(seconds=30), watch_primary=True))  # Should be OK again
        with self.assertRaises(QueryIndexNotFoundException):
            await self.mgr.watch_indexes(self.cluster_info.bucket_name, ['nonexist'], WatchQueryIndexOptions(timeout=timedelta(seconds=10)))

    @async_test
    async def test_watch(self):
        # Create primary index
        await self.mgr.create_primary_index(
            self.cluster_info.bucket_name, deferred=True)
        ix = await self.mgr.get_all_indexes(self.cluster_info.bucket_name)
        self.assertEqual('deferred', next(iter(ix)).state)

        # Create a bunch of other indexes
        for n in range(5):
            defer = False
            if n % 2 == 0:
                defer = True
            await self.mgr.create_index(self.cluster_info.bucket_name,
                                        'ix{0}'.format(n), fields=['fld{0}'.format(n)], deferred=defer)

        ix = await self.mgr.get_all_indexes(self.cluster_info.bucket_name)
        self.assertEqual(6, len(ix))
        # by not buildind deffered indexes, should timeout
        with self.assertRaises(WatchQueryIndexTimeoutException):
            await self.mgr.watch_indexes(self.cluster_info.bucket_name, [i.name for i in ix], WatchQueryIndexOptions(timeout=timedelta(seconds=5)))


# TODO:  PYCBC-1220 - add tests for collections
# class AcouchbaseQueryIndexManagerCollectionTests(AsyncioTestCase):
#     pass
