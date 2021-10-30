import asyncio
from unittest import SkipTest
from datetime import timedelta

from acouchbase.cluster import (Cluster, get_event_loop,
                                close_event_loop)
from couchbase_tests.async_base import AsyncioTestCase, async_test
from couchbase.exceptions import (BucketDoesNotExistException, NotSupportedException,
                                  DocumentNotFoundException, ScopeNotFoundException,
                                  ScopeAlreadyExistsException, CollectionAlreadyExistsException,
                                  CollectionNotFoundException)
from couchbase.management.buckets import CreateBucketSettings
from couchbase.management.collections import CollectionSpec


class AcouchbaseCollectionManagerTests(AsyncioTestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super(AcouchbaseCollectionManagerTests, cls).setUpClass(
            get_event_loop(), cluster_class=Cluster)

    @classmethod
    def tearDownClass(cls) -> None:
        super(AcouchbaseCollectionManagerTests, cls).tearDownClass()
        close_event_loop()

    def setUp(self):
        super(AcouchbaseCollectionManagerTests, self).setUp()
        self.loop.run_until_complete(self._initialize())

    async def _initialize(self):
        # SkipTest if collections not supported
        try:
            await self.bucket.collections().get_all_scopes()
        except NotSupportedException:
            raise SkipTest('cluster does not support collections')

        # Need this so we use RBAC.
        # TODO: lets perhaps move this into the base classes?  Then we can
        # maybe not need the default user, etc...
        self.cluster._cluster.authenticate(
            username=self.cluster_info.admin_username,
            password=self.cluster_info.admin_password)
        self.bm = self.cluster.buckets()

        # insure other-bucket is gone first
        try:
            await self.bm.drop_bucket("other-bucket")
        except BaseException:
            # it maybe isn't there, that's fine
            pass
        await self.try_n_times_till_exception_async(
            10,
            1,
            self.bm.get_bucket,
            "other-bucket",
            expected_exceptions=(
                BucketDoesNotExistException,
            ))

        # now re-create it fresh (maybe we could just flush, but we may test
        # settings which would not be flushed)
        await self.try_n_times_async(10, 1, self.bm.create_bucket, CreateBucketSettings(
            name="other-bucket", bucket_type="couchbase", ram_quota_mb=100))
        await self.try_n_times_async(10, 1, self.bm.get_bucket, "other-bucket")
        # we need to get the bucket, but sometimes this fails for a few seconds depending on what
        # the cluster is doing.  So, try_n_times...

        async def get_bucket(name):
            b = self.cluster.bucket(name)
            await b.on_connect()
            return b

        self.other_bucket = await self.try_n_times_async(10, 3, get_bucket, "other-bucket")
        self.cm = self.other_bucket.collections()

    async def _get_cluster_bucket(self, name):
        b = self.cluster.bucket(name)
        await b.on_connect()
        return b

    async def _get_scope(self, bucket_name, scope_name):
        cm = self.cm
        if bucket_name:
            bucket = await self.try_n_times_async(10, 3, self._get_cluster_bucket, bucket_name)
            if bucket:
                cm = bucket.collections()
            else:
                return None

        scopes = await cm.get_all_scopes()
        return next((s for s in scopes if s.name == scope_name), None)

    async def _get_collection(self, bucket_name, scope_name, coll_name):
        scope = await self._get_scope(bucket_name, scope_name)
        if scope:
            return next(
                (c for c in scope.collections if c.name == coll_name), None)

        return None

    @async_test
    async def test_create_scope(self):
        await self.cm.create_scope("other-scope")
        scope = await self._get_scope(None, "other-scope")
        self.assertIsNotNone(scope)

    @async_test
    async def test_create_scope_already_exists(self):
        await self.cm.create_scope("other-scope")
        scope = await self._get_scope(None, "other-scope")
        self.assertIsNotNone(scope)
        with self.assertRaises(ScopeAlreadyExistsException):
            await self.cm.create_scope("other-scope")

    @async_test
    async def test_get_all_scopes(self):
        scopes = await self.cm.get_all_scopes()
        # this is a brand-new bucket, so it should only have _default scope and
        # a _default collection
        self.assertTrue(len(scopes) == 1)
        scope = scopes[0]
        self.assertEqual(scope.name, "_default")
        self.assertEqual(1, len(scope.collections))
        collection = scope.collections[0]
        self.assertEqual("_default", collection.name)
        self.assertEqual("_default", collection.scope_name)

    @async_test
    async def test_drop_scope(self):
        scope_name = "other-scope"
        await self.cm.create_scope(scope_name)
        scope = await self._get_scope(self.other_bucket.bucket, scope_name)
        self.assertIsNotNone(scope)
        await self.cm.drop_scope(scope_name)
        with self.assertRaises(ScopeNotFoundException):
            await self.cm.drop_scope(scope_name)

    @async_test
    async def test_drop_scope_not_found(self):
        with self.assertRaises(ScopeNotFoundException):
            await self.cm.drop_scope("somerandomscope")

    @async_test
    async def test_create_collection(self):
        collection = CollectionSpec("other-collection")
        await self.cm.create_collection(collection)
        found = await self._get_collection(self.other_bucket.bucket,
                                           collection.scope_name,
                                           collection.name)
        self.assertIsNotNone(found)

    @async_test
    async def test_create_collection_max_ttl(self):
        collection = CollectionSpec(
            "other-collection",
            max_ttl=timedelta(
                seconds=2))
        await self.cm.create_collection(collection)
        found = await self._get_collection(self.other_bucket.bucket,
                                           collection.scope_name,
                                           collection.name)
        self.assertIsNotNone(found)
        # pop a doc in with no ttl, verify it goes away...

        async def try_get_collection(collection_name):
            for _ in range(10):
                coll = self.other_bucket.collection(collection_name)
                if coll:
                    break
                await asyncio.sleep(1)
            return coll

        coll = await try_get_collection("other-collection")
        key = self.gen_key("cmtest")
        # we _can_ get a temp fail here, as we just created the collection.  So we
        # retry the upsert.
        await self.try_n_times_async(10, 1, coll.upsert, key, {"some": "thing"})
        await self.try_n_times_async(10, 1, coll.get, key)
        await self.try_n_times_till_exception_async(
            4, 1, coll.get, key, expected_exceptions=(
                DocumentNotFoundException,))

    @async_test
    async def test_create_collection_bad_scope(self):
        with self.assertRaises(ScopeNotFoundException):
            await self.cm.create_collection(CollectionSpec("imnotgonnawork", "notarealscope"))

    @async_test
    async def test_create_collection_already_exists(self):
        collection = CollectionSpec("other-collection")
        await self.cm.create_collection(collection)
        # verify the collection exists w/in other-bucket
        found = await self._get_collection(self.other_bucket.bucket,
                                           collection.scope_name,
                                           collection.name)
        self.assertIsNotNone(found)
        # now, it will fail if we try to create it again...
        with self.assertRaises(CollectionAlreadyExistsException):
            await self.cm.create_collection(collection)

    @async_test
    async def test_collection_goes_in_correct_bucket(self):
        collection = CollectionSpec("other-collection")
        await self.cm.create_collection(collection)
        # make sure it actually is in the other-bucket
        found = await self._get_collection(self.other_bucket.bucket,
                                           collection.scope_name,
                                           collection.name)
        self.assertIsNotNone(found)
        # also be sure this isn't in the default bucket
        found = await self._get_collection(self.bucket.bucket,
                                           collection.scope_name,
                                           collection.name)
        self.assertIsNone(found)

    @async_test
    async def test_drop_collection(self):
        collection = CollectionSpec("other-collection")
        await self.cm.create_collection(collection)
        # verify the collection exists w/in other-bucket
        await self.try_n_times_till_exception_async(
            4, 1, self.cm.create_collection, collection, expected_exceptions=(
                CollectionAlreadyExistsException,))
        # attempt to drop it again will raise CollectionNotFoundException
        await self.cm.drop_collection(collection)
        with self.assertRaises(CollectionNotFoundException):
            await self.cm.drop_collection(collection)

    @async_test
    async def test_drop_collection_not_found(self):
        with self.assertRaises(CollectionNotFoundException):
            await self.cm.drop_collection(CollectionSpec("somerandomname"))

    @async_test
    async def test_drop_collection_scope_not_found(self):
        with self.assertRaises(ScopeNotFoundException):
            await self.cm.drop_collection(CollectionSpec("collectionname", "scopename"))
