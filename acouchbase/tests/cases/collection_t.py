import asyncio
from functools import wraps
from nose.tools import nottest
from datetime import datetime
from datetime import timedelta
from unittest import SkipTest
from flaky import flaky

from acouchbase.cluster import (Cluster, get_event_loop,
                                close_event_loop)
from couchbase_tests.async_base import AsyncioTestCase
from couchbase.collection import UpsertOptions, ReplaceOptions, GetOptions
from couchbase.durability import ClientDurability, PersistTo, ReplicateTo
from couchbase.exceptions import (DocumentNotFoundException,
                                  DocumentExistsException, CASMismatchException, DurabilityImpossibleException,
                                  PathNotFoundException, InvalidArgumentException,
                                  DocumentLockedException, TemporaryFailException)


@nottest
def async_test(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        return self.loop.run_until_complete(func(self, *args, **kwargs))

    return wrapper


class AcouchbaseCollectionTestSuite(object):
    CONTENT = {"some": "content"}
    KEY = "imakey"
    NOKEY = "somerandomkey"

    async def initialize(self):
        # retry just in case doc is locked from previous test
        await self.try_n_times_async(1, 3, self.collection.upsert, self.KEY, self.CONTENT)

        # be sure NOKEY isn't in there
        try:
            await self.collection.remove(self.NOKEY)
        except DocumentNotFoundException:
            pass
        # make sure NOKEY is gone
        await self.try_n_times_till_exception_async(1, 1, self.collection.get, self.NOKEY)

    @async_test
    async def test_upsert(self):
        await self.collection.upsert(self.NOKEY, {"some": "thing"},
                                     UpsertOptions(timeout=timedelta(seconds=3)))
        result = await self.try_n_times_async(10, 1, self.collection.get, self.NOKEY)
        self.assertEqual(self.NOKEY, result.id)
        self.assertDictEqual({"some": "thing"}, result.content_as[dict])
        await asyncio.sleep(4)
        self.assertTrue(True)

    @async_test
    async def test_insert(self):
        await self.collection.insert(self.NOKEY, {"some": "thing"})
        result = await self.try_n_times_async(10, 1, self.collection.get, self.NOKEY)
        self.assertEqual(self.NOKEY, result.id)
        self.assertDictEqual({"some": "thing"}, result.content_as[dict])

    @async_test
    async def test_insert_fail(self):
        with self.assertRaises(DocumentExistsException):
            await self.collection.insert(self.KEY, self.CONTENT)

    @async_test
    async def test_replace(self):
        result = await self.collection.replace(self.KEY, {"some": "other content"})
        self.assertTrue(result.success)

    @async_test
    async def test_replace_preserve_expiry_not_used(self):
        if self.is_mock:
            raise SkipTest("Mock does not support preserve expiry")
        if int(self.cluster_version.split('.')[0]) < 7:
            raise SkipTest("Preserve expiry only in CBS 7.0+")
        result = await self.collection.upsert(self.KEY, {"some": "other content"}, UpsertOptions(
            expiry=timedelta(seconds=5)))
        expiry1 = await self.collection.get(self.KEY, GetOptions(
            with_expiry=True)).expiryTime
        result = await self.collection.replace(self.KEY, {"some": "replaced content"})
        self.assertTrue(result.success)
        expiry2 = await self.collection.get(self.KEY, GetOptions(
            with_expiry=True)).expiryTime
        self.assertIsNotNone(expiry1)
        self.assertIsInstance(expiry1, datetime)
        self.assertIsNone(expiry2)
        self.assertNotEqual(expiry1, expiry2)
        # if expiry was set, should be expired by now
        await asyncio.sleep(6)
        result = await self.collection.get(self.KEY)
        self.assertIsNotNone(result)

    @async_test
    async def test_replace_preserve_expiry(self):
        if self.is_mock:
            raise SkipTest("Mock does not support preserve expiry")
        if int(self.cluster_version.split('.')[0]) < 7:
            raise SkipTest("Preserve expiry only in CBS 7.0+")
        result = await self.collection.upsert(self.KEY, {"some": "other content"}, UpsertOptions(
            expiry=timedelta(seconds=5)))
        expiry1 = await self.collection.get(self.KEY, GetOptions(
            with_expiry=True)).expiryTime
        result = await self.collection.replace(
            self.KEY, {"some": "replaced content"}, ReplaceOptions(preserve_expiry=True))
        self.assertTrue(result.success)
        expiry2 = await self.collection.get(self.KEY, GetOptions(
            with_expiry=True)).expiryTime
        self.assertIsNotNone(expiry1)
        self.assertIsInstance(expiry1, datetime)
        self.assertIsNotNone(expiry2)
        self.assertIsInstance(expiry2, datetime)
        self.assertEqual(expiry1, expiry2)
        # if expiry was preserved, should be expired by now
        await asyncio.sleep(6)
        with self.assertRaises(DocumentNotFoundException):
            await self.collection.get(self.KEY)

    @async_test
    async def test_replace_preserve_expiry_fail(self):
        if self.is_mock:
            raise SkipTest("Mock does not support preserve expiry")
        if int(self.cluster_version.split('.')[0]) < 7:
            raise SkipTest("Preserve expiry only in CBS 7.0+")
        opts = ReplaceOptions(expiry=timedelta(
            seconds=5), preserve_expiry=True)
        with self.assertRaises(InvalidArgumentException):
            await self.collection.replace(self.KEY, {"some": "other content"}, opts)

    @async_test
    async def test_replace_with_cas(self):
        result = await self.collection.get(self.KEY)
        old_cas = result.cas
        result = await self.collection.replace(
            self.KEY, self.CONTENT, ReplaceOptions(cas=old_cas))
        self.assertTrue(result.success)
        # try same cas again, must fail.
        with self.assertRaises(CASMismatchException):
            await self.collection.replace(self.KEY, self.CONTENT, ReplaceOptions(cas=old_cas))

    @async_test
    async def test_replace_fail(self):
        with self.assertRaises(DocumentNotFoundException):
            await self.collection.get(self.NOKEY)

        with self.assertRaises(DocumentNotFoundException):
            await self.collection.replace(self.NOKEY, self.CONTENT)

    @async_test
    async def test_remove(self):
        result = await self.collection.remove(self.KEY)
        self.assertTrue(result.success)
        await self.try_n_times_till_exception_async(10, 1, self.collection.get, self.KEY)

    @async_test
    async def test_remove_fail(self):
        with self.assertRaises(DocumentNotFoundException):
            await self.collection.remove(self.NOKEY)

    @async_test
    async def test_get(self):
        result = await self.collection.get(self.KEY)
        self.assertIsNotNone(result.cas)
        self.assertEqual(result.id, self.KEY)
        self.assertIsNone(result.expiryTime)
        self.assertDictEqual(self.CONTENT, result.content_as[dict])

    @async_test
    async def test_get_fails(self):
        with self.assertRaises(DocumentNotFoundException):
            await self.collection.get(self.NOKEY)

    @async_test
    async def test_expiry_really_expires(self):
        result = await self.collection.upsert(
            self.KEY, self.CONTENT, UpsertOptions(expiry=timedelta(seconds=3)))
        self.assertTrue(result.success)
        await asyncio.sleep(4)
        with self.assertRaises(DocumentNotFoundException):
            await self.collection.get(self.KEY)

    @async_test
    async def test_get_with_expiry(self):
        if self.is_mock:
            raise SkipTest("mock will not return the expiry in the xaddrs")

        expiry_time = 300
        await self.collection.upsert(self.KEY, self.CONTENT, UpsertOptions(
            expiry=timedelta(seconds=expiry_time)))

        result = await self.collection.get(self.KEY, GetOptions(with_expiry=True))
        self.assertIsNotNone(result.expiryTime)
        self.assertDictEqual(self.CONTENT, result.content_as[dict])
        expires_in = (result.expiryTime - datetime.now()).total_seconds()
        # seems to be consistently 15 seconds off
        expiry_check = expiry_time + 15
        self.assertTrue(expiry_check >= expires_in > 0,
                        msg="Expected expires_in {} to be between {} and 0".format(expires_in, expiry_check))

    @async_test
    async def test_project(self):
        content = {"a": "aaa", "b": "bbb", "c": "ccc"}
        result = await self.collection.upsert(self.KEY, content)
        cas = result.cas

        async def cas_matches(c, new_cas):
            get_res = await c.get(self.KEY)

            if new_cas != get_res.cas:
                raise Exception("nope")

        await self.try_n_times_async(10, 3, cas_matches, self.collection, cas)
        result = await self.collection.get(self.KEY, GetOptions(project=["a"]))
        self.assertEqual({"a": "aaa"}, result.content_as[dict])
        self.assertIsNotNone(result.cas)
        self.assertEqual(result.id, self.KEY)
        self.assertIsNone(result.expiryTime)

    @async_test
    async def test_project_bad_path(self):
        result = await self.collection.get(self.KEY, GetOptions(project=["some", "qzx"]))
        self.assertTrue(result.success)
        with self.assertRaisesRegex(PathNotFoundException, 'qzx'):
            result.content_as[dict]

    @async_test
    async def test_project_bad_project_string(self):
        with self.assertRaises(InvalidArgumentException):
            await self.collection.get(self.KEY, GetOptions(project="something"))

    @async_test
    async def test_project_bad_project_too_long(self):
        project = []
        for _ in range(17):
            project.append("something")

        with self.assertRaisesRegex(InvalidArgumentException, "16 operations or less"):
            self.collection.get(self.KEY, GetOptions(project=project))

    @async_test
    async def test_touch(self):
        await self.collection.touch(self.KEY, timedelta(seconds=3))
        await asyncio.sleep(4)
        with self.assertRaises(DocumentNotFoundException):
            await self.collection.get(self.KEY)

    @async_test
    async def test_get_and_touch(self):
        await self.collection.get_and_touch(self.KEY, timedelta(seconds=3))
        await self.collection.get(self.KEY)
        await self.try_n_times_till_exception_async(
            10, 3, self.collection.get, self.KEY, DocumentNotFoundException)

    @async_test
    async def test_get_and_lock(self):
        await self.collection.get_and_lock(self.KEY, timedelta(seconds=3))
        # upsert should definitely fail
        with self.assertRaises(DocumentLockedException):
            await self.collection.upsert(self.KEY, self.CONTENT)
        # but succeed eventually
        await self.try_n_times_async(10, 1, self.collection.upsert, self.KEY, self.CONTENT)

    @async_test
    async def test_get_and_lock_upsert_with_cas(self):
        result = await self.collection.get_and_lock(self.KEY, timedelta(seconds=15))
        cas = result.cas
        with self.assertRaises(DocumentLockedException):
            await self.collection.upsert(self.KEY, self.CONTENT)
        await self.collection.replace(self.KEY, self.CONTENT, ReplaceOptions(cas=cas))

    @async_test
    async def test_unlock(self):
        result = await self.collection.get_and_lock(self.KEY, timedelta(seconds=15))
        cas = result.cas
        await self.collection.unlock(self.KEY, cas)
        await self.collection.upsert(self.KEY, self.CONTENT)

    @flaky(5, 1)
    @async_test
    async def test_unlock_wrong_cas(self):
        result = await self.collection.get_and_lock(self.KEY, timedelta(seconds=15))
        cas = result.cas
        expectedException = TemporaryFailException if self.is_mock else DocumentLockedException
        with self.assertRaises(expectedException):
            await self.collection.unlock(self.KEY, 100)
        await self.collection.unlock(self.KEY, cas)

    @async_test
    async def test_client_durable_upsert(self):
        num_replicas = self.bucket._bucket.configured_replica_count
        if num_replicas < 2:
            raise SkipTest('need replicas to test ClientDurability')

        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        await self.collection.upsert(self.NOKEY, self.CONTENT,
                                     UpsertOptions(durability=durability))
        result = await self.collection.get(self.NOKEY)
        self.assertEqual(self.CONTENT, result.content_as[dict])

    # TODO:  durability and replica testing


class AcouchbaseDefaultCollectionTests(AsyncioTestCase, AcouchbaseCollectionTestSuite):
    @classmethod
    def setUpClass(cls) -> None:
        super(AcouchbaseDefaultCollectionTests, cls).setUpClass(
            get_event_loop(), cluster_class=Cluster)

    @classmethod
    def tearDownClass(cls) -> None:
        super(AcouchbaseDefaultCollectionTests, cls).tearDownClass()
        close_event_loop()

    def setUp(self):
        super(AcouchbaseDefaultCollectionTests, self).setUp()

        self.loop.run_until_complete(self.initialize())

# TODO: use once CCBC-1412 is worked out.
# class AcouchbaseCollectionTests(AsyncioTestCase, AcouchbaseCollectionTestSuite):

#     @classmethod
#     def setUpClass(cls) -> None:
#         super(AcouchbaseCollectionTests, cls).setUpClass(
#             get_event_loop(), cluster_class=Cluster, use_scopes_and_colls=True)

#     @classmethod
#     def tearDownClass(cls) -> None:
#         super(AcouchbaseCollectionTests, cls).tearDownClass()
#         close_event_loop()

#     def setUp(self):
#         super(AcouchbaseCollectionTests, self).setUp()

#         if not self.supports_scopes_and_collections:
#             raise SkipTest('Scopes and Collections not supported.')

#         self.loop.run_until_complete(self.initialize())
