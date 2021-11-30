from couchbase_core._libcouchbase import FMT_UTF8, FMT_BYTES
from acouchbase.cluster import (Cluster, get_event_loop,
                                close_event_loop)
from couchbase_tests.async_base import AsyncioTestCase, async_test
from couchbase.exceptions import DocumentNotFoundException, NotStoredException
from couchbase.collection import IncrementOptions, DecrementOptions, DeltaValue, SignedInt64


class AcouchbaseBinaryCollectionTestSuite(object):
    UTF8_KEY = "async_bcoll_utf8"
    BYTES_KEY = "async_bcoll_bytes"
    COUNTER_KEY = "async_bcoll_counter"
    KEY = "imakey"
    NOKEY = "somerandomkey"

    async def initialize(self):
        await self.collection.upsert(self.UTF8_KEY, "", format=FMT_UTF8)
        await self.collection.upsert(self.BYTES_KEY, b"", format=FMT_BYTES)
        await self.try_n_times_async(5, 3, self.collection.get, self.UTF8_KEY)
        await self.try_n_times_async(5, 3, self.collection.get, self.BYTES_KEY)

    async def tear_down(self):
        await self.try_n_times_till_exception_async(10, 1,
                                                    self.collection.remove,
                                                    self.UTF8_KEY,
                                                    expected_exceptions=(DocumentNotFoundException,))
        await self.try_n_times_till_exception_async(10, 1,
                                                    self.collection.remove,
                                                    self.BYTES_KEY,
                                                    expected_exceptions=(DocumentNotFoundException,))
        await self.try_n_times_till_exception_async(10, 1,
                                                    self.collection.remove,
                                                    self.COUNTER_KEY,
                                                    expected_exceptions=(DocumentNotFoundException,))

    @async_test
    async def test_append_string_nokey(self):
        await self.collection.remove(self.UTF8_KEY)
        await self.try_n_times_till_exception_async(10, 1, self.collection.get, self.UTF8_KEY)
        with self.assertRaises(NotStoredException):
            await self.collection.binary().append(self.UTF8_KEY, "foo")

    @async_test
    async def test_prepend_string_nokey(self):
        await self.collection.remove(self.UTF8_KEY)
        await self.try_n_times_till_exception_async(10, 1, self.collection.get, self.UTF8_KEY)
        with self.assertRaises(NotStoredException):
            await self.collection.binary().prepend(self.UTF8_KEY, "foo")

    @async_test
    async def test_append_string(self):
        result = await self.collection.binary().append(self.UTF8_KEY, "foo")
        self.assertIsNotNone(result.cas)
        # make sure it really worked
        result = await self.collection.get(self.UTF8_KEY)
        self.assertEqual("foo", result.content_as[str])

    @async_test
    async def test_prepend_string(self):
        result = await self.collection.binary().prepend(self.UTF8_KEY, "foo")
        self.assertIsNotNone(result.cas)
        result = await self.collection.get(self.UTF8_KEY)
        self.assertEqual("foo", result.content_as[str])

    @async_test
    async def test_append_string_not_empty(self):
        await self.collection.upsert(self.UTF8_KEY, "XXXX", format=FMT_UTF8)
        result = await self.collection.binary().append(self.UTF8_KEY, "foo", format=FMT_UTF8)
        self.assertIsNotNone(result.cas)
        result = await self.collection.get(self.UTF8_KEY)
        self.assertEqual("XXXXfoo", result.content_as[str])

    @async_test
    async def test_prepend_string_not_empty(self):
        await self.collection.upsert(self.UTF8_KEY, "XXXX", format=FMT_UTF8)
        result = await self.collection.binary().prepend(self.UTF8_KEY, "foo", format=FMT_UTF8)
        self.assertIsNotNone(result.cas)
        result = await self.collection.get(self.UTF8_KEY)
        self.assertEqual("fooXXXX", result.content_as[str])

    @async_test
    async def test_prepend_bytes(self):
        result = await self.collection.binary().prepend(self.BYTES_KEY, b'XXX', format=FMT_BYTES)
        self.assertIsNotNone(result.cas)
        result = await self.collection.get(self.BYTES_KEY)
        self.assertEqual(b'XXX', result.content_as[bytes])

    @async_test
    async def test_append_bytes(self):
        result = await self.collection.binary().append(self.BYTES_KEY, b'XXX', format=FMT_BYTES)
        self.assertIsNotNone(result.cas)
        result = await self.collection.get(self.BYTES_KEY)
        self.assertEqual(b'XXX', result.content_as[bytes])

    @async_test
    async def test_prepend_bytes_not_empty(self):
        await self.collection.upsert(self.BYTES_KEY, b'XXX', format=FMT_BYTES)
        result = await self.collection.binary().prepend(self.BYTES_KEY, b'foo', format=FMT_BYTES)
        self.assertIsNotNone(result.cas)
        result = await self.collection.get(self.BYTES_KEY)
        self.assertEqual(b'fooXXX', result.content_as[bytes])

    @async_test
    async def test_append_bytes_not_empty(self):
        await self.collection.upsert(self.BYTES_KEY, b'XXX', format=FMT_BYTES)
        result = await self.collection.binary().append(self.BYTES_KEY, b'foo', format=FMT_BYTES)
        self.assertIsNotNone(result.cas)
        result = await self.collection.get(self.BYTES_KEY)
        self.assertEqual(b'XXXfoo', result.content_as[bytes])

    @async_test
    async def test_counter_increment(self):
        await self.collection.upsert(self.COUNTER_KEY, 100)
        result = await self.collection.binary().increment(self.COUNTER_KEY)
        self.assertTrue(result.success)
        result = await self.collection.get(self.COUNTER_KEY)
        self.assertEqual(101, result.content_as[int])

    @async_test
    async def test_counter_decrement(self):
        await self.collection.upsert(self.COUNTER_KEY, 100)
        result = await self.collection.binary().decrement(self.COUNTER_KEY)
        self.assertTrue(result.success)
        result = await self.collection.get(self.COUNTER_KEY)
        self.assertEqual(99, result.content_as[int])

    @async_test
    async def test_counter_increment_default_no_key(self):
        result = await self.collection.binary().increment(
            self.COUNTER_KEY, IncrementOptions(
                initial=SignedInt64(100)))
        self.assertTrue(result.success)
        result = await self.collection.get(self.COUNTER_KEY)
        self.assertEqual(100, result.content_as[int])

    @async_test
    async def test_counter_decrement_default_no_key(self):
        result = await self.collection.binary().decrement(
            self.COUNTER_KEY, DecrementOptions(
                initial=SignedInt64(100)))
        self.assertTrue(result.success)
        result = await self.collection.get(self.COUNTER_KEY)
        self.assertEqual(100, result.content_as[int])

    @async_test
    async def test_counter_increment_non_default(self):
        await self.collection.upsert(self.COUNTER_KEY, 100)
        result = await self.collection.binary().increment(
            self.COUNTER_KEY,
            IncrementOptions(
                delta=DeltaValue(3)))
        self.assertTrue(result.success)
        result = await self.collection.get(self.COUNTER_KEY)
        self.assertEqual(103, result.content_as[int])

    @async_test
    async def test_counter_decrement_non_default(self):
        await self.collection.upsert(self.COUNTER_KEY, 100)
        result = await self.collection.binary().decrement(
            self.COUNTER_KEY,
            DecrementOptions(
                delta=DeltaValue(3)))
        self.assertTrue(result.success)
        result = await self.collection.get(self.COUNTER_KEY)
        self.assertEqual(97, result.content_as[int])

    # skip test_unsigned_int + test_signed_int_64 as they are tested
    # via couchbase API and there are no changes for them in the acouchbase API


class AcouchbaseDefaultBinaryCollectionTests(
        AsyncioTestCase, AcouchbaseBinaryCollectionTestSuite):
    @classmethod
    def setUpClass(cls) -> None:
        super(AcouchbaseDefaultBinaryCollectionTests, cls).setUpClass(
            get_event_loop(), cluster_class=Cluster)

    @classmethod
    def tearDownClass(cls) -> None:
        super(AcouchbaseDefaultBinaryCollectionTests, cls).tearDownClass()
        close_event_loop()

    def setUp(self):
        super(AcouchbaseDefaultBinaryCollectionTests, self).setUp()
        self.loop.run_until_complete(self.initialize())

    def tearDown(self):
        self.loop.run_until_complete(self.tear_down())
        super(AcouchbaseDefaultBinaryCollectionTests, self).tearDown()
