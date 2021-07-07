import asyncio
from functools import wraps
from datetime import datetime, timedelta

from unittest import SkipTest
from nose.tools import nottest

from acouchbase.cluster import (Cluster, get_event_loop,
                                close_event_loop)
from couchbase_tests.async_base import AsyncioTestCase
import couchbase.subdocument as SD
from couchbase.collection import (GetOptions, LookupInOptions, MutateInOptions)
from couchbase.durability import ClientDurability
from couchbase.exceptions import (DocumentNotFoundException, InvalidArgumentException, PathExistsException,
                                  PathNotFoundException, DurabilityImpossibleException, SubdocCantInsertValueException, SubdocPathMismatchException)


@nottest
def async_test(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        return self.loop.run_until_complete(func(self, *args, **kwargs))

    return wrapper


class AcouchbaseSubdocTestSuite(object):
    KEY = "imakey"

    async def initialize(self):
        # be sure KEY isn't in there
        try:
            await self.collection.remove(self.KEY)
        except DocumentNotFoundException:
            pass
        # make sure NOKEY is gone
        await self.try_n_times_till_exception_async(1, 1, self.collection.get, self.KEY)

    async def _cas_matches(self, key, cas):
        result = await self.collection.get(key)
        if result.cas == cas:
            return result
        raise Exception("nope")

    @async_test
    async def test_lookup_in_simple_get(self):
        result = await self.collection.upsert(self.KEY, {"a": "aaa", "b": [1, 2, 3, 4]})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        result = await self.collection.lookup_in(self.KEY, (SD.get("b"),))
        self.assertEqual(result.cas, cas)
        self.assertEqual([1, 2, 3, 4], result.content_as[list](0))

    @async_test
    async def test_lookup_in_simple_exists(self):
        result = await self.collection.upsert(self.KEY, {"a": "aaa", "b": [1, 2, 3, 4]})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        result = await self.collection.lookup_in(self.KEY, (SD.exists("b"),))
        self.assertEqual(result.cas, cas)
        self.assertTrue(result.exists(0))
        # no content; only valid path, returns None so is False
        self.assertFalse(result.content_as[bool](0))

    @async_test
    async def test_lookup_in_simple_exists_bad_path(self):
        result = await self.collection.upsert(self.KEY, {"a": "aaa", "b": [1, 2, 3, 4]})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        result = await self.collection.lookup_in(self.KEY, (SD.exists("qzzxy"),))
        self.assertEqual(result.cas, cas)
        self.assertFalse(result.exists(0))
        self.assertRaises(PathNotFoundException, result.content_as[bool], 0)

    @async_test
    async def test_lookup_in_one_path_not_found(self):
        result = await self.collection.upsert(self.KEY, {"a": "aaa", "b": [1, 2, 3, 4]})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        result = await self.collection.lookup_in(
            self.KEY, (SD.exists("a"), SD.exists("qzzxy"),))
        self.assertTrue(result.exists(0))
        self.assertFalse(result.exists(1))

    @async_test
    async def test_lookup_in_simple_get_longer_path(self):
        result = await self.collection.upsert(
            self.KEY, {"a": "aaa", "b": {"c": {"d": "yo!"}}})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        result = await self.collection.lookup_in(self.KEY, (SD.get("b.c.d"),))
        self.assertEqual(result.cas, cas)
        self.assertEqual("yo!", result.content_as[str](0))

    @async_test
    async def test_lookup_in_multiple_specs(self):
        if self.is_mock:
            raise SkipTest(
                "mock doesn't support getting xattrs (like $document.expiry)")
        result = await self.collection.upsert(
            self.KEY, {"a": "aaa", "b": {"c": {"d": "yo!"}}})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        result = await self.collection.lookup_in(self.KEY,
                                                 (SD.with_expiry(),
                                                  SD.get("a"),
                                                     SD.exists("b"),
                                                     SD.get("b.c")))
        self.assertTrue(result.success)
        self.assertIsNone(result.expiry)
        self.assertEqual("aaa", result.content_as[str](1))
        self.assertTrue(result.exists(2))
        self.assertDictEqual({"d": "yo!"}, result.content_as[dict](3))

    @async_test
    async def test_mutate_in_simple(self):
        result = await self.collection.upsert(self.KEY, {"a": "aaa", "b": "bbb"})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)

        result = await self.collection.mutate_in(
            self.KEY, (SD.upsert("c", "ccc"), SD.replace("b", "XXX"),))
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        result = await self.collection.get(self.KEY)
        self.assertDictEqual(
            {"a": "aaa", "b": "XXX", "c": "ccc"}, result.content_as[dict])

    @async_test
    async def test_mutate_in_expiry(self):
        if self.is_mock:
            raise SkipTest(
                "mock doesn't support getting xattrs (like $document.expiry)")

        result = await self.collection.upsert(self.KEY, {"a": "aaa", "b": "bbb"})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)

        result = await self.collection.mutate_in(self.KEY,
                                                 (SD.upsert("c", "ccc"),
                                                  SD.replace("b", "XXX"),),
                                                 MutateInOptions(expiry=timedelta(seconds=1000)))
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        result = await self.collection.get(self.KEY, GetOptions(with_expiry=True))
        expires_in = (result.expiryTime - datetime.now()).total_seconds()
        self.assertTrue(0 < expires_in < 1001)

    @async_test
    async def test_mutate_in_durability(self):
        if self.is_mock:
            raise SkipTest(
                "mock doesn't support getting xattrs (like $document.expiry)")

        result = await self.collection.upsert(self.KEY, {"a": "aaa", "b": "bbb"})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)

        with self.assertRaises(DurabilityImpossibleException):
            await self.collection.mutate_in(self.KEY,
                                            (SD.upsert("c", "ccc"),
                                             SD.replace("b", "XXX"),),
                                            MutateInOptions(durability=ClientDurability(replicate_to=5)))

    # refactor!  Also, this seems like it should timeout.  I suspect a bug here.  I don't really
    # believe there is any way this could not timeout on the first lookup_in
    @async_test
    async def test_lookup_in_timeout(self):
        await self.collection.upsert("id", {'someArray': ['wibble', 'gronk']})
        # wait till it is there
        await self.try_n_times_async(10, 1, self.collection.get, "id")

        # ok, it is there...
        await self.collection.get("id", GetOptions(
            project=["someArray"], timeout=timedelta(seconds=1.0)))
        self.assertRaisesRegex(InvalidArgumentException, "Expected timedelta", self.collection.get, "id",
                               GetOptions(project=["someArray"], timeout=456))
        sdresult_2 = await self.collection.lookup_in(
            "id", (SD.get("someArray"),), LookupInOptions(timeout=timedelta(microseconds=1)))
        self.assertEqual(['wibble', 'gronk'], sdresult_2.content_as[list](0))
        sdresult_2 = await self.collection.lookup_in("id", (SD.get("someArray"),), LookupInOptions(
            timeout=timedelta(seconds=1)), timeout=timedelta(microseconds=1))
        self.assertEqual(['wibble', 'gronk'], sdresult_2.content_as[list](0))

    @async_test
    async def test_array_append(self):
        result = await self.collection.upsert(self.KEY, {"a": "aaa", "b": [1, 2, 3, 4]})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        await self.collection.mutate_in(self.KEY, (SD.array_append("b", 5),))
        result = await self.collection.get(self.KEY)
        content = result.content_as[dict]
        self.assertIsInstance(content["b"], list)
        self.assertEqual(len(content["b"]), 5)
        self.assertEqual(5, content["b"][4])

    @async_test
    async def test_array_prepend(self):
        result = await self.collection.upsert(
            self.KEY, {"a": "aaa", "b": [1, 2, 3, 4]})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        await self.collection.mutate_in(self.KEY, (SD.array_prepend("b", 0),))
        result = await self.collection.get(self.KEY)
        content = result.content_as[dict]
        self.assertIsInstance(content["b"], list)
        self.assertEqual(len(content["b"]), 5)
        self.assertEqual(0, content["b"][0])

    @async_test
    async def test_array_insert(self):
        result = await self.collection.upsert(self.KEY, {"a": "aaa", "b": [0, 1, 3, 4]})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        await self.collection.mutate_in(self.KEY, (SD.array_insert("b.[2]", 2),))
        result = await self.collection.get(self.KEY)
        content = result.content_as[dict]
        self.assertIsInstance(content["b"], list)
        self.assertEqual(len(content["b"]), 5)
        self.assertEqual(2, content["b"][2])

    @async_test
    async def test_array_as_document(self):
        result = await self.collection.upsert(self.KEY, [])
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        await self.collection.mutate_in(self.KEY, (SD.array_append(
            "", 2), SD.array_prepend("", 0), SD.array_insert("[1]", 1)))
        result = await self.collection.get(self.KEY)
        content = result.content_as[list]
        self.assertIsInstance(content, list)
        self.assertEqual(len(content), 3)
        self.assertEqual(0, content[0])
        self.assertEqual(1, content[1])
        self.assertEqual(2, content[2])

    @async_test
    async def test_array_append_multi_insert(self):
        result = await self.collection.upsert(
            self.KEY, {"a": "aaa", "b": [1, 2, 3, 4, 5, 6, 7]})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        await self.collection.mutate_in(self.KEY, (SD.array_append("b", 8, 9, 10),))
        result = await self.collection.get(self.KEY)
        content = result.content_as[dict]
        self.assertIsInstance(content["b"], list)
        insert_res = content["b"][7:]
        self.assertEqual(len(insert_res), 3)
        self.assertEqual(insert_res, [8, 9, 10])

    @async_test
    async def test_array_prepend_multi_insert(self):
        result = await self.collection.upsert(
            self.KEY, {"a": "aaa", "b": [4, 5, 6, 7, 8, 9, 10]})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        await self.collection.mutate_in(self.KEY, (SD.array_prepend("b", 1, 2, 3),))
        result = await self.collection.get(self.KEY)
        content = result.content_as[dict]
        self.assertIsInstance(content["b"], list)
        insert_res = content["b"][:3]
        self.assertEqual(len(insert_res), 3)
        self.assertEqual(insert_res, [1, 2, 3])

    @async_test
    async def test_array_insert_multi_insert(self):
        result = await self.collection.upsert(
            self.KEY, {"a": "aaa", "b": [1, 2, 3, 4, 8, 9, 10]})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        await self.collection.mutate_in(self.KEY, (SD.array_insert("b.[4]", 5, 6, 7),))
        result = await self.collection.get(self.KEY)
        content = result.content_as[dict]
        self.assertIsInstance(content["b"], list)
        insert_res = content["b"][4:7]
        self.assertEqual(len(insert_res), 3)
        self.assertEqual(insert_res, [5, 6, 7])

    @async_test
    async def test_array_add_unique(self):
        result = await self.collection.upsert(self.KEY, {"a": "aaa", "b": [0, 1, 2, 3]})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        await self.collection.mutate_in(self.KEY, (SD.array_addunique("b", 4),))
        result = await self.collection.get(self.KEY)
        content = result.content_as[dict]
        self.assertIsInstance(content["b"], list)
        self.assertEqual(len(content["b"]), 5)
        self.assertIn(4, content["b"])

    @async_test
    async def test_array_add_unique_fail(self):
        result = await self.collection.upsert(self.KEY, {"a": "aaa", "b": [0, 1, 2, 3], "c": [
            1.25, 1.5, {"nested": ["str", "array"]}]})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        with self.assertRaises(PathExistsException):
            await self.collection.mutate_in(self.KEY, (SD.array_addunique("b", 3),))

        with self.assertRaises(SubdocCantInsertValueException):
            await self.collection.mutate_in(
                self.KEY, (SD.array_addunique("b", [4, 5, 6]),))

        # apparently adding floats is okay?
        # with self.assertRaises(SubdocCantInsertValueException):
        await self.collection.mutate_in(self.KEY, (SD.array_addunique("b", 4.5),))

        with self.assertRaises(SubdocCantInsertValueException):
            await self.collection.mutate_in(
                self.KEY, (SD.array_addunique("b", {"b1": "b1b1b1"}),))

        with self.assertRaises(SubdocPathMismatchException):
            await self.collection.mutate_in(self.KEY, (SD.array_addunique("c", 2),))

    @async_test
    async def test_counter_increment(self):
        result = await self.collection.upsert(self.KEY, {"a": "aaa", "count": 100})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        await self.collection.mutate_in(self.KEY, (SD.counter("count", 50),))
        result = await self.collection.get(self.KEY)
        content = result.content_as[dict]
        self.assertEqual(150, content["count"])

    @async_test
    async def test_counter_decrement(self):
        result = await self.collection.upsert(self.KEY, {"a": "aaa", "count": 100})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        await self.collection.mutate_in(self.KEY, (SD.counter("count", -50),))
        result = await self.collection.get(self.KEY)
        content = result.content_as[dict]
        self.assertEqual(50, content["count"])

    @async_test
    async def test_insert_create_parents(self):
        result = await self.collection.upsert(
            self.KEY, {"a": "aaa", "b": [0, 1, 3, 4]})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        await self.collection.mutate_in(self.KEY, (SD.insert(
            "c.some_string", "parents created", create_parents=True),))
        result = await self.collection.get(self.KEY)
        content = result.content_as[dict]
        self.assertEqual("parents created", content["c"]["some_string"])

    @async_test
    async def test_upsert_create_parents(self):
        result = await self.collection.upsert(
            self.KEY, {"a": "aaa", "b": [0, 1, 3, 4]})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        await self.collection.mutate_in(self.KEY, (SD.upsert(
            "c.some_string", "parents created", create_parents=True),))
        result = await self.collection.get(self.KEY)
        content = result.content_as[dict]
        self.assertEqual("parents created", content["c"]["some_string"])

    @async_test
    async def test_array_append_create_parents(self):
        result = await self.collection.upsert(
            self.KEY, {"a": "aaa", "b": [0, 1, 3, 4]})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        await self.collection.mutate_in(self.KEY, (SD.array_append(
            "some.array", "Hello", create_parents=True), SD.array_append("some.array", "World")))

        result = await self.collection.get(self.KEY)
        content = result.content_as[dict]
        self.assertIsInstance(content["some"]["array"], list)
        self.assertEqual(content["some"]["array"], ["Hello", "World"])

    @async_test
    async def test_array_prepend_create_parents(self):
        result = await self.collection.upsert(
            self.KEY, {"a": "aaa", "b": [0, 1, 3, 4]})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        await self.collection.mutate_in(self.KEY, (SD.array_prepend(
            "some.array", "World", create_parents=True), SD.array_prepend("some.array", "Hello")))

        result = await self.collection.get(self.KEY)
        content = result.content_as[dict]
        self.assertIsInstance(content["some"]["array"], list)
        self.assertEqual(content["some"]["array"], ["Hello", "World"])

    @async_test
    async def test_array_add_unique_create_parents(self):
        result = await self.collection.upsert(
            self.KEY, {"a": "aaa", "b": [0, 1, 3, 4]})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        await self.collection.mutate_in(self.KEY, (SD.array_addunique(
            "some.set", "my", create_parents=True), SD.array_addunique("some.set", "unique"), SD.array_addunique("some.set", "set")))
        result = await self.collection.get(self.KEY)
        content = result.content_as[dict]
        self.assertIsInstance(content["some"]["set"], list)
        self.assertIn("my", content["some"]["set"])
        self.assertIn("unique", content["some"]["set"])
        self.assertIn("set", content["some"]["set"])

    @async_test
    async def test_counter_create_parents(self):
        result = await self.collection.upsert(
            self.KEY, {"a": "aaa", "b": [0, 1, 3, 4]})
        cas = result.cas
        await self.try_n_times_async(10, 3, self._cas_matches, self.KEY, cas)
        await self.collection.mutate_in(self.KEY, (SD.counter(
            "some.counter", 100, create_parents=True),))
        result = await self.collection.get(self.KEY)
        content = result.content_as[dict]
        self.assertEqual(100, content["some"]["counter"])


class AcouchbaseDefaultCollectionSubdocTests(AsyncioTestCase, AcouchbaseSubdocTestSuite):
    @classmethod
    def setUpClass(cls) -> None:
        super(AcouchbaseDefaultCollectionSubdocTests, cls).setUpClass(
            get_event_loop(), cluster_class=Cluster)

    @classmethod
    def tearDownClass(cls) -> None:
        super(AcouchbaseDefaultCollectionSubdocTests, cls).tearDownClass()
        close_event_loop()

    def setUp(self):
        super(AcouchbaseDefaultCollectionSubdocTests, self).setUp()

        self.loop.run_until_complete(self.initialize())
