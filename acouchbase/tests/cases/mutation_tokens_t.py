from unittest import SkipTest
from functools import wraps
from nose.tools import nottest

from acouchbase.cluster import (Cluster, get_event_loop,
                                close_event_loop)
from couchbase_tests.async_base import AsyncioTestCase
from couchbase.exceptions import DocumentNotFoundException
import couchbase.subdocument as SD


@nottest
def async_test(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        return self.loop.run_until_complete(func(self, *args, **kwargs))

    return wrapper


class AcouchbaseMutationTokensEnabledTests(AsyncioTestCase):

    CONTENT = {"some": "content"}
    KEY = "imakey"
    NOKEY = "somerandomkey"

    @classmethod
    def setUpClass(cls) -> None:
        super(AcouchbaseMutationTokensEnabledTests, cls).setUpClass(
            get_event_loop(), cluster_class=Cluster)

    @classmethod
    def tearDownClass(cls) -> None:
        super(AcouchbaseMutationTokensEnabledTests, cls).tearDownClass()
        close_event_loop()

    def setUp(self):
        super(AcouchbaseMutationTokensEnabledTests, self).setUp()

        self.loop.run_until_complete(self.initialize())

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

    def verify_mutation_tokens(self, result):
        mutation_token = result.mutation_token()
        self.assertTrue(mutation_token)
        vb, uuid, seq, bktname = mutation_token.as_tuple()
        self.assertIsInstance(vb, int)
        self.assertIsInstance(uuid, int)
        self.assertIsInstance(seq, int)
        self.assertEqual(self.bucket_name, bktname)

    @async_test
    async def test_mutation_tokens_upsert(self):
        result = await self.collection.upsert(self.NOKEY, {"some": "thing"})
        self.verify_mutation_tokens(result)

    @async_test
    async def test_mutation_tokens_insert(self):
        result = await self.collection.insert(self.NOKEY, {"some": "thing"})
        self.verify_mutation_tokens(result)

    @async_test
    async def test_mutation_tokens_replace(self):
        result = await self.collection.replace(self.KEY, {"some": "other content"})
        self.verify_mutation_tokens(result)

    @async_test
    async def test_mutation_tokens_remove(self):
        result = await self.collection.remove(self.KEY)
        self.verify_mutation_tokens(result)

    def test_mutation_tokens_touch(self):
        raise SkipTest('Pending mutation token implementation for touch')
        # result = await self.collection.touch(self.KEY, timedelta(seconds=3))
        # self.verify_mutation_tokens(result)

    @async_test
    async def test_mutation_tokens_mutate_in(self):
        async def cas_matches(key, cas):
            result = await self.collection.get(key)
            if result.cas == cas:
                return result
            raise Exception("nope")
        res = await self.collection.upsert(self.KEY, {"a": "aaa", "b": {"c": {"d": "yo!"}}})
        await self.try_n_times_async(10, 3, cas_matches, self.KEY, res.cas)
        result = await self.collection.mutate_in(self.KEY, (SD.upsert("c", "ccc"), SD.replace("b", "XXX"),))
        self.verify_mutation_tokens(result)


class AcouchbaseMutationTokensDisabledTests(AsyncioTestCase):

    CONTENT = {"some": "content"}
    KEY = "imakey"
    NOKEY = "somerandomkey"

    @classmethod
    def setUpClass(cls) -> None:
        super(AcouchbaseMutationTokensDisabledTests, cls).setUpClass(
            get_event_loop(), cluster_class=Cluster, enable_mutation_tokens=False)

    @classmethod
    def tearDownClass(cls) -> None:
        super(AcouchbaseMutationTokensDisabledTests, cls).tearDownClass()
        close_event_loop()

    def setUp(self):
        super(AcouchbaseMutationTokensDisabledTests, self).setUp()

        self.loop.run_until_complete(self.initialize())

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
    async def test_mutinfo_upsert(self):
        result = await self.collection.upsert(self.NOKEY, {"some": "thing"})
        self.assertIsNone(result.mutation_token())

    @async_test
    async def test_mutinfo_insert(self):
        result = await self.collection.insert(self.NOKEY, {"some": "thing"})
        self.assertIsNone(result.mutation_token())

    @async_test
    async def test_mutinfo_replace(self):
        result = await self.collection.replace(self.KEY, {"some": "other content"})
        self.assertIsNone(result.mutation_token())

    @async_test
    async def test_mutinfo_remove(self):
        result = await self.collection.remove(self.KEY)
        self.assertIsNone(result.mutation_token())

    def test_mutation_tokens_touch(self):
        raise SkipTest('Pending mutation token implementation for touch')
        # result = await self.collection.touch(self.KEY, timedelta(seconds=3))
        # self.assertIsNone(result.mutation_token())

    @async_test
    async def test_mutation_tokens_mutate_in(self):
        async def cas_matches(key, cas):
            result = await self.collection.get(key)
            if result.cas == cas:
                return result
            raise Exception("nope")
        res = await self.collection.upsert(
            self.KEY, {"a": "aaa", "b": {"c": {"d": "yo!"}}})
        await self.try_n_times_async(10, 3, cas_matches, self.KEY, res.cas)
        result = await self.collection.mutate_in(self.KEY, (SD.upsert("c", "ccc"), SD.replace("b", "XXX"),))
        self.assertIsNone(result.mutation_token())
