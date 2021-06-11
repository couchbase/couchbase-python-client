from unittest import SkipTest

from couchbase_tests.base import CollectionTestCase
from couchbase.exceptions import DocumentNotFoundException
import couchbase.subdocument as SD


class MutationTokensEnabledTests(CollectionTestCase):
    """
    These tests should just test the collection interface, as simply
    as possible.  We have the Scenario tests for more complicated
    stuff.
    """
    CONTENT = {"some": "content"}
    KEY = "imakey"
    NOKEY = "somerandomkey"

    def setUp(self):
        super(MutationTokensEnabledTests, self).setUp()
        # retry just in case doc is locked from previous test
        self.try_n_times(10, 3, self.cb.upsert, self.KEY, self.CONTENT)
        # be sure NOKEY isn't in there
        try:
            self.cb.remove(self.NOKEY)
        except DocumentNotFoundException as e:
            pass
        # make sure NOKEY is gone
        self.try_n_times_till_exception(10, 1, self.cb.get, self.NOKEY)

    def verify_mutation_tokens(self, result):
        mutation_token = result.mutation_token()
        self.assertTrue(mutation_token)
        vb, uuid, seq, bktname = mutation_token.as_tuple()
        self.assertIsInstance(vb, int)
        self.assertIsInstance(uuid, int)
        self.assertIsInstance(seq, int)
        self.assertEqual(self.cb.bucket.bucket, bktname)

    def test_mutation_tokens_upsert(self):
        result = self.cb.upsert(self.NOKEY, {"some": "thing"})
        self.verify_mutation_tokens(result)

    def test_mutation_tokens_insert(self):
        result = self.cb.insert(self.NOKEY, {"some": "thing"})
        self.verify_mutation_tokens(result)

    def test_mutation_tokens_replace(self):
        result = self.cb.replace(self.KEY, {"some": "other content"})
        self.verify_mutation_tokens(result)

    def test_mutation_tokens_remove(self):
        result = self.cb.remove(self.KEY)
        self.verify_mutation_tokens(result)

    def test_mutation_tokens_touch(self):
        raise SkipTest('Pending mutation token implementation for touch')
        #result = self.cb.touch(self.KEY, timedelta(seconds=3))
        # self.verify_mutation_tokens(result)

    def test_mutation_tokens_mutate_in(self):
        def cas_matches(key, cas):
            result = self.cb.get(key)
            if result.cas == cas:
                return result
            raise Exception("nope")
        cas = self.cb.upsert(
            self.KEY, {"a": "aaa", "b": {"c": {"d": "yo!"}}}).cas
        self.try_n_times(10, 3, cas_matches, self.KEY, cas)
        result = self.coll.mutate_in(
            self.KEY, (SD.upsert("c", "ccc"), SD.replace("b", "XXX"),))
        self.verify_mutation_tokens(result)


class MutationTokensDisabledTests(CollectionTestCase):
    """
    These tests should just test the collection interface, as simply
    as possible.  We have the Scenario tests for more complicated
    stuff.
    """
    CONTENT = {"some": "content"}
    KEY = "imakey"
    NOKEY = "somerandomkey"

    def setUp(self):
        super(MutationTokensDisabledTests, self).setUp(
            enable_mutation_tokens=False)
        # retry just in case doc is locked from previous test
        self.try_n_times(10, 3, self.cb.upsert, self.KEY, self.CONTENT)
        # be sure NOKEY isn't in there
        try:
            self.cb.remove(self.NOKEY)
        except DocumentNotFoundException as e:
            pass
        # make sure NOKEY is gone
        self.try_n_times_till_exception(10, 1, self.cb.get, self.NOKEY)

    def test_mutinfo_upsert(self):
        result = self.cb.upsert(self.NOKEY, {"some": "thing"})
        self.assertIsNone(result.mutation_token())

    def test_mutinfo_insert(self):
        result = self.cb.insert(self.NOKEY, {"some": "thing"})
        self.assertIsNone(result.mutation_token())

    def test_mutinfo_replace(self):
        result = self.cb.replace(self.KEY, {"some": "other content"})
        self.assertIsNone(result.mutation_token())

    def test_mutinfo_remove(self):
        result = self.cb.remove(self.KEY)
        self.assertIsNone(result.mutation_token())

    def test_mutation_tokens_touch(self):
        raise SkipTest('Pending mutation token implementation for touch')
        #result = self.cb.touch(self.KEY, timedelta(seconds=3))
        # self.assertIsNone(result.mutation_token())

    def test_mutation_tokens_mutate_in(self):
        def cas_matches(key, cas):
            result = self.cb.get(key)
            if result.cas == cas:
                return result
            raise Exception("nope")
        cas = self.cb.upsert(
            self.KEY, {"a": "aaa", "b": {"c": {"d": "yo!"}}}).cas
        self.try_n_times(10, 3, cas_matches, self.KEY, cas)
        result = self.coll.mutate_in(
            self.KEY, (SD.upsert("c", "ccc"), SD.replace("b", "XXX"),))
        self.assertIsNone(result.mutation_token())
