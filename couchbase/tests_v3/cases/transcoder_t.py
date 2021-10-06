import platform
import json
from datetime import timedelta
from unittest import SkipTest
from nose.tools import nottest

from couchbase_tests.base import CollectionTestCase
from couchbase.exceptions import DocumentNotFoundException, ValueFormatException, DocumentLockedException
from couchbase.transcoder import (JSONTranscoder, RawJSONTranscoder,
                                  RawStringTranscoder, RawBinaryTranscoder, LegacyTranscoder)

from couchbase.collection import (GetOptions, UpsertOptions, InsertOptions, ReplaceOptions,
                                  GetAndTouchOptions, GetAndLockOptions, GetAnyReplicaOptions, GetAllReplicasOptions)


class DefaultTranscoderTestSuite(object):
    """
    These tests should just test the collection interface, as simply
    as possible.  We have the Scenario tests for more complicated
    stuff.
    """
    CONTENT = {"some": "content", "num": 1,
               "list": [1, 2, 3], "nested": {"a": "b"}}
    KEY = "imakey"

    def test_default_tc_json_upsert(self):
        self.cb.upsert(self.KEY, self.CONTENT)
        resp = self.cb.get(self.KEY)
        result = resp.content_as[dict]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(self.CONTENT, result)

    def test_default_tc_json_insert(self):
        self.cb.insert(self.KEY, self.CONTENT)
        resp = self.cb.get(self.KEY)
        result = resp.content_as[dict]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(self.CONTENT, result)

    def test_default_tc_json_replace(self):
        self.cb.upsert(self.KEY, self.CONTENT)
        new_content = self.CONTENT
        new_content["some"] = "new content"
        self.cb.replace(self.KEY, new_content)
        resp = self.cb.get(self.KEY)
        result = resp.content_as[dict]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(new_content, result)

    # default TC: no transcoder set in ClusterOptions or KV options
    def test_default_tc_string_upsert(self):
        content = "some string content"
        self.cb.upsert(self.KEY, content)
        resp = self.cb.get(self.KEY)
        result = resp.content_as[str]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertEqual(content, result)

    def test_default_tc_string_insert(self):
        content = "some string content"
        self.cb.insert(self.KEY, content)
        resp = self.cb.get(self.KEY)
        result = resp.content_as[str]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertEqual(content, result)

    def test_default_tc_string_replace(self):
        content = "some string content"
        self.cb.upsert(self.KEY, content)
        new_content = "new string content"
        self.cb.replace(self.KEY, new_content)
        resp = self.cb.get(self.KEY)
        result = resp.content_as[str]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertEqual(new_content, result)

    def test_default_tc_binary_upsert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        with self.assertRaises(ValueFormatException):
            self.cb.upsert(self.KEY, content)

    def test_default_tc_bytearray_upsert(self):
        content = bytearray(json.dumps("Here are some bytes"), "utf-8")
        with self.assertRaises(ValueFormatException):
            self.cb.upsert(self.KEY, content)

    def test_default_tc_binary_insert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        with self.assertRaises(ValueFormatException):
            self.cb.insert(self.KEY, content)

    def test_default_tc_binary_replace(self):
        content = "Lets to a str first"
        self.cb.upsert(self.KEY, content)
        new_content = bytes(json.dumps("Here are some newer bytes"), "utf-8")
        with self.assertRaises(ValueFormatException):
            self.cb.replace(self.KEY, new_content)


class DefaultTranscoderTests(CollectionTestCase, DefaultTranscoderTestSuite):

    def setUp(self):
        super(DefaultTranscoderTests, self).setUp()
        try:
            self.cb.remove(self.KEY)
        except DocumentNotFoundException:
            pass


class DefaultJsonTranscoderTests(CollectionTestCase, DefaultTranscoderTestSuite):

    @classmethod
    def setUpClass(cls) -> None:
        super(DefaultJsonTranscoderTests, cls).setUpClass(
            transcoder=JSONTranscoder())

    @classmethod
    def tearDownClass(cls) -> None:
        super(DefaultJsonTranscoderTests, cls).tearDownClass()

    def setUp(self):
        super(DefaultJsonTranscoderTests, self).setUp()
        try:
            self.cb.remove(self.KEY)
        except DocumentNotFoundException:
            pass


class RawJsonTranscoderTests(CollectionTestCase):

    CONTENT = {"some": "content", "num": 1,
               "list": [1, 2, 3], "nested": {"a": "b"}}
    KEY = "imakey"

    @classmethod
    def setUpClass(cls) -> None:
        super(RawJsonTranscoderTests, cls).setUpClass(
            transcoder=RawJSONTranscoder())

    @classmethod
    def tearDownClass(cls) -> None:
        super(RawJsonTranscoderTests, cls).tearDownClass()

    def setUp(self):
        super(RawJsonTranscoderTests, self).setUp()
        try:
            self.cb.remove(self.KEY)
        except DocumentNotFoundException:
            pass

    def test_raw_json_tc_json_upsert(self):
        with self.assertRaises(ValueFormatException):
            self.cb.upsert(self.KEY, self.CONTENT)

    def test_raw_json_tc_json_insert(self):
        with self.assertRaises(ValueFormatException):
            self.cb.insert(self.KEY, self.CONTENT)

    def test_raw_json_tc_json_replace(self):
        self.cb.upsert(self.KEY, "some string content")
        with self.assertRaises(ValueFormatException):
            self.cb.replace(self.KEY, self.CONTENT)

    def test_raw_json_tc_string_upsert(self):
        content = "some string content"
        self.cb.upsert(self.KEY, content)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result.decode("utf-8"))

    def test_raw_json_tc_string_insert(self):
        content = "some string content"
        self.cb.insert(self.KEY, content)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result.decode("utf-8"))

    def test_raw_json_tc_string_replace(self):
        content = "some string content"
        self.cb.upsert(self.KEY, content)
        new_content = "new string content"
        self.cb.replace(self.KEY, new_content)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(new_content, result.decode("utf-8"))

    def test_raw_json_tc_binary_upsert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        self.cb.upsert(self.KEY, content)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    def test_raw_json_tc_bytearray_upsert(self):
        content = bytearray(json.dumps("Here are some bytes"), "utf-8")
        self.cb.upsert(self.KEY, content)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    def test_raw_json_tc_binary_insert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        self.cb.insert(self.KEY, content)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    def test_raw_json_tc_binary_replace(self):
        content = "Lets to a str first"
        self.cb.upsert(self.KEY, content)
        new_content = bytes(json.dumps("Here are some newer bytes"), "utf-8")
        self.cb.replace(self.KEY, new_content)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(new_content, result)

    def test_pass_through(self):
        content = json.dumps(self.CONTENT)
        self.cb.upsert(self.KEY, content)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertNotEqual(self.CONTENT, result)
        # json.loads expects a string in Python 3.5
        if float(platform.python_version()[:3]) <= 3.5:
            result = result.decode("utf-8")
        decoded = json.loads(result)
        self.assertEqual(self.CONTENT, decoded)


class RawStringTranscoderTests(CollectionTestCase):
    CONTENT = {"some": "content", "num": 1,
               "list": [1, 2, 3], "nested": {"a": "b"}}
    KEY = "imakey"

    @classmethod
    def setUpClass(cls) -> None:
        super(RawStringTranscoderTests, cls).setUpClass(
            transcoder=RawStringTranscoder())

    @classmethod
    def tearDownClass(cls) -> None:
        super(RawStringTranscoderTests, cls).tearDownClass()

    def setUp(self):
        super(RawStringTranscoderTests, self).setUp()
        try:
            self.cb.remove(self.KEY)
        except DocumentNotFoundException:
            pass

    def test_raw_str_tc_json_upsert(self):
        with self.assertRaises(ValueFormatException):
            self.cb.upsert(self.KEY, self.CONTENT)

    def test_raw_str_tc_json_insert(self):
        with self.assertRaises(ValueFormatException):
            self.cb.insert(self.KEY, self.CONTENT)

    def test_raw_str_tc_json_replace(self):
        self.cb.upsert(self.KEY, "some string content")
        with self.assertRaises(ValueFormatException):
            self.cb.replace(self.KEY, self.CONTENT)

    def test_raw_json_tc_string_upsert(self):
        content = "some string content"
        self.cb.upsert(self.KEY, content)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertEqual(content, result)

    def test_raw_json_tc_string_insert(self):
        content = "some string content"
        self.cb.insert(self.KEY, content)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertEqual(content, result)

    def test_raw_json_tc_string_replace(self):
        content = "some string content"
        self.cb.upsert(self.KEY, content)
        new_content = "new string content"
        self.cb.replace(self.KEY, new_content)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertEqual(new_content, result)

    def test_raw_str_tc_binary_upsert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        with self.assertRaises(ValueFormatException):
            self.cb.upsert(self.KEY, content)

    def test_raw_str_tc_bytearray_upsert(self):
        content = bytearray(json.dumps("Here are some bytes"), "utf-8")
        with self.assertRaises(ValueFormatException):
            self.cb.upsert(self.KEY, content)

    def test_raw_str_tc_binary_insert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        with self.assertRaises(ValueFormatException):
            self.cb.insert(self.KEY, content)

    def test_raw_str_tc_binary_replace(self):
        self.cb.upsert(self.KEY, "some string content")
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        with self.assertRaises(ValueFormatException):
            self.cb.replace(self.KEY, content)


class RawBinaryTranscoderTests(CollectionTestCase):
    CONTENT = {"some": "content", "num": 1,
               "list": [1, 2, 3], "nested": {"a": "b"}}
    KEY = "imakey"

    @classmethod
    def setUpClass(cls) -> None:
        super(RawBinaryTranscoderTests, cls).setUpClass(
            transcoder=RawBinaryTranscoder())

    @classmethod
    def tearDownClass(cls) -> None:
        super(RawBinaryTranscoderTests, cls).tearDownClass()

    def setUp(self):
        super(RawBinaryTranscoderTests, self).setUp()
        try:
            self.cb.remove(self.KEY)
        except DocumentNotFoundException:
            pass

    def test_raw_bin_tc_json_upsert(self):
        with self.assertRaises(ValueFormatException):
            self.cb.upsert(self.KEY, self.CONTENT)

    def test_raw_bin_tc_json_insert(self):
        with self.assertRaises(ValueFormatException):
            self.cb.insert(self.KEY, self.CONTENT)

    def test_raw_bin_tc_json_replace(self):
        self.cb.upsert(self.KEY, bytes("some string content", "utf-8"))
        with self.assertRaises(ValueFormatException):
            self.cb.replace(self.KEY, self.CONTENT)

    def test_raw_bin_tc_str_upsert(self):
        with self.assertRaises(ValueFormatException):
            self.cb.upsert(self.KEY, "some string content")

    def test_raw_bin_tc_str_insert(self):
        with self.assertRaises(ValueFormatException):
            self.cb.insert(self.KEY, "some string content")

    def test_raw_bin_tc_str_replace(self):
        self.cb.upsert(self.KEY, bytes("some string content", "utf-8"))
        with self.assertRaises(ValueFormatException):
            self.cb.replace(self.KEY, "some new string content")

    def test_raw_bin_tc_binary_upsert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        self.cb.upsert(self.KEY, content)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    def test_raw_bin_tc_bytearray_upsert(self):
        content = bytearray(json.dumps("Here are some bytes"), "utf-8")
        self.cb.upsert(self.KEY, content)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    def test_raw_bin_tc_binary_insert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        self.cb.insert(self.KEY, content)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    def test_raw_bin_tc_binary_replace(self):
        self.cb.upsert(self.KEY, bytes("Lets to a str first", "utf-8"))
        new_content = bytes(json.dumps("Here are some newer bytes"), "utf-8")
        self.cb.replace(self.KEY, new_content)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(new_content, result)


@nottest
class FakeObject(object):
    PROP = "fake prop"
    PROP1 = 12345


class LegacyTranscoderTests(CollectionTestCase):
    CONTENT = {"some": "content", "num": 1,
               "list": [1, 2, 3], "nested": {"a": "b"}}
    KEY = "imakey"

    @classmethod
    def setUpClass(cls) -> None:
        super(LegacyTranscoderTests, cls).setUpClass(
            transcoder=LegacyTranscoder())

    @classmethod
    def tearDownClass(cls) -> None:
        super(LegacyTranscoderTests, cls).tearDownClass()

    def setUp(self):
        super(LegacyTranscoderTests, self).setUp()
        try:
            self.cb.remove(self.KEY)
        except DocumentNotFoundException:
            pass

    def test_legacy_tc_json_upsert(self):
        self.cb.upsert(self.KEY, self.CONTENT)
        resp = self.cb.get(self.KEY)
        result = resp.content_as[dict]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(self.CONTENT, result)

    def test_legacy_tc_json_insert(self):
        self.cb.insert(self.KEY, self.CONTENT)
        resp = self.cb.get(self.KEY)
        result = resp.content_as[dict]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(self.CONTENT, result)

    def test_legacy_tc_json_replace(self):
        self.cb.upsert(self.KEY, self.CONTENT)
        new_content = self.CONTENT
        new_content["some"] = "new content"
        self.cb.replace(self.KEY, new_content)
        resp = self.cb.get(self.KEY)
        result = resp.content_as[dict]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(new_content, result)

    def test_legacy_tc_pickle_upsert(self):
        fake_obj = FakeObject()
        self.cb.upsert(self.KEY, fake_obj)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, FakeObject)
        self.assertEqual(fake_obj.PROP, result.PROP)
        self.assertEqual(fake_obj.PROP1, result.PROP1)

    def test_legacy_tc_pickle_insert(self):
        fake_obj = FakeObject()
        self.cb.insert(self.KEY, fake_obj)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, FakeObject)
        self.assertEqual(fake_obj.PROP, result.PROP)
        self.assertEqual(fake_obj.PROP1, result.PROP1)

    def test_legacy_tc_pickle_replace(self):
        fake_obj = FakeObject()
        self.cb.upsert(self.KEY, self.CONTENT)
        self.cb.replace(self.KEY, fake_obj)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, FakeObject)
        self.assertEqual(fake_obj.PROP, result.PROP)
        self.assertEqual(fake_obj.PROP1, result.PROP1)

    def test_legacy_tc_string_upsert(self):
        content = "some string content"
        self.cb.upsert(self.KEY, content)
        resp = self.cb.get(self.KEY)
        result = resp.content_as[str]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertEqual(content, result)

    def test_legacy_tc_string_insert(self):
        content = "some string content"
        self.cb.insert(self.KEY, content)
        resp = self.cb.get(self.KEY)
        result = resp.content_as[str]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertEqual(content, result)

    def test_legacy_tc_string_replace(self):
        content = "some string content"
        self.cb.upsert(self.KEY, content)
        new_content = "new string content"
        self.cb.replace(self.KEY, new_content)
        resp = self.cb.get(self.KEY)
        result = resp.content_as[str]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertEqual(new_content, result)

    def test_legacy_tc_binary_upsert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        self.cb.upsert(self.KEY, content)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    def test_legacy_tc_bytearray_upsert(self):
        content = bytearray(json.dumps("Here are some bytes"), "utf-8")
        self.cb.upsert(self.KEY, content)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    def test_legacy_tc_binary_insert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        self.cb.insert(self.KEY, content)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    def test_legacy_tc_binary_replace(self):
        self.cb.upsert(self.KEY, bytes("Lets to a str first", "utf-8"))
        new_content = bytes(json.dumps("Here are some newer bytes"), "utf-8")
        self.cb.replace(self.KEY, new_content)
        resp = self.cb.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(new_content, result)


class KeyValueOpTranscoderTests(CollectionTestCase):
    CONTENT = {"some": "content", "num": 1,
               "list": [1, 2, 3], "nested": {"a": "b"}}
    KEY = "imakey"

    @classmethod
    def setUpClass(cls) -> None:
        super(KeyValueOpTranscoderTests, cls).setUpClass(
            transcoder=JSONTranscoder())

    @classmethod
    def tearDownClass(cls) -> None:
        super(KeyValueOpTranscoderTests, cls).tearDownClass()

    def setUp(self):
        super(KeyValueOpTranscoderTests, self).setUp()
        try:
            self.cb.remove(self.KEY)
        except DocumentNotFoundException:
            pass

    def test_upsert(self):
        content = bytes(json.dumps(self.CONTENT), "utf-8")
        # use RawBinaryTranscoder() so that get() fails as excpected
        # since get() w/o passing in transcoder uses the default JSONTranscoder()
        self.cb.upsert(self.KEY, content, UpsertOptions(
            transcoder=RawBinaryTranscoder()))
        with self.assertRaises(ValueFormatException):
            self.cb.get(self.KEY)

    def test_insert(self):
        # use RawStringTranscoder() so that get() fails as excpected
        # since get() w/o passing in transcoder uses the default JSONTranscoder()
        self.cb.upsert(self.KEY, "some string contente",
                       InsertOptions(transcoder=RawStringTranscoder()))
        with self.assertRaises(ValueFormatException):
            self.cb.get(self.KEY)

    def test_replace(self):
        content = bytes(json.dumps(self.CONTENT), "utf-8")
        # use RawBinaryTranscoder() so that get() fails as excpected
        # since get() w/o passing in transcoder uses the default JSONTranscoder()
        self.cb.upsert(self.KEY, self.CONTENT)
        self.cb.replace(self.KEY, content, ReplaceOptions(
            transcoder=RawBinaryTranscoder()))
        with self.assertRaises(ValueFormatException):
            self.cb.get(self.KEY)

    def test_get(self):
        content = bytes(json.dumps(self.CONTENT), "utf-8")
        tc = RawBinaryTranscoder()
        self.cb.upsert(self.KEY, content, UpsertOptions(transcoder=tc))
        with self.assertRaises(ValueFormatException):
            self.cb.get(self.KEY)
        resp = self.cb.get(self.KEY, GetOptions(transcoder=tc))
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    def test_get_and_touch(self):
        content = bytes(json.dumps(self.CONTENT), "utf-8")
        tc = RawBinaryTranscoder()
        self.cb.upsert(self.KEY, content, UpsertOptions(transcoder=tc))
        with self.assertRaises(ValueFormatException):
            self.cb.get_and_touch(self.KEY, timedelta(seconds=30))

        resp = self.cb.get_and_touch(self.KEY, timedelta(
            seconds=3), GetAndTouchOptions(transcoder=tc))
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)
        self.try_n_times_till_exception(
            10, 3, self.cb.get, self.KEY, GetOptions(transcoder=tc), DocumentNotFoundException)

    def test_get_and_lock(self):
        content = bytes(json.dumps(self.CONTENT), "utf-8")
        tc = RawBinaryTranscoder()
        self.cb.upsert(self.KEY, content, UpsertOptions(transcoder=tc))
        with self.assertRaises(ValueFormatException):
            self.cb.get_and_lock(self.KEY, timedelta(seconds=1))

        self.try_n_times(10, 1, self.cb.upsert, self.KEY,
                         content, UpsertOptions(transcoder=tc))
        resp = self.cb.get_and_lock(self.KEY, timedelta(
            seconds=3), GetAndLockOptions(transcoder=tc))
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)
        # upsert should definitely fail
        self.assertRaises(DocumentLockedException,
                          self.cb.upsert, self.KEY, self.CONTENT)
        # but succeed eventually
        self.try_n_times(10, 1, self.cb.upsert, self.KEY, self.CONTENT)

    def test_get_any_replica(self):
        num_replicas = self.bucket.configured_replica_count
        if num_replicas < 2:
            raise SkipTest('Need replicas to test')

        content = bytes(json.dumps(self.CONTENT), "utf-8")
        tc = RawBinaryTranscoder()
        self.cb.upsert(self.KEY, content, UpsertOptions(transcoder=tc))

        with self.assertRaises(ValueFormatException):
            self.cb.get_any_replica(self.KEY)

        resp = self.try_n_times(
            10, 3, self.coll.get_any_replica, self.KEY, GetAnyReplicaOptions(transcoder=tc))

        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    def test_get_all_replicas(self):
        num_replicas = self.bucket.configured_replica_count
        if num_replicas < 2:
            raise SkipTest('Need replicas to test')

        # TODO:  is this check needed?
        # kv_results = self.bucket.ping().endpoints.get(ServiceType.KeyValue, None)
        # if not kv_results or len(kv_results) < num_replicas+1:
        #     raise SkipTest('Not all replicas are online')

        content = bytes(json.dumps(self.CONTENT), "utf-8")
        tc = RawBinaryTranscoder()
        self.cb.upsert(self.KEY, content, UpsertOptions(transcoder=tc))

        with self.assertRaises(ValueFormatException):
            self.cb.get_all_replicas(self.KEY)

        resp = self.try_n_times(
            10, 3, self.coll.get_all_replicas, self.KEY, GetAllReplicasOptions(transcoder=tc))

        for r in resp:
            result = r.content
            self.assertIsNotNone(result)
            self.assertIsInstance(result, bytes)
            self.assertEqual(content, result)
