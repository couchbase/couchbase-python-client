import json
import platform
from datetime import timedelta
from unittest import SkipTest
from nose.tools import nottest
from functools import wraps

from acouchbase.cluster import (Cluster, get_event_loop,
                                close_event_loop)
from couchbase_tests.async_base import AsyncioTestCase
from couchbase.exceptions import DocumentNotFoundException, ValueFormatException, DocumentLockedException
from couchbase.transcoder import (JSONTranscoder, RawJSONTranscoder,
                                  RawStringTranscoder, RawBinaryTranscoder, LegacyTranscoder)

from couchbase.collection import (GetOptions, UpsertOptions, InsertOptions, ReplaceOptions,
                                  GetAndTouchOptions, GetAndLockOptions, GetAnyReplicaOptions, GetAllReplicasOptions)


@nottest
def async_test(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        return self.loop.run_until_complete(func(self, *args, **kwargs))

    return wrapper


class AcouchbaseDefaultTranscoderTestSuite(object):
    CONTENT = {"some": "content", "num": 1,
               "list": [1, 2, 3], "nested": {"a": "b"}}
    KEY = "imakey"

    async def initialize(self):
        try:
            await self.collection.remove(self.KEY)
        except DocumentNotFoundException:
            pass

    @async_test
    async def test_default_tc_json_upsert(self):
        await self.collection.upsert(self.KEY, self.CONTENT)
        resp = await self.collection.get(self.KEY)
        result = resp.content_as[dict]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(self.CONTENT, result)

    @async_test
    async def test_default_tc_json_insert(self):
        await self.collection.insert(self.KEY, self.CONTENT)
        resp = await self.collection.get(self.KEY)
        result = resp.content_as[dict]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(self.CONTENT, result)

    @async_test
    async def test_default_tc_json_replace(self):
        await self.collection.upsert(self.KEY, self.CONTENT)
        new_content = self.CONTENT
        new_content["some"] = "new content"
        await self.collection.replace(self.KEY, new_content)
        resp = await self.collection.get(self.KEY)
        result = resp.content_as[dict]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(new_content, result)

    @async_test
    async def test_default_tc_string_upsert(self):
        content = "some string content"
        await self.collection.upsert(self.KEY, content)
        resp = await self.collection.get(self.KEY)
        result = resp.content_as[str]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertEqual(content, result)

    @async_test
    async def test_default_tc_string_insert(self):
        content = "some string content"
        await self.collection.insert(self.KEY, content)
        resp = await self.collection.get(self.KEY)
        result = resp.content_as[str]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertEqual(content, result)

    @async_test
    async def test_default_tc_string_replace(self):
        content = "some string content"
        await self.collection.upsert(self.KEY, content)
        new_content = "new string content"
        await self.collection.replace(self.KEY, new_content)
        resp = await self.collection.get(self.KEY)
        result = resp.content_as[str]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertEqual(new_content, result)

    @async_test
    async def test_default_tc_binary_upsert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        with self.assertRaises(ValueFormatException):
            await self.collection.upsert(self.KEY, content)

    @async_test
    async def test_default_tc_bytearray_upsert(self):
        content = bytearray(json.dumps("Here are some bytes"), "utf-8")
        with self.assertRaises(ValueFormatException):
            await self.collection.upsert(self.KEY, content)

    @async_test
    async def test_default_tc_binary_insert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        with self.assertRaises(ValueFormatException):
            await self.collection.insert(self.KEY, content)

    @async_test
    async def test_default_tc_binary_replace(self):
        content = "Lets to a str first"
        await self.collection.upsert(self.KEY, content)
        new_content = bytes(json.dumps("Here are some newer bytes"), "utf-8")
        with self.assertRaises(ValueFormatException):
            await self.collection.replace(self.KEY, new_content)


class AcouchbaseDefaultTranscoderTests(
        AsyncioTestCase, AcouchbaseDefaultTranscoderTestSuite):
    @classmethod
    def setUpClass(cls) -> None:
        super(AcouchbaseDefaultTranscoderTests, cls).setUpClass(
            get_event_loop(), cluster_class=Cluster)

    @classmethod
    def tearDownClass(cls) -> None:
        super(AcouchbaseDefaultTranscoderTests, cls).tearDownClass()
        close_event_loop()

    def setUp(self):
        super(AcouchbaseDefaultTranscoderTests, self).setUp()

        self.loop.run_until_complete(self.initialize())


class AcouchbaseDefaultJsonTranscoderTests(AsyncioTestCase, AcouchbaseDefaultTranscoderTestSuite):

    @classmethod
    def setUpClass(cls) -> None:
        super(AcouchbaseDefaultJsonTranscoderTests, cls).setUpClass(
            get_event_loop(), cluster_class=Cluster, transcoder=JSONTranscoder())

    @classmethod
    def tearDownClass(cls) -> None:
        super(AcouchbaseDefaultJsonTranscoderTests, cls).tearDownClass()

    def setUp(self):
        super(AcouchbaseDefaultJsonTranscoderTests, self).setUp()

        self.loop.run_until_complete(self.initialize())


class AcouchbaseRawJsonTranscoderTests(AsyncioTestCase):

    CONTENT = {"some": "content", "num": 1,
               "list": [1, 2, 3], "nested": {"a": "b"}}
    KEY = "imakey"

    @classmethod
    def setUpClass(cls) -> None:
        super(AcouchbaseRawJsonTranscoderTests, cls).setUpClass(
            get_event_loop(), cluster_class=Cluster, transcoder=RawJSONTranscoder())

    @classmethod
    def tearDownClass(cls) -> None:
        super(AcouchbaseRawJsonTranscoderTests, cls).tearDownClass()

    async def initialize(self):
        try:
            await self.collection.remove(self.KEY)
        except DocumentNotFoundException:
            pass

    def setUp(self):
        super(AcouchbaseRawJsonTranscoderTests, self).setUp()

        self.loop.run_until_complete(self.initialize())

    @async_test
    async def test_raw_json_tc_json_upsert(self):
        with self.assertRaises(ValueFormatException):
            await self.collection.upsert(self.KEY, self.CONTENT)

    @async_test
    async def test_raw_json_tc_json_insert(self):
        with self.assertRaises(ValueFormatException):
            await self.collection.insert(self.KEY, self.CONTENT)

    @async_test
    async def test_raw_json_tc_json_replace(self):
        await self.collection.upsert(self.KEY, "some string content")
        with self.assertRaises(ValueFormatException):
            await self.collection.replace(self.KEY, self.CONTENT)

    @async_test
    async def test_raw_json_tc_string_upsert(self):
        content = "some string content"
        await self.collection.upsert(self.KEY, content)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result.decode("utf-8"))

    @async_test
    async def test_raw_json_tc_string_insert(self):
        content = "some string content"
        await self.collection.insert(self.KEY, content)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result.decode("utf-8"))

    @async_test
    async def test_raw_json_tc_string_replace(self):
        content = "some string content"
        await self.collection.upsert(self.KEY, content)
        new_content = "new string content"
        await self.collection.replace(self.KEY, new_content)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(new_content, result.decode("utf-8"))

    @async_test
    async def test_raw_json_tc_binary_upsert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        await self.collection.upsert(self.KEY, content)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    @async_test
    async def test_raw_json_tc_bytearray_upsert(self):
        content = bytearray(json.dumps("Here are some bytes"), "utf-8")
        await self.collection.upsert(self.KEY, content)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    @async_test
    async def test_raw_json_tc_binary_insert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        await self.collection.insert(self.KEY, content)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    @async_test
    async def test_raw_json_tc_binary_replace(self):
        content = "Lets to a str first"
        await self.collection.upsert(self.KEY, content)
        new_content = bytes(json.dumps("Here are some newer bytes"), "utf-8")
        await self.collection.replace(self.KEY, new_content)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(new_content, result)

    @async_test
    async def test_pass_through(self):
        content = json.dumps(self.CONTENT)
        await self.collection.upsert(self.KEY, content)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertNotEqual(self.CONTENT, result)
        # json.loads expects a string in Python 3.5
        if float(platform.python_version()[:3]) <= 3.5:
            result = result.decode("utf-8")
        decoded = json.loads(result)
        self.assertEqual(self.CONTENT, decoded)


class AcouchbaseRawStringTranscoderTests(AsyncioTestCase):
    CONTENT = {"some": "content", "num": 1,
               "list": [1, 2, 3], "nested": {"a": "b"}}
    KEY = "imakey"

    @classmethod
    def setUpClass(cls) -> None:
        super(AcouchbaseRawStringTranscoderTests, cls).setUpClass(
            get_event_loop(), cluster_class=Cluster, transcoder=RawStringTranscoder())

    @classmethod
    def tearDownClass(cls) -> None:
        super(AcouchbaseRawStringTranscoderTests, cls).tearDownClass()

    async def initialize(self):
        try:
            await self.collection.remove(self.KEY)
        except DocumentNotFoundException:
            pass

    def setUp(self):
        super(AcouchbaseRawStringTranscoderTests, self).setUp()

        self.loop.run_until_complete(self.initialize())

    @async_test
    async def test_raw_str_tc_json_upsert(self):
        with self.assertRaises(ValueFormatException):
            await self.collection.upsert(self.KEY, self.CONTENT)

    @async_test
    async def test_raw_str_tc_json_insert(self):
        with self.assertRaises(ValueFormatException):
            await self.collection.insert(self.KEY, self.CONTENT)

    @async_test
    async def test_raw_str_tc_json_replace(self):
        await self.collection.upsert(self.KEY, "some string content")
        with self.assertRaises(ValueFormatException):
            await self.collection.replace(self.KEY, self.CONTENT)

    @async_test
    async def test_raw_json_tc_string_upsert(self):
        content = "some string content"
        await self.collection.upsert(self.KEY, content)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertEqual(content, result)

    @async_test
    async def test_raw_json_tc_string_insert(self):
        content = "some string content"
        await self.collection.insert(self.KEY, content)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertEqual(content, result)

    @async_test
    async def test_raw_json_tc_string_replace(self):
        content = "some string content"
        await self.collection.upsert(self.KEY, content)
        new_content = "new string content"
        await self.collection.replace(self.KEY, new_content)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertEqual(new_content, result)

    @async_test
    async def test_raw_str_tc_binary_upsert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        with self.assertRaises(ValueFormatException):
            await self.collection.upsert(self.KEY, content)

    @async_test
    async def test_raw_str_tc_bytearray_upsert(self):
        content = bytearray(json.dumps("Here are some bytes"), "utf-8")
        with self.assertRaises(ValueFormatException):
            await self.collection.upsert(self.KEY, content)

    @async_test
    async def test_raw_str_tc_binary_insert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        with self.assertRaises(ValueFormatException):
            await self.collection.insert(self.KEY, content)

    @async_test
    async def test_raw_str_tc_binary_replace(self):
        await self.collection.upsert(self.KEY, "some string content")
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        with self.assertRaises(ValueFormatException):
            await self.collection.replace(self.KEY, content)


class AcouchbaseRawBinaryTranscoderTests(AsyncioTestCase):
    CONTENT = {"some": "content", "num": 1,
               "list": [1, 2, 3], "nested": {"a": "b"}}
    KEY = "imakey"

    @classmethod
    def setUpClass(cls) -> None:
        super(AcouchbaseRawBinaryTranscoderTests, cls).setUpClass(
            get_event_loop(), cluster_class=Cluster, transcoder=RawBinaryTranscoder())

    @classmethod
    def tearDownClass(cls) -> None:
        super(AcouchbaseRawBinaryTranscoderTests, cls).tearDownClass()

    async def initialize(self):
        try:
            await self.collection.remove(self.KEY)
        except DocumentNotFoundException:
            pass

    def setUp(self):
        super(AcouchbaseRawBinaryTranscoderTests, self).setUp()

        self.loop.run_until_complete(self.initialize())

    @async_test
    async def test_raw_bin_tc_json_upsert(self):
        with self.assertRaises(ValueFormatException):
            await self.collection.upsert(self.KEY, self.CONTENT)

    @async_test
    async def test_raw_bin_tc_json_insert(self):
        with self.assertRaises(ValueFormatException):
            await self.collection.insert(self.KEY, self.CONTENT)

    @async_test
    async def test_raw_bin_tc_json_replace(self):
        await self.collection.upsert(self.KEY, bytes("some string content", "utf-8"))
        with self.assertRaises(ValueFormatException):
            await self.collection.replace(self.KEY, self.CONTENT)

    @async_test
    async def test_raw_bin_tc_str_upsert(self):
        with self.assertRaises(ValueFormatException):
            await self.collection.upsert(self.KEY, "some string content")

    @async_test
    async def test_raw_bin_tc_str_insert(self):
        with self.assertRaises(ValueFormatException):
            await self.collection.insert(self.KEY, "some string content")

    @async_test
    async def test_raw_bin_tc_str_replace(self):
        await self.collection.upsert(self.KEY, bytes("some string content", "utf-8"))
        with self.assertRaises(ValueFormatException):
            await self.collection.replace(self.KEY, "some new string content")

    @async_test
    async def test_raw_bin_tc_binary_upsert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        await self.collection.upsert(self.KEY, content)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    @async_test
    async def test_raw_bin_tc_bytearray_upsert(self):
        content = bytearray(json.dumps("Here are some bytes"), "utf-8")
        await self.collection.upsert(self.KEY, content)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    @async_test
    async def test_raw_bin_tc_binary_insert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        await self.collection.insert(self.KEY, content)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    @async_test
    async def test_raw_bin_tc_binary_replace(self):
        await self.collection.upsert(self.KEY, bytes("Lets to a str first", "utf-8"))
        new_content = bytes(json.dumps("Here are some newer bytes"), "utf-8")
        await self.collection.replace(self.KEY, new_content)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(new_content, result)


@nottest
class FakeObject(object):
    PROP = "fake prop"
    PROP1 = 12345


class AcouchbaseLegacyTranscoderTests(AsyncioTestCase):
    CONTENT = {"some": "content", "num": 1,
               "list": [1, 2, 3], "nested": {"a": "b"}}
    KEY = "imakey"

    @classmethod
    def setUpClass(cls) -> None:
        super(AcouchbaseLegacyTranscoderTests, cls).setUpClass(
            get_event_loop(), cluster_class=Cluster, transcoder=LegacyTranscoder())

    @classmethod
    def tearDownClass(cls) -> None:
        super(AcouchbaseLegacyTranscoderTests, cls).tearDownClass()

    async def initialize(self):
        try:
            await self.collection.remove(self.KEY)
        except DocumentNotFoundException:
            pass

    def setUp(self):
        super(AcouchbaseLegacyTranscoderTests, self).setUp()

        self.loop.run_until_complete(self.initialize())

    @async_test
    async def test_legacy_tc_json_upsert(self):
        await self.collection.upsert(self.KEY, self.CONTENT)
        resp = await self.collection.get(self.KEY)
        result = resp.content_as[dict]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(self.CONTENT, result)

    @async_test
    async def test_legacy_tc_json_insert(self):
        await self.collection.insert(self.KEY, self.CONTENT)
        resp = await self.collection.get(self.KEY)
        result = resp.content_as[dict]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(self.CONTENT, result)

    @async_test
    async def test_legacy_tc_json_replace(self):
        await self.collection.upsert(self.KEY, self.CONTENT)
        new_content = self.CONTENT
        new_content["some"] = "new content"
        await self.collection.replace(self.KEY, new_content)
        resp = await self.collection.get(self.KEY)
        result = resp.content_as[dict]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(new_content, result)

    @async_test
    async def test_legacy_tc_pickle_upsert(self):
        fake_obj = FakeObject()
        await self.collection.upsert(self.KEY, fake_obj)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, FakeObject)
        self.assertEqual(fake_obj.PROP, result.PROP)
        self.assertEqual(fake_obj.PROP1, result.PROP1)

    @async_test
    async def test_legacy_tc_pickle_insert(self):
        fake_obj = FakeObject()
        await self.collection.insert(self.KEY, fake_obj)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, FakeObject)
        self.assertEqual(fake_obj.PROP, result.PROP)
        self.assertEqual(fake_obj.PROP1, result.PROP1)

    @async_test
    async def test_legacy_tc_pickle_replace(self):
        fake_obj = FakeObject()
        await self.collection.upsert(self.KEY, self.CONTENT)
        await self.collection.replace(self.KEY, fake_obj)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, FakeObject)
        self.assertEqual(fake_obj.PROP, result.PROP)
        self.assertEqual(fake_obj.PROP1, result.PROP1)

    @async_test
    async def test_legacy_tc_string_upsert(self):
        content = "some string content"
        await self.collection.upsert(self.KEY, content)
        resp = await self.collection.get(self.KEY)
        result = resp.content_as[str]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertEqual(content, result)

    @async_test
    async def test_legacy_tc_string_insert(self):
        content = "some string content"
        await self.collection.insert(self.KEY, content)
        resp = await self.collection.get(self.KEY)
        result = resp.content_as[str]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertEqual(content, result)

    @async_test
    async def test_legacy_tc_string_replace(self):
        content = "some string content"
        await self.collection.upsert(self.KEY, content)
        new_content = "new string content"
        await self.collection.replace(self.KEY, new_content)
        resp = await self.collection.get(self.KEY)
        result = resp.content_as[str]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertEqual(new_content, result)

    @async_test
    async def test_legacy_tc_binary_upsert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        await self.collection.upsert(self.KEY, content)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    @async_test
    async def test_legacy_tc_bytearray_upsert(self):
        content = bytearray(json.dumps("Here are some bytes"), "utf-8")
        await self.collection.upsert(self.KEY, content)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    @async_test
    async def test_legacy_tc_binary_insert(self):
        content = bytes(json.dumps("Here are some bytes"), "utf-8")
        await self.collection.insert(self.KEY, content)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    @async_test
    async def test_legacy_tc_binary_replace(self):
        await self.collection.upsert(self.KEY, bytes("Lets to a str first", "utf-8"))
        new_content = bytes(json.dumps("Here are some newer bytes"), "utf-8")
        await self.collection.replace(self.KEY, new_content)
        resp = await self.collection.get(self.KEY)
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(new_content, result)


class AcouchbaseKeyValueOpTranscoderTests(AsyncioTestCase):
    CONTENT = {"some": "content", "num": 1,
               "list": [1, 2, 3], "nested": {"a": "b"}}
    KEY = "imakey"

    @classmethod
    def setUpClass(cls) -> None:
        super(AcouchbaseKeyValueOpTranscoderTests, cls).setUpClass(
            get_event_loop(), cluster_class=Cluster, transcoder=JSONTranscoder())

    @classmethod
    def tearDownClass(cls) -> None:
        super(AcouchbaseKeyValueOpTranscoderTests, cls).tearDownClass()

    async def initialize(self):
        try:
            await self.collection.remove(self.KEY)
        except DocumentNotFoundException:
            pass

    def setUp(self):
        super(AcouchbaseKeyValueOpTranscoderTests, self).setUp()

        self.loop.run_until_complete(self.initialize())

    @async_test
    async def test_upsert(self):
        content = bytes(json.dumps(self.CONTENT), "utf-8")
        # use RawBinaryTranscoder() so that get() fails as excpected
        # since get() w/o passing in transcoder uses the default JSONTranscoder()
        await self.collection.upsert(self.KEY, content, UpsertOptions(transcoder=RawBinaryTranscoder()))
        with self.assertRaises(ValueFormatException):
            await self.collection.get(self.KEY)

    @async_test
    async def test_insert(self):
        # use RawStringTranscoder() so that get() fails as excpected
        # since get() w/o passing in transcoder uses the default JSONTranscoder()
        await self.collection.upsert(self.KEY, "some string content", InsertOptions(transcoder=RawStringTranscoder()))
        with self.assertRaises(ValueFormatException):
            await self.collection.get(self.KEY)

    @async_test
    async def test_replace(self):
        content = bytes(json.dumps(self.CONTENT), "utf-8")
        # use RawBinaryTranscoder() so that get() fails as excpected
        # since get() w/o passing in transcoder uses the default JSONTranscoder()
        await self.collection.upsert(self.KEY, self.CONTENT)
        await self.collection.replace(self.KEY, content, ReplaceOptions(transcoder=RawBinaryTranscoder()))
        with self.assertRaises(ValueFormatException):
            await self.collection.get(self.KEY)

    @async_test
    async def test_get(self):
        content = bytes(json.dumps(self.CONTENT), "utf-8")
        tc = RawBinaryTranscoder()
        await self.collection.upsert(self.KEY, content, UpsertOptions(transcoder=tc))
        with self.assertRaises(ValueFormatException):
            await self.collection.get(self.KEY)
        resp = await self.collection.get(self.KEY, GetOptions(transcoder=tc))
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)

    @async_test
    async def test_get_and_touch(self):
        content = bytes(json.dumps(self.CONTENT), "utf-8")
        tc = RawBinaryTranscoder()
        await self.collection.upsert(self.KEY, content, UpsertOptions(transcoder=tc))
        with self.assertRaises(ValueFormatException):
            await self.collection.get_and_touch(self.KEY, timedelta(seconds=30))

        resp = await self.collection.get_and_touch(self.KEY, timedelta(seconds=3), GetAndTouchOptions(transcoder=tc))
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)
        await self.try_n_times_till_exception_async(
            10, 3, self.collection.get, self.KEY, GetOptions(transcoder=tc), DocumentNotFoundException)

    @async_test
    async def test_get_and_lock(self):
        content = bytes(json.dumps(self.CONTENT), "utf-8")
        tc = RawBinaryTranscoder()
        await self.collection.upsert(self.KEY, content, UpsertOptions(transcoder=tc))
        with self.assertRaises(ValueFormatException):
            await self.collection.get_and_lock(self.KEY, timedelta(seconds=1))

        await self.try_n_times_async(10, 1, self.collection.upsert, self.KEY, content, UpsertOptions(transcoder=tc))
        resp = await self.collection.get_and_lock(self.KEY, timedelta(seconds=3), GetAndLockOptions(transcoder=tc))
        result = resp.content
        self.assertIsNotNone(result)
        self.assertIsInstance(result, bytes)
        self.assertEqual(content, result)
        # upsert should definitely fail
        with self.assertRaises(DocumentLockedException):
            await self.collection.upsert(self.KEY, self.CONTENT)
        # but succeed eventually
        await self.try_n_times_async(10, 1, self.collection.upsert, self.KEY, self.CONTENT)

    # TODO: replica ops are not available w/ async
    # @async_test
    # async def test_get_any_replica(self):
    #     num_replicas = self.bucket.configured_replica_count
    #     if num_replicas < 2:
    #         raise SkipTest('Need replicas to test')

    #     content = bytes(json.dumps(self.CONTENT), "utf-8")
    #     tc = RawBinaryTranscoder()
    #     await self.collection.upsert(self.KEY, content, UpsertOptions(transcoder=tc))

    #     with self.assertRaises(ValueFormatException):
    #         await self.collection.get_any_replica(self.KEY)

    #     resp = await self.try_n_times_async(
    #         10, 3, self.collection.get_any_replica, self.KEY, GetAnyReplicaOptions(transcoder=tc))

    #     result = resp.content
    #     self.assertIsNotNone(result)
    #     self.assertIsInstance(result, bytes)
    #     self.assertEqual(content, result)

    # @async_test
    # async def test_get_all_replicas(self):
    #     num_replicas = self.bucket.configured_replica_count
    #     if num_replicas < 2:
    #         raise SkipTest('Need replicas to test')

    #     # TODO:  is this check needed?
    #     # kv_results = self.bucket.ping().endpoints.get(ServiceType.KeyValue, None)
    #     # if not kv_results or len(kv_results) < num_replicas+1:
    #     #     raise SkipTest('Not all replicas are online')

    #     content = bytes(json.dumps(self.CONTENT), "utf-8")
    #     tc = RawBinaryTranscoder()
    #     await self.collection.upsert(self.KEY, content, UpsertOptions(transcoder=tc))

    #     with self.assertRaises(ValueFormatException):
    #         await self.collection.get_all_replicas(self.KEY)

    #     resp = await self.try_n_times_async(
    #         10, 3, self.collection.get_all_replicas, self.KEY, GetAllReplicasOptions(transcoder=tc))

    #     for r in resp:
    #         result = r.content
    #         self.assertIsNotNone(result)
    #         self.assertIsInstance(result, bytes)
    #         self.assertEqual(content, result)
