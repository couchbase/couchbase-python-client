# -*- coding:utf-8 -*-
#
# Copyright 2020, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import time
from datetime import timedelta
from datetime import datetime
from unittest import SkipTest

from couchbase.durability import ClientDurability
from couchbase_tests.base import CollectionTestCase
from couchbase.collection import GetOptions, LookupInOptions, MutateInOptions
from couchbase.exceptions import (
    InvalidArgumentException,
    PathExistsException,
    PathNotFoundException,
    DurabilityImpossibleException,
    SubdocCantInsertValueException,
    DocumentNotFoundException,
    SubdocPathMismatchException,
)
import couchbase.subdocument as SD


class SubdocTests(CollectionTestCase):
    CONTENT = {"a": "aaa", "b": "bbb"}
    KEY = "imakey"

    def setUp(self):
        super().setUp()
        cas = self.coll.upsert(self.KEY, self.CONTENT).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)

    def _cas_matches(self, key, cas):
        result = self.coll.get(key)
        if self.coll.get(key).cas == cas:
            return result
        raise Exception("nope")

    def test_lookup_in_simple_get(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": [1, 2, 3, 4]}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        result = self.coll.lookup_in(self.KEY, (SD.get("b"),))
        self.assertEqual(result.cas, cas)
        self.assertEqual([1, 2, 3, 4], result.content_as[list](0))

    def test_lookup_in_simple_exists(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": [1, 2, 3, 4]}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        result = self.coll.lookup_in(self.KEY, (SD.exists("b"),))
        self.assertEqual(result.cas, cas)
        self.assertTrue(result.exists(0))
        # no content; only valid path, returns None so is False
        self.assertFalse(result.content_as[bool](0))

    def test_lookup_in_simple_exists_bad_path(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": [1, 2, 3, 4]}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        result = self.coll.lookup_in(self.KEY, (SD.exists("qzzxy"),))
        self.assertEqual(result.cas, cas)
        self.assertFalse(result.exists(0))
        self.assertRaises(PathNotFoundException, result.content_as[bool], 0)

    def test_lookup_in_one_path_not_found(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": [1, 2, 3, 4]}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        result = self.coll.lookup_in(
            self.KEY,
            (
                SD.exists("a"),
                SD.exists("qzzxy"),
            ),
        )
        self.assertTrue(result.exists(0))
        self.assertFalse(result.exists(1))

    def test_lookup_in_simple_get_longer_path(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": {"c": {"d": "yo!"}}}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        result = self.coll.lookup_in(self.KEY, (SD.get("b.c.d"),))
        self.assertEqual(result.cas, cas)
        self.assertEqual("yo!", result.content_as[str](0))

    def test_lookup_in_multiple_specs(self):
        if self.is_mock:
            raise SkipTest(
                "mock doesn't support getting xattrs (like $document.expiry)"
            )
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": {"c": {"d": "yo!"}}}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        result = self.coll.lookup_in(
            self.KEY, (SD.with_expiry(), SD.get("a"), SD.exists("b"), SD.get("b.c"))
        )
        self.assertTrue(result.success)
        self.assertIsNone(result.expiry)
        self.assertEqual("aaa", result.content_as[str](1))
        self.assertTrue(result.exists(2))
        self.assertDictEqual({"d": "yo!"}, result.content_as[dict](3))

    def test_mutate_in_simple(self):
        cas = self.coll.mutate_in(
            self.KEY,
            (
                SD.upsert("c", "ccc"),
                SD.replace("b", "XXX"),
            ),
        ).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        result = self.coll.get(self.KEY).content_as[dict]
        self.assertDictEqual({"a": "aaa", "b": "XXX", "c": "ccc"}, result)

    def test_mutate_in_expiry(self):
        if self.is_mock:
            raise SkipTest(
                "mock doesn't support getting xattrs (like $document.expiry)"
            )

        cas = self.coll.mutate_in(
            self.KEY,
            (
                SD.upsert("c", "ccc"),
                SD.replace("b", "XXX"),
            ),
            MutateInOptions(expiry=timedelta(seconds=1000)),
        ).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        result = self.coll.get(self.KEY, GetOptions(with_expiry=True))
        expires_in = (result.expiryTime - datetime.now()).total_seconds()
        self.assertTrue(0 < expires_in < 1001)

    def test_mutate_in_preserve_expiry_not_used(self):
        if self.is_mock:
            raise SkipTest(
                "mock doesn't support getting xattrs (like $document.expiry)"
            )
        if int(self.get_cluster_version().split(".")[0]) < 7:
            raise SkipTest("Preserve expiry only in CBS 7.0+")

        result = self.coll.mutate_in(
            self.KEY,
            (
                SD.upsert("c", "ccc"),
                SD.replace("b", "XXX"),
            ),
            MutateInOptions(expiry=timedelta(seconds=5)),
        )
        self.assertTrue(result.success)
        expiry1 = self.coll.get(self.KEY, GetOptions(with_expiry=True)).expiryTime

        result = self.coll.mutate_in(self.KEY, (SD.upsert("d", "ddd"),))
        self.assertTrue(result.success)
        expiry2 = self.cb.get(self.KEY, GetOptions(with_expiry=True)).expiryTime
        self.assertIsNotNone(expiry1)
        self.assertIsInstance(expiry1, datetime)
        self.assertIsNone(expiry2)
        self.assertNotEqual(expiry1, expiry2)
        # if expiry was set, should be expired by now
        time.sleep(6)
        result = self.cb.get(self.KEY)
        self.assertIsNotNone(result)

    def test_mutate_in_preserve_expiry(self):
        if self.is_mock:
            raise SkipTest(
                "mock doesn't support getting xattrs (like $document.expiry)"
            )

        if int(self.get_cluster_version().split(".")[0]) < 7:
            raise SkipTest("Preserve expiry only in CBS 7.0+")

        result = self.coll.mutate_in(
            self.KEY,
            (
                SD.upsert("c", "ccc"),
                SD.replace("b", "XXX"),
            ),
            MutateInOptions(expiry=timedelta(seconds=5)),
        )
        self.assertTrue(result.success)
        expiry1 = self.coll.get(self.KEY, GetOptions(with_expiry=True)).expiryTime

        result = self.coll.mutate_in(
            self.KEY, (SD.upsert("d", "ddd"),), MutateInOptions(preserve_expiry=True)
        )
        self.assertTrue(result.success)
        expiry2 = self.coll.get(self.KEY, GetOptions(with_expiry=True)).expiryTime
        self.assertIsNotNone(expiry1)
        self.assertIsInstance(expiry1, datetime)
        self.assertIsNotNone(expiry2)
        self.assertIsInstance(expiry2, datetime)
        self.assertEqual(expiry1, expiry2)
        # if expiry was preserved, should be expired by now
        time.sleep(6)
        with self.assertRaises(DocumentNotFoundException):
            self.coll.get(self.KEY)

    def test_mutate_in_preserve_expiry_fails(self):
        if self.is_mock:
            raise SkipTest(
                "mock doesn't support getting xattrs (like $document.expiry)"
            )

        if int(self.get_cluster_version().split(".")[0]) < 7:
            raise SkipTest("Preserve expiry only in CBS 7.0+")

        with self.assertRaises(InvalidArgumentException):
            self.coll.mutate_in(
                self.KEY,
                (SD.insert("c", "ccc"),),
                MutateInOptions(preserve_expiry=True),
            )

        with self.assertRaises(InvalidArgumentException):
            self.coll.mutate_in(
                self.KEY,
                (SD.replace("c", "ccc"),),
                MutateInOptions(expiry=timedelta(seconds=5), preserve_expiry=True),
            )

    def test_mutate_in_durability(self):
        if self.is_mock:
            raise SkipTest(
                "mock doesn't support getting xattrs (like $document.expiry)"
            )
        self.assertRaises(
            DurabilityImpossibleException,
            self.coll.mutate_in,
            self.KEY,
            (
                SD.upsert("c", "ccc"),
                SD.replace("b", "XXX"),
            ),
            MutateInOptions(durability=ClientDurability(replicate_to=5)),
        )

    # refactor!  Also, this seems like it should timeout.  I suspect a bug here.  I don't really
    # believe there is any way this could not timeout on the first lookup_in
    def test_lookup_in_timeout(self):
        self.coll.upsert("id", {"someArray": ["wibble", "gronk"]})
        # wait till it is there
        self.try_n_times(10, 1, self.coll.get, "id")

        # ok, it is there...
        self.coll.get(
            "id", GetOptions(project=["someArray"], timeout=timedelta(seconds=1.0))
        )
        self.assertRaisesRegex(
            InvalidArgumentException,
            "Expected timedelta",
            self.coll.get,
            "id",
            GetOptions(project=["someArray"], timeout=456),
        )
        sdresult_2 = self.coll.lookup_in(
            "id",
            (SD.get("someArray"),),
            LookupInOptions(timeout=timedelta(microseconds=1)),
        )
        self.assertEqual(["wibble", "gronk"], sdresult_2.content_as[list](0))
        sdresult_2 = self.coll.lookup_in(
            "id",
            (SD.get("someArray"),),
            LookupInOptions(timeout=timedelta(seconds=1)),
            timeout=timedelta(microseconds=1),
        )
        self.assertEqual(["wibble", "gronk"], sdresult_2.content_as[list](0))

    def test_array_append(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": [1, 2, 3, 4]}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        self.coll.mutate_in(self.KEY, (SD.array_append("b", 5),))
        result = self.coll.get(self.KEY).content_as[dict]
        self.assertIsInstance(result["b"], list)
        self.assertEqual(len(result["b"]), 5)
        self.assertEqual(5, result["b"][4])

    def test_array_prepend(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": [1, 2, 3, 4]}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        self.coll.mutate_in(self.KEY, (SD.array_prepend("b", 0),))
        result = self.coll.get(self.KEY).content_as[dict]
        self.assertIsInstance(result["b"], list)
        self.assertEqual(len(result["b"]), 5)
        self.assertEqual(0, result["b"][0])

    def test_array_insert(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": [0, 1, 3, 4]}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        self.coll.mutate_in(self.KEY, (SD.array_insert("b.[2]", 2),))
        result = self.coll.get(self.KEY).content_as[dict]
        self.assertIsInstance(result["b"], list)
        self.assertEqual(len(result["b"]), 5)
        self.assertEqual(2, result["b"][2])

    def test_array_as_document(self):
        cas = self.coll.upsert(self.KEY, []).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        self.coll.mutate_in(
            self.KEY,
            (
                SD.array_append("", 2),
                SD.array_prepend("", 0),
                SD.array_insert("[1]", 1),
            ),
        )
        result = self.coll.get(self.KEY).content_as[list]
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 3)
        self.assertEqual(0, result[0])
        self.assertEqual(1, result[1])
        self.assertEqual(2, result[2])

    def test_array_append_multi_insert(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": [1, 2, 3, 4, 5, 6, 7]}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        self.coll.mutate_in(self.KEY, (SD.array_append("b", 8, 9, 10),))
        result = self.coll.get(self.KEY).content_as[dict]
        self.assertIsInstance(result["b"], list)
        insert_res = result["b"][7:]
        self.assertEqual(len(insert_res), 3)
        self.assertEqual(insert_res, [8, 9, 10])

    def test_array_prepend_multi_insert(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": [4, 5, 6, 7, 8, 9, 10]}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        self.coll.mutate_in(self.KEY, (SD.array_prepend("b", 1, 2, 3),))
        result = self.coll.get(self.KEY).content_as[dict]
        self.assertIsInstance(result["b"], list)
        insert_res = result["b"][:3]
        self.assertEqual(len(insert_res), 3)
        self.assertEqual(insert_res, [1, 2, 3])

    def test_array_insert_multi_insert(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": [1, 2, 3, 4, 8, 9, 10]}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        self.coll.mutate_in(self.KEY, (SD.array_insert("b.[4]", 5, 6, 7),))
        result = self.coll.get(self.KEY).content_as[dict]
        self.assertIsInstance(result["b"], list)
        insert_res = result["b"][4:7]
        self.assertEqual(len(insert_res), 3)
        self.assertEqual(insert_res, [5, 6, 7])

    def test_array_add_unique(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": [0, 1, 2, 3]}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        self.coll.mutate_in(self.KEY, (SD.array_addunique("b", 4),))
        result = self.coll.get(self.KEY).content_as[dict]
        self.assertIsInstance(result["b"], list)
        self.assertEqual(len(result["b"]), 5)
        self.assertIn(4, result["b"])

    def test_array_add_unique_fail(self):
        cas = self.coll.upsert(
            self.KEY,
            {
                "a": "aaa",
                "b": [0, 1, 2, 3],
                "c": [1.25, 1.5, {"nested": ["str", "array"]}],
            },
        ).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        with self.assertRaises(PathExistsException):
            self.coll.mutate_in(self.KEY, (SD.array_addunique("b", 3),))

        with self.assertRaises(SubdocCantInsertValueException):
            self.coll.mutate_in(self.KEY, (SD.array_addunique("b", [4, 5, 6]),))

        # apparently adding floats is okay?
        # with self.assertRaises(SubdocCantInsertValueException):
        self.coll.mutate_in(self.KEY, (SD.array_addunique("b", 4.5),))

        with self.assertRaises(SubdocCantInsertValueException):
            self.coll.mutate_in(self.KEY, (SD.array_addunique("b", {"b1": "b1b1b1"}),))

        with self.assertRaises(SubdocPathMismatchException):
            self.coll.mutate_in(self.KEY, (SD.array_addunique("c", 2),))

    def test_counter_increment(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "count": 100}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        self.coll.mutate_in(self.KEY, (SD.counter("count", 50),))
        result = self.coll.get(self.KEY).content_as[dict]
        self.assertEqual(150, result["count"])

    def test_counter_decrement(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "count": 100}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        self.coll.mutate_in(self.KEY, (SD.counter("count", -50),))
        result = self.coll.get(self.KEY).content_as[dict]
        self.assertEqual(50, result["count"])

    def test_insert_create_parents(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": [0, 1, 3, 4]}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        self.coll.mutate_in(
            self.KEY,
            (SD.insert("c.some_string", "parents created", create_parents=True),),
        )
        result = self.coll.get(self.KEY).content_as[dict]
        self.assertEqual("parents created", result["c"]["some_string"])

    def test_upsert_create_parents(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": [0, 1, 3, 4]}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        self.coll.mutate_in(
            self.KEY,
            (SD.upsert("c.some_string", "parents created", create_parents=True),),
        )
        result = self.coll.get(self.KEY).content_as[dict]
        self.assertEqual("parents created", result["c"]["some_string"])

    def test_array_append_create_parents(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": [0, 1, 3, 4]}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        self.coll.mutate_in(
            self.KEY,
            (
                SD.array_append("some.array", "Hello", create_parents=True),
                SD.array_append("some.array", "World"),
            ),
        )

        result = self.coll.get(self.KEY).content_as[dict]
        self.assertIsInstance(result["some"]["array"], list)
        self.assertEqual(result["some"]["array"], ["Hello", "World"])

    def test_array_prepend_create_parents(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": [0, 1, 3, 4]}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        self.coll.mutate_in(
            self.KEY,
            (
                SD.array_prepend("some.array", "World", create_parents=True),
                SD.array_prepend("some.array", "Hello"),
            ),
        )

        result = self.coll.get(self.KEY).content_as[dict]
        self.assertIsInstance(result["some"]["array"], list)
        self.assertEqual(result["some"]["array"], ["Hello", "World"])

    def test_array_add_unique_create_parents(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": [0, 1, 3, 4]}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        self.coll.mutate_in(
            self.KEY,
            (
                SD.array_addunique("some.set", "my", create_parents=True),
                SD.array_addunique("some.set", "unique"),
                SD.array_addunique("some.set", "set"),
            ),
        )
        result = self.coll.get(self.KEY).content_as[dict]
        self.assertIsInstance(result["some"]["set"], list)
        self.assertIn("my", result["some"]["set"])
        self.assertIn("unique", result["some"]["set"])
        self.assertIn("set", result["some"]["set"])

    def test_counter_create_parents(self):
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": [0, 1, 3, 4]}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        self.coll.mutate_in(
            self.KEY, (SD.counter("some.counter", 100, create_parents=True),)
        )
        result = self.coll.get(self.KEY).content_as[dict]
        self.assertEqual(100, result["some"]["counter"])
