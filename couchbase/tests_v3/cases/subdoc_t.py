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
from couchbase.durability import ClientDurability
from couchbase_tests.base import CollectionTestCase
from couchbase.collection import GetOptions, LookupInOptions
from couchbase.exceptions import InvalidArgumentException, PathNotFoundException, DurabilityImpossibleException
from couchbase.collection import MutateInOptions
from datetime import timedelta
import couchbase.subdocument as SD
from unittest import SkipTest
from datetime import datetime


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
        result = self.coll.lookup_in(self.KEY, (SD.exists("a"), SD.exists("qzzxy"),))
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
            raise SkipTest("mock doesn't support getting xattrs (like $document.expiry)")
        cas = self.coll.upsert(self.KEY, {"a": "aaa", "b": {"c": {"d": "yo!"}}}).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        result = self.coll.lookup_in(self.KEY,
                                     (SD.with_expiry(),
                                      SD.get("a"),
                                      SD.exists("b"),
                                      SD.get("b.c")))
        self.assertTrue(result.success)
        self.assertIsNone(result.expiry)
        self.assertEqual("aaa", result.content_as[str](1))
        self.assertTrue(result.exists(2))
        self.assertDictEqual({"d": "yo!"}, result.content_as[dict](3))

    def test_mutate_in_simple(self):
        cas = self.coll.mutate_in(self.KEY, (SD.upsert("c", "ccc"), SD.replace("b", "XXX"),)).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        result = self.coll.get(self.KEY).content_as[dict]
        self.assertDictEqual({"a": "aaa", "b": "XXX", "c": "ccc"}, result)

    def test_mutate_in_expiry(self):
        if self.is_mock:
            raise SkipTest("mock doesn't support getting xattrs (like $document.expiry)")

        cas = self.coll.mutate_in(self.KEY,
                                  (SD.upsert("c", "ccc"), SD.replace("b", "XXX"),),
                                  MutateInOptions(expiry=timedelta(seconds=1000))).cas
        self.try_n_times(10, 3, self._cas_matches, self.KEY, cas)
        result = self.coll.get(self.KEY, GetOptions(with_expiry=True))
        expires_in = (result.expiryTime - datetime.now()).total_seconds()
        self.assertTrue(0 < expires_in < 1001)

    def test_mutate_in_durability(self):
        if self.is_mock:
            raise SkipTest("mock doesn't support getting xattrs (like $document.expiry)")
        self.assertRaises(DurabilityImpossibleException, self.coll.mutate_in,self.KEY,
                                  (SD.upsert("c", "ccc"), SD.replace("b", "XXX"),),
                                  MutateInOptions(durability=ClientDurability(replicate_to=5)))

    # refactor!  Also, this seems like it should timeout.  I suspect a bug here.  I don't really
    # believe there is any way this could not timeout on the first lookup_in
    def test_lookup_in_timeout(self):
        self.coll.upsert("id", {'someArray': ['wibble', 'gronk']})
        # wait till it is there
        self.try_n_times(10, 1, self.coll.get, "id")

        # ok, it is there...
        self.coll.get("id", GetOptions(project=["someArray"], timeout=timedelta(seconds=1.0)))
        self.assertRaisesRegex(InvalidArgumentException, "Expected timedelta", self.coll.get, "id",
                               GetOptions(project=["someArray"], timeout=456))
        sdresult_2 = self.coll.lookup_in("id", (SD.get("someArray"),), LookupInOptions(timeout=timedelta(microseconds=1)))
        self.assertEqual(['wibble', 'gronk'],sdresult_2.content_as[list](0))
        sdresult_2 = self.coll.lookup_in("id", (SD.get("someArray"),), LookupInOptions(timeout=timedelta(seconds=1)), timeout=timedelta(microseconds=1))
        self.assertEqual(['wibble', 'gronk'],sdresult_2.content_as[list](0))

