# -*- coding:utf-8 -*-
#
# Copyright 2019, Couchbase, Inc.
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
from couchbase_tests.base import skip_if_no_collections, CollectionTestCase
from couchbase.collection import GetOptions, LookupInOptions, UpsertOptions, ReplaceOptions
from couchbase.exceptions import NotFoundError, InvalidArgumentsException, DocumentUnretrievableException, \
    KeyExistsException, KeyNotFoundException, TempFailException
import unittest
from datetime import timedelta
import couchbase.subdocument as SD
from unittest import SkipTest
from couchbase.diagnostics import ServiceType
import uuid


class CollectionTests(CollectionTestCase):
    """
    These tests should just test the collection interface, as simply
    as possible.  We have the Scenario tests for more complicated
    stuff.
    """
    CONTENT = {"some":"content"}
    KEY = "imakey"
    NOKEY = "somerandomkey"

    def setUp(self):
        super(CollectionTests, self).setUp()
        self.cb.upsert(self.KEY, self.CONTENT)
        # make sure it is available
        self.try_n_times(10, 1, self.cb.get, self.KEY)
        # be sure NOKEY isn't in there
        try:
            self.cb.remove(self.NOKEY)
        except:
            pass
        # make sure NOKEY is gone
        self.try_n_times_till_exception(10, 1, self.cb.get, self.NOKEY)

    def test_exists(self):
        if self.is_mock:
            raise SkipTest("mock does not support exists")
        self.assertTrue(self.cb.exists(self.KEY).exists)

    def test_exists_when_it_does_not_exist(self):
        if self.is_mock:
            raise SkipTest("mock does not support exists")
        key = str(uuid.uuid4())
        self.assertRaises(KeyNotFoundException, self.cb.get, key)
        self.assertFalse(self.cb.exists(key).exists)

    @unittest.skip("this verifies CCBC-1187 - skip till fixed")
    def test_exists_with_recently_removed_key(self):
        if self.is_mock:
            raise SkipTest("mock does not support exists")
        self.cb.remove(self.KEY)
        self.assertRaises(KeyNotFoundException, self.cb.get, self.KEY)
        self.assertFalse(self.cb.exists(self.KEY).exists)

    def test_upsert(self):
        self.cb.upsert(self.NOKEY, {"some":"thing"}, UpsertOptions(timeout=timedelta(seconds=3)))
        result = self.try_n_times(10, 1, self.cb.get, self.NOKEY)
        self.assertEqual(self.NOKEY, result.id)
        self.assertDictEqual({"some":"thing"}, result.content_as[dict])

    def test_insert(self):
        self.cb.insert(self.NOKEY, {"some": "thing"})
        result = self.try_n_times(10, 1, self.cb.get, self.NOKEY)
        self.assertEqual(self.NOKEY, result.id)
        self.assertDictEqual({"some":"thing"}, result.content_as[dict])

    def test_insert_fail(self):
        self.assertRaises(KeyExistsException, self.cb.insert, self.KEY, self.CONTENT)

    def test_replace(self):
        result = self.cb.replace(self.KEY, {"some":"other content"})
        self.assertTrue(result.success)

    def test_replace_with_cas(self):
        old_cas = self.cb.get(self.KEY).cas
        result = self.cb.replace(self.KEY, self.CONTENT, ReplaceOptions(cas=old_cas))
        self.assertTrue(result.success)
        # try same cas again, must fail.  TODO: this seems wrong - lets be sure there
        # isn't perhaps a more sensible exception out there.
        self.assertRaises(KeyExistsException, self.cb.replace, self.KEY, self.CONTENT, ReplaceOptions(cas=old_cas))

    def test_replace_fail(self):
        self.assertRaises(KeyNotFoundException, self.cb.get, self.NOKEY)
        self.assertRaises(KeyNotFoundException, self.cb.replace, self.NOKEY, self.CONTENT)

    def test_remove(self):
        result = self.cb.remove(self.KEY)
        self.assertTrue(result.success)
        self.try_n_times_till_exception(10, 1, self.cb.get, self.KEY)

    def test_remove_fail(self):
        self.assertRaises(KeyNotFoundException, self.cb.remove,self.NOKEY)

    def test_get(self):
        result = self.cb.get(self.KEY)
        self.assertIsNotNone(result.cas)
        self.assertEqual(result.id, self.KEY)
        self.assertIsNone(result.expiry)
        self.assertDictEqual(self.CONTENT, result.content_as[dict])

    def test_get_options(self):
        result = self.cb.get(self.KEY, GetOptions(timeout=timedelta(seconds=2), with_expiry=False))
        self.assertIsNotNone(result.cas)
        self.assertEqual(result.id, self.KEY)
        self.assertIsNone(result.expiry)
        self.assertDictEqual(self.CONTENT, result.content_as[dict])

    def test_get_fails(self):
        self.assertRaises(NotFoundError, self.cb.get, self.NOKEY)

    @unittest.skip("get does not properly do a subdoc lookup and get the xattr expiry yet")
    def test_get_with_expiry(self):
        result = self.cb.get(self.KEY, GetOptions(with_expiry=True))
        self.assertIsNotNone(result.expiry)

    @unittest.skip("get does not properly do a subdoc lookup so project will not work yet")
    def test_project(self):
        result = self.cb.get(self.KEY, GetOptions(project=["some"]))
        self.ssertIsNotNone(result.cas)
        self.assertEqual(result.id, self.KEY)
        self.assertIsNone(result.expiry)
        self.assertDictEqual(self.CONTENT, result.content_as[dict])

    def test_lookup_in_timeout(self):
        self.coll.upsert("id", {'someArray': ['wibble', 'gronk']})
        # wait till it is there
        self.try_n_times(10, 1, self.coll.get, "id")

        # ok, it is there...
        self.coll.get("id", GetOptions(project=["someArray"], timeout=timedelta(seconds=1.0)))
        self.assertRaisesRegex(InvalidArgumentsException, "Expected timedelta", self.coll.get, "id",
                               GetOptions(project=["someArray"], timeout=456))
        sdresult_2 = self.coll.lookup_in("id", (SD.get("someArray"),), LookupInOptions(timeout=timedelta(microseconds=1)))
        self.assertEqual(['wibble', 'gronk'],sdresult_2.content_as[list](0))
        sdresult_2 = self.coll.lookup_in("id", (SD.get("someArray"),), LookupInOptions(timeout=timedelta(seconds=1)), timeout=timedelta(microseconds=1))
        self.assertEqual(['wibble', 'gronk'],sdresult_2.content_as[list](0))

    def _check_replicas(self, all_up=True):
        num_replicas = self.bucket._bucket.configured_replica_count
        if num_replicas < 1:
            raise SkipTest('need replicas to test get_all_replicas')
            # TODO: this is far to difficult - having to use the test framework to get the bucket
        kv_results = self.bucket.ping().endpoints.get(ServiceType.KeyValue, None)
        num_expected = num_replicas+1 if all_up else 2 # 2 means at least one replica is up
        if not kv_results or len(kv_results) < num_expected:
            raise SkipTest('not all replicas are online')

    def test_get_any_replica(self):
        self._check_replicas(False)
        if self.supports_collections():
            raise SkipTest("get_any_replica fails if using collections it seems")
        self.coll.upsert('imakey100', self.CONTENT)
        result = self.try_n_times(10, 3, self.coll.get_any_replica, 'imakey100')
        self.assertDictEqual(self.CONTENT, result.content_as[dict])

    @unittest.skip("This sometimes will segfault - skip for now -- see PYCBC-833")
    def test_get_all_replicas(self):
        self._check_replicas()
        self.coll.upsert('imakey100', self.CONTENT)
        # wait till it it there...
        result = self.try_n_times(10, 3, self.coll.get_all_replicas, "imakey100")
        if not hasattr(result, '__iter__'):
            result = [result]
        for r in result:
            self.assertDictEqual(self.CONTENT, r.content_as[dict])

    @unittest.skip("get_all_replicas will sometimes segfault - see PYCBD-833")
    def test_get_all_replicas_returns_master(self):
        self._check_replicas()
        self.coll.upsert('imakey100', self.CONTENT)
        result = self.try_n_times(10, 3, self.coll.get_all_replicas, 'imakey100')
        if not hasattr(result, '__iter__'):
            result = [result]
        # TODO: this isn't implemented yet - waiting on CCBC-1169
        # when it does work, we just need to make sure one of the
        # results returns True for is_replica()
        for r in result:
            with self.assertRaises(NotImplementedError):
                r.is_replica()

    def test_touch(self):
        self.cb.touch(self.KEY, timedelta(seconds=3))
        self.try_n_times_till_exception(10, 3, self.cb.get, self.KEY)
        self.assertRaises(KeyNotFoundException, self.cb.get, self.KEY)

    def test_get_and_touch(self):
        self.cb.get_and_touch(self.KEY, timedelta(seconds=3))
        self.try_n_times_till_exception(10, 3, self.cb.get, self.KEY)
        self.assertRaises(KeyNotFoundException, self.cb.get, self.KEY)

    def test_get_and_lock(self):
        self.cb.get_and_lock(self.KEY, timedelta(seconds=3))
        self.try_n_times(10, 1, self.cb.upsert, self.KEY, self.CONTENT)
        self.cb.get(self.KEY)

    def test_get_and_lock_upsert_with_cas(self):
        result = self.cb.get_and_lock(self.KEY, timedelta(seconds=15))
        cas = result.cas
        self.assertRaises(KeyExistsException, self.cb.upsert, self.KEY, self.CONTENT)
        self.cb.replace(self.KEY, self.CONTENT, ReplaceOptions(cas=cas))

    def test_unlock(self):
        cas = self.cb.get_and_lock(self.KEY, timedelta(seconds=15)).cas
        self.cb.unlock(self.KEY, cas)
        self.cb.upsert(self.KEY, self.CONTENT)

    def test_unlock_wrong_cas(self):
        cas = self.cb.get_and_lock(self.KEY, timedelta(seconds=15)).cas
        self.assertRaises(TempFailException, self.cb.unlock, self.KEY, 100)
        self.cb.unlock(self.KEY, cas)



