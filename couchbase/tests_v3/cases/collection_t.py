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
from couchbase.collection import GetOptions, LookupInOptions
from couchbase.exceptions import NotFoundError, InvalidArgumentsException, DocumentUnretrievableException
import unittest
from datetime import timedelta
import couchbase.subdocument as SD
from unittest import SkipTest

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

    # TODO: lets turn skipIfMock into an annotation
    def test_exists(self):
        self.skipIfMock()
        self.assertTrue(self.cb.exists(self.KEY).exists)

    # TODO: lets turn skipIfMock into an annotation
    def test_exists_when_it_does_not_exist(self):
        self.skipIfMock()
        self.assertFalse(self.cb.exists(self.NOKEY).exists)

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
        assertIsNotNone(result.cas)
        assertEqual(result.id, self.KEY)
        assertIsNone(result.expiry)
        assertDictEqual(self.CONTENT, result.content_as[dict])

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

    def test_get_any_replica(self):
        if self.bucket._bucket.configured_replica_count < 1:
            raise SkipTest('need replicas to test get_any_replica')

        try:
            self.coll.upsert('imakey100', self.CONTENT)
            # wait till it it there...
            self.try_n_times(10, 1, self.coll.get, "imakey100")
            # ok it is there...
            result = self.coll.get_any_replica('imakey100')
            self.assertDictEqual(self.CONTENT, result.content_as[dict])
        except DocumentUnretrievableException as e:
            # probably, you have replicas enabled, but on single node
            print("Perhaps your test server configured the default bucket to have replicas, but no other servers are up?")
            raise

    def test_get_all_replicas(self):
        if self.bucket._bucket.configured_replica_count < 1:
            raise SkipTest('need replicas to test get_all_replicas')
        try:
            self.coll.upsert('imakey100', self.CONTENT)
            # wait till it it there...
            self.try_n_times(10, 1, self.coll.get, "imakey100")
            # ok it is there...
            result = self.coll.get_all_replicas('imakey100')
            for r in result:
                self.assertDictEqual(self.CONTENT, r.content_as[dict])
        except DocumentUnretrievableException as e:
            # probably, you have replicas enabled, but on single node
            print("Perhaps your test server configured the default bucket to have replicas, but no other servers are up?")
            raise

    def test_get_any_replica_gives_active(self):
        if self.bucket._bucket.configured_replica_count < 1:
            raise SkipTest('need replicas to test get_any_replica')
        raise SkipTest('need is_active support in LCB, skipping for now')

    def test_get_any_replica_fail(self):
        if self.bucket._bucket.configured_replica_count < 1:
            raise SkipTest('need replicas to test get_any_replica')

        try:
            self.coll.upsert('imakey100', self.CONTENT)
            # wait till it it there...
            self.try_n_times(10, 1, self.coll.get, "imakey100")
            # ok it is there...
            result = self.coll.get_all_replicas('imakey100')
            # TODO: this isn't implemented yet - waiting on CCBC-1169
            # when is does work, we just need to make sure one of the
            # results returns True for is_replica()
            for r in result:
                with self.assertRaises(NotImplementedError):
                    r.is_replica()
        except DocumentUnretrievableException as e:
            # probably, you have replicas enabled, but on single node
            print("Perhaps your test server configured the default bucket to have replicas, but no other servers are up?")
            raise

