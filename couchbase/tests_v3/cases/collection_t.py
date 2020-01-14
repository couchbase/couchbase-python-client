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
from couchbase.collection import GetOptions
from couchbase.exceptions import NotFoundError
import unittest
from datetime import timedelta

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
    # be sure NOKEY isn't in there
    try:
      self.cb.remove(self.NOKEY)
    except:
      pass

  def test_exists(self):
    self.assertTrue(self.cb.exists(self.KEY).exists)

  @unittest.skip("LCB seems to not return an error anymore from exists, so lets fix that")
  def test_exists_when_it_does_not_exist(self):
    self.assertFalse(self.cb.exists(self.NOKEY).exists)

  def test_get(self):
    result = self.cb.get(self.KEY)
    self.assertIsNotNone(result.cas)
    self.assertEquals(result.id, self.KEY)
    self.assertIsNone(result.expiry)
    self.assertDictEqual(self.CONTENT, result.content_as[dict])

  def test_get_options(self):
    result = self.cb.get(self.KEY, GetOptions(timeout=timedelta(seconds=2), with_expiry=False))
    self.assertIsNotNone(result.cas)
    self.assertEquals(result.id, self.KEY)
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
    assertEquals(result.id, self.KEY)
    assertIsNone(result.expiry)
    assertDictEqual(self.CONTENT, result.content_as[dict])
