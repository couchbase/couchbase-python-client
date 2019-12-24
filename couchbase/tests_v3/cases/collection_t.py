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

class CollectionTests(CollectionTestCase):
  """
  These tests should just test the collection interface, as simply
  as possible.  We have the Scenario tests for more complicated
  stuff.
  """
  def setUp(self):
    super(CollectionTests, self).setUp()

  @skip_if_no_collections
  def test_exists(self):
    self.cb.upsert("imakey", {"some":"content"})
    self.assertTrue(self.cb.exists("imakey").exists)

  @skip_if_no_collections
  def test_exists_when_it_does_not_exist(self):
    self.assertFalse(self.cb.exists("somerandomkey").exists)




