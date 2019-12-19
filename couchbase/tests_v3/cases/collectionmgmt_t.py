# Copyright 2019, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from couchbase.bucket import Bucket
from couchbase.management.collections import CollectionSpec, ScopeSpec
from couchbase_tests.base import SkipTest, CollectionTestCase, ClusterTestCase
from couchbase.exceptions import KeyNotFoundException
from couchbase_core.exceptions import NotSupportedError
from couchbase.exceptions import ScopeNotFoundException, ScopeAlreadyExistsException, CollectionAlreadyExistsException, CollectionNotFoundException
from couchbase.management.buckets import CreateBucketSettings
import time

class CollectionManagerTestCase(CollectionTestCase):
  def setUp(self, *args, **kwargs):
    super(CollectionManagerTestCase, self).setUp()

    # SkipTest if collections not supported
    try:
      self.bucket.collections().get_all_scopes()
    except NotSupportedError:
      raise SkipTest('cluster does not support collections')

    # Need this so we use RBAC.  TODO: lets perhaps move this into the base classes?  Then we can maybe not need the
    # default user, etc...
    self.cluster._cluster.authenticate(username=self.cluster_info.admin_username, password=self.cluster_info.admin_password)
    self.bm = self.cluster.buckets()
    try:
      self.bm.drop_bucket('other-bucket')
    except:
      # we are just insuring it isn't there already
      pass
    self.bm.create_bucket(CreateBucketSettings(name='other-bucket', bucket_type='couchbase', ram_quota_mb=100))
    # HACK: we need to loop a bit, until the bucket exists.
    for x in range(10):
      try:
        time.sleep(10)
        self.other_bucket = self.cluster.bucket('other-bucket')
        # ok if we got here, lets stop looping
        break
      except:
        # if we have looped 10 times, fail
        self.assertTrue(x<9)
        pass
    self.cm = self.other_bucket.collections()

  def tearDown(self):
    self.bm.drop_bucket('other-bucket')
    # HACK: I'm not 100% sure this is necessary
    time.sleep(10)
    super(CollectionManagerTestCase, self).tearDown()

  def testCreateCollection(self):
    self.cm.create_collection(CollectionSpec('other-collection'))
    self.assertIsNotNone([c for c in self.cm.get_all_scopes()[0].collections if c.name == 'other-collection'])

  def testCreateCollectionBadScope(self):
    self.assertRaises(ScopeNotFoundException, self.cm.create_collection, CollectionSpec('imnotgonnawork', 'notarealscope'))

  def testCreateCollectionAlreadyExists(self):
    self.cm.create_collection(CollectionSpec('other-collection'))
    self.assertIsNotNone([c for c in self.cm.get_all_scopes()[0].collections if c.name == 'other-collection'])
    # now, it will fail if we try to create it again...
    self.assertRaises(CollectionAlreadyExistsException, self.cm.create_collection, CollectionSpec('other-collection'))

  def testCollectionGoesInCorrectBucket(self):
    self.cm.create_collection(CollectionSpec('other-collection'))
    # HACK! it seems you can get temp fail when upserting if you immediately
    # upsert after creating a new collection.  Oddly, opening the collection
    # seems to work even when we get the temp fail on the subsequent upsert
    # <sigh>
    time.sleep(5)
    # make sure it actually is in the other-bucket
    self.assertIsNotNone([c for c in self.cm.get_all_scopes()[0].collections if c.name == 'other-collection'])
    # also be sure this isn't in the default bucket
    self.assertFalse([ c for c in self.bucket.collections().get_all_scopes()[0].collections if c.name == 'other-collection'])

  def testCreateScope(self):
    self.cm.create_scope('other-scope')
    scopes = self.cm.get_all_scopes()
    self.assertIsNotNone([s for s in scopes if s.name == 'other-scope'])

  def testCreateScopeAlreadyExists(self):
    self.cm.create_scope('other-scope')
    scopes = self.cm.get_all_scopes()
    self.assertIsNotNone([s for s in scopes if s.name == 'other-scope'])
    self.assertRaises(ScopeAlreadyExistsException, self.cm.create_scope, 'other-scope')

  def testGetAllScopes(self):
    scopes = self.cm.get_all_scopes()
    # this is a brand-new bucket, so it should only have _default scope and a _default collection
    self.assertTrue(len(scopes) == 1)
    scope = scopes[0]
    self.assertEqual(scope.name, '_default')
    self.assertEqual(1, len(scope.collections))
    collection = scope.collections[0]
    self.assertEqual('_default', collection.name)
    self.assertEqual('_default', collection.scope_name)

  def testGetScope(self):
    self.assertIsNotNone(self.cm.get_scope('_default'))

  def testGetScopeNoScope(self):
    self.assertRaises(ScopeNotFoundException, self.cm.get_scope, 'somerandomname')

  def testDropCollection(self):
    self.cm.create_collection(CollectionSpec('other-collection'))
    self.assertTrue([c for c in self.cm.get_all_scopes()[0].collections if c.name == 'other-collection'])
    self.cm.drop_collection(CollectionSpec('other-collection'))
    self.assertFalse([c for c in self.cm.get_all_scopes()[0].collections if c.name == 'other-collection'])

  def testDropCollectionNotFound(self):
    self.assertRaises(CollectionNotFoundException, self.cm.drop_collection, CollectionSpec('somerandomname'))

  def testDropCollectionScopeNotFound(self):
    self.assertRaises(ScopeNotFoundException, self.cm.drop_collection, CollectionSpec('collectionname', 'scopename'))

  def testDropScope(self):
    self.cm.create_scope('other-scope')
    self.assertTrue([s for s in self.cm.get_all_scopes() if s.name == 'other-scope'])
    self.cm.drop_scope('other-scope')
    self.assertFalse([s for s in self.cm.get_all_scopes() if s.name == 'other-scope'])

  def testDropScopeNotFound(self):
    self.assertRaises(ScopeNotFoundException, self.cm.drop_scope, 'somerandomscope')
