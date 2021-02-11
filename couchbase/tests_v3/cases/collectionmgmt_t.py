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

from couchbase.management.collections import CollectionSpec
from couchbase_tests.base import SkipTest, CollectionTestCase
from couchbase.exceptions import BucketDoesNotExistException, DocumentExistsException, DocumentNotFoundException, NotSupportedException, ScopeNotFoundException, ScopeAlreadyExistsException, \
    CollectionAlreadyExistsException, CollectionNotFoundException
from couchbase.management.buckets import CreateBucketSettings
from datetime import timedelta
import time


class CollectionManagerTestCase(CollectionTestCase):
    
    def setUp(self, *args, **kwargs):
        super(CollectionManagerTestCase, self).setUp()

        # SkipTest if collections not supported
        try:
          self.bucket.collections().get_all_scopes()
        except NotSupportedException:
          raise SkipTest('cluster does not support collections')

        # Need this so we use RBAC.
        # TODO: lets perhaps move this into the base classes?  Then we can maybe not need the default user, etc...
        self.cluster._cluster.authenticate(username=self.cluster_info.admin_username, password=self.cluster_info.admin_password)
        self.bm = self.cluster.buckets()

        # insure other-bucket is gone first
        try:
            self.bm.drop_bucket('other-bucket')
        except:
            # it maybe isn't there, that's fine
            pass
        self.try_n_times_till_exception(10, 1, self.bm.get_bucket, 'other-bucket', expected_exceptions=(BucketDoesNotExistException,))

        # now re-create it fresh (maybe we could just flush, but we may test settings which would not be flushed)
        self.try_n_times(10, 1, self.bm.create_bucket, CreateBucketSettings(name='other-bucket', bucket_type='couchbase', ram_quota_mb=100))
        self.try_n_times(10, 1, self.bm.get_bucket, 'other-bucket')
        # we need to get the bucket, but sometimes this fails for a few seconds depending on what
        # the cluster is doing.  So, try_n_times...
        def get_bucket(name):
            return self.cluster.bucket(name)
        self.other_bucket = self.try_n_times(10, 3, get_bucket, 'other-bucket')
        self.cm = self.other_bucket.collections()

    def get_scope(self, bucket_name, scope_name):
        bucket = self.try_n_times(10, 3, self.cluster.bucket, bucket_name)
        if bucket:
            cm = bucket.collections()
            return next((s for s in cm.get_all_scopes() if s.name == scope_name), None)

        return None

    def get_collection(self, bucket_name, scope_name, coll_name):
        scope = self.get_scope(bucket_name, scope_name)
        if scope:
            return next((c for c in scope.collections if c.name == coll_name), None)

        return None

    def testCreateCollection(self):
        collection = CollectionSpec('other-collection')
        self.cm.create_collection(collection)
        self.assertIsNotNone(self.get_collection(self.other_bucket.bucket,collection.scope_name, collection.name))

    def testCreateCollectionMaxTTL(self):
        collection = CollectionSpec('other-collection', max_ttl=timedelta(seconds=2))
        self.cm.create_collection(collection)
        self.assertIsNotNone(self.get_collection(self.other_bucket.bucket, collection.scope_name, collection.name))
        # pop a doc in with no ttl, verify it goes away...
        coll = self.try_n_times(10, 1, self.other_bucket.collection, 'other-collection')
        key = self.gen_key('cmtest')
        # we _can_ get a temp fail here, as we just created the collection.  So we
        # retry the upsert.
        self.try_n_times(10, 1, coll.upsert, key, {"some":"thing"})
        self.try_n_times(10, 1, coll.get, key)
        self.try_n_times_till_exception(4, 1, coll.get, key, expected_exceptions=(DocumentNotFoundException,))

    def testCreateCollectionBadScope(self):
        self.assertRaises(ScopeNotFoundException, self.cm.create_collection, CollectionSpec('imnotgonnawork', 'notarealscope'))

    def testCreateCollectionAlreadyExists(self):
        collection = CollectionSpec('other-collection')
        self.cm.create_collection(collection)
        #verify the collection exists w/in other-bucket
        self.assertIsNotNone(self.get_collection(self.other_bucket.bucket, collection.scope_name, collection.name))
        # now, it will fail if we try to create it again...
        self.assertRaises(CollectionAlreadyExistsException, self.cm.create_collection, collection)

    def testCollectionGoesInCorrectBucket(self):
        collection = CollectionSpec('other-collection')
        self.cm.create_collection(collection)
        # make sure it actually is in the other-bucket
        self.assertIsNotNone(self.get_collection(self.other_bucket.bucket, collection.scope_name, collection.name))
        # also be sure this isn't in the default bucket
        self.assertIsNone(self.get_collection(self.bucket.bucket, collection.scope_name, collection.name))

    def testCreateScope(self):
        self.cm.create_scope('other-scope')
        self.assertIsNotNone(self.get_scope(self.other_bucket.bucket, 'other-scope'))

    def testCreateScopeAlreadyExists(self):
        self.cm.create_scope('other-scope')
        self.assertIsNotNone(self.get_scope(self.other_bucket.bucket, 'other-scope'))
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
        collection = CollectionSpec('other-collection')
        self.cm.create_collection(collection)
        #verify the collection exists w/in other-bucket
        self.try_n_times_till_exception(4, 1, self.cm.create_collection, collection, expected_exceptions=(CollectionAlreadyExistsException,))
        # attempt to drop it again will raise CollectionNotFoundException
        self.cm.drop_collection(collection)
        self.assertRaises(CollectionNotFoundException, self.cm.drop_collection, collection)

    def testDropCollectionNotFound(self):
        self.assertRaises(CollectionNotFoundException, self.cm.drop_collection, CollectionSpec('somerandomname'))

    def testDropCollectionScopeNotFound(self):
        self.assertRaises(ScopeNotFoundException, self.cm.drop_collection, CollectionSpec('collectionname', 'scopename'))

    def testDropScope(self):
        scope = 'other-scope'
        self.cm.create_scope(scope)
        self.assertIsNotNone(self.get_scope(self.other_bucket.bucket, scope))
        self.cm.drop_scope(scope)
        self.assertRaises(ScopeNotFoundException, self.cm.drop_scope, scope)

    def testDropScopeNotFound(self):
      self.assertRaises(ScopeNotFoundException, self.cm.drop_scope, 'somerandomscope')
