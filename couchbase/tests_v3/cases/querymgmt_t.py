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
from datetime import timedelta
from typing import *
import time

from nose.plugins.attrib import attr

from couchbase.management.queries import (CreateQueryIndexOptions, DropQueryIndexOptions,
                                          GetAllQueryIndexOptions, QueryIndexManager,
                                          WatchQueryIndexOptions, CreatePrimaryQueryIndexOptions,
                                          BuildDeferredQueryIndexOptions)
from couchbase.exceptions import (CouchbaseException, QueryIndexNotFoundException,
                                  QueryIndexAlreadyExistsException, WatchQueryIndexTimeoutException,
                                  NotSupportedException)
from couchbase_tests.base import SkipTest, CollectionTestCase


@attr("index")
class IndexManagementTestCase(CollectionTestCase):
    def _clear_indexes(self):
        # Drop all indexes!
        for index in self.mgr.get_all_indexes(self.cluster_info.bucket_name):
            self.mgr.drop_index(self.cluster_info.bucket_name, index.name)
        for _ in range(10):
            if 0 == len(self.mgr.get_all_indexes(
                    self.cluster_info.bucket_name)):
                return
            time.sleep(3)
        self.fail(
            "indexes were not dropped after {} waits of {} seconds each".format(10, 3))

    def setUp(self, *args, **kwargs):
        super(IndexManagementTestCase, self).setUp()
        self.mgr = self.cluster.query_indexes()  # type: QueryIndexManager
        self.skipIfMock()
        if self.cluster._is_dev_preview():
            raise SkipTest(
                "dev preview is on, that means index creation will fail")
        self._clear_indexes()

    def tearDown(self):
        self._clear_indexes()
        super(IndexManagementTestCase, self).tearDown()

    def test_create_primary(self):
        bucket_name = self.cluster_info.bucket_name
        self.mgr.create_primary_index(
            bucket_name, timeout=timedelta(seconds=60))

        # Ensure we can issue a query
        qstr = 'select * from {0} limit 1'.format(
            self.cluster_info.bucket_name)
        result = self.cluster.query(qstr).rows()
        # Drop the primary index
        self.mgr.drop_primary_index(bucket_name)
        # Ensure we get an error when executing the query
        self.assertRaises(
            CouchbaseException, lambda x: x.rows(), self.cluster.query(qstr))

    def test_create_named_primary(self):
        bucket_name = self.cluster_info.bucket_name
        ixname = 'namedPrimary'
        qstr = 'select * from {0} limit 1'.format(
            self.cluster_info.bucket_name)
        # Try to create a _named_ primary index
        self.mgr.create_index(bucket_name, ixname, [], primary=True)
        self.cluster.query(qstr).rows()
        # All OK
        self.mgr.drop_index(bucket_name, ixname)
        self.assertRaises(
            CouchbaseException, lambda x: x.rows(), self.cluster.query(qstr))

    def test_create_primary_ignore_if_exists(self):
        bucket_name = self.cluster_info.bucket_name
        self.mgr.create_primary_index(bucket_name)
        self.mgr.create_primary_index(
            bucket_name, CreatePrimaryQueryIndexOptions(ignore_if_exists=True))

        self.assertRaises(QueryIndexAlreadyExistsException,
                          self.mgr.create_primary_index, bucket_name)

    def test_create_primary_ignore_if_exists_kwargs(self):
        bucket_name = self.cluster_info.bucket_name
        self.mgr.create_primary_index(bucket_name)
        self.mgr.create_primary_index(bucket_name, ignore_if_exists=True)

        self.assertRaises(QueryIndexAlreadyExistsException,
                          self.mgr.create_primary_index, bucket_name)

    def test_drop_primary_ignore_if_not_exists(self):
        bucket_name = self.cluster_info.bucket_name
        self.mgr.drop_primary_index(bucket_name, ignore_if_not_exists=True)
        self.assertRaises(QueryIndexNotFoundException,
                          self.mgr.drop_primary_index, bucket_name)

    def test_create_named_primary_ignore_if_exists(self):
        ixname = 'namedPrimary'
        bucket_name = self.cluster_info.bucket_name
        self.mgr.create_index(bucket_name, ixname, [], primary=True)
        self.mgr.create_index(bucket_name, ixname, [],
                              primary=True, ignore_if_exists=True)
        self.assertRaises(QueryIndexAlreadyExistsException, self.mgr.create_index, bucket_name,
                          ixname, [], primary=True)

    def test_drop_named_primary_ignore_if_exists(self):
        ixname = 'namedPrimary'
        bucket_name = self.cluster_info.bucket_name
        self.mgr.create_index(bucket_name, ixname, [], primary=True)
        self.mgr.drop_index(bucket_name, ixname)
        self.mgr.drop_index(bucket_name, ixname, ignore_missing=True)
        self.assertRaises(QueryIndexNotFoundException,
                          self.mgr.drop_index, bucket_name, ixname)

    def test_create_secondary_indexes(self):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        bucket_name = self.cluster_info.bucket_name
        self.mgr._admin_bucket.timeout = 10000
        self.mgr.create_index(bucket_name, ixname,
                              fields=fields, timeout=timedelta(days=1))
        qq = "select {1}, {2} from `{0}` where {1}=1 and {2}=2 limit 1".format(
            bucket_name, *fields)
        self.cluster.query(qq).rows()

    def test_create_secondary_indexes_condition(self):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        bucket_name = self.cluster_info.bucket_name
        self.mgr._admin_bucket.timeout = 10000

        self.try_n_times_till_exception(10, 5, self.mgr.drop_index, bucket_name, ixname,
                                        expected_exceptions=(QueryIndexNotFoundException,))
        CONDITION = '((`fld1` = 1) and (`fld2` = 2))'
        self.mgr.create_index(bucket_name, ixname, fields,
                              CreateQueryIndexOptions(timeout=timedelta(days=1), condition=CONDITION))

        def check_index():
            result = next(
                iter(x for x in self.mgr.get_all_indexes(bucket_name) if x.name == ixname), None)
            self.assertIsNotNone(result)
            return result
        result = self.try_n_times(10, 5, check_index)
        self.assertEqual(CONDITION, result.condition)

    def test_drop_secondary_indexes(self):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        bucket_name = self.cluster_info.bucket_name
        self.mgr.create_index(bucket_name, ixname,
                              fields=fields, timeout=timedelta(days=1))

        qq = "select {1}, {2} from `{0}` where {1}=1 and {2}=2 limit 1".format(
            bucket_name, *fields)

        # Drop the index
        self.mgr.drop_index(bucket_name, ixname)
        # Issue the query again
        self.assertRaises(CouchbaseException,
                          lambda x: x.rows(), self.cluster.query(qq))

    def test_create_index_no_fields(self):
        bucket_name = self.cluster_info.bucket_name
        self.assertRaises((ValueError, TypeError),
                          self.mgr.create_index, bucket_name, 'withoutFields')

    def test_create_secondary_indexes_ignore_if_exists(self):
        ixname = 'ix2'
        bucket_name = self.cluster_info.bucket_name
        self.mgr.create_index(bucket_name, ixname, fields=['hello'])
        self.mgr.create_index(bucket_name, ixname, fields=[
                              'hello'], ignore_if_exists=True)
        self.assertRaises(QueryIndexAlreadyExistsException, self.mgr.create_index,
                          bucket_name, ixname, fields=['hello'])

    def test_drop_secondary_indexes_ignore_if_not_exists(self):
        # Create it
        ixname = 'ix2'
        bucket_name = self.cluster_info.bucket_name
        self.mgr.create_index(bucket_name, ixname, fields=['hello'])
        # Drop it
        self.mgr.drop_index(bucket_name, ixname)
        self.mgr.drop_index(bucket_name, ixname, ignore_if_not_exists=True)
        self.assertRaises(QueryIndexNotFoundException,
                          self.mgr.drop_index, bucket_name, ixname)

    def test_list_indexes(self):
        # start with no indexes
        ixs = list(self.mgr.get_all_indexes(self.cluster_info.bucket_name))
        self.assertEqual(0, len(ixs))

        # Create the primary index
        self.mgr.create_primary_index(self.cluster_info.bucket_name)
        # type: List[QueryIndex]
        ixs = list(self.mgr.get_all_indexes(self.cluster_info.bucket_name))
        self.assertEqual(1, len(ixs))
        self.assertTrue(ixs[0].is_primary)
        self.assertEqual('#primary', ixs[0].name)
        self.assertEqual(self.cluster_info.bucket_name, ixs[0].keyspace)

    def test_index_partition_info(self):
        bucket_name = self.cluster_info.bucket_name
        # use query to create index w/ partition, cannot do that via manager
        # ATM
        qstr = 'CREATE INDEX idx_fld1 ON `{0}`(fld1) PARTITION BY HASH(fld1)'.format(
            bucket_name)
        self.cluster.query(qstr).execute()
        ixs = list(self.mgr.get_all_indexes(bucket_name))
        idx = next((ix for ix in ixs if ix.name == "idx_fld1"), None)
        self.assertIsNotNone(idx)
        self.assertIsNotNone(idx.partition)
        self.assertEqual(idx.partition, "HASH(`fld1`)")

    def test_deferred(self):
        # Create primary index
        self.mgr.create_primary_index(
            self.cluster_info.bucket_name, deferred=True)
        ix = self.mgr.get_all_indexes(self.cluster_info.bucket_name)
        self.assertEqual('deferred', next(iter(ix)).state)

        # Create a bunch of other indexes
        for n in range(5):
            self.mgr.create_index(self.cluster_info.bucket_name,
                                  'ix{0}'.format(n), fields=['fld{0}'.format(n)], deferred=True)

        ix = self.mgr.get_all_indexes(self.cluster_info.bucket_name)
        self.assertEqual(6, len(ix))

        pending = self.mgr.build_deferred_indexes(
            self.cluster_info.bucket_name)
        self.assertEqual(6, len(pending))
        self.mgr.watch_indexes(
            self.cluster_info.bucket_name, pending, WatchQueryIndexOptions(timeout=timedelta(seconds=30)))  # Should be OK
        self.mgr.watch_indexes(self.cluster_info.bucket_name,
                               pending, WatchQueryIndexOptions(timeout=timedelta(seconds=30), watch_primary=True))  # Should be OK again
        self.assertRaises(QueryIndexNotFoundException, self.mgr.watch_indexes,
                          self.cluster_info.bucket_name, ['nonexist'], WatchQueryIndexOptions(timeout=timedelta(seconds=10)))

    def test_watch(self):
        # Create primary index
        self.mgr.create_primary_index(
            self.cluster_info.bucket_name, deferred=True)
        ix = self.mgr.get_all_indexes(self.cluster_info.bucket_name)
        self.assertEqual('deferred', next(iter(ix)).state)

        # Create a bunch of other indexes
        for n in range(5):
            defer = False
            if n % 2 == 0:
                defer = True
            self.mgr.create_index(self.cluster_info.bucket_name,
                                  'ix{0}'.format(n), fields=['fld{0}'.format(n)], deferred=defer)

        ix = self.mgr.get_all_indexes(self.cluster_info.bucket_name)
        self.assertEqual(6, len(ix))
        # by not buildind deffered indexes, should timeout
        self.assertRaises(WatchQueryIndexTimeoutException, self.mgr.watch_indexes,
                          self.cluster_info.bucket_name, [i.name for i in ix], WatchQueryIndexOptions(timeout=timedelta(seconds=5)))


class IndexManagementCollectionTests(CollectionTestCase):
    def setUp(self):
        super(IndexManagementCollectionTests, self).setUp(bucket='beer-sample')

        if not self.is_realserver:
            raise SkipTest('Mock does not mock queries')

        # SkipTest if collections not supported
        try:
            self.bucket.collections().get_all_scopes()
        except NotSupportedException:
            raise SkipTest('Cluster does not support collections')

        self.cm = self.bucket.collections()
        self.create_beer_sample_collections()
        self.mgr = self.cluster.query_indexes()  # type: QueryIndexManager

        self._scope = self.beer_sample_collections.scope
        self._coll1 = "beers"
        self._coll2 = "breweries"
        self.beers_fqdn = '`{}`.`{}`.`{}`'.format(
            self.bucket_name, self._scope, self._coll1)
        self._clear_indexes()

    def tearDown(self):
        self._clear_indexes()
        super(IndexManagementCollectionTests, self).tearDown()

    @classmethod
    def setUpClass(cls) -> None:
        super(IndexManagementCollectionTests, cls).setUpClass(True)

    @classmethod
    def tearDownClass(cls) -> None:
        super(IndexManagementCollectionTests, cls).tearDownClass()

    def _clear_indexes(self):
        # Drop all indexes!
        for coll in (self._coll1, self._coll2):
            for index in self.mgr.get_all_indexes(self.bucket_name, GetAllQueryIndexOptions(scope_name=self._scope, collection_name=coll)):
                try:
                    self.mgr.drop_index(self.bucket_name, index.name, DropQueryIndexOptions(
                        scope_name=self._scope, collection_name=coll))
                except QueryIndexNotFoundException:
                    pass
            for _ in range(10):
                if 0 == len(self.mgr.get_all_indexes(self.bucket_name, GetAllQueryIndexOptions(scope_name=self._scope, collection_name=coll))):
                    return
                time.sleep(3)
            self.fail(
                "indexes were not dropped after {} waits of {} seconds each".format(10, 3))

    def test_create_primary(self):
        self.mgr.create_primary_index(
            self.bucket_name, CreatePrimaryQueryIndexOptions(scope_name=self._scope, collection_name=self._coll1))

        # Ensure we can issue a query
        qstr = 'select * from {0} limit 1'.format(
            self.beers_fqdn)
        self.cluster.query(qstr).rows()

    def test_create_named_primary(self):
        ixname = 'namedPrimary'
        # Try to create a _named_ primary index
        self.mgr.create_index(self.bucket_name, ixname, [
        ], primary=True, scope_name=self._scope, collection_name=self._coll1)

        # Ensure we can issue a query
        qstr = 'select * from {0} limit 1'.format(
            self.beers_fqdn)
        self.cluster.query(qstr).rows()

    def test_create_primary_ignore_if_exists(self):
        self.mgr.create_primary_index(
            self.bucket_name, CreatePrimaryQueryIndexOptions(scope_name=self._scope,
                                                             collection_name=self._coll1))
        self.mgr.create_primary_index(
            self.bucket_name, CreatePrimaryQueryIndexOptions(scope_name=self._scope,
                                                             collection_name=self._coll1,
                                                             ignore_if_exists=True))

        self.assertRaises(QueryIndexAlreadyExistsException,
                          self.mgr.create_primary_index, self.bucket_name,
                          CreatePrimaryQueryIndexOptions(scope_name=self._scope,
                                                         collection_name=self._coll1))

    def test_drop_primary_ignore_if_not_exists(self):
        self.mgr.drop_primary_index(self.bucket_name, ignore_if_not_exists=True,
                                    scope_name=self._scope, collection_name=self._coll1)
        self.assertRaises(QueryIndexNotFoundException,
                          self.mgr.drop_primary_index, self.bucket_name, scope_name=self._scope, collection_name=self._coll1)

    def test_create_secondary_indexes(self):
        ixname = 'name_abv'
        fields = ('name', 'abv')
        self.mgr._admin_bucket.timeout = 10000
        self.mgr.create_index(self.bucket_name, ixname,
                              fields=fields, timeout=timedelta(minutes=2), scope_name=self._scope, collection_name=self._coll1)
        qq = "select {1}, {2} from {0} where {1}=1 and {2}=2 limit 1".format(
            self.beers_fqdn, *fields)
        self.cluster.query(qq).rows()

    def test_drop_secondary_indexes(self):
        ixname = 'name_abv'
        fields = ('name', 'abv')
        self.mgr.create_index(self.bucket_name, ixname,
                              fields, CreateQueryIndexOptions(scope_name=self._scope, collection_name=self._coll1, timeout=timedelta(minutes=2)))

        qq = "select {1}, {2} from {0} where {1}=1 and {2}=2 limit 1".format(
            self.beers_fqdn, *fields)

        # Drop the index
        self.mgr.drop_index(self.bucket_name, ixname, DropQueryIndexOptions(
            scope_name=self._scope, collection_name=self._coll1))
        # Issue the query again
        self.assertRaises(CouchbaseException,
                          lambda x: x.rows(), self.cluster.query(qq))

    def test_create_secondary_indexes_ignore_if_exists(self):
        ixname = 'ix2'
        fields = ['hello']
        self.mgr.create_index(self.bucket_name, ixname, fields, CreateQueryIndexOptions(
            scope_name=self._scope, collection_name=self._coll1))
        self.mgr.create_index(self.bucket_name, ixname, fields, CreateQueryIndexOptions(
            scope_name=self._scope, collection_name=self._coll1, ignore_if_exists=True))
        self.assertRaises(QueryIndexAlreadyExistsException, self.mgr.create_index,
                          self.bucket_name, ixname, fields, scope_name=self._scope, collection_name=self._coll1)

    def test_drop_secondary_indexes_ignore_if_not_exists(self):
        # Create it
        ixname = 'ix2'
        fields = ['hello']
        self.mgr.create_index(self.bucket_name, ixname, fields, CreateQueryIndexOptions(
            scope_name=self._scope, collection_name=self._coll1))
        # Drop it
        self.mgr.drop_index(self.bucket_name, ixname, DropQueryIndexOptions(
            scope_name=self._scope, collection_name=self._coll1))
        self.mgr.drop_index(self.bucket_name, ixname, DropQueryIndexOptions(
            scope_name=self._scope, collection_name=self._coll1, ignore_if_not_exists=True))
        self.assertRaises(QueryIndexNotFoundException,
                          self.mgr.drop_index, self.bucket_name, ixname, scope_name=self._scope, collection_name=self._coll1)

    def test_list_indexes(self):
        # start with no indexes
        ixs = list(self.mgr.get_all_indexes(self.bucket_name, GetAllQueryIndexOptions(
            scope_name=self._scope, collection_name=self._coll1)))
        self.assertEqual(0, len(ixs))

        # Create the primary index
        self.mgr.create_primary_index(self.bucket_name, CreatePrimaryQueryIndexOptions(
            scope_name=self._scope, collection_name=self._coll1))
        # type: List[QueryIndex]
        ixs = list(self.mgr.get_all_indexes(self.bucket_name, GetAllQueryIndexOptions(
            scope_name=self._scope, collection_name=self._coll1)))
        self.assertEqual(1, len(ixs))
        self.assertTrue(ixs[0].is_primary)
        self.assertEqual('#primary', ixs[0].name)
        self.assertEqual(self.bucket_name, ixs[0].bucket_name)
        self.assertEqual(self._scope, ixs[0].scope_name)
        self.assertEqual(self._coll1, ixs[0].collection_name)

    def test_deferred(self):
        # Create primary index
        self.mgr.create_primary_index(
            self.bucket_name, CreatePrimaryQueryIndexOptions(deferred=True,
                                                             scope_name=self._scope,
                                                             collection_name=self._coll1))
        ix = self.mgr.get_all_indexes(self.bucket_name, GetAllQueryIndexOptions(
            scope_name=self._scope,
            collection_name=self._coll1))
        self.assertEqual('deferred', next(iter(ix)).state)

        # Create a bunch of other indexes
        for n in range(5):
            self.mgr.create_index(self.bucket_name,
                                  'ix{0}'.format(n),
                                  fields=['fld{0}'.format(n)],
                                  deferred=True,
                                  scope_name=self._scope,
                                  collection_name=self._coll1)

        ix = self.mgr.get_all_indexes(self.bucket_name,
                                      scope_name=self._scope,
                                      collection_name=self._coll1)
        self.assertEqual(6, len(ix))

        ix_names = list(map(lambda i: i.name, ix))

        self.mgr.build_deferred_indexes(
            self.bucket_name, BuildDeferredQueryIndexOptions(scope_name=self._scope, collection_name=self._coll1))
        self.mgr.watch_indexes(
            self.bucket_name, ix_names, WatchQueryIndexOptions(timeout=timedelta(seconds=30),
                                                              scope_name=self._scope,
                                                              collection_name=self._coll1))  # Should be OK
        self.mgr.watch_indexes(self.bucket_name,
                               ix_names, WatchQueryIndexOptions(timeout=timedelta(seconds=30),
                                                               watch_primary=True,
                                                               scope_name=self._scope,
                                                               collection_name=self._coll1))  # Should be OK again
        self.assertRaises(QueryIndexNotFoundException, self.mgr.watch_indexes,
                          self.bucket_name, ['nonexist'], WatchQueryIndexOptions(timeout=timedelta(seconds=10),
                                                                                 scope_name=self._scope,
                                                                                 collection_name=self._coll1))

    def test_watch(self):
        # Create primary index
        self.mgr.create_primary_index(
            self.bucket_name, CreatePrimaryQueryIndexOptions(deferred=True,
                                                             scope_name=self._scope,
                                                             collection_name=self._coll1))
        ix = self.mgr.get_all_indexes(self.bucket_name, GetAllQueryIndexOptions(
            scope_name=self._scope,
            collection_name=self._coll1))
        self.assertEqual('deferred', next(iter(ix)).state)

        # Create a bunch of other indexes
        for n in range(5):
            defer = False
            if n % 2 == 0:
                defer = True
            self.mgr.create_index(self.bucket_name,
                                  'ix{0}'.format(n), fields=['fld{0}'.format(n)],
                                  deferred=defer,
                                  scope_name=self._scope,
                                  collection_name=self._coll1)

        ix = self.mgr.get_all_indexes(self.bucket_name,
                                      scope_name=self._scope,
                                      collection_name=self._coll1)
        self.assertEqual(6, len(ix))
        # by not buildind deffered indexes, should timeout
        self.assertRaises(WatchQueryIndexTimeoutException, self.mgr.watch_indexes,
                          self.bucket_name, [i.name for i in ix],
                          WatchQueryIndexOptions(timeout=timedelta(seconds=5),
                                                 scope_name=self._scope,
                                                 collection_name=self._coll1))
