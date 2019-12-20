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

from couchbase.bucket import Bucket
from couchbase.management.queries import QueryIndex, QueryIndexNotFoundException, IndexAlreadyExistsException
from couchbase_tests.base import SkipTest, CollectionTestCase
from nose.plugins.attrib import attr
from typing import *
from couchbase.exceptions import KeyExistsException, CouchbaseError


@attr("index")
class IndexManagementTestCase(CollectionTestCase):
    def _clear_indexes(self):
        # Drop all indexes!
        for index in self.mgr.get_all_indexes(self.cluster_info.bucket_name):
            self.mgr.drop_index(self.cluster_info.bucket_name, index.name)

    def setUp(self, *args, **kwargs):
        super(IndexManagementTestCase, self).setUp()
        self.mgr = self.cluster.query_indexes()
        self.skipIfMock()
        if self.bucket.__class__ is not Bucket:
            raise SkipTest('Only supported for synchronous bucket')

        self._clear_indexes()

    def tearDown(self):
        self._clear_indexes()
        super(IndexManagementTestCase, self).tearDown()

    def test_create_primary(self):
        cb = self.bucket.default_collection()
        mgr = self.cluster.query_indexes()

        bucket_name = self.cluster_info.bucket_name
        mgr.create_primary_index(bucket_name,timeout=timedelta(seconds=60))

        # Ensure we can issue a query
        qstr = 'select * from {0} limit 1'.format(self.cluster_info.bucket_name)
        cb.query(qstr).execute()
        # Drop the primary index
        mgr.drop_primary_index(bucket_name)
        # Ensure we get an error when executing the query
        self.assertRaises(
            CouchbaseError, lambda x: x.rows(), self.cluster.query(qstr))

        ixname = 'namedPrimary'
        # Try to create a _named_ primary index
        mgr.create_index(bucket_name, ixname, [], primary=True)
        self.cluster.query(qstr).rows()
        # All OK
        mgr.drop_index(bucket_name, ixname)
        self.assertRaises(
            CouchbaseError, lambda x: x.rows(), self.cluster.query(qstr))

        # Create the primary index the first time
        mgr.create_primary_index(bucket_name)
        mgr.create_primary_index(bucket_name, ignore_if_exists=True)

        self.assertRaises(IndexAlreadyExistsException, mgr.create_primary_index, bucket_name)

        # Drop the indexes
        mgr.drop_primary_index(bucket_name)
        mgr.drop_primary_index(bucket_name, ignore_if_not_exists=True)
        self.assertRaises(QueryIndexNotFoundException, mgr.drop_primary_index, bucket_name)

        # Test with _named_ primaries
        mgr.create_index(bucket_name, ixname, [], primary=True)
        mgr.create_index(bucket_name, ixname, [], primary=True, ignore_if_exists=True)
        self.assertRaises(IndexAlreadyExistsException, mgr.create_index, bucket_name,
                          ixname, [], primary=True)

        mgr.drop_index(bucket_name, ixname)
        mgr.drop_index(bucket_name, ixname, ignore_missing=True)
        self.assertRaises(QueryIndexNotFoundException, mgr.drop_index, bucket_name, ixname)

    def test_alt_indexes(self):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        bucket_name=self.cluster_info.bucket_name
        self.mgr._admin_bucket.timeout=10000
        self.mgr.create_index(bucket_name, ixname, fields=fields,timeout=timedelta(days=1))
        mgr=self.mgr
        qq = 'select {1}, {2} from `{0}` where {1}=1 and {2}=2 limit 1'\
            .format(bucket_name, *fields)
        self.cluster.query(qq)

        # Drop the index
        self.mgr.drop_index(bucket_name, ixname)
        # Issue the query again
        self.assertRaises(CouchbaseError,
                          lambda x: x.rows(), self.cluster.query(qq))
        self.assertRaises((ValueError, TypeError),
                          mgr.create_index, bucket_name, 'withoutFields')
        mgr.create_index(bucket_name, ixname, fields=['hello'])
        mgr.create_index(bucket_name, ixname, fields=['hello'], ignore_if_exists=True)
        self.assertRaises(IndexAlreadyExistsException, mgr.create_index,
                          bucket_name, ixname, fields=['hello'])

        # Drop it
        mgr.drop_index(bucket_name, ixname)
        mgr.drop_index(bucket_name, ixname, ignore_if_not_exists=True)
        self.assertRaises(QueryIndexNotFoundException, mgr.drop_index, bucket_name, ixname)

        # Create an index with a condition
        ixname = 'ix_with_condition'
        cond = '((`foo` = "foo") and (`bar` = "bar"))'
        mgr.create_index(bucket_name, ixname, fields=['foo'], condition=cond)
        ll = list(filter(lambda x: x.name == ixname, mgr.get_all_indexes(bucket_name)))
        self.assertTrue(ll)
        self.assertEqual(cond, ll[0].condition)

    def test_list_indexes(self):
        mgr = self.mgr
        ixs = list(mgr.get_all_indexes(self.cluster_info.bucket_name))
        self.assertEqual(0, len(ixs))

        # Create the primary index
        mgr.create_primary_index(self.cluster_info.bucket_name)
        ixs = list(mgr.get_all_indexes(self.cluster_info.bucket_name))  # type: List[QueryIndex]
        self.assertEqual(1, len(ixs))
        self.assertTrue(ixs[0].is_primary)
        self.assertEqual('#primary', ixs[0].name)
        self.assertEqual(self.cluster_info.bucket_name, ixs[0].keyspace)

    def test_deferred(self):
        # Create primary index
        mgr=self.mgr
        self.mgr.create_primary_index(self.cluster_info.bucket_name, deferred=True)
        ix = self.mgr.get_all_indexes(self.cluster_info.bucket_name)
        self.assertEqual('deferred', next(iter(ix)).state)

        # Create a bunch of other indexes
        for n in range(5):
            self.mgr.create_index(self.cluster_info.bucket_name,
                'ix{0}'.format(n), fields=['fld{0}'.format(n)], deferred=True)

        ix = mgr.get_all_indexes(self.cluster_info.bucket_name)
        self.assertEqual(6, len(ix))

        pending = mgr.build_deferred_indexes(self.cluster_info.bucket_name)
        self.assertEqual(6, len(pending))
        mgr.watch_indexes(self.cluster_info.bucket_name, pending)  # Should be OK
        mgr.watch_indexes(self.cluster_info.bucket_name, pending)  # Should be OK again
        self.assertRaises(QueryIndexNotFoundException, mgr.watch_indexes, self.cluster_info.bucket_name, ['nonexist'])
