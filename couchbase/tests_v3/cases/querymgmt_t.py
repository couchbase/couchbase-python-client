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

from couchbase.management.queries import QueryIndex, CreateQueryIndexOptions, QueryIndexManager
from couchbase.exceptions import CouchbaseException, \
    QueryIndexNotFoundException, QueryIndexAlreadyExistsException
from couchbase_tests.base import SkipTest, CollectionTestCase
from nose.plugins.attrib import attr
from typing import *
import time

@attr("index")
class IndexManagementTestCase(CollectionTestCase):
    def _clear_indexes(self):
        # Drop all indexes!
        for index in self.mgr.get_all_indexes(self.cluster_info.bucket_name):
            print("dropping {}".format(index.name))
            self.mgr.drop_index(self.cluster_info.bucket_name, index.name)
        for _ in range(10):
            if 0 == len(self.mgr.get_all_indexes(self.cluster_info.bucket_name)):
                return
            time.sleep(3)
        self.fail("indexes were not dropped after {} waits of {} seconds each".format(10, 3))

    def setUp(self, *args, **kwargs):
        super(IndexManagementTestCase, self).setUp()
        self.mgr = self.cluster.query_indexes()  # type: QueryIndexManager
        self.skipIfMock()
        if self.cluster._is_dev_preview():
            raise SkipTest("dev preview is on, that means index creation will fail")
        self._clear_indexes()

    def tearDown(self):
        self._clear_indexes()
        super(IndexManagementTestCase, self).tearDown()

    def test_create_primary(self):
        bucket_name = self.cluster_info.bucket_name
        self.mgr.create_primary_index(bucket_name, timeout=timedelta(seconds=60))

        # Ensure we can issue a query
        qstr = 'select * from {0} limit 1'.format(self.cluster_info.bucket_name)
        result = self.cluster.query(qstr).rows()
        # Drop the primary index
        self.mgr.drop_primary_index(bucket_name)
        # Ensure we get an error when executing the query
        self.assertRaises(
            CouchbaseException, lambda x: x.rows(), self.cluster.query(qstr))

    def test_create_named_primary(self):
        bucket_name = self.cluster_info.bucket_name
        ixname = 'namedPrimary'
        qstr = 'select * from {0} limit 1'.format(self.cluster_info.bucket_name)
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
        self.mgr.create_primary_index(bucket_name, ignore_if_exists=True)

        self.assertRaises(QueryIndexAlreadyExistsException, self.mgr.create_primary_index, bucket_name)

    def test_drop_primary_ignore_if_not_exists(self):
        bucket_name = self.cluster_info.bucket_name
        self.mgr.drop_primary_index(bucket_name, ignore_if_not_exists=True)
        self.assertRaises(QueryIndexNotFoundException, self.mgr.drop_primary_index, bucket_name)

    def test_create_named_primary_ignore_if_exists(self):
        ixname = 'namedPrimary'
        bucket_name = self.cluster_info.bucket_name
        self.mgr.create_index(bucket_name, ixname, [], primary=True)
        self.mgr.create_index(bucket_name, ixname, [], primary=True, ignore_if_exists=True)
        self.assertRaises(QueryIndexAlreadyExistsException, self.mgr.create_index, bucket_name,
                          ixname, [], primary=True)

    def test_drop_named_primary_ignore_if_exists(self):
        ixname = 'namedPrimary'
        bucket_name = self.cluster_info.bucket_name
        self.mgr.create_index(bucket_name, ixname, [], primary=True)
        self.mgr.drop_index(bucket_name, ixname)
        self.mgr.drop_index(bucket_name, ixname, ignore_missing=True)
        self.assertRaises(QueryIndexNotFoundException, self.mgr.drop_index, bucket_name, ixname)

    def test_create_secondary_indexes(self):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        bucket_name = self.cluster_info.bucket_name
        self.mgr._admin_bucket.timeout=10000
        self.mgr.create_index(bucket_name, ixname, fields=fields, timeout=timedelta(days=1))
        qq = 'select {1}, {2} from `{0}` where {1}=1 and {2}=2 limit 1'\
            .format(bucket_name, *fields)
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
            result=next(
                iter(x for x in self.mgr.get_all_indexes(bucket_name) if x.name==ixname), None)
            self.assertIsNotNone(result)
            return result
        result=self.try_n_times(10, 5, check_index)
        self.assertEqual(CONDITION,result.condition)

    def test_drop_secondary_indexes(self):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        bucket_name = self.cluster_info.bucket_name
        self.mgr.create_index(bucket_name, ixname, fields=fields, timeout=timedelta(days=1))

        qq = 'select {1}, {2} from `{0}` where {1}=1 and {2}=2 limit 1' \
            .format(bucket_name, *fields)

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
        bucket_name=self.cluster_info.bucket_name
        self.mgr.create_index(bucket_name, ixname, fields=['hello'])
        self.mgr.create_index(bucket_name, ixname, fields=['hello'], ignore_if_exists=True)
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
        self.assertRaises(QueryIndexNotFoundException, self.mgr.drop_index, bucket_name, ixname)

    def test_list_indexes(self):
        # start with no indexes
        ixs = list(self.mgr.get_all_indexes(self.cluster_info.bucket_name))
        self.assertEqual(0, len(ixs))

        # Create the primary index
        self.mgr.create_primary_index(self.cluster_info.bucket_name)
        ixs = list(self.mgr.get_all_indexes(self.cluster_info.bucket_name))  # type: List[QueryIndex]
        self.assertEqual(1, len(ixs))
        self.assertTrue(ixs[0].is_primary)
        self.assertEqual('#primary', ixs[0].name)
        self.assertEqual(self.cluster_info.bucket_name, ixs[0].keyspace)

    def test_deferred(self):
        # Create primary index
        self.mgr.create_primary_index(self.cluster_info.bucket_name, deferred=True)
        ix = self.mgr.get_all_indexes(self.cluster_info.bucket_name)
        self.assertEqual('deferred', next(iter(ix)).state)

        # Create a bunch of other indexes
        for n in range(5):
            self.mgr.create_index(self.cluster_info.bucket_name,
                'ix{0}'.format(n), fields=['fld{0}'.format(n)], deferred=True)

        ix = self.mgr.get_all_indexes(self.cluster_info.bucket_name)
        self.assertEqual(6, len(ix))

        pending = self.mgr.build_deferred_indexes(self.cluster_info.bucket_name)
        self.assertEqual(6, len(pending))
        self.mgr.watch_indexes(self.cluster_info.bucket_name, pending)  # Should be OK
        self.mgr.watch_indexes(self.cluster_info.bucket_name, pending)  # Should be OK again
        self.assertRaises(QueryIndexNotFoundException, self.mgr.watch_indexes, self.cluster_info.bucket_name, ['nonexist'])
