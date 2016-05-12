# Copyright 2016, Couchbase, Inc.
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

from couchbase.tests.base import RealServerTestCase, SkipTest
from couchbase.bucketmanager import BucketManager
from couchbase.bucket import Bucket
import couchbase.exceptions as E


class IndexManagementTestCase(RealServerTestCase):
    def _clear_indexes(self):
        # Drop all indexes!
        cb = self.cb
        mgr = cb.bucket_manager()  # type: BucketManager
        for index in mgr.n1ql_index_list():
            mgr.n1ql_index_drop(index)

    def setUp(self):
        super(IndexManagementTestCase, self).setUp()
        if self.cb.__class__ is not Bucket:
            raise SkipTest('Only supported for synchronous bucket')

        import couchbase._libcouchbase as C
        if not hasattr(C, 'LCB_N1XSPEC_F_DEFER'):
            raise SkipTest('LCB headers are too old!')

        self._clear_indexes()

    def tearDown(self):
        self._clear_indexes()
        super(IndexManagementTestCase, self).tearDown()

    def test_create_primary(self):
        cb = self.cb
        mgr = cb.bucket_manager()  # type: BucketManager
        mgr.n1ql_index_create_primary()
        # Ensure we can issue a query
        qstr = 'select * from {0} limit 1'.format(cb.bucket)
        cb.n1ql_query(qstr).execute()
        # Drop the primary index
        mgr.n1ql_index_drop_primary()
        # Ensure we get an error when executing the query
        self.assertRaises(
            E.CouchbaseError, lambda x: x.execute(), cb.n1ql_query(qstr))

        ixname = 'namedPrimary'
        # Try to create a _named_ primary index
        mgr.n1ql_index_create(ixname, primary=True)
        cb.n1ql_query(qstr).execute()
        # All OK
        mgr.n1ql_index_drop(ixname)
        self.assertRaises(
            E.CouchbaseError, lambda x: x.execute(), cb.n1ql_query(qstr))

        # Create the primary index the first time
        mgr.n1ql_index_create_primary()
        mgr.n1ql_index_create_primary(ignore_exists=True)
        self.assertRaises(E.KeyExistsError, mgr.n1ql_index_create_primary)

        # Drop the indexes
        mgr.n1ql_index_drop_primary()
        mgr.n1ql_index_drop_primary(ignore_missing=True)
        self.assertRaises(E.NotFoundError, mgr.n1ql_index_drop_primary)

        # Test with _named_ primaries
        mgr.n1ql_index_create(ixname, primary=True)
        mgr.n1ql_index_create(ixname, primary=True, ignore_exists=True)
        self.assertRaises(E.KeyExistsError, mgr.n1ql_index_create,
                          ixname, primary=True)

        mgr.n1ql_index_drop(ixname)
        mgr.n1ql_index_drop(ixname, ignore_missing=True)
        self.assertRaises(E.NotFoundError, mgr.n1ql_index_drop, ixname)

    def test_alt_indexes(self):
        cb = self.cb  # type: Bucket
        mgr = cb.bucket_manager()
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        mgr.n1ql_index_create(ixname, fields=fields)
        qq = 'select {1}, {2} from `{0}` where {1}=1 and {2}=2 limit 1'\
            .format(cb.bucket, *fields)
        cb.n1ql_query(qq).execute()

        # Drop the index
        mgr.n1ql_index_drop(ixname)
        # Issue the query again
        self.assertRaises(E.CouchbaseError,
                          lambda x: x.execute(), cb.n1ql_query(qq))

        self.assertRaises((ValueError, TypeError),
                          mgr.n1ql_index_create, 'withoutFields')
        mgr.n1ql_index_create(ixname, fields=['hello'])
        mgr.n1ql_index_create(ixname, fields=['hello'], ignore_exists=True)
        self.assertRaises(E.KeyExistsError, mgr.n1ql_index_create,
                          ixname, fields=['hello'])

        # Drop it
        mgr.n1ql_index_drop(ixname)
        mgr.n1ql_index_drop(ixname, ignore_missing=True)
        self.assertRaises(E.NotFoundError, mgr.n1ql_index_drop, ixname)

        # Create an index with a condition
        ixname = 'ix_with_condition'
        cond = '((`foo` = "foo") and (`bar` = "bar"))'
        mgr.n1ql_index_create(ixname, fields=['foo'], condition=cond)
        ll = filter(lambda x: x.name == ixname, mgr.n1ql_index_list())
        self.assertTrue(ll)
        self.assertEqual(cond, ll[0].condition)

    def test_list_indexes(self):
        cb = self.cb  # type: Bucket
        mgr = cb.bucket_manager()
        ixs = mgr.n1ql_index_list()
        self.assertEqual(0, len(ixs))

        # Create the primary index
        mgr.n1ql_index_create_primary()
        ixs = mgr.n1ql_index_list()
        self.assertEqual(1, len(ixs))
        self.assertTrue(ixs[0].primary)
        self.assertEqual('#primary', ixs[0].name)
        self.assertEqual(cb.bucket, ixs[0].keyspace)

    def test_deferred(self):
        cb = self.cb  # type: Bucket
        mgr = cb.bucket_manager()
        # Create primary index
        mgr.n1ql_index_create_primary(defer=True)
        ix = mgr.n1ql_index_list()[0]
        self.assertEqual('deferred', ix.state)

        # Create a bunch of other indexes
        for n in range(5):
            mgr.n1ql_index_create(
                'ix{0}'.format(n), fields=['fld{0}'.format(n)], defer=True)

        ix = mgr.n1ql_index_list()
        self.assertEqual(6, len(ix))

        pending = mgr.n1ql_index_build_deferred()
        self.assertEqual(6, len(pending))
        mgr.n1ql_index_watch(pending)  # Should be OK
        mgr.n1ql_index_watch(pending)  # Should be OK again
        self.assertRaises(E.NotFoundError, mgr.n1ql_index_watch, ['nonexist'])