# Copyright 2020, Couchbase, Inc.
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
from flaky import flaky

from couchbase_tests.base import CollectionTestCase, SkipTest, skip_if_no_collections
from couchbase.exceptions import InvalidArgumentException, SearchIndexNotFoundException
from couchbase.management.search import SearchIndex
import uuid


@flaky(10,3)
class SearchIndexManagerTestCase(CollectionTestCase):
    def setUp(self):
        super(SearchIndexManagerTestCase, self).setUp()
        if not self.is_realserver:
            raise SkipTest('no management tests for mock')

        self.indexmgr = self.cluster.search_indexes()
        self.assertIsNotNone(self.indexmgr)
        self.indexname = "idx-{}".format(str(uuid.uuid4()))
        try:
            self.indexmgr.drop_index(self.indexname)
        except SearchIndexNotFoundException:
            # maybe it isn't there, that's ok
            pass
        finally:
            # now lets wait till it really seems gone...
            self.try_n_times_till_exception(10, 3, self.indexmgr.get_index, self.indexname)

            # Now lets create a new one
            self.indexmgr.upsert_index(SearchIndex(name=self.indexname, source_name='default'))

            # insure it is there before we begin test
            self.try_n_times(10, 3, self.indexmgr.get_index, self.indexname)

            # seems strange, but even that above can still lead to a INDEX_NOT_FOUND in
            # jenkins at least.  Perhaps the http round-robins the calls and hits a machine
            # on the cluster that has not gotten it yet?  <sigh>
            # A guess here - maybe we need to have it succeed once per server (assuming it
            # round-robins.  HACK - lets just see...
            self.try_n_times(10, 3, self.indexmgr.get_index, self.indexname)
            self.try_n_times(10, 3, self.indexmgr.get_index, self.indexname)

    def tearDown(self):
        try:
            self.indexmgr.drop_index(self.indexname)
        except:
            pass

    def test_ingestion_control(self):
        # can't easily test this, but lets at least call them and insure we get no
        # exceptions
        self.assertIsNone(self.try_n_times(10, 3, self.indexmgr.pause_ingest, self.indexname))
        self.assertIsNone(self.try_n_times(10, 3, self.indexmgr.resume_ingest, self.indexname))

    def test_query_control(self):
        self.assertIsNone(self.try_n_times(10, 3, self.indexmgr.disallow_querying, self.indexname))
        self.assertIsNone(self.try_n_times(10, 3, self.indexmgr.allow_querying, self.indexname))

    def test_plan_freeze_control(self):
        self.assertIsNone(self.try_n_times(10, 3, self.indexmgr.freeze_plan, self.indexname))
        self.assertIsNone(self.try_n_times(10, 3, self.indexmgr.unfreeze_plan, self.indexname))

    def test_get_indexed_document_count(self):
        # just be sure we get something back.  NOTE: immediately after creation,
        # the document count can give an exception.  So...  lets try a few times
        # with a sleep.
        self.assertIsNotNone(self.try_n_times(5, 2, self.indexmgr.get_indexed_documents_count, self.indexname))

    def test_drop_index(self):
        # you may not be able to drop an index immediately after creating it, so
        # lets retry it till successful.
        self.try_n_times(10, 3, self.indexmgr.drop_index, self.indexname)
        self.try_n_times_till_exception(10, 3, self.indexmgr.get_index, self.indexname)
        self.assertRaises(SearchIndexNotFoundException, self.indexmgr.get_index, self.indexname)

    def test_get_all_indexes(self):
        # we know of one, lets make sure it is in the list
        indexes = self.try_n_times(10, 3, self.indexmgr.get_all_indexes)
        for idx in indexes:
            if idx.name == self.indexname:
                return;
        self.fail('did not find {} as expected'.format(self.indexname))

    def test_get_index(self):
        index = self.try_n_times(10, 3, self.indexmgr.get_index, self.indexname)
        self.assertIsNotNone(index)

    def test_get_index_fail_no_index_name(self):
        self.assertRaises(InvalidArgumentException, self.indexmgr.get_index, None)

    def test_get_index_fail(self):
        self.assertRaises(SearchIndexNotFoundException, self.indexmgr.get_index, 'foo')

    def test_upsert_index(self):
        index = self.try_n_times(10, 3, self.indexmgr.get_index, self.indexname)
        self.assertIsNone(
            self.indexmgr.upsert_index(SearchIndex(uuid=index.uuid, name=self.indexname, source_name='default')))

    @skip_if_no_collections
    def test_analyze_doc(self):
        # like getting the doc count, this can fail immediately after index creation
        doc = {"field": "I got text in here"}
        analysis = self.try_n_times(5, 2, self.indexmgr.analyze_document, self.indexname, doc)
        self.assertIsNotNone(analysis)
        self.assertEquals(analysis['status'], 'ok')
