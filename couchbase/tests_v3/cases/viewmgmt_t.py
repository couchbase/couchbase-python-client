#
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
#

from couchbase.management.views import DesignDocumentNamespace, View, DesignDocument, DesignDocumentNotFoundException, \
    GetDesignDocumentOptions, GetAllDesignDocumentsOptions, PublishDesignDocumentOptions
from couchbase_tests.base import ClusterTestCase
from couchbase.exceptions import HTTPException
from datetime import timedelta


class DesignDocManagementTest(ClusterTestCase):
    DOCNAME = 'tmp'
    VIEW = View(map="function(doc){emit(null, null);}")
    DOC = DesignDocument(name=DOCNAME, views={"myview": VIEW})

    def setUp(self):
        super(DesignDocManagementTest, self).setUp()
        self.skipIfMock()
        self.mgr = self.bucket.view_indexes()

        # insist that it exists in the development namespace
        self.mgr.upsert_design_document(self.DOC, DesignDocumentNamespace.DEVELOPMENT)
        # and be sure to drop it from the production namespace
        try:
            self.mgr.drop_design_document(self.DOCNAME, DesignDocumentNamespace.PRODUCTION)
        except HTTPException:
            pass
        # now wait till we are sure the design doc is there
        self.try_n_times(10, 3, self.mgr.get_design_document, self.DOCNAME,
                         DesignDocumentNamespace.DEVELOPMENT)
        # and that it isn't in production
        self.try_n_times_till_exception(10, 3, self.mgr.get_design_document,
                                        self.DOCNAME, DesignDocumentNamespace.PRODUCTION)
        self.cb = self.bucket

    def tearDown(self):
        super(DesignDocManagementTest, self).tearDown()

    def test_get_design_document_fail(self):
        self.assertRaises(DesignDocumentNotFoundException,
                          self.mgr.get_design_document,
                          self.DOCNAME,
                          DesignDocumentNamespace.PRODUCTION,
                          GetDesignDocumentOptions(timeout=timedelta(seconds=5)))

    def test_get_design_document(self):
        ddoc = self.mgr.get_design_document(self.DOCNAME, DesignDocumentNamespace.DEVELOPMENT,
                                            timeout=timedelta(seconds=5))
        self.assertIsNotNone(ddoc)
        self.assertEqual(ddoc.name, self.DOCNAME)

    def test_get_all_design_documents(self):
        # should start out in _some_ state.  Since we don't know for sure, but we
        # do know it does have self.DOCNAME in it in development ONLY, lets assert on that and that
        # it succeeds, meaning we didn't get an exception.
        result = self.mgr.get_all_design_documents(DesignDocumentNamespace.DEVELOPMENT,
                                                   GetAllDesignDocumentsOptions(timeout=timedelta(seconds=10)))
        names = [doc.name for doc in result if doc.name == self.DOCNAME]
        self.assertTrue(names.count(self.DOCNAME) > 0)

    def test_get_all_design_documents_excludes_namespaces(self):
        # we know the self.DOCNAME is _only_ in development, so...
        result = self.mgr.get_all_design_documents(DesignDocumentNamespace.PRODUCTION)
        names = [doc.name for doc in result if doc.name == self.DOCNAME]
        self.assertEqual(0, names.count(self.DOCNAME))

    def test_upsert_design_doc(self):
        # we started with this already in here, so this isn't really necessary...`
        self.mgr.upsert_design_document(self.DOC, DesignDocumentNamespace.DEVELOPMENT)
        self.try_n_times(10, 3, self.mgr.get_design_document, self.DOCNAME, DesignDocumentNamespace.DEVELOPMENT)

    def test_drop_design_doc(self):
        self.mgr.drop_design_document(self.DOCNAME, DesignDocumentNamespace.DEVELOPMENT)
        self.try_n_times_till_exception(10, 3, self.mgr.get_design_document, self.DOCNAME,
                                        DesignDocumentNamespace.DEVELOPMENT,
                                        GetDesignDocumentOptions(timeout=timedelta(seconds=10)))

    def test_drop_design_doc_fail(self):
        self.assertRaises(DesignDocumentNotFoundException, self.mgr.drop_design_document,
                          self.DOCNAME, DesignDocumentNamespace.PRODUCTION)

    def test_publish_design_doc(self):
        # starts off not in prod
        self.assertRaises(DesignDocumentNotFoundException, self.mgr.get_design_document,
                          self.DOCNAME, DesignDocumentNamespace.PRODUCTION)
        self.mgr.publish_design_document(self.DOCNAME, PublishDesignDocumentOptions(timeout=timedelta(seconds=10)))
        # should be in prod now
        self.try_n_times(10, 3, self.mgr.get_design_document, self.DOCNAME, DesignDocumentNamespace.PRODUCTION)
        # and still in dev
        self.try_n_times(10, 3, self.mgr.get_design_document, self.DOCNAME, DesignDocumentNamespace.DEVELOPMENT)
