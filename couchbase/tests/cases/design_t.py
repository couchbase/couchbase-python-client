0#
# Copyright 2013, Couchbase, Inc.
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
import time

from nose.plugins.attrib import attr

from couchbase.tests.base import DDocTestCase
from couchbase.exceptions import HTTPError, CouchbaseError

DNAME = "tmp"
VNAME = "a_view"

DESIGN_JSON = {
    'language' : 'javascript',
    'views' : {
        VNAME : {
            'map' : "function(doc) { emit(null,null); }"
        }
    }
}

@attr('slow')
class DesignDocManagementTest(DDocTestCase):
    def setUp(self):
        super(DesignDocManagementTest, self).setUp()
        self.skipIfMock()
        self.mgr = self.cb.bucket_manager()

        try:
            self.mgr.design_delete(DNAME, use_devmode=False, syncwait=5)
        except HTTPError:
            pass

        try:
            self.mgr.design_delete(DNAME, use_devmode=True, syncwait=5)
        except HTTPError:
            pass

    def tearDown(self):
        del self.mgr
        super(DesignDocManagementTest, self).tearDown()


    def test_design_management(self):
        rv = self.mgr.design_create(
            DNAME, DESIGN_JSON, use_devmode=True, syncwait=5)
        self.assertTrue(rv.success)

        rv = self.cb._view(DNAME, VNAME, use_devmode=True,
                           params = { 'limit':10 })
        print(rv)
        self.assertTrue(rv.success)

        rv = self.mgr.design_publish(DNAME, syncwait=5)
        self.assertTrue(rv.success)

        rv = self.cb._view(DNAME, VNAME, use_devmode=False,
                           params = { 'limit':10 })
        self.assertTrue(rv.success)

        self.assertRaises(HTTPError,
                          self.cb._view,
                          DNAME, VNAME,
                          use_devmode=True)

        rv = self.mgr.design_delete(DNAME, use_devmode=False, syncwait=5)
        self.assertTrue(rv.success)

        self.assertRaises(HTTPError,
                          self.cb._view,
                          DNAME, VNAME,
                          use_devmode=False)

    def test_design_headers(self):
        rv = self.mgr.design_create(DNAME, DESIGN_JSON, use_devmode=True,
                                   syncwait=5)

        rv = self.mgr.design_get(DNAME, use_devmode=True)
        self.assertTrue(rv.headers)
        print(rv.headers)
        self.assertTrue('X-Couchbase-Meta' in rv.headers)
