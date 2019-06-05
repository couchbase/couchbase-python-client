#
# Copyright 2017, Couchbase, Inc.
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
import unittest

from couchbase_v2.exceptions import (CouchbaseError)
from couchbase_tests.base import CouchbaseTestCase
from couchbase_core.auth_domain import AuthDomain
import sys
import time
from nose import SkipTest

class EnhancedErrorTest(CouchbaseTestCase):
    def setUp(self):
        super(EnhancedErrorTest, self).setUp()

        if not self._realserver_info:
            raise SkipTest("Need real server")

        self.admin = self.make_admin_connection()

    @property
    def cluster_info(self):
        return self.realserver_info

    def tearDown(self):
        super(EnhancedErrorTest, self).tearDown()
        if self.should_check_refcount:
            rc = sys.getrefcount(self.admin)
            #TODO: revise GC handling - broken on Mac
            #self.assertEqual(rc, 2)

        del self.admin

    def test_enhanced_err_present_authorisation(self):
        import couchbase_core.subdocument as SD
        users=[('writer',('s3cr3t',[('data_reader', 'default'), ('data_writer', 'default')])),
              ('reader',('s3cr3t',[('data_reader', 'default')]))]
        #self.mockclient._do_request("SET_ENHANCED_ERRORS",{"enabled":True})
        for user in users:
            print(str(user))
            (userid, password, roles) = user[0],user[1][0],user[1][1]
            # add user
            self.admin.user_upsert(AuthDomain.Local, userid, password, roles)
            time.sleep(1)
            try:
                connection = self.make_connection(username=userid,password=password)

                key = self.gen_key('create_doc')
                connection.mutate_in(key, SD.upsert('new.path', 'newval'), upsert_doc=True)
            except CouchbaseError as e:
                print(str(e))
                if userid=="writer":
                    raise e
                else:
                    self.assertRegexpMatches(e.context,r".*Authorization failure.*","doesn't have correct Context field")
                    self.assertRegexpMatches(e.ref,r"(.*?)-(.*?)-.*","doesn't have correct Ref field")
                    self.assertRegexpMatches(str(e),r".*Context=Authorization failure.*,.*Ref=.*","exception as string doesn't contain both fields")
            finally:
                #remove user
                self.admin.user_remove(AuthDomain.Local, userid)

if __name__ == "__main__":
    unittest.main()