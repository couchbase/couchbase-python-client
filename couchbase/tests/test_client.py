#
# Copyright 2012, Couchbase, Inc.
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

try:
    import unittest2 as unittest
except ImportError:
    import unittest
import types
import warnings

from warnings_catcher import setup_warning_catcher
from testconfig import config
from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest
from nose.tools import nottest
from couchbase.client import *
from couchbase.couchbaseclient import *


class ClientTest(unittest.TestCase):
    def setUp(self):
        self.host = config['node-1']['host']
        self.port = config['node-1']['port']
        self.username = config['node-1']['username']
        self.password = config['node-1']['password']
        self.bucket_name = config['node-1']['bucket']

    def tearDown(self):
        pass

    @nottest
    def setup_cb(self):
        self.cb = Couchbase(self.host + ':' + self.port,
                            self.username, self.password)

    @attr(cbv="1.0.0")
    def test_couchbase_object_construction(self):
        cb = Couchbase(self.host + ':' + self.port, self.username,
                       self.password)
        self.assertIsInstance(cb.servers, types.ListType)

    @attr(cbv="1.0.0")
    def test_server_object_construction(self):
        w = setup_warning_catcher()
        warnings.simplefilter("always")
        cb = Server(self.host + ':' + self.port, self.username, self.password)
        self.assertIsInstance(cb.servers, types.ListType)
        self.assertTrue(len(w) == 1)
        self.assertTrue("deprecated" in str(w[-1].message))

    @attr(cbv="1.0.0")
    def test_couchbase_object_construction_without_port(self):
        if self.port != "8091":
            raise SkipTest
        cb = Couchbase(self.host, self.username, self.password)
        self.assertIsInstance(cb.servers, types.ListType)

    @attr(cbv="1.0.0")
    def test_vbucketawarecouchbaseclient_object_construction(self):
        w = setup_warning_catcher()
        warnings.simplefilter("always")
        cb = VBucketAwareCouchbaseClient("http://" + self.host + ':'
                                         + self.port + "/pools/default",
                                         self.bucket_name, "")
        self.assertIsInstance(cb.servers, types.ListType)
        self.assertTrue(len(w) == 1)
        self.assertTrue("deprecated" in str(w[-1].message))

    @attr(cbv="1.0.0")
    def test_bucket(self):
        self.setup_cb()
        self.assertIsInstance(self.cb.bucket(self.bucket_name), Bucket)

    @attr(cbv="1.0.0")
    def test_buckets(self):
        self.setup_cb()
        buckets = self.cb.buckets()
        self.assertIsInstance(buckets, types.ListType)
        self.assertIsInstance(buckets[0], Bucket)

    @attr(cbv="1.0.0")
    def test_create(self):
        self.setup_cb()
        bucket_name = str(uuid.uuid4())
        bucket = self.cb.create(bucket_name)
        self.assertIsInstance(bucket, Bucket)
        exists = [b for b in self.cb.buckets() if b.bucket_name == bucket_name]
        self.assertTrue(len(exists))
        self.cb.delete(bucket_name)

if __name__ == "__main__":
    unittest.main()
