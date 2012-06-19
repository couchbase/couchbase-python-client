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

import sys
import time
from nose.plugins.attrib import attr
from couchbase.couchbaseclient import *
from couchbase.exception import *
from couchbase.tests.test_memcachedclient import MemcachedClientTest


class CouchbaseClientTest(MemcachedClientTest):
    def setUp(self):
        MemcachedClientTest.setUp(self)
        self.client = CouchbaseClient(self.url, self.bucket_name, "", True)

    def tearDown(self):
        self.client.flush()
        self.client.done()

if __name__ == '__main__':
    unittest.main()
