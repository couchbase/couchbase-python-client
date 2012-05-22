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

import unittest
import types
import warnings

from warnings_catcher import setup_warning_catcher
from testconfig import config
from couchbase.client import *


class ClientTest(unittest.TestCase):


    def setUp(self):
        self.host = config['node-1']['host']
        self.port = config['node-1']['port']
        self.username = config['node-1']['username']
        self.password = config['node-1']['password']


    def tearDown(self):
        pass

    def test_couchbase_object_construction(self):
        cb = Couchbase(self.host + ':' + self.port, self.username, self.password)
        self.assertTrue(isinstance(cb.servers, types.ListType))

    def test_server_object_construction(self):
        w = setup_warning_catcher()
        warnings.simplefilter("always")
        cb = Server(self.host + ':' + self.port, self.username, self.password)
        self.assertTrue(isinstance(cb.servers, types.ListType))
        self.assertTrue(len(w) == 1)
        self.assertTrue("deprecated" in str(w[-1].message))



if __name__ == "__main__":
    unittest.main()
