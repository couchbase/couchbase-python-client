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
from testconfig import config
from nose.plugins.attrib import attr
from couchbase.rest_client import RestConnection, RestHelper


class RestClientTest(unittest.TestCase):
    def setUp(self):
        self.host = config['node-1']['host']
        self.port = config['node-1']['port']
        self.username = config['node-1']['username']
        self.password = config['node-1']['password']

    def tearDown(self):
        pass

    @attr(cbv="2.0.0")
    def test_rest_client_object_creation(self):
        server_info = {"ip": self.host,
                       "port": self.port,
                       "username": self.username,
                       "password": self.password}
        rest = RestConnection(server_info)
        self.assertEqual(rest.baseUrl, "http://%s:%s/" %
                         (self.host, self.port))


class RestHelperTest(unittest.TestCase):
    def setUp(self):
        self.host = config['node-1']['host']
        self.port = config['node-1']['port']
        self.username = config['node-1']['username']
        self.password = config['node-1']['password']
        self.bucket_name = config['node-1']['bucket']

        server_info = {"ip": self.host,
                       "port": self.port,
                       "username": self.username,
                       "password": self.password}
        self.rest = RestConnection(server_info)
        self.rest_helper = None

    def tearDown(self):
        pass

    @attr(cbv="2.0.0")
    def setup_rest_helper(self):
        self.rest_helper = RestHelper(self.rest)

    @attr(cbv="2.0.0")
    def test_rest_helper_object_creation(self):
        self.setup_rest_helper()
        self.assertEqual(self.rest, self.rest_helper.rest)

    @attr(cbv="2.0.0")
    def test_is_ns_server_running(self):
        self.setup_rest_helper()
        self.assertTrue(self.rest_helper.is_ns_server_running())

    @attr(cbv="2.0.0")
    def test_is_cluster_healthy(self):
        self.setup_rest_helper()
        self.assertTrue(self.rest_helper.is_cluster_healthy())

    @attr(cbv="2.0.0")
    def test_vbucket_map_ready(self):
        self.setup_rest_helper()
        self.assertTrue(self.rest_helper.vbucket_map_ready(self.bucket_name))

if __name__ == "__main__":
    unittest.main()
