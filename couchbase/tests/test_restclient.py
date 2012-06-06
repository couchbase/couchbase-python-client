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
try:
    import json
except:
    import simplejson as json
import uuid
from testconfig import config
from nose.plugins.attrib import attr
from couchbase.rest_client import RestConnection, RestHelper


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

    @attr(cbv="2.0.0")
    def test_bucket_exists(self):
        self.setup_rest_helper()
        self.assertTrue(self.rest_helper.bucket_exists(self.bucket_name))
        self.assertFalse(self.rest_helper.bucket_exists(str(uuid.uuid4())))

    @attr(cbv="2.0.0")
    def test_all_nodes_replicated(self):
        self.setup_rest_helper()
        self.assertTrue(self.rest_helper.all_nodes_replicated(debug=True))


class RestConnectionTest(unittest.TestCase):
    def setUp(self):
        self.host = config['node-1']['host']
        self.port = config['node-1']['port']
        self.username = config['node-1']['username']
        self.password = config['node-1']['password']
        self.bucket_name = config['node-1']['bucket']

    def tearDown(self):
        pass

    @attr(cbv="2.0.0")
    def setup_rest_connection(self):
        server_info = {"ip": self.host,
                       "port": self.port,
                       "username": self.username,
                       "password": self.password}
        self.rest = RestConnection(server_info)

    @attr(cbv="2.0.0")
    def test_rest_connection_object_creation(self):
        self.setup_rest_connection()
        self.assertEqual(self.rest.baseUrl, "http://%s:%s/" %
                         (self.host, self.port))

    @attr(cbv="2.0.0")
    def test_rest_connection_object_creation_with_server_object(self):
        class ServerInfo:
            ip = self.host
            port = self.port
            rest_username = self.username
            rest_password = self.password

        rest = RestConnection(ServerInfo())
        self.assertEqual(rest.baseUrl, "http://%s:%s/" % (self.host,
                                                          self.port))

    @attr(cbv="2.0.0")
    def test_create_design_doc(self):
        self.setup_rest_connection()
        ddoc_name = uuid.uuid4()
        design_doc = json.dumps({"views":
                      {"testing":
                       {"map":
                        "function(doc) { emit(doc._id, null); }"
                        }
                       }
                      })
        resp = self.rest.create_design_doc(self.bucket_name, ddoc_name,
                                           design_doc)
        self.assertTrue(resp["ok"])

        # Cleanup: delete the design doc; just gonna assume this worked
        self.rest.delete_design_doc(self.bucket_name, ddoc_name)

if __name__ == "__main__":
    unittest.main()
