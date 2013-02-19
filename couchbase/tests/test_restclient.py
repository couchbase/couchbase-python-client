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
import json
import uuid
import warnings

from testconfig import config
from nose.tools import nottest
from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest

from couchbase.vbucketawareclient import VBucketAwareClient
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

    @nottest
    def setup_rest_helper(self):
        self.rest_helper = RestHelper(self.rest)

    @attr(cbv="1.8.0")
    def test_rest_helper_object_creation(self):
        self.setup_rest_helper()
        self.assertEqual(self.rest, self.rest_helper.rest)

    @attr(cbv="1.8.0")
    def test_is_ns_server_running(self):
        self.setup_rest_helper()
        self.assertTrue(self.rest_helper.is_ns_server_running())

    @attr(cbv="1.8.0")
    def test_is_cluster_healthy(self):
        self.setup_rest_helper()
        self.assertTrue(self.rest_helper.is_cluster_healthy())

    @attr(cbv="1.8.0")
    def test_vbucket_map_ready(self):
        self.setup_rest_helper()
        self.assertTrue(self.rest_helper.vbucket_map_ready(self.bucket_name))

    @attr(cbv="1.8.0")
    def test_bucket_exists(self):
        self.setup_rest_helper()
        self.assertTrue(self.rest_helper.bucket_exists(self.bucket_name))
        self.assertFalse(self.rest_helper.bucket_exists(str(uuid.uuid4())))

    @attr(cbv="1.8.0")
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
        # list of design_docs for tearDown to destroy, if they've hung around
        self.design_docs = []

    def tearDown(self):
        for ddoc in self.design_docs:
            self.rest.delete_design_doc(self.bucket_name, ddoc)
        pass

    @nottest
    def setup_rest_connection(self):
        server_info = {"ip": self.host,
                       "port": self.port,
                       "username": self.username,
                       "password": self.password}
        self.rest = RestConnection(server_info)

    @nottest
    def setup_create_design_doc(self):
        self.setup_rest_connection()
        if self.rest.couch_api_base is None:
            raise SkipTest
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
        self.design_docs.append(ddoc_name)
        return ddoc_name, resp

    @nottest
    def setup_vbucketawareclient(self):
        self.client = VBucketAwareClient(self.host, 11210)

    @attr(cbv="1.8.0")
    def test_rest_connection_object_creation(self):
        self.setup_rest_connection()
        self.assertEqual(self.rest.base_url,
                         "http://{0}:{1}".format(self.host, self.port))

    @attr(cbv="1.8.0")
    def test_rest_connection_object_creation_with_server_object(self):
        class ServerInfo:
            ip = self.host
            port = self.port
            rest_username = self.username
            rest_password = self.password

        rest = RestConnection(ServerInfo())
        self.assertEqual(rest.base_url,
                         "http://{0}:{1}".format(self.host, self.port))

    @attr(cbv="2.0.0")
    def test_create_design_doc(self):
        (ddoc_name, resp) = self.setup_create_design_doc()
        self.assertTrue(resp["ok"])

    @attr(cbv="2.0.0")
    def test_get_design_doc(self):
        ddoc_name, resp = self.setup_create_design_doc()
        ddoc = self.rest.get_design_doc(self.bucket_name, ddoc_name)
        self.assertIn("views", ddoc.keys())
        self.assertRaises(Exception, self.rest.get_design_doc,
                          (self.bucket_name, str(uuid.uuid4())))

    @attr(cbv="2.0.0")
    def test_delete_design_doc(self):
        ddoc_name, resp = self.setup_create_design_doc()
        self.assertTrue(self.rest.delete_design_doc(self.bucket_name,
                                                    ddoc_name))
        self.assertRaises(Exception,
                          self.rest.delete_design_doc,
                          (self.bucket_name, ddoc_name))
        self.design_docs = filter(lambda id: id is not ddoc_name,
                                  self.design_docs)

    @attr(cbv="2.0.0")
    def test_get_view(self):
        (ddoc_name, resp) = self.setup_create_design_doc()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            # Trigger a warning.
            self.rest.get_view(self.bucket_name, ddoc_name, "testing")
            # Verify some things
            self.assertTrue(len(w) == 1)
            self.assertTrue("deprecated" in str(w[-1].message))

    @attr(cbv="2.0.0")
    def test_view_results(self):
        ddoc_name, resp = self.setup_create_design_doc()
        view = self.rest.view_results(self.bucket_name, ddoc_name, "testing",
                                      {})
        if "error" in view:
            self.fail(view)
        else:
            self.assertIn("rows", view.keys())
        # let's add some sample docs
        self.setup_vbucketawareclient()
        kvs = [(str(uuid.uuid4()), str(uuid.uuid4())) for i in range(0, 100)]
        for k, v in kvs:
            self.client.set(k, 0, 0, v)
        # rerun the view
        view = self.rest.view_results(self.bucket_name, ddoc_name, "testing",
                                      {})
        if "error" in view:
            self.fail(view)
        else:
            self.assertIn("rows", view.keys())
        # remove sample docs
        for k, v in kvs:
            self.client.delete(k)

    @attr(cbv="1.8.0")
    def test_create_headers(self):
        self.setup_rest_connection()
        headers = self.rest._create_headers()
        self.assertEqual(headers['Content-Type'],
                         'application/x-www-form-urlencoded')

    @attr(cbv="1.0.0")
    def test_create_bucket(self):
        self.setup_rest_connection()
        # test membase/couchbase creation defaults
        status = self.rest.create_bucket('newbucket')
        self.assertTrue(status)
        bucket = self.rest.get_bucket('newbucket')
        self.assertEqual(bucket.stats.ram / 1024 / 1024, 100)
        self.assertEqual(bucket.authType, 'sasl')
        self.assertEqual(bucket.type, 'membase')
        self.assertEqual(bucket.numReplicas, 1)
        self.rest.delete_bucket('newbucket')
        # test memcached creation defaults
        status = self.rest.create_bucket(bucket='newbucket',
                                         bucketType='memcached')
        self.assertTrue(status)
        bucket = self.rest.get_bucket('newbucket')
        self.assertEqual(bucket.stats.ram / 1024 / 1024, 100)
        self.assertEqual(bucket.authType, 'sasl')
        self.assertEqual(bucket.type, 'memcached')
        self.assertEqual(bucket.numReplicas, 0)
        # make sure creating an existing bucket fails properly
        self.assertRaises(Exception, self.rest.create_bucket,
                          bucket='newbucket', bucketType='memcached')
        self.rest.delete_bucket('newbucket')
        # test setting ramQuotaMB too low
        self.assertRaises(AssertionError,
                          self.rest.create_bucket,
                          bucket='newbucket', bucketType='memcached',
                          ramQuotaMB=10)
        self.assertRaises(AssertionError,
                          self.rest.create_bucket,
                          bucket='newbucket', ramQuotaMB=90)

if __name__ == "__main__":
    unittest.main()
