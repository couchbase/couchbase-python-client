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

import types
import warnings
import uuid
import time
import json
from collections import Set

from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest
from nose.tools import nottest

from couchbase.client import Couchbase, Server, Bucket, DesignDoc, View
from couchbase.couchbaseclient \
    import CouchbaseClient, VBucketAwareCouchbaseClient
from couchbase.tests.base import Base
from couchbase.exception import MemcachedError


class ServerTest(Base):
    @attr(cbv="1.0.0")
    def test_server_object_construction(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            # Trigger a warning.
            cb = Server(self.host + ':' + self.port, self.username,
                        self.password)
            self.assertIsInstance(cb.servers, types.ListType)
            # Verify some things
            self.assertTrue(len(w) == 1)
            self.assertTrue("deprecated" in str(w[-1].message))


class CouchbaseTest(Base):
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
    def test_couchbase_object_construction_without_port(self):
        if self.port != "8091":
            raise SkipTest
        cb = Couchbase(self.host, self.username, self.password)
        self.assertIsInstance(cb.servers, types.ListType)

    @attr(cbv="1.0.0")
    def test_vbucketawarecouchbaseclient_object_construction(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            # Trigger a warning.
            cb = VBucketAwareCouchbaseClient("http://" + self.host + ':'
                                             + self.port + "/pools/default",
                                             self.bucket_name, "")
            self.assertIsInstance(cb.servers, types.ListType)
            # Verify some things
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
        exists = [b for b in self.cb.buckets() if b.name == bucket_name]
        self.assertTrue(len(exists))
        self.cb.delete(bucket_name)

    @attr(cbv="1.0.0")
    def test_delete(self):
        self.setup_cb()
        bucket_name = str(uuid.uuid4())
        bucket = self.cb.create(bucket_name)
        self.assertIsInstance(self.cb[bucket_name], Bucket)
        self.cb.delete(bucket_name)
        self.assertNotIn(bucket_name, self.cb)


class BucketTest(Base):
    def setUp(self):
        super(BucketTest, self).setUp()
        self.cb = Couchbase(self.host + ':' + self.port, self.username,
                            self.password)
        self.client = self.cb[self.bucket_name]

    @attr(cbv="1.0.0")
    def test_bucket_object_creation(self):
        cb = Couchbase(self.host + ':' + self.port, self.username,
                       self.password)
        bucket = Bucket(self.bucket_name, cb)
        self.assertIsInstance(bucket.server, Couchbase)
        self.assertIsInstance(bucket.mc_client, CouchbaseClient)

    @attr(cbv="1.0.0")
    def test_simple_add(self):
        key = 'test_simple_add'
        self.client.add(key, 0, 0, 'value')
        self.assertTrue(self.client.get(key)[2] == 'value')
        self.client.delete(key)

    @attr(cbv="1.0.0")
    def test_simple_append(self):
        key = 'test_simple_append'
        self.client.set(key, 0, 0, 'value')
        self.client.append(key, 'appended')
        self.assertTrue(self.client.get(key)[2] == 'valueappended')
        self.client.delete(key)

    @attr(cbv="1.0.0")
    def test_simple_delete(self):
        key = 'test_simple_delete'
        self.client.set(key, 0, 0, 'value')
        self.client.delete(key)
        self.assertRaises(MemcachedError, self.client.get, key)

    @attr(cbv="1.0.0")
    def test_simple_decr(self):
        key = 'test_simple_decr'
        self.client.set(key, 0, 0, '4')
        self.client.decr(key, 1)
        self.assertTrue(self.client.get(key)[2] == 3)
        # test again using set with an int
        self.client.set(key, 0, 0, 4)
        self.client.decr(key, 1)
        self.assertTrue(self.client.get(key)[2] == 3)
        self.client.delete(key)

    @attr(cbv="1.0.0")
    def test_simple_incr(self):
        key = 'test_simple_incr'
        self.client.set(key, 0, 0, '1')
        self.client.incr(key, 1)
        self.assertTrue(self.client.get(key)[2] == 2)
        # test again using set with an int
        self.client.set(key, 0, 0, 1)
        self.client.incr(key, 1)
        self.assertTrue(self.client.get(key)[2] == 2)
        self.client.delete(key)

    @attr(cbv="1.0.0")
    def test_simple_get(self):
        key = 'test_simple_get'
        try:
            self.client.get(key)
            raise Exception('Key existed that should not have')
        except MemcachedError as e:
            if e.status != 1:
                raise e
        self.client.set(key, 0, 0, 'value')
        self.assertTrue(self.client.get(key)[2] == 'value')
        self.client.delete(key)

    @attr(cbv="1.0.0")
    def test_simple_prepend(self):
        key = 'test_simple_prepend'
        self.client.set(key, 0, 0, 'value')
        self.client.prepend(key, 'prepend')
        self.assertTrue(self.client.get(key)[2] == 'prependvalue')
        self.client.delete(key)

    @attr(cbv="1.0.0")
    def test_simple_replace(self):
        key = 'test_simple_replace'
        self.client.set(key, 0, 0, 'value')
        self.client.replace(key, 0, 0, 'replaced')
        self.assertTrue(self.client.get(key)[2] == 'replaced')
        self.client.delete(key)

    @attr(cbv="1.0.0")
    def test_simple_touch(self):
        key = 'test_simple_touch'
        self.client.set(key, 2, 0, 'value')
        self.client.touch(key, 5)
        time.sleep(3)
        self.assertTrue(self.client.get(key)[2] == 'value')
        self.client.delete(key)

    @attr(cbv="1.0.0")
    def test_set_and_get(self):
        kvs = [(str(uuid.uuid4()), str(uuid.uuid4())) for i in range(0, 100)]
        for k, v in kvs:
            self.client.set(k, 0, 0, v)

        for k, v in kvs:
            value = self.client.get(k)[2]
            self.assertEqual(v, value)

        for k, v in kvs:
            self.client.delete(k)

    @attr(cbv="1.0.0")
    def test_set_and_delete(self):
        kvs = [(str(uuid.uuid4()), str(uuid.uuid4())) for i in range(0, 100)]
        for k, v in kvs:
            self.client.set(k, 0, 0, v)
        for k, v in kvs:
            self.assertTrue(isinstance(self.client.delete(k), tuple))
            self.assertRaises(MemcachedError, self.client.get, k)

    @attr(cbv="1.0.0")
    def test_getl(self):
        key, value = 'test_getl', str(uuid.uuid4())
        self.client.set(key, 0, 0, value)
        _, cas, rv = self.client.getl(key)
        self.assertEqual(rv, value)
        self.assertRaises(MemcachedError, self.client.set, key, 0, 0, value)
        # unlock the key
        self.client.cas(key, 0, 0, cas, value)
        # now that it's unlocked, clean it up
        self.client.delete(key)

    @attr(cbv="1.0.0")
    def test_gat(self):
        key, value = 'test_gat', str(uuid.uuid4())
        self.client.set(key, 2, 0, value)
        set_value = self.client.gat(key, 5)[2]
        self.assertTrue(set_value == value)
        time.sleep(3)
        self.assertTrue(self.client.get(key)[2] == value)
        self.client.delete(key)

    @attr(cbv="2.0.0")
    def test_save(self):
        """Test deprecated save() method"""
        # test memcached key/value "saving"
        key = self.client.save({'_id': 'testing_save', 'name': 'Couchbase'})
        self.assertEqual(key, 'testing_save')
        self.client.delete('testing_save')
        # test ddoc handling
        design_doc = {"_id": "_design/testing_save_ddoc",
                      "views":
                      {"testing":
                       {"map":
                        "function(doc) { emit(doc.name, doc.num); }"
                        }
                       }
                      }
        key = self.client.save(design_doc)
        self.assertEqual(key, '_design/testing_save_ddoc')
        rest = self.client.server._rest()
        rest.delete_design_doc(self.client.name, 'testing_save_ddoc')

    @attr(cbv="1.0.0")
    def test_setitem(self):
        # test int
        self.client['int'] = 10
        self.assertEqual(self.client['int'][2], 10)
        # test long
        self.client['long'] = long(10)
        self.assertEqual(self.client['long'][2], long(10))
        # test string
        self.client['str'] = 'string'
        self.assertEqual(self.client['str'][2], 'string')
        # test json
        # dictionaries are serialized to JSON objects
        self.client['json'] = {'json':'obj'}
        # but come out as strings for now
        self.assertEqual(self.client['json'][2], json.dumps({'json':'obj'}))
        # tear down
        for key in ['int', 'long', 'str', 'json']:
            self.client.delete(key)

    @attr(cbv="2.0.0")
    def test_getitem(self):
        """Test unique _design/doc handling in __getitem__"""
        ddoc_name = 'test_ddoc'
        design_doc = {"views":
                      {"testing":
                       {"map":
                        "function(doc) { emit(doc.name, doc.num); }"
                        }
                       }
                      }
        rest = self.client.server._rest()
        rest.create_design_doc(self.client.name, ddoc_name,
                               json.dumps(design_doc))
        self.assertIsInstance(self.client['_design/' + ddoc_name], DesignDoc)
        rest.delete_design_doc(self.client.name, ddoc_name)

    @attr(cbv="2.0.0")
    def test_view(self):
        design_doc = {"views":
                      {"testing":
                       {"map":
                        "function(doc) { emit(doc.name, doc.num); }"
                        }
                       }
                      }
        rest = self.client.server._rest()
        if rest.couch_api_base is None:
            raise SkipTest
        rest.create_design_doc(self.client.name, 'test_ddoc',
                               json.dumps(design_doc))
        results = self.client.view('_design/test_ddoc/_view/testing')
        self.assertIsInstance(results, types.ListType)

    @attr(cbv="2.0.0")
    def test_design_docs(self):
        doc_names = []
        # set up some docs we can find
        for i in range(0, 10):
            doc_names.append('doc' + str(i))
            self.client['doc' + str(i)] = {'name': 'doc' + str(i), 'num': i}

        design_doc = {"views":
                      {"testing":
                       {"map":
                        "function(doc) { emit(doc.name, doc.num); }"
                        }
                       }
                      }
        rest = self.client.server._rest()
        if rest.couch_api_base is None:
            raise SkipTest
        rest.create_design_doc(self.client.name, 'test_ddoc',
                               json.dumps(design_doc))
        ddocs = self.client.design_docs()
        self.assertIsInstance(ddocs, types.ListType)
        self.assertIn('test_ddoc', [ddoc for ddoc in ddocs])
        self.assertIn(design_doc, [ddoc for ddoc in ddocs])
        rest.delete_design_doc(self.client.name, 'test_ddoc')
        for key in doc_names:
            self.client.delete(key)

    @attr(cbv="2.0.0")
    def test_design_doc_creation_via_setitem(self):
        design_doc = {"views":
                      {"testing":
                       {"map":
                        "function(doc) { emit(doc.name, doc.num); }"
                        }
                       }
                      }
        self.client['_design/testing_setitem'] = design_doc
        self.assertIn('testing_setitem', self.client.design_docs())
        rest = self.client.server._rest()
        rest.delete_design_doc(self.client.name, 'testing_setitem')


class DesignDocTest(Base):
    def setUp(self):
        super(DesignDocTest, self).setUp()
        self.cb = Couchbase(self.host + ':' + self.port, self.username,
                            self.password)
        self.client = self.cb[self.bucket_name]
        self.rest = self.client.server._rest()
        if self.rest.couch_api_base is None:
            raise SkipTest

        self.ddoc = {"views":
                     {"testing":
                      {"map":
                       """function(doc, meta) {
                           if (meta.type === 'json' && doc.name) {
                             emit(doc.name, doc.num);
                           }
                        }""",
                       "reduce": "_count"
                       }
                      }
                     }
        self.rest.create_design_doc(self.client.name, 'test_ddoc',
                               json.dumps(self.ddoc))
        self.design_docs = self.client.design_docs()

    def tearDown(self):
        self.rest.delete_design_doc(self.client.name, 'test_ddoc')

    def test_views(self):
        """List views from a given cluster. PYCBC-7"""
        views = self.design_docs[0].views()
        self.assertIsInstance(views, types.ListType)
        self.assertIn('testing', views)
        self.assertIn(self.ddoc['views'], views)

    @attr(cbv="2.0.0")
    def test_getitem(self):
        """Instantiate an Object that represents a view. PYCBC-7"""
        view = self.design_docs[0]['testing']
        self.assertIsInstance(view, View)


class ViewTest(DesignDocTest):
    @nottest
    def setup_sample_docs(self):
        self.doc_names = []
        for i in range(0, 10):
            self.doc_names.append('doc' + str(i))
            self.client['doc' + str(i)] = {'name': 'doc' + str(i), 'num': i}

    @nottest
    def teardown_sample_docs(self):
        for key in self.doc_names:
            self.client.delete(key)

    @attr(cbv="2.0.0")
    def test_results(self):
        """Test retrieval of view results"""
        self.setup_sample_docs()
        view = self.design_docs[0]['testing']
        # Retrieve reduced results from a View. PYCBC-7
        #   (the format is the same, but there is no associated docid)
        results = view.results({'stale': False})
        self.assertEqual(results, 10)
        # Assemble query parameters for a View. PYCBC-7
        results = view.results({'stale': False, 'reduce': False})
        if "error" in results:
            self.fail(results)
        else:
            self.assertIsInstance(results, Set)
            self.assertIs(len(results), 10)
        # test again with include_docs=true
        # Retrieve non-reduced results from a View. PYCBC-7
        #   Be able to get the underlying document from the non-reduced results
        #   (this request should flow over binprot)
        # TODO: upgrade to use binary protocol rather than HTTP-based one
        results = view.results({'stale': False, 'include_docs': True,
                                'reduce': False})
        if "error" in results:
            self.fail(results)
        else:
            self.assertIsInstance(results, Set)
            self.assertIs(len(results), 10)
            for row in results:
                self.assertIn('doc', row)
                self.assertIn('meta', row['doc'])
                self.assertIn('json', row['doc'])
        self.teardown_sample_docs()


if __name__ == "__main__":
    unittest.main()
