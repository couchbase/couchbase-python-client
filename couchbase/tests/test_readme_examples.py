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

from nose.plugins.skip import SkipTest

from couchbase.tests.base import Base


class ReadmeTest(Base):
    def test_old_set_get_create_example(self):
        from couchbase.couchbaseclient import CouchbaseClient
        from couchbase.rest_client import RestConnection, RestHelper

        client = CouchbaseClient(self.url, self.bucket_name, "", False)
        client.set("key1", 0, 0, "value1")
        client.get("key1")

        server_info = {"ip": self.host,
                       "port": self.port,
                       "username": self.username,
                       "password": self.password}
        rest = RestConnection(server_info)
        rest.create_bucket(bucket='newbucket',
                           ramQuotaMB=100,
                           authType='none',
                           saslPassword='',
                           replicaNumber=1,
                           proxyPort=11215,
                           bucketType='membase')

        self.assertTrue(RestHelper(rest).bucket_exists('newbucket'))
        rest.delete_bucket('newbucket')

    def test_old_unified_client_example(self):
        from couchbase import Couchbase

        # connect to a couchbase server
        cb = Couchbase(self.host + ':' + self.port,
                       username=self.username,
                       password=self.password)
        if not cb.couch_api_base:
            raise SkipTest

        default_bucket = cb[self.bucket_name]
        default_bucket['key1'] = 'value1'

        default_bucket2 = cb.bucket(self.bucket_name)
        default_bucket2['key2'] = {'value': 'value2', 'expiration': 0}

        default_bucket.set('key3', 0, 0, 'value3')

        self.assertEqual(str(default_bucket.get('key1')[2]), 'value1')
        self.assertEqual(str(default_bucket2.get('key2')[2]), 'value2')
        self.assertEqual(str(default_bucket2['key3'][2]), 'value3')

        # create a new bucket
        try:
            newbucket = cb.create('newbucket', ram_quota_mb=100, replica=1)
        except:
            newbucket = cb['newbucket']

        # set a JSON document using the more "pythonic" interface
        newbucket['json_test'] = {'type': 'item', 'value': 'json test'}
        print 'json_test ' + str(newbucket['json_test'])
        # use the more verbose API which allows for setting expiration & flags
        newbucket.set('key4', 0, 0, {'type': 'item', 'value': 'json test'})
        print 'key4 ' + str(newbucket['key4'])

        design_doc = {"views":
                      {"all_by_types":
                       {"map":
                        '''function (doc, meta) {
                             emit([meta.type, doc.type, meta.id], doc.value);
                             // row output: ['json', 'item', 'key4'], 'json test'
                           }'''
                        },
                       },
                      }
        # save a design document
        newbucket['_design/testing'] = design_doc

        all_by_types_view = newbucket['_design/testing']['all_by_types']
        rows = all_by_types_view.results({'stale': False})
        for row in rows:
            self.assertTrue(row is not None)

        cb.delete('newbucket')
