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

from nose.plugins.attrib import attr
from nose.tools import nottest

from couchbase.tests.base import Base
from couchbase.couchbaseclient import CouchbaseClient
from couchbase.rest_client import RestConnection


class CouchbaseClientTest(Base):
    def setUp(self):
        super(CouchbaseClientTest, self).setUp()
        self.client = CouchbaseClient(self.url, self.bucket_name, "", True)

    def tearDown(self):
        self.client.done()

    @nottest
    def setup_memcached_bucket(self):
        self.memcached_bucket = 'testing-memcached'
        self.rest_client = RestConnection({'ip': self.host,
                                           'port': self.port,
                                           'username': self.username,
                                           'password': self.password})
        self.rest_client.create_bucket(self.memcached_bucket,
                                       bucketType='memcached',
                                       authType='sasl', ramQuotaMB=64)
        self.client_for_memcached_bucket = \
            CouchbaseClient(self.url, self.memcached_bucket, verbose=True)

    @nottest
    def teardown_memcached_bucket(self):
        self.rest_client.delete_bucket(self.memcached_bucket)

    @attr(cbv="1.0.0")
    def test_set_integer_value(self):
        self.client.set('int', 0, 0, 10)
        self.assertEqual(self.client.get('int')[2], 10,
                         'value should be the integer 10')
        self.client.incr('int')
        self.assertEqual(self.client.get('int')[2], 11,
                         'value should be the integer 11')
        self.client.delete('int')

    @attr(cbv="1.0.0")
    def test_bucket_of_type_memcached(self):
        self.setup_memcached_bucket()
        self.assertIsInstance(self.client_for_memcached_bucket,
                              CouchbaseClient)
        self.teardown_memcached_bucket()
