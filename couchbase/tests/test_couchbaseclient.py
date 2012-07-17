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

from couchbase.tests.base import Base
from couchbase.couchbaseclient import CouchbaseClient
from couchbase.rest_client import RestConnection


class CouchbaseClientTest(Base):
    def setUp(self):
        super(CouchbaseClientTest, self).setUp()
        self.client = CouchbaseClient(self.url, self.bucket_name, "", True)

    def tearDown(self):
        self.client.flush()
        self.client.done()

    def test_set_integer_value(self):
        self.client.set('int', 0, 0, 10)
        self.assertEqual(self.client.get('int')[2], 10,
                         'value should be the integer 10')
        self.client.incr('int')
        self.assertEqual(self.client.get('int')[2], 11,
                         'value should be the integer 11')

    def test_bucket_of_type_memcached(self):
        """Our code used to be very vBucket-only. This tests to be sure we can
        work with our other database type: memcached"""
        temp_bucket_name = 'testing-memcached'
        self.rest_client = RestConnection({'ip': self.host,
                                           'port': self.port,
                                           'username': self.username,
                                           'password': self.password})
        self.rest_client.create_bucket(temp_bucket_name,
                                       bucketType='memcached',
                                       authType='sasl', ramQuotaMB=64)

        self.client_for_memcached_bucket = CouchbaseClient(self.url,
                                                           temp_bucket_name,
                                                           verbose=True)
        self.assertIsInstance(self.client_for_memcached_bucket,
                              CouchbaseClient)

        self.rest_client.delete_bucket(temp_bucket_name)
