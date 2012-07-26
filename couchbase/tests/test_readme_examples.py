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
