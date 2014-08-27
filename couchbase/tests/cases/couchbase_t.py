#
# Copyright 2013, Couchbase, Inc.
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

from couchbase import Couchbase
from couchbase.connection import Connection
from couchbase.tests.base import CouchbaseTestCase
from couchbase.connstr import ConnectionString

BUCKET_NAME = 'test_bucket_for_pythonsdk'


class CouchbaseTest(CouchbaseTestCase):
    def test_is_instance_of_connection(self):
        cs = "http://{0}:{1}/{2}".format(self.cluster_info.host,
                                         self.cluster_info.port,
                                         self.cluster_info.bucket_prefix)
        self.assertIsInstance(
            Couchbase.connect(cs, password=self.cluster_info.bucket_password),
            Connection)


if __name__ == '__main__':
    unittest.main()
