#
# Copyright 2017, Couchbase, Inc.
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

from couchbase.tests.base import CouchbaseTestCase
from couchbase.connstr import ConnectionString
from couchbase.cluster import Cluster, ClassicAuthenticator


class ClusterTest(CouchbaseTestCase):
    def test_cluster(self):
        connargs = self.make_connargs()
        connstr = ConnectionString.parse(str(connargs.pop('connection_string')))
        password = connstr.options.pop('password', '')
        connstr.options.pop('username', '')
        bucket = connstr.bucket
        connstr.bucket = None

        # Can I open a new bucket via open_bucket?
        cluster = Cluster(connstr, bucket_class=self.factory)
        cluster.authenticate(ClassicAuthenticator(buckets={bucket: password}))

        cb = cluster.open_bucket(bucket, **connargs)
        key = self.gen_key('cluster_test')
        cb.upsert(key, 'cluster test')