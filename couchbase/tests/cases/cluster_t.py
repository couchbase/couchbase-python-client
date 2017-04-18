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
from couchbase.cluster import Cluster, ClassicAuthenticator,\
    PasswordAuthenticator, NoBucketError, MixedAuthError
import gc


class ClusterTest(CouchbaseTestCase):
    def _create_cluster(self):
        connargs = self.make_connargs()
        connstr = ConnectionString.parse(str(connargs.pop('connection_string')))
        connstr.clear_option('username')
        bucket = connstr.bucket
        connstr.bucket = None
        password = connargs.get('password', '')

        # Can I open a new bucket via open_bucket?
        cluster = Cluster(connstr, bucket_class=self.factory)
        cluster.authenticate(ClassicAuthenticator(buckets={bucket: password}))
        return cluster, bucket

    def test_cluster(self):
        cluster, bucket_name = self._create_cluster()
        cb = cluster.open_bucket(bucket_name)
        key = self.gen_key('cluster_test')
        cb.upsert(key, 'cluster test')

    def test_query(self):
        self.skipUnlessMock()

        cluster, bucket_name = self._create_cluster()

        # Should fail if no bucket is active yet
        self.assertRaises(NoBucketError, cluster.n1ql_query, "select mockrow")

        # Open a bucket
        cb = cluster.open_bucket(bucket_name)
        row = cluster.n1ql_query('select mockrow').get_single_result()

        # Should fail again once the bucket has been GC'd
        del cb
        gc.collect()

        self.assertRaises(NoBucketError, cluster.n1ql_query, 'select mockrow')

    def test_no_mixed_auth(self):
        cluster, bucket_name = self._create_cluster()
        auther = PasswordAuthenticator(bucket_name,
                                       self.cluster_info.bucket_password)

        cluster.authenticate(auther)
        cb1 = cluster.open_bucket(bucket_name)
        self.assertRaises(MixedAuthError, cluster.open_bucket, bucket_name,
                          password=self.cluster_info.bucket_password)

        cluster2, bucket_name = self._create_cluster()
        cb2 = cluster2.open_bucket(bucket_name,
                                   password=self.cluster_info.bucket_password)