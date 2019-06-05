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

from couchbase_tests.base import CouchbaseTestCase
from couchbase_core.connstr import ConnectionString
from couchbase_v2.cluster import Cluster, ClassicAuthenticator,PasswordAuthenticator, NoBucketError, MixedAuthError, CertAuthenticator
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
        cluster.authenticate(ClassicAuthenticator(buckets={bucket: password},cluster_password=self.cluster_info.admin_password, cluster_username=self.cluster_info.admin_username))
        return cluster, bucket

    def test_cluster(self):
        cluster, bucket_name = self._create_cluster()
        cb = cluster.open_bucket(bucket_name)
        key = self.gen_key('cluster_test')
        cb.upsert(key, 'cluster test')

    def test_cluster_manager(self):
        cluster, bucket_name = self._create_cluster()

        # Create an Admin object from the cluster
        admin = cluster.cluster_manager()

        # Ensure we can retrieve bucket info
        # If we don't error out, this works
        bucket_info = admin.bucket_info(bucket_name)

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

    def test_PYCBC_488(self):
        cluster = Cluster('couchbases://10.142.175.101?certpath=/Users/daschl/tmp/ks/chain.pem&keypath=/Users/daschl/tmp/ks/pkey.key')
        with self.assertRaises(MixedAuthError) as maerr:
            cluster.open_bucket("pixels",
                                 password=self.cluster_info.bucket_password)
        exception = maerr.exception
        self.assertIsInstance(exception, MixedAuthError)
        self.assertRegex(exception.message, r'.*CertAuthenticator.*password.*')

    def test_PYCBC_489(self):
        from couchbase_v2.cluster import Cluster
        with self.assertRaises(MixedAuthError) as maerr:
            cluster = Cluster('couchbases://10.142.175.101?certpath=/Users/daschl/tmp/ks/chain.pem&keypath=/Users/daschl/tmp/ks/pkey.key')
            cb = cluster.open_bucket('pixels', password = 'foo')
            cb.upsert('u:king_arthur', {'name': 'Arthur', 'email': 'kingarthur@couchbase.com', 'interests': ['Holy Grail', 'African Swallows']})
        exception = maerr.exception
        self.assertIsInstance(exception, MixedAuthError)
        self.assertRegex(exception.message, r'.*CertAuthenticator-style.*password.*')

    def test_no_mixed_cert_auth(self):
        cluster3, bucket_name = self._create_cluster()
        auther_cert = CertAuthenticator(cert_path="dummy",key_path="dummy2")
        cluster3.authenticate(auther_cert)
        with self.assertRaises(MixedAuthError) as maerr:
            cluster3.open_bucket(bucket_name,
                          password=self.cluster_info.bucket_password)
        exception = maerr.exception
        self.assertIsInstance(exception, MixedAuthError)

        self.assertRegex(exception.message, r'.*CertAuthenticator.*password.*')

    def _create_cluster_clean(self, authenticator):
        connargs = self.make_connargs()
        connstr = ConnectionString.parse(str(connargs.pop('connection_string')))
        connstr.clear_option('username')
        bucket = connstr.bucket
        connstr.bucket = None
        password = connargs.get('password', None)
        keys_to_skip = authenticator.get_credentials(bucket)['options'].keys()
        for entry in keys_to_skip:
            connstr.clear_option(entry)
        cluster = Cluster(connstr, bucket_class=self.factory)
        cluster.authenticate(ClassicAuthenticator(buckets={bucket: password}))
        return cluster, bucket

    def test_cert_auth(self):
        certpath=getattr(self.cluster_info,'certpath',None)
        keypath=getattr(self.cluster_info,'keypath',None)

        auther_cert = CertAuthenticator(cert_path=certpath or "dummy",key_path=keypath or "dummy")

        cluster3, bucket_name = self._create_cluster_clean(auther_cert)
        cluster3.authenticate(auther_cert)
        try:
            cluster3.open_bucket(bucket_name)
        except Exception as e:
            if self.is_realserver and certpath and keypath:
                raise e
            else:
                pass

    def test_pathless_connstr(self):
        # Not strictly a cluster test, but relevant
        connstr = ConnectionString.parse('couchbase://localhost?opt1=val1&opt2=val2')
        self.assertTrue('opt1' in connstr.options)
        self.assertTrue('opt2' in connstr.options)

    def test_validate_authenticate(self):

        cluster, bucket_name = self._create_cluster()
        self.assertRaises(ValueError, cluster.authenticate, username=None, password=None)
        self.assertRaises(ValueError, cluster.authenticate, username='', password='')
        self.assertRaises(ValueError, cluster.authenticate, username='username', password=None)
        self.assertRaises(ValueError, cluster.authenticate, username='username', password='')
        self.assertRaises(ValueError, cluster.authenticate, username=None, password='password')
        self.assertRaises(ValueError, cluster.authenticate, username='', password='password')

    def test_can_authenticate_with_username_password(self):

        cluster, bucket_name = self._create_cluster()
        cluster.authenticate(username='Administrator', password='password')

        bucket = cluster.open_bucket(bucket_name)
        self.assertIsNotNone(bucket)
