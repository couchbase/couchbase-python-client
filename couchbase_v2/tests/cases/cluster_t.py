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
from unittest import SkipTest

from couchbase_tests.base import CouchbaseTestCase
from couchbase_core.connstr import ConnectionString
from couchbase_core.cluster import _Cluster as Cluster
from couchbase.auth import MixedAuthException, PasswordAuthenticator, ClassicAuthenticator, CertAuthenticator

import os
import warnings
from couchbase.exceptions import NetworkException, CouchbaseFatalException, CouchbaseInputException, CouchbaseException


CERT_PATH = os.getenv("PYCBC_CERT_PATH")


class ClusterTest(CouchbaseTestCase):

    def _create_cluster(self):
        connargs = self.make_connargs()
        connstr = ConnectionString.parse(str(connargs.pop('connection_string')))
        connstr.clear_option('username')
        bucket = connstr.bucket
        connstr.bucket = None
        password = connargs.get('password', '')

        # Can I open a new bucket via open_bucket?
        cluster = Cluster(connstr, bucket_factory=self.factory)
        cluster.authenticate(ClassicAuthenticator(buckets={bucket: password},cluster_password=self.cluster_info.admin_password, cluster_username=self.cluster_info.admin_username))
        return cluster, bucket

    def test_cluster(self):
        cluster, bucket_name = self._create_cluster()
        cb = cluster.open_bucket(bucket_name)
        key = self.gen_key('cluster_test')
        cb.upsert(key, 'cluster test')

    def test_no_mixed_auth(self):
        cluster, bucket_name = self._create_cluster()
        auther = PasswordAuthenticator(bucket_name,
                                       self.cluster_info.bucket_password)

        cluster.authenticate(auther)
        cb1 = cluster.open_bucket(bucket_name)
        self.assertRaises(MixedAuthException, cluster.open_bucket, bucket_name,
                          password=self.cluster_info.bucket_password)

        cluster2, bucket_name = self._create_cluster()
        cb2 = cluster2.open_bucket(bucket_name,
                                   password=self.cluster_info.bucket_password)

    def test_PYCBC_488(self):
        cluster = Cluster('couchbases://10.142.175.101?certpath=/Users/daschl/tmp/ks/chain.pem&keypath=/Users/daschl/tmp/ks/pkey.key')
        with self.assertRaises(MixedAuthException) as maerr:
            cluster.open_bucket("pixels",
                                 password=self.cluster_info.bucket_password)
        exception = maerr.exception
        self.assertIsInstance(exception, MixedAuthException)
        self.assertRegex(exception.message, r'.*CertAuthenticator.*password.*')

    def test_PYCBC_489(self):
        from couchbase_v2.cluster import Cluster
        with self.assertRaises(MixedAuthException) as maerr:
            cluster = Cluster('couchbases://10.142.175.101?certpath=/Users/daschl/tmp/ks/chain.pem&keypath=/Users/daschl/tmp/ks/pkey.key')
            cb = cluster.open_bucket('pixels', password = 'foo')
            cb.upsert('u:king_arthur', {'name': 'Arthur', 'email': 'kingarthur@couchbase.com', 'interests': ['Holy Grail', 'African Swallows']})
        exception = maerr.exception
        self.assertIsInstance(exception, MixedAuthException)
        self.assertRegex(exception.message, r'.*CertAuthenticator-style.*password.*')

    def test_no_mixed_cert_auth(self):
        cluster3, bucket_name = self._create_cluster()
        auther_cert = CertAuthenticator(cert_path="dummy",key_path="dummy2")
        cluster3.authenticate(auther_cert)
        with self.assertRaises(MixedAuthException) as maerr:
            cluster3.open_bucket(bucket_name,
                          password=self.cluster_info.bucket_password)
        exception = maerr.exception
        self.assertIsInstance(exception, MixedAuthException)

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
        cluster = Cluster(connstr, bucket_factory=self.factory)
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
        except CouchbaseException as e:
            self.assertRegex(str(e), r'.*LCB_ERR_SSL_ERROR.*')
            if self.is_realserver and certpath and keypath:
                raise e
            else:
                raise SkipTest("SSL error but expected so skipping")

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

    def _test_allow_cert_path_with_SSL_mock_errors(self, func, *args, **kwargs):
        try:
            func(*args,**kwargs)
        except Exception as e:
            if self.is_realserver and CERT_PATH:
                raise
            try:
                raise e
            except NetworkException as f:
                self.assertRegex(str(e),r'.*(refused the connection).*')
            except CouchbaseFatalException as f:
                self.assertRegex(str(e),r'.*(SSL subsystem).*')
            except CouchbaseInputException as f:
                self.assertRegex(str(e),r'.*(not supported).*')
            except CouchbaseException as f:
                self.assertRegex(str(e),r'.*(LCB_ERR_SSL_ERROR|LCB_ERR_SDK_FEATURE_UNAVAILABLE).*')

            warnings.warn("Got exception {} but acceptable error for Mock with  SSL+cert_path tests".format(str(e)))

    def test_can_authenticate_with_cert_path_and_username_password_via_PasswordAuthenticator(self):
        cluster = Cluster(
            'couchbases://{host}?certpath={certpath}'.format(host=self.cluster_info.host, certpath=CERT_PATH))
        authenticator = PasswordAuthenticator(self.cluster_info.admin_username, self.cluster_info.admin_password)
        cluster.authenticate(authenticator)
        self._test_allow_cert_path_with_SSL_mock_errors(cluster.open_bucket, self.cluster_info.bucket_name)

    def test_can_authenticate_with_cert_path_and_username_password_via_ClassicAuthenticator(self):
        cluster = Cluster(
            'couchbases://{host}?certpath={certpath}'.format(host=self.cluster_info.host, certpath=CERT_PATH))
        authenticator = ClassicAuthenticator(buckets={self.cluster_info.bucket_name: self.cluster_info.bucket_password},
                                             cluster_username=self.cluster_info.admin_username,
                                             cluster_password=self.cluster_info.admin_password)
        cluster.authenticate(authenticator)
        self._test_allow_cert_path_with_SSL_mock_errors(cluster.open_bucket, self.cluster_info.bucket_name)

    def test_can_authenticate_with_cert_path_and_username_password_via_kwargs(self):
        cluster = Cluster(
            'couchbases://{host}?certpath={certpath}'.format(host=self.cluster_info.host, certpath=CERT_PATH))
        self._test_allow_cert_path_with_SSL_mock_errors(cluster.open_bucket, self.cluster_info.bucket_name,
                                                        username=self.cluster_info.admin_username,
                                                        password=self.cluster_info.admin_password)
