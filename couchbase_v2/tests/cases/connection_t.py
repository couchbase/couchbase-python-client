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

import tempfile
import os

from nose.plugins.attrib import attr

from couchbase_core.exceptions import (AuthError, BucketNotFoundError, CouchbaseNetworkError,
                                     NotFoundError, InvalidError,
                                     TimeoutError)
from couchbase_tests.base import CouchbaseTestCase, SkipTest, ConnectionTestCase
from couchbase_core.connstr import ConnectionString


class ConnectionTest(CouchbaseTestCase):
    @attr('slow')
    def test_server_not_found(self):
        connargs = self.make_connargs()
        cs = ConnectionString.parse(connargs['connection_string'])
        cs.hosts = [ 'example.com' ]
        connargs['connection_string'] = cs.encode()
        self.assertRaises((CouchbaseNetworkError, TimeoutError),
            self.factory, **connargs)

        cs.hosts = [ self.cluster_info.host + ':' + str(34567)]
        self.assertRaises(CouchbaseNetworkError, self.factory, **connargs)

    def test_bucket(self):
        cb = self.factory(**self.make_connargs())
        self.assertIsInstance(cb, self.factory)

    def test_bucket_not_found(self):
        connargs = self.make_connargs(bucket='this_bucket_does_not_exist')
        self.assertRaises(
            (BucketNotFoundError, AuthError), self.factory, **connargs)

    def test_quiet(self):
        connparams = self.make_connargs()
        cb = self.factory(**connparams)
        self.assertRaises(NotFoundError, cb.get, 'missing_key')

        cb = self.factory(quiet=True, **connparams)
        cb.remove('missing_key', quiet=True)
        val1 = cb.get('missing_key')
        self.assertFalse(val1.success)

        cb = self.factory(quiet=False, **connparams)
        self.assertRaises(NotFoundError, cb.get, 'missing_key')


    def test_configcache(self):
        cachefile = None
        # On Windows, the NamedTemporaryFile is deleted right when it's
        # created. So we need to ensure it's not deleted, and delete it
        # ourselves when it's closed
        try:
            cachefile = tempfile.NamedTemporaryFile(delete=False)
            cb = self.factory(**self.make_connargs(config_cache=cachefile.name))
            self.assertTrue(cb.upsert("foo", "bar").success)

            cb2 = self.factory(**self.make_connargs(config_cache=cachefile.name))

            self.assertTrue(cb2.upsert("foo", "bar").success)
            self.assertEqual("bar", cb.get("foo").value)

            sb = os.stat(cachefile.name)

            # For some reason this fails on Windows?
            self.assertTrue(sb.st_size > 0)
        finally:
            # On windows, we can't delete if the file is still being used
            cachefile.close()
            os.unlink(cachefile.name)

        # TODO, see what happens when bad path is used
        # apparently libcouchbase does not report this failure.

    def test_invalid_hostname(self):
        self.assertRaises(InvalidError, self.factory,
                          str('couchbase://12345:qwer###/default'))

    def test_multi_hosts(self):
        passwd = self.cluster_info.bucket_password
        cs = ConnectionString(bucket=self.cluster_info.bucket_name,
                              hosts=[self.cluster_info.host])

        if not self.mock:
            cb = self.factory(str(cs), password=passwd)
            self.assertTrue(cb.upsert("foo", "bar").success)

        cs.hosts = [ self.cluster_info.host + ':' + str(self.cluster_info.port) ]
        cs.scheme = 'http'
        cb = self.factory(str(cs), username=self.cluster_info.bucket_username, password=self.cluster_info.bucket_password)
        self.assertTrue(cb.upsert("foo", "bar").success)

        cs.hosts.insert(0, 'localhost:1')
        cb = self.factory(str(cs), username=self.cluster_info.bucket_username, password=self.cluster_info.bucket_password)
        self.assertTrue(cb.upsert("foo", "bar").success)

    def test_enable_error_map(self):
        # enabled via connection string param, invalid params cause error
        conn_str = 'http://{0}:{1}?enable_errmap=true'.format(self.cluster_info.host, self.cluster_info.port)
        cb = self.factory(conn_str,username=self.cluster_info.bucket_username,password=self.cluster_info.bucket_password)
        self.assertTrue(cb.upsert("foo", "bar").success)


class AlternateNamesTest(ConnectionTestCase):
    def setUp(self):
        super(AlternateNamesTest, self).setUp()
        self.args = dict()
        self.cb = None

    def test_external(self):
        if self.is_mock:
            raise SkipTest("Not supported with mock")
        super(AlternateNamesTest, self).setUp(network="external", **self.args)
        self.check_all_services()

    def test_default(self):
        super(AlternateNamesTest, self).setUp(network="default", **self.args)
        self.check_all_services()

    def test_blank(self):
        super(AlternateNamesTest, self).setUp(network=None, **self.args)
        self.check_all_services()

    def check_all_services(self):
        import uuid
        unique_str = str(uuid.uuid4())
        self.cb.upsert("network", unique_str)
        self.assertEqual(self.cb.get("network").value, unique_str)


if __name__ == '__main__':
    unittest.main()
