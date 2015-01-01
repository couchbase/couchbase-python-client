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

from couchbase.exceptions import (AuthError, ArgumentError,
                                  BucketNotFoundError, ConnectError,
                                  CouchbaseNetworkError,
                                  NotFoundError, InvalidError,
                                  TimeoutError)
from couchbase.tests.base import CouchbaseTestCase, SkipTest


class ConnectionTest(CouchbaseTestCase):
    def test_connection_host_port(self):
        cb = self.factory(host=self.cluster_info.host,
                          port=self.cluster_info.port,
                          password=self.cluster_info.bucket_password,
                          bucket=self.cluster_info.bucket_prefix)
        # Connection didn't throw an error
        self.assertIsInstance(cb, self.factory)

    @attr('slow')
    def test_server_not_found(self):
        connargs = self.make_connargs()
        connargs['host'] = 'example.com'
        self.assertRaises((CouchbaseNetworkError, TimeoutError),
                          self.factory, **connargs)

        connargs['host'] = self.cluster_info.host
        connargs['port'] = 34567
        self.assertRaises(CouchbaseNetworkError, self.factory, **connargs)

    def test_bucket(self):
        cb = self.factory(**self.make_connargs())
        self.assertIsInstance(cb, self.factory)

    def test_bucket_not_found(self):
        connargs = self.make_connargs(bucket='this_bucket_does_not_exist')
        self.assertRaises(BucketNotFoundError, self.factory, **connargs)

    def test_quiet(self):
        connparams = self.make_connargs()
        cb = self.factory(**connparams)
        self.assertRaises(NotFoundError, cb.get, 'missing_key')

        cb = self.factory(quiet=True, **connparams)
        cb.delete('missing_key', quiet=True)
        val1 = cb.get('missing_key')
        self.assertFalse(val1.success)

        cb = self.factory(quiet=False, **connparams)
        self.assertRaises(NotFoundError, cb.get, 'missing_key')


    def test_conncache(self):
        cachefile = None
        # On Windows, the NamedTemporaryFile is deleted right when it's
        # created. So we need to ensure it's not deleted, and delete it
        # ourselves when it's closed
        try:
            cachefile = tempfile.NamedTemporaryFile(delete=False)
            cb = self.factory(conncache=cachefile.name, **self.make_connargs())
            self.assertTrue(cb.set("foo", "bar").success)

            cb2 = self.factory(config_cache=cachefile.name, **self.make_connargs())

            self.assertTrue(cb2.set("foo", "bar").success)
            self.assertEquals("bar", cb.get("foo").value)

            sb = os.stat(cachefile.name)

            # For some reason this fails on Windows?
            self.assertTrue(sb.st_size > 0)
        finally:
            # On windows, we can't delete if the file is still being used
            cachefile.close()
            os.unlink(cachefile.name)

        # TODO, see what happens when bad path is used
        # apparently libcouchbase does not report this failure.

    def test_connection_errors(self):
        cb = self.factory(password='bad',
                          bucket='meh',
                          host='localhost',
                          port=1,
                          _no_connect_exceptions=True)
        errors = cb.errors()
        self.assertTrue(len(errors))
        self.assertEqual(len(errors[0]), 2)

        cb = self.factory(**self.make_connargs())
        self.assertFalse(len(cb.errors()))

    def test_invalid_hostname(self):
        self.assertRaises(InvalidError,
                          self.factory,
                          bucket='default', host='12345:qwer###')

    def test_multi_hosts(self):
        kwargs = {
            'password' : self.cluster_info.bucket_password,
            'bucket' : self.cluster_info.bucket_name
        }

        if not self.mock:
            cb = self.factory(host=[self.cluster_info.host], **kwargs)
            self.assertTrue(cb.set("foo", "bar").success)

        hostspec = [(self.cluster_info.host, self.cluster_info.port)]
        cb = self.factory(host=hostspec, **kwargs)
        self.assertTrue(cb.set("foo", "bar").success)

        hostlist = [
            ('localhost', 1),
            (self.cluster_info.host,
             self.cluster_info.port)
        ]
        cb = self.factory(host=hostlist, **kwargs)
        self.assertTrue(cb.set("foo", "bar").success)

if __name__ == '__main__':
    unittest.main()
