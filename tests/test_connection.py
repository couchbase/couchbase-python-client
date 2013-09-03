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

from couchbase.exceptions import (AuthError, ArgumentError,
                                  BucketNotFoundError, ConnectError,
                                  NotFoundError, InvalidError)
from couchbase.connection import Connection

from tests.base import CouchbaseTestCase


class ConnectionTest(CouchbaseTestCase):
    def test_connection_host_port(self):
        cb = Connection(host=self.host,
                        port=self.port,
                        password=self.bucket_password,
                        bucket=self.bucket_prefix)
        # Connection didn't throw an error
        self.assertIsInstance(cb, Connection)

    def test_server_not_found(self):
        self.slowTest()
        connargs = self.make_connargs()
        connargs['host'] = 'example.com'
        self.assertRaises(ConnectError, Connection, **connargs)

        connargs['host'] = self.host
        connargs['port'] = 34567
        self.assertRaises(ConnectError, Connection, **connargs)

    def test_bucket(self):
        cb = Connection(**self.make_connargs())
        self.assertIsInstance(cb, Connection)

    def test_sasl_bucket(self):
        self.skipUnlessSasl()
        connargs = self.make_connargs()
        sasl_params = self.get_sasl_params()

        connargs['bucket'] = sasl_params['bucket']
        connargs['password'] = sasl_params['password']
        cb = Connection(**connargs)
        self.assertIsInstance(cb, Connection)

    def test_bucket_not_found(self):
        connargs = self.make_connargs(bucket='this_bucket_does_not_exist')
        self.assertRaises(BucketNotFoundError, Connection, **connargs)

    def test_bucket_wrong_credentials(self):
        self.skipIfMock()

        self.assertRaises(AuthError, Connection,
                          **self.make_connargs(password='bad_pass'))

        self.assertRaises(AuthError, Connection,
                          **self.make_connargs(password='wrong_password'))

    def test_sasl_bucket_wrong_credentials(self):
        self.skipUnlessSasl()
        sasl_bucket = self.get_sasl_params()['bucket']
        self.assertRaises(AuthError, Connection,
                          **self.make_connargs(password='wrong_password',
                                               bucket=sasl_bucket))

    def test_quiet(self):
        connparams = self.make_connargs()
        cb = Connection(**connparams)
        self.assertRaises(NotFoundError, cb.get, 'missing_key')

        cb = Connection(quiet=True, **connparams)
        cb.delete('missing_key', quiet=True)
        val1 = cb.get('missing_key')
        self.assertFalse(val1.success)

        cb = Connection(quiet=False, **connparams)
        self.assertRaises(NotFoundError, cb.get, 'missing_key')


    def test_conncache(self):
        cachefile = None
        # On Windows, the NamedTemporaryFile is deleted right when it's
        # created. So we need to ensure it's not deleted, and delete it
        # ourselves when it's closed
        try:
            cachefile = tempfile.NamedTemporaryFile(delete=False)
            cb = Connection(conncache=cachefile.name, **self.make_connargs())
            self.assertTrue(cb.set("foo", "bar").success)

            cb2 = Connection(conncache=cachefile.name, **self.make_connargs())

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
        cb = Connection(password='bad',
                        bucket='meh',
                        host='localhost',
                        port=1,
                        _no_connect_exceptions=True)
        errors = cb.errors()
        self.assertTrue(len(errors))
        self.assertEqual(len(errors[0]), 2)

        cb = Connection(**self.make_connargs())
        self.assertFalse(len(cb.errors()))

    def test_invalid_hostname(self):
        self.assertRaises(InvalidError,
                          Connection,
                          bucket='default', host='12345:qwer###')

    def test_multi_hosts(self):
        kwargs = {
            'password' : self.bucket_password,
            'bucket' : self.bucket_prefix
        }

        if not self.mock:
            cb = Connection(host=[self.host], **kwargs)
            self.assertTrue(cb.set("foo", "bar").success)

        cb = Connection(host=[(self.host, self.port)], **kwargs)
        self.assertTrue(cb.set("foo", "bar").success)

        cb = Connection(host=[('localhost', 1), (self.host, self.port)], **kwargs)
        self.assertTrue(cb.set("foo", "bar").success)

if __name__ == '__main__':
    unittest.main()
