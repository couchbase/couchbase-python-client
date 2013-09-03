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

try:
    from configparser import ConfigParser
except ImportError:
    # Python <3.0 fallback
    from ConfigParser import SafeConfigParser as ConfigParser
import os
import sys

import unittest
from nose.exc import SkipTest
import types
from couchbase.connection import Connection
from couchbase.exceptions import CouchbaseError
from couchbase.admin import Admin
from couchbase.mockserver import (
    CouchbaseMock, BucketSpec, MockControlClient)

from couchbase._pyport import basestring

CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'tests.ini')


class CouchbaseTestCase(unittest.TestCase):
    @classmethod
    def _setup_mock(self, mockpath, mockurl):

        bspec_dfl = BucketSpec('default', 'couchbase')
        bspec_sasl = BucketSpec('default_sasl', 'couchbase', 'secret')

        self.mock = CouchbaseMock([bspec_dfl, bspec_sasl],
                                  mockpath,
                                  mockurl,
                                  replicas=2,
                                  nodes=4)
        self.mock.start()
        self.bucket_prefix = "default"
        self.bucket_password = "secret"
        self.port = self.mock.rest_port
        self.host = "127.0.0.1"
        self.admin_username = "Administrator"
        self.admin_password = "password"
        self.extra_buckets = True

    @classmethod
    def setupClass(self):
        config = ConfigParser()
        config.read(CONFIG_FILE)
        self.host = config.get('node-1', 'host')
        self.port = config.getint('node-1', 'port')
        self.admin_username = config.get('node-1', 'admin_username')
        self.admin_password = config.get('node-1', 'admin_password')
        self.bucket_prefix = config.get('node-1', 'bucket_prefix')
        self.bucket_password = config.get('node-1', 'bucket_password')
        self.nosleep = os.environ.get('PYCBC_TESTS_NOSLEEP', False)
        self.extra_buckets = bool(int(config.get('node-1', 'extra_buckets')))

        self.mock = None
        if config.has_option("mock", "enabled"):
            if config.getboolean("mock", "enabled"):
                mockpath = config.get("mock", "path")
                if config.has_option("mock", "url"):
                    mockurl = config.get("mock", "url")
                else:
                    mockurl = None

                self._setup_mock(mockpath, mockurl)

    @classmethod
    def teardownClass(self):
        if self.mock:
            self.mock.stop()
        self.mock = None



    def setUp(self):
        if not hasattr(self, 'assertIsInstance'):
            def tmp(self, a, *bases):
                self.assertTrue(isinstance(a, bases))
            self.assertIsInstance = types.MethodType(tmp, self)
        if not hasattr(self, 'assertIsNone'):
            def tmp(self, a):
                self.assertTrue(a is None)
            self.assertIsNone = types.MethodType(tmp, self)

        self._key_counter = 0


    def get_sasl_params(self):
        if not self.bucket_password:
            return None
        ret = self.make_connargs()
        ret = { 'password' : self.bucket_password, 'bucket' : self.bucket_prefix }
        if self.extra_buckets:
            ret['bucket'] += "_sasl"
        return ret

    def skipUnlessSasl(self):
        sasl_params = self.get_sasl_params()
        if not sasl_params:
            raise SkipTest("No SASL buckets configured")


    def skipLcbMin(self, vstr):
        """
        Test requires a libcouchbase version of at least vstr.
        This may be a hex number (e.g. 0x020007) or a string (e.g. "2.0.7")
        """

        if isinstance(vstr, basestring):
            components = vstr.split('.')
            hexstr = "0x"
            for comp in components:
                if len(comp) > 2:
                    raise ValueError("Version component cannot be larger than 99")
                hexstr += "{0:02}".format(int(comp))

            vernum = int(hexstr, 16)
        else:
            vernum = vstr
            components = []
            # Get the display
            for x in range(0, 3):
                comp = (vernum & 0xff << (x*8)) >> x*8
                comp = "{0:x}".format(comp)
                components = [comp] + components
            vstr = ".".join(components)

        rtstr, rtnum = Connection.lcb_version()
        if rtnum < vernum:
            raise SkipTest(("Test requires {0} to run (have {1})")
                            .format(vstr, rtstr))


    def tearDown(self):
        pass

    def skipIfMock(self):
        if self.mock:
            raise SkipTest("Test not supported on Mock")

    def skipUnlessMock(self):
        if not self.mock:
            raise SkipTest("Test requires CouchbaseMock")

    def make_connargs(self, **overrides):
        ret = {
            'host' : self.host,
            'port' : self.port,
            'password' : self.bucket_password,
            'bucket' : self.bucket_prefix
        }
        ret.update(overrides)
        return ret

    def slowTest(self):
        if self.nosleep:
            raise SkipTest("Skipping slow/sleep-based test")

    def make_connection(self, **kwargs):
        return Connection(**self.make_connargs(**kwargs))

    def make_admin_connection(self):
        return Admin(self.admin_username, self.admin_password, self.host, self.port)

    def gen_key(self, prefix=None):
        if not prefix:
            prefix = "python-couchbase-key_"

        ret = "{0}{1}".format(prefix, self._key_counter)
        self._key_counter += 1
        return ret

    def gen_key_list(self, amount=5, prefix=None):
        ret = [ self.gen_key(prefix) for x in range(amount) ]
        return ret

    def gen_kv_dict(self, amount=5, prefix=None):
        ret = {}
        keys = self.gen_key_list(amount=amount, prefix=prefix)
        for k in keys:
            ret[k] = "Value_For_" + k
        return ret


class ConnectionTestCase(CouchbaseTestCase):
    def setUp(self):
        super(ConnectionTestCase, self).setUp()
        self.cb = self.make_connection()

    def tearDown(self):
        super(ConnectionTestCase, self).tearDown()
        oldrc = sys.getrefcount(self.cb)
        self.assertEqual(oldrc, 2)
        del self.cb

# Class which sets up all the necessary Mock stuff
class MockTestCase(ConnectionTestCase):
    def setUp(self):
        super(MockTestCase, self).setUp()
        self.skipUnlessMock()
        self.mockclient = MockControlClient(self.mock.rest_port)
