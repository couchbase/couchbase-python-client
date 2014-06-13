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

from couchbase.tests.base import ConnectionTestCase
from couchbase.user_constants import FMT_JSON, FMT_AUTO, FMT_JSON, FMT_PICKLE
from couchbase.exceptions import ClientTemporaryFailError
from couchbase import Couchbase
from couchbase.exceptions import CouchbaseError

class ConnectionMiscTest(ConnectionTestCase):

    def test_server_nodes(self):
        nodes = self.cb.server_nodes
        self.assertIsInstance(nodes, (list, tuple))
        self.assertTrue(len(nodes) > 0)
        for n in nodes:
            self.assertIsInstance(n, str)

        def _set_nodes():
            self.cb.server_nodes = 'sdf'
        self.assertRaises((AttributeError, TypeError), _set_nodes)

    def test_lcb_version(self):
        verstr, vernum = self.factory.lcb_version()
        self.assertIsInstance(verstr, str)
        self.assertIsInstance(vernum, int)

    def test_bucket(self):
        bucket_str = self.cb.bucket
        self.assertEqual(bucket_str, self.make_connargs()['bucket'])

    def test_conn_repr(self):
        repr(self.cb)


    def test_connection_defaults(self):
        # This will only work on the basic Connection class
        from couchbase.connection import Connection
        ctor_params = self.make_connargs()
        # XXX: Change these if any of the defaults change
        defaults = {
            'timeout' : 2.5,
            'quiet' : False,
            'default_format' : FMT_JSON,
            'unlock_gil' : True,
            'transcoder' : None
        }

        cb_ctor = Connection(**ctor_params)
        cb_connect = Couchbase.connect(**ctor_params)

        for option, value in defaults.items():
            actual = getattr(cb_ctor, option)
            self.assertEqual(actual, value)

            actual = getattr(cb_connect, option)
            self.assertEqual(actual, value)

    def test_closed(self):
        cb = self.cb
        self.assertFalse(cb.closed)
        cb._close()
        self.assertTrue(cb.closed)
        self.assertRaises(ClientTemporaryFailError, self.cb.get, "foo")


    def test_fmt_args(self):
        # Regression
        cb = self.make_connection(default_format=123)
        self.assertEqual(cb.default_format, 123)

        key = self.gen_key("fmt_auto_ctor")
        cb = self.make_connection(default_format = FMT_AUTO)
        cb.set("foo", set([]))
        rv = cb.get("foo")
        self.assertEqual(rv.flags, FMT_PICKLE)


    def test_cntl(self):
        cb = self.make_connection()
        # Get the timeout
        rv = cb._cntl(0x01)
        self.assertEqual(75000000, rv)

        cb._cntl(0x01, rv)
        # Doesn't crash? good enough

        # Try with something invalid
        self.assertRaises(CouchbaseError, cb._cntl, 0xf000)
        self.assertRaises(CouchbaseError, cb._cntl, 0x01, "string")

        # Try with something else now. Operation timeout
        rv = cb._cntl(0x00, value_type="timeout")
        self.assertEqual(2.5, rv)

        rv = cb._cntl(0x00, value_type="uint32_t")
        self.assertEqual(2500000, rv)

        # Modification:
        cb._cntl(0x00, 100000, value_type="uint32_t")
        rv = cb._cntl(0x00, value_type="timeout")
        self.assertEqual(0.1, rv)

    def test_newer_ctls(self):
        cb = self.make_connection()
        self.skipLcbMin("2.3.1")
        rv = cb._cntl(0x1f, value_type="string") # LCB_CNTL_CHANGESET
        "" + rv # String

        # CONFIG_CACHE_LOADED
        rv = cb._cntl(0x15, value_type="int") #
        self.assertEqual(0, rv)

    def test_vbmap(self):
        # We don't know what the vbucket map is supposed to be, so just
        # check it doesn't fail
        cb = self.make_connection()
        vb, ix = cb._vbmap("hello")
        int(vb)
        int(ix)
