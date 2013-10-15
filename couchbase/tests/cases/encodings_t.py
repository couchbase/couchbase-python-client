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

from couchbase import FMT_BYTES, FMT_JSON, FMT_PICKLE, FMT_UTF8
from couchbase.connection import Connection
from couchbase.exceptions import ValueFormatError, CouchbaseError
from couchbase.tests.base import ConnectionTestCase, SkipTest

BLOB_ORIG =  b'\xff\xfe\xe9\x05\xdc\x05\xd5\x05\xdd\x05'

class ConnectionEncodingTest(ConnectionTestCase):

    def test_default_format(self):
        self.assertEqual(self.cb.default_format, FMT_JSON)

    def test_unicode(self):
        txt = BLOB_ORIG.decode('utf-16')
        for f in (FMT_BYTES, FMT_PICKLE):
            cas = self.cb.set(txt, txt.encode('utf-16'), format=f).cas
            server_val = self.cb.get(txt).value
            self.assertEquals(server_val, BLOB_ORIG)

    def test_json_unicode(self):
        self.assertEqual(self.cb.default_format, FMT_JSON)
        uc = BLOB_ORIG.decode('utf-16')
        rv = self.cb.set(uc, uc)
        self.assertTrue(rv.success)
        rv = self.cb.get(uc)
        self.assertEqual(rv.value, uc)
        self.assertEqual(rv.key, uc)

    def test_json_compact(self):
        # This ensures our JSON encoder doesn't store huge blobs of data in the
        # server. This was added as a result of PYCBC-108
        self.assertEqual(self.cb.default_format, FMT_JSON)
        uc = BLOB_ORIG.decode('utf-16')
        key = self.gen_key('json_compact')
        self.cb.set(key, uc, format=FMT_JSON)
        self.cb.data_passthrough = 1
        rv = self.cb.get(key)

        expected = '"'.encode('utf-8') + uc.encode('utf-8') + '"'.encode('utf-8')
        self.assertEqual(expected, rv.value)

        self.cb.data_passthrough = 0

    def test_blob(self):
        blob = b'\x00\x01\x00\xfe\xff\x01\x42'
        for f in (FMT_BYTES, FMT_PICKLE):
            cas = self.cb.set("key", blob, format=f).cas
            self.assertTrue(cas)
            rv = self.cb.get("key").value
            self.assertEquals(rv, blob)

    def test_bytearray(self):
        ba = bytearray(b"Hello World")
        self.cb.set("key", ba, format=FMT_BYTES)
        rv = self.cb.get("key")
        self.assertEqual(ba, rv.value)

    def test_passthrough(self):
        self.cb.data_passthrough = True
        self.cb.set("malformed", "some json")
        self.cb.append("malformed", "blobs")
        rv = self.cb.get("malformed")

        self.assertTrue(rv.success)
        self.assertEqual(rv.flags, FMT_JSON)
        self.assertEqual(rv.value, b'"some json"blobs')

        self.cb.data_passthrough = False
        self.assertRaises(ValueFormatError, self.cb.get, "malformed")

    def test_zerolength(self):
        rv = self.cb.set("key", b"", format=FMT_BYTES)
        self.assertTrue(rv.success)
        rv = self.cb.get("key")
        self.assertEqual(rv.value, b"")

        self.assertRaises(CouchbaseError, self.cb.set, "", "value")

    def test_blob_keys_py2(self):
        if bytes == str:
            rv = self.cb.set(b"\0", "value")
            rv = self.cb.get(b"\0")
        else:
            self.assertRaises(ValueFormatError, self.cb.set, b"\0", "value")
