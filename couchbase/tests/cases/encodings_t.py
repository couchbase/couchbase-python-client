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

from couchbase import (
    FMT_BYTES, FMT_JSON, FMT_PICKLE, FMT_UTF8,
    FMT_LEGACY_MASK, FMT_COMMON_MASK)
from couchbase.exceptions import ValueFormatError, CouchbaseError
from couchbase.tests.base import ConnectionTestCase, SkipTest
from couchbase.transcoder import TranscoderPP, LegacyTranscoderPP

BLOB_ORIG =  b'\xff\xfe\xe9\x05\xdc\x05\xd5\x05\xdd\x05'

class EncodingTest(ConnectionTestCase):

    def test_default_format(self):
        self.assertEqual(self.cb.default_format, FMT_JSON)

    def test_unicode(self):
        txt = BLOB_ORIG.decode('utf-16')
        for f in (FMT_BYTES, FMT_PICKLE):
            cas = self.cb.upsert(txt, txt.encode('utf-16'), format=f).cas
            server_val = self.cb.get(txt).value
            self.assertEqual(server_val, BLOB_ORIG)

    def test_json_unicode(self):
        self.assertEqual(self.cb.default_format, FMT_JSON)
        uc = BLOB_ORIG.decode('utf-16')
        rv = self.cb.upsert(uc, uc)
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
        self.cb.upsert(key, uc, format=FMT_JSON)
        self.cb.data_passthrough = 1
        rv = self.cb.get(key)

        expected = '"'.encode('utf-8') + uc.encode('utf-8') + '"'.encode('utf-8')
        self.assertEqual(expected, rv.value)

        self.cb.data_passthrough = 0

    def test_blob(self):
        blob = b'\x00\x01\x00\xfe\xff\x01\x42'
        for f in (FMT_BYTES, FMT_PICKLE):
            cas = self.cb.upsert("key", blob, format=f).cas
            self.assertTrue(cas)
            rv = self.cb.get("key").value
            self.assertEqual(rv, blob)

    def test_bytearray(self):
        ba = bytearray(b"Hello World")
        self.cb.upsert("key", ba, format=FMT_BYTES)
        rv = self.cb.get("key")
        self.assertEqual(ba, rv.value)

    def test_passthrough(self):
        self.cb.data_passthrough = True
        self.cb.upsert("malformed", "some json")
        self.cb.append("malformed", "blobs")
        rv = self.cb.get("malformed")

        self.assertTrue(rv.success)
        self.assertEqual(rv.flags, FMT_JSON)
        self.assertEqual(rv.value, b'"some json"blobs')

        self.cb.data_passthrough = False
        self.assertRaises(ValueFormatError, self.cb.get, "malformed")

    def test_zerolength(self):
        rv = self.cb.upsert("key", b"", format=FMT_BYTES)
        self.assertTrue(rv.success)
        rv = self.cb.get("key")
        self.assertEqual(rv.value, b"")

        self.assertRaises(CouchbaseError, self.cb.upsert, "", "value")

    def test_blob_keys_py2(self):
        if bytes == str:
            rv = self.cb.upsert(b"\0", "value")
            rv = self.cb.get(b"\0")
        else:
            self.assertRaises(ValueFormatError, self.cb.upsert, b"\0", "value")

    def test_compat_interop(self):
        # Check that we can interact with older versions, and vice versa:

        # Some basic sanity checks:
        self.assertEqual(0x00, FMT_JSON & FMT_LEGACY_MASK)
        self.assertEqual(0x01, FMT_PICKLE & FMT_LEGACY_MASK)
        self.assertEqual(0x02, FMT_BYTES & FMT_LEGACY_MASK)
        self.assertEqual(0x04, FMT_UTF8 & FMT_LEGACY_MASK)

        self.cb.transcoder = TranscoderPP()
        self.cb.upsert('foo', { 'foo': 'bar' }) # JSON
        self.cb.transcoder = LegacyTranscoderPP()
        rv = self.cb.get('foo')
        self.assertIsInstance(rv.value, dict)

        # Set it back now
        self.cb.upsert('foo', { 'foo': 'bar' })
        self.cb.transcoder = TranscoderPP()
        rv = self.cb.get('foo')
        self.assertIsInstance(rv.value, dict)
        self.assertEqual(rv.flags, FMT_JSON & FMT_LEGACY_MASK)


        ## Try with Bytes
        self.cb.transcoder = TranscoderPP()
        self.cb.upsert('bytesval', 'Hello World'.encode('utf-8'), format=FMT_BYTES)
        self.cb.transcoder = LegacyTranscoderPP()
        rv = self.cb.get('bytesval')
        self.assertEqual(FMT_BYTES, rv.flags)
        self.assertEqual('Hello World'.encode('utf-8'), rv.value)

        # Set it back
        self.cb.transcoder = LegacyTranscoderPP()
        self.cb.upsert('bytesval', 'Hello World'.encode('utf-8'), format=FMT_BYTES)
        self.cb.transcoder = TranscoderPP()
        rv = self.cb.get('bytesval')
        self.assertEqual(FMT_BYTES & FMT_LEGACY_MASK, rv.flags)
        self.assertEqual('Hello World'.encode('utf-8'), rv.value)
