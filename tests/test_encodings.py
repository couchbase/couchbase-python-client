from couchbase import FMT_BYTES, FMT_JSON, FMT_PICKLE, FMT_UTF8
from couchbase.libcouchbase import Connection
from couchbase.exceptions import ValueFormatError, CouchbaseError
from tests.base import CouchbaseTestCase
from nose.exc import SkipTest


BLOB_ORIG =  b'\xff\xfe\xe9\x05\xdc\x05\xd5\x05\xdd\x05'

class ConnectionEncodingTest(CouchbaseTestCase):
    def setUp(self):
        super(ConnectionEncodingTest, self).setUp()
        self.cb = self.make_connection()

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
        cb = self.make_connection()
        cb.data_passthrough = True
        cb.set("malformed", "some json")
        cb.append("malformed", "blobs")
        rv = cb.get("malformed")

        self.assertTrue(rv.success)
        self.assertEqual(rv.flags, FMT_JSON)
        self.assertEqual(rv.value, b'"some json"blobs')

        cb.data_passthrough = False
        self.assertRaises(ValueFormatError, cb.get, "malformed")

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
