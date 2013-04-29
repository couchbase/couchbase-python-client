from couchbase import FMT_PLAIN
from couchbase.libcouchbase import Connection
from tests.base import CouchbaseTestCase
import sys

class ConnectionEncodingTest(CouchbaseTestCase):
    def setUp(self):
        super(ConnectionEncodingTest, self).setUp()
        self.cb = Connection(self.host, self.port, self.username,
                             self.password, self.bucket_prefix)

    def test_unicode(self):
        txt_orig = b'\xff\xfe\xe9\x05\xdc\x05\xd5\x05\xdd\x05'
        txt = txt_orig.decode('utf-16')
        cas = self.cb.set(txt, txt.encode('utf-16'), format=FMT_PLAIN)
        server_val = self.cb.get(txt)
        self.assertEquals(server_val, txt_orig)


    def test_blob(self):
        blob = b'\x00\x00\x00\xff'
        cas = self.cb.set("key", blob, format=FMT_PLAIN)
        self.assertTrue(cas)
        rv = self.cb.get("key")
        self.assertEquals(rv, blob)
