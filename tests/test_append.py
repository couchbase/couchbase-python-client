from couchbase import FMT_JSON, FMT_PICKLE, FMT_BYTES, FMT_UTF8

from couchbase.exceptions import (KeyExistsError, ValueFormatError,
                                  ArgumentError, NotFoundError,
                                  NotStoredError)

from tests.base import CouchbaseTestCase


class ConnectionAppendTest(CouchbaseTestCase):
    def setUp(self):
        super(ConnectionAppendTest, self).setUp()
        self.cb = self.make_connection()

    def test_append_prepend(self):
        vbase = "middle"
        self.cb.set("append_key", vbase, format=FMT_UTF8)
        self.cb.prepend("append_key", "begin ")
        self.cb.append("append_key", " end")
        self.assertEquals(self.cb.get("append_key").value,
                          "begin middle end")


    def test_append_binary(self):
        initial = b'\x10'
        kname = "binary_append"
        self.cb.set(kname, initial, format=FMT_BYTES)
        self.cb.append(kname, b'\x20', format=FMT_BYTES)
        self.cb.prepend(kname, b'\x00', format=FMT_BYTES)

        res = self.cb.get(kname)
        self.assertEqual(res.value, b'\x00\x10\x20')

    def test_append_nostr(self):
        self.cb.set("key", "value")
        rv = self.cb.append("key", "a_string")
        self.assertTrue(rv.cas)

        self.assertRaises(ValueFormatError,
                          self.cb.append, "key", { "some" : "object" })

    def test_append_enoent(self):
        self.cb.delete("key", quiet=True)
        self.assertRaises(NotStoredError,
                          self.cb.append,"key", "value")

    def test_append_multi(self):
        kv = { }
        for x in range(4):
            kv["append_" + str(x)] = "middle_" + str(x)

        self.cb.set_multi(kv, format=FMT_UTF8)
        self.cb.append_multi(kv)
        self.cb.prepend_multi(kv)

        rvs = self.cb.get_multi(list(kv.keys()))
        self.assertTrue(rvs.all_ok)
        self.assertEqual(len(rvs), 4)

        for k, v in rvs.items():
            basekey = kv[k]
            self.assertEqual(v.value, basekey * 3)
