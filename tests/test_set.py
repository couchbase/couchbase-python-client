import unittest
from time import sleep

from couchbase import FMT_JSON, FMT_PICKLE, FMT_PLAIN
from couchbase.exceptions import (KeyExistsError, ValueFormatError,
                                  ArgumentError, NotFoundError,
                                  NotStoredError)
from couchbase.libcouchbase import Connection

from tests.base import CouchbaseTestCase


class ConnectionSetTest(CouchbaseTestCase):
    def setUp(self):
        super(ConnectionSetTest, self).setUp()
        self.cb = Connection(self.host, self.port, self.username,
                             self.password, self.bucket_prefix)

    def test_trivial_set(self):
        cas = self.cb.set('key_trivial1', 'value1')
        self.assertTrue(cas > 0)
        cas = self.cb.set('key_trivial2', 'value2')
        self.assertTrue(cas > 0)

    def test_set_with_cas(self):
        cas1 = self.cb.set('key_cas', 'value1')
        self.assertTrue(cas1 > 0)

        self.assertRaises(KeyExistsError, self.cb.set,
                          'key_cas', 'value2', cas=cas1+1)

        cas2 = self.cb.set('key_cas', 'value3', cas=cas1)
        self.assertTrue(cas2 > 0)
        self.assertNotEqual(cas2, cas1)

        cas3 = self.cb.set('key_cas', 'value4')
        self.assertTrue(cas3 > 0)
        self.assertNotEqual(cas3, cas2)
        self.assertNotEqual(cas3, cas1)

    def test_set_with_ttl(self):
        self.cb.set('key_ttl', 'value_ttl', ttl=2)
        val = self.cb.get('key_ttl')
        self.assertEqual(val, 'value_ttl')
        # Make sure the key expires
        sleep(3)
        self.assertRaises(NotFoundError, self.cb.get, 'key_ttl')

    def test_set_format(self):
        cas1 = self.cb.set('key_format1', {'some': 'value1'}, format=FMT_JSON)
        self.assertTrue(cas1 > 0)
        self.assertRaises(ValueFormatError, self.cb.set,
                          'key_format2', object(), format=FMT_JSON)

        cas3 = self.cb.set('key_format3', {'some': 'value3'},
                           format=FMT_PICKLE)
        self.assertTrue(cas3 > 0)
        cas4 = self.cb.set('key_format4', object(), format=FMT_PICKLE)
        self.assertTrue(cas4 > 0)

        self.assertRaises(ValueFormatError, self.cb.set,
                          'key_format5', {'some': 'value5'}, format=FMT_PLAIN)
        cas6 = self.cb.set('key_format6', b'some value6', format=FMT_PLAIN)
        self.assertTrue(cas6 > 0)

    def test_multi_set(self):
        set1 = self.cb.set({'key_multi1': 'value1', 'key_multi3': 'value3',
                            'key_multi2': 'value2'})
        self.assertTrue(set1['key_multi1'] > 0)
        self.assertTrue(set1['key_multi2'] > 0)
        self.assertTrue(set1['key_multi3'] > 0)

        self.assertRaises(ArgumentError, self.cb.set,
                          {'key_multi4': 'value4', 'key_multi5': 'value5'},
                          cas = 123)

    def _make_bytes(self, s):
        # Workaround for PYCBC-90
        return bytes(bytearray(s, 'utf-8'))

    def test_append_nostr(self):
        self.cb.set("key", "value")
        cas = self.cb.append("key", self._make_bytes("a_string"))
        self.assertTrue(cas)

        self.assertRaises(ValueFormatError,
                          self.cb.append, "key", { "some" : "object" })

    def test_append_enoent(self):
        self.cb.delete("key", quiet=True)
        self.assertRaises(NotStoredError,
                          self.cb.append,"key", self._make_bytes("value"))

    def test_add(self):
        self.cb.delete("key", quiet=True)
        cas = self.cb.add("key", "value")
        self.assertTrue(cas)

        self.assertRaises(KeyExistsError,
                          self.cb.add, "key", "value")

    def test_replace(self):
        self.cb.set("key", "value")
        cas = self.cb.replace("key", "value")
        self.assertTrue(cas)

        cas = self.cb.replace("key", "value", cas=cas)
        self.assertTrue(cas)

        self.assertRaises(KeyExistsError,
                          self.cb.replace, "key", "value", cas=0xdeadbeef)

        self.cb.delete("key", quiet=True)
        self.assertRaises(NotFoundError,
                          self.cb.replace, "key", "value")


if __name__ == '__main__':
    unittest.main()
