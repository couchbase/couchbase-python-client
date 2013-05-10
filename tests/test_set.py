from time import sleep

from couchbase import FMT_JSON, FMT_PICKLE, FMT_BYTES, FMT_UTF8

from couchbase.exceptions import (KeyExistsError, ValueFormatError,
                                  ArgumentError, NotFoundError,
                                  NotStoredError)

from tests.base import CouchbaseTestCase


class ConnectionSetTest(CouchbaseTestCase):
    def setUp(self):
        super(ConnectionSetTest, self).setUp()
        self.cb = self.make_connection()

    def test_trivial_set(self):
        rv = self.cb.set('key_trivial1', 'value1')
        self.assertTrue(rv)
        self.assertTrue(rv.cas > 0)
        rv = self.cb.set('key_trivial2', 'value2')
        self.assertTrue(rv.cas > 0)

    def test_set_with_cas(self):
        rv1 = self.cb.set('key_cas', 'value1')
        self.assertTrue(rv1.cas > 0)

        self.assertRaises(KeyExistsError, self.cb.set,
                          'key_cas', 'value2', cas=rv1.cas+1)

        rv2 = self.cb.set('key_cas', 'value3', cas=rv1.cas)
        self.assertTrue(rv2.cas > 0)
        self.assertNotEqual(rv1.cas, rv2.cas)

        rv3 = self.cb.set('key_cas', 'value4')
        self.assertTrue(rv3.cas > 0)
        self.assertNotEqual(rv3.cas, rv2.cas)
        self.assertNotEqual(rv3.cas, rv1.cas)

    def test_set_with_ttl(self):
        self.cb.set('key_ttl', 'value_ttl', ttl=2)
        rv = self.cb.get('key_ttl')
        self.assertEqual(rv.value, 'value_ttl')
        # Make sure the key expires
        sleep(3)
        self.assertRaises(NotFoundError, self.cb.get, 'key_ttl')

    def test_set_format(self):
        rv1 = self.cb.set('key_format1', {'some': 'value1'}, format=FMT_JSON)
        self.assertTrue(rv1.cas > 0)

        self.assertRaises(ValueFormatError, self.cb.set,
                          'key_format2', object(), format=FMT_JSON)

        rv3 = self.cb.set('key_format3', {'some': 'value3'},
                           format=FMT_PICKLE)
        self.assertTrue(rv3.cas > 0)
        rv4 = self.cb.set('key_format4', object(), format=FMT_PICKLE)
        self.assertTrue(rv4.cas > 0)

        self.assertRaises(ValueFormatError, self.cb.set,
                          'key_format5', {'some': 'value5'},
                          format=FMT_BYTES)
        self.assertRaises(ValueFormatError, self.cb.set,
                          'key_format5.1', { 'some' : 'value5.1'},
                          format=FMT_UTF8)

        rv6 = self.cb.set('key_format6', b'some value6', format=FMT_BYTES)
        self.assertTrue(rv6.cas > 0)

        rv7 = self.cb.set('key_format7', b"\x42".decode('utf-8'),
                          format=FMT_UTF8)
        self.assertTrue(rv7.success)
        

    def test_set_objects(self):
        for v in (None, False, True):
            for fmt in (FMT_JSON, FMT_PICKLE):
                rv = self.cb.set("key", v, format=fmt)
                self.assertTrue(rv.success)
                rv = self.cb.get("key")
                self.assertTrue(rv.success)
                self.assertEqual(rv.value, v)

    def test_multi_set(self):
        rvs = self.cb.set_multi({'key_multi1': 'value1', 'key_multi3': 'value3',
                            'key_multi2': 'value2'})
        self.assertTrue(rvs['key_multi1'].cas > 0)
        self.assertTrue(rvs['key_multi2'].cas > 0)
        self.assertTrue(rvs['key_multi3'].cas > 0)

        self.assertRaises((ArgumentError,TypeError), self.cb.set_multi,
                          {'key_multi4': 'value4', 'key_multi5': 'value5'},
                          cas = 123)

    def test_add(self):
        self.cb.delete("key", quiet=True)
        rv = self.cb.add("key", "value")
        self.assertTrue(rv.cas)

        self.assertRaises(KeyExistsError,
                          self.cb.add, "key", "value")

    def test_replace(self):
        rv = self.cb.set("key", "value")
        self.assertTrue(rv.success)

        rv = self.cb.replace("key", "value")
        self.assertTrue(rv.cas)

        rv = self.cb.replace("key", "value", cas=rv.cas)
        self.assertTrue(rv.cas)

        self.assertRaises(KeyExistsError,
                          self.cb.replace, "key", "value", cas=0xdeadbeef)

        self.cb.delete("key", quiet=True)
        self.assertRaises(NotFoundError,
                          self.cb.replace, "key", "value")


if __name__ == '__main__':
    unittest.main()
