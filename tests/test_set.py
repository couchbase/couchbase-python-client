from couchbase.exceptions import KeyExistsError
from couchbase.libcouchbase import Connection

from tests.base import CouchbaseTestCase


class ConnectionSetTest(CouchbaseTestCase):
    def setUp(self):
        super(ConnectionSetTest, self).setUp()
        self.cb = Connection(self.host, self.port, self.username, self.password,
                             self.bucket_prefix)

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


if __name__ == '__main__':
    unittest.main()
