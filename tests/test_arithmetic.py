from couchbase.exceptions import (NotFoundError, DeltaBadvalError)
from couchbase.libcouchbase import Connection

from tests.base import CouchbaseTestCase


class ConnectionArithmeticTest(CouchbaseTestCase):
    def setUp(self):
        super(ConnectionArithmeticTest, self).setUp()
        self.cb = Connection(self.host, self.port, self.username,
                             self.password, self.bucket_prefix)

    def test_trivial_incrdecr(self):
        self.cb.delete("key", quiet=True)
        rv = self.cb.incr("key", initial=1)
        val = self.cb.get("key")
        self.assertEquals(int(val), 1)

        rv = self.cb.incr("key")
        self.assertEquals(rv, 2)

        rv = self.cb.decr("key")
        self.assertEquals(rv, 1)
        self.assertEquals(int(self.cb.get("key")), 1)

        rv = self.cb.decr("key")
        self.assertEquals(rv, 0)
        self.assertEquals(int(self.cb.get("key")), 0)

    def test_incr_notfound(self):
        self.cb.delete("key", quiet=True)
        self.assertRaises(NotFoundError,
                          self.cb.incr, "key")

    def test_incr_badval(self):
        self.cb.set("key", "THIS IS SPARTA")
        self.assertRaises(DeltaBadvalError,
                          self.cb.incr, "key")

    def test_incr_multi(self):
        keys = []
        for x in range(5):
            keys.append("Key_" + str(x))

        self.cb.delete(keys, quiet=True)
        self.cb.incr(keys, initial=5)
        for k in keys:
            rv = self.cb.get(k)
            self.assertEquals(int(rv), 5)

        self.cb.delete(keys[0])

        self.assertRaises(NotFoundError,
                          self.cb.incr, keys)

    def test_incr_extended(self):
        self.cb.delete("key", quiet=True)
        rv = self.cb.incr("key", extended=True, initial=10)
        self.assertEquals(rv.value, 10)
        srv = self.cb.set("key", "42", cas=rv.cas)
        self.assertTrue(srv)

        # test with multiple values?
        klist = [ "key_" + str(x) for x in range(10) ]
        self.cb.delete(klist, quiet=True)
        rvs = self.cb.incr(klist, initial=40, extended=True)
        [ self.assertEquals(x.value, 40) for x in rvs.values() ]
        self.assertEquals(sorted(list(rvs.keys())), sorted(klist))


if __name__ == '__main__':
    unittest.main()
