from couchbase.exceptions import (AuthError, BucketNotFoundError, ConnectError,
                                  ArgumentError)
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


if __name__ == '__main__':
    unittest.main()
