import unittest

from couchbase.libcouchbase import Connection
from couchbase.exceptions import ConnectError


class ConnectionTest(unittest.TestCase):
    def test_connection_valid(self):
        cb = Connection('localhost', 8091)
        # Connection didn't throw an error
        self.assertIsInstance(cb, Connection)

    def test_server_not_found(self):
        self.assertRaises(ConnectError, Connection, 'example.com', 8091)
        self.assertRaises(ConnectError, Connection, 'localhost', 34567)

if __name__ == '__main__':
    unittest.main()
