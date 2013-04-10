import unittest

from couchbase import Couchbase
from couchbase.libcouchbase import Connection


class CouchbaseTest(unittest.TestCase):
    def test_is_instance_of_connection(self):
        self.assertIsInstance(Couchbase.connect('localhost', 8091),
                              Connection)


if __name__ == '__main__':
    unittest.main()
