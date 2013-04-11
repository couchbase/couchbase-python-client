from couchbase import Couchbase
from couchbase.libcouchbase import Connection

from tests.base import CouchbaseTestCase


BUCKET_NAME = 'test_bucket_for_pythonsdk'


class CouchbaseTest(CouchbaseTestCase):
    def test_is_instance_of_connection(self):
        self.assertIsInstance(
            Couchbase.connect(self.host, self.port, self.username,
                              self.password, self.bucket_prefix),
            Connection)


if __name__ == '__main__':
    unittest.main()
