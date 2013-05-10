from couchbase import Couchbase
from couchbase.libcouchbase import Connection

from tests.base import CouchbaseTestCase


BUCKET_NAME = 'test_bucket_for_pythonsdk'


class CouchbaseTest(CouchbaseTestCase):
    def test_is_instance_of_connection(self):
        self.assertIsInstance(
            Couchbase.connect(host=self.host,
                              port=self.port,
                              username=self.username,
                              password=self.password,
                              bucket=self.bucket_prefix),
            Connection)


if __name__ == '__main__':
    unittest.main()
