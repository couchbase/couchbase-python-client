import unittest
import uuid

from couchbase.couchbaseclient import VBucketAwareCouchbaseClient

class CouchbaseClientTest(unittest.TestCase):
    url = "http://localhost:8091/pools/default"
    bucket = "default"
    client = None

    def setUp(self):
        self.client = VBucketAwareCouchbaseClient(self.url, self.bucket, "", True)

    def tearDown(self):
        self.client.done()

    def test_set_and_get(self):
        kvs = [(str(uuid.uuid4()), str(uuid.uuid4())) for i in range(0, 100)]
        for k, v in kvs:
            self.client.set(k, 0, 0, v)

        for k, v in kvs:
            self.client.get(k)

    def test_set_and_delete(self):
        kvs = [(str(uuid.uuid4()), str(uuid.uuid4())) for i in range(0, 100)]
        for k, v in kvs:
            self.client.set(k, 0, 0, v)
        for k, v in kvs:
            self.client.delete(k)
