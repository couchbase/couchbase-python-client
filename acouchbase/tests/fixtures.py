import asyncio
import unittest

from couchbase.tests.base import ConnectionConfiguration, MockResourceManager
from acouchbase.bucket import Bucket

from functools import wraps

config = ConnectionConfiguration()

manager = MockResourceManager(config)
mock_info = manager.make()
if mock_info:
    beer_bucket = mock_info.make_connection(Bucket, bucket="beer-sample")
    default_bucket = mock_info.make_connection(Bucket)
else:
    beer_bucket = None
    default_bucket = None


def asynct(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        future = f(*args, **kwargs)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)

    return wrapper

class AioTestCase(unittest.TestCase):

    def setUp(self):
        if not beer_bucket:
            self.skipTest("Mock Server required.")
