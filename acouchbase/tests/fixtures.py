import asyncio
import unittest

from couchbase.tests.base import ConnectionConfiguration
from acouchbase.bucket import Bucket

from functools import wraps

config = ConnectionConfiguration()
cluster_info = config.realserver_info

if cluster_info:
    beer_bucket = cluster_info.make_connection(Bucket, bucket="beer-sample")
    default_bucket = cluster_info.make_connection(Bucket)
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
        if not cluster_info:
            self.skipTest("Real Server required.")
