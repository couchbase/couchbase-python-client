import asyncio
import unittest

from couchbase.tests.base import ConnectionConfiguration, MockResourceManager, MockTestCase
from acouchbase.bucket import Bucket

from functools import wraps


def asynct(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        future = f(*args, **kwargs)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)

    return wrapper

class AioTestCase(MockTestCase):
    factory = Bucket
    should_check_refcount = False
