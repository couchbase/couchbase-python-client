from couchbase_tests.base import MockTestCase
from functools import wraps
from parameterized import parameterized_class
from collections import namedtuple

Details = namedtuple('Details', ['factory', 'get_value'])

try:
    from acouchbase.bucket import Bucket
    from acouchbase.bucket import V3CoreClient
    from acouchbase.bucket import asyncio


    def asynct(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            future = f(*args, **kwargs)
            loop = asyncio.get_event_loop()
            loop.run_until_complete(future)

        return wrapper


    def gen_collection(*args, **kwargs):
        try:
            base_bucket = Bucket(*args, **kwargs)
            return base_bucket.default_collection()
        except Exception as e:
            raise


    target_dict = {'V3CoreClient': Details(V3CoreClient, lambda x: x.value),
                   'Collection': Details(gen_collection, lambda x: x.content)}

except (ImportError, SyntaxError):
    target_dict = {}

targets = list(map(lambda x: (x,), target_dict.keys()))


def parameterize_asyncio(cls):
    return parameterized_class(('factory_name',), targets)(cls)


class AioTestCase(MockTestCase):
    factory_name = None  # type: str

    def __init__(self, *args, **kwargs):
        self.details = target_dict[self.factory_name]
        super(AioTestCase, self).__init__(*args, **kwargs)

    @property
    def factory(self):
        return self.details.factory

    should_check_refcount = False
