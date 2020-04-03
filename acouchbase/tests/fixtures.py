from couchbase_tests.base import MockTestCase
from functools import wraps
from parameterized import parameterized_class
from collections import namedtuple
from acouchbase.cluster import Bucket


Details = namedtuple('Details', ['factories', 'get_value'])

try:
    from acouchbase.cluster import Bucket, get_event_loop
    from acouchbase.cluster import V3CoreClient
    from acouchbase.cluster import asyncio

    def asynct(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            future = f(*args, **kwargs)
            loop = get_event_loop()
            loop.run_until_complete(future)

        return wrapper


    def gen_collection(*args, **kwargs):
        try:
            base_bucket = Bucket(*args, **kwargs)
            return base_bucket.default_collection()
        except Exception as e:
            raise


    default = Details({'Collection': gen_collection, 'Bucket': Bucket}, lambda x: x.content)
    target_dict = {'V3CoreClient': Details({'Collection': V3CoreClient, 'Bucket': V3CoreClient}, lambda x: x.value),
                   'Collection':   default}

except (ImportError, SyntaxError):
    target_dict = {}

targets = list(map(lambda x: (x,), target_dict.keys()))


def parameterize_asyncio(cls):
    return parameterized_class(('factory_name',), targets)(cls)


class AioTestCase(MockTestCase):
    factory_name = None  # type: str

    def setUp(self, type='Collection', **kwargs):
        asyncio.set_event_loop(get_event_loop())
        self._factory = self.details.factories[type]
        super(AioTestCase, self).setUp(**kwargs)

    def __init__(self, *args, **kwargs):
        self.details = target_dict.get(self.factory_name,default)
        super(AioTestCase, self).__init__(*args, **kwargs)

    @property
    def factory(self):
        return self._factory

    should_check_refcount = False
