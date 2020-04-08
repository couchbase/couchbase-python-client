from couchbase_tests.base import MockTestCase, AsyncClusterTestCase, ClusterTestCase
from functools import wraps
from collections import namedtuple
from acouchbase.cluster import Bucket, ACluster

Details = namedtuple('Details', ['factories', 'get_value'])

try:
    from acouchbase.cluster import Bucket, Cluster, get_event_loop
    from acouchbase.cluster import asyncio

    def asynct(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            future = f(*args, **kwargs)
            loop = get_event_loop()
            loop.run_until_complete(future)

        return wrapper


    def gen_collection(connection_string, *args, **kwargs):
        try:
            base_cluster = Cluster(connection_string, *args, **kwargs)
            base_bucket = base_cluster.bucket(*args, **kwargs)
            return base_bucket.default_collection()
        except Exception as e:
            raise


    default = Details({'Collection': gen_collection, 'Bucket': Bucket}, lambda x: x.content)
    target_dict = {
                   'Collection':   default}

except (ImportError, SyntaxError):
    target_dict = {}

targets = list(map(lambda x: (x,), target_dict.keys()))



class AioTestCase(AsyncClusterTestCase, ClusterTestCase):
    factory_name = None  # type: str

    def setUp(self, **kwargs):
        asyncio.set_event_loop(get_event_loop())
        super(AioTestCase, self).setUp(**kwargs)

    def __init__(self, *args, **kwargs):
        super(AioTestCase, self).__init__(*args, **kwargs)


    @property
    def cluster_class(self):  # type: (...) -> Cluster
        return ACluster

    @property
    def cluster_factory(self):
        return ACluster

    should_check_refcount = False
