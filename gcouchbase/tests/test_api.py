from couchbase_tests.base import ApiImplementationMixin, SkipTest
try:
    import gevent
except ImportError as e:
    raise SkipTest(e)

from gcouchbase.bucket import Bucket, GView
from couchbase_tests.importer import get_configured_classes


class GEventImplMixin(ApiImplementationMixin):
    factory = Bucket
    viewfactory = GView
    should_check_refcount = True

    def _implDtorHook(self):
        import gc
        if not self.cb.closed:
            waiter = self.cb._get_close_future()
            del self.cb
            gc.collect()
            if not waiter.wait(7):
                raise Exception("Not properly cleaned up!")


skiplist = ('IopsTest', 'LockmodeTest', 'PipelineTest')

configured_classes = get_configured_classes(GEventImplMixin,
                                            skiplist=skiplist)

globals().update(configured_classes)
