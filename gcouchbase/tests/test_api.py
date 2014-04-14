from couchbase.tests.base import ApiImplementationMixin, SkipTest
try:
    import gevent
except ImportError as e:
    raise SkipTest(e)

from gcouchbase.connection import GConnection, GView
from couchbase.tests.importer import get_configured_classes

class GEventImplMixin(ApiImplementationMixin):
    factory = GConnection
    viewfactor = GView
    should_check_refcount = False


skiplist = ('ConnectionIopsTest', 'LockmodeTest', 'ConnectionPipelineTest')

configured_classes = get_configured_classes(GEventImplMixin,
                                            skiplist=skiplist)
globals().update(configured_classes)
