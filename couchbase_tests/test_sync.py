from couchbase.bucket import Bucket
from couchbase_core.views.iterator import View
from .base import ApiImplementationMixin
from .importer import get_configured_classes


class SyncImplMixin(ApiImplementationMixin):
    factory = Bucket
    viewfactory = View
    should_check_refcount = True


configured_cases = get_configured_classes(SyncImplMixin)
globals().update(configured_cases)
