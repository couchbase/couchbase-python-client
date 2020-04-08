# SyntaxError will trigger if yield or async is not supported
# ImportError will fail for python 3.3 because asyncio does not exist

import logging, traceback


import acouchbase.tests.py34only


class CouchbaseBeerKVTestSpecific(acouchbase.tests.py34only.CouchbaseBeerKVTest):
    pass


class AIOClusterTest(acouchbase.tests.py34only.AIOClusterTest):
    pass


class AIOAnalyticsClusterTest(acouchbase.tests.py34only.AnalyticsTest):
    pass


logging.error("Got exception {}".format(traceback.format_exc()))

try:
    import acouchbase.tests.py35only

    class CouchbasePy35TestSpecific(acouchbase.tests.py35only.CouchbasePy35Test):
        pass
except (ImportError, SyntaxError) as e:
    logging.error("Got exception {}".format(traceback.format_exc()))

