# SyntaxError will trigger if yield or async is not supported
# ImportError will fail for python 3.3 because asyncio does not exist

import logging, traceback
from .fixtures import parameterize_asyncio


import acouchbase.tests.py34only
@parameterize_asyncio
class CouchbaseBeerKVTestSpecific(acouchbase.tests.py34only.CouchbaseBeerKVTest):
    pass
@parameterize_asyncio
class CouchbaseBeerKVTestSpecific(acouchbase.tests.py34only.CouchbaseBeerViewTest):
    pass

@parameterize_asyncio
class CouchbaseDefaultTestSpecificN1QL(acouchbase.tests.py34only.CouchbaseDefaultTestN1QL):
    pass

@parameterize_asyncio
class CouchbaseDefaultTestSpecificKV(acouchbase.tests.py34only.CouchbaseDefaultTestKV):
    pass

logging.error("Got exception {}".format(traceback.format_exc()))

try:
    import acouchbase.tests.py35only
    @parameterize_asyncio
    class CouchbasePy35TestSpecific(acouchbase.tests.py35only.CouchbasePy35Test):
        pass
except (ImportError, SyntaxError) as e:
    logging.error("Got exception {}".format(traceback.format_exc()))
