import gevent.monkey; gevent.monkey.patch_all()
import sys

from couchbase_v2.bucket import Bucket

def test(x):
    c = Bucket('couchbase://localhost/default', experimental_gevent_support=True)
    c.upsert("tmp-" + str(x), 1)
    sys.stdout.write(str(x) + " ")
    sys.stdout.flush()

print("Gevent starting..")
gevent.joinall([gevent.spawn(test, x) for x in xrange(100)])
print("")
