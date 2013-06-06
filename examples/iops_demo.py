#!/usr/bin/env python
import gevent
import gevent.monkey; gevent.monkey.patch_all()
import sys

from couchbase import Couchbase

def test(x):
    c = Couchbase.connect(bucket='default', experimental_gevent_support=True)
    c.set("tmp-" + str(x), 1)
    sys.stdout.write(str(x) + " ")
    sys.stdout.flush()

print("Gevent starting..")
gevent.joinall([gevent.spawn(test, x) for x in xrange(100)])
print("")
