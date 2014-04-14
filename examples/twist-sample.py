from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, Deferred

from txcouchbase.connection import Connection

class MyClient(object):
    def __init__(self):
        self.cb = Connection(bucket='default')
        self.do_set()

    def on_op_error(self, msg):
        print "Got operation error!" + str(msg)

    def do_set(self):
        self.cb.set("foo", "bar").addCallback(self.on_set)

    def on_set(self, res):
        print res
        self.cb.get("foo").addCallback(self.on_get)

    def on_get(self, res):
        print res

@inlineCallbacks
def run_sync_example():
    cb = Connection(bucket='default')
    rv_set = yield cb.set("foo", "bar")
    print rv_set
    rv_get = yield cb.get("foo")
    print rv_get

cb = MyClient()
run_sync_example()
reactor.run()
