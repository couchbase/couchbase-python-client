from twisted.internet import reactor

from txcouchbase.connection import Connection as TxCouchbase

cb = TxCouchbase(bucket='default')
def on_set(ret):
    print("Set key. Result", ret)

def on_get(ret):
    print("Got key. Result", ret)
    reactor.stop()

cb.set("key", "value").addCallback(on_set)
cb.get("key").addCallback(on_get)
reactor.run()
