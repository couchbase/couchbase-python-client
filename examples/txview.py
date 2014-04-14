from twisted.internet import reactor

from txcouchbase.connection import Connection

def on_view_rows(res):
    for row in res:
        print "Got row", row.key

cb = Connection(bucket='beer-sample')
d = cb.queryAll("beer", "brewery_beers", limit=20)
d.addCallback(on_view_rows)
reactor.run()
