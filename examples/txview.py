from twisted.internet import reactor

from txcouchbase.bucket import Bucket

def on_view_rows(res):
    for row in res:
        print "Got row", row.key

cb = Bucket('couchbase://localhost/beer-sample')
d = cb.queryAll("beer", "brewery_beers", limit=20)
d.addCallback(on_view_rows)
reactor.run()
