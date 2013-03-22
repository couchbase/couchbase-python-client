from lcb import Couchbase

cb = Couchbase('127.0.0.1:8091')
cb.connect()

for i in range(0, 1000000):
    key = 'please-' + str(i)
    value = '{"it": "works", : ' + str(i) + '}'
    cb.set(key, value)
