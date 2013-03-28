from lcb import Couchbase

cb = Couchbase('127.0.0.1:8091')
cb.connect()

for i in range(0, 10):
    key = 'please-' + str(i)
    value = {"it": "works-" + str(i)}
    cb.set(key, value)

got = cb.get('please-5')
print(got)
