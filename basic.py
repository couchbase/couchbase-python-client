from couchbase import Couchbase, CB_FMT_PICKLE

cb = Couchbase('127.0.0.1:8091')
#cb.default_format = CB_FMT_PICKLE
cb.connect()

for i in range(0, 10):
    key = 'please-' + str(i)
    value = {"it": "works-" + str(i)}
    cb.set(key, value)

got = cb.get('please-5')
print(got)
