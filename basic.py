from couchbase import Couchbase, FMT_PICKLE

cb = Couchbase.connect('127.0.0.1', 8091, '', '', 'default')
#cb.default_format = FMT_PICKLE

for i in range(0, 10):
    key = 'please-' + str(i)
    value = {"it": "works-" + str(i)}
    cb.set(key, value)

got = cb.get('please-5')
print(got)
