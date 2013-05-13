from tests.base import CouchbaseTestCase
from couchbase.exceptions import ArgumentError, ValueFormatError
from couchbase.libcouchbase import FMT_UTF8

class ConnectionItertypeTest(CouchbaseTestCase):
    def setUp(self):
        super(ConnectionItertypeTest, self).setUp()
        self.cb = self.make_connection()

    def test_itertypes(self):
        kvs = {}
        for x in range(10):
            k = "key_" + str(x)
            v = "value_" + str(x)
            kvs[k] = v

        intlist = set(["k1", "k2", "k3"])

        self.cb.delete_multi(kvs.keys(), quiet=True)
        self.cb.set_multi(kvs)
        self.cb.get_multi(kvs.keys())
        self.cb.get_multi(kvs.values(), quiet=True)

        self.cb.incr_multi(intlist, initial=10)
        self.cb.decr_multi(intlist)
        self.cb.get_multi(intlist)

    def test_bad_elements(self):
        badlist = ("key1", None, "key2")
        for fn in (self.cb.incr_multi,
                   self.cb.delete_multi,
                   self.cb.get_multi):
            self.assertRaises(
                (ArgumentError, ValueFormatError),
                fn, badlist)

        self.assertRaises(
            (ArgumentError, ValueFormatError),
            self.cb.set_multi,
            { None: "value" })

        self.assertRaises(ValueFormatError,
                          self.cb.set_multi,
                          { "Value" : None},
                          format=FMT_UTF8)

    def test_iterclass(self):
        class IterTemp(object):
            def __init__(self, gen_ints = False, badlen=False):
                self.current = 0
                self.max = 5
                self.gen_ints = gen_ints
                self.badlen = badlen

            def __iter__(self):
                while self.current < self.max:
                    ret = self.current
                    if not self.gen_ints:
                        ret = "Key_" + str(ret)
                    self.current += 1
                    yield ret

            def __len__(self):
                if self.badlen:
                    return 100
                return self.max

        self.cb.incr_multi(IterTemp(gen_ints = False), initial=10)
        self.cb.decr_multi(IterTemp(gen_ints = False), initial=10)
        self.cb.get_multi(IterTemp(gen_ints=False))
        self.cb.delete_multi(IterTemp(gen_ints = False))

        # Try with a mismatched len-iter
        self.assertRaises(ArgumentError,
                          self.cb.get_multi,
                          IterTemp(gen_ints=False, badlen=True))
