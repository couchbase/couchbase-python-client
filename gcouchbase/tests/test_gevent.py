from itertools import cycle

from couchbase_tests.base import SkipTest
try:
    import gevent
except ImportError as e:
    raise SkipTest(e)

from gcouchbase.cluster import Bucket, GView
from couchbase_tests.base import ConnectionTestCase


class GeventSpecificTest(ConnectionTestCase):
    factory = Bucket
    viewfactory = GView
    should_check_refcount = False

    def setUp(self):
        super(GeventSpecificTest, self).setUp()
        self.foo = self.gen_key('foo')
        self.bar = self.gen_key('bar')
        self.cb.upsert(self.foo, 'foo')
        self.cb.upsert(self.bar, 'bar')

    def test_killing_greenlet(self):
        def load_foo_and_bar_forever():
            for key, value in cycle([(self.foo, 'foo'), (self.bar, 'bar')]):
                try:
                    r = self.cb.get(key)
                except gevent.GreenletExit:
                    continue
                else:
                    self.assertEqual(r.value, value)
        def kill_greenlet_forever(g):
            while True:
                gevent.sleep(0.1)
                g.kill()
        g = gevent.spawn(load_foo_and_bar_forever)
        greenlets = [g, gevent.spawn(kill_greenlet_forever, g)]
        try:
            with gevent.Timeout(1, False):
                gevent.joinall(greenlets, raise_error=True)
        finally:
            gevent.killall(greenlets, StopIteration)

    def test_timeout(self):
        def load_foo_and_bar_forever():
            for key, value in cycle([(self.foo, 'foo'), (self.bar, 'bar')]):
                try:
                    with gevent.Timeout(0.0000001):
                        r = self.cb.get(key)
                except gevent.Timeout:
                    continue
                else:
                    self.assertEqual(r.value, value)
        g = gevent.spawn(load_foo_and_bar_forever)
        try:
            with gevent.Timeout(1, False):
                g.get()
        finally:
            g.kill(StopIteration)
