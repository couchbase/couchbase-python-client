from couchbase.tests.base import SkipTest
try:
    import gevent
except ImportError as e:
    raise SkipTest(e)

from gcouchbase.bucket import Bucket, GView
from couchbase.tests.base import ConnectionTestCase


class GeventSpecificTest(ConnectionTestCase):
    factory = Bucket
    viewfactory = GView
    should_check_refcount = False

    def test_killing_greenlet(self):
        foo = self.gen_key('foo')
        bar = self.gen_key('bar')
        self.cb.upsert(foo, 'foo')
        self.cb.upsert(bar, 'bar')
        def load_foo_and_bar_forever():
            while True:
                try:
                    r = self.cb.get(foo)
                except gevent.GreenletExit:
                    continue
                else:
                    self.assertEqual(r.value, 'foo')
                try:
                    r = self.cb.get(bar)
                except gevent.GreenletExit:
                    continue
                else:
                    self.assertEqual(r.value, 'bar')
        def kill_greenlet_forever(g):
            while True:
                gevent.sleep(0.1)
                g.kill()
        g = gevent.spawn(load_foo_and_bar_forever)
        greenlets = [g, gevent.spawn(kill_greenlet_forever, g)]
        with gevent.Timeout(1, False):
            gevent.joinall(greenlets, raise_error=True)
