from collections import deque

from gevent.event import AsyncResult, Event
from gevent.hub import get_hub, getcurrent, Waiter

from couchbase.async.connection import Async
from couchbase.async.view import AsyncViewBase
from couchbase.views.iterator import AlreadyQueriedError
try:
    from gcouchbase.iops_gevent0x import IOPS
except ImportError:
    from gcouchbase.iops_gevent10 import IOPS

class GView(AsyncViewBase):
    def __init__(self, *args, **kwargs):
        """
        Subclass of :class:`~couchbase.async.view.AsyncViewBase`
        This doesn't expose an API different from the normal synchronous
        view API. It's just implemented differently
        """
        super(GView, self).__init__(*args, **kwargs)

        # We use __double_underscore to mangle names. This is because
        # the views class has quite a bit of data attached to it.
        self.__waiter = Waiter()
        self.__iterbufs = deque()
        self.__done_called = False

    def raise_include_docs(self):
        # We allow include_docs in the RowProcessor
        pass

    def _callback(self, *args):
        # Here we need to make sure the callback is invoked
        # from within the context of the calling greenlet. Since
        # we're invoked from the hub, we will need to issue more
        # blocking calls and thus ensure we're not doing the processing
        # from here.
        self.__waiter.switch(args)

    def on_rows(self, rows):
        self.__iterbufs.appendleft(rows)

    def on_error(self, ex):
        raise ex

    def on_done(self):
        self.__done_called = True

    def __wait_rows(self):
        """
        Called when we need more data..
        """
        args = self.__waiter.get()
        super(GView, self)._callback(*args)

    def __iter__(self):
        if not self._do_iter:
            raise AlreadyQueriedError.pyexc("Already queried")

        while self._do_iter and not self.__done_called:
            self.__wait_rows()
            while len(self.__iterbufs):
                ri = self.__iterbufs.pop()
                for r in ri:
                    yield r

        self._do_iter = False

class GConnection(Async):
    def __init__(self, *args, **kwargs):
        """
        This class is a 'GEvent'-optimized subclass of libcouchbase
        which utilizes the underlying IOPS structures and the gevent
        event primitives to efficiently utilize couroutine switching.
        """
        super(GConnection, self).__init__(IOPS(), *args, **kwargs)

    def _do_ctor_connect(self):
        if self.connected:
            return

        self._connect()
        self._evconn = AsyncResult()
        self._conncb = self._on_connected
        self._evconn.get()
        self._evconn = None

    def _on_connected(self, err):
        if err:
            self._evconn.set_exception(err)
        else:
            self._evconn.set(None)

    def _waitwrap(self, cbasync):
        cur_thread = getcurrent()
        cbasync.callback = cur_thread.switch
        cbasync.errback = lambda r, x, y, z: cur_thread.throw(x, y, z)

        return get_hub().switch()

    def _meth_factory(meth, name):
        def ret(self, *args, **kwargs):
            return self._waitwrap(meth(self, *args, **kwargs))
        return ret

    def _http_request(self, **kwargs):
        res = super(GConnection, self)._http_request(**kwargs)
        if kwargs.get('chunked', False):
            return res #views

        e = Event()
        res._callback = lambda x, y: e.set()

        e.wait()

        res._maybe_raise()
        return res

    def query(self, *args, **kwargs):
        kwargs['itercls'] = GView
        ret = super(GConnection, self).query(*args, **kwargs)
        ret.start()
        return ret

    locals().update(Async._gen_memd_wrappers(_meth_factory))
