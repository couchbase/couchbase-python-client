from gevent import GreenletExit
from gevent.event import AsyncResult, Event
from gevent.hub import get_hub, getcurrent, Waiter

from couchbase.async.bucket import AsyncBucket
from couchbase.async.view import AsyncViewBase
from couchbase.async.n1ql import AsyncN1QLRequest
from couchbase.views.iterator import AlreadyQueriedError
try:
    from gcouchbase.iops_gevent0x import IOPS
except ImportError:
    from gcouchbase.iops_gevent10 import IOPS


class GRowsHandler(object):
    def __init__(self):
        """
        Subclass of :class:`~.AsyncViewBase`
        This doesn't expose an API different from the normal
        synchronous view API. It's just implemented differently
        """

        # We use __double_underscore to mangle names. This is because
        # the views class has quite a bit of data attached to it.
        self.__waiter = Waiter()
        self.__raw_rows = []
        self.__done_called = False
        self.start()
        self.raw.rows_per_call = 100000

    def _callback(self, *args):
        # This method overridden from the parent. Rather than do the processing
        # on demand, we must defer it for later. This is done by copying the
        # rows to a list. In the typical case we shouldn't accumulate all
        # the rows in the buffer (since .switch() will typically have something
        # waiting for us). However if the view is destroyed prematurely,
        # or if the user is not actively iterating over us, or if something
        # else happens (such as more rows arriving during a get request with
        # include_docs), we simply accumulate the rows here.
        self.__raw_rows.append(self.raw.rows)
        if self.raw.done:
            self._clear()
        self.__waiter.switch()

    def _errback(self, mres, *args):
        self._clear()
        self.__waiter.throw(*args)

    def __iter__(self):
        if not self._do_iter:
            raise AlreadyQueriedError.pyexc("Already queried")

        while self._do_iter and not self.__done_called:
            self.__waiter.get()

            rowset_list = self.__raw_rows
            self.__raw_rows = []
            for rowset in rowset_list:
                for row in self._process_payload(rowset):
                    yield row

        self._do_iter = False


class GView(GRowsHandler, AsyncViewBase):
    def __init__(self, *args, **kwargs):
        AsyncViewBase.__init__(self, *args, **kwargs)
        GRowsHandler.__init__(self)


class GN1QLRequest(GRowsHandler, AsyncN1QLRequest):
    def __init__(self, *args, **kwargs):
        AsyncN1QLRequest.__init__(self, *args, **kwargs)
        GRowsHandler.__init__(self)


def dummy_callback(*args):
    pass


class Bucket(AsyncBucket):
    def __init__(self, *args, **kwargs):
        """
        This class is a 'GEvent'-optimized subclass of libcouchbase
        which utilizes the underlying IOPS structures and the gevent
        event primitives to efficiently utilize couroutine switching.
        """
        super(Bucket, self).__init__(IOPS(), *args, **kwargs)

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
        errback = lambda r, x, y, z: cur_thread.throw(x, y, z)
        cbasync.set_callbacks(cur_thread.switch, errback)
        try:
            return get_hub().switch()
        finally:
            # Deregister callbacks to prevent another request on the same
            # greenlet to get the result from this context.
            cbasync.set_callbacks(dummy_callback, dummy_callback)

    def _meth_factory(meth, name):
        def ret(self, *args, **kwargs):
            return self._waitwrap(meth(self, *args, **kwargs))
        return ret

    def _http_request(self, **kwargs):
        res = super(Bucket, self)._http_request(**kwargs)

        w = Waiter()
        res.callback = lambda x: w.switch(x)
        res.errback = lambda x, c, o, b: w.throw(c, o, b)
        return w.get()

    def query(self, *args, **kwargs):
        kwargs['itercls'] = GView
        return super(Bucket, self).query(*args, **kwargs)

    def n1ql_query(self, query, *args, **kwargs):
        kwargs['itercls'] = GN1QLRequest
        return super(Bucket, self).n1ql_query(query, *args, **kwargs)

    def _get_close_future(self):
        ev = Event()
        def _dtor_cb(*args):
            ev.set()
        self._dtorcb = _dtor_cb
        return ev


    locals().update(AsyncBucket._gen_memd_wrappers(_meth_factory))
