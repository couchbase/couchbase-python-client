# Copyright 2013, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file contains the twisted-specific bits for the Couchbase client.
"""

from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.python.failure import Failure

from couchbase.async.bucket import AsyncBucket
from couchbase.async.view import AsyncViewBase
from couchbase.async.n1ql import AsyncN1QLRequest
from couchbase.async.events import EventQueue
from couchbase.exceptions import CouchbaseError
from txcouchbase.iops import v0Iops


class BatchedRowMixin(object):
    def __init__(self, *args, **kwargs):
        """
        Iterator/Container object for a single-call row-based results.

        This functions as an iterator over all results of the query, once the
        query has been completed.

        Additional metadata may be obtained by examining the object. See
        :class:`~couchbase.views.iterator.Views` for more details.

        You will normally not need to construct this object manually.
        """
        self._d = Deferred()
        self.__rows = [] # likely a superlcass might have this?

    def _getDeferred(self):
        return self._d

    def start(self):
        super(BatchedRowMixin, self).start()
        self.raw.rows_per_call = -1
        return self

    def on_rows(self, rowiter):
        """
        Reimplemented from :meth:`~AsyncViewBase.on_rows`
        """
        self.__rows = rowiter
        self._d.callback(self)
        self._d = None

    def on_error(self, ex):
        """
        Reimplemented from :meth:`~AsyncViewBase.on_error`
        """
        if self._d:
            self._d.errback()
            self._d = None

    def on_done(self):
        """
        Reimplemented from :meth:`~AsyncViewBase.on_done`
        """
        if self._d:
            self._d.callback(self)
            self._d = None

    def __iter__(self):
        """
        Iterate over the rows in this resultset
        """
        return iter(self.__rows)


class BatchedView(BatchedRowMixin, AsyncViewBase):
    def __init__(self, *args, **kwargs):
        AsyncViewBase.__init__(self, *args, **kwargs)
        BatchedRowMixin.__init__(self, *args, **kwargs)


class BatchedN1QLRequest(BatchedRowMixin, AsyncN1QLRequest):
    def __init__(self, *args, **kwargs):
        AsyncN1QLRequest.__init__(self, *args, **kwargs)
        BatchedRowMixin.__init__(self, *args, **kwargs)


class TxEventQueue(EventQueue):
    """
    Subclass of EventQueue. This implements the relevant firing methods,
    treating an 'Event' as a 'Deferred'
    """
    def fire_async(self, event):
        reactor.callLater(0, event.callback, None)

    def call_single_success(self, event, *args, **kwargs):
        event.callback(None)

    def call_single_failure(self, event, *args, **kwargs):
        event.errback(None)

class ConnectionEventQueue(TxEventQueue):
    """
    For events fired upon connect
    """
    def maybe_raise(self, err, *args, **kwargs):
        if not err:
            return
        raise err

class RawBucket(AsyncBucket):
    def __init__(self, connstr=None, **kwargs):
        """
        Bucket subclass for Twisted. This inherits from the 'AsyncBucket' class,
        but also adds some twisted-specific logic for hooking on a connection.
        """
        if connstr and 'connstr' not in kwargs:
            kwargs['connstr'] = connstr
        iops = v0Iops(reactor)
        super(RawBucket, self).__init__(iops=iops, **kwargs)

        self._evq = {
            'connect': ConnectionEventQueue(),
            '_dtor': TxEventQueue()
        }

        self._conncb = self._evq['connect']
        self._dtorcb = self._evq['_dtor']

    def registerDeferred(self, event, d):
        """
        Register a defer to be fired at the firing of a specific event.

        :param string event: Currently supported values are `connect`. Another
          value may be `_dtor` which will register an event to fire when this
          object has been completely destroyed.

        :param event: The defered to fire when the event succeeds or failes
        :type event: :class:`Deferred`

        If this event has already fired, the deferred will be triggered
        asynchronously.

        Example::

          def on_connect(*args):
              print("I'm connected")
          def on_connect_err(*args):
              print("Connection failed")

          d = Deferred()
          cb.registerDeferred('connect', d)
          d.addCallback(on_connect)
          d.addErrback(on_connect_err)

        :raise: :exc:`ValueError` if the event name is unrecognized
        """
        try:
            self._evq[event].schedule(d)
        except KeyError:
            raise ValueError("No such event type", event)

    def connect(self):
        """
        Short-hand for the following idiom::

            d = Deferred()
            cb.registerDeferred('connect', d)
            return d

        :return: A :class:`Deferred`
        """
        d = Deferred()
        self.registerDeferred('connect', d)
        return d

    def defer(self, opres):
        """
        Converts a raw :class:`couchbase.results.AsyncResult` object
        into a :class:`Deferred`.

        This is shorthand for the following "non-idiom"::

          d = Deferred()
          opres = cb.upsert("foo", "bar")
          opres.callback = d.callback

          def d_err(res, ex_type, ex_val, ex_tb):
              d.errback(opres, ex_type, ex_val, ex_tb)

          opres.errback = d_err
          return d

        :param opres: The operation to wrap
        :type opres: :class:`couchbase.results.AsyncResult`

        :return: a :class:`Deferred` object.

        Example::

          opres = cb.upsert("foo", "bar")
          d = cb.defer(opres)
          def on_ok(res):
              print("Result OK. Cas: {0}".format(res.cas))
          d.addCallback(opres)


        """
        d = Deferred()
        opres.callback = d.callback

        def _on_err(mres, ex_type, ex_val, ex_tb):
            try:
                raise ex_type(ex_val)
            except CouchbaseError:
                d.errback()
        opres.errback = _on_err
        return d

    def queryEx(self, viewcls, *args, **kwargs):
        """
        Query a view, with the ``viewcls`` instance receiving events
        of the query as they arrive.

        :param type viewcls: A class (derived from :class:`AsyncViewBase`)
          to instantiate

        Other arguments are passed to the standard `query` method.

        This functions exactly like the :meth:`~couchbase.async.AsyncBucket.query`
        method, except it automatically schedules operations if the connection
        has not yet been negotiated.
        """

        kwargs['itercls'] = viewcls
        o = super(AsyncBucket, self).query(*args, **kwargs)
        if not self.connected:
            self.connect().addCallback(lambda x: o.start())
        else:
            o.start()

        return o

    def queryAll(self, *args, **kwargs):
        """
        Returns a :class:`Deferred` object which will have its callback invoked
        with a :class:`BatchedView` when the results are complete.

        Parameters follow conventions of
        :meth:`~couchbase.bucket.Bucket.query`.

        Example::

          d = cb.queryAll("beer", "brewery_beers")
          def on_all_rows(rows):
              for row in rows:
                 print("Got row {0}".format(row))

          d.addCallback(on_all_rows)

        """

        if not self.connected:
            cb = lambda x: self.queryAll(*args, **kwargs)
            return self.connect().addCallback(cb)

        kwargs['itercls'] = BatchedView
        o = super(RawBucket, self).query(*args, **kwargs)
        o.start()
        return o._getDeferred()

    def n1qlQueryEx(self, cls, *args, **kwargs):
        """
        Execute a N1QL statement providing a custom handler for rows.

        This method allows you to define your own subclass (of
        :class:`~AsyncN1QLRequest`) which can handle rows as they are
        received from the network.

        :param cls: The subclass (not instance) to use
        :param args: Positional arguments for the class constructor
        :param kwargs: Keyword arguments for the class constructor

        .. seealso:: :meth:`queryEx`, around which this method wraps
        """
        kwargs['itercls'] = cls
        o = super(AsyncBucket, self).n1ql_query(*args, **kwargs)
        if not self.connected:
            self.connect().addCallback(lambda x: o.start())
        else:
            o.start()
        return o

    def n1qlQueryAll(self, *args, **kwargs):
        """
        Execute a N1QL query, retrieving all rows.

        This method returns a :class:`Deferred` object which is executed
        with a :class:`~.N1QLRequest` object. The object may be iterated
        over to yield the rows in the result set.

        This method is similar to :meth:`~couchbase.bucket.Bucket.n1ql_query`
        in its arguments.

        Example::

            def handler(req):
                for row in req:
                    # ... handle row

            d = cb.n1qlQueryAll('SELECT * from `travel-sample` WHERE city=$1`,
                            'Reno')
            d.addCallback(handler)

        :return: A :class:`Deferred`

        .. seealso:: :meth:`~couchbase.bucket.Bucket.n1ql_query`
        """
        if not self.connected:
            cb = lambda x: self.n1qlQueryAll(*args, **kwargs)
            return self.connect().addCallback(cb)

        kwargs['itercls'] = BatchedN1QLRequest
        o = super(RawBucket, self).n1ql_query(*args, **kwargs)
        o.start()
        return o._getDeferred()


class Bucket(RawBucket):
    def __init__(self, *args, **kwargs):
        """
        This class inherits from :class:`RawBucket`.
        In addition to the connection methods, this class' data access methods
        return :class:`Deferreds` instead of :class:`AsyncResult` objects.

        Operations such as :meth:`get` or :meth:`set` will invoke the
        :attr:`Deferred.callback` with the result object when the result is
        complete, or they will invoke the :attr:`Deferred.errback` with an
        exception (or :class:`Failure`) in case of an error. The rules of the
        :attr:`~couchbase.connection.Connection.quiet` attribute for raising
        exceptions apply to the invocation of the ``errback``. This means that
        in the case where the synchronous client would raise an exception,
        the Deferred API will have its ``errback`` invoked. Otherwise, the
        result's :attr:`~couchbase.result.Result.success` field should be
        inspected.


        Likewise multi operations will be invoked with a
        :class:`~couchbase.result.MultiResult` compatible object.

        Some examples:

        Using single items::

          d_set = cb.upsert("foo", "bar")
          d_get = cb.get("foo")

          def on_err_common(*args):
              print("Got an error: {0}".format(args)),
          def on_set_ok(res):
              print("Successfuly set key with CAS {0}".format(res.cas))
          def on_get_ok(res):
              print("Successfuly got key with value {0}".format(res.value))

          d_set.addCallback(on_set_ok).addErrback(on_err_common)
          d_get.addCallback(on_get_ok).addErrback(on_get_common)

          # Note that it is safe to do this as operations performed on the
          # same key are *always* performed in the order they were scheduled.

        Using multiple items::

          d_get = cb.get_multi(("Foo", "bar", "baz))
          def on_mres(mres):
              for k, v in mres.items():
                  print("Got result for key {0}: {1}".format(k, v.value))
          d.addCallback(mres)

        """
        super(Bucket, self).__init__(*args, **kwargs)

    def _connectSchedule(self, f, meth, *args, **kwargs):
        qop = Deferred()
        qop.addCallback(lambda x: f(meth, *args, **kwargs))
        self._evq['connect'].schedule(qop)
        return qop

    def _wrap(self, meth, *args, **kwargs):
        """
        Calls a given method with the appropriate arguments, or defers such
        a call until the instance has been connected
        """
        if not self.connected:
            return self._connectSchedule(self._wrap, meth, *args, **kwargs)

        opres = meth(self, *args, **kwargs)
        return self.defer(opres)


    ### Generate the methods
    def _meth_factory(meth, name):
        def ret(self, *args, **kwargs):
            return self._wrap(meth, *args, **kwargs)
        return ret

    locals().update(RawBucket._gen_memd_wrappers(_meth_factory))
    for x in RawBucket._MEMCACHED_OPERATIONS:
        if locals().get(x+'_multi', None):
            locals().update({x+"Multi": locals()[x+"_multi"]})
