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

from couchbase.async.connection import Async
from couchbase.async.view import AsyncViewBase
from couchbase.async.events import EventQueue
from couchbase.exceptions import CouchbaseError
from txcouchbase.iops import v0Iops

class BatchedView(AsyncViewBase):
    def __init__(self, *args, **kwargs):
        """
        Iterator/Container object for a single-call view result.

        This functions as an iterator over all results of the query, once the
        query has been completed.

        Additional metadata may be obtained by examining the object. See
        :class:`~couchbase.views.iterator.Views` for more details.

        You will normally not need to construct this object manually.
        """
        super(BatchedView, self).__init__(*args, **kwargs)
        self._d = Deferred()
        self.__rows = None # likely a superlcass might have this?

    def _getDeferred(self):
        return self._d

    def start(self):
        super(BatchedView, self).start()
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
        return self.__rows


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

class TxAsyncConnection(Async):
    def __init__(self, **kwargs):
        """
        Connection subclass for Twisted. This inherits from the 'Async' class,
        but also adds some twisted-specific logic for hooking on a connection.
        """

        iops = v0Iops(reactor)
        super(TxAsyncConnection, self).__init__(iops=iops, **kwargs)

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

        :param opres: The operation to wrap
        :type opres: :class:`couchbase.results.AsyncResult`

        :return: a :class:`Deferred` object.
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

class Connection(TxAsyncConnection):
    """
    This class inherits from TxAsyncConnection. In addition to the connection
    methods, this class' data access methods return Deferreds instead of
    AsyncResult objects
    """

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

    locals().update(TxAsyncConnection._gen_memd_wrappers(_meth_factory))
    for x in TxAsyncConnection._MEMCACHED_OPERATIONS:
        if locals().get(x+'_multi', None):
            locals().update({x+"Multi": locals()[x+"_multi"]})

    def queryEx(self, viewcls, *args, **kwargs):
        """
        Query a view, with the ``viewcls`` instance receiving events
        of the query as they arrive.

        :param type viewcls: A class (derived from :class:`AsyncViewBase`)
          to instantiate

        Other arguments are passed to the standard `query` method.

        This functions exactly like the :meth:`~couchbase.async.Connection.query`
        method, except it automatically schedules operations if the connection
        has not yet been negotiated.
        """

        kwargs['itercls'] = viewcls
        o = super(Connection, self).query(*args, **kwargs)
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
        :meth:`~couchbase.connection.Connection.query`.
        """

        if not self.connected:
            cb = lambda x: self.queryAll(*args, **kwargs)
            return self.connect().addCallback(cb)

        kwargs['itercls'] = BatchedView
        o = super(Connection, self).query(*args, **kwargs)
        o.start()
        return o._getDeferred()
