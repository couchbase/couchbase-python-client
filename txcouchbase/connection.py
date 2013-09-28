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

from twisted.internet import reactor as tx_reactor
from twisted.internet.defer import Deferred
from twisted.python.failure import Failure

from couchbase.async.connection import Async
from couchbase.async.view import AsyncViewBase
from couchbase.async.events import EventQueue
from couchbase.exceptions import CouchbaseError
from txcouchbase.iops import v0Iops

class QAllView(AsyncViewBase):
    def __init__(self, *args, **kwargs):
        self._d = Deferred()
        self._rowsbuf = []
        super(QAllView, self).__init__(*args, **kwargs)

    def on_rows(self, rowiter):
        self._rowsbuf += [ x for x in rowiter ]

    def on_error(self, ex):
        if self._d:
            self._d.errback()
            self._d = None

    def on_done(self):
        if self._d:
            self._d.callback(self._rowsbuf)
            self._d = None

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
    def __init__(self, reactor=None, **kwargs):
        """
        Connection subclass for Twisted. This inherits from the 'Async' class,
        but also adds some twisted-specific logic for hooking on a connection.
        """
        if not reactor:
            reactor = tx_reactor

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

    def get(self, *args, **kwargs):
        return self._wrap(Async.get, *args, **kwargs)

    def set(self, *args, **kwargs):
        return self._wrap(Async.set, *args, **kwargs)

    def add(self, *args, **kwargs):
        return sef._wrap(Async.add, *args, **kwargs)

    def replace(self, *args, **kwargs):
        return self._wrap(Async.replace, *args, **kwargs)

    def append(self, *args, **kwargs):
        return self._wrap(Async.append, *args, **kwargs)

    def prepend(self, *args, **kwargs):
        return self._wrap(Async.prepend, *args, **kwargs)

    def delete(self, *args, **kwargs):
        return self._wrap(Async.delete, *args, **kwargs)

    def lock(self, *args, **kwargs):
        return self._wrap(Async.lock, *args, **kwargs)

    def unlock(self, *args, **kwargs):
        return self._wrap(Async.unlock, *args, **kwargs)

    def get_multi(self, *args, **kwargs):
        return self._wrap(Async.get_multi, *args, **kwargs)

    getMulti = get_multi

    def set_multi(self, *args, **kwargs):
        return self._wrap(Async.set_multi, *args, **kwargs)

    setMulti = set_multi

    def add_multi(self, *args, **kwargs):
        return self._wrap(Async.add_multi, *args, **kwargs)

    addMulti = add_multi

    def replace_multi(self, *args, **kwargs):
        return self._wrap(Async.replace_multi, *args, **kwargs)

    replaceMulti = replace_multi

    def append_multi(self, *args, **kwargs):
        return self._wrap(Async.append_multi, *args, **kwargs)

    appendMulti = append_multi

    def prepend_multi(self, *args, **kwargs):
        return self._wrap(Async.prepend_multi, *args, **kwargs)

    prependMulti = prepend_multi

    def lock_multi(self, *args, **kwargs):
        return self._wrap(Async.lock_multi, *args, **kwargs)

    lockMulti = lock_multi

    def unlock_multi(self, *args, **kwargs):
        return self._wrap(Async.unlock_multi, *args, **kwargs)

    unlockMulti = unlock_multi

    def query(self, *args, **kwargs):
        kwargs['itercls'] = QAllView
        ret = super(Connection, self).query(*args, **kwargs)
        ret.start_query()
        return ret._d
