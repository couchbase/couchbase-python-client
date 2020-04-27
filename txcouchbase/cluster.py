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

from typing import *

from twisted.internet import reactor
from twisted.internet.defer import Deferred

from couchbase_core.asynchronous.analytics import AsyncAnalyticsRequest
from couchbase.asynchronous import AsyncViewResult, AsyncQueryResultBase, AsyncAnalyticsResultBase, AsyncSearchResult
from couchbase.cluster import Cluster as V3SyncCluster, AsyncCluster as V3AsyncCluster
from couchbase.collection import AsyncCBCollection as BaseAsyncCBCollection
from couchbase_core.asynchronous.events import EventQueue
from couchbase.asynchronous.search import AsyncSearchRequest
from couchbase_core.asynchronous.n1ql import AsyncN1QLRequest
from couchbase_core.asynchronous.view import AsyncViewBase
from couchbase_core.client import Client as CoreClient
from couchbase.exceptions import CouchbaseException
from couchbase_core.supportability import internal
from txcouchbase.iops import v0Iops
from couchbase.bucket import AsyncBucket as V3AsyncBucket


class BatchedRowMixin(object):
    def __init__(self, *args, **kwargs):
        """
        Iterator/Container object for a single-call row-based results.

        This functions as an iterator over all results of the query, once the
        query has been completed.

        Additional metadata may be obtained by examining the object. See
        :class:`~couchbase_core.views.iterator.Views` for more details.

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


class BatchedViewResult(BatchedRowMixin, AsyncViewResult):
    def __init__(self, *args, **kwargs):
        AsyncViewResult.__init__(self, *args, **kwargs)
        BatchedRowMixin.__init__(self, *args, **kwargs)


class BatchedN1QLRequest(BatchedRowMixin, AsyncN1QLRequest):
    def __init__(self, *args, **kwargs):
        AsyncN1QLRequest.__init__(self, *args, **kwargs)
        BatchedRowMixin.__init__(self, *args, **kwargs)


class BatchedQueryResult(BatchedRowMixin, AsyncQueryResultBase):
    def __init__(self, *args, **kwargs):
        AsyncQueryResultBase.__init__(self, *args, **kwargs)
        BatchedRowMixin.__init__(self, *args, **kwargs)


class BatchedAnalyticsRequest(BatchedRowMixin, AsyncAnalyticsRequest):
    def __init__(self, *args, **kwargs):
        AsyncAnalyticsRequest.__init__(self, *args, **kwargs)
        BatchedRowMixin.__init__(self, *args, **kwargs)


class BatchedAnalyticsResult(BatchedRowMixin, AsyncAnalyticsResultBase):
    def __init__(self, *args, **kwargs):
        AsyncAnalyticsResultBase.__init__(self, *args, **kwargs)
        BatchedRowMixin.__init__(self, *args, **kwargs)


class BatchedSearchRequest(BatchedRowMixin, AsyncSearchRequest):
    def __init__(self, *args, **kwargs):
        AsyncSearchRequest.__init__(self, *args, **kwargs)
        BatchedRowMixin.__init__(self, *args, **kwargs)


class BatchedSearchResult(BatchedRowMixin, AsyncSearchResult):
    def __init__(self, *args, **kwargs):
        AsyncSearchResult.__init__(self, *args, **kwargs)
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


T = TypeVar('T', bound=CoreClient)


class TxRawClientMixin(object):
    @internal
    def __init__(self, connstr=None, **kwargs):
        """
        Client mixin for Twisted. This inherits from an 'AsyncClient' class,
        but also adds some twisted-specific logic for hooking on a connection.
        """
        if connstr and 'connstr' not in kwargs:
            kwargs['connstr'] = connstr
        iops = v0Iops(reactor)
        super(TxRawClientMixin, self).__init__(iops=iops, **kwargs)

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

    def on_connect(self):
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
        Converts a raw :class:`couchbase_core.results.AsyncResult` object
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
        :type opres: :class:`couchbase_core.results.AsyncResult`

        :return: a :class:`Deferred` object.

        Example::

          opres = cb.upsert("foo", "bar")
          d = cb.defer(opres)
          def on_ok(res):
              print("Result OK. Cas: {0}".format(res.cas))
          d.addCallback(opres)


        """
        d = Deferred()

        def _on_err(mres, ex_type, ex_val, ex_tb):
            try:
                raise ex_type(ex_val)
            except CouchbaseException:
                d.errback()

        opres.set_callbacks(d.callback, _on_err)
        return d

    def deferred_verb(self, itercls, raw_verb, cooked_verb, *args, **kwargs):
        if not self.connected:
            cb = lambda x: cooked_verb(*args, **kwargs)
            return self.on_connect().addCallback(cb)
        kwargs['itercls'] = itercls
        o = raw_verb(*args, **kwargs)
        o.start()
        return o._getDeferred()

    connected = CoreClient.connected


class TxDeferredClientMixin(TxRawClientMixin):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "TxDeferred_Wrapped"):
            for k, v in cls._gen_memd_wrappers(TxDeferredClientMixin._meth_factory).items():
                setattr(cls, k, v)
            cls.TxDeferred_Wrapped = True
        return super(TxDeferredClientMixin, cls).__new__(cls, *args, **kwargs)

    @internal
    def __init__(self, *args, **kwargs):
        """
        This mixin inherits from :class:`TxRawClientMixin`.
        In addition to the connection methods, this class' data access methods
        return :class:`Deferreds` instead of :class:`Result` objects.

        Operations such as :meth:`get` or :meth:`set` will invoke the
        :attr:`Deferred.callback` with the result object when the result is
        complete, or they will invoke the :attr:`Deferred.errback` with an
        exception (or :class:`Failure`) in case of an error. The rules of the
        :attr:`~couchbase_core.client.Client.quiet` attribute for raising
        exceptions apply to the invocation of the ``errback``. This means that
        in the case where the synchronous client would raise an exception,
        the Deferred API will have its ``errback`` invoked. Otherwise, the
        result's :attr:`~couchbase_v2.result.Result.success` field should be
        inspected.


        Likewise multi operations will be invoked with a
        :class:`~couchbase.result.MultiResultBase` compatible object.

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

          d_get = cb.get_multi(("Foo", "bar", "baz"))
          def on_mres(mres):
              for k, v in mres.items():
                  print("Got result for key {0}: {1}".format(k, v.value))
          d_get.addCallback(on_mres)

        """
        super(TxDeferredClientMixin, self).__init__(*args, **kwargs)

    def _connectSchedule(self, f, meth, *args, **kwargs):
        qop = Deferred()
        qop.addCallback(lambda x: f(meth, *args, **kwargs))
        self._evq['connect'].schedule(qop)
        return qop

    def _wrap(self,  # type: TxDeferredClient
              meth, *args, **kwargs):
        """
        Calls a given method with the appropriate arguments, or defers such
        a call until the instance has been connected
        """
        if not self.connected:
            return self._connectSchedule(self._wrap, meth, *args, **kwargs)

        opres = meth(self, *args, **kwargs)
        return self.defer(opres)


    ### Generate the methods
    @staticmethod
    def _meth_factory(meth, _):
        def ret(self, *args, **kwargs):
            return self._wrap(meth, *args, **kwargs)
        return ret


class TxRawCollection(TxRawClientMixin, BaseAsyncCBCollection):
    pass


class TxCollection(TxDeferredClientMixin, TxRawCollection):
    pass


class TxRawBucket(TxRawClientMixin, V3AsyncBucket):
    @internal
    def __init__(self, *args, **kwargs):
        super(TxRawBucket, self).__init__(collection_factory=kwargs.pop('collection_factory', TxRawCollection), *args, **kwargs)

    def view_query_ex(self, viewcls, *args, **kwargs):
        """
        Query a view, with the ``viewcls`` instance receiving events
        of the query as they arrive.

        :param type viewcls: A class (derived from :class:`AsyncViewBase`)
          to instantiate

        Other arguments are passed to the standard `query` method.

        This functions exactly like the :meth:`~couchbase.asynchronous.AsyncClient.query`
        method, except it automatically schedules operations if the connection
        has not yet been negotiated.
        """

        kwargs['itercls'] = viewcls
        o = super(TxRawBucket, self).view_query(*args, **kwargs)
        if not self.connected:
            self.on_connect().addCallback(lambda x: o.start())
        else:
            o.start()

        return o

    def view_query(self, *args, **kwargs):
        """
        Returns a :class:`Deferred` object which will have its callback invoked
        with a :class:`BatchedView` when the results are complete.

        Parameters follow conventions of
        :meth:`~couchbase_v2.bucket.Bucket.query`.

        Example::

          d = cb.queryAll("beer", "brewery_beers")
          def on_all_rows(rows):
              for row in rows:
                 print("Got row {0}".format(row))

          d.addCallback(on_all_rows)

        """

        if not self.connected:
            cb = lambda x: self.view_query(*args, **kwargs)
            return self.on_connect().addCallback(cb)

        kwargs['itercls'] = BatchedViewResult
        o = super(TxRawBucket, self).view_query(*args, **kwargs)
        try:
            o.start()
        except Exception as e:
            raise
        return o._getDeferred()


class TxBucket(TxDeferredClientMixin, TxRawBucket):
    @internal
    def __init__(self, *args, **kwargs):
        super(TxBucket,self).__init__(collection_factory=TxCollection, *args, **kwargs)


class TxBaseCluster(TxRawClientMixin, V3AsyncCluster):
    def bucket(self, *args, **kwargs):
        return super(TxBaseCluster, self).bucket(*args, **kwargs)


class TxRawCluster(TxBaseCluster):
    def __init__(self, *args, **kwargs):
        super(TxRawCluster, self).__init__(*args, bucket_factory=kwargs.pop('bucket_factory', TxRawBucket), **kwargs)

    def query_ex(self, cls, *args, **kwargs):
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
        o = super(TxRawCluster, self).query(*args, **kwargs)
        if not self.connected:
            self.on_connect().addCallback(lambda x: o.start())
        else:
            o.start()
        return o

    def query(self, *args, **kwargs):
        """
        Execute a N1QL query, retrieving all rows.

        This method returns a :class:`Deferred` object which is executed
        with a :class:`~.N1QLRequest` object. The object may be iterated
        over to yield the rows in the result set.

        This method is similar to :meth:`~couchbase_v2.bucket.Bucket.n1ql_query`
        in its arguments.

        Example::

            def handler(req):
                for row in req:
                    # ... handle row

            d = cb.n1qlQueryAll('SELECT * from `travel-sample` WHERE city=$1`,
                            'Reno')
            d.addCallback(handler)

        :return: A :class:`Deferred`

        .. seealso:: :meth:`~couchbase_v2.bucket.Bucket.n1ql_query`
        """
        return self.deferred_verb(BatchedQueryResult, super(TxRawCluster,self).query, self.query, *args, **kwargs)

    def analytics_query(self, *args, **kwargs):
        return self.deferred_verb(BatchedAnalyticsResult, super(TxRawCluster, self).analytics_query, self.analytics_query, *args,
                                  **kwargs)

    def search(self, cls, *args, **kwargs):
        """
        Experimental Method

        Execute a Search query providing a custom handler for rows.

        This method allows you to define your own subclass (of
        :class:`~AsyncSearchRequest`) which can handle rows as they are
        received from the network.

        :param cls: The subclass (not instance) to use
        :param args: Positional arguments for the class constructor
        :param kwargs: Keyword arguments for the class constructor

        .. seealso:: :meth:`search`, around which this method wraps
        """
        kwargs['itercls'] = cls
        o = super(TxRawCluster, self).search_query(*args, **kwargs)
        if not self.connected:
            self.on_connect().addCallback(lambda x: o.start())
        else:
            o.start()
        return o

    def search_query(self, *args, **kwargs):
        """
        Experimental Method

        Execute a Search query, retrieving all rows.

        This method returns a :class:`Deferred` object which is executed
        with a :class:`~.SearchRequest` object. The object may be iterated
        over to yield the rows in the result set.

        This method is similar to :meth:`~couchbase_v2.bucket.Bucket.search`
        in its arguments.

        Example::

            def handler(req):
                for row in req:
                    # ... handle row

            d = cb.search('name', ft.MatchQuery('nosql'), limit=10)
            d.addCallback(handler)

        :return: A :class:`Deferred`

        .. seealso:: :meth:`~couchbase_v2.bucket.Bucket.search`
        """

        if not self.connected:
            cb = lambda x: self.search_query(*args, **kwargs)
            return self.on_connect().addCallback(cb)

        kwargs['itercls'] = BatchedSearchResult
        o = super(TxRawCluster, self).search_query(*args, **kwargs)
        o.start()
        return o._getDeferred()


class TxCluster(TxDeferredClientMixin, TxRawCluster):
    def __init__(self, *args, **kwargs):
        super(TxCluster, self).__init__(*args, bucket_factory=kwargs.pop('bucket_factory', TxBucket), **kwargs)


class TxSyncCluster(V3SyncCluster):
    def __init__(self, *args, **kwargs):
        super(TxSyncCluster, self).__init__(*args, bucket_factory=kwargs.pop('bucket_factory', TxBucket), **kwargs)
