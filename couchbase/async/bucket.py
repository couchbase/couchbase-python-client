#
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

import couchbase._bootstrap
from couchbase._libcouchbase import (
    AsyncResult,
    PYCBC_CONN_F_ASYNC,
    PYCBC_CONN_F_ASYNC_DTOR)

from couchbase.result import AsyncResult
from couchbase.async.view import AsyncViewBase
from couchbase.bucket import Bucket
from couchbase.exceptions import ArgumentError

class AsyncBucket(Bucket):
    """
    This class contains the low-level async implementation of the
    :class:`~.Bucket` interface. **This module is not intended to be
    used directly by applications**.

    .. warning::

        Using this module directly may cause odd error messages or
        application crashes. Use an existing subclass designated for
        your I/O framework (`txcouchbase`, `gcouchbase`, `acouchbase`)
        or subclass this module (continue reading) if one does not
        already exist.

        Additionally, this module is considered internal API, as
        such, the interface is subject to change.


    An asynchronous bucket must be wired to a so-called `IOPS`
    implementation (see :class:`~couchbase.iops.base.IOPS`). The
    purpose of the `IOPS` class is to provide the basic I/O wiring
    between the module and the underlying event system.

    In non-asynchronous use modes (e.g. the normal asynchronous
    `Bucket`), the wiring is done internally within the C library
    via an event loop that is "run" for each operation and is
    "stopped" whenever all operations complete.

    In order to successfully implement an asynchronous bucket,
    rather than running and stopping the event loop for each
    operation, it is assumed the event loop is driving the entire
    application, and is implicitly run whenever control is returned
    to it.

    In Python, two main styles of asynchronous programming exist:

    * Explicit callback-based asynchronous programming (such that
      is found in Twisted). This style explicitly makes applications
      aware of an event loop (or "reactor") and requests that they
      register callbacks for various events.
    * Coroutine-based asynchronous programming, that involves
      *implicitly* _yielding_ to an event loop. In this style,
      the programming style seems to be synchronous, and the actual
      event library (for example, `gevent`, or `tulip`) will
      implicitly yield to the event loop when the current coroutine
      awaits I/O completion. These forms of event loops, are from the
      library's perspective, identical to the classic callback-based
      event loops (but see below).


    In both event models, the internal I/O notification system is
    callback-based. The main difference is in how the high-level
    `Bucket` functions (for example, :meth:`~.Bucket.get` operate:

    In callback-based models, these return objects which allow a
    callback to be assigned to them, whereas in coroutine-based
    models, these will implicitly yield to other couroutines.

    In both cases, the operations (from this class itself) will
    return an object which allows the callback to be set. Subclasses
    of this module should ensure that this return value is wrapped
    into a suitable object appropriate to whichever event framework
    is actually being used.

    Several known subclasses exist:

    * :class:`acouchbase.bucket.Bucket` - this is the Python3/Tulip
      based implementation, and uses a hybrid callback/implicit
      yield functionality (by returning "future" objects).
    * :class:`gcouchbase.bucket.Bucket` - this is the `gevent`
      based implementation, and uses an implicit yield model; where
      the bucket class will yield to the event loop and return
      actual "result" objects
    * :class:`txcouchbase.bucket.RawBucket` - this is a thin wrapper
      around this class, which returns :class:`~.AsyncResult` objects:
      Since Twisted is callback-based, it is possible to return these
      raw objects and still remain somewhat idiomatic.
    * :class:`txcouchbase.bucket.Bucket` - this wraps the `RawBucket`
      class (above) and returns Deferred objects.

    """

    def __init__(self, iops=None, *args, **kwargs):
        """
        Create a new Async Bucket. An async Bucket is an object
        which functions like a normal synchronous bucket connection,
        except that it returns future objects
        (i.e. :class:`~couchbase.result.AsyncResult`
        objects) instead of :class:`~couchbase.result.Result`.
        These objects are actually :class:`~couchbase.result.MultiResult`
        objects which are empty upon retun. As operations complete, this
        object becomes populated with the relevant data.

        Note that the AsyncResult object must currently have valid
        :attr:`~couchbase.result.AsyncResult.callback` and
        :attr:`~couchbase.result.AsyncResult.errback` fields initialized
        *after* they are returned from
        the API methods. If this is not the case then an exception will be
        raised when the callbacks are about to arrive. This behavior is the
        primary reason why this interface isn't public, too :)

        :param iops: An :class:`~couchbase.iops.base.IOPS`-interface
          conforming object. This object must not be used between two
          instances, and is owned by the connection object.

        :param kwargs: Additional arguments to pass to
          the :class:`~couchbase.bucket.Bucket` constructor
        """
        if not iops:
            raise ValueError("Must have IOPS")

        kwargs.setdefault('_flags', 0)

        # Must have an IOPS implementation
        kwargs['_iops'] = iops

        # Flags should be async
        kwargs['_flags'] |= PYCBC_CONN_F_ASYNC|PYCBC_CONN_F_ASYNC_DTOR

        # Don't lock/unlock GIL as the enter/leave points are not coordinated
        # kwargs['unlock_gil'] = False
        # This is always set to false in connection.c

        super(AsyncBucket, self).__init__(*args, **kwargs)

    def query(self, *args, **kwargs):
        """
        Reimplemented from base class.

        This method does not add additional functionality of the
        base class' :meth:`~.Bucket.query` method (all the
        functionality is encapsulated in the view class anyway). However it
        does require one additional keyword argument

        :param class itercls: A class used for instantiating the view
          object. This should be a subclass of
          :class:`~couchbase.async.view.AsyncViewBase`.
        """
        if not issubclass(kwargs.get('itercls', None), AsyncViewBase):
            raise ArgumentError.pyexc("itercls must be defined "
                                      "and must be derived from AsyncViewBase")

        return super(AsyncBucket, self).query(*args, **kwargs)

    def endure(self, key, *args, **kwargs):
        res = super(AsyncBucket, self).endure_multi([key], *args, **kwargs)
        res._set_single()
        return res
