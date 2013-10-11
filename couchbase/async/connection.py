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

"""
This file contains the stub Async implementation.
This module is prefixed by an underscore and thus is not public API,
meaning the interface may change. Its presence is here primarily to
expose potential integrators to the mechanisms by which the library
may be extended to support other async frameworks
"""

import couchbase._bootstrap
from couchbase._libcouchbase import (
    AsyncResult,
    PYCBC_CONN_F_ASYNC,
    PYCBC_CONN_F_ASYNC_DTOR)

from couchbase.result import AsyncResult
from couchbase.async.view import AsyncViewBase
from couchbase.connection import Connection
from couchbase.exceptions import ArgumentError

class Async(Connection):
    def __init__(self, iops=None, **kwargs):
        """
        Create a new Async connection. An async connection is an object
        which functions like a normal synchronous connection, except that it
        returns future objects (i.e. :class:`~couchbase.result.AsyncResult`
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
          the :class:`~couchbase.connection.Connection` constructor
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

        super(Async, self).__init__(**kwargs)

    def query(self, *args, **kwargs):
        """
        Reimplemented from base class. This method does not add additional
        functionality of the base class`
        :meth:`~couchbase.connection.Connection.query` method (all the
        functionality is encapsulated in the view class anyway). However it
        does require one additional keyword argument

        :param class itercls: A class used for instantiating the view
          object. This should be a subclass of
          :class:`~couchbase.async.view.AsyncViewBase`.
        """
        if not issubclass(kwargs.get('itercls', None), AsyncViewBase):
            raise ArgumentError.pyexc("itercls must be defined "
                                      "and must be derived from AsyncViewBase")

        return super(Async, self).query(*args, **kwargs)

    def endure(self, key, *args, **kwargs):
        res = super(Async, self).endure_multi([key], *args, **kwargs)
        res._set_single()
        return res
