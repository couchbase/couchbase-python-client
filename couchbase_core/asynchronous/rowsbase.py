#
# Copyright 2015, Couchbase, Inc.
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
Abstract class for handling async row-based APIs (i.e. N1QL, Views, 2i, etc)
"""

from couchbase_core._pyport import PyErr_Restore


class AsyncRowsBase(object):
    # This class relies on having various properties defined in the base
    # class. Unfortunately, abc doesn't allow the proper inheritance
    # diagram.

    def __iter__(self):
        """
        Unlike our base class, iterating does not make sense here
        """
        raise NotImplementedError("Iteration not supported on async view")

    def on_error(self, ex):
        """
        Called when there is a failure with the response data

        :param Exception ex: The exception caught.

        This must be implemented in a subclass
        """
        raise NotImplementedError("Must be implemented in subclass")

    def on_rows(self, rowiter):
        """
        Called when there are more processed views.

        :param iterable rowiter: An iterable which will yield results
          as defined by the :class:`RowProcessor` implementation

        This method must be implemented in a subclass
        """
        raise NotImplementedError("Must be implemented in subclass")

    def on_done(self):
        """
        Called when this request has completed. Once this method is called,
        no other methods will be invoked on this object.

        This method must be implemented in a subclass
        """
        raise NotImplementedError("Must be implemented in subclass")

    def _callback(self, mres):
        """
        This is invoked as the row callback.
        If 'rows' is true, then we are a row callback, otherwise
        the request has ended and it's time to collect the other data
        """
        try:
            rows = self._process_payload(self.raw.rows)
            if rows:
                self.on_rows(rows)
            if self.raw.done:
                self.on_done()
        finally:
            if self.raw.done:
                self._clear()

    def _errback(self, mres, ex_cls, ex_obj, ex_bt):
        try:
            PyErr_Restore(ex_cls, ex_obj, ex_bt)
        except Exception as e:
            self.on_error(e)
            self.on_done()
        finally:
            self._clear()

    def _start(self):
        super(AsyncRowsBase, self)._start()
        self._mres.callback = self._callback
        self._mres.errback = self._errback

    def start(self):
        return self._start()