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
This file contains the view implementation for Async
"""
from couchbase.views.iterator import View
from couchbase.exceptions import CouchbaseError

class AsyncViewBase(View):
    def __init__(self, *args, **kwargs):
        """
        Initialize a new AsyncViewBase object. This is intended to be
        subclassed in order to implement the require methods to be
        invoked on error, data, and row events
        """
        kwargs['streaming'] = True
        super(AsyncViewBase, self).__init__(*args, **kwargs)

    def __iter__(self):
        raise NotImplementedError("Iteration not supported on async view")


    def on_error(self, ex):
        """
        Called when there is a failure with the response data
        :param Exception ex: The exception caught
        """
        raise ex

    def on_rows(self, rowiter):
        """
        Called when there are more processed views.
        :param iterable rowiter: An iterable which will yield results
        as defined by the :class:`RowProcessor` implementation
        """
        raise NotImplementedError()

    def on_data(self, data):
        """
        Called for any non-view-row HTTP data
        :param list data: Raw HTTP data received from the request, in a list
          form. The type of elements in the list will be either JSON objects
          or string objects, depending on the content.
        """

    def on_done(self):
        """
        Called when this request has completed
        """

    def _callback(self, htres, rows):
        """
        This is invoked as the row callback.
        If 'rows' is true, then we are a row callback, otherwise
        the request has ended and it's time to collect the other data
        """
        try:
            self._process_payload(rows)
            if self._rp_iter:
                self.on_rows(self._rp_iter)

            if self.raw.done:
                self.raw._maybe_raise()
                self.on_done()

        except CouchbaseError as e:
            self.on_error(e)

        finally:
            self._rp_iter = None

    def start_query(self):
        """
        Initiate the callbacks for this query. These callbacks
        will be invoked until the request has completed
        """
        self._setup_streaming_request()
        self._do_iter = True
        self.raw._callback = self._callback
