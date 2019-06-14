#
# Copyright 2017, Couchbase, Inc.
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
from couchbase_core._libcouchbase import FMT_JSON

import couchbase_core.n1ql as N
import couchbase_core.exceptions
import couchbase_core._libcouchbase as LCB
import time
from couchbase_core.exceptions import CouchbaseInternalError
try:
    import urlparse
except:
    import urllib.parse as urlparse
from typing import *


class AnalyticsQuery(N.N1QLQuery):
    def __init__(self, querystr, *args, **kwargs):
        """
        Create an Analytics Query object. This may be passed as the
        `params` argument to :class:`AnalyticsRequest`.

        :param querystr: The query string to execute
        :param args: Positional placeholder arguments. These satisfy
            the placeholder values for positional placeholders in the
            query string, demarcated by ``?``.
        :param kwargs: Named placeholder arguments. These satisfy
            named placeholders in the query string, such as
            ``$name``, ``$email`` and so on. For the placeholder
            values, omit the leading dollar sign (``$``).

        Use positional parameters::

            q = AnalyticsQuery("SELECT VALUE bw FROM breweries "
                               "bw WHERE bw.name = ?", 'Kona Brewing')

            for row in cb.analytics_query(q, "127.0.0.1"):
                print('Got {}'.format(str(row))

        Use named parameters::

            q = AnalyticsQuery("SELECT VALUE bw FROM breweries "
                               "bw WHERE bw.name = $brewery", brewery='Kona Brewing')
            for row in cb.analytics_query(q, "127.0.0.1"):
                print('Got {}'.format(str(row))


        When using placeholders, ensure that the placeholder value is
        the *unserialized* (i.e. native) Python value, not the JSON
        serialized value. For example the query
        ``SELECT VALUE bw FROM breweries bw WHERE bw.name IN
        ['Kona Brewing','21st Amendment Brewery Cafe']``
        can be rewritten using placeholders:

        Correct::

            AnalyticsQuery('SELECT VALUE bw FROM breweries bw WHERE bw.name IN ?',
                      ['Kona Brewing', '21st Amendment Brewery Cafe'])

        Incorrect::

            AnalyticsQuery('SELECT VALUE bw FROM breweries bw WHERE bw.name IN ?',
                      "[\\"Kona Brewing\\",\\"21st Amendment Brewery Cafe\\"]")
        Since the placeholders are serialized to JSON internally anyway.
        
        """
        querystr = querystr.rstrip()
        if not querystr.endswith(';'):
            querystr += ';'
        super(AnalyticsQuery, self).__init__(querystr,*args,**kwargs)

    def update(self, *args, **kwargs):
        if args:
            if 'args' in self._body:
                raise couchbase_core.exceptions.ArgumentError(
                    "Cannot append positional args to existing query positional args")
            else:
                self._add_pos_args(args)
        if kwargs:
            overlapping_keys = set(kwargs.keys()) & set(self._body.keys())
            if overlapping_keys:
                raise couchbase_core.exceptions.ArgumentError("Cannot overwrite named args in query")
            else:
                self._set_named_args(**kwargs)


class DeferredAnalyticsQuery(AnalyticsQuery):
    def __init__(self, querystr, *args, **kwargs):
        """
        Create a Deferred Analytics Query object. This may be passed as the
        `params` argument to :class:`DeferredAnalyticsRequest`. Parameters
        are the same as an AnalyticsQuery.

        Please note this is a volatile API, which is subject to change in future.

        :param querystr: The query string to execute
        :param args: Positional placeholder arguments. These satisfy
            the placeholder values for positional placeholders in the
            query string, demarcated by ``?``.
        :param kwargs: Named placeholder arguments. These satisfy
            named placeholders in the query string, such as
            ``$name``, ``$email`` and so on. For the placeholder
            values, omit the leading dollar sign (``$``).
       """
        super(DeferredAnalyticsQuery, self).__init__(querystr, *args, **kwargs)
        self.set_option("mode", "async")
        self._timeout = None

    @property
    def timeout(self):
        return self._timeout

    @timeout.setter
    def timeout(self, value):
        self._timeout = value


class AnalyticsRequest(N.N1QLRequest):
    def __init__(self, params, host, parent):
        """
        Object representing the execution of the request on the
        server.

        .. warning::

            You should typically not call this constructor by
            yourself, rather use the :meth:`~.Bucket.analytics_query`
            method (or one of its async derivatives).

        :param params: An :class:`AnalyticsQuery` object.
        :param host: the host to send the request to.
        :param parent: The parent :class:`~.couchbase_core.bucket.Bucket` object

        To actually receive results of the query, iterate over this
        object.
        """
        self._host = host
        super(AnalyticsRequest, self).__init__(params, parent)

    def _submit_query(self):
        return self._parent._cbas_query(self._params.encoded,
                                        self._host)


class DeferredAnalyticsRequest(AnalyticsRequest):
    def __init__(self, params, host, parent, timeout = None, interval = None):
        # type: (DeferredAnalyticsQuery, str, couchbase_core.bucket.Bucket, Optional[float], Optional[float]) -> None
        """
        Object representing the execution of a deferred request on the
        server.

        Please note this is a volatile API, which is subject to change in future.

        .. warning::

            You should typically not call this constructor by
            yourself, rather use the :meth:`~.Bucket.analytics_query`
            method (or one of its async derivatives).

        :param params: An :class:`DeferredAnalyticsQuery` object.
        :param host: the host to send the request to.
        :param parent: The parent :class:`~.couchbase_core.bucket.Bucket` object.
        :param timeout: Timeout in seconds.
        :param interval: Interval in seconds for deferred polling.

        To actually receive results of the query, iterate over this
        object.
        """
        handle_req = AnalyticsRequest(params, host, parent)

        handle = handle_req.meta.get('handle')

        if not handle:
            raise CouchbaseInternalError("{} does not support deferred queries".format(host))

        self.parent = parent
        self._final_response = None
        self.finish_time = time.time() + (timeout if timeout else params._timeout)
        self.handle_host=urlparse.urlparse(handle)
        self.interval = interval or 10
        super(DeferredAnalyticsRequest,self).__init__(params,host,parent)

    def _submit_query(self):
        return {None:self.final_response()}

    def _is_ready(self):
        """
        Return True if and only if final result has been received, optionally blocking
        until this is the case, or the timeout is exceeded.

        This is a synchronous implementation but an async one can
        be added by subclassing this.

        :return: True if ready, False if not
        """
        while not self.finish_time or time.time() < self.finish_time:
            result=self._poll_deferred()
            if result=='success':
                return True
            if result=='failed':
                raise couchbase_core.exceptions.InternalError("Failed exception")
            time.sleep(self.interval)

        raise couchbase_core.exceptions.TimeoutError("Deferred query timed out")

    class MRESWrapper:
        def __init__(self, response):
            self.response=response
            if response.value:
                self.iter=iter(response.value if isinstance(response.value,list) else [response.value])
            else:
                self.iter=iter([])

        def __getattr__(self, item):
            return getattr(self.response,item)

        @property
        def done(self):
            return not len(self.fetch(None))

        def fetch(self, mres):
            result = next(self.iter,None)
            return [result] if result else []

    def _poll_deferred(self):
        status = "pending"
        response_value = {}
        try:
            response = self.parent._http_request(type=LCB.LCB_HTTP_TYPE_CBAS, method=LCB.LCB_HTTP_METHOD_GET,
                                                 path=self.handle_host.path, response_format=FMT_JSON,
                                                 host=self._to_host_URI(self.handle_host))
            response_value = response.value
            status = response_value.get('status')
        except Exception as e:
            pass
        if status == 'failed':
            raise couchbase_core.exceptions.InternalError("Deferred Query Failed")
        if status == 'success':
            response_handle = response_value.get('handle')
            if not response_handle:
                raise couchbase_core.exceptions.InternalError("Got success but no handle from deferred query response")
            try:
                parsed_response_handle = urlparse.urlparse(response_handle)
            except Exception as e:
                raise couchbase_core.exceptions.InternalError("Got invalid url: {}".format(e))
            final_response = self.parent._http_request(type=LCB.LCB_HTTP_TYPE_CBAS,
                                                       method=LCB.LCB_HTTP_METHOD_GET,
                                                       path=parsed_response_handle.path,
                                                       host=self._to_host_URI(parsed_response_handle),
                                                       response_format=FMT_JSON)
            self._final_response = DeferredAnalyticsRequest.MRESWrapper(final_response)
        return status

    @staticmethod
    def _to_host_URI(parsed_response_handle):
        host_URI = "{}://{}:{}".format(parsed_response_handle.scheme, parsed_response_handle.hostname,
                                       parsed_response_handle.port)
        return host_URI

    def final_response(self, default=None):
        if self._final_response or self._is_ready():
            return self._final_response
        return default

    @property
    def raw(self):
        return self.final_response()


def gen_request(query, *args, **kwargs):
    if isinstance(query, DeferredAnalyticsQuery):
        return DeferredAnalyticsRequest(query,*args,**kwargs)
    elif isinstance(query,AnalyticsQuery):
        return AnalyticsRequest(query,*args,**kwargs)


