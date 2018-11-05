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

import couchbase.n1ql as N
import couchbase.exceptions

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
                raise couchbase.exceptions.ArgumentError(
                    "Cannot append positional args to existing query positional args")
            else:
                self._add_pos_args(args)
        if kwargs:
            overlapping_keys = set(kwargs.keys()) & set(self._body.keys())
            if overlapping_keys:
                raise couchbase.exceptions.ArgumentError("Cannot overwrite named args in query")
            else:
                self._set_named_args(**kwargs)


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
        :param parent: The parent :class:`~.couchbase.bucket.Bucket` object

        To actually receive results of the query, iterate over this
        object.
        """
        self._host = host
        super(AnalyticsRequest, self).__init__(params, parent)

    def _submit_query(self):
        return self._parent._cbas_query(self._params.encoded,
                                        self._host)

