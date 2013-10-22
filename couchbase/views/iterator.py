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

from collections import namedtuple
from copy import deepcopy
import json
from warnings import warn

from couchbase.exceptions import ArgumentError, CouchbaseError, ViewEngineError
from couchbase.views.params import Query, UNSPEC, make_dvpath
from couchbase._pyport import ulp, xrange
from couchbase.user_constants import FMT_JSON
import couchbase._libcouchbase as C

MAX_URI_LENGTH = 2048 # Let's be safe

class AlreadyQueriedError(CouchbaseError):
    """Thrown when iterating over a View which was already iterated over"""


ViewRow = namedtuple('ViewRow', ['key', 'value', 'docid', 'doc'])
"""
Default class for a single row.
"""


class RowProcessor(object):
    """
    This class contains the handling and conversion functions between
    multiple rows and the means by which they are returned from the
    view iterator.

    This class should be overidden if you are:

    * Using a custom row class
        This saves on processing time and memory by converting from the raw
        results rather than having to unpack from the default class. This
        class returns a :class:`ViewRow` object by default. (This can also
        be overridden using the :attr:`rowclass` attribute)

    * Fetching multiple documents for each row
        You can use the :meth:`~couchbase.connection.Connection.get_multi`
        method to efficiently fetch multiple docs beforehand for the entire
        page.

    .. attribute:: rowclass

        Class or function to call for each result (row) received. This
        is called as ``rowclass(key, value, docid, doc)``

        * ``key`` is the key as returned by the first argument to the
            view function's ``emit``.
        * ``value`` is the value returned as the second argument to the
            view function's ``emit``, or the value of the ``reduce``
            function
        * ``docid`` is the ID of the document itself (as stored by one
            of the :meth:`~couchbase.connection.Connection.set` family of
            methods).
            If ``reduce`` was set to true for the view, this will always
            be None.
        * ``doc`` is the document itself - Only valid if ``include_docs``
            is set to true - in which case a
            :class:`~couchbase.connection.Result` object is passed.
            If ``reduce`` was set to true for the view, this will always
            be None
            Otherwise, ``None`` is passed instead.

        By default, the :class:`ViewRow` is used.
    """
    def __init__(self, rowclass=ViewRow):
        self._riter = None
        self._docs = None
        self.rowclass = rowclass

    def handle_rows(self, rows, connection, include_docs):
        """
        Preprocesses a page of rows.

        :param list rows: A list of rows. Each row is a JSON object containing
            the decoded JSON of the view as returned from the server
        :param connection: The connection object (pass to the :class:`View`
            constructor)
        :param include_docs: Whether to include documents in the return value.
            This is ``True`` or ``False`` depending on what was passed to the
            :class:`View` constructor

        :return: an iterable. When the iterable is exhausted, this method will
            be called again with a new 'page'.
        """
        self._riter = iter(rows)

        if not include_docs:
            return iter(self)

        keys = tuple(x['id'] for x in rows)
        self._docs = connection.get_multi(keys, quiet=True)
        return iter(self)

    def __iter__(self):
        if not self._riter:
            return

        for ret in self._riter:
            doc = None
            if self._docs is not None:
                # We still want to go through this if we have an empty dict
                try:
                    doc = self._docs[ret['id']]
                except KeyError:
                    warn("Error encountered when executing view. "
                         "Inspect 'errors' for more information")

            yield self.rowclass(ret['key'],
                                ret['value'],
                                # Use get, because reduce values don't have
                                # IDs
                                ret.get('id'),
                                doc)

        self._docs = None
        self._riter = None


class View(object):
    def __init__(self,
                 parent,
                 design,
                 view,
                 row_processor=None,
                 streaming=0,
                 include_docs=False,
                 query=None,
                 **params):
        """
        Construct a iterable which can be used to iterate over view query
        results.

        :param parent: The parent Connection object
        :type parent: :class:`~couchbase.connection.Connection`
        :param string design: The design document
        :param string view: The name of the view within the design document
        :param callable row_processor: See :attr:`row_processor` for more
            details.

        :param boolean include_docs: If set, the document itself will be
            retrieved for each row in the result. The default algorithm
            uses :meth:`~couchbase.connection.Connection.get_multi` for each
            page (i.e. every :attr:`streaming` results).

            The :attr:`~couchbase.views.params.Query.reduce`
            family of attributes must not be active, as results fro
            ``reduce`` views do not have corresponding
            doc IDs (as these are aggregation functions).

        :param bool streaming:
            Whether a streaming chunked request should be used. This is
            helpful for handling the view results in small chunks rather
            than loading the entire resultset into memory at once. By default,
            a single request is made and the response is decoded at once. With
            streaming enabled, rows are decoded incrementally.

        :param query: If set, is a :class:`~couchbase.views.params.Query`
            object. It is illegal to use this in conjunction with
            additional ``params``

        :param params: Extra view options. This may be used to pass view
            arguments (as defined in :class:`~couchbase.views.params.Query`)
            without explicitly constructing a
            :class:`~couchbase.views.params.Query` object.
            It is illegal to use this together with the ``query`` argument.
            If you wish to 'inline' additional arguments to the provided
            ``query`` object, use the
            query's :meth:`~couchbase.views.params.Query.update` method
            instead.

        This object is an iterator - it does not send out the request until
        the first item from the iterator is request. See :meth:`__iter__` for
        more details on what this object returns.


        Simple view query, with no extra options::

            # c is the Connection object.

            for result in View(c, "beer", "brewery_beers"):
                print("emitted key: {0}, doc_id: {1}"
                        .format(result.key, result.docid))


        Execute a view with extra query options::

            # Implicitly creates a Query object

            view = View(c, "beer", "by_location",
                        limit=4,
                        reduce=True,
                        group_level=2)

        Pass a Query object::

            q = Query(
                stale=False,
                inclusive_end=True,
                mapkey_range=[
                    ["21st_ammendment_brewery_cafe"],
                    ["21st_ammendment_brewery_cafe", Query.STRING_RANGE_END]
                ]
            )

            view = View(c, "beer", "brewery_beer", query=q)

        Add extra parameters to query object for single call::

            view = View(c, "beer", "brewery_beer",
                        query=q.update(debug=True, copy=True))


        Include documents with query::

            view = View(c, "beer", "brewery_beer",
                        query=q, include_docs=True)

            for result in view:
                print("Emitted key: {0}, Document: {1}".format(
                    result.key, result.doc.value))
        """

        self._parent = parent
        self.design = design
        self.view = view
        self.errors = []
        self.raw = None
        self.rows_returned = 0

        self.include_docs = include_docs
        self.indexed_rows = 0

        if not row_processor:
            row_processor = RowProcessor()
        self.row_processor = row_processor
        self._rp_iter = None

        if query and params:
            raise ArgumentError.pyexc(
                "Extra parameters are mutually exclusive with the "
                "'query' argument. Use query.update() to add extra arguments")

        if query:
            self._query = deepcopy(query)
        else:
            self._query = Query.from_any(params)

        if include_docs:
            if (self._query.reduce or
                    self._query.group or
                    self._query.group_level):

                raise ArgumentError.pyexc("include_docs is only applicable "
                                          "for map-only views, but 'reduce', "
                                          "'group', or 'group_level' "
                                          "was specified",
                                          self._query)

        # The original 'limit' parameter, passed to the query.
        self._streaming = streaming
        self._do_iter = True

    @property
    def streaming(self):
        """
        Read-Only. Returns whether streaming is enabled for this view.
        """
        return self._streaming

    @property
    def query(self):
        """
        Returns the :class:`~couchbase.views.params.Query` object associated
        with this execution instance.

        Note that is normally a modified version
        of the passed object (in the constructor's ``query`` params). It should
        not be directly modified.
        """
        return self._query

    def _handle_errors(self, errors):
        if not errors:
            return

        self.errors += [ errors ]

        if self._query.on_error != 'continue':
            raise ViewEngineError.pyexc("Error while executing view.",
                                        self.errors)
        else:
            warn("Error encountered when executing view. Inspect 'errors' "
                 "for more information")

    def _handle_meta(self, value):
        if not isinstance(value, dict):
            return

        self.indexed_rows = value.get('total_rows', 0)
        self._handle_errors(value.get('errors'))

    def _process_page(self, rows):
        if not rows:
            return

        self.rows_returned += len(rows)

        self._rp_iter = self.row_processor.handle_rows(rows,
                                                       self._parent,
                                                       self.include_docs)

        # Raise exceptions early on
        self._rp_iter = iter(self._rp_iter)

    def _handle_single_view(self):
        self.raw = self._create_raw()
        self._process_page(self.raw.value['rows'])
        self._handle_meta(self.raw.value)

    def _create_raw(self, **kwargs):
        """
        Return common parameters for _libcouchbase._http_request
        """
        d = {
            'type': C.LCB_HTTP_TYPE_VIEW,
            'fetch_headers': True,
            'quiet': False,
            'response_format': FMT_JSON
        }

        # Figure out the path
        qstr = self._query.encoded
        uri = make_dvpath(self.design, self.view)

        if len(uri) + len(qstr) > MAX_URI_LENGTH:
            (uriparams, post_data) = self._query._long_query_encoded

            d['method'] = C.LCB_HTTP_METHOD_POST
            d['post_data'] = post_data
            d['path'] = uri + uriparams
            d['content_type'] = "application/json"

        else:
            d['method'] = C.LCB_HTTP_METHOD_GET
            d['path'] = "{0}{1}".format(uri, qstr)


        d.update(**kwargs)
        return self._parent._http_request(**d)

    def _setup_streaming_request(self):
        """
        Sets up the streaming request. This contains a streaming
        :class:`couchbase.results.HttpResult` object
        """
        self.raw = self._create_raw(chunked=True)

    def _process_payload(self, rows):
        if rows:
            rows = tuple(json.loads(r) for r in rows)
            self._process_page(rows)

        if self.raw.done:
            self._handle_meta(self.raw.value)
            self._do_iter = False

        # No rows and nothing to iterate over?
        elif not self._rp_iter:
            self._rp_iter = iter([])

    def _get_page(self):
        if not self._streaming:
            self._handle_single_view()
            self._do_iter = False
            return

        if not self.raw:
            self._setup_streaming_request()

        # Fetch the rows:
        rows = self.raw._fetch()
        self._process_payload(rows)

    def __iter__(self):
        """
        Returns a row for each query.
        The type of the row depends on the :attr:`row_processor` being used.

        :raise: :exc:`~couchbase.exceptions.ViewEngineError`

            If an error was encountered while processing the view, and the
            :attr:`~couchbase.views.params.Query.on_error`
            attribute was not set to `continue`.

            If `continue` was specified, a warning message is printed to the
            screen (via ``warnings.warn`` and operation continues). To inspect
            the error, examine :attr:`errors`

        :raise: :exc:`AlreadyQueriedError`

            If this object was already iterated
            over and the last result was already returned.
        """
        if not self._do_iter:
            raise AlreadyQueriedError.pyexc(
                "This object has already been executed. Create a new one to "
                "query again")

        while self._do_iter:
            self._get_page()
            if not self._rp_iter:
                break

            for r in self._rp_iter:
                yield r

            self._rp_iter = None

    def __repr__(self):
        details = []
        details.append("Design={0}".format(self.design))
        details.append("View={0}".format(self.view))
        details.append("Query={0}".format(self._query))
        details.append("Rows Fetched={0}".format(self.rows_returned))
        return '{cls}<{details}>'.format(cls=self.__class__.__name__,
                                         details=', '.join(details))
