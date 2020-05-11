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

from typing import NamedTuple, Any
from copy import deepcopy
from warnings import warn

from couchbase.exceptions import InvalidArgumentException, CouchbaseException, ViewEngineException, NotSupportedException
from .params import ViewQuery, SpatialQuery, QueryBase
from couchbase_core._pyport import basestring
import couchbase_core._libcouchbase as C


class AlreadyQueriedException(CouchbaseException):
    """Thrown when iterating over a View which was already iterated over"""


ViewRow = NamedTuple('ViewRow', [('key', str), ('value', Any), ('docid', str), ('doc',Any)])
"""
Default class for a single row.
"""

SpatialRow = NamedTuple('SpatialRow',
                        [('key', str), ('value', Any), ('geometry', Any), ('docid', Any), ('doc', Any)])
"""
Default class for a spatial row
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
        You can use the :meth:`~couchbase_core.client.Client.get_multi`
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
            of the :meth:`~couchbase_core.client.Client.upsert` family of
            methods).
            If ``reduce`` was set to true for the view, this will always
            be None.
        * ``doc`` is the document itself - Only valid if ``include_docs``
            is set to true - in which case a
            :class:`~couchbase_core.connection.Result` object is passed.
            If ``reduce`` was set to true for the view, this will always
            be None
            Otherwise, ``None`` is passed instead.

        By default, the :class:`ViewRow` is used.
    """
    def __init__(self, rowclass):
        self.rowclass = rowclass

    def handle_rows(self, rows, *_):
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
        for row in rows:
            yield self.rowclass(row['key'], row['value'],
                                row.get('id'), get_row_doc(row))


class SpatialRowProcessor(RowProcessor):
    def __init__(self, rowclass):
        super(SpatialRowProcessor, self).__init__(rowclass)

    def handle_rows(self, rows, *_):
        for row in rows:
            yield self.rowclass(row['key'], row['value'], row.get('geometry'),
                                row.get('id'), get_row_doc(row))


def get_row_doc(row_json):
    """
    Gets the document for the given parsed JSON row.

    Use this function in custom :class:`~.RowProcessor`
    implementations to extract the actual document. The document
    itself is stored within a private field of the row itself, and
    should only be accessed by this function.

    :param dict row_json: The parsed row (passed to the processor)
    :return: The document, or None
    """
    return row_json.get('__DOCRESULT__')


class View(object):
    def __init__(self, parent, design, view, row_processor=None,
                 include_docs=False, query=None, streaming=True, spatial_row_factory=SpatialRow, row_factory=ViewRow, **params):
        """
        Construct a iterable which can be used to iterate over view query
        results.

        :param parent: The parent Client object
        :type parent: :class:`~couchbase_core.client.Client`
        :param string design: The design document
        :param string view: The name of the view within the design document
        :param callable row_processor: See :attr:`row_processor` for more
            details.

        :param boolean include_docs: If set, the document itself will be
            retrieved for each row in the result. The default algorithm
            uses :meth:`~couchbase_core.client.Client.get_multi` for each
            page (i.e. every :attr:`streaming` results).

            The :attr:`~couchbase_core.views.params.Query.reduce`
            family of attributes must not be active, as results fro
            ``reduce`` views do not have corresponding
            doc IDs (as these are aggregation functions).

        :param query: If set, should be a :class:`~couchbase_core.views.params.Query` or
            :class:`~.SpatialQuery` object. It is illegal to use
            this in conjunction with additional ``params``

        :param params: Extra view options. This may be used to pass view
            arguments (as defined in :class:`~couchbase_core.views.params.Query`)
            without explicitly constructing a
            :class:`~couchbase_core.views.params.Query` object.
            It is illegal to use this together with the ``query`` argument.
            If you wish to 'inline' additional arguments to the provided
            ``query`` object, use the
            query's :meth:`~couchbase_core.views.params.Query.update` method
            instead.

        This object is an iterator - it does not send out the request until
        the first item from the iterator is request. See :meth:`__iter__` for
        more details on what this object returns.


        Simple view query, with no extra options::

            # c is the Client object.

            for result in View(c, "beer", "brewery_beers"):
                print("emitted key: {0}, doc_id: {1}"
                        .format(result.key, result.docid))


        Execute a view with extra query options::

            # Implicitly creates a Query object

            view = View(c, "beer", "by_location",
                        limit=4,
                        reduce=True,
                        group_level=2)

        Execute a spatial view::

            from couchbase_core.views.params import SpatialQuery
            # ....
            q = SpatialQuery()
            q.start_range = [ -119.9556, 38.7056 ]
            q.end_range = [ -118.8122, 39.7086 ]
            view = View(c, 'geodesign', 'spatialview', query=q)
            for row in view:
                print('Location is {0}'.format(row.geometry))

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

        if parent.btype == C.LCB_BTYPE_EPHEMERAL:
            raise NotSupportedException("Ephemeral bucket")
        self._parent = parent
        self.design = design
        self.view = view
        self._errors = []
        self.rows_returned = 0
        self._indexed_rows = 0

        # Sentinel used to ensure confusing metadata properties don't get
        # returned unless metadata is actually parsed (or query is complete)
        self.__meta_received = False

        if query and params:
            raise InvalidArgumentException.pyexc(
                "Extra parameters are mutually exclusive with the "
                "'query' argument. Use query.update() to add extra arguments")

        if query:
            if isinstance(query, basestring):
                self._query = ViewQuery.from_string(query)
            else:
                self._query = deepcopy(query)
        else:
            self._query = QueryBase.from_any(params)

        self._flags = 0
        if include_docs:
            self._flags |= C.LCB_CMDVIEWQUERY_F_INCLUDE_DOCS
        if isinstance(self._query, SpatialQuery):
            self._flags |= C.LCB_CMDVIEWQUERY_F_SPATIAL

        if include_docs and self._query.reduce:
            raise InvalidArgumentException.pyexc(
                "include_docs is only applicable for map-only views, but "
                "'reduce', 'group', or 'group_level' was specified; or "
                "this is a spatial query",
                self._query)

        # The original 'limit' parameter, passed to the query.
        self._do_iter = True
        self._mres = None

        if not row_processor:
            if self._spatial:
                row_processor = SpatialRowProcessor(spatial_row_factory)
            else:
                row_processor = RowProcessor(row_factory)
        self.row_processor = row_processor

    @property
    def _spatial(self):
        return self._flags & C.LCB_CMDVIEWQUERY_F_SPATIAL

    def _start(self):
        if self._mres:
            return

        self._mres = self._parent._view_request(
            design=self.design, view=self.view, options=self.query,
            _flags=self._flags)
        self.__raw = self._mres[None]

    def _clear(self):
        """
        Clears references to other internal objects.

        This is useful to break out of a reference cycle
        """
        del self._parent
        del self._mres

    @property
    def query(self):
        """
        Returns the :class:`~couchbase_core.views.params.Query` object associated
        with this execution instance.

        Note that is normally a modified version
        of the passed object (in the constructor's ``query`` params). It should
        not be directly modified.
        """
        return self._query

    @property
    def raw(self):
        return self.__raw

    def __meta_or_raise(self):
        """
        Check if meta has been handled and raise an exception otherwise.
        Thrown from 'meta-only' properties
        """
        if not self.__meta_received:
            raise RuntimeError(
                'This property only valid once all rows are received')

    @property
    def indexed_rows(self):
        self.__meta_or_raise()
        return self._indexed_rows

    @property
    def errors(self):
        self.__meta_or_raise()
        return self._errors

    def _handle_errors(self, errors):
        if not errors:
            return

        self._errors += [errors]

        if self._query.on_error != 'continue':
            raise ViewEngineException.pyexc("Error while executing view.",
                                            self._errors)
        else:
            warn("Error encountered when executing view. Inspect 'errors' "
                 "for more information")

    def _handle_meta(self, value):
        self.__meta_received = True
        if not isinstance(value, dict):
            return
        self._indexed_rows = value.get('total_rows', 0)
        self._handle_errors(value.get('errors'))
        self._debug = value

    @property
    def debug(self):
        self.__meta_or_raise()
        return self._debug

    def _process_payload(self, rows):
        if rows:
            self.rows_returned += len(rows)
            return self.row_processor.handle_rows(rows, self._parent, False)

        elif self.raw.done:
            self._handle_meta(self.raw.value)
            self._do_iter = False

        return []

    def __iter__(self):
        """
        Returns a row for each query.
        The type of the row depends on the :attr:`row_processor` being used.

        :raise: :exc:`~couchbase.exceptions.ViewEngineException`

            If an error was encountered while processing the view, and the
            :attr:`~couchbase_core.views.params.Query.on_error`
            attribute was not set to `continue`.

            If `continue` was specified, a warning message is printed to the
            screen (via ``warnings.warn`` and operation continues). To inspect
            the error, examine :attr:`errors`

        :raise: :exc:`AlreadyQueriedException`

            If this object was already iterated
            over and the last result was already returned.
        """
        if not self._do_iter:
            raise AlreadyQueriedException.pyexc(
                "This object has already been executed. Create a new one to "
                "query again")

        self._start()
        while self._do_iter:
            raw_rows = self.raw.fetch(self._mres)
            for row in self._process_payload(raw_rows):
                yield row

    def __repr__(self):
        details = []
        details.append("Design={0}".format(self.design))
        details.append("View={0}".format(self.view))
        details.append("Query={0}".format(self._query))
        details.append("Rows Fetched={0}".format(self.rows_returned))
        return '{cls}<{details}>'.format(cls=self.__class__.__name__,
                                         details=', '.join(details))
