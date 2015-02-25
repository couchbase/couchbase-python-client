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
import json

from couchbase._pyport import long, basestring
from couchbase._libcouchbase import _N1QLParams
from couchbase.views.iterator import AlreadyQueriedError
from couchbase.exceptions import CouchbaseError


class N1QLError(CouchbaseError):
    @property
    def n1ql_errcode(self):
        return self.objextra['code']


class N1QLRow(object):
    def __init__(self, jsobj):
        """
        Default class wrapping a row returned by an N1QL query.

        The fields in the row may be obtained by using the index
        syntax (``['name']`` or ``[4]``) to retrieve a named or
        indexed result field.
        :param jsobj: The raw JSON from the row
        """
        self._jsobj = jsobj

    def __getitem__(self, item):
        if isinstance(item, (int, long)):
            try:
                return self._jsobj['${0}'.format(item)]
            except IndexError:
                raise IndexError('Row has no positional index {0}'.format(item))
        else:
            return self._jsobj[item]

    @property
    def raw(self):
        """
        Retrieve the underlying JSON object
        """
        return self._jsobj

    def named_field(self, name):
        """
        Retrieve a field by its name
        :param name: The field name
        :return: The field value
        :raise: :exc:`KeyError` if there is no such field
        """
        return self._jsobj[name]

    def pos_field(self, pos):
        """
        Retrieve a field by its index.
        The field must have been retrieved as an indexed parameter
        :param pos: The numeric index
        :return: The field value
        :raise: :exc:`IndexError` if there is no such field
        """
        return self._jsobj['${0}'.format(pos)]

    def pos_values(self):
        """
        Get all positional field values as a list
        :return: All positional field values.
        :raise: :exc:`ValueError` if there are no positional results.
        """
        ll = []
        i = 1
        while True:
            try:
                ll.append(self[i])
                i += 1
            except (IndexError, KeyError):
                break
        if ll:
            return ll
        else:
            raise ValueError('No positional arguments')

    def unwrap(self):
        """
        Unwraps the result from any surrounding placeholders.

        This method is useful if the query yields a single top level
        field (either positional or named).

        This will fail if there is more than one top level object in the row
        :return: A new :class:`N1QLRow` which encapsulates the top level
        field.
        """
        if len(self._jsobj) > 1:
            raise ValueError('Cannot unwrap!')
        return self.__class__(self._jsobj[self._jsobj.keys()[0]])


class N1QLQuery(object):
    def __init__(self, query, prepared=False):
        """
        Create an N1QL Query object. This may be passed as the
        `params` argument to :class:`N1QLRequest`.

        :param query: The query body. This may either be a query string,
            or a dictionary representing a prepared query.
        :param prepared: Set to true if the query object should be treated
            as a prepared statement
        """
        self._cparams = _N1QLParams()
        if prepared:
            if not isinstance(query, dict):
                raise ValueError('Prepared query must be a dictionary')
            self._cparams.setquery(json.dumps(query), 2)
        else:
            self._cparams.setquery(query)
        self._stmt = query

        self.posargs = []
        self.namedargs = {}

    def set_args(self, **kv):
        """
        Set a named parameter in the query. The named field must
        exist in the query itself.
        :param kv: Key-Value pairs representing values within the
            query. These values should be stripped of their leading
            `$` identifier.
        """
        for k in kv:
            self.set_rawargs({'$'+k: kv[k]}, encoded=False)
        return self

    def set_rawargs(self, kv, encoded=False):
        """
        :param kv: A dictionary containing the raw key-values for
            the query arguments.
        :param encoded: Whether the values are already JSON encoded.
        """
        if encoded:
            self.namedargs.update(**kv)
            return self
        for k in kv:
            self.namedargs[k] = json.dumps(kv[k])

        return self

    def set_option(self, name, value):
        self._cparams.setopt(name, value)

    def add_args(self, *args):
        for arg in args:
            self.posargs.append(json.dumps(arg))

    @property
    def statement(self):
        return self._stmt

    def clear(self):
        self._cparams.clear()

    def _presubmit(self):
        for arg in self.posargs:
            self._cparams.add_pos_param(arg)
        for k in self.namedargs:
            self._cparams.set_named_param(k, self.namedargs[k])

    def __repr__(self):
        return ('<{cls} stmt={stmt} at {oid}>'.format(
            cls=self.__class__.__name__,
            stmt=repr(self._stmt),
            oid=id(self)))


class N1QLInsertQuery(N1QLQuery):
    def __init__(self, keyspace, kv, encode=True):
        """
        Create an INSERT query
        :param string keyspace: The keyspace
        :param kv: key=value entries to insert
        :param boolean encode: Whether to encode the values
        """

        kvp = []
        for k in kv:
            v = kv[k]
            cur = 'VALUES("{0}",{1})'
            cur = cur.format(k, json.dumps(v) if encode else v)
            kvp.append(cur)

        ss = 'INSERT INTO {0} {1}'.format(
            keyspace, ','.join(kvp)
        )
        super(N1QLInsertQuery, self).__init__(ss)


class N1QLRequest(object):
    def __init__(self, params, parent, row_factory=N1QLRow, _host=''):
        """
        Object representing the execution of the request on the
        server.
        :param params: An :class:`N1QLQuery` object.
        :param parent: The parent :class:`.Bucket` object
        :param row_factory: Callable which accepts the JSON encoded
            rows and converts them to Python objects. The default is
            :class:`N1QLRow`. You may use ``lambda x: x`` to just
            return the raw JSON
        :param _host: `host:port` specifier, useful for standalone
            instances of Developer Preview N1QL versions.

        To actually receive results of the query, iterate over this
        object.
        """
        if isinstance(params, basestring):
            params = N1QLQuery(params)

        self._params = params
        self._parent = parent
        self.row_factory = row_factory
        self.errors = []
        self._mres = None
        self._do_iter = True
        self._host = _host
        self.__raw = False

    def _start(self):
        if self._mres:
            return

        self._params._presubmit()
        self._mres = self._parent._n1ql_query(
            self._params._cparams, _host=self._host)
        self.__raw = self._mres[None]

    @property
    def raw(self):
        return self.__raw

    def _clear(self):
        del self._parent
        del self._mres

    def _handle_meta(self, value):
        if not isinstance(value, dict):
            return
        if 'errors' in value:
            for err in value['errors']:
                raise N1QLError.pyexc('N1QL Execution failed', err)

    def _process_payload(self, rows):
        if rows:
            return [self.row_factory(row) for row in rows]

        elif self.raw.done:
            self._handle_meta(self.raw.value)
            self._do_iter = False
            return []

    def execute(self):
        """
        Execute the statement and raise an exception on failure.

        This method is useful for statements which modify data or
        indexes, where the application does not need to extract any
        data, but merely determine success or failure.
        """
        for r in self:
            pass

    def get_single_result(self):
        """
        Execute the statement and return its single result.

        This should only be used on statements which are intended to
        return only a single result.
        :return: The single result, as encapsulated by the
            `row_factory`
        """
        for r in self:
            return r

    def __iter__(self):
        if not self._do_iter:
            raise AlreadyQueriedError()

        self._start()
        while self._do_iter:
            raw_rows = self.raw.fetch(self._mres)
            for row in self._process_payload(raw_rows):
                yield row
