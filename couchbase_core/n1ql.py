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

from couchbase_core._pyport import basestring
from couchbase_core.deprecation import deprecate_module_attribute
from couchbase_core.views.iterator import AlreadyQueriedError
from couchbase_core.exceptions import CouchbaseError
import sys

# Not used internally, but by other modules


class N1QLError(CouchbaseError):
    @property
    def n1ql_errcode(self):
        return self.objextra['code']


sys.modules[__name__] = deprecate_module_attribute(sys.modules[__name__],
                                                   deprecated=['CONSISTENCY_NONE', 'UNBOUNDED'])

STATEMENT_PLUS = 'statement_plus'

NOT_BOUNDED = 'not_bounded'
"""
For use with :attr:`~.N1QLQuery.consistency`, will allow cached
values to be returned. This will improve performance but may not
reflect the latest data in the server.
"""
REQUEST_PLUS = 'request_plus'
"""
For use with :attr:`~.N1QLQuery.consistency`, will ensure that query
results always reflect the latest data in the server
"""
UNBOUNDED = 'none'
"""
.. deprecated:: 2.3.3
   Use :attr:`couchbase_core.n1ql.NOT_BOUNDED` instead. This had no effect in its advertised
   usage as a value for :attr:`~.N1QLQuery.consistency` before.
"""

CONSISTENCY_UNBOUNDED = NOT_BOUNDED
CONSISTENCY_REQUEST = REQUEST_PLUS
CONSISTENCY_NONE = UNBOUNDED
"""
.. deprecated:: 2.3.3
   Use :attr:`couchbase_core.n1ql.CONSISTENCY_UNBOUNDED` instead. This had no effect in its advertised
   usage as a value for :attr:`~.N1QLQuery.consistency` before. By default
   'Not Bounded' mode is used so the effect was functionally equivalent, but
   this is not guaranteed in future.
"""

PROFILE_OFF = 'off'
PROFILE_PHASES = 'phases'
PROFILE_TIMINGS = 'timings'
VALID_PROFILES = [PROFILE_OFF, PROFILE_TIMINGS, PROFILE_PHASES]

class N1QLQuery(object):
    def __init__(self, query, *args, **kwargs):
        """
        Create an N1QL Query object. This may be passed as the
        `params` argument to :class:`N1QLRequest`.

        :param query: The query string to execute
        :param args: Positional placeholder arguments. These satisfy
            the placeholder values for positional placeholders in the
            query string, such as ``$1``, ``$2`` and so on.
        :param kwargs: Named placeholder arguments. These satisfy
            named placeholders in the query string, such as
            ``$name``, ``$email`` and so on. For the placeholder
            values, omit the leading sigil (``$``).

        Use positional parameters::

            q = N1QLQuery('SELECT * FROM `travel-sample` '
                          'WHERE type=$1 AND id=$2',
                          'airline', 0)

            for row in cb.n1ql_query(q):
                print 'Got', row

        Use named parameters::

            q = N1QLQuery('SELECT * FROM `travel-sample` '
                          'WHERE type=$type AND id=$id',
                           type='airline', id=0)
            for row in cb.n1ql_query(q):
                print 'Got', row


        When using placeholders, ensure that the placeholder value is
        the *unserialized* (i.e. native) Python value, not the JSON
        serialized value. For example the query
        ``SELECT * FROM products WHERE tags IN ["sale", "clearance"]``
        can be rewritten using placeholders:

        Correct::

            N1QLQuery('SELECT * FROM products WHERE tags IN $1',
                      ['sale', 'clearance'])

        Incorrect::

            N1QLQuery('SELECT * FROM products WHERE tags IN $1',
                      "[\\"sale\\",\\"clearance\\"]")

        Since the placeholders are serialized to JSON internally anyway.
        """

        self._adhoc = True
        self._cross_bucket = False
        self._body = {'statement': query}
        if args:
            self._add_pos_args(*args)
        if kwargs:
            self._set_named_args(**kwargs)

    def _set_named_args(self, **kv):
        """
        Set a named parameter in the query. The named field must
        exist in the query itself.

        :param kv: Key-Value pairs representing values within the
            query. These values should be stripped of their leading
            `$` identifier.

        """
        for k in kv:
            self._body['${0}'.format(k)] = kv[k]
        return self

    def _add_pos_args(self, *args):
        """
        Set values for *positional* placeholders (``$1,$2,...``)

        :param args: Values to be used
        """
        arg_array = self._body.setdefault('args', [])
        arg_array.extend(args)

    def set_option(self, name, value):
        """
        Set a raw option in the query. This option is encoded
        as part of the query parameters without any client-side
        verification. Use this for settings not directly exposed
        by the Python client.

        :param name: The name of the option
        :param value: The value of the option
        """
        self._body[name] = value

    @property
    def metrics(self):
        return self._body.get('metrics', True)

    @metrics.setter
    def metrics(self, value):
        self.set_option('metrics', value)

    @property
    def statement(self):
        return self._body['statement']

    @property
    def consistency(self):
        """
        Sets the consistency level.

        :see: :data:`NOT_BOUNDED`, :data:`REQUEST_PLUS`
        """
        return self._body.get('scan_consistency', NOT_BOUNDED)

    @consistency.setter
    def consistency(self, value):
        self._body['scan_consistency'] = value

    def consistent_with(self, state):
        """
        Indicate that the query should be consistent with one or more
        mutations.

        :param state: The state of the mutations it should be consistent
            with.
        :type state: :class:`~.couchbase_core.mutation_state.MutationState`
        """
        if self.consistency not in (UNBOUNDED, NOT_BOUNDED, 'at_plus'):
            raise TypeError(
                'consistent_with not valid with other consistency options')

        if not state:
            raise TypeError('Passed empty or invalid state', state)
        self.consistency = 'at_plus'
        self._body['scan_vectors'] = state._sv

    # TODO: I really wish Sphinx were able to automatically
    # document instance vars
    @property
    def adhoc(self):
        """
        A non-`adhoc` query can be internally optimized so that repeated
        executions of the same query can be quicker. If this query is issued
        repeatedly in your application, then you should set this property to
        `False`.

        Note that this optimization involves an up-front "preparation"
        cost, and should only be used for queries that are issued multiple
        times.
        """
        return self._adhoc

    @adhoc.setter
    def adhoc(self, arg):
        self._adhoc = arg

    @property
    def cross_bucket(self):
        """
        Set this to true to indicate that the query string involves multiple
        buckets. This makes the query a "cluster-level" query. Cluster level
        queries have access to documents in multiple buckets, using credentials
        supplied via :meth:`.Bucket.add_bucket_creds`
        """
        return self._cross_bucket

    @cross_bucket.setter
    def cross_bucket(self, value):
        self._cross_bucket = value

    @property
    def timeout(self):
        """
        Optional per-query timeout. If set, this will limit the amount
        of time in which the query can be executed and waited for.

        .. note::

            The effective timeout for the query will be either this property
            or the value of :attr:`couchbase_core.bucket.Bucket.n1ql_timeout`
            property, whichever is *lower*.

        .. seealso:: couchbase_core.bucket.Bucket.n1ql_timeout
        """
        value = self._body.get('timeout', '0s')
        value = value[:-1]
        return float(value)

    @timeout.setter
    def timeout(self, value):
        if not value:
            self._body.pop('timeout', 0)
        else:
            value = float(value)
            self._body['timeout'] = '{0}s'.format(value)

    @property
    def encoded(self):
        """
        Get an encoded representation of the query.

        This is used internally by the client, and can be useful
        to debug queries.
        """
        return json.dumps(self._body)

    @property
    def scan_cap(self):
        """
        Maximum buffered channel size between the indexer client and the query
        service for index scans. This parameter controls when to use scan
        backfill. Use 0 or a negative number to disable.

        Available from Couchbase Server 5.0
        """
        value = self._body.get('scan_cap', '0')
        return int(value)

    @scan_cap.setter
    def scan_cap(self, value):
        self._body['scan_cap'] = str(value)

    @property
    def pipeline_batch(self):
        """
        Controls the number of items execution operators can batch for Fetch
        from the KV.

        Available from Couchbase Server 5.0
        """
        value = self._body.get('pipeline_batch', '0')
        return int(value)

    @pipeline_batch.setter
    def pipeline_batch(self, value):
        self._body['pipeline_batch'] = str(value)

    @property
    def pipeline_cap(self):
        """
        Maximum number of items each execution operator can buffer between
        various operators.

        Available from Couchbase Server 5.0
        """
        value = self._body.get('pipeline_cap', '0')
        return int(value)

    @pipeline_cap.setter
    def pipeline_cap(self, value):
        self._body['pipeline_cap'] = str(value)

    @property
    def readonly(self):
        """
        Controls whether a query can change a resulting recordset.
        If readonly is true, then the following statements are not allowed:
        CREATE INDEX
        DROP INDEX
        INSERT
        MERGE
        UPDATE
        UPSERT
        DELETE

        Available from Couchbase Server 5.0
        """
        value = self._body.get('readonly', False)
        return value

    @readonly.setter
    def readonly(self, value):
        self._body['readonly'] = value

    @property
    def profile(self):
        """
        Get the N1QL profile type.
        :return: The profile type.
        """
        value = self._body.get('profile', PROFILE_OFF)
        return value

    @profile.setter
    def profile(self, value):
        """
        Sets the N1QL profile type. Must be one of: 'off', 'phases', 'timings'
        :param value: The profile type to use.
        :return:
        """
        if value not in VALID_PROFILES:
            raise TypeError('Profile option must be one of: ' + ', '.join(VALID_PROFILES))

        self._body['profile'] = value

    def __repr__(self):
        return ('<{cls} stmt={stmt} at {oid}>'.format(
            cls=self.__class__.__name__,
            stmt=repr(self._body),
            oid=id(self)))


class N1QLRequest(object):
    def __init__(self, params, parent, row_factory=lambda x: x, meta_lookahead = True, **kwargs):
        """
        Object representing the execution of the request on the
        server.

        .. warning::

            You should typically not call this constructor by
            yourself, rather use the :meth:`~.Bucket.n1ql_query`
            method (or one of its async derivatives).

        :param params: An :class:`N1QLQuery` object.
        :param parent: The parent :class:`~.couchbase_core.bucket.Bucket` object
        :param row_factory: Callable which accepts the raw dictionary
            of each row, and can wrap them in a customized class.
            The default is simply to return the dictionary itself.

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
        self.__raw = False
        self.__meta_received = False
        self.buffered_remainder = []
        self.meta_lookahead = meta_lookahead

    def _submit_query(self):
        return self._parent._n1ql_query(self._params.encoded,
                                        not self._params.adhoc,
                                        cross_bucket=self._params.cross_bucket)

    def _start(self):
        if self._mres:
            return

        self._mres = self._submit_query()
        self.__raw = self._mres[None]

    @property
    def raw(self):
        return self.__raw

    @property
    def meta(self):
        return self.meta_retrieve()

    @property
    def metrics(self):
        return self.meta_retrieve().get('metrics', None)

    def meta_retrieve(self, meta_lookahead = None):
        """
        Get metadata from the query itself. This is guaranteed to only
        return a Python dictionary.

        Note that if the query failed, the metadata might not be in JSON
        format, in which case there may be additional, non-JSON data
        which can be retrieved using the following

        ::

            raw_meta = req.raw.value

        :return: A dictionary containing the query metadata
        """
        if not self.__meta_received:
            if meta_lookahead or self.meta_lookahead:
                self.buffered_remainder = list(self)
            else:
                raise RuntimeError(
                    'This property only valid once all rows are received!')

        if isinstance(self.raw.value, dict):
            return self.raw.value
        return {}

    def _clear(self):
        del self._parent
        del self._mres

    def _handle_meta(self, value):
        self.__meta_received = True
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
        else:
            # We can only get here if another concurrent query broke out the
            # event loop before we did.
            return []

    def execute(self):
        """
        Execute the statement and raise an exception on failure.

        This method is useful for statements which modify data or
        indexes, where the application does not need to extract any
        data, but merely determine success or failure.
        """
        for _ in self:
            pass

        return self

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
        # type: ()->JSON
        if self.buffered_remainder:
            while len(self.buffered_remainder)>0:
                yield self.buffered_remainder.pop(0)
        elif not self._do_iter:
            raise AlreadyQueriedError()

        self._start()
        while self._do_iter:
            raw_rows = self.raw.fetch(self._mres)
            for row in self._process_payload(raw_rows):
                yield row
