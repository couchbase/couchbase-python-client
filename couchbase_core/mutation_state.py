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
from couchbase_core.exceptions import CouchbaseError
from couchbase_core import _from_json, _to_json

class MissingTokenError(CouchbaseError):
    pass


class MutationState(object):
    """
    .. warning::

        The API and implementation of this class are subject to change.

    This class acts as a container for one or more mutations. It may
    then be used with the :meth:`~.N1QLQuery.consistent_with` method to
    indicate that a given query should be bounded by the contained
    mutations.

    Using `consistent_with` is similar to setting
    :attr:`~.N1QLQuery.consistency` to :data:`REQUEST_PLUS`,
    but is more optimal as the query will use cached data, *except*
    when the given mutation(s) are concerned. This option is useful
    for use patterns when an application has just performed a mutation,
    and wishes to perform a query in which the newly-performed mutation
    should reflect on the query results.

    .. note::

        This feature requires Couchbase Server 4.5 or greater,
        and also requires that `fetch_mutation_tokens=true`
        be specified in the connection string when creating
        a :class:`~couchbase_core.bucket.Bucket`

    .. code-block:: python

        cb = Bucket('couchbase://localhost/default?fetch_mutation_tokens=true')

        rvs = cb.upsert_multi({
            'foo': {'type': 'user', 'value': 'a foo value'},
            'bar': {'type': 'user', 'value': 'a bar value'}
        })

        nq = N1QLQuery('SELECT type, value FROM default WHERE type="user"')
        ms = MutationToken()
        ms.add_result(rv
        nq.consistent_with_ops(*rvs.values())
        for row in cb.n1ql_query(nq):
            # ...
    """
    def __init__(self, *docs):
        self._sv = {}
        if docs:
            self.add_results(*docs)

    def _add_scanvec(self, mutinfo):
        """
        Internal method used to specify a scan vector.
        :param mutinfo: A tuple in the form of
            `(vbucket id, vbucket uuid, mutation sequence)`
        """
        vb, uuid, seq, bktname = mutinfo
        self._sv.setdefault(bktname, {})[vb] = (seq, str(uuid))

    def encode(self):
        """
        Encodes this state object to a string. This string may be passed
        to the :meth:`decode` at a later time. The returned object is safe
        for sending over the network.
        :return: A serialized string representing the state
        """
        return _to_json(self._sv)

    def _to_fts_encodable(self):
        if len(self._sv) > 1:
            raise TypeError('Cannot use more than a single bucket with FTS')
        from couchbase_core._pyport import single_dict_key
        out = {}
        for vb, params in self._sv[single_dict_key(self._sv)].items():
            out['{0}/{1}'.format(vb, params[1])] = params[0]
        return out

    @classmethod
    def decode(cls, s):
        """
        Create a :class:`MutationState` from the encoded string
        :param s: The encoded string
        :return: A new MutationState restored from the string
        """
        d = _from_json(s)
        o = MutationState()
        o._sv = d
        # TODO: Validate

    def add_results(self, *rvs, **kwargs):
        """
        Changes the state to reflect the mutation which yielded the given
        result.

        In order to use the result, the `fetch_mutation_tokens` option must
        have been specified in the connection string, _and_ the result
        must have been successful.

        :param rvs: One or more :class:`~.OperationResult` which have been
            returned from mutations
        :param quiet: Suppress errors if one of the results does not
            contain a convertible state.
        :return: `True` if the result was valid and added, `False` if not
            added (and `quiet` was specified
        :raise: :exc:`~.MissingTokenError` if `result` does not contain
            a valid token
        """
        if not rvs:
            raise MissingTokenError.pyexc(message='No results passed')
        for rv in rvs:
            mi = rv._mutinfo
            if not mi:
                if kwargs.get('quiet'):
                    return False
                raise MissingTokenError.pyexc(
                    message='Result does not contain token')
            self._add_scanvec(mi)
        return True

    def add_all(self, bucket, quiet=False):
        """
        Ensures the query result is consistent with all prior
        mutations performed by a given bucket.

        Using this function is equivalent to keeping track of all
        mutations performed by the given bucket, and passing them to
        :meth:`~add_result`

        :param bucket: A :class:`~couchbase_core.bucket.Bucket` object
            used for the mutations
        :param quiet: If the bucket contains no valid mutations, this
            option suppresses throwing exceptions.
        :return: `True` if at least one mutation was added, `False` if none
            were added (and `quiet` was specified)
        :raise: :exc:`~.MissingTokenError` if no mutations were added and
            `quiet` was not specified
        """
        added = False
        for mt in bucket._mutinfo():
            added = True
            self._add_scanvec(mt)
        if not added and not quiet:
            raise MissingTokenError('Bucket object contains no tokens!')
        return added

    def __repr__(self):
        return repr(self._sv)

    def __nonzero__(self):
        return bool(self._sv)

    __bool__ = __nonzero__
