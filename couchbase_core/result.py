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
from couchbase_core._libcouchbase import (
    Result,
    ValueResult,
    OperationResult,
    HttpResult,
    MultiResult,
    ObserveInfo,
    AsyncResult)
from couchbase_core._pyport import long, xrange, izip
import couchbase_core._libcouchbase as C
import couchbase.exceptions as E
from datetime import datetime
import couchbase_core.priv_constants as _P

class SubdocResult(C._SDResult):
    """
    Class for objects returned via a subdoc operation. This may contain
    one or more values depending on the number of input commands.

    The actual values from the result can be retrieved by iteration:

    Iteration::

        for value in rv:
            print(value)

    Index::

        value = rv['some.path']
        value = rv[2]

    Or by using the :meth:`get()` method::

        error, value = rv.get('some.path')
        error, value = rv.get(2)


    Iterator and index access will raise exceptions when encountering
    a non-successful item. The :meth:`get()` usage will not throw an
    exception, however
    """

    def _pycbc_repr_extra(self):
        ret = ["specs={0}".format(repr(self._specs))]
        if hasattr(self, '_results'):
            ret.append('results={0}'.format(repr(self._results)))
        return ret

    def __path2index(self, path):
        if not hasattr(self, '__path_cache'):
            self.__path_cache = {}
            for x in xrange(len(self._specs)):
                spec = self._specs[x]
                self.__path_cache[spec[1]] = x

        return self.__path_cache[path]

    def _resolve(self, item):
        if isinstance(item, (int, long)):
            # Get by value
            return self._results[item]
        else:
            return self._results[self.__path2index(item)]

    def __getitem__(self, item):
        rv = self._resolve(item)
        if rv[0]:
            raise E.exc_from_rc(rv[0], obj=item)
        else:
            return rv[1]

    def __iter__(self):
        for resinfo, specinfo in izip(self._results, self._specs):
            err, value, path = resinfo[0], resinfo[1], specinfo[1]
            if err:
                raise E.exc_from_rc(err, obj=path)
            yield value

    @property
    def command_count(self):
        """
        Total number of input commands received.

        For mutations (i.e. :cb_bmeth:`mutate_in`) this might be more
        than :py:attr:`~.result_count`.
        """
        return len(self._specs)

    @property
    def result_count(self):
        """
        Total number of results available. For mutations, this might be less
        than the :py:attr:`~.command_count`
        """
        try:
            return len(self._results)
        except AttributeError:
            return 0

    def get(self, path_or_index, default=None):
        """
        Get details about a given result

        :param path_or_index: The path (or index) of the result to fetch.
        :param default: If the given result does not exist, return this value
            instead
        :return: A tuple of `(error, value)`. If the entry does not exist
            then `(err, default)` is returned, where `err` is the actual error
            which occurred.
            You can use :meth:`couchbase.exceptions.CouchbaseException.rc_to_exctype`
            to convert the error code to a proper exception class
        :raise: :exc:`IndexError` or :exc:`KeyError` if `path_or_index`
            is not an initially requested path. This is a programming error
            as opposed to a constraint error where the path is not found.
        """
        err, value = self._resolve(path_or_index)
        value = default if err else value
        return err, value

    def exists(self, path_or_index):
        """
        Checks if a path exists in the document. This is meant to be used
        for a corresponding :meth:`~couchbase_core.subdocument.exists` request.

        :param path_or_index: The path (or index) to check
        :return: `True` if the path exists, `False` if the path does not exist
        :raise: An exception if the server-side check failed for a reason other
            than the path not existing.
        """
        result = self._resolve(path_or_index)
        if not result[0]:
            return True
        elif E.PathNotFoundException._can_derive(result[0]):
            return False
        else:
            raise E.exc_from_rc(result[0])

    def __contains__(self, item):
        return self.exists(item)

    @property
    def expiry(self):
        # if expiry is there, it has to be the very first result...
        try:
            ret = self.get('$document.exptime', 0)[1]
            if ret > 0:
                return datetime.fromtimestamp(ret)
            return None
        except KeyError:
            return None

    @property
    def get_full(self):
        # look at specs and get the correct index, as other
        # operations could also have blank path
        for idx, s in enumerate(self._specs):
            if s[0] == _P.SDCMD_GET_FULLDOC:
                return self._results[idx][1]
        return None

    @property
    def access_ok(self):
        """
        Dynamic property indicating if the document could be accessed (and thus
        results can be retrieved)

        :return: True if the document is accessible, False otherwise
        """
        return self.rc == 0 or self.rc == C.LCB_ERR_SUBDOC_PATH_NOT_FOUND

    @property
    def value(self):
        raise AttributeError(".value not applicable in multiple result operation")
