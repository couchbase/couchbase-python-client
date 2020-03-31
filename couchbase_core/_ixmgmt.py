# Copyright 2016, Couchbase, Inc.
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
# This module is used internally by the BucketManager class. Do not use
# this module directly.
import couchbase.exceptions as E
import couchbase_core._libcouchbase as C
from couchbase_core import _to_json

N1QL_PRIMARY_INDEX = '#primary'


def _genprop(dictkey):
    def fget(self):
        return self.raw.get(dictkey, None)

    def fset(self, val):
        self.raw[dictkey] = val

    def fdel(self):
        try:
            del self.raw[dictkey]
        except KeyError:
            pass

    return property(fget, fset, fdel)


class N1qlIndex(object):
    def __init__(self, raw=None):
        if not raw:
            self.raw = {}
            return

        if isinstance(raw, N1qlIndex):
            raw = raw.raw
        self.raw = raw.copy()

    name = _genprop('name')  # type: str
    primary = _genprop('is_primary')  # type: bool
    keyspace = _genprop('keyspace_id')  # type: str
    state = _genprop('state')  # type: str
    condition = _genprop('condition')  # type: str
    fields = _genprop('index_key')  # type: list[str]

    def __repr__(self):
        return ('Index<name={0.name}, primary={0.primary}, raw={0.raw!r}>'
                .format(self))

    def __str__(self):
        return self.name

    @classmethod
    def from_any(cls, obj, bucket):
        """
        Ensure the current object is an index. Always returns a new object
        :param obj: string or IndexInfo object
        :param bucket: The bucket name
        :return: A new IndexInfo object
        """
        if isinstance(obj, cls):
            return cls(obj.raw)

        return cls({
            'namespace_id': 'default',
            'keyspace_id': bucket,
            'name': obj if obj else N1QL_PRIMARY_INDEX,
            'using': 'gsi'
        })


def index_to_rawjson(ix):
    """
    :param ix: dict or IndexInfo object
    :return: serialized JSON
    """
    if isinstance(ix, N1qlIndex):
        ix = ix.raw
    return _to_json(ix)


class IxmgmtRequest(object):
    """
    This class has similar functionality as N1QLRequest and View. It
    implements iteration over index management results.
    """
    def __init__(self, parent, cmd, index, **kwargs):
        """
        :param Client parent: parent Client object
        :param str cmd: Type of command to execute. Can be `watch`, `drop`,
            `create`, `list`, or `build`
        :param index: serialized JSON which should correspond to an index
            definition. :meth:`index_to_rawjson` will do this.
            If `cmd` is `watch` then this should be a *list* of serialized
            JSON strings
        :param kwargs: Additional options for the command.
        """
        self._index = index
        self._cmd = cmd
        self._parent = parent
        self._mres = None
        self.__raw = False
        self._options = kwargs
        self._ignore_exists = kwargs.pop('ignore_exists', False)
        self._ignore_missing = kwargs.pop('ignore_missing', False)

        if cmd == 'create' and self._options.pop('defer', False):
            self._options['flags'] = (
                self._options.setdefault('flags', 0) | C.LCB_N1XSPEC_F_DEFER)

    def _start(self):
        if self._mres:
            return

        if self._cmd == 'watch':
            self._mres = self._parent._ixwatch(
                [index_to_rawjson(x) for x in self._index], **self._options)
        else:
            self._mres = self._parent._ixmanage(
                self._cmd, index_to_rawjson(self._index), **self._options)

        self.__raw = self._mres[None]

    def _process_payload(self, rows):
        return rows if rows else []

    @property
    def raw(self):
        return self.__raw

    def __iter__(self):
        self._start()
        while not self.raw.done:
            try:
                raw_rows = self.__raw.fetch(self._mres)
            except E.CouchbaseException as ex:
                if ex.CODE == E.DocumentExistsException.CODE and self._ignore_exists:
                    break
                elif ex.CODE == E.DocumentNotFoundException.CODE and self._ignore_missing:
                    break
                else:
                    raise

            for row in self._process_payload(raw_rows):
                yield N1qlIndex(row)

    def execute(self):
        return [x for x in self]
