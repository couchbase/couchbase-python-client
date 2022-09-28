#  Copyright 2016-2022. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Optional,
                    Tuple,
                    Union)

from couchbase._utils import to_microseconds
from couchbase.exceptions import ErrorMapper, InvalidArgumentException
from couchbase.exceptions import exception as CouchbaseBaseException
from couchbase.logic.options import ViewOptionsBase
from couchbase.management.views import DesignDocumentNamespace
from couchbase.options import UnsignedInt64, ViewOptions
from couchbase.pycbc_core import view_query
from couchbase.serializer import DefaultJsonSerializer, Serializer
from couchbase.tracing import CouchbaseSpan

if TYPE_CHECKING:
    from couchbase._utils import JSONType


class ViewScanConsistency(Enum):
    NOT_BOUNDED = 'ok'
    REQUEST_PLUS = 'false'
    UPDATE_AFTER = 'update_after'


class ViewOrdering(Enum):
    DESCENDING = 'true'
    ASCENDING = 'false'


class ViewErrorMode(Enum):
    CONTINUE = 'continue'
    STOP = 'stop'


class ViewMetaData:
    def __init__(self, raw  # type: Dict[str, Any]
                 ) -> None:
        if raw is not None:
            self._raw = raw.get('metadata', None)
        else:
            self._raw = None

    def debug_info(self) -> Optional[str]:
        return self._raw.get("debug_info", None)

    def total_rows(self) -> Optional[UnsignedInt64]:
        return self._raw.get("total_rows", None)

    def __repr__(self):
        return f'ViewMetaData({self._raw})'


@dataclass
class ViewRow(object):
    key: str = None
    id: str = None
    value: object = None
    document: object = None


class ViewQuery:

    # empty transform will skip updating the attribute when creating an
    # N1QLQuery object
    _VALID_OPTS = {
        "timeout": {"timeout": lambda x: x},
        "skip": {"skip": lambda x: x},
        "limit": {"limit": lambda x: x},
        "scan_consistency": {"consistency": lambda x: x},
        "startkey": {"startkey": lambda x: x},
        "endkey": {"endkey": lambda x: x},
        "startkey_docid": {"startkey_docid": lambda x: x},
        "endkey_docid": {"endkey_docid": lambda x: x},
        "inclusive_end": {"inclusive_end": lambda x: x},
        "group": {"group": lambda x: x},
        "group_level": {"group_level": lambda x: x},
        "key": {"key": lambda x: x},
        "keys": {"keys": lambda x: x},
        "reduce": {"reduce": lambda x: x},
        "order": {"order": lambda x: x},
        "on_error": {"on_error": lambda x: x},
        "namespace": {"namespace": lambda x: x},
        "debug": {"debug": lambda x: x},
        "client_context_id": {"client_context_id": lambda x: x},
        "raw": {"raw": lambda x: x},
        "query_string": {"query_string": lambda x: x},
        "serializer": {"serializer": lambda x: x},
        "span": {"span": lambda x: x}
    }

    def __init__(self,
                 bucket_name,  # type: str
                 design_doc_name,  # type: str
                 view_name,  # type: str
                 *args,
                 **kwargs):
        self._params = {
            'bucket_name': bucket_name,
            'document_name': design_doc_name,
            'view_name': view_name
        }

    def set_option(self, name, value):
        """
        Set a raw option in the query. This option is encoded
        as part of the query parameters without any client-side
        verification. Use this for settings not directly exposed
        by the Python client.

        :param name: The name of the option
        :param value: The value of the option
        """
        self._params[name] = value

    # @TODO:  I imagine some things might need to be jsonified...

    def as_encodable(self) -> Dict[str, Any]:
        return self._params

    @property
    def timeout(self) -> Optional[float]:
        value = self._params.get('timeout', None)
        if not value:
            return None
        value = value[:-1]
        return float(value)

    @timeout.setter
    def timeout(self, value  # type: Union[timedelta,float,int]
                ) -> None:
        if not value:
            self._params.pop('timeout', 0)
        else:
            total_us = to_microseconds(value)
            self.set_option('timeout', total_us)

    @property
    def limit(self) -> Optional[int]:
        return self._params.get('limit', None)

    @limit.setter
    def limit(self, value  # type: int
              ) -> None:
        self.set_option('limit', value)

    @property
    def skip(self) -> Optional[int]:
        return self._params.get('skip', None)

    @skip.setter
    def skip(self, value  # type: int
             ) -> None:
        self.set_option('skip', value)

    @property
    def consistency(self) -> ViewScanConsistency:
        value = self._params.get(
            'scan_consistency', None
        )
        if value is None:
            return ViewScanConsistency.NOT_BOUNDED
        if isinstance(value, str):
            if value == 'ok':
                return ViewScanConsistency.NOT_BOUNDED
            elif value == 'false':
                return ViewScanConsistency.REQUEST_PLUS
            else:
                return ViewScanConsistency.UPDATE_AFTER

    @consistency.setter
    def consistency(self, value  # type: Union[ViewScanConsistency, str]
                    ) -> None:
        if isinstance(value, ViewScanConsistency):
            self.set_option('scan_consistency', value.value)
        elif isinstance(value, str) and value in [sc.value for sc in ViewScanConsistency]:
            self.set_option('scan_consistency', value)
        else:
            raise InvalidArgumentException(message=("Excepted consistency to be either of type "
                                                    "ViewScanConsistency or str representation "
                                                    "of ViewScanConsistency"))

    @property
    def startkey(self) -> Optional[str]:
        return self._params.get('start_key', None)

    @startkey.setter
    def startkey(self, value  # type: JSONType
                 ) -> None:
        self.set_option('start_key', json.dumps(value))

    @property
    def endkey(self) -> Optional[str]:
        return self._params.get('end_key', None)

    @endkey.setter
    def endkey(self, value  # type: JSONType
               ) -> None:
        self.set_option('end_key', json.dumps(value))

    @property
    def startkey_docid(self) -> Optional[str]:
        return self._params.get('start_key_doc_id', None)

    @startkey_docid.setter
    def startkey_docid(self, value  # type: str
                       ) -> None:
        self.set_option('start_key_doc_id', value)

    @property
    def endkey_docid(self) -> Optional[str]:
        return self._params.get('end_key_doc_id', None)

    @endkey_docid.setter
    def endkey_docid(self, value  # type: str
                     ) -> None:
        self.set_option('end_key_doc_id', value)

    @property
    def inclusive_end(self) -> Optional[bool]:
        return self._params.get('inclusive_end', None)

    @inclusive_end.setter
    def inclusive_end(self, value  # type: bool
                      ) -> None:
        self.set_option('inclusive_end', value)

    @property
    def group(self) -> Optional[bool]:
        return self._params.get('group', None)

    @group.setter
    def group(self, value  # type: bool
              ) -> None:
        self.set_option('group', value)

    @property
    def group_level(self) -> Optional[int]:
        return self._params.get('group_level', None)

    @group_level.setter
    def group_level(self, value  # type: int
                    ) -> None:
        self.set_option('group_level', value)

    @property
    def key(self) -> Optional[str]:
        return self._params.get('key', None)

    @key.setter
    def key(self, value  # type: JSONType
            ) -> None:
        self.set_option('key', json.dumps(value))

    @property
    def keys(self) -> Optional[List[str]]:
        return self._params.get('keys', None)

    @keys.setter
    def keys(self, value  # type: JSONType
             ) -> None:
        if not isinstance(value, list):
            raise InvalidArgumentException('keys must be a list.')
        # couchbase++ wants a list of JSONified strings
        self.set_option('keys', list(map(lambda k: json.dumps(k), value)))

    @property
    def reduce(self) -> Optional[bool]:
        return self._params.get('reduce', None)

    @reduce.setter
    def reduce(self, value  # type: bool
               ) -> None:
        self.set_option('reduce', value)

    @property
    def order(self) -> ViewOrdering:
        value = self._params.get(
            'order', None
        )
        if value is None:
            return ViewOrdering.DESCENDING
        if isinstance(value, str):
            if value == 'false':
                return ViewOrdering.ASCENDING
            else:
                return ViewOrdering.DESCENDING

    @order.setter
    def order(self, value  # type: Union[ViewOrdering, str]
              ) -> None:
        if isinstance(value, ViewOrdering):
            self.set_option('order', value.value)
        elif isinstance(value, str) and value in [sc.value for sc in ViewOrdering]:
            self.set_option('order', value)
        else:
            raise InvalidArgumentException(message=("Excepted order to be either of type "
                                                    "ViewOrdering or str representation "
                                                    "of ViewOrdering"))

    @property
    def on_error(self) -> ViewErrorMode:
        value = self._params.get(
            'on_error', None
        )
        if value is None:
            return ViewErrorMode.STOP
        if isinstance(value, str):
            if value == 'continue':
                return ViewErrorMode.CONTINUE
            else:
                return ViewErrorMode.STOP

    @on_error.setter
    def on_error(self, value  # type: Union[ViewErrorMode, str]
                 ) -> None:
        if isinstance(value, ViewErrorMode):
            self.set_option('on_error', value.value)
        elif isinstance(value, str) and value in [sc.value for sc in ViewErrorMode]:
            self.set_option('on_error', value)
        else:
            raise InvalidArgumentException(message=("Excepted on_error to be either of type "
                                                    "ViewErrorMode or str representation "
                                                    "of ViewErrorMode"))

    @property
    def namespace(self) -> DesignDocumentNamespace:
        value = self._params.get(
            'namespace', None
        )
        if value is None:
            return DesignDocumentNamespace.DEVELOPMENT

        if isinstance(value, str):
            if value == 'production':
                return DesignDocumentNamespace.PRODUCTION
            else:
                return DesignDocumentNamespace.DEVELOPMENT
        if isinstance(value, bool):
            if not value:
                return DesignDocumentNamespace.PRODUCTION

        return DesignDocumentNamespace.DEVELOPMENT

    @namespace.setter
    def namespace(self, value  # type: Union[DesignDocumentNamespace, str]
                  ) -> None:
        if isinstance(value, DesignDocumentNamespace):
            self.set_option('namespace', value.value)
        elif isinstance(value, str) and value in [sc.value for sc in DesignDocumentNamespace]:
            self.set_option('namespace', value)
        else:
            raise InvalidArgumentException(message=("Excepted namespace to be either of type "
                                                    "DesignDocumentNamespace or str representation "
                                                    "of DesignDocumentNamespace"))

    @property
    def debug(self) -> Optional[bool]:
        return self._params.get('debug', None)

    @debug.setter
    def debug(self, value  # type: bool
              ) -> None:
        self.set_option('debug', value)

    @property
    def raw(self) -> Optional[Tuple[str, Any]]:
        return self._params.get('raw', None)

    @raw.setter
    def raw(self, value  # type: Tuple[str, Any]
            ) -> None:
        self.set_option('raw', value)

    @property
    def query_string(self) -> Optional[List[str]]:
        return self._params.get('query_string', None)

    @query_string.setter
    def query_string(self, value  # type: str
                     ) -> None:
        if not isinstance(value, list):
            raise InvalidArgumentException('query_string must be a list.')
        self.set_option('query_string', value)

    @property
    def client_context_id(self) -> Optional[str]:
        return self._params.get('client_context_id', None)

    @client_context_id.setter
    def client_context_id(self, value  # type: str
                          ) -> None:
        self.set_option('client_context_id', value)

    @property
    def serializer(self) -> Optional[Serializer]:
        return self._params.get('serializer', None)

    @serializer.setter
    def serializer(self, value  # type: Serializer
                   ):
        if not issubclass(value.__class__, Serializer):
            raise InvalidArgumentException(message='Serializer should implement Serializer interface.')
        self.set_option('serializer', value)

    @property
    def span(self) -> Optional[CouchbaseSpan]:
        return self._params.get('span', None)

    @span.setter
    def span(self, value  # type: CouchbaseSpan
             ):
        if not issubclass(value.__class__, CouchbaseSpan):
            raise InvalidArgumentException(message='Span should implement CouchbaseSpan interface')
        self.set_option('span', value)

    @classmethod
    def create_view_query_object(cls,
                                 bucket_name,  # type: str
                                 design_doc_name,  # type: str
                                 view_name,  # type: str
                                 *options,  # type: ViewOptions
                                 **kwargs    # type: Dict[str, Any]
                                 ) -> ViewQuery:

        # lets make a copy of the options, and update with kwargs...
        opt = ViewOptions()
        # TODO: is it possible that we could have [ViewOptions, ViewOptions, ...]??
        #       If so, why???
        opts = list(options)
        for o in opts:
            if isinstance(o, (ViewOptions, ViewOptionsBase)):
                opt = o
                opts.remove(o)
        args = opt.copy()
        args.update(kwargs)

        query = cls(bucket_name, design_doc_name, view_name)

        for k, v in ((k, args[k]) for k in (args.keys() & cls._VALID_OPTS)):
            for target, transform in cls._VALID_OPTS[k].items():
                setattr(query, target, transform(v))
        return query


class ViewRequestLogic:
    def __init__(self,
                 connection,
                 encoded_query,
                 row_factory=ViewRow,
                 **kwargs
                 ):

        self._connection = connection
        self._encoded_query = encoded_query
        self.row_factory = row_factory
        self._streaming_result = None
        self._default_serializer = kwargs.pop('default_serializer', DefaultJsonSerializer())
        self._serializer = None
        self._started_streaming = False
        self._done_streaming = False
        self._metadata = None

    @property
    def encoded_query(self) -> Dict[str, Any]:
        return self._encoded_query

    @property
    def serializer(self) -> Serializer:
        if self._serializer:
            return self._serializer

        serializer = self.encoded_query.get('serializer', None)
        if not serializer:
            serializer = self._default_serializer

        self._serializer = serializer
        return self._serializer

    @property
    def started_streaming(self) -> bool:
        return self._started_streaming

    @property
    def done_streaming(self) -> bool:
        return self._done_streaming

    def metadata(self) -> ViewMetaData:
        # @TODO:  raise if query isn't complete?
        return self._metadata

    def _set_metadata(self, views_response):
        if isinstance(views_response, CouchbaseBaseException):
            raise ErrorMapper.build_exception(views_response)

        self._metadata = ViewMetaData(views_response.raw_result.get('value', None))

    def _submit_query(self, **kwargs):
        if self.done_streaming:
            return

        self._started_streaming = True
        span = self.encoded_query.pop('span', None)
        view_kwargs = {
            'conn': self._connection,
            'op_args': self.encoded_query
        }
        if span:
            view_kwargs['span'] = span
        # this is for txcouchbase...
        callback = kwargs.pop('callback', None)
        if callback:
            view_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            view_kwargs['errback'] = errback

        self._streaming_result = view_query(**view_kwargs)

    def __iter__(self):
        raise NotImplementedError(
            'Cannot use synchronous iterator, are you using `async for`?'
        )

    def __aiter__(self):
        raise NotImplementedError(
            'Cannot use asynchronous iterator.'
        )
