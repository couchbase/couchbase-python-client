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

import json
from datetime import timedelta
from enum import Enum
from typing import (Any,
                    Dict,
                    List,
                    Optional,
                    Union)

from couchbase._utils import to_microseconds
from couchbase.exceptions import ErrorMapper, InvalidArgumentException
from couchbase.exceptions import exception as CouchbaseBaseException
from couchbase.logic.options import AnalyticsOptionsBase
from couchbase.options import AnalyticsOptions, UnsignedInt64
from couchbase.pycbc_core import analytics_query
from couchbase.serializer import DefaultJsonSerializer, Serializer
from couchbase.tracing import CouchbaseSpan


class AnalyticsScanConsistency(Enum):
    """
    For use with :attr:`~.AnalyticsQuery.consistency`, will allow cached
    values to be returned. This will improve performance but may not
    reflect the latest data in the server.
    """
    NOT_BOUNDED = "not_bounded"
    REQUEST_PLUS = "request_plus"


class AnalyticsStatus(Enum):
    """
    Represents the status of an analytics query.
    """
    RUNNING = "running"
    SUCCESS = "success"
    ERRORS = "errors"
    COMPLETED = "completed"
    STOPPED = "stopped"
    TIMEOUT = "timeout"
    CLOSED = "closed"
    FATAL = "fatal"
    ABORTED = "aborted"
    UNKNOWN = "unknown"


class AnalyticsProblem(object):
    def __init__(self, raw):
        self._raw = raw

    def code(self) -> int:
        return self._raw.get("code", None)

    def message(self) -> str:
        return self._raw.get("message", None)


class AnalyticsWarning(AnalyticsProblem):
    def __init__(self, analytics_warning):
        super().__init__(analytics_warning)

    def __repr__(self):
        return "AnalyticsWarning:{}".format(super()._raw)


class AnalyticsError(AnalyticsProblem):
    def __init__(self, analytics_error):
        super().__init__(analytics_error)

    def __repr__(self):
        return "AnalyticsError:{}".format(super()._raw)


class AnalyticsMetrics(object):

    def __init__(self, raw  # type: Dict[str, Any]
                 ) -> None:
        self._raw = raw

    def elapsed_time(self) -> timedelta:
        us = self._raw.get("elapsed_time") / 1000
        return timedelta(microseconds=us)

    def execution_time(self) -> timedelta:
        us = self._raw.get("execution_time") / 1000
        return timedelta(microseconds=us)

    def result_count(self) -> UnsignedInt64:
        return UnsignedInt64(self._raw.get("result_count", 0))

    def result_size(self) -> UnsignedInt64:
        return UnsignedInt64(self._raw.get("result_size", 0))

    def error_count(self) -> UnsignedInt64:
        return UnsignedInt64(self._raw.get("error_count", 0))

    def processed_objects(self) -> UnsignedInt64:
        return UnsignedInt64(self._raw.get("processed_objects", 0))

    def warning_count(self) -> UnsignedInt64:
        return UnsignedInt64(self._raw.get("warning_count", 0))

    def __repr__(self):
        return "AnalyticsMetrics:{}".format(self._raw)


class AnalyticsMetaData:
    def __init__(self, raw  # type: Dict[str, Any]
                 ) -> None:
        if raw is not None:
            self._raw = raw.get('metadata', None)
            sig = self._raw.get('signature', None)
            if sig is not None:
                self._raw['signature'] = json.loads(sig)
        else:
            self._raw = None

    def request_id(self) -> str:
        return self._raw.get("request_id", None)

    def client_context_id(self) -> str:
        return self._raw.get("client_context_id", None)

    def status(self) -> AnalyticsStatus:
        return AnalyticsStatus[self._raw.get("status", "unknown").upper()]

    def signature(self) -> Optional[Dict[str, Any]]:
        return self._raw.get("signature", None)

    def warnings(self) -> List[AnalyticsWarning]:
        return list(
            map(AnalyticsWarning, self._raw.get("warnings", []))
        )

    def errors(self) -> List[AnalyticsError]:
        return list(
            map(AnalyticsError, self._raw.get("errors", []))
        )

    def metrics(self) -> Optional[AnalyticsMetrics]:
        if "metrics" in self._raw:
            return AnalyticsMetrics(self._raw.get("metrics", {}))
        return None

    def __repr__(self):
        return "AnalyticsMetaData:{}".format(self._raw)


class AnalyticsQuery:

    _VALID_OPTS = {
        'timeout': {'timeout': lambda x: x},
        'read_only': {'readonly': lambda x: x},
        'scan_consistency': {'consistency': lambda x: x.value},
        'client_context_id': {'client_context_id': lambda x: x},
        'priority': {'priority': lambda x: x},
        'query_context': {'query_context': lambda x: x},
        'serializer': {'serializer': lambda x: x},
        'raw': {'raw': lambda x: x},
        'positional_parameters': {},
        'named_parameters': {},
        'span': {'span': lambda x: x}
    }

    def __init__(self, query, *args, **kwargs):

        self._adhoc = True
        self._params = {"statement": query}
        self._raw = None
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
        # named_params = {}
        # for k in kv:
        #     named_params["${0}".format(k)] = json.dumps(kv[k])
        # couchbase++ wants all args JSONified
        named_params = {f'${k}': json.dumps(v) for k, v in kv.items()}

        self._params["named_parameters"] = named_params
        return self

    def _add_pos_args(self, *args):
        """
        Set values for *positional* placeholders (``$1,$2,...``)

        :param args: Values to be used
        """
        arg_array = self._params.setdefault("positional_parameters", [])
        # couchbase++ wants all args JSONified
        json_args = [json.dumps(arg) for arg in args]
        arg_array.extend(json_args)

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

    @property
    def params(self):
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
    def metrics(self):
        return self._params.get("metrics", True)

    @metrics.setter
    def metrics(self, value):
        self.set_option("metrics", value)

    @property
    def priority(self):
        return self._params.get("priority", False)

    @priority.setter
    def priority(self, value):
        self.set_option("priority", value)

    @property
    def statement(self):
        return self._params["statement"]

    @property
    def consistency(self):
        return self._params.get(
            "scan_consistency", AnalyticsScanConsistency.NOT_BOUNDED.value
        )

    @consistency.setter
    def consistency(self, value):
        self._params["scan_consistency"] = value

    @property
    def client_context_id(self) -> Optional[str]:
        return self._params.get('client_context_id', None)

    @client_context_id.setter
    def client_context_id(self, value  # type: str
                          ) -> None:
        self.set_option('client_context_id', value)

    @property
    def readonly(self):
        value = self._params.get("readonly", False)
        return value

    @readonly.setter
    def readonly(self, value):
        self._params["readonly"] = value

    @property
    def query_context(self) -> Optional[str]:
        return self._params.get('scope_qualifier', None)

    @query_context.setter
    def query_context(self, value  # type: str
                      ) -> None:
        self.set_option('scope_qualifier', value)

    @property
    def serializer(self):
        return self._params.get("serializer", None)

    @serializer.setter
    def serializer(self, value):
        if not issubclass(value.__class__, Serializer):
            raise InvalidArgumentException('Serializer should implement Serializer interface.')
        self._params["serializer"] = value

    @property
    def raw(self) -> Optional[Dict[str, Any]]:
        return self._params.get('raw', None)

    @raw.setter
    def raw(self, value  # type: Dict[str, Any]
            ) -> None:
        if not isinstance(value, dict):
            raise TypeError("Raw option must be of type Dict[str, Any].")
        for k in value.keys():
            if not isinstance(k, str):
                raise TypeError("key for raw value must be str")
        raw_params = {f'{k}': json.dumps(v) for k, v in value.items()}
        self.set_option('raw', raw_params)

    @property
    def span(self) -> Optional[CouchbaseSpan]:
        return self._params.get('span', None)

    @span.setter
    def span(self, value  # type: CouchbaseSpan
             ):
        if not issubclass(value.__class__, CouchbaseSpan):
            raise InvalidArgumentException('Span should implement CouchbaseSpan interface.')
        self.set_option('span', value)

    @classmethod
    def create_query_object(cls, statement, *options, **kwargs):
        # lets make a copy of the options, and update with kwargs...
        opt = AnalyticsOptions()
        # TODO: is it possible that we could have [AnalyticsOptions, AnalyticsOptions, ...]??
        #       If so, why???
        opts = list(options)
        for o in opts:
            if isinstance(o, (AnalyticsOptions, AnalyticsOptionsBase)):
                opt = o
                opts.remove(o)
        args = opt.copy()
        args.update(kwargs)

        # now lets get positional parameters.  Actual positional
        # params OVERRIDE positional_parameters
        positional_parameters = args.pop("positional_parameters", [])
        if opts and len(opts) > 0:
            positional_parameters = opts

        # now the named parameters.  NOTE: all the kwargs that are
        # not VALID_OPTS must be named parameters, and the kwargs
        # OVERRIDE the list of named_parameters
        new_keys = list(filter(lambda x: x not in cls._VALID_OPTS, args.keys()))
        named_parameters = args.pop("named_parameters", {})
        for k in new_keys:
            named_parameters[k] = args[k]

        query = cls(statement, *positional_parameters, **named_parameters)
        # now lets try to setup the options.
        # but for now we will use the existing N1QLQuery.  Could be we can
        # add to it, etc...

        # default to True on analytics metrics
        query.metrics = args.get("metrics", True)

        for k, v in ((k, args[k]) for k in (args.keys() & cls._VALID_OPTS)):
            for target, transform in cls._VALID_OPTS[k].items():
                setattr(query, target, transform(v))
        return query


class AnalyticsRequestLogic:
    def __init__(self,
                 connection,
                 query_params,
                 row_factory=lambda x: x,
                 **kwargs
                 ):

        self._connection = connection
        self._query_params = query_params
        self.row_factory = row_factory
        self._streaming_result = None
        self._default_serializer = kwargs.pop('default_serializer', DefaultJsonSerializer())
        self._serializer = None
        self._started_streaming = False
        self._streaming_timeout = kwargs.pop('streaming_timeout', None)
        self._done_streaming = False
        self._metadata = None

    @property
    def params(self) -> Dict[str, Any]:
        return self._query_params

    @property
    def serializer(self) -> Serializer:
        if self._serializer:
            return self._serializer

        serializer = self.params.get('serializer', None)
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

    def metadata(self):
        # @TODO:  raise if query isn't complete?
        return self._metadata

    def _set_metadata(self, analytics_response):
        if isinstance(analytics_response, CouchbaseBaseException):
            raise ErrorMapper.build_exception(analytics_response)

        self._metadata = AnalyticsMetaData(analytics_response.raw_result.get('value', None))

    def _submit_query(self, **kwargs):
        if self.done_streaming:
            return

        self._started_streaming = True
        analytics_kwargs = {
            'conn': self._connection,
        }
        analytics_kwargs.update(self.params)

        streaming_timeout = self.params.get('timeout', self._streaming_timeout)
        if streaming_timeout:
            analytics_kwargs['streaming_timeout'] = streaming_timeout

        # this is for txcouchbase...
        callback = kwargs.pop('callback', None)
        if callback:
            analytics_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            analytics_kwargs['errback'] = errback

        self._streaming_result = analytics_query(**analytics_kwargs)

    def __iter__(self):
        raise NotImplementedError(
            'Cannot use synchronous iterator, are you using `async for`?'
        )

    def __aiter__(self):
        raise NotImplementedError(
            'Cannot use asynchronous iterator.'
        )
