import asyncio
import json
import queue
from datetime import timedelta
from enum import Enum
from typing import (Any,
                    Dict,
                    List,
                    Optional)

from couchbase.exceptions import (PYCBC_ERROR_MAP,
                                  CouchbaseException,
                                  ErrorMapper,
                                  ExceptionMap,
                                  InvalidArgumentException)
from couchbase.options import AnalyticsOptions, UnsignedInt64
from couchbase.pycbc_core import analytics_query
from couchbase.serializer import DefaultJsonSerializer, Serializer


class AnalyticsScanConsistency(Enum):
    """
    For use with :attr:`~.AnalyticsQuery.consistency`, will allow cached
    values to be returned. This will improve performance but may not
    reflect the latest data in the server.
    """
    NOT_BOUNDED = "not_bounded"
    REQUEST_PLUS = "request_plus"


class AnalyticsStatus(Enum):
    RUNNING = ()
    SUCCESS = ()
    ERRORS = ()
    COMPLETED = ()
    STOPPED = ()
    TIMEOUT = ()
    CLOSED = ()
    FATAL = ()
    ABORTED = ()
    UNKNOWN = ()


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

    @property
    def _raw_metrics(self):
        return self._raw

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
        'timeout': {'timeout': timedelta.seconds},
        'read_only': {'readonly': lambda x: x},
        'scan_consistency': {'consistency': lambda x: x.value},
        'client_context_id': {'client_context_id': lambda x: x},
        'priority': {'priority': lambda x: x},
        'positional_parameters': {},
        'named_parameters': {},
        'query_context': {'query_context': lambda x: x},
        'raw': {},
        'serializer': {},
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
    def metrics(self):
        return self._params.get("metrics", True)

    @metrics.setter
    def metrics(self, value):
        self.set_option("metrics", value)

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
    def readonly(self):
        value = self._params.get("readonly", False)
        return value

    @readonly.setter
    def readonly(self, value):
        self._params["readonly"] = value

    @property
    def serializer(self):
        return self._params.get("serializer", None)

    @serializer.setter
    def serializer(self, value):
        if not issubclass(value, Serializer):
            raise InvalidArgumentException('Serializer should implement Serializer interface.')
        self._params["serializer"] = value

    @classmethod
    def create_query_object(cls, statement, *options, **kwargs):
        # lets make a copy of the options, and update with kwargs...
        opt = AnalyticsOptions()
        # TODO: is it possible that we could have [QueryOptions, QueryOptions, ...]??
        #       If so, why???
        opts = list(options)
        for o in opts:
            if isinstance(o, AnalyticsOptions):
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

        # default to false on metrics
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
        self._rows = asyncio.Queue()
        self._raw_rows = queue.Queue()
        self._query_request_ftr = None
        self._ROWS_STOP = object()
        self._streaming_result = None
        self._serializer = None
        self._started_streaming = False
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
            serializer = DefaultJsonSerializer()

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

    def _handle_query_result_exc(self, analytics_response):
        base_exc = analytics_response.raw_result.get('exc', None)
        exc_info = analytics_response.raw_result.get('exc_info', None)

        excptn = None
        if base_exc is None and exc_info:
            exc_cls = PYCBC_ERROR_MAP.get(exc_info.get('error_code', None), CouchbaseException)
            new_exc_info = {k: v for k, v in exc_info if k in ['cinfo', 'inner_cause']}
            excptn = exc_cls(message=exc_info.get('message', None), exc_info=new_exc_info)
        else:
            err_ctx = base_exc.error_context()
            if err_ctx is not None:
                excptn = ErrorMapper.parse_error_context(base_exc)
            else:
                exc_cls = PYCBC_ERROR_MAP.get(base_exc.err(), CouchbaseException)
                excptn = exc_cls(message=base_exc.strerror())

        if excptn is None:
            exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
            excptn = exc_cls(message='Unknown error.')

        raise excptn

    def _set_metadata(self, analytics_response):
        has_exception = analytics_response.raw_result.get('has_exception', None)
        if has_exception:
            self._handle_query_result_exc(analytics_response)

        self._metadata = AnalyticsMetaData(analytics_response.raw_result.get('value', None))

    def _submit_query(self):
        if self.done_streaming:
            return

        self._started_streaming = True
        kwargs = {
            'conn': self._connection,
        }
        kwargs.update(self.params)
        self._streaming_result = analytics_query(**kwargs)

    # def _set_metadata(self):
    #     result = self._query_request_ftr.result()
    #     self._metadata = AnalyticsMetaData(result.raw_result.get('value', None))

    # async def handle_query_row(self, row):
    #     print(f'row: {row}')
    #     await self._rows.put(row)
    #     return row

    # def _submit_query(self):
    #     # print(f'submitting query from thread: {current_thread()}')
    #     if self._query_request_ftr is not None:
    #         return

    #     if self.params.get('serializer', None) is None:
    #         self.params['serializer'] = DefaultJsonSerializer()

    #     kwargs = {
    #         'conn': self._connection,
    #         'callback': self._on_query_complete,
    #         'errback': self._on_query_exception,
    #         **self.params
    #     }
    #     print(f'kwargs: {kwargs}')
    #     self._query_request_ftr = self._loop.create_future()
    #     self._streaming_result = analytics_query(**kwargs)

    def _on_query_complete(self, result):
        print(f'_on_query_callback: {result}')
        self._loop.call_soon_threadsafe(self._query_request_ftr.set_result, result)

    def _on_query_exception(self, exc):
        err_ctx = exc.error_context()
        print(f"error context: {err_ctx}")
        if err_ctx is not None:
            excptn = ErrorMapper.parse_error_context(exc)
        else:
            exc_cls = PYCBC_ERROR_MAP.get(exc.err(), CouchbaseException)
            excptn = exc_cls(exc)
        self._loop.call_soon_threadsafe(self._query_request_ftr.set_exception, excptn)

    def __iter__(self):
        raise NotImplementedError(
            'Cannot use synchronous iterator, are you using `async for`?'
        )

    def __aiter__(self):
        raise NotImplementedError(
            'Cannot use asynchronous iterator.'
        )
