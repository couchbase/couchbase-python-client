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
from datetime import timedelta
from enum import Enum
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Optional,
                    Union)

from couchbase._utils import (JSONType,
                              timedelta_as_microseconds,
                              to_microseconds)
from couchbase.exceptions import ErrorMapper, InvalidArgumentException
from couchbase.exceptions import exception as CouchbaseBaseException
from couchbase.logic.options import QueryOptionsBase
from couchbase.options import QueryOptions, UnsignedInt64
from couchbase.pycbc_core import n1ql_query
from couchbase.serializer import DefaultJsonSerializer, Serializer
from couchbase.tracing import CouchbaseSpan

if TYPE_CHECKING:
    from couchbase.mutation_state import MutationState  # noqa: F401


class QueryScanConsistency(Enum):
    """
    Represents the various scan consistency options that are available when querying against the query service.

    .. warning::
        Importing :class:`.QueryScanConsistency` from ``couchbase.cluster`` is deprecated.
        :class:`.QueryScanConsistency` should be imported from ``couchbase.n1ql``.

    """

    NOT_BOUNDED = "not_bounded"
    REQUEST_PLUS = "request_plus"
    AT_PLUS = "at_plus"


class QueryProfile(Enum):
    """
    Specifies the profiling mode for a query.

    .. warning::
        Importing :class:`.QueryProfile` from ``couchbase.cluster`` is deprecated.
        :class:`.QueryProfile` should be imported from ``couchbase.n1ql``.

    """

    OFF = "off"
    PHASES = "phases"
    TIMINGS = "timings"


class QueryStatus(Enum):
    """
    Represents the status of a query.
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

    @classmethod   # noqa: C901
    def from_str(cls, value  # type: str   # noqa: C901
                 ) -> str:
        if isinstance(value, str):
            if value == cls.RUNNING.value:
                return cls.RUNNING
            elif value == cls.SUCCESS.value:
                return cls.SUCCESS
            elif value == cls.ERRORS.value:
                return cls.ERRORS
            elif value == cls.COMPLETED.value:
                return cls.COMPLETED
            elif value == cls.STOPPED.value:
                return cls.STOPPED
            elif value == cls.TIMEOUT.value:
                return cls.TIMEOUT
            elif value == cls.CLOSED.value:
                return cls.CLOSED
            elif value == cls.FATAL.value:
                return cls.FATAL
            elif value == cls.ABORTED.value:
                return cls.ABORTED
            elif value == cls.UNKNOWN.value:
                return cls.UNKNOWN
        raise InvalidArgumentException(message=(f"{value} is not a valid QueryStatus option. "
                                                "Excepted str representation of type QueryStatus."))


class QueryProblem(object):
    def __init__(self, raw):
        self._raw = raw

    def code(self) -> int:
        return self._raw.get("code", None)

    def message(self) -> str:
        return self._raw.get("message", None)


class QueryWarning(QueryProblem):
    def __init__(self, query_warning  # type: QueryProblem
                 ):
        super().__init__(query_warning)

    def __repr__(self):
        return "QueryWarning:{}".format(super()._raw)


class QueryError(QueryProblem):
    def __init__(self, query_error  # type: QueryProblem
                 ):
        super().__init__(query_error)

    def __repr__(self):
        return "QueryError:{}".format(super()._raw)


class QueryMetrics(object):
    def __init__(self, raw  # type: Dict[str, Any]
                 ) -> None:
        self._raw = raw

    def elapsed_time(self) -> timedelta:
        """Get the total amount of time spent running the query.

        Returns:
            timedelta: The total amount of time spent running the query.
        """
        us = self._raw.get("elapsed_time") / 1000
        return timedelta(microseconds=us)

    def execution_time(self) -> timedelta:
        """Get the total amount of time spent executing the query.

        Returns:
            timedelta: The total amount of time spent executing the query.
        """
        us = self._raw.get("execution_time") / 1000
        return timedelta(microseconds=us)

    def sort_count(self) -> UnsignedInt64:
        """Get the total number of rows which were part of the sorting for the query.

        Returns:
            :class:`~couchbase.options.UnsignedInt64`: The total number of rows which were part of the
            sorting for the query.
        """
        return UnsignedInt64(self._raw.get("sort_count", 0))

    def result_count(self) -> UnsignedInt64:
        """Get the total number of rows which were part of the result set.

        Returns:
            :class:`~couchbase.options.UnsignedInt64`: The total number of rows which were part of the result set.
        """
        return UnsignedInt64(self._raw.get("result_count", 0))

    def result_size(self) -> UnsignedInt64:
        """Get the total number of bytes which were generated as part of the result set.

        Returns:
            :class:`~couchbase.options.UnsignedInt64`: The total number of bytes which were generated as
            part of the result set.
        """
        return UnsignedInt64(self._raw.get("result_size", 0))

    def mutation_count(self) -> UnsignedInt64:
        """Get the total number of rows which were altered by the query.

        Returns:
            :class:`~couchbase.options.UnsignedInt64`: The total number of rows which were altered by the query.
        """
        return UnsignedInt64(self._raw.get("mutation_count", 0))

    def error_count(self) -> UnsignedInt64:
        """Get the total number of errors which were encountered during the execution of the query.

        Returns:
            :class:`~couchbase.options.UnsignedInt64`: The total number of errors which were encountered during
            the execution of the query.
        """
        return UnsignedInt64(self._raw.get("error_count", 0))

    def warning_count(self) -> UnsignedInt64:
        """Get the total number of warnings which were encountered during the execution of the query.

        Returns:
            :class:`~couchbase.options.UnsignedInt64`: The total number of warnings which were encountered during
            the execution of the query.
        """
        return UnsignedInt64(self._raw.get("warning_count", 0))

    def __repr__(self):
        return "QueryMetrics:{}".format(self._raw)


class QueryMetaData:
    def __init__(self, raw  # type: Dict[str, Any]
                 ) -> None:
        if raw is not None:
            self._raw = raw.get('metadata', None)
            sig = self._raw.get('signature', None)
            if sig is not None:
                self._raw['signature'] = json.loads(sig)
            prof = self._raw.get('profile', None)
            if prof is not None:
                self._raw['profile'] = json.loads(prof)
        else:
            self._raw = None

    def request_id(self) -> str:
        """Get the request ID which is associated with the executed query.

        Returns:
            str: The request ID which is associated with the executed query.
        """
        return self._raw.get("request_id", None)

    def client_context_id(self) -> str:
        """Get the client context id which is assoicated with the executed query.

        Returns:
            str: The client context id which is assoicated with the executed query.
        """
        return self._raw.get("client_context_id", None)

    def status(self) -> QueryStatus:
        """Get the status of the query at the time the query meta-data was generated.

        Returns:
            :class:`.QueryStatus`: The status of the query at the time the query meta-data was generated.
        """
        return QueryStatus.from_str(self._raw.get("status", "unknown"))

    def signature(self) -> Optional[JSONType]:
        """Provides the signature of the query.

        Returns:
            Optional[JSONType]:  The signature of the query.
        """
        return self._raw.get("signature", None)

    def warnings(self) -> List[QueryWarning]:
        """Get warnings that occurred during the execution of the query.

        Returns:
            List[:class:`.QueryWarning`]: Any warnings that occurred during the execution of the query.
        """
        return list(
            map(QueryWarning, self._raw.get("warnings", []))
        )

    def errors(self) -> List[QueryError]:
        """Get errors that occurred during the execution of the query.

        Returns:
            List[:class:`.QueryWarning`]: Any errors that occurred during the execution of the query.
        """
        return list(
            map(QueryError, self._raw.get("errors", []))
        )

    def metrics(self) -> Optional[QueryMetrics]:
        """Get the various metrics which are made available by the query engine.

        Returns:
            Optional[:class:`.QueryMetrics`]: A :class:`.QueryMetrics` instance.
        """
        if "metrics" in self._raw:
            return QueryMetrics(self._raw.get("metrics", {}))
        return None

    def profile(self) -> Optional[JSONType]:
        """Get the various profiling details that were generated during execution of the query.

        Returns:
            Optional[JSONType]: Profiling details.
        """
        return self._raw.get("profile", None)

    def __repr__(self):
        return "QueryMetaData:{}".format(self._raw)


class N1QLQuery:

    # empty transform will skip updating the attribute when creating an
    # N1QLQuery object
    _VALID_OPTS = {
        "timeout": {"timeout": lambda x: x},
        "read_only": {"readonly": lambda x: x},
        "scan_consistency": {"consistency": lambda x: x},
        "consistent_with": {"consistent_with": lambda x: x},
        "adhoc": {"adhoc": lambda x: x},
        "client_context_id": {"client_context_id": lambda x: x},
        "max_parallelism": {"max_parallelism": lambda x: x},
        "pipeline_batch": {"pipeline_batch": lambda x: x},
        "pipeline_cap": {"pipeline_cap": lambda x: x},
        "profile": {"profile": lambda x: x},
        "query_context": {"query_context": lambda x: x},
        "raw": {"raw": lambda x: x},
        "scan_cap": {"scan_cap": lambda x: x},
        "scan_wait": {"scan_wait": timedelta_as_microseconds},
        "metrics": {"metrics": lambda x: x},
        "flex_index": {"flex_index": lambda x: x},
        "preserve_expiry": {"preserve_expiry": lambda x: x},
        "use_replica": {"use_replica": lambda x: x},
        "serializer": {"serializer": lambda x: x},
        "positional_parameters": {},
        "named_parameters": {},
        "span": {"span": lambda x: x}
    }

    def __init__(self, query, *args, **kwargs):

        self._params = {"statement": query}
        self._serializer = DefaultJsonSerializer()
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
        arg_dict = self._params.setdefault("named_parameters", {})
        arg_dict.update(kv)

    def _add_pos_args(self, *args):
        """
        Set values for *positional* placeholders (``$1,$2,...``)

        :param args: Values to be used
        """
        arg_array = self._params.setdefault("positional_parameters", [])
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
        self._params[name] = value

    @property
    def params(self) -> Dict[str, Any]:
        params = self._params

        # couchbase++ wants all args JSONified,
        # For now encode to bytes to make couchbase::json_string <--> std::vector<std::byte> easier
        raw = params.pop('raw', None)
        if raw:
            params['raw'] = {f'{k}': self._serializer.serialize(v) for k, v in raw.items()}

        positional_args = params.pop('positional_parameters', None)
        if positional_args:
            params['positional_parameters'] = [self._serializer.serialize(arg) for arg in positional_args]

        named_params = params.pop('named_parameters', None)
        if named_params:
            params['named_parameters'] = {f'${k}': self._serializer.serialize(v) for k, v in named_params.items()}
        return params

    @property
    def metrics(self) -> bool:
        return self._params.get('metrics', False)

    @metrics.setter
    def metrics(self, value  # type: bool
                ) -> None:
        self.set_option('metrics', value)

    @property
    def statement(self) -> str:
        return self._params['statement']

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
    def readonly(self) -> bool:
        return self._params.get('readonly', False)

    @readonly.setter
    def readonly(self, value  # type: bool
                 ) -> None:
        self._params['readonly'] = value

    @property
    def consistency(self) -> QueryScanConsistency:
        value = self._params.get(
            'scan_consistency', None
        )
        if value is None and 'mutation_state' in self._params:
            return QueryScanConsistency.AT_PLUS
        if value is None:
            return QueryScanConsistency.NOT_BOUNDED
        if isinstance(value, str):
            return QueryScanConsistency.REQUEST_PLUS if value == 'request_plus' else QueryScanConsistency.NOT_BOUNDED

    @consistency.setter
    def consistency(self, value  # type: Union[QueryScanConsistency, str]
                    ) -> None:
        invalid_argument = False
        if 'mutation_state' not in self._params:
            if isinstance(value, QueryScanConsistency):
                if value == QueryScanConsistency.AT_PLUS:
                    invalid_argument = True
                else:
                    self.set_option('scan_consistency', value.value)
            elif isinstance(value, str) and value in [sc.value for sc in QueryScanConsistency]:
                if value == QueryScanConsistency.AT_PLUS.value:
                    invalid_argument = True
                else:
                    self.set_option('scan_consistency', value)
            else:
                raise InvalidArgumentException(message=("Excepted consistency to be either of type "
                                                        "QueryScanConsistency or str representation "
                                                        "of QueryScanConsistency"))

        if invalid_argument:
            raise InvalidArgumentException(message=("Cannot set consistency to AT_PLUS.  Use "
                                                    "consistent_with instead or set consistency "
                                                    "to NOT_BOUNDED or REQUEST_PLUS"))

    @property
    def consistent_with(self):
        return {
            'consistency': self.consistency,
            'scan_vectors': self._params.get('mutation_state', None)
        }

    @consistent_with.setter
    def consistent_with(self, value  # type: MutationState
                        ):
        """
        Indicate that the query should be consistent with one or more
        mutations.

        :param value: The state of the mutations it should be consistent
            with.
        :type state: :class:`~.couchbase.mutation_state.MutationState`
        """
        if self.consistency != QueryScanConsistency.NOT_BOUNDED:
            raise TypeError(
                'consistent_with not valid with other consistency options')

        # avoid circular import
        from couchbase.mutation_state import MutationState  # noqa: F811
        if not (isinstance(value, MutationState) and len(value._sv) > 0):
            raise TypeError('Passed empty or invalid state')
        # 3.x SDK had to set the consistency, couchbase++ will take care of that for us
        self._params.pop('scan_consistency', None)
        self.set_option('mutation_state', list(mt.as_dict() for mt in value._sv))

    @property
    def adhoc(self) -> bool:
        return self._params.get('adhoc', True)

    @adhoc.setter
    def adhoc(self, value  # type: bool
              ) -> None:
        self.set_option('adhoc', value)

    @property
    def client_context_id(self) -> Optional[str]:
        return self._params.get('client_context_id', None)

    @client_context_id.setter
    def client_context_id(self, value  # type: str
                          ) -> None:
        self.set_option('client_context_id', value)

    @property
    def max_parallelism(self) -> Optional[int]:
        return self._params.get('max_parallelism', None)

    @max_parallelism.setter
    def max_parallelism(self, value  # type: int
                        ) -> None:
        self.set_option('max_parallelism', value)

    @property
    def pipeline_batch(self) -> Optional[int]:
        return self._params.get('pipeline_batch', None)

    @pipeline_batch.setter
    def pipeline_batch(self, value  # type: int
                       ) -> None:
        self.set_option('pipeline_batch', value)

    @property
    def pipeline_cap(self) -> Optional[int]:
        return self._params.get('pipeline_cap', None)

    @pipeline_cap.setter
    def pipeline_cap(self, value  # type: int
                     ) -> None:
        self.set_option('pipeline_cap', value)

    @property
    def profile(self) -> QueryProfile:
        value = self._params.get(
            'profile_mode', None
        )

        if value is None:
            return QueryProfile.OFF
        if isinstance(value, str):
            if value == 'off':
                return QueryProfile.OFF
            elif value == 'phases':
                return QueryProfile.PHASES
            else:
                return QueryProfile.TIMINGS

    @profile.setter
    def profile(self, value  # type: Union[QueryProfile, str]
                ) -> None:
        if isinstance(value, QueryProfile):
            self.set_option('profile_mode', value.value)
        elif isinstance(value, str) and value in [pm.value for pm in QueryProfile]:
            self.set_option('profile_mode', value)
        else:
            raise InvalidArgumentException(message=("Excepted profile to be either of type "
                                                    "QueryProfile or str representation of QueryProfile"))

    @property
    def query_context(self) -> Optional[str]:
        return self._params.get('query_context', None)

    @query_context.setter
    def query_context(self, value  # type: str
                      ) -> None:
        self.set_option('query_context', value)

    @property
    def send_to_node(self) -> Optional[str]:
        return self._params.get('send_to_node', None)

    @send_to_node.setter
    def send_to_node(self, value  # type: str
                     ) -> None:
        self.set_option('send_to_node', value)

    @property
    def scan_cap(self) -> Optional[int]:
        return self._params.get('scan_cap', None)

    @scan_cap.setter
    def scan_cap(self, value  # type: int
                 ) -> None:
        self.set_option('scan_cap', value)

    @property
    def scan_wait(self) -> Optional[float]:
        value = self._params.get('scan_wait', None)
        if not value:
            return None
        value = value[:-1]
        return float(value)

    @scan_wait.setter
    def scan_wait(self, value  # type: timedelta
                  ) -> None:
        if not value:
            self._params.pop('scan_wait', 0)
        else:
            # if using the setter, need to validate/transform timedelta, otherwise, just add the value
            if 'scan_wait' in self._params:
                value = timedelta_as_microseconds(value)

            self.set_option('scan_wait', value)

    @property
    def flex_index(self) -> bool:
        return self._params.get('flex_index', False)

    @flex_index.setter
    def flex_index(self, value  # type: bool
                   ) -> None:
        self.set_option('flex_index', value)

    @property
    def preserve_expiry(self) -> bool:
        return self._params.get('preserve_expiry', False)

    @preserve_expiry.setter
    def preserve_expiry(self, value  # type: bool
                        ) -> None:
        self.set_option('preserve_expiry', value)

    @property
    def use_replica(self) -> Optional[bool]:
        return self._params.get('use_replica', None)

    @use_replica.setter
    def use_replica(self, value  # type: Optional[bool]
                    ) -> None:
        self.set_option('use_replica', value)

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
        self.set_option('raw', value)

    @property
    def span(self) -> Optional[CouchbaseSpan]:
        return self._params.get('span', None)

    @span.setter
    def span(self, value  # type CouchbaseSpan
             ) -> None:
        if not issubclass(value.__class__, CouchbaseSpan):
            raise InvalidArgumentException(message='Span should implement CouchbaseSpan interface')
        self.set_option('span', value)

    @property
    def serializer(self) -> Optional[Serializer]:
        return self._params.get('serializer', None)

    @serializer.setter
    def serializer(self, value  # type: Serializer
                   ):
        if not issubclass(value.__class__, Serializer):
            raise InvalidArgumentException(message='Serializer should implement Serializer interface.')
        self.set_option('serializer', value)

    @classmethod
    def create_query_object(cls, statement, *options, **kwargs):
        # lets make a copy of the options, and update with kwargs...
        opt = QueryOptions()
        # TODO: is it possible that we could have [QueryOptions, QueryOptions, ...]??
        #       If so, why???
        opts = list(options)
        for o in opts:
            if isinstance(o, (QueryOptions, QueryOptionsBase)):
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
        query.metrics = args.get("metrics", False)

        for k, v in ((k, args[k]) for k in (args.keys() & cls._VALID_OPTS)):
            for target, transform in cls._VALID_OPTS[k].items():
                setattr(query, target, transform(v))
        return query


class QueryRequestLogic:
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
        self._started_streaming = False
        self._done_streaming = False
        self._metadata = None
        self._default_serializer = kwargs.pop('default_serializer', DefaultJsonSerializer())
        self._serializer = None
        self._streaming_timeout = kwargs.pop('streaming_timeout', None)

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

    def _set_metadata(self, query_response):
        if isinstance(query_response, CouchbaseBaseException):
            raise ErrorMapper.build_exception(query_response)

        self._metadata = QueryMetaData(query_response.raw_result.get('value', None))

    def _submit_query(self, **kwargs):
        if self.done_streaming:
            return

        self._started_streaming = True
        n1ql_kwargs = {
            'conn': self._connection,
            'query_args': self.params,
        }

        streaming_timeout = self.params.get('timeout', self._streaming_timeout)
        if streaming_timeout:
            n1ql_kwargs['streaming_timeout'] = streaming_timeout

        # this is for txcouchbase...
        callback = kwargs.pop('callback', None)
        if callback:
            n1ql_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            n1ql_kwargs['errback'] = errback

        self._streaming_result = n1ql_query(**n1ql_kwargs)
