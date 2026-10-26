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
from typing import TYPE_CHECKING, Any

from couchbase.common._observability import SpanProtocol
from couchbase.common._transforms import timedelta_as_milliseconds, to_milliseconds
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.n1ql import QueryProfile, QueryScanConsistency
from couchbase.common.options import QueryOptions
from couchbase.serializer import DefaultJsonSerializer, Serializer

if TYPE_CHECKING:
    from couchbase.mutation_state import MutationState  # noqa: F401


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
        "send_to_node": {"send_to_node": lambda x: x},
        "raw": {"raw": lambda x: x},
        "scan_cap": {"scan_cap": lambda x: x},
        "scan_wait": {"scan_wait": lambda x: x},
        "metrics": {"metrics": lambda x: x},
        "flex_index": {"flex_index": lambda x: x},
        "preserve_expiry": {"preserve_expiry": lambda x: x},
        "use_replica": {"use_replica": lambda x: x},
        "serializer": {"serializer": lambda x: x},
        "positional_parameters": {},
        "named_parameters": {},
        "span": {"span": lambda x: x},
        "parent_span": {"parent_span": lambda x: x},
    }

    def __init__(self, query: str, *args: Any, **kwargs: Any) -> None:

        self._params: dict[str, Any] = {"statement": query}
        self._serializer = DefaultJsonSerializer()
        self._raw = None
        if args:
            self._add_pos_args(*args)
        if kwargs:
            self._set_named_args(**kwargs)

    def _set_named_args(self, **kv: Any) -> N1QLQuery:
        """
        Set a named parameter in the query. The named field must
        exist in the query itself.

        Args:
            kv: Key-Value pairs representing values within the query. These values should be stripped of their leading
                `$` identifier.

        """
        arg_dict = self._params.setdefault("named_parameters", {})
        # C++ core wants all args JSONified bytes
        named_params = {f'${k}': json.dumps(v).encode('utf-8') for k, v in kv.items()}
        arg_dict.update(named_params)
        return self

    def _add_pos_args(self, *args: Any) -> None:
        """
        Set values for *positional* placeholders (``$1,$2,...``)

        Args:
            args: Values to be used
        """
        arg_array = self._params.setdefault("positional_parameters", [])
        # C++ core wants all args JSONified bytes
        json_args = [json.dumps(arg).encode('utf-8') for arg in args]
        arg_array.extend(json_args)

    def set_option(self, name: str, value: Any) -> None:
        """
        Set a raw option in the query. This option is encoded
        as part of the query parameters without any client-side
        verification. Use this for settings not directly exposed
        by the Python client.

        Args:
            name: The name of the option
            value: The value of the option
        """
        self._params[name] = value

    @property
    def params(self) -> dict[str, Any]:
        return self._params

    @property
    def metrics(self) -> bool:
        return self._params.get('metrics', False)

    @metrics.setter
    def metrics(self, value: bool) -> None:
        self.set_option('metrics', value)

    @property
    def statement(self) -> str:
        return self._params['statement']

    @property
    def timeout(self) -> float | None:
        total_ms = self._params.get('timeout', None)
        if not total_ms:
            return None
        return total_ms / 1000

    @timeout.setter
    def timeout(self, value: timedelta | float | int) -> None:
        if not value:
            self._params.pop('timeout', 0)
        else:
            total_ms = to_milliseconds(value)
            self.set_option('timeout', total_ms)

    @property
    def readonly(self) -> bool:
        return self._params.get('readonly', False)

    @readonly.setter
    def readonly(self, value: bool) -> None:
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
        raise InvalidArgumentException(
            message=(f"{value} is not a valid QueryScanConsistency option. "
                     "Expected str representation of type QueryScanConsistency."))

    @consistency.setter
    def consistency(self, value: QueryScanConsistency | str) -> None:
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
                raise InvalidArgumentException(message=("Expected consistency to be either of type "
                                                        "QueryScanConsistency or str representation "
                                                        "of QueryScanConsistency"))

        if invalid_argument:
            raise InvalidArgumentException(message=("Cannot set consistency to AT_PLUS.  Use "
                                                    "consistent_with instead or set consistency "
                                                    "to NOT_BOUNDED or REQUEST_PLUS"))

    @property
    def consistent_with(self) -> dict[str, Any]:
        return {
            'consistency': self.consistency,
            'scan_vectors': self._params.get('mutation_state', None)
        }

    @consistent_with.setter
    def consistent_with(self, value: MutationState) -> None:
        """
        Indicate that the query should be consistent with one or more
        mutations.

        Args:
            value (:class:`~couchbase.mutation_state.MutationState`): The state of the mutations it should be
                consistent with.
        """
        if self.consistency != QueryScanConsistency.NOT_BOUNDED:
            raise TypeError('consistent_with not valid with other consistency options')

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
    def adhoc(self, value: bool) -> None:
        self.set_option('adhoc', value)

    @property
    def client_context_id(self) -> str | None:
        return self._params.get('client_context_id', None)

    @client_context_id.setter
    def client_context_id(self, value: str) -> None:
        self.set_option('client_context_id', value)

    @property
    def max_parallelism(self) -> int | None:
        return self._params.get('max_parallelism', None)

    @max_parallelism.setter
    def max_parallelism(self, value: int) -> None:
        self.set_option('max_parallelism', value)

    @property
    def pipeline_batch(self) -> int | None:
        return self._params.get('pipeline_batch', None)

    @pipeline_batch.setter
    def pipeline_batch(self, value: int) -> None:
        self.set_option('pipeline_batch', value)

    @property
    def pipeline_cap(self) -> int | None:
        return self._params.get('pipeline_cap', None)

    @pipeline_cap.setter
    def pipeline_cap(self, value: int) -> None:
        self.set_option('pipeline_cap', value)

    @property
    def profile(self) -> QueryProfile:
        value = self._params.get(
            'profile', None
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
        raise InvalidArgumentException(
            message=(f"{value} is not a valid QueryProfile option. "
                     "Expected str representation of type QueryProfile."))

    @profile.setter
    def profile(self, value: QueryProfile | str) -> None:
        if isinstance(value, QueryProfile):
            self.set_option('profile', value.value)
        elif isinstance(value, str) and value in [pm.value for pm in QueryProfile]:
            self.set_option('profile', value)
        else:
            raise InvalidArgumentException(message=("Expected profile to be either of type "
                                                    "QueryProfile or str representation of QueryProfile"))

    @property
    def query_context(self) -> str | None:
        return self._params.get('query_context', None)

    @query_context.setter
    def query_context(self, value: str) -> None:
        self.set_option('query_context', value)

    @property
    def send_to_node(self) -> str | None:
        return self._params.get('send_to_node', None)

    @send_to_node.setter
    def send_to_node(self, value: str) -> None:
        self.set_option('send_to_node', value)

    @property
    def scan_cap(self) -> int | None:
        return self._params.get('scan_cap', None)

    @scan_cap.setter
    def scan_cap(self, value: int) -> None:
        self.set_option('scan_cap', value)

    @property
    def scan_wait(self) -> float | None:
        total_ms = self._params.get('scan_wait', None)
        if not total_ms:
            return None
        return total_ms / 1000

    @scan_wait.setter
    def scan_wait(self, value: timedelta) -> None:
        if not value:
            self._params.pop('scan_wait', 0)
        else:
            self.set_option('scan_wait', timedelta_as_milliseconds(value))

    @property
    def flex_index(self) -> bool:
        return self._params.get('flex_index', False)

    @flex_index.setter
    def flex_index(self, value: bool) -> None:
        self.set_option('flex_index', value)

    @property
    def preserve_expiry(self) -> bool:
        return self._params.get('preserve_expiry', False)

    @preserve_expiry.setter
    def preserve_expiry(self, value: bool) -> None:
        self.set_option('preserve_expiry', value)

    @property
    def use_replica(self) -> bool | None:
        return self._params.get('use_replica', None)

    @use_replica.setter
    def use_replica(self, value: bool | None) -> None:
        self.set_option('use_replica', value)

    @property
    def raw(self) -> dict[str, Any] | None:
        return self._params.get('raw', None)

    @raw.setter
    def raw(self, value: dict[str, Any]) -> None:
        if not isinstance(value, dict):
            raise TypeError("Raw option must be of type Dict[str, Any].")
        for k in value.keys():
            if not isinstance(k, str):
                raise TypeError("key for raw value must be str")
        raw_params = {f'{k}': json.dumps(v).encode('utf-8') for k, v in value.items()}
        self.set_option('raw', raw_params)

    @property
    def span(self) -> SpanProtocol | None:
        return self._params.get('span', None)

    @span.setter
    def span(self, value: SpanProtocol) -> None:
        self.set_option('span', value)

    @property
    def parent_span(self) -> SpanProtocol | None:
        return self._params.get('parent_span', None)

    @parent_span.setter
    def parent_span(self, value: SpanProtocol) -> None:
        self.set_option('parent_span', value)

    @property
    def serializer(self) -> Serializer | None:
        return self._params.get('serializer', None)

    @serializer.setter
    def serializer(self, value: Serializer) -> None:
        if not issubclass(value.__class__, Serializer):
            raise InvalidArgumentException(message='Serializer should implement Serializer interface.')
        self.set_option('serializer', value)

    @classmethod
    def create_query_object(cls, statement: str, *options: Any, **kwargs: Any) -> N1QLQuery:
        # lets make a copy of the options, and update with kwargs...
        opt = QueryOptions()
        opts = list(options)
        for o in opts:
            if isinstance(o, QueryOptions):
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
