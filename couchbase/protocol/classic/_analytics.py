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
from typing import Any

from couchbase.common._observability import SpanProtocol
from couchbase.common._transforms import to_milliseconds
from couchbase.common.analytics import AnalyticsScanConsistency
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.options import AnalyticsOptions
from couchbase.serializer import Serializer


class AnalyticsQuery:

    _VALID_OPTS = {
        'timeout': {'timeout': lambda x: x},
        'read_only': {'readonly': lambda x: x},
        'scan_consistency': {'consistency': lambda x: x.value},
        'client_context_id': {'client_context_id': lambda x: x},
        'metrics': {'metrics': lambda x: x},
        'priority': {'priority': lambda x: x},
        'query_context': {'query_context': lambda x: x},
        'serializer': {'serializer': lambda x: x},
        'raw': {'raw': lambda x: x},
        'positional_parameters': {},
        'named_parameters': {},
        'span': {'span': lambda x: x},
        'parent_span': {'parent_span': lambda x: x}
    }

    def __init__(self, query: str, *args: Any, **kwargs: Any) -> None:

        self._adhoc = True
        self._params: dict[str, Any] = {"statement": query}
        self._raw = None
        if args:
            self._add_pos_args(*args)
        if kwargs:
            self._set_named_args(**kwargs)

    def _set_named_args(self, **kv: Any) -> AnalyticsQuery:
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
    def metrics(self) -> bool:
        return self._params.get("metrics", True)

    @metrics.setter
    def metrics(self, value: bool) -> None:
        self.set_option("metrics", value)

    @property
    def priority(self) -> bool:
        return self._params.get("priority", False)

    @priority.setter
    def priority(self, value: bool) -> None:
        self.set_option("priority", value)

    @property
    def statement(self) -> str:
        return self._params["statement"]

    @property
    def consistency(self) -> str:
        return self._params.get(
            "scan_consistency", AnalyticsScanConsistency.NOT_BOUNDED.value
        )

    @consistency.setter
    def consistency(self, value: str) -> None:
        self._params["scan_consistency"] = value

    @property
    def client_context_id(self) -> str | None:
        return self._params.get('client_context_id', None)

    @client_context_id.setter
    def client_context_id(self, value: str) -> None:
        self.set_option('client_context_id', value)

    @property
    def readonly(self) -> bool:
        value = self._params.get("readonly", False)
        return value

    @readonly.setter
    def readonly(self, value: bool) -> None:
        self._params["readonly"] = value

    @property
    def query_context(self) -> str | None:
        return self._params.get('scope_qualifier', None)

    @query_context.setter
    def query_context(self, value: str) -> None:
        self.set_option('scope_qualifier', value)

    @property
    def serializer(self) -> Serializer | None:
        return self._params.get("serializer", None)

    @serializer.setter
    def serializer(self, value: Serializer) -> None:
        if not issubclass(value.__class__, Serializer):
            raise InvalidArgumentException('Serializer should implement Serializer interface.')
        self._params["serializer"] = value

    @property
    def raw(self) -> dict[str, Any] | None:
        return self._params.get('raw', None)

    @raw.setter
    def raw(self, value: dict[str, Any]) -> None:
        if not isinstance(value, dict):
            raise TypeError("Raw option must be of type dict[str, Any].")
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

    @classmethod
    def create_query_object(cls, statement: str, *options: Any, **kwargs: Any) -> AnalyticsQuery:
        # lets make a copy of the options, and update with kwargs...
        opt = AnalyticsOptions()
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

        # default to True on analytics metrics
        query.metrics = args.get("metrics", True)

        for k, v in ((k, args[k]) for k in (args.keys() & cls._VALID_OPTS)):
            for target, transform in cls._VALID_OPTS[k].items():
                setattr(query, target, transform(v))
        return query
