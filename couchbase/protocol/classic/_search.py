#  Copyright 2016-2026. Couchbase, Inc.
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
from collections.abc import Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from couchbase.common import _search_args
from couchbase.common._observability import SpanProtocol
from couchbase.common._search_args import FacetDict
from couchbase.common._transforms import to_milliseconds
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.options import SearchOptions
from couchbase.common.search import (HighlightStyle,
                                     SearchScanConsistency,
                                     Sort)
from couchbase.common.search_scoring import SearchScoring
from couchbase.common.vector_search import VectorQueryCombination
from couchbase.serializer import Serializer

if TYPE_CHECKING:
    from couchbase.common.search_queries import SearchQuery
    from couchbase.common.search_request import SearchRequest
    from couchbase.common.vector_search import VectorSearch
    from couchbase.mutation_state import MutationState  # noqa: F401


class SearchQueryBuilder:

    # empty transform will skip updating the attribute when creating an
    # SearchQueryBuilder object
    _VALID_OPTS = {
        "timeout": {"timeout": lambda x: x},
        "limit": {"limit": lambda x: x},
        "skip": {"skip": lambda x: x},
        "explain": {"explain": lambda x: x},
        "fields": {"fields": lambda x: x},
        "highlight_style": {"highlight_style": lambda x: x},
        "highlight_fields": {"highlight_fields": lambda x: x},
        "scan_consistency": {"consistency": lambda x: x},
        "consistent_with": {"consistent_with": lambda x: x},
        "raw": {"raw": lambda x: x},
        "disable_scoring": {"disable_scoring": lambda x: x},
        "scoring": {"scoring": lambda x: x},
        "scope_name": {"scope_name": lambda x: x},
        "collections": {"collections": lambda x: x},
        "include_locations": {"include_locations": lambda x: x},
        "client_context_id": {"client_context_id": lambda x: x},
        "serializer": {"serializer": lambda x: x},
        "facets": {},
        "sort": {},
        "show_request": {"show_request": lambda x: x},
        "span": {"span": lambda x: x},
        "parent_span": {"parent_span": lambda x: x},
        "vector_query_combination": {"vector_query_combination": lambda x: x},
        "log_request": {"log_request": lambda x: x},
        "log_response": {"log_response": lambda x: x}
    }

    def __init__(self,
                 index_name: str,
                 query: SearchQuery,
                 vector_search: VectorSearch | None = None,
                 **kwargs: Any) -> None:

        self._index_name = index_name
        self._params: dict[str, Any] = {}
        self._query = query
        self._vector_search = vector_search
        self.facets = FacetDict(**kwargs.pop('facets', {}))
        self._sort: Sequence[str | Sort] | None = None
        sort = kwargs.pop('sort', None)
        if sort is not None:
            self.sort = sort

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

    def encode_vector_search(self) -> list[dict[str, Any]] | None:
        if self._vector_search is None:
            return None

        encoded_queries = []
        for query in self._vector_search.queries:
            encoded_query = {
                'field': query.field_name,
                'k': query.num_candidates if query.num_candidates is not None else 3
            }
            if query.vector is not None:
                encoded_query['vector'] = query.vector
            else:
                encoded_query['vector_base64'] = query.vector_base64
            if query.boost is not None:
                encoded_query['boost'] = query.boost
            if query.prefilter is not None:
                encoded_query['filter'] = query.prefilter.encodable
            encoded_queries.append(encoded_query)

        return encoded_queries

    def as_encodable(self) -> dict[str, Any]:
        params: dict[str, Any] = {
            'index_name': self._index_name
        }
        query = json.dumps(self._query.encodable)
        params['query'] = query.encode('utf-8')
        vector_search = self.encode_vector_search()
        if vector_search:
            params['vector_search'] = json.dumps(vector_search).encode('utf-8')

        # deprecate the scope_name option, no need to pass it to the C++ client
        # as the search API will not use
        params.update({k: v for k, v in self._params.items() if k not in ['scope_name']})
        # The binding reads the core's enumerator names and takes any other string as
        # `combination_and`, so the builder's `and` and `or` are renamed on the way out.
        combination = params.get('vector_query_combination')
        if combination is not None:
            params['vector_query_combination'] = f'combination_{combination}'

        if self.facets:
            encoded_facets: dict[str, str] = {}
            for name, facet in self.facets.items():
                encoded_facets[name] = json.dumps(facet.encodable)
            params['facets'] = encoded_facets

        if self.sort:
            params['sort_specs'] = [json.dumps(s if isinstance(s, str) else s.as_encodable()) for s in self.sort]

        return params

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
        return self._params.get('metrics', True)

    @metrics.setter
    def metrics(self, value: bool) -> None:
        self.set_option('metrics', value)

    @property
    def limit(self) -> int | None:
        return self._params.get('limit', None)

    @limit.setter
    def limit(self, value: int) -> None:
        self.set_option('limit', value)

    @property
    def skip(self) -> int | None:
        return self._params.get('skip', None)

    @skip.setter
    def skip(self, value: int) -> None:
        self.set_option('skip', value)

    @property
    def explain(self) -> bool:
        return self._params.get('explain', False)

    @explain.setter
    def explain(self, value: bool) -> None:
        self.set_option('explain', value)

    @property
    def disable_scoring(self) -> bool:
        return self._params.get('disable_scoring', False)

    @disable_scoring.setter
    def disable_scoring(self, value: bool) -> None:
        # False keeps the default behavior and does not need a warning.
        if value:
            _search_args.warn_deprecated('disable_scoring')
        self.set_option('disable_scoring', value)

    @property
    def scoring(self) -> dict[str, Any] | None:
        return self._params.get('scoring', None)

    @scoring.setter
    def scoring(self, value: SearchScoring) -> None:
        self.set_option('scoring', _search_args.scoring(value).as_encodable())

    @property
    def include_locations(self) -> bool:
        return self._params.get('include_locations', False)

    @include_locations.setter
    def include_locations(self, value: bool) -> None:
        self.set_option('include_locations', value)

    @property
    def fields(self) -> list[str] | None:
        return self._params.get('fields', None)

    @fields.setter
    def fields(self, value: list[str]) -> None:
        _search_args.check_string_list(value)
        self.set_option('fields', value)

    @property
    def highlight_style(self) -> HighlightStyle | None:
        value = self._params.get('highlight_style', None)
        if isinstance(value, HighlightStyle):
            return value
        if isinstance(value, str):
            return HighlightStyle.Html if value == 'html' else HighlightStyle.Ansi
        return None

    @highlight_style.setter
    def highlight_style(self, value: HighlightStyle | str) -> None:
        style = _search_args.highlight_style(value)
        if style is not None:
            self.set_option('highlight_style', style.value)

    @property
    def highlight_fields(self) -> list[str] | None:
        return self._params.get('highlight_fields', None)

    @highlight_fields.setter
    def highlight_fields(self, value: list[str]) -> None:
        _search_args.check_string_list(value)
        self.set_option('highlight_fields', value)

    @property
    def consistency(self) -> SearchScanConsistency:
        value = self._params.get(
            'scan_consistency', None
        )
        if value is None and 'mutation_state' in self._params:
            return SearchScanConsistency.AT_PLUS
        if value is None:
            return SearchScanConsistency.NOT_BOUNDED
        if isinstance(value, str):
            return SearchScanConsistency.REQUEST_PLUS if value == 'request_plus' else SearchScanConsistency.NOT_BOUNDED
        raise InvalidArgumentException(
            message=(f"{value} is not a valid SearchScanConsistency option. "
                     "Expected str representation of type SearchScanConsistency."))

    @consistency.setter
    def consistency(self, value: SearchScanConsistency | str) -> None:
        if 'mutation_state' not in self._params:
            self.set_option('scan_consistency', _search_args.scan_consistency(value).value)

    @property
    def consistent_with(self) -> dict[str, Any]:
        return {
            'consistency': self.consistency,
            'scan_vectors': self._params.get('mutation_state', None)
        }

    @consistent_with.setter
    def consistent_with(self, value: MutationState) -> None:
        """
        Indicate that the search should be consistent with one or more
        mutations.

        Args:
            value (:class:`~couchbase.mutation_state.MutationState`): The state of the mutations it should be
                consistent with.
        """
        if self.consistency != SearchScanConsistency.NOT_BOUNDED:
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
    def scope_name(self) -> str | None:
        _search_args.warn_deprecated('scope_name')
        return self._params.get('scope_name', None)

    @scope_name.setter
    def scope_name(self, value: str) -> None:
        _search_args.warn_deprecated('scope_name')
        self.set_option('scope_name', value)

    @property
    def collections(self) -> list[str] | None:
        return self._params.get('collections', None)

    @collections.setter
    def collections(self, value: list[str]) -> None:
        _search_args.check_string_list(value)
        self.set_option('collections', value)

    @property
    def client_context_id(self) -> str | None:
        return self._params.get('client_context_id', None)

    @client_context_id.setter
    def client_context_id(self, value: str) -> None:
        self.set_option('client_context_id', value)

    @property
    def sort(self) -> Sequence[str | Sort] | None:
        return self._sort

    @sort.setter
    def sort(self, value: list[str] | list[Sort] | list[str | Sort]) -> None:
        self._sort = _search_args.sort(value)

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
    def serializer(self) -> Serializer | None:
        return self._params.get('serializer', None)

    @serializer.setter
    def serializer(self, value: Serializer) -> None:
        _search_args.check_serializer(value)
        self.set_option('serializer', value)

    @property
    def show_request(self) -> bool:
        return self._params.get('show_request', False)

    @show_request.setter
    def show_request(self, value: bool) -> None:
        self.set_option('show_request', value)

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
    def vector_query_combination(self) -> VectorQueryCombination | None:
        value = self._params.get('vector_query_combination', None)
        if isinstance(value, VectorQueryCombination):
            return value
        if isinstance(value, str):
            return VectorQueryCombination.AND if value == 'and' else VectorQueryCombination.OR
        return None

    @vector_query_combination.setter
    def vector_query_combination(self, value: VectorQueryCombination | str) -> None:
        combination = _search_args.vector_query_combination(value)
        if combination is not None:
            self.set_option('vector_query_combination', combination.value)

    @property
    def log_request(self) -> bool:
        return self._params.get('log_request', False)

    @log_request.setter
    def log_request(self, value: bool) -> None:
        self.set_option('log_request', value)

    @property
    def log_response(self) -> bool:
        return self._params.get('log_response', False)

    @log_response.setter
    def log_response(self, value: bool) -> None:
        self.set_option('log_response', value)

    def _validate_options(self) -> None:
        # Option order is undefined, so validate combinations after applying all options.
        _search_args.check_scoring_alone('scoring' in self._params, self._params.get('disable_scoring', False))

    @classmethod
    def _apply_opts(cls, query: SearchQueryBuilder, args: dict[str, Any]) -> SearchQueryBuilder:
        for k, v in ((k, args[k]) for k in (args.keys() & cls._VALID_OPTS)):
            for target, transform in cls._VALID_OPTS[k].items():
                setattr(query, target, transform(v))
        query._validate_options()
        return query

    @classmethod
    def create_search_query_object(cls,
                                   index_name: str,
                                   search_query: SearchQuery,
                                   *options: SearchOptions | None,
                                   **kwargs: Any) -> SearchQueryBuilder:
        return cls._create(index_name, search_query, None, _search_args.search_args(*options, **kwargs))

    @classmethod
    def create_search_query_from_request(cls,
                                         index_name: str,
                                         request: SearchRequest | SearchQuery,
                                         *options: SearchOptions | None,
                                         **kwargs: Any) -> SearchQueryBuilder:
        """The builder for `search()`, which takes a request, or a bare query with a deprecation warning."""
        search_query, vector_search = _search_args.search_argument(request)
        args = _search_args.search_args(*options, **kwargs)
        # Without showrequest false the service echoes the whole request, vectors included.
        args.setdefault('show_request', False)
        return cls._create(index_name, search_query, vector_search, args)

    @classmethod
    def _create(cls,
                index_name: str,
                search_query: SearchQuery,
                vector_search: VectorSearch | None,
                args: dict[str, Any]) -> SearchQueryBuilder:
        facets = args.pop('facets', {})
        sort = args.pop('sort', None)

        # only set query vector_query_combination if applicable
        if vector_search and vector_search.options:
            combo = vector_search.options.get('vector_query_combination', None)
            if combo:
                args['vector_query_combination'] = combo

        query = cls(index_name, query=search_query, vector_search=vector_search, facets=facets, sort=sort)

        # metrics defaults to True
        query.metrics = args.get("metrics", True)

        return cls._apply_opts(query, args)
