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
from collections.abc import Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from couchbase.common._observability import SpanProtocol
from couchbase.common._option_spec import BLOCKS
from couchbase.common._supportability import Supportability
from couchbase.common._transforms import to_milliseconds
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.options import SearchOptions
from couchbase.common.search import (Facet,
                                     HighlightStyle,
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

# The two deprecated search options warn from their setters rather than from `to_wire`, which
# never sees a block a request builder maps, and in the words of their docstring directives.
_DEPRECATED = {key: BLOCKS['SearchOptions'].keys[key].deprecated for key in ('disable_scoring', 'scope_name')}


class _FacetDict(dict[str, Facet]):
    """
    Internal dict subclass which ensures that facets added to this dictionary
    have properly defined ranges.
    """

    # noinspection PyMissingConstructor
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.update(*args, **kwargs)

    def __setitem__(self, key: str, value: Facet) -> None:
        if not isinstance(value, Facet):
            raise ValueError('Can only add facet')
        if hasattr(value, 'date_ranges') and not getattr(value, 'date_ranges'):
            raise InvalidArgumentException(message=(f'{value.__class__.__name__} object must have at '
                                                    'least one range. Use add_range()'))
        if hasattr(value, 'numeric_ranges') and not getattr(value, 'numeric_ranges'):
            raise InvalidArgumentException(message=(f'{value.__class__.__name__} object must have at '
                                                    'least one range. Use add_range()'))

        super().__setitem__(key, value)

    def update(self, *args: Any, **kwargs: Any) -> None:
        if args:
            if len(args) > 1:
                raise TypeError('only one merge at a time!')
            other = dict(args[0])
            for key in other:
                self[key] = other[key]
        for key in kwargs:
            self[key] = kwargs[key]

    def setdefault(self, key: str, value: Any = None) -> Facet:
        if key not in self:
            self[key] = value
        return self[key]


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
        self.facets = _FacetDict(**kwargs.pop('facets', {}))
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
            Supportability.option_deprecated('disable_scoring', message=_DEPRECATED['disable_scoring'])
        self.set_option('disable_scoring', value)

    @property
    def scoring(self) -> dict[str, Any] | None:
        return self._params.get('scoring', None)

    @scoring.setter
    def scoring(self, value: SearchScoring) -> None:
        if not isinstance(value, SearchScoring):
            raise InvalidArgumentException(message=('Expected scoring to be of type SearchScoring, i.e. '
                                                    'ReciprocalRankFusion, RelativeScoreFusion or ScoringNone.'))
        self.set_option('scoring', value.as_encodable())

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
        if not (isinstance(value, list) and all(map(lambda f: isinstance(f, str), value))):
            raise InvalidArgumentException(message='Expected a list of strings')
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
        if isinstance(value, HighlightStyle):
            self.set_option('highlight_style', value.value)
        elif isinstance(value, str):
            if value in [HighlightStyle.Html.value, HighlightStyle.Ansi.value]:
                self.set_option('highlight_style', value)
        else:
            raise InvalidArgumentException(message=("Expected highlight_style to be either of type "
                                                    "HighlightStyle or str representation "
                                                    "of HighlightStyle"))

    @property
    def highlight_fields(self) -> list[str] | None:
        return self._params.get('highlight_fields', None)

    @highlight_fields.setter
    def highlight_fields(self, value: list[str]) -> None:
        if not (isinstance(value, list) and all(map(lambda f: isinstance(f, str), value))):
            raise InvalidArgumentException(message='Expected a list of strings')
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
        invalid_argument = False
        if 'mutation_state' not in self._params:
            if isinstance(value, SearchScanConsistency):
                if value == SearchScanConsistency.AT_PLUS:
                    invalid_argument = True
                else:
                    self.set_option('scan_consistency', value.value)
            elif isinstance(value, str) and value in [sc.value for sc in SearchScanConsistency]:
                if value == SearchScanConsistency.AT_PLUS.value:
                    invalid_argument = True
                else:
                    self.set_option('scan_consistency', value)
            else:
                raise InvalidArgumentException(message=("Expected consistency to be either of type "
                                                        "SearchScanConsistency or str representation "
                                                        "of SearchScanConsistency"))

        if invalid_argument:
            raise InvalidArgumentException(message=("Cannot set consistency to AT_PLUS.  Use "
                                                    "consistent_with instead or set consistency "
                                                    "to NOT_BOUNDED"))

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
        Supportability.option_deprecated('scope_name', message=_DEPRECATED['scope_name'])
        return self._params.get('scope_name', None)

    @scope_name.setter
    def scope_name(self, value: str) -> None:
        Supportability.option_deprecated('scope_name', message=_DEPRECATED['scope_name'])
        self.set_option('scope_name', value)

    @property
    def collections(self) -> list[str] | None:
        return self._params.get('collections', None)

    @collections.setter
    def collections(self, value: list[str]) -> None:
        if not (isinstance(value, list) and all(map(lambda f: isinstance(f, str), value))):
            raise InvalidArgumentException(message='Expected a list of strings')
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
        if isinstance(value, str):
            value = [value]
        if all(map(lambda s: isinstance(s, str), value)):
            self._sort = value
        elif all(map(lambda s: isinstance(s, (Sort, str)), value)):
            self._sort = value
        else:
            raise InvalidArgumentException(
                message='sort option must be either List[str] | List[Sort] | List[Sort | str]')

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
    def serializer(self) -> Serializer | None:
        return self._params.get('serializer', None)

    @serializer.setter
    def serializer(self, value: Serializer) -> None:
        if not issubclass(value.__class__, Serializer):
            raise InvalidArgumentException(message='Serializer should implement Serializer interface.')
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
        if isinstance(value, VectorQueryCombination):
            self.set_option('vector_query_combination', value.value)
        elif isinstance(value, str):
            if value.lower() in [VectorQueryCombination.AND.value, VectorQueryCombination.OR.value]:
                self.set_option('vector_query_combination', value.lower())
        else:
            raise InvalidArgumentException(message=("Expected vector_query_combination to be either of type "
                                                    "VectorQueryCombination or str representation "
                                                    "of VectorQueryCombination"))

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
        if 'scoring' in self._params and self._params.get('disable_scoring', False):
            raise InvalidArgumentException(message='scoring cannot be used together with disable_scoring.')

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
        args = SearchQueryBuilder.get_search_query_args(*options, **kwargs)

        facets = args.pop('facets', {})
        sort = args.pop('sort', None)

        query = cls(index_name, query=search_query, facets=facets, sort=sort)

        # metrics defaults to True
        query.metrics = args.get("metrics", True)

        return cls._apply_opts(query, args)

    @classmethod
    def create_search_query_from_request(cls,
                                         index_name: str,
                                         request: SearchRequest,
                                         *options: SearchOptions | None,
                                         **kwargs: Any) -> SearchQueryBuilder:
        args = SearchQueryBuilder.get_search_query_args(*options, **kwargs)

        facets = args.pop('facets', {})
        sort = args.pop('sort', None)

        # the search_query should default to MatchNoneQuery if SearchRequest only has vector_search
        search_query = request.search_query
        if search_query is None:
            # avoid circular import
            from couchbase.common.search_queries import MatchNoneQuery
            search_query = MatchNoneQuery()

        # only set query vector_query_combination if applicable
        if request.vector_search and request.vector_search.options:
            combo = request.vector_search.options.get('vector_query_combination', None)
            if combo:
                args['vector_query_combination'] = combo

        query = cls(index_name,
                    query=search_query,
                    vector_search=request.vector_search,
                    facets=facets,
                    sort=sort)

        # metrics defaults to True
        query.metrics = args.get("metrics", True)
        # show_request defaults to False
        query.show_request = args.get("show_request", False)

        return cls._apply_opts(query, args)

    @staticmethod
    def get_search_query_args(*options: SearchOptions | None, **kwargs: Any) -> dict[str, Any]:
        # lets make a copy of the options, and update with kwargs...
        opt = SearchOptions()
        opts = list(options)
        for o in opts:
            if isinstance(o, SearchOptions):
                opt = o
                opts.remove(o)
        args = opt.copy()
        args.update(kwargs)
        return args
