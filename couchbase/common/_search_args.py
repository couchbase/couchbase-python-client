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

"""What a search caller's arguments mean, whichever protocol sends them."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

from couchbase.common._option_spec import BLOCKS
from couchbase.common._supportability import Supportability
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.options import SearchOptions
from couchbase.common.search import (Facet,
                                     HighlightStyle,
                                     SearchScanConsistency,
                                     Sort)
from couchbase.common.search_queries import MatchNoneQuery, SearchQuery
from couchbase.common.search_request import SearchRequest
from couchbase.common.search_scoring import SearchScoring
from couchbase.common.serializer import Serializer
from couchbase.common.vector_search import VectorQueryCombination, VectorSearch

# The two deprecated search options warn when they are read, in the words of their docstring directives.
_DEPRECATED = {key: BLOCKS['SearchOptions'].keys[key].deprecated for key in ('disable_scoring', 'scope_name')}


class FacetDict(dict[str, Facet]):
    """A dict of facets by name that refuses anything but a facet, and a range facet with no range."""

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


def search_args(*options: SearchOptions | None, **kwargs: Any) -> dict[str, Any]:
    """A `SearchOptions` from `options`, with `kwargs` laid over it.

    With more than one, which is used depends on their positions: the first of two in a row, the
    third of three in a row. PYCBC-2019 makes that consistent.
    """
    opt = SearchOptions()
    opts = list(options)
    for o in opts:
        if isinstance(o, SearchOptions):
            opt = o
            opts.remove(o)
    args = opt.copy()
    args.update(kwargs)
    return args


def search_argument(request: SearchRequest | SearchQuery) -> tuple[SearchQuery, VectorSearch | None]:
    """`search()`'s argument as its query and its vector search, warning on the deprecated bare query.

    A request holding only a vector search runs it beside a query that matches nothing.
    """
    if isinstance(request, SearchQuery):
        Supportability.method_param_type_deprecated('search', 'request', 'SearchQuery', 'SearchRequest')
        return request, None
    return request.search_query or MatchNoneQuery(), request.vector_search


def check_search_query_argument(query: object) -> None:
    """`search_query()` takes a query; a request goes to `search()`."""
    if isinstance(query, SearchRequest):
        raise InvalidArgumentException(
            message=('A SearchRequest is not a valid search_query() query. '
                     'Pass it to search() instead.'))


def warn_deprecated(key: str) -> None:
    """Warn that `key`, `disable_scoring` or `scope_name`, is deprecated."""
    Supportability.option_deprecated(key, message=_DEPRECATED[key])


def check_string_list(value: object) -> None:
    if not (isinstance(value, list) and all(map(lambda f: isinstance(f, str), value))):
        raise InvalidArgumentException(message='Expected a list of strings')


def highlight_style(value: object) -> HighlightStyle | None:
    """The style asked for, or None for a str that names none, which is ignored."""
    if isinstance(value, HighlightStyle):
        return value
    if isinstance(value, str):
        return next((style for style in HighlightStyle if style.value == value), None)
    raise InvalidArgumentException(message=("Expected highlight_style to be either of type "
                                            "HighlightStyle or str representation "
                                            "of HighlightStyle"))


def scan_consistency(value: object) -> SearchScanConsistency:
    """The consistency asked for.  `AT_PLUS` is refused: `consistent_with` is the way to ask for it."""
    if isinstance(value, str) and value in [sc.value for sc in SearchScanConsistency]:
        value = SearchScanConsistency(value)
    if not isinstance(value, SearchScanConsistency):
        raise InvalidArgumentException(message=("Expected consistency to be either of type "
                                                "SearchScanConsistency or str representation "
                                                "of SearchScanConsistency"))
    if value == SearchScanConsistency.AT_PLUS:
        raise InvalidArgumentException(message=("Cannot set consistency to AT_PLUS.  Use "
                                                "consistent_with instead or set consistency "
                                                "to NOT_BOUNDED"))
    return value


def scoring(value: object) -> SearchScoring:
    if not isinstance(value, SearchScoring):
        raise InvalidArgumentException(message=('Expected scoring to be of type SearchScoring, i.e. '
                                                'ReciprocalRankFusion, RelativeScoreFusion or ScoringNone.'))
    return value


def check_scoring_alone(has_scoring: bool, disable_scoring: object) -> None:
    if has_scoring and disable_scoring:
        raise InvalidArgumentException(message='scoring cannot be used together with disable_scoring.')


def check_serializer(value: object) -> None:
    if not isinstance(value, Serializer):
        raise InvalidArgumentException(message='Serializer should implement Serializer interface.')


def sort(value: Any) -> Sequence[str | Sort]:
    """The sort as field names and sort objects; a lone str is a list of one."""
    if isinstance(value, str):
        value = [value]
    if all(map(lambda s: isinstance(s, (Sort, str)), value)):
        return cast('Sequence[str | Sort]', value)
    raise InvalidArgumentException(message='sort option must be list[str] | list[Sort] | list[Sort | str]')


def vector_query_combination(value: object) -> VectorQueryCombination | None:
    """The combination asked for, or None for a str that names none, which is ignored."""
    if isinstance(value, VectorQueryCombination):
        return value
    if isinstance(value, str):
        return next((c for c in VectorQueryCombination if c.value == value.lower()), None)
    raise InvalidArgumentException(message=("Expected vector_query_combination to be either of type "
                                            "VectorQueryCombination or str representation "
                                            "of VectorQueryCombination"))
