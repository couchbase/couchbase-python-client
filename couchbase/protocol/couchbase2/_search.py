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

"""A search's request to the gateway, and its answer read back as the SDK's rows, facets and metadata.

The gateway takes a search as structured fields rather than the Search service's JSON, so the
query, sort and facet classes are encoded here a second way.  The encoding is total and made at the
call, before anything is sent: each object is encoded or raises `FeatureUnavailableException`, for
exactly what `UNSENDABLE` lists.  An entry lists a class, or a class and one of its fields, and says
when the gateway would send something other than what the object asks for.

Where the gateway's defaults differ from the Search service's and a field can say so, the SDK sends
the service's default: a range's minimum is inclusive.
"""

from __future__ import annotations

import json
import logging
import math
import struct
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from google.protobuf import json_format
from google.protobuf.message import Message

from couchbase.common import _search_args
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._redaction import user_data
from couchbase.common.exceptions import FeatureUnavailableException, InvalidArgumentException
from couchbase.common.search import (DateFacet,
                                     Facet,
                                     MatchOperator,
                                     NumericFacet,
                                     SearchDateRangeFacet,
                                     SearchFacetResult,
                                     SearchMetaData,
                                     SearchNumericRangeFacet,
                                     SearchRow,
                                     SearchRowFields,
                                     SearchRowLocation,
                                     SearchRowLocations,
                                     SearchScanConsistency,
                                     SearchTermFacet,
                                     Sort,
                                     SortField,
                                     SortGeoDistance,
                                     SortID,
                                     SortScore,
                                     _SearchFinalMetadata,
                                     _SearchMetaDataRecord,
                                     _SearchMetricsRecord)
from couchbase.common.search_queries import (BooleanFieldQuery,
                                             BooleanQuery,
                                             ConjunctionQuery,
                                             DateRangeQuery,
                                             DisjunctionQuery,
                                             DocIdQuery,
                                             GeoBoundingBoxQuery,
                                             GeoDistanceQuery,
                                             GeoPolygonQuery,
                                             MatchAllQuery,
                                             MatchNoneQuery,
                                             MatchPhraseQuery,
                                             MatchQuery,
                                             NumericRangeQuery,
                                             PhraseQuery,
                                             PrefixQuery,
                                             QueryStringQuery,
                                             RawQuery,
                                             RegexQuery,
                                             SearchQuery,
                                             TermQuery,
                                             TermRangeQuery,
                                             WildcardQuery)
from couchbase.common.search_scoring import ScoringNone
from couchbase.protocol.couchbase2._cluster_types import ClusterOptionsBlock, SearchQueryRequest
from couchbase.protocol.couchbase2._core import timeout_seconds
from couchbase.protocol.couchbase2._generated.search.v1 import search_pb2

if TYPE_CHECKING:
    from couchbase.common._observability.handler import OpAttributeOptions
    from couchbase.common.vector_search import VectorSearch
    from couchbase.protocol.couchbase2._core import ClusterCore
    from couchbase.serializer import Serializer

logger = logging.getLogger('couchbase.couchbase2')

_FLOAT32 = struct.Struct('<f')

_UINT32_MAX = 2**32 - 1

_HIGHLIGHT_STYLE = {
    'html': search_pb2.SearchQueryRequest.HIGHLIGHT_STYLE_HTML,
    'ansi': search_pb2.SearchQueryRequest.HIGHLIGHT_STYLE_ANSI,
}

_MATCH_OPERATOR = {
    MatchOperator.OR: search_pb2.MatchQuery.OPERATOR_OR,
    MatchOperator.AND: search_pb2.MatchQuery.OPERATOR_AND,
}


def exact_float32(value: float) -> bool:
    """Whether the gateway carries `value` to the Search service unchanged.

    The gateway holds the value as a 32-bit float (ING-2856) and writes it as the shortest decimal
    that names that float, so the value survives when that decimal is the value itself.
    """
    try:
        double = float(value)
        single = _FLOAT32.unpack(_FLOAT32.pack(double))[0]
    except OverflowError:
        return False
    if not math.isfinite(double):
        return False
    for digits in range(1, 10):
        text = f'{single:.{digits}g}'
        if _FLOAT32.unpack(_FLOAT32.pack(float(text)))[0] == single:
            return float(text) == value
    return bool(single == value)


def _inexact(*values: float | None) -> bool:
    return any(value is not None and not exact_float32(value) for value in values)


def _set(name: str) -> Callable[[Any], bool]:
    return lambda obj: getattr(obj, name) is not None


def _always(obj: object) -> bool:
    return True


def _has_props(obj: Any) -> bool:
    return bool(obj._props)


@dataclass(frozen=True)
class Gap:
    """What the gateway cannot send for an instance of `cls`, or for its `field`, when `applies` says so."""
    cls: type
    field: str | None
    reason: str
    applies: Callable[[Any], bool] = _always


_FLOAT32_BOUND = 'the gateway carries a bound as a 32-bit float, which cannot hold this one'

# Checked in order and the first that applies is reported, so a class's gaps come before its base's.
UNSENDABLE: tuple[Gap, ...] = (
    Gap(RawQuery, None, 'a raw query is JSON, and the gateway takes a query only as its fields'),
    Gap(MatchAllQuery, 'boost', "the gateway's match-all query has no boost", _set('boost')),
    Gap(MatchNoneQuery, 'boost', "the gateway's match-none query has no boost", _set('boost')),
    Gap(NumericRangeQuery, 'min', _FLOAT32_BOUND, lambda q: _inexact(q.min)),
    Gap(NumericRangeQuery, 'max', _FLOAT32_BOUND, lambda q: _inexact(q.max)),
    # ING-2855
    Gap(DateRangeQuery, 'start', "the gateway excludes a date range's start, which the Search service includes "
        'unless inclusive_start is False', lambda q: q.start is not None and q.inclusive_start is not False),
    Gap(DateRangeQuery, 'inclusive_end', "the gateway excludes a date range's end", lambda q: q.inclusive_end is True),
    Gap(SearchQuery, 'set_prop', 'set_prop writes JSON, and the gateway takes a query only as its fields', _has_props),
    Gap(SearchQuery, 'boost', 'the gateway carries a boost as a 32-bit float, which cannot hold this one',
        lambda q: _inexact(q.boost)),
    # ING-2857
    Gap(SortField, 'mode', 'the gateway answers a sort with a mode with an error', _set('mode')),
    Gap(Sort, 'set_prop', 'set_prop writes JSON, and the gateway takes a sort only as its fields', _has_props),
    Gap(Sort, 'by', 'the gateway sorts by a field only through SortField',
        lambda s: type(s) is Sort and s._by not in ('score', 'id')),
    Gap(NumericFacet, 'numeric_ranges', _FLOAT32_BOUND,
        lambda f: any(_inexact(r.get('min'), r.get('max')) for r in f.numeric_ranges)),
    Gap(Facet, 'set_prop', 'set_prop writes JSON, and the gateway takes a facet only as its fields', _has_props),
)


def unsendable(obj: SearchQuery | Sort | Facet) -> str | None:
    """Why the gateway cannot send `obj` as it is, or None when it can."""
    gap = next((g for g in UNSENDABLE if isinstance(obj, g.cls) and g.applies(obj)), None)
    if gap is None:
        return None
    name = type(obj).__name__ if gap.field is None else f'{type(obj).__name__}.{gap.field}'
    return f'{name} cannot be sent over the couchbase2 protocol: {gap.reason}.'


def _check(obj: SearchQuery | Sort | Facet) -> None:
    reason = unsendable(obj)
    if reason is not None:
        raise FeatureUnavailableException(reason)


def _message(message_type: Callable[..., Any], owner: object, **fields: Any) -> Any:
    """`message_type` holding each of `fields` that is set, refusing a value its field cannot hold."""
    try:
        return message_type(**{name: value for name, value in fields.items() if value is not None})
    except (TypeError, ValueError) as e:
        raise InvalidArgumentException(f'{type(owner).__name__} cannot be sent: {e}') from None


def _lat_lng(point: list[float] | None) -> search_pb2.LatLng | None:
    return None if point is None else search_pb2.LatLng(longitude=point[0], latitude=point[1])


def _inclusive_min(minimum: object, inclusive: bool | None) -> bool | None:
    # The gateway reads an unset flag as exclusive, and the Search service's default is inclusive
    # (ING-2854).
    return True if minimum is not None and inclusive is None else inclusive


def _conjunction(q: ConjunctionQuery) -> search_pb2.ConjunctionQuery:
    return _message(search_pb2.ConjunctionQuery, q, queries=[encode_query(c) for c in q.conjuncts], boost=q.boost)


def _disjunction(q: DisjunctionQuery) -> search_pb2.DisjunctionQuery:
    # Always sent, as classic sends it.  The gateway, like the Search service, reads an unset minimum
    # as 0, which would leave a boolean query's should clause optional.
    return _message(search_pb2.DisjunctionQuery, q, queries=[encode_query(c) for c in q.disjuncts],
                    minimum=q.min, boost=q.boost)


def _clause(q: Any, build: Callable[[Any], Any]) -> Any:
    """A boolean query's clause, which is a query of its own and is checked as one."""
    if q is None:
        return None
    q.validate()
    _check(q)
    return build(q)


def _range(message_type: Callable[..., Any]) -> Callable[[Any], Any]:
    return lambda q: _message(message_type, q, min=q.min, max=q.max,
                              inclusive_min=_inclusive_min(q.min, q.inclusive_min), inclusive_max=q.inclusive_max,
                              field=q.field, boost=q.boost)


# Each query class, as the field of the gateway's query that holds it and how it is built.
_QUERIES: dict[type, tuple[str, Callable[[Any], Any]]] = {
    TermQuery: ('term_query', lambda q: _message(search_pb2.TermQuery, q, term=q.term, field=q.field,
                                                 fuzziness=q.fuzziness, prefix_length=q.prefix_length,
                                                 boost=q.boost)),
    QueryStringQuery: ('query_string_query', lambda q: _message(search_pb2.QueryStringQuery, q, query_string=q.query,
                                                                boost=q.boost)),
    WildcardQuery: ('wildcard_query', lambda q: _message(search_pb2.WildcardQuery, q, wildcard=q.wildcard,
                                                         field=q.field, boost=q.boost)),
    DocIdQuery: ('doc_id_query', lambda q: _message(search_pb2.DocIdQuery, q, ids=q.ids, boost=q.boost)),
    MatchQuery: ('match_query', lambda q: _message(
        search_pb2.MatchQuery, q, value=q.match, field=q.field, analyzer=q.analyzer, fuzziness=q.fuzziness,
        prefix_length=q.prefix_length, boost=q.boost,
        operator=None if q.match_operator is None else _MATCH_OPERATOR[q.match_operator])),
    MatchPhraseQuery: ('match_phrase_query', lambda q: _message(search_pb2.MatchPhraseQuery, q, phrase=q.match_phrase,
                                                                field=q.field, analyzer=q.analyzer, boost=q.boost)),
    PhraseQuery: ('phrase_query', lambda q: _message(search_pb2.PhraseQuery, q, terms=q.terms, field=q.field,
                                                     boost=q.boost)),
    PrefixQuery: ('prefix_query', lambda q: _message(search_pb2.PrefixQuery, q, prefix=q.prefix, field=q.field,
                                                     boost=q.boost)),
    RegexQuery: ('regexp_query', lambda q: _message(search_pb2.RegexpQuery, q, regexp=q.regexp, field=q.field,
                                                    boost=q.boost)),
    BooleanFieldQuery: ('boolean_field_query', lambda q: _message(search_pb2.BooleanFieldQuery, q, value=q.bool,
                                                                  field=q.field, boost=q.boost)),
    GeoDistanceQuery: ('geo_distance_query', lambda q: _message(search_pb2.GeoDistanceQuery, q, distance=q.distance,
                                                                center=_lat_lng(q.location), field=q.field,
                                                                boost=q.boost)),
    GeoBoundingBoxQuery: ('geo_bounding_box_query', lambda q: _message(
        search_pb2.GeoBoundingBoxQuery, q, top_left=_lat_lng(q.top_left), bottom_right=_lat_lng(q.bottom_right),
        field=q.field, boost=q.boost)),
    GeoPolygonQuery: ('geo_polygon_query', lambda q: _message(
        search_pb2.GeoPolygonQuery, q, field=q.field, boost=q.boost,
        vertices=None if q.polygon_points is None else [_lat_lng(p) for p in q.polygon_points])),
    NumericRangeQuery: ('numeric_range_query', _range(search_pb2.NumericRangeQuery)),
    DateRangeQuery: ('date_range_query', lambda q: _message(search_pb2.DateRangeQuery, q, start_date=q.start,
                                                            end_date=q.end, date_time_parser=q.datetime_parser,
                                                            field=q.field, boost=q.boost)),
    TermRangeQuery: ('term_range_query', _range(search_pb2.TermRangeQuery)),
    ConjunctionQuery: ('conjunction_query', _conjunction),
    DisjunctionQuery: ('disjunction_query', _disjunction),
    BooleanQuery: ('boolean_query', lambda q: _message(search_pb2.BooleanQuery, q,
                                                       must=_clause(q.must, _conjunction),
                                                       must_not=_clause(q.must_not, _disjunction),
                                                       should=_clause(q.should, _disjunction),
                                                       boost=q.boost)),
    MatchAllQuery: ('match_all_query', lambda q: search_pb2.MatchAllQuery()),
    MatchNoneQuery: ('match_none_query', lambda q: search_pb2.MatchNoneQuery()),
}


def encode_query(query: SearchQuery) -> search_pb2.Query:
    """`query` as the gateway's query, checked first."""
    query.validate()
    _check(query)
    encoder = _QUERIES.get(type(query))
    if encoder is None:
        raise FeatureUnavailableException(f'{type(query).__name__} cannot be sent over the couchbase2 protocol.')
    field, build = encoder
    return search_pb2.Query(**{field: build(query)})


def _string_sort(spec: str) -> search_pb2.Sorting:
    """A sort named the Search service's way: a field, `_id` or `_score`, descending after a `-`."""
    descending = spec.startswith('-')
    name = spec[1:] if descending else spec
    if name == '_score':
        return search_pb2.Sorting(score_sorting=search_pb2.ScoreSorting(descending=descending))
    if name == '_id':
        return search_pb2.Sorting(id_sorting=search_pb2.IdSorting(descending=descending))
    return search_pb2.Sorting(field_sorting=search_pb2.FieldSorting(field=name, descending=descending))


def encode_sort(sort: str | Sort) -> search_pb2.Sorting:
    if isinstance(sort, str):
        return _string_sort(sort)
    _check(sort)
    s: Any = sort
    descending = bool(s.desc)
    if isinstance(s, SortField):
        return search_pb2.Sorting(field_sorting=_message(search_pb2.FieldSorting, s, field=s.field,
                                                         descending=descending, missing=s.missing,
                                                         type=s.type))
    if isinstance(s, SortGeoDistance):
        return search_pb2.Sorting(geo_distance_sorting=_message(search_pb2.GeoDistanceSorting, s, field=s.field,
                                                                descending=descending,
                                                                center=_lat_lng(s.location), unit=s.unit))
    if isinstance(s, SortID) or s._by == 'id':
        return search_pb2.Sorting(id_sorting=search_pb2.IdSorting(descending=descending))
    if isinstance(s, SortScore) or s._by == 'score':
        return search_pb2.Sorting(score_sorting=search_pb2.ScoreSorting(descending=descending))
    raise FeatureUnavailableException(f'{type(s).__name__} cannot be sent over the couchbase2 protocol.')


def encode_facet(facet: Facet) -> search_pb2.Facet:
    """`facet` as the gateway's; one with no limit is sent with a size of 0, as the Search service reads it."""
    _check(facet)
    size = facet.limit or 0
    if isinstance(facet, NumericFacet):
        ranges = [_message(search_pb2.NumericRange, facet, name=r['name'], min=r.get('min'), max=r.get('max'))
                  for r in facet.numeric_ranges]
        return search_pb2.Facet(numeric_range_facet=_message(search_pb2.NumericRangeFacet, facet, field=facet.field,
                                                             size=size, numeric_ranges=ranges))
    if isinstance(facet, DateFacet):
        ranges = [_message(search_pb2.DateRange, facet, name=r['name'], start=r.get('start'), end=r.get('end'))
                  for r in facet.date_ranges]
        return search_pb2.Facet(date_range_facet=_message(search_pb2.DateRangeFacet, facet, field=facet.field,
                                                          size=size, date_ranges=ranges))
    return search_pb2.Facet(term_facet=_message(search_pb2.TermFacet, facet, field=facet.field, size=size))


def _log(label: str, message: Message, asked: object) -> None:
    """`message` as JSON, at INFO when the caller asked for it and at DEBUG otherwise.

    It is what the SDK sends to the gateway or reads from it, not the Search service's JSON, which
    the gateway writes and reads.
    """
    level = logging.INFO if asked else logging.DEBUG
    if logger.isEnabledFor(level):
        logger.log(level, '%s: %s', label,
                   user_data(json_format.MessageToJson(message, preserving_proto_field_name=True, indent=None)))


def _count(args: Mapping[str, Any], key: str) -> int | None:
    value = args.get(key)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= _UINT32_MAX:
        raise InvalidArgumentException(f'Expected {key} to be an int from 0 to {_UINT32_MAX}, got {value!r}.')
    return value


def _unavailable(what: str, why: str) -> FeatureUnavailableException:
    return FeatureUnavailableException(f'{what} is not available over the couchbase2 protocol: {why}.')


def _set_options(message: search_pb2.SearchQueryRequest, args: Mapping[str, Any]) -> None:
    if 'scan_consistency' in args:
        if _search_args.scan_consistency(args['scan_consistency']) != SearchScanConsistency.NOT_BOUNDED:
            raise _unavailable('REQUEST_PLUS scan consistency', 'the gateway reads a search index as it is')
    limit = _count(args, 'limit')
    if limit == 0:
        # ING-2858
        raise _unavailable('A limit of 0', 'the gateway reads it as no limit')
    if limit is not None:
        message.limit = limit
    skip = _count(args, 'skip')
    if skip is not None:
        message.skip = skip
    message.include_explanation = bool(args.get('explain'))
    message.include_locations = bool(args.get('include_locations'))
    for key in ('fields', 'highlight_fields', 'collections'):
        if key in args:
            _search_args.check_string_list(args[key])
            getattr(message, key).extend(args[key])
    style = _search_args.highlight_style(args['highlight_style']) if 'highlight_style' in args else None
    if style is not None:
        message.highlight_style = _HIGHLIGHT_STYLE[style.value]


def _disable_scoring(args: Mapping[str, Any]) -> bool:
    """Whether to score no hit: the deprecated `disable_scoring`, or `ScoringNone`, its replacement."""
    disable_scoring = bool(args.get('disable_scoring', False))
    if disable_scoring:
        _search_args.warn_deprecated('disable_scoring')
    if 'scoring' not in args:
        return disable_scoring
    scoring = _search_args.scoring(args['scoring'])
    _search_args.check_scoring_alone(True, disable_scoring)
    if not isinstance(scoring, ScoringNone):
        raise _unavailable(f'{type(scoring).__name__} scoring',
                           'it combines vector results, which the gateway cannot search for')
    return True


def create_search_request(core: ClusterCore,
                          index_name: str,
                          query: SearchQuery,
                          vector_search: VectorSearch | None,
                          args: dict[str, Any],
                          *,
                          location: tuple[str, str] | None = None) -> SearchQueryRequest:
    """The request for a search of `index_name`, in the bucket and scope `location` names when given.

    `args` are the caller's options and keywords merged; any the gateway cannot act on has already
    been reported, and is not read here.  The gateway does not read a search's bucket and scope
    (ING-2853), so a scope's index is named in full, `bucket.scope.index`, which is the Search
    service's own name for it.
    """
    core.validate_options(ClusterOptionsBlock.Search, args)
    if vector_search is not None:
        # ING-2861
        raise _unavailable('Vector search', 'the gateway answers a search with vector queries with an error')
    if args.get('consistent_with') is not None:
        raise _unavailable('consistent_with', "the gateway's search request carries no mutation state")

    full_name = index_name if location is None else '.'.join((*location, index_name))
    message = search_pb2.SearchQueryRequest(index_name=full_name, query=encode_query(query))

    if 'scope_name' in args:
        _search_args.warn_deprecated('scope_name')
    _set_options(message, args)
    message.disable_scoring = _disable_scoring(args)
    if args.get('sort') is not None:
        for sort in _search_args.sort(args['sort']):
            message.sort.append(encode_sort(sort))
    facets = _search_args.FacetDict(**args.get('facets', {}))
    for name, facet in facets.items():
        message.facets[name].CopyFrom(encode_facet(facet))

    _log('SEARCH', message, args.get('log_request'))

    if 'serializer' in args:
        _search_args.check_serializer(args['serializer'])
    serializer: Serializer = args['serializer'] if 'serializer' in args else core.settings.default_serializer
    attributes: OpAttributeOptions = {
        'parent_span': ObservableRequestHandler.maybe_get_parent_span(span=args.get('span'),
                                                                      parent_span=args.get('parent_span')),
        # The request is lazy, so its span starts when it is sent rather than when it was built.
        'use_now_as_start_time': True,
    }
    if location is not None:
        attributes['bucket_name'], attributes['scope_name'] = location
    return SearchQueryRequest(message=message,
                              serializer=serializer,
                              timeout=timeout_seconds(args, core.search_timeout),
                              span_attributes=attributes,
                              facets=dict(facets),
                              fragments=bool(message.highlight_style or message.highlight_fields),
                              locations=bool(message.include_locations or message.highlight_style
                                             or message.highlight_fields),
                              log_response=bool(args.get('log_response')))


def _fields(fields: Mapping[str, bytes], serializer: Serializer) -> Any:
    """The stored fields, which the gateway sends one by one, read as the one object the service sent."""
    if not fields:
        return None
    body = b'{' + b','.join(json.dumps(name).encode() + b':' + value for name, value in fields.items()) + b'}'
    value = serializer.deserialize(body)
    return SearchRowFields(**value) if isinstance(value, dict) else value


def _location(location: search_pb2.SearchQueryResponse.Location) -> SearchRowLocation:
    return SearchRowLocation(field=location.field,
                             term=location.term,
                             position=location.position,
                             start=location.start,
                             end=location.end,
                             array_positions=list(location.array_positions) or None)


def search_row(hit: search_pb2.SearchQueryResponse.SearchQueryRow, req: SearchQueryRequest) -> SearchRow:
    """`hit` as the SDK's row.

    The gateway highlights every search (ING-2859), so each hit carries fragments and locations
    whether or not the request asked; a row keeps them only when it did, as the Search service would
    send them.
    """
    locations = [_location(location) for location in hit.locations] if req.locations else []
    return SearchRow(index=hit.index,
                     id=hit.id,
                     score=hit.score,
                     fields=_fields(hit.fields, req.serializer),
                     locations=SearchRowLocations(locations) if locations else None,
                     fragments=({name: list(fragment.content) for name, fragment in hit.fragments.items()}
                                if req.fragments else {}),
                     explanation=json.loads(hit.explanation) if hit.explanation else {})


def _requested_ranges(facet: Facet | None, kind: str) -> dict[str, dict[str, Any]]:
    return {r['name']: r for r in getattr(facet, kind, None) or ()}


def search_facet(name: str,
                 result: search_pb2.SearchQueryResponse.FacetResult,
                 requested: Facet | None) -> SearchFacetResult:
    """`result` as the SDK's facet.

    The gateway sends a numeric range's bounds as integers and a date range's as timestamps
    (ING-2860), so each range's bounds are the ones the request gave it, which the service returns
    as given.
    """
    kind = result.WhichOneof('search_facet')
    if kind == 'term_facet':
        term = result.term_facet
        return SearchFacetResult(name=name, field=term.field, total=term.total, missing=term.missing, other=term.other,
                                 terms=[SearchTermFacet(term=t.name, count=t.size) for t in term.terms] or None,
                                 numeric_ranges=None, date_ranges=None)
    if kind == 'numeric_range_facet':
        numeric = result.numeric_range_facet
        sent = _requested_ranges(requested, 'numeric_ranges')
        ranges = [SearchNumericRangeFacet(name=r.name, count=r.size,
                                          min=sent.get(r.name, {}).get('min'), max=sent.get(r.name, {}).get('max'))
                  for r in numeric.numeric_ranges]
        return SearchFacetResult(name=name, field=numeric.field, total=numeric.total, missing=numeric.missing,
                                 other=numeric.other, terms=None, numeric_ranges=ranges or None, date_ranges=None)
    date = result.date_range_facet
    sent = _requested_ranges(requested, 'date_ranges')
    dates = [SearchDateRangeFacet(name=r.name, count=r.size,
                                  start=sent.get(r.name, {}).get('start'), end=sent.get(r.name, {}).get('end'))
             for r in date.date_ranges]
    return SearchFacetResult(name=name, field=date.field, total=date.total, missing=date.missing, other=date.other,
                             terms=None, numeric_ranges=None, date_ranges=dates or None)


def search_metadata(meta: search_pb2.SearchQueryResponse.MetaData) -> _SearchMetaDataRecord:
    """The gateway's metadata as a record.  Its search request carries no client context ID, so it has none."""
    metrics = meta.metrics if meta.HasField('metrics') else None
    return _SearchMetaDataRecord(
        client_context_id=None,
        errors=dict(meta.errors),
        metrics=None if metrics is None else _SearchMetricsRecord(
            took_ns=metrics.execution_time.ToNanoseconds(),
            total_rows=metrics.total_rows,
            max_score=metrics.max_score,
            total_partition_count=metrics.total_partition_count,
            success_partition_count=metrics.success_partition_count,
            error_partition_count=metrics.error_partition_count))


class SearchRows:
    """A search's hits, then its metadata and the facets, which the gateway may send in any message."""

    def __init__(self, req: SearchQueryRequest) -> None:
        self._req = req
        self._hits: deque[search_pb2.SearchQueryResponse.SearchQueryRow] = deque()
        self._facets: dict[str, search_pb2.SearchQueryResponse.FacetResult] = {}
        self._meta_data: search_pb2.SearchQueryResponse.MetaData | None = None

    def take(self, message: search_pb2.SearchQueryResponse) -> None:
        self._hits.extend(message.hits)
        self._facets.update(message.facets)
        if message.HasField('meta_data'):
            self._meta_data = message.meta_data

    def __bool__(self) -> bool:
        return bool(self._hits)

    def next_row(self) -> SearchRow:
        return search_row(self._hits.popleft(), self._req)

    def final_metadata(self) -> _SearchFinalMetadata:
        """The metadata and facets, logged when the request asked, as the hits never are."""
        if self._req.log_response:
            _log('SEARCH RESPONSE', search_pb2.SearchQueryResponse(meta_data=self._meta_data, facets=self._facets),
                 True)
        facets = {name: search_facet(name, result, self._req.facets.get(name))
                  for name, result in self._facets.items()}
        metadata = None if self._meta_data is None else SearchMetaData(search_metadata(self._meta_data))
        return _SearchFinalMetadata(metadata=metadata, facets=facets or None)
