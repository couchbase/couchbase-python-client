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

# !! AUTO-GENERATED from tools/autogen/config/protos.yaml: DO NOT EDIT MANUALLY !!
# Regenerate with `python -m tools.autogen protos generate --write`.

from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class KnnOperator(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    KNN_OPERATOR_OR: _ClassVar[KnnOperator]
    KNN_OPERATOR_AND: _ClassVar[KnnOperator]
KNN_OPERATOR_OR: KnnOperator
KNN_OPERATOR_AND: KnnOperator

class BooleanFieldQuery(_message.Message):
    __slots__ = ("boost", "field", "value")
    BOOST_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    boost: float
    field: str
    value: bool
    def __init__(self, boost: _Optional[float] = ..., field: _Optional[str] = ..., value: bool = ...) -> None: ...

class BooleanQuery(_message.Message):
    __slots__ = ("boost", "must", "must_not", "should")
    BOOST_FIELD_NUMBER: _ClassVar[int]
    MUST_FIELD_NUMBER: _ClassVar[int]
    MUST_NOT_FIELD_NUMBER: _ClassVar[int]
    SHOULD_FIELD_NUMBER: _ClassVar[int]
    boost: float
    must: ConjunctionQuery
    must_not: DisjunctionQuery
    should: DisjunctionQuery
    def __init__(self, boost: _Optional[float] = ..., must: _Optional[_Union[ConjunctionQuery, _Mapping]] = ..., must_not: _Optional[_Union[DisjunctionQuery, _Mapping]] = ..., should: _Optional[_Union[DisjunctionQuery, _Mapping]] = ...) -> None: ...

class ConjunctionQuery(_message.Message):
    __slots__ = ("boost", "queries")
    BOOST_FIELD_NUMBER: _ClassVar[int]
    QUERIES_FIELD_NUMBER: _ClassVar[int]
    boost: float
    queries: _containers.RepeatedCompositeFieldContainer[Query]
    def __init__(self, boost: _Optional[float] = ..., queries: _Optional[_Iterable[_Union[Query, _Mapping]]] = ...) -> None: ...

class DateRangeQuery(_message.Message):
    __slots__ = ("boost", "field", "date_time_parser", "start_date", "end_date")
    BOOST_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    DATE_TIME_PARSER_FIELD_NUMBER: _ClassVar[int]
    START_DATE_FIELD_NUMBER: _ClassVar[int]
    END_DATE_FIELD_NUMBER: _ClassVar[int]
    boost: float
    field: str
    date_time_parser: str
    start_date: str
    end_date: str
    def __init__(self, boost: _Optional[float] = ..., field: _Optional[str] = ..., date_time_parser: _Optional[str] = ..., start_date: _Optional[str] = ..., end_date: _Optional[str] = ...) -> None: ...

class DisjunctionQuery(_message.Message):
    __slots__ = ("boost", "queries", "minimum")
    BOOST_FIELD_NUMBER: _ClassVar[int]
    QUERIES_FIELD_NUMBER: _ClassVar[int]
    MINIMUM_FIELD_NUMBER: _ClassVar[int]
    boost: float
    queries: _containers.RepeatedCompositeFieldContainer[Query]
    minimum: int
    def __init__(self, boost: _Optional[float] = ..., queries: _Optional[_Iterable[_Union[Query, _Mapping]]] = ..., minimum: _Optional[int] = ...) -> None: ...

class DocIdQuery(_message.Message):
    __slots__ = ("boost", "ids")
    BOOST_FIELD_NUMBER: _ClassVar[int]
    IDS_FIELD_NUMBER: _ClassVar[int]
    boost: float
    ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, boost: _Optional[float] = ..., ids: _Optional[_Iterable[str]] = ...) -> None: ...

class LatLng(_message.Message):
    __slots__ = ("latitude", "longitude")
    LATITUDE_FIELD_NUMBER: _ClassVar[int]
    LONGITUDE_FIELD_NUMBER: _ClassVar[int]
    latitude: float
    longitude: float
    def __init__(self, latitude: _Optional[float] = ..., longitude: _Optional[float] = ...) -> None: ...

class GeoBoundingBoxQuery(_message.Message):
    __slots__ = ("boost", "field", "top_left", "bottom_right")
    BOOST_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    TOP_LEFT_FIELD_NUMBER: _ClassVar[int]
    BOTTOM_RIGHT_FIELD_NUMBER: _ClassVar[int]
    boost: float
    field: str
    top_left: LatLng
    bottom_right: LatLng
    def __init__(self, boost: _Optional[float] = ..., field: _Optional[str] = ..., top_left: _Optional[_Union[LatLng, _Mapping]] = ..., bottom_right: _Optional[_Union[LatLng, _Mapping]] = ...) -> None: ...

class GeoDistanceQuery(_message.Message):
    __slots__ = ("boost", "field", "center", "distance")
    BOOST_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    CENTER_FIELD_NUMBER: _ClassVar[int]
    DISTANCE_FIELD_NUMBER: _ClassVar[int]
    boost: float
    field: str
    center: LatLng
    distance: str
    def __init__(self, boost: _Optional[float] = ..., field: _Optional[str] = ..., center: _Optional[_Union[LatLng, _Mapping]] = ..., distance: _Optional[str] = ...) -> None: ...

class GeoPolygonQuery(_message.Message):
    __slots__ = ("boost", "field", "vertices")
    BOOST_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    VERTICES_FIELD_NUMBER: _ClassVar[int]
    boost: float
    field: str
    vertices: _containers.RepeatedCompositeFieldContainer[LatLng]
    def __init__(self, boost: _Optional[float] = ..., field: _Optional[str] = ..., vertices: _Optional[_Iterable[_Union[LatLng, _Mapping]]] = ...) -> None: ...

class MatchAllQuery(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class MatchNoneQuery(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class MatchPhraseQuery(_message.Message):
    __slots__ = ("boost", "field", "phrase", "analyzer")
    BOOST_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    PHRASE_FIELD_NUMBER: _ClassVar[int]
    ANALYZER_FIELD_NUMBER: _ClassVar[int]
    boost: float
    field: str
    phrase: str
    analyzer: str
    def __init__(self, boost: _Optional[float] = ..., field: _Optional[str] = ..., phrase: _Optional[str] = ..., analyzer: _Optional[str] = ...) -> None: ...

class MatchQuery(_message.Message):
    __slots__ = ("boost", "field", "value", "analyzer", "fuzziness", "operator", "prefix_length")
    class Operator(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        OPERATOR_OR: _ClassVar[MatchQuery.Operator]
        OPERATOR_AND: _ClassVar[MatchQuery.Operator]
    OPERATOR_OR: MatchQuery.Operator
    OPERATOR_AND: MatchQuery.Operator
    BOOST_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    ANALYZER_FIELD_NUMBER: _ClassVar[int]
    FUZZINESS_FIELD_NUMBER: _ClassVar[int]
    OPERATOR_FIELD_NUMBER: _ClassVar[int]
    PREFIX_LENGTH_FIELD_NUMBER: _ClassVar[int]
    boost: float
    field: str
    value: str
    analyzer: str
    fuzziness: int
    operator: MatchQuery.Operator
    prefix_length: int
    def __init__(self, boost: _Optional[float] = ..., field: _Optional[str] = ..., value: _Optional[str] = ..., analyzer: _Optional[str] = ..., fuzziness: _Optional[int] = ..., operator: _Optional[_Union[MatchQuery.Operator, str]] = ..., prefix_length: _Optional[int] = ...) -> None: ...

class NumericRangeQuery(_message.Message):
    __slots__ = ("boost", "field", "min", "max", "inclusive_min", "inclusive_max")
    BOOST_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    MIN_FIELD_NUMBER: _ClassVar[int]
    MAX_FIELD_NUMBER: _ClassVar[int]
    INCLUSIVE_MIN_FIELD_NUMBER: _ClassVar[int]
    INCLUSIVE_MAX_FIELD_NUMBER: _ClassVar[int]
    boost: float
    field: str
    min: float
    max: float
    inclusive_min: bool
    inclusive_max: bool
    def __init__(self, boost: _Optional[float] = ..., field: _Optional[str] = ..., min: _Optional[float] = ..., max: _Optional[float] = ..., inclusive_min: bool = ..., inclusive_max: bool = ...) -> None: ...

class PhraseQuery(_message.Message):
    __slots__ = ("boost", "field", "terms")
    BOOST_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    TERMS_FIELD_NUMBER: _ClassVar[int]
    boost: float
    field: str
    terms: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, boost: _Optional[float] = ..., field: _Optional[str] = ..., terms: _Optional[_Iterable[str]] = ...) -> None: ...

class PrefixQuery(_message.Message):
    __slots__ = ("boost", "field", "prefix")
    BOOST_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    boost: float
    field: str
    prefix: str
    def __init__(self, boost: _Optional[float] = ..., field: _Optional[str] = ..., prefix: _Optional[str] = ...) -> None: ...

class QueryStringQuery(_message.Message):
    __slots__ = ("boost", "query_string")
    BOOST_FIELD_NUMBER: _ClassVar[int]
    QUERY_STRING_FIELD_NUMBER: _ClassVar[int]
    boost: float
    query_string: str
    def __init__(self, boost: _Optional[float] = ..., query_string: _Optional[str] = ...) -> None: ...

class RegexpQuery(_message.Message):
    __slots__ = ("boost", "field", "regexp")
    BOOST_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    REGEXP_FIELD_NUMBER: _ClassVar[int]
    boost: float
    field: str
    regexp: str
    def __init__(self, boost: _Optional[float] = ..., field: _Optional[str] = ..., regexp: _Optional[str] = ...) -> None: ...

class TermQuery(_message.Message):
    __slots__ = ("boost", "field", "term", "fuzziness", "prefix_length")
    BOOST_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    FUZZINESS_FIELD_NUMBER: _ClassVar[int]
    PREFIX_LENGTH_FIELD_NUMBER: _ClassVar[int]
    boost: float
    field: str
    term: str
    fuzziness: int
    prefix_length: int
    def __init__(self, boost: _Optional[float] = ..., field: _Optional[str] = ..., term: _Optional[str] = ..., fuzziness: _Optional[int] = ..., prefix_length: _Optional[int] = ...) -> None: ...

class TermRangeQuery(_message.Message):
    __slots__ = ("boost", "field", "min", "max", "inclusive_min", "inclusive_max")
    BOOST_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    MIN_FIELD_NUMBER: _ClassVar[int]
    MAX_FIELD_NUMBER: _ClassVar[int]
    INCLUSIVE_MIN_FIELD_NUMBER: _ClassVar[int]
    INCLUSIVE_MAX_FIELD_NUMBER: _ClassVar[int]
    boost: float
    field: str
    min: str
    max: str
    inclusive_min: bool
    inclusive_max: bool
    def __init__(self, boost: _Optional[float] = ..., field: _Optional[str] = ..., min: _Optional[str] = ..., max: _Optional[str] = ..., inclusive_min: bool = ..., inclusive_max: bool = ...) -> None: ...

class WildcardQuery(_message.Message):
    __slots__ = ("boost", "field", "wildcard")
    BOOST_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    WILDCARD_FIELD_NUMBER: _ClassVar[int]
    boost: float
    field: str
    wildcard: str
    def __init__(self, boost: _Optional[float] = ..., field: _Optional[str] = ..., wildcard: _Optional[str] = ...) -> None: ...

class Query(_message.Message):
    __slots__ = ("boolean_field_query", "boolean_query", "conjunction_query", "date_range_query", "disjunction_query", "doc_id_query", "geo_bounding_box_query", "geo_distance_query", "geo_polygon_query", "match_all_query", "match_none_query", "match_phrase_query", "match_query", "numeric_range_query", "phrase_query", "prefix_query", "query_string_query", "regexp_query", "term_query", "term_range_query", "wildcard_query")
    BOOLEAN_FIELD_QUERY_FIELD_NUMBER: _ClassVar[int]
    BOOLEAN_QUERY_FIELD_NUMBER: _ClassVar[int]
    CONJUNCTION_QUERY_FIELD_NUMBER: _ClassVar[int]
    DATE_RANGE_QUERY_FIELD_NUMBER: _ClassVar[int]
    DISJUNCTION_QUERY_FIELD_NUMBER: _ClassVar[int]
    DOC_ID_QUERY_FIELD_NUMBER: _ClassVar[int]
    GEO_BOUNDING_BOX_QUERY_FIELD_NUMBER: _ClassVar[int]
    GEO_DISTANCE_QUERY_FIELD_NUMBER: _ClassVar[int]
    GEO_POLYGON_QUERY_FIELD_NUMBER: _ClassVar[int]
    MATCH_ALL_QUERY_FIELD_NUMBER: _ClassVar[int]
    MATCH_NONE_QUERY_FIELD_NUMBER: _ClassVar[int]
    MATCH_PHRASE_QUERY_FIELD_NUMBER: _ClassVar[int]
    MATCH_QUERY_FIELD_NUMBER: _ClassVar[int]
    NUMERIC_RANGE_QUERY_FIELD_NUMBER: _ClassVar[int]
    PHRASE_QUERY_FIELD_NUMBER: _ClassVar[int]
    PREFIX_QUERY_FIELD_NUMBER: _ClassVar[int]
    QUERY_STRING_QUERY_FIELD_NUMBER: _ClassVar[int]
    REGEXP_QUERY_FIELD_NUMBER: _ClassVar[int]
    TERM_QUERY_FIELD_NUMBER: _ClassVar[int]
    TERM_RANGE_QUERY_FIELD_NUMBER: _ClassVar[int]
    WILDCARD_QUERY_FIELD_NUMBER: _ClassVar[int]
    boolean_field_query: BooleanFieldQuery
    boolean_query: BooleanQuery
    conjunction_query: ConjunctionQuery
    date_range_query: DateRangeQuery
    disjunction_query: DisjunctionQuery
    doc_id_query: DocIdQuery
    geo_bounding_box_query: GeoBoundingBoxQuery
    geo_distance_query: GeoDistanceQuery
    geo_polygon_query: GeoPolygonQuery
    match_all_query: MatchAllQuery
    match_none_query: MatchNoneQuery
    match_phrase_query: MatchPhraseQuery
    match_query: MatchQuery
    numeric_range_query: NumericRangeQuery
    phrase_query: PhraseQuery
    prefix_query: PrefixQuery
    query_string_query: QueryStringQuery
    regexp_query: RegexpQuery
    term_query: TermQuery
    term_range_query: TermRangeQuery
    wildcard_query: WildcardQuery
    def __init__(self, boolean_field_query: _Optional[_Union[BooleanFieldQuery, _Mapping]] = ..., boolean_query: _Optional[_Union[BooleanQuery, _Mapping]] = ..., conjunction_query: _Optional[_Union[ConjunctionQuery, _Mapping]] = ..., date_range_query: _Optional[_Union[DateRangeQuery, _Mapping]] = ..., disjunction_query: _Optional[_Union[DisjunctionQuery, _Mapping]] = ..., doc_id_query: _Optional[_Union[DocIdQuery, _Mapping]] = ..., geo_bounding_box_query: _Optional[_Union[GeoBoundingBoxQuery, _Mapping]] = ..., geo_distance_query: _Optional[_Union[GeoDistanceQuery, _Mapping]] = ..., geo_polygon_query: _Optional[_Union[GeoPolygonQuery, _Mapping]] = ..., match_all_query: _Optional[_Union[MatchAllQuery, _Mapping]] = ..., match_none_query: _Optional[_Union[MatchNoneQuery, _Mapping]] = ..., match_phrase_query: _Optional[_Union[MatchPhraseQuery, _Mapping]] = ..., match_query: _Optional[_Union[MatchQuery, _Mapping]] = ..., numeric_range_query: _Optional[_Union[NumericRangeQuery, _Mapping]] = ..., phrase_query: _Optional[_Union[PhraseQuery, _Mapping]] = ..., prefix_query: _Optional[_Union[PrefixQuery, _Mapping]] = ..., query_string_query: _Optional[_Union[QueryStringQuery, _Mapping]] = ..., regexp_query: _Optional[_Union[RegexpQuery, _Mapping]] = ..., term_query: _Optional[_Union[TermQuery, _Mapping]] = ..., term_range_query: _Optional[_Union[TermRangeQuery, _Mapping]] = ..., wildcard_query: _Optional[_Union[WildcardQuery, _Mapping]] = ...) -> None: ...

class FieldSorting(_message.Message):
    __slots__ = ("field", "descending", "missing", "mode", "type")
    FIELD_FIELD_NUMBER: _ClassVar[int]
    DESCENDING_FIELD_NUMBER: _ClassVar[int]
    MISSING_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    field: str
    descending: bool
    missing: str
    mode: str
    type: str
    def __init__(self, field: _Optional[str] = ..., descending: bool = ..., missing: _Optional[str] = ..., mode: _Optional[str] = ..., type: _Optional[str] = ...) -> None: ...

class GeoDistanceSorting(_message.Message):
    __slots__ = ("field", "descending", "center", "unit")
    FIELD_FIELD_NUMBER: _ClassVar[int]
    DESCENDING_FIELD_NUMBER: _ClassVar[int]
    CENTER_FIELD_NUMBER: _ClassVar[int]
    UNIT_FIELD_NUMBER: _ClassVar[int]
    field: str
    descending: bool
    center: LatLng
    unit: str
    def __init__(self, field: _Optional[str] = ..., descending: bool = ..., center: _Optional[_Union[LatLng, _Mapping]] = ..., unit: _Optional[str] = ...) -> None: ...

class IdSorting(_message.Message):
    __slots__ = ("descending",)
    DESCENDING_FIELD_NUMBER: _ClassVar[int]
    descending: bool
    def __init__(self, descending: bool = ...) -> None: ...

class ScoreSorting(_message.Message):
    __slots__ = ("descending",)
    DESCENDING_FIELD_NUMBER: _ClassVar[int]
    descending: bool
    def __init__(self, descending: bool = ...) -> None: ...

class Sorting(_message.Message):
    __slots__ = ("field_sorting", "geo_distance_sorting", "id_sorting", "score_sorting")
    FIELD_SORTING_FIELD_NUMBER: _ClassVar[int]
    GEO_DISTANCE_SORTING_FIELD_NUMBER: _ClassVar[int]
    ID_SORTING_FIELD_NUMBER: _ClassVar[int]
    SCORE_SORTING_FIELD_NUMBER: _ClassVar[int]
    field_sorting: FieldSorting
    geo_distance_sorting: GeoDistanceSorting
    id_sorting: IdSorting
    score_sorting: ScoreSorting
    def __init__(self, field_sorting: _Optional[_Union[FieldSorting, _Mapping]] = ..., geo_distance_sorting: _Optional[_Union[GeoDistanceSorting, _Mapping]] = ..., id_sorting: _Optional[_Union[IdSorting, _Mapping]] = ..., score_sorting: _Optional[_Union[ScoreSorting, _Mapping]] = ...) -> None: ...

class DateRange(_message.Message):
    __slots__ = ("name", "start", "end")
    NAME_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    name: str
    start: str
    end: str
    def __init__(self, name: _Optional[str] = ..., start: _Optional[str] = ..., end: _Optional[str] = ...) -> None: ...

class DateRangeFacet(_message.Message):
    __slots__ = ("field", "size", "date_ranges")
    FIELD_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    DATE_RANGES_FIELD_NUMBER: _ClassVar[int]
    field: str
    size: int
    date_ranges: _containers.RepeatedCompositeFieldContainer[DateRange]
    def __init__(self, field: _Optional[str] = ..., size: _Optional[int] = ..., date_ranges: _Optional[_Iterable[_Union[DateRange, _Mapping]]] = ...) -> None: ...

class NumericRange(_message.Message):
    __slots__ = ("name", "min", "max")
    NAME_FIELD_NUMBER: _ClassVar[int]
    MIN_FIELD_NUMBER: _ClassVar[int]
    MAX_FIELD_NUMBER: _ClassVar[int]
    name: str
    min: float
    max: float
    def __init__(self, name: _Optional[str] = ..., min: _Optional[float] = ..., max: _Optional[float] = ...) -> None: ...

class NumericRangeFacet(_message.Message):
    __slots__ = ("field", "size", "numeric_ranges")
    FIELD_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    NUMERIC_RANGES_FIELD_NUMBER: _ClassVar[int]
    field: str
    size: int
    numeric_ranges: _containers.RepeatedCompositeFieldContainer[NumericRange]
    def __init__(self, field: _Optional[str] = ..., size: _Optional[int] = ..., numeric_ranges: _Optional[_Iterable[_Union[NumericRange, _Mapping]]] = ...) -> None: ...

class TermFacet(_message.Message):
    __slots__ = ("field", "size")
    FIELD_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    field: str
    size: int
    def __init__(self, field: _Optional[str] = ..., size: _Optional[int] = ...) -> None: ...

class Facet(_message.Message):
    __slots__ = ("date_range_facet", "numeric_range_facet", "term_facet")
    DATE_RANGE_FACET_FIELD_NUMBER: _ClassVar[int]
    NUMERIC_RANGE_FACET_FIELD_NUMBER: _ClassVar[int]
    TERM_FACET_FIELD_NUMBER: _ClassVar[int]
    date_range_facet: DateRangeFacet
    numeric_range_facet: NumericRangeFacet
    term_facet: TermFacet
    def __init__(self, date_range_facet: _Optional[_Union[DateRangeFacet, _Mapping]] = ..., numeric_range_facet: _Optional[_Union[NumericRangeFacet, _Mapping]] = ..., term_facet: _Optional[_Union[TermFacet, _Mapping]] = ...) -> None: ...

class KnnQuery(_message.Message):
    __slots__ = ("field", "k", "vector", "boost")
    FIELD_FIELD_NUMBER: _ClassVar[int]
    K_FIELD_NUMBER: _ClassVar[int]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    BOOST_FIELD_NUMBER: _ClassVar[int]
    field: str
    k: int
    vector: _containers.RepeatedScalarFieldContainer[float]
    boost: float
    def __init__(self, field: _Optional[str] = ..., k: _Optional[int] = ..., vector: _Optional[_Iterable[float]] = ..., boost: _Optional[float] = ...) -> None: ...

class SearchQueryRequest(_message.Message):
    __slots__ = ("index_name", "query", "scan_consistency", "limit", "skip", "include_explanation", "highlight_style", "highlight_fields", "fields", "sort", "disable_scoring", "collections", "include_locations", "facets", "knn", "knn_operator", "bucket_name", "scope_name")
    class ScanConsistency(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SCAN_CONSISTENCY_NOT_BOUNDED: _ClassVar[SearchQueryRequest.ScanConsistency]
    SCAN_CONSISTENCY_NOT_BOUNDED: SearchQueryRequest.ScanConsistency
    class HighlightStyle(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        HIGHLIGHT_STYLE_DEFAULT: _ClassVar[SearchQueryRequest.HighlightStyle]
        HIGHLIGHT_STYLE_HTML: _ClassVar[SearchQueryRequest.HighlightStyle]
        HIGHLIGHT_STYLE_ANSI: _ClassVar[SearchQueryRequest.HighlightStyle]
    HIGHLIGHT_STYLE_DEFAULT: SearchQueryRequest.HighlightStyle
    HIGHLIGHT_STYLE_HTML: SearchQueryRequest.HighlightStyle
    HIGHLIGHT_STYLE_ANSI: SearchQueryRequest.HighlightStyle
    class FacetsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Facet
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Facet, _Mapping]] = ...) -> None: ...
    INDEX_NAME_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    SCAN_CONSISTENCY_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    SKIP_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_EXPLANATION_FIELD_NUMBER: _ClassVar[int]
    HIGHLIGHT_STYLE_FIELD_NUMBER: _ClassVar[int]
    HIGHLIGHT_FIELDS_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    SORT_FIELD_NUMBER: _ClassVar[int]
    DISABLE_SCORING_FIELD_NUMBER: _ClassVar[int]
    COLLECTIONS_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_LOCATIONS_FIELD_NUMBER: _ClassVar[int]
    FACETS_FIELD_NUMBER: _ClassVar[int]
    KNN_FIELD_NUMBER: _ClassVar[int]
    KNN_OPERATOR_FIELD_NUMBER: _ClassVar[int]
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    index_name: str
    query: Query
    scan_consistency: SearchQueryRequest.ScanConsistency
    limit: int
    skip: int
    include_explanation: bool
    highlight_style: SearchQueryRequest.HighlightStyle
    highlight_fields: _containers.RepeatedScalarFieldContainer[str]
    fields: _containers.RepeatedScalarFieldContainer[str]
    sort: _containers.RepeatedCompositeFieldContainer[Sorting]
    disable_scoring: bool
    collections: _containers.RepeatedScalarFieldContainer[str]
    include_locations: bool
    facets: _containers.MessageMap[str, Facet]
    knn: _containers.RepeatedCompositeFieldContainer[KnnQuery]
    knn_operator: KnnOperator
    bucket_name: str
    scope_name: str
    def __init__(self, index_name: _Optional[str] = ..., query: _Optional[_Union[Query, _Mapping]] = ..., scan_consistency: _Optional[_Union[SearchQueryRequest.ScanConsistency, str]] = ..., limit: _Optional[int] = ..., skip: _Optional[int] = ..., include_explanation: bool = ..., highlight_style: _Optional[_Union[SearchQueryRequest.HighlightStyle, str]] = ..., highlight_fields: _Optional[_Iterable[str]] = ..., fields: _Optional[_Iterable[str]] = ..., sort: _Optional[_Iterable[_Union[Sorting, _Mapping]]] = ..., disable_scoring: bool = ..., collections: _Optional[_Iterable[str]] = ..., include_locations: bool = ..., facets: _Optional[_Mapping[str, Facet]] = ..., knn: _Optional[_Iterable[_Union[KnnQuery, _Mapping]]] = ..., knn_operator: _Optional[_Union[KnnOperator, str]] = ..., bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ...) -> None: ...

class SearchQueryResponse(_message.Message):
    __slots__ = ("hits", "facets", "meta_data")
    class SearchQueryRow(_message.Message):
        __slots__ = ("id", "score", "index", "explanation", "locations", "fragments", "fields")
        class FragmentsEntry(_message.Message):
            __slots__ = ("key", "value")
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: SearchQueryResponse.Fragment
            def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[SearchQueryResponse.Fragment, _Mapping]] = ...) -> None: ...
        class FieldsEntry(_message.Message):
            __slots__ = ("key", "value")
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: bytes
            def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
        ID_FIELD_NUMBER: _ClassVar[int]
        SCORE_FIELD_NUMBER: _ClassVar[int]
        INDEX_FIELD_NUMBER: _ClassVar[int]
        EXPLANATION_FIELD_NUMBER: _ClassVar[int]
        LOCATIONS_FIELD_NUMBER: _ClassVar[int]
        FRAGMENTS_FIELD_NUMBER: _ClassVar[int]
        FIELDS_FIELD_NUMBER: _ClassVar[int]
        id: str
        score: float
        index: str
        explanation: bytes
        locations: _containers.RepeatedCompositeFieldContainer[SearchQueryResponse.Location]
        fragments: _containers.MessageMap[str, SearchQueryResponse.Fragment]
        fields: _containers.ScalarMap[str, bytes]
        def __init__(self, id: _Optional[str] = ..., score: _Optional[float] = ..., index: _Optional[str] = ..., explanation: _Optional[bytes] = ..., locations: _Optional[_Iterable[_Union[SearchQueryResponse.Location, _Mapping]]] = ..., fragments: _Optional[_Mapping[str, SearchQueryResponse.Fragment]] = ..., fields: _Optional[_Mapping[str, bytes]] = ...) -> None: ...
    class Location(_message.Message):
        __slots__ = ("field", "term", "position", "start", "end", "array_positions")
        FIELD_FIELD_NUMBER: _ClassVar[int]
        TERM_FIELD_NUMBER: _ClassVar[int]
        POSITION_FIELD_NUMBER: _ClassVar[int]
        START_FIELD_NUMBER: _ClassVar[int]
        END_FIELD_NUMBER: _ClassVar[int]
        ARRAY_POSITIONS_FIELD_NUMBER: _ClassVar[int]
        field: str
        term: str
        position: int
        start: int
        end: int
        array_positions: _containers.RepeatedScalarFieldContainer[int]
        def __init__(self, field: _Optional[str] = ..., term: _Optional[str] = ..., position: _Optional[int] = ..., start: _Optional[int] = ..., end: _Optional[int] = ..., array_positions: _Optional[_Iterable[int]] = ...) -> None: ...
    class Fragment(_message.Message):
        __slots__ = ("content",)
        CONTENT_FIELD_NUMBER: _ClassVar[int]
        content: _containers.RepeatedScalarFieldContainer[str]
        def __init__(self, content: _Optional[_Iterable[str]] = ...) -> None: ...
    class FacetResult(_message.Message):
        __slots__ = ("term_facet", "date_range_facet", "numeric_range_facet")
        TERM_FACET_FIELD_NUMBER: _ClassVar[int]
        DATE_RANGE_FACET_FIELD_NUMBER: _ClassVar[int]
        NUMERIC_RANGE_FACET_FIELD_NUMBER: _ClassVar[int]
        term_facet: SearchQueryResponse.TermFacetResult
        date_range_facet: SearchQueryResponse.DateRangeFacetResult
        numeric_range_facet: SearchQueryResponse.NumericRangeFacetResult
        def __init__(self, term_facet: _Optional[_Union[SearchQueryResponse.TermFacetResult, _Mapping]] = ..., date_range_facet: _Optional[_Union[SearchQueryResponse.DateRangeFacetResult, _Mapping]] = ..., numeric_range_facet: _Optional[_Union[SearchQueryResponse.NumericRangeFacetResult, _Mapping]] = ...) -> None: ...
    class TermResult(_message.Message):
        __slots__ = ("name", "field", "size")
        NAME_FIELD_NUMBER: _ClassVar[int]
        FIELD_FIELD_NUMBER: _ClassVar[int]
        SIZE_FIELD_NUMBER: _ClassVar[int]
        name: str
        field: str
        size: int
        def __init__(self, name: _Optional[str] = ..., field: _Optional[str] = ..., size: _Optional[int] = ...) -> None: ...
    class TermFacetResult(_message.Message):
        __slots__ = ("field", "total", "missing", "other", "terms")
        FIELD_FIELD_NUMBER: _ClassVar[int]
        TOTAL_FIELD_NUMBER: _ClassVar[int]
        MISSING_FIELD_NUMBER: _ClassVar[int]
        OTHER_FIELD_NUMBER: _ClassVar[int]
        TERMS_FIELD_NUMBER: _ClassVar[int]
        field: str
        total: int
        missing: int
        other: int
        terms: _containers.RepeatedCompositeFieldContainer[SearchQueryResponse.TermResult]
        def __init__(self, field: _Optional[str] = ..., total: _Optional[int] = ..., missing: _Optional[int] = ..., other: _Optional[int] = ..., terms: _Optional[_Iterable[_Union[SearchQueryResponse.TermResult, _Mapping]]] = ...) -> None: ...
    class DateRangeResult(_message.Message):
        __slots__ = ("name", "size", "start", "end")
        NAME_FIELD_NUMBER: _ClassVar[int]
        SIZE_FIELD_NUMBER: _ClassVar[int]
        START_FIELD_NUMBER: _ClassVar[int]
        END_FIELD_NUMBER: _ClassVar[int]
        name: str
        size: int
        start: _timestamp_pb2.Timestamp
        end: _timestamp_pb2.Timestamp
        def __init__(self, name: _Optional[str] = ..., size: _Optional[int] = ..., start: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., end: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
    class DateRangeFacetResult(_message.Message):
        __slots__ = ("field", "total", "missing", "other", "date_ranges")
        FIELD_FIELD_NUMBER: _ClassVar[int]
        TOTAL_FIELD_NUMBER: _ClassVar[int]
        MISSING_FIELD_NUMBER: _ClassVar[int]
        OTHER_FIELD_NUMBER: _ClassVar[int]
        DATE_RANGES_FIELD_NUMBER: _ClassVar[int]
        field: str
        total: int
        missing: int
        other: int
        date_ranges: _containers.RepeatedCompositeFieldContainer[SearchQueryResponse.DateRangeResult]
        def __init__(self, field: _Optional[str] = ..., total: _Optional[int] = ..., missing: _Optional[int] = ..., other: _Optional[int] = ..., date_ranges: _Optional[_Iterable[_Union[SearchQueryResponse.DateRangeResult, _Mapping]]] = ...) -> None: ...
    class NumericRangeResult(_message.Message):
        __slots__ = ("name", "size", "min", "max")
        NAME_FIELD_NUMBER: _ClassVar[int]
        SIZE_FIELD_NUMBER: _ClassVar[int]
        MIN_FIELD_NUMBER: _ClassVar[int]
        MAX_FIELD_NUMBER: _ClassVar[int]
        name: str
        size: int
        min: int
        max: int
        def __init__(self, name: _Optional[str] = ..., size: _Optional[int] = ..., min: _Optional[int] = ..., max: _Optional[int] = ...) -> None: ...
    class NumericRangeFacetResult(_message.Message):
        __slots__ = ("field", "total", "missing", "other", "numeric_ranges")
        FIELD_FIELD_NUMBER: _ClassVar[int]
        TOTAL_FIELD_NUMBER: _ClassVar[int]
        MISSING_FIELD_NUMBER: _ClassVar[int]
        OTHER_FIELD_NUMBER: _ClassVar[int]
        NUMERIC_RANGES_FIELD_NUMBER: _ClassVar[int]
        field: str
        total: int
        missing: int
        other: int
        numeric_ranges: _containers.RepeatedCompositeFieldContainer[SearchQueryResponse.NumericRangeResult]
        def __init__(self, field: _Optional[str] = ..., total: _Optional[int] = ..., missing: _Optional[int] = ..., other: _Optional[int] = ..., numeric_ranges: _Optional[_Iterable[_Union[SearchQueryResponse.NumericRangeResult, _Mapping]]] = ...) -> None: ...
    class MetaData(_message.Message):
        __slots__ = ("metrics", "errors")
        class ErrorsEntry(_message.Message):
            __slots__ = ("key", "value")
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: str
            def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
        METRICS_FIELD_NUMBER: _ClassVar[int]
        ERRORS_FIELD_NUMBER: _ClassVar[int]
        metrics: SearchQueryResponse.SearchMetrics
        errors: _containers.ScalarMap[str, str]
        def __init__(self, metrics: _Optional[_Union[SearchQueryResponse.SearchMetrics, _Mapping]] = ..., errors: _Optional[_Mapping[str, str]] = ...) -> None: ...
    class SearchMetrics(_message.Message):
        __slots__ = ("execution_time", "total_rows", "max_score", "total_partition_count", "success_partition_count", "error_partition_count")
        EXECUTION_TIME_FIELD_NUMBER: _ClassVar[int]
        TOTAL_ROWS_FIELD_NUMBER: _ClassVar[int]
        MAX_SCORE_FIELD_NUMBER: _ClassVar[int]
        TOTAL_PARTITION_COUNT_FIELD_NUMBER: _ClassVar[int]
        SUCCESS_PARTITION_COUNT_FIELD_NUMBER: _ClassVar[int]
        ERROR_PARTITION_COUNT_FIELD_NUMBER: _ClassVar[int]
        execution_time: _duration_pb2.Duration
        total_rows: int
        max_score: float
        total_partition_count: int
        success_partition_count: int
        error_partition_count: int
        def __init__(self, execution_time: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., total_rows: _Optional[int] = ..., max_score: _Optional[float] = ..., total_partition_count: _Optional[int] = ..., success_partition_count: _Optional[int] = ..., error_partition_count: _Optional[int] = ...) -> None: ...
    class FacetsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: SearchQueryResponse.FacetResult
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[SearchQueryResponse.FacetResult, _Mapping]] = ...) -> None: ...
    HITS_FIELD_NUMBER: _ClassVar[int]
    FACETS_FIELD_NUMBER: _ClassVar[int]
    META_DATA_FIELD_NUMBER: _ClassVar[int]
    hits: _containers.RepeatedCompositeFieldContainer[SearchQueryResponse.SearchQueryRow]
    facets: _containers.MessageMap[str, SearchQueryResponse.FacetResult]
    meta_data: SearchQueryResponse.MetaData
    def __init__(self, hits: _Optional[_Iterable[_Union[SearchQueryResponse.SearchQueryRow, _Mapping]]] = ..., facets: _Optional[_Mapping[str, SearchQueryResponse.FacetResult]] = ..., meta_data: _Optional[_Union[SearchQueryResponse.MetaData, _Mapping]] = ...) -> None: ...
