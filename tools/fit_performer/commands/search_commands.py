from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterator
from datetime import date, timedelta
from functools import wraps
from time import perf_counter_ns
from typing import (TYPE_CHECKING,
                    Any,
                    Concatenate,
                    Literal,
                    ParamSpec,
                    overload)

from google.protobuf import timestamp_pb2 as timestamp_pb
from google.protobuf.message import Message

from couchbase.cluster import Cluster
from couchbase.exceptions import CouchbaseException
from couchbase.options import SearchOptions, VectorSearchOptions
from couchbase.scope import Scope
from couchbase.search import (BooleanFieldQuery,
                              BooleanQuery,
                              ConjunctionQuery,
                              DateFacet,
                              DateRangeQuery,
                              DisjunctionQuery,
                              DocIdQuery,
                              GeoBoundingBoxQuery,
                              GeoDistanceQuery,
                              HighlightStyle,
                              MatchAllQuery,
                              MatchNoneQuery,
                              MatchOperator,
                              MatchPhraseQuery,
                              MatchQuery,
                              NumericFacet,
                              NumericRangeQuery,
                              PhraseQuery,
                              PrefixQuery,
                              QueryStringQuery,
                              RegexQuery,
                              SearchQuery,
                              SearchRequest,
                              SearchScanConsistency,
                              Sort,
                              SortField,
                              SortGeoDistance,
                              SortID,
                              SortScore,
                              TermFacet,
                              TermQuery,
                              TermRangeQuery,
                              WildcardQuery)
from couchbase.search_scoring import (ReciprocalRankFusion,
                                      RelativeScoreFusion,
                                      ScoringNone)
from couchbase.vector_search import (VectorQuery,
                                     VectorQueryCombination,
                                     VectorSearch)

from run import top_level_pb2 as run_pb
from sdk import search_pb2 as search_pb
from sdk import workload_pb2 as sdk_pb
from shared import content_pb2 as content_pb
from shared import exceptions_pb2 as exceptions_pb
from streams import top_level_pb2 as streams_pb

if TYPE_CHECKING:
    from couchbase.result import SearchResult
    from couchbase.search import Facet, SearchFacetResult, SearchMetaData, SearchRow

    from ..telemetry.span_owner import SpanOwner

from .sdk_commands import (SdkCommand,
                           SdkCommandOptions,
                           SdkCommandResult,
                           validate_command)

P = ParamSpec('P')

VALID_SEARCH_COMMAND_ARGS: dict[str, Callable[[Any], bool]] = {
    'cluster': lambda c: c is None or isinstance(c, Cluster),
    'scope': lambda s: s is None or isinstance(s, Scope),
    'index_name': lambda i: isinstance(i, str),
    'stream_config': lambda sc: sc is not None,
    'return_result': lambda rr: isinstance(rr, bool),
    'initiated': lambda i: isinstance(i, timestamp_pb.Timestamp),
    'options': lambda o: True,
    'query': lambda q: q is not None,
    'request': lambda r: r is not None,
    'fields_as': lambda f: f is None or isinstance(f, content_pb.ContentAs),
    'span_owner': lambda s: True
}


class SearchCommandOptions(SdkCommandOptions):
    _VALID_HIGHLIGHT_STYLES = {
        search_pb.HighlightStyle.HIGHLIGHT_STYLE_HTML: HighlightStyle.Html,
        search_pb.HighlightStyle.HIGHLIGHT_STYLE_ANSI: HighlightStyle.Ansi,
    }

    _VALID_SCAN_CONSISTENCIES = {
        search_pb.SearchScanConsistency.SEARCH_SCAN_CONSISTENCY_NOT_BOUNDED: SearchScanConsistency.NOT_BOUNDED
    }

    _VALID_GEO_DISTANCE_UNITS = {
        search_pb.SearchGeoDistanceUnits.SEARCH_GEO_DISTANCE_UNITS_METERS: 'meters',
        search_pb.SearchGeoDistanceUnits.SEARCH_GEO_DISTANCE_UNITS_MILES: 'miles',
        search_pb.SearchGeoDistanceUnits.SEARCH_GEO_DISTANCE_UNITS_CENTIMETERS: 'centimeters',
        search_pb.SearchGeoDistanceUnits.SEARCH_GEO_DISTANCE_UNITS_MILLIMETERS: 'millimeters',
        search_pb.SearchGeoDistanceUnits.SEARCH_GEO_DISTANCE_UNITS_NAUTICAL_MILES: 'nauticalmiles',
        search_pb.SearchGeoDistanceUnits.SEARCH_GEO_DISTANCE_UNITS_KILOMETERS: 'kilometers',
        search_pb.SearchGeoDistanceUnits.SEARCH_GEO_DISTANCE_UNITS_FEET: 'feet',
        search_pb.SearchGeoDistanceUnits.SEARCH_GEO_DISTANCE_UNITS_YARDS: 'yards',
        search_pb.SearchGeoDistanceUnits.SEARCH_GEO_DISTANCE_UNITS_INCHES: 'inch',
    }

    _VALID_VECTOR_QUERY_COMBINATION = {
        search_pb.VectorQueryCombination.AND: VectorQueryCombination.AND,
        search_pb.VectorQueryCombination.OR: VectorQueryCombination.OR,
    }

    @staticmethod
    def get_timeout(options: Any) -> timedelta | None:
        if not options.HasField("timeout_millis"):
            return None

        return timedelta(milliseconds=options.timeout_millis)

    @staticmethod
    def get_limit(options: search_pb.SearchOptions) -> int | None:
        return SearchCommandOptions.get_simple_option(options, 'limit')

    @staticmethod
    def get_skip(options: search_pb.SearchOptions) -> int | None:
        return SearchCommandOptions.get_simple_option(options, 'skip')

    @staticmethod
    def get_explain(options: search_pb.SearchOptions) -> bool | None:
        return SearchCommandOptions.get_simple_option(options, 'explain')

    @staticmethod
    def get_highlight_style(options: search_pb.SearchOptions) -> HighlightStyle | None:
        if not (options.HasField('highlight') and options.highlight.HasField('style')):
            return None

        return SearchCommandOptions._VALID_HIGHLIGHT_STYLES[options.highlight.style]

    @staticmethod
    def get_highlight_fields(options: search_pb.SearchOptions) -> list[str] | None:
        if (not options.HasField('highlight')) or len(options.highlight.fields) == 0:
            return None

        return list(options.highlight.fields)

    @staticmethod
    def get_fields(options: search_pb.SearchOptions) -> list[str] | None:
        if len(options.fields) == 0:
            return None

        return list(options.fields)

    @staticmethod
    def get_scan_consistency(options: search_pb.SearchOptions) -> SearchScanConsistency | None:
        if not options.HasField('scan_consistency'):
            return None

        return SearchCommandOptions._VALID_SCAN_CONSISTENCIES[options.scan_consistency]

    @staticmethod
    def get_sort(options: search_pb.SearchOptions) -> list[Sort | str] | None:  # noqa: C901
        if len(options.sort) == 0:
            return None

        res: list[Sort | str] = []
        for proto_sort in options.sort:
            sort_type = proto_sort.WhichOneof('sort')

            if sort_type == 'score':
                score_sort = SortScore()
                if proto_sort.score.HasField('desc'):
                    score_sort.desc = proto_sort.score.desc
                res.append(score_sort)

            elif sort_type == 'id':
                id_sort = SortID()
                if proto_sort.id.HasField('desc'):
                    id_sort.desc = proto_sort.id.desc
                res.append(id_sort)

            elif sort_type == 'field':
                field_sort = SortField(proto_sort.field.field)
                if proto_sort.field.HasField('desc'):
                    field_sort.desc = proto_sort.field.desc
                if proto_sort.field.HasField('type'):
                    field_sort.type = proto_sort.field.type
                if proto_sort.field.HasField('mode'):
                    field_sort.mode = proto_sort.field.mode
                if proto_sort.field.HasField('missing'):
                    field_sort.missing = proto_sort.field.missing
                res.append(field_sort)

            elif sort_type == 'geo_distance':
                geo_sort = SortGeoDistance(
                    (proto_sort.geo_distance.location.lon, proto_sort.geo_distance.location.lat),
                    proto_sort.geo_distance.field
                )
                if proto_sort.geo_distance.HasField('desc'):
                    geo_sort.desc = proto_sort.geo_distance.desc
                if proto_sort.geo_distance.HasField('unit'):
                    geo_sort.unit = SearchCommandOptions._VALID_GEO_DISTANCE_UNITS[proto_sort.geo_distance.unit]
                res.append(geo_sort)

            elif sort_type == 'raw':
                res.append(proto_sort.raw)
        return res

    @staticmethod
    def get_facets(options: search_pb.SearchOptions) -> dict[str, Facet] | None:
        if len(options.facets) == 0:
            return None

        res: dict[str, Facet] = {}
        for key, facet in options.facets.items():
            facet_type = facet.WhichOneof('facet')

            if facet_type == 'term':
                res[key] = TermFacet(
                    field=facet.term.field,
                    limit=(facet.term.size if facet.term.HasField('size') else None)
                )

            elif facet_type == 'numeric_range':
                num_facet = NumericFacet(
                    field=facet.numeric_range.field,
                    limit=(facet.numeric_range.size if facet.numeric_range.HasField('size') else None)
                )

                for proto_num_range in facet.numeric_range.numeric_ranges:
                    num_facet.add_range(
                        name=proto_num_range.name,
                        min=(proto_num_range.min if proto_num_range.HasField('min') else None),
                        max=(proto_num_range.max if proto_num_range.HasField('max') else None)
                    )

                res[key] = num_facet

            elif facet_type == 'date_range':
                date_facet = DateFacet(
                    field=facet.date_range.field,
                    limit=(facet.date_range.size if facet.date_range.HasField('size') else None)
                )

                for proto_date_range in facet.date_range.date_ranges:
                    start = None
                    if proto_date_range.HasField('start'):
                        start = str(date.fromtimestamp(proto_date_range.start.seconds))

                    end = None
                    if proto_date_range.HasField('end'):
                        end = str(date.fromtimestamp(proto_date_range.end.seconds))

                    date_facet.add_range(
                        name=proto_date_range.name,
                        start=start,
                        end=end
                    )

                res[key] = date_facet

        return res

    @staticmethod
    def get_raw(options: search_pb.SearchOptions) -> dict[str, str] | None:
        if len(options.raw) == 0:
            return None

        return dict(options.raw)

    @staticmethod
    def get_include_locations(options: search_pb.SearchOptions) -> bool | None:
        return SearchCommandOptions.get_simple_option(options, 'include_locations')

    @staticmethod
    def get_disable_scoring(options: search_pb.SearchOptions) -> bool | None:
        return SearchCommandOptions.get_simple_option(options, 'disable_scoring')

    @staticmethod
    def get_scoring(options: search_pb.SearchOptions
                    ) -> ReciprocalRankFusion | RelativeScoreFusion | ScoringNone | None:
        if not options.HasField('scoring'):
            return None

        mode = options.scoring.WhichOneof('mode')
        if mode == 'reciprocal_rank_fusion':
            rrf = options.scoring.reciprocal_rank_fusion
            return ReciprocalRankFusion(
                rank_constant=rrf.rank_constant if rrf.HasField('rank_constant') else None,
                window_size=rrf.window_size if rrf.HasField('window_size') else None
            )
        elif mode == 'relative_score_fusion':
            rsf = options.scoring.relative_score_fusion
            return RelativeScoreFusion(
                window_size=rsf.window_size if rsf.HasField('window_size') else None
            )
        elif mode == 'none':
            return ScoringNone()
        else:
            raise NotImplementedError(f"Search scoring mode '{mode}' not supported")

    @staticmethod
    def get_vector_query_combination(options: search_pb.VectorSearchOptions) -> VectorQueryCombination | None:
        if not options.HasField('vector_query_combination'):
            return None

        return SearchCommandOptions._VALID_VECTOR_QUERY_COMBINATION[options.vector_query_combination]


class SearchCommandResult(SdkCommandResult):
    @classmethod
    def as_blocking_search_result(cls,
                                  fn: Callable[Concatenate[SearchQueryCommand, P], SearchResult],
                                  ) -> Callable[Concatenate[SearchQueryCommand, P], run_pb.Result]:
        @wraps(fn)
        def wrapped_fn(self: SearchQueryCommand, *args: P.args, **kwargs: P.kwargs) -> run_pb.Result:
            try:
                start = perf_counter_ns()
                result = fn(self, *args, **kwargs)
                end = perf_counter_ns()
            except Exception as e:
                return cls.exception_as_result(e)

            return cls.to_blocking_search_result(result, self._initiated, (end - start), self._fields_as)

        return wrapped_fn

    @classmethod
    def as_search_result_stream(cls,
                                fn: Callable[Concatenate[SearchQueryCommand, P], SearchResult],
                                ) -> Callable[Concatenate[SearchQueryCommand, P],
                                              Iterator[run_pb.Result | exceptions_pb.Exception]]:
        @wraps(fn)
        def wrapped_fn(self: SearchQueryCommand,
                       *args: P.args,
                       **kwargs: P.kwargs,
                       ) -> Iterator[run_pb.Result | exceptions_pb.Exception]:
            try:
                result = fn(self, *args, **kwargs)
            except Exception as e:
                exception = cls.to_exception(e)
                return iter((exception,))

            return cls.to_search_result_iterator(
                result, self._initiated, self._stream_config.stream_id, self._fields_as
            )

        return wrapped_fn

    @classmethod
    def to_blocking_search_result(cls,
                                  result: SearchResult,
                                  initiated: timestamp_pb.Timestamp,
                                  elapsed_nanos: int,
                                  fields_as: content_pb.ContentAs | None = None,
                                  ) -> run_pb.Result:
        logger = logging.getLogger(__name__)
        rows = [cls.to_search_row_result(r, initiated, elapsed_nanos, fields_as=fields_as, return_as_search_row=True)
                for r in result.rows()]
        blocking_result: dict[str, Any] = {
            'rows': rows,
        }
        facets = result.facets()
        if facets is not None:
            blocking_result['facets'] = cls.to_search_facets_result(facets,
                                                                    initiated,
                                                                    return_as_search_facets=True)
        blocking_result['meta_data'] = cls.to_search_meta_data_result(cls._require_metadata(result),
                                                                      initiated,
                                                                      return_as_search_meta_data=True)

        search_blocking_result = search_pb.BlockingSearchResult(**blocking_result)
        logger.info(f'Got {len(rows)} results from search query')
        logger.info(f'Blocking search result: {search_blocking_result}')

        return run_pb.Result(
            sdk=sdk_pb.Result(search_blocking_result=search_blocking_result),
            elapsedNanos=elapsed_nanos,
            initiated=initiated
        )

    @classmethod
    def to_search_result_iterator(cls,
                                  result: SearchResult,
                                  initiated: timestamp_pb.Timestamp,
                                  stream_id: str,
                                  fields_as: content_pb.ContentAs | None = None,
                                  ) -> Iterator[run_pb.Result | exceptions_pb.Exception]:
        logger = logging.getLogger(__name__)
        iterator = result.rows()

        cnt = 0
        while True:
            try:
                start = perf_counter_ns()
                row = next(iterator)
                end = perf_counter_ns()
                cnt += 1
            except StopIteration:
                logger.info(f"Got {cnt} results from search query")
                facets = result.facets()
                if facets is not None:
                    yield cls.to_search_facets_result(facets, initiated, stream_id)
                yield cls.to_search_meta_data_result(cls._require_metadata(result), initiated, stream_id)
                return
            except Exception as e:
                if not isinstance(e, CouchbaseException):
                    logger.warning(f"Caught {type(e).__name__} exception ({str(e)})")
                exception = cls.to_exception(e)
                yield exception
                return

            yield cls.to_search_row_result(row, initiated, (end - start), stream_id, fields_as)

    @staticmethod
    def _require_metadata(result: SearchResult) -> SearchMetaData:
        metadata = result.metadata()
        if metadata is None:
            raise RuntimeError('The search result has no metadata once its rows are read.')
        return metadata

    @staticmethod
    def _require_stream_id(stream_id: str | None) -> str:
        if stream_id is None:
            raise ValueError('A streamed search result needs a stream ID.')
        return stream_id

    @overload
    @classmethod
    def to_search_row_result(cls,
                             search_row: SearchRow,
                             initiated: timestamp_pb.Timestamp,
                             elapsed_nanos: int,
                             stream_id: str | None = None,
                             fields_as: content_pb.ContentAs | None = None,
                             return_as_search_row: Literal[False] = False
                             ) -> run_pb.Result: ...

    @overload
    @classmethod
    def to_search_row_result(cls,
                             search_row: SearchRow,
                             initiated: timestamp_pb.Timestamp,
                             elapsed_nanos: int,
                             stream_id: str | None = None,
                             fields_as: content_pb.ContentAs | None = None,
                             *,
                             return_as_search_row: Literal[True]
                             ) -> search_pb.SearchRow: ...

    @classmethod
    def to_search_row_result(cls,
                             search_row: SearchRow,
                             initiated: timestamp_pb.Timestamp,
                             elapsed_nanos: int,
                             stream_id: str | None = None,
                             fields_as: content_pb.ContentAs | None = None,
                             return_as_search_row: bool = False
                             ) -> run_pb.Result | search_pb.SearchRow:
        locations = []
        if search_row.locations is not None:
            for loc in search_row.locations.get_all():
                locations.append(search_pb.SearchRowLocation(
                    field=loc.field or '',
                    term=loc.term or '',
                    position=loc.position or 0,
                    start=loc.start or 0,
                    end=loc.end or 0,
                    array_positions=[] if loc.array_positions is None else list(loc.array_positions)
                ))

        kwargs: dict[str, Any] = {
            'index': search_row.index,
            'id': search_row.id,
            'score': search_row.score,
            'explanation': json.dumps(search_row.explanation).encode('utf-8'),
            'locations': locations
        }

        if fields_as is not None:
            kwargs['fields'] = cls.to_content(search_row.fields, fields_as)

        proto_row = search_pb.SearchRow(**kwargs)
        if return_as_search_row is True:
            return proto_row
        search_streaming_result = search_pb.StreamingSearchResult(stream_id=cls._require_stream_id(stream_id),
                                                                  row=proto_row)

        return run_pb.Result(
            sdk=sdk_pb.Result(search_streaming_result=search_streaming_result),
            elapsedNanos=elapsed_nanos,
            initiated=initiated
        )

    @overload
    @classmethod
    def to_search_facets_result(cls,
                                search_facets: dict[str, SearchFacetResult],
                                initiated: timestamp_pb.Timestamp,
                                stream_id: str | None = None,
                                return_as_search_facets: Literal[False] = False,
                                ) -> run_pb.Result: ...

    @overload
    @classmethod
    def to_search_facets_result(cls,
                                search_facets: dict[str, SearchFacetResult],
                                initiated: timestamp_pb.Timestamp,
                                stream_id: str | None = None,
                                *,
                                return_as_search_facets: Literal[True],
                                ) -> search_pb.SearchFacets: ...

    @classmethod
    def to_search_facets_result(cls,
                                search_facets: dict[str, SearchFacetResult],
                                initiated: timestamp_pb.Timestamp,
                                stream_id: str | None = None,
                                return_as_search_facets: bool = False,
                                ) -> run_pb.Result | search_pb.SearchFacets:
        proto_facets = search_pb.SearchFacets()
        for key, facet_result in search_facets.items():
            proto_facets.facets[key].name = facet_result.name
            proto_facets.facets[key].field = facet_result.field
            proto_facets.facets[key].total = facet_result.total
            proto_facets.facets[key].missing = facet_result.missing
            proto_facets.facets[key].other = facet_result.other

        if return_as_search_facets is True:
            return proto_facets

        search_streaming_result = search_pb.StreamingSearchResult(stream_id=cls._require_stream_id(stream_id),
                                                                  facets=proto_facets)

        return run_pb.Result(
            sdk=sdk_pb.Result(search_streaming_result=search_streaming_result),
            initiated=initiated
        )

    @overload
    @classmethod
    def to_search_meta_data_result(cls,
                                   search_meta_data: SearchMetaData,
                                   initiated: timestamp_pb.Timestamp,
                                   stream_id: str | None = None,
                                   return_as_search_meta_data: Literal[False] = False,
                                   ) -> run_pb.Result: ...

    @overload
    @classmethod
    def to_search_meta_data_result(cls,
                                   search_meta_data: SearchMetaData,
                                   initiated: timestamp_pb.Timestamp,
                                   stream_id: str | None = None,
                                   *,
                                   return_as_search_meta_data: Literal[True],
                                   ) -> search_pb.SearchMetaData: ...

    @classmethod
    def to_search_meta_data_result(cls,
                                   search_meta_data: SearchMetaData,
                                   initiated: timestamp_pb.Timestamp,
                                   stream_id: str | None = None,
                                   return_as_search_meta_data: bool = False,
                                   ) -> run_pb.Result | search_pb.SearchMetaData:

        metrics = search_meta_data.metrics()
        proto_metrics = None
        if metrics is not None:
            proto_metrics = search_pb.SearchMetrics(
                took_msec=metrics.took().microseconds,
                total_rows=metrics.total_rows(),
                max_score=metrics.max_score(),
                total_partition_count=metrics.total_partition_count(),
                success_partition_count=metrics.success_partition_count(),
                error_partition_count=metrics.error_partition_count()
            )

        proto_meta_data = search_pb.SearchMetaData(metrics=proto_metrics)
        proto_meta_data.errors.update(search_meta_data.errors())

        if return_as_search_meta_data is True:
            return proto_meta_data

        search_streaming_result = search_pb.StreamingSearchResult(stream_id=cls._require_stream_id(stream_id),
                                                                  meta_data=proto_meta_data)

        return run_pb.Result(
            sdk=sdk_pb.Result(search_streaming_result=search_streaming_result),
            initiated=initiated
        )


class SearchQueryCommand(SdkCommand):
    def __init__(self, **kwargs: Any) -> None:
        validate_command(VALID_SEARCH_COMMAND_ARGS, **kwargs)
        self._cluster: Cluster | None = kwargs.get('cluster')
        self._scope: Scope | None = kwargs.get('scope')
        self._index_name: str = kwargs['index_name']
        self._stream_config: streams_pb.Config = kwargs['stream_config']
        self._initiated = kwargs['initiated']
        self._return_result: bool | None = kwargs.get('return_result')
        self._raw_options: search_pb.SearchOptions | None = kwargs.get('options')
        self._raw_query: search_pb.SearchQuery | None = kwargs.get('query')
        self._fields_as: content_pb.ContentAs | None = kwargs.get('fields_as')
        self._query: SearchQuery | None = None
        self._options: SearchOptions | None = None
        self._span_owner: SpanOwner | None = kwargs.get('span_owner')
        self._raw_request: search_pb.SearchRequest | None = kwargs.get('request')
        self._search_request: SearchRequest | None = None

    @property
    def stream_type(self) -> streams_pb.Type.ValueType:
        return streams_pb.Type.STREAM_FULL_TEXT_SEARCH

    @property
    def stream_config(self) -> streams_pb.Config:
        return self._stream_config

    def set_options(self) -> None:
        if self._raw_options is None:
            return

        opt_kwargs: dict[str, Any] = {
            'timeout': SearchCommandOptions.get_timeout(self._raw_options),
            'limit': SearchCommandOptions.get_limit(self._raw_options),
            'skip': SearchCommandOptions.get_skip(self._raw_options),
            'explain': SearchCommandOptions.get_explain(self._raw_options),
            'fields': SearchCommandOptions.get_fields(self._raw_options),
            'highlight_style': SearchCommandOptions.get_highlight_style(self._raw_options),
            'highlight_fields': SearchCommandOptions.get_highlight_fields(self._raw_options),
            'scan_consistency': SearchCommandOptions.get_scan_consistency(self._raw_options),
            'facets': SearchCommandOptions.get_facets(self._raw_options),
            'include_locations': SearchCommandOptions.get_include_locations(self._raw_options),
            'disable_scoring': SearchCommandOptions.get_disable_scoring(self._raw_options),
            'scoring': SearchCommandOptions.get_scoring(self._raw_options),
            'sort': SearchCommandOptions.get_sort(self._raw_options),
            'consistent_with': SearchCommandOptions.get_consistent_with(self._raw_options),
            'raw': SearchCommandOptions.get_raw(self._raw_options),
            'span': SearchCommandOptions.resolve_parent_span(self._raw_options, self._span_owner),
        }

        self._options = SearchOptions(**opt_kwargs)

    def set_search_query(self) -> None:
        if self._raw_query is None:
            raise ValueError('A search command needs a query.')
        self._query = SearchQueryBuilder.build_search_query(self._raw_query)

    def set_search_request(self) -> None:  # noqa: C901
        if self._raw_request is None:
            raise ValueError('A search v2 command needs a request.')
        search_query: SearchQuery | None = None
        vector_search: VectorSearch | None = None
        if self._raw_request.HasField('search_query'):
            search_query = SearchQueryBuilder.build_search_query(self._raw_request.search_query)
        if self._raw_request.HasField('vector_search'):
            queries = []
            for vq in self._raw_request.vector_search.vector_query:
                vq_opts: dict[str, Any] = {}
                if vq.HasField('options'):
                    if vq.options.HasField('num_candidates'):
                        vq_opts['num_candidates'] = vq.options.num_candidates
                    if vq.options.HasField('boost'):
                        vq_opts['boost'] = vq.options.boost
                    if vq.options.HasField('prefilter'):
                        prefilter_query = SearchQueryBuilder.build_search_query(vq.options.prefilter)

                        vq_opts['prefilter'] = prefilter_query
                if vq.HasField('base64_vector_query'):
                    queries.append(VectorQuery.create(vq.vector_field_name, vq.base64_vector_query, **vq_opts))
                else:
                    queries.append(VectorQuery.create(vq.vector_field_name, [v for v in vq.vector_query], **vq_opts))
            vs_opts = None
            if self._raw_request.vector_search.HasField('options'):
                combo = SearchCommandOptions.get_vector_query_combination(self._raw_request.vector_search.options)
                if combo:
                    vs_opts = VectorSearchOptions(vector_query_combination=combo)
            vector_search = VectorSearch(queries, options=vs_opts)
        if search_query:
            self._search_request = SearchRequest.create(search_query)
        if vector_search:
            if self._search_request:
                self._search_request.with_vector_search(vector_search)
            else:
                self._search_request = SearchRequest.create(vector_search)
        if search_query is None and vector_search is None:
            # An empty request is passed on so that the SDK raises its InvalidArgumentException.
            self._search_request = SearchRequest.create(None)  # type: ignore[arg-type]

    # TODO:  can use as_search_result_stream once C++ supports streaming in operational SDK
    # @SearchCommandResult.as_search_result_stream
    @SearchCommandResult.as_blocking_search_result
    def execute_command(self) -> SearchResult:
        use_search_v2 = False
        use_search_v2 = self._raw_request is not None

        if use_search_v2:
            if self._search_request is None:
                raise RuntimeError('The search request has not been set.')
            if self._scope is not None:
                return self._scope.search(self._index_name, self._search_request, self._options)
            return self._require_cluster().search(self._index_name, self._search_request, self._options)

        if self._query is None:
            raise RuntimeError('The search query has not been set.')
        if self._scope is None:
            return self._require_cluster().search_query(self._index_name, self._query, self._options)
        return self._scope.search_query(self._index_name, self._query, self._options)

    def _require_cluster(self) -> Cluster:
        if self._cluster is None:
            raise RuntimeError('A search command needs a cluster or a scope.')
        return self._cluster

    @staticmethod
    def create_command(**kwargs: Any) -> SearchQueryCommand:
        command = SearchQueryCommand(**kwargs)
        command.set_search_query()
        command.set_options()
        return command

    @staticmethod
    def create_v2_command(**kwargs: Any) -> SearchQueryCommand:
        command = SearchQueryCommand(**kwargs)
        command.set_search_request()
        command.set_options()
        return command


class SearchQueryBuilder:
    _VALID_MATCH_OPERATORS = {
        search_pb.MatchOperator.SEARCH_MATCH_OPERATOR_OR: MatchOperator.OR,
        search_pb.MatchOperator.SEARCH_MATCH_OPERATOR_AND: MatchOperator.AND
    }

    # `raw_query` is whichever query message the builder is filling from, and the fields are named by string.
    def __init__(self, query: SearchQuery, raw_query: Message) -> None:
        self._query = query
        self._raw_query = raw_query

    def set_primitive_field(self, field_name: str, sdk_field_name: str | None = None) -> None:
        if sdk_field_name is None:
            sdk_field_name = field_name

        if self._raw_query.HasField(field_name):
            setattr(self._query, sdk_field_name, getattr(self._raw_query, field_name))

    def set_primitive_fields(self, *args: str) -> None:
        for field_name in args:
            self.set_primitive_field(field_name)

    def set_match_operator(self) -> None:
        if not (isinstance(self._query, MatchQuery) and isinstance(self._raw_query, search_pb.MatchQuery)):
            raise TypeError('Only a match query has a match operator.')
        if self._raw_query.HasField('operator'):
            self._query.match_operator = self._VALID_MATCH_OPERATORS[self._raw_query.operator]

    def set_should_min(self) -> None:
        if not (isinstance(self._query, BooleanQuery) and isinstance(self._raw_query, search_pb.BooleanQuery)):
            raise TypeError('Only a boolean query has should_min.')
        if self._raw_query.HasField('should_min'):
            should = self._query.should
            if should is None:
                raise ValueError('should_min was given without any should queries.')
            should.min = self._raw_query.should_min

    @property
    def query(self) -> SearchQuery:
        return self._query

    @classmethod
    def build_search_query(cls, raw_query: search_pb.SearchQuery) -> SearchQuery:  # noqa: C901
        query_type = raw_query.WhichOneof('query')
        if query_type is None:
            raise NotImplementedError('Search query has no query type set')
        wrapped_raw_query = getattr(raw_query, query_type)

        if query_type == 'match':
            return cls.build_match_query(wrapped_raw_query)
        elif query_type == 'match_phrase':
            return cls.build_match_phrase_query(wrapped_raw_query)
        elif query_type == 'regexp':
            return cls.build_regex_query(wrapped_raw_query)
        elif query_type == 'query_string':
            return cls.build_query_string_query(wrapped_raw_query)
        elif query_type == 'wildcard':
            return cls.build_wildcard_query(wrapped_raw_query)
        elif query_type == 'doc_id':
            return cls.build_doc_id_query(wrapped_raw_query)
        elif query_type == 'search_boolean_field':
            return cls.build_boolean_field_query(wrapped_raw_query)
        elif query_type == 'date_range':
            return cls.build_date_range_query(wrapped_raw_query)
        elif query_type == 'numeric_range':
            return cls.build_numeric_range_query(wrapped_raw_query)
        elif query_type == 'term_range':
            return cls.build_term_range_query(wrapped_raw_query)
        elif query_type == 'geo_distance':
            return cls.build_geo_distance_query(wrapped_raw_query)
        elif query_type == 'geo_bounding_box':
            return cls.build_geo_bounding_box_query(wrapped_raw_query)
        elif query_type == 'conjunction':
            return cls.build_conjunction_query(wrapped_raw_query)
        elif query_type == 'disjunction':
            return cls.build_disjunction_query(wrapped_raw_query)
        elif query_type == 'boolean':
            return cls.build_boolean_query(wrapped_raw_query)
        elif query_type == 'term':
            return cls.build_term_query(wrapped_raw_query)
        elif query_type == 'prefix':
            return cls.build_prefix_query(wrapped_raw_query)
        elif query_type == 'phrase':
            return cls.build_phrase_query(wrapped_raw_query)
        elif query_type == 'match_all':
            return MatchAllQuery()
        elif query_type == 'match_none':
            return MatchNoneQuery()
        else:
            raise NotImplementedError(f"Search query type '{query_type}' not supported")

    @classmethod
    def build_match_query(cls, raw_query: search_pb.MatchQuery) -> SearchQuery:
        builder = cls(MatchQuery(raw_query.match), raw_query)
        builder.set_primitive_fields('field', 'analyzer', 'prefix_length', 'fuzziness', 'boost')
        builder.set_match_operator()
        return builder.query

    @classmethod
    def build_match_phrase_query(cls, raw_query: search_pb.MatchPhraseQuery) -> SearchQuery:
        builder = cls(MatchPhraseQuery(raw_query.match_phrase), raw_query)
        builder.set_primitive_fields('analyzer', 'field', 'boost')
        return builder.query

    @classmethod
    def build_regex_query(cls, raw_query: search_pb.RegexpQuery) -> SearchQuery:
        builder = cls(RegexQuery(raw_query.regexp), raw_query)
        builder.set_primitive_fields('field', 'boost')
        return builder.query

    @classmethod
    def build_query_string_query(cls, raw_query: search_pb.QueryStringQuery) -> SearchQuery:
        builder = cls(QueryStringQuery(raw_query.query), raw_query)
        builder.set_primitive_fields('boost')
        return builder.query

    @classmethod
    def build_wildcard_query(cls, raw_query: search_pb.WildcardQuery) -> SearchQuery:
        builder = cls(WildcardQuery(raw_query.wildcard), raw_query)
        builder.set_primitive_fields('field', 'boost')
        return builder.query

    @classmethod
    def build_doc_id_query(cls, raw_query: search_pb.DocIdQuery) -> SearchQuery:
        builder = cls(DocIdQuery(list(raw_query.ids)), raw_query)
        builder.set_primitive_fields('boost')
        return builder.query

    @classmethod
    def build_boolean_field_query(cls, raw_query: search_pb.BooleanFieldQuery) -> SearchQuery:
        builder = cls(BooleanFieldQuery(raw_query.bool), raw_query)
        builder.set_primitive_fields('field', 'boost')
        return builder.query

    @classmethod
    def build_date_range_query(cls, raw_query: search_pb.DateRangeQuery) -> SearchQuery:
        builder = cls(DateRangeQuery(raw_query.start, raw_query.end), raw_query)
        builder.set_primitive_fields('datetime_parser', 'field', 'boost')
        builder.set_primitive_field('inclusive_start')
        builder.set_primitive_field('inclusive_end')
        return builder.query

    @classmethod
    def build_numeric_range_query(cls, raw_query: search_pb.NumericRangeQuery) -> SearchQuery:
        # round() is a temporary hack, if we do not round, we have floating point approximation issues
        # TODO(SDKQE-3584):  Fix lossy transmission of floating point proto fields
        builder = cls(NumericRangeQuery(round(raw_query.min, 7), round(raw_query.max, 7)), raw_query)
        builder.set_primitive_fields('field', 'boost')
        builder.set_primitive_field('inclusive_min')
        builder.set_primitive_field('inclusive_max')
        return builder.query

    @classmethod
    def build_term_range_query(cls, raw_query: search_pb.TermRangeQuery) -> SearchQuery:
        builder = cls(TermRangeQuery(raw_query.min, raw_query.max), raw_query)
        builder.set_primitive_fields('field', 'boost')
        builder.set_primitive_field('inclusive_min')
        builder.set_primitive_field('inclusive_max')
        return builder.query

    @classmethod
    def build_geo_distance_query(cls, raw_query: search_pb.GeoDistanceQuery) -> SearchQuery:
        builder = cls(
            GeoDistanceQuery(raw_query.distance, (raw_query.location.lon, raw_query.location.lat)),
            raw_query
        )
        builder.set_primitive_fields('field', 'boost')
        return builder.query

    @classmethod
    def build_geo_bounding_box_query(cls, raw_query: search_pb.GeoBoundingBoxQuery) -> SearchQuery:
        builder = cls(
            GeoBoundingBoxQuery(
                (raw_query.top_left.lon, raw_query.top_left.lat),
                (raw_query.bottom_right.lon, raw_query.bottom_right.lat)
            ),
            raw_query
        )
        builder.set_primitive_fields('field', 'boost')
        return builder.query

    @classmethod
    def build_conjunction_query(cls, raw_query: search_pb.ConjunctionQuery) -> SearchQuery:
        builder = cls(
            ConjunctionQuery(*(cls.build_search_query(q) for q in raw_query.conjuncts)),
            raw_query
        )
        builder.set_primitive_fields('boost')
        return builder.query

    @classmethod
    def build_disjunction_query(cls, raw_query: search_pb.DisjunctionQuery) -> SearchQuery:
        builder = cls(
            DisjunctionQuery(*(cls.build_search_query(q) for q in raw_query.disjuncts)),
            raw_query
        )
        builder.set_primitive_fields('min', 'boost')
        return builder.query

    @classmethod
    def build_boolean_query(cls, raw_query: search_pb.BooleanQuery) -> SearchQuery:
        boolean_kwargs: dict[str, list[SearchQuery]] = {}
        if len(raw_query.must) > 0:
            boolean_kwargs['must'] = [cls.build_search_query(q) for q in raw_query.must]
        if len(raw_query.should) > 0:
            boolean_kwargs['should'] = [cls.build_search_query(q) for q in raw_query.should]
        if len(raw_query.must_not) > 0:
            boolean_kwargs['must_not'] = [cls.build_search_query(q) for q in raw_query.must_not]
        builder = cls(BooleanQuery(**boolean_kwargs), raw_query)
        builder.set_primitive_fields('boost')
        builder.set_should_min()
        return builder.query

    @classmethod
    def build_term_query(cls, raw_query: search_pb.TermQuery) -> SearchQuery:
        builder = cls(TermQuery(raw_query.term), raw_query)
        builder.set_primitive_fields('field', 'fuzziness', 'prefix_length', 'boost')
        return builder.query

    @classmethod
    def build_prefix_query(cls, raw_query: search_pb.PrefixQuery) -> SearchQuery:
        builder = cls(PrefixQuery(raw_query.prefix), raw_query)
        builder.set_primitive_fields('field', 'boost')
        return builder.query

    @classmethod
    def build_phrase_query(cls, raw_query: search_pb.PhraseQuery) -> SearchQuery:
        builder = cls(PhraseQuery(terms=list(raw_query.terms)), raw_query)
        builder.set_primitive_fields('field', 'boost')
        return builder.query


class SearchCommandBuilder:
    @staticmethod
    def build_command(search_cmd: search_pb.Search,
                      **cmd_kwargs: Any
                      ) -> SearchQueryCommand:
        cmd_kwargs.update({
            'index_name': search_cmd.indexName,
            'query': search_cmd.query,
            'stream_config': search_cmd.stream_config,
        })

        if search_cmd.HasField('options'):
            cmd_kwargs['options'] = search_cmd.options

        if search_cmd.HasField('fields_as'):
            cmd_kwargs['fields_as'] = search_cmd.fields_as

        return SearchQueryCommand.create_command(**cmd_kwargs)

    @staticmethod
    def build_command_v2(search_v2_cmd: search_pb.SearchWrapper,
                         **cmd_kwargs: Any
                         ) -> SearchQueryCommand:
        search_cmd = search_v2_cmd.search
        cmd_kwargs.update({
            'index_name': search_cmd.indexName,
            'request': search_cmd.request,
            'stream_config': search_v2_cmd.stream_config,
        })

        if search_cmd.HasField('options'):
            cmd_kwargs['options'] = search_cmd.options

        if search_v2_cmd.HasField('fields_as'):
            cmd_kwargs['fields_as'] = search_v2_cmd.fields_as

        return SearchQueryCommand.create_v2_command(**cmd_kwargs)
