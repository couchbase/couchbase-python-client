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

"""couchbase2 search: the query, sort and facet encoding, the request, the answer, and the impls end to end."""

from __future__ import annotations

import json
import logging
import warnings
from collections.abc import (Callable,
                             Coroutine,
                             Iterator)
from datetime import timedelta
from typing import Any

import grpc
import pytest
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

import couchbase.search as search
from acouchbase.protocol.couchbase2._cluster_impl import AsyncClusterImpl
from acouchbase.protocol.couchbase2._streaming import buffered_rows
from couchbase.auth import PasswordAuthenticator
from couchbase.common import _redaction
from couchbase.common._option_spec import BLOCKS
from couchbase.common.warnings import CouchbaseApplicabilityWarning, CouchbaseDeprecationWarning
from couchbase.exceptions import (FeatureUnavailableException,
                                  InvalidArgumentException,
                                  NoChildrenException,
                                  QueryIndexNotFoundException)
from couchbase.mutation_state import MutationState
from couchbase.options import (ClusterOptions,
                               ClusterTimeoutOptions,
                               SearchOptions)
from couchbase.protocol.couchbase2._cluster_impl import ClusterImpl
from couchbase.protocol.couchbase2._cluster_req_builder import ClusterRequestBuilder
from couchbase.protocol.couchbase2._cluster_types import SearchQueryRequest
from couchbase.protocol.couchbase2._core import ClusterCore
from couchbase.protocol.couchbase2._generated.search.v1 import search_pb2
from couchbase.protocol.couchbase2._scope_req_builder import ScopeRequestBuilder
from couchbase.protocol.couchbase2._search import (SearchRows,
                                                   encode_facet,
                                                   encode_query,
                                                   encode_sort,
                                                   exact_float32,
                                                   unsendable)
from couchbase.result import SearchResult
from couchbase.search_scoring import ReciprocalRankFusion, ScoringNone
from couchbase.serializer import DefaultJsonSerializer
from couchbase.vector_search import VectorQuery, VectorSearch
from tests.couchbase2_gateway import (Certificates,
                                      Fail,
                                      FakeGateway,
                                      make_certificates,
                                      new_gateway,
                                      serving)
from tests.event_loop import run_on_own_loop

pb = search_pb2
Response = pb.SearchQueryResponse
LL = pb.LatLng


@pytest.fixture(scope='module')
def core() -> Iterator[ClusterCore]:
    built = ClusterCore('couchbase2://localhost', ClusterOptions(PasswordAuthenticator('Administrator', 'password')))
    try:
        yield built
    finally:
        built.close()


def _request(core: ClusterCore,
             query: search.SearchQuery | search.SearchRequest | None = None,
             *options: SearchOptions,
             **kwargs: Any) -> SearchQueryRequest:
    request = query or search.SearchRequest(search.MatchAllQuery())
    return ClusterRequestBuilder(core).build_search_request('ix', request, *options, **kwargs)


def _boosted(query: search.SearchQuery) -> search.SearchQuery:
    query.boost = 2.0
    return query


class EncodeQueryUnitTests:

    @pytest.mark.parametrize('query, expected', [
        (search.TermQuery('park', field='f', fuzziness=2, prefix_length=1),
         pb.Query(term_query=pb.TermQuery(term='park', field='f', fuzziness=2, prefix_length=1))),
        (search.QueryStringQuery('+a -b'), pb.Query(query_string_query=pb.QueryStringQuery(query_string='+a -b'))),
        (search.WildcardQuery('f?o*', field='f'),
         pb.Query(wildcard_query=pb.WildcardQuery(wildcard='f?o*', field='f'))),
        (search.DocIdQuery(['a', 'b']), pb.Query(doc_id_query=pb.DocIdQuery(ids=['a', 'b']))),
        (search.MatchQuery('m', match_operator=search.MatchOperator.AND, field='f', analyzer='en', fuzziness=1,
                           prefix_length=2),
         pb.Query(match_query=pb.MatchQuery(value='m', operator=pb.MatchQuery.OPERATOR_AND, field='f',
                                            analyzer='en', fuzziness=1, prefix_length=2))),
        (search.MatchPhraseQuery('a b', field='f', analyzer='en'),
         pb.Query(match_phrase_query=pb.MatchPhraseQuery(phrase='a b', field='f', analyzer='en'))),
        (search.PhraseQuery('a', 'b', field='f'), pb.Query(phrase_query=pb.PhraseQuery(terms=['a', 'b'], field='f'))),
        (search.PrefixQuery('pre', field='f'), pb.Query(prefix_query=pb.PrefixQuery(prefix='pre', field='f'))),
        (search.RegexQuery('a.*', field='f'), pb.Query(regexp_query=pb.RegexpQuery(regexp='a.*', field='f'))),
        (search.BooleanFieldQuery(False, field='f'),
         pb.Query(boolean_field_query=pb.BooleanFieldQuery(value=False, field='f'))),
        (search.MatchAllQuery(), pb.Query(match_all_query=pb.MatchAllQuery())),
        (search.MatchNoneQuery(), pb.Query(match_none_query=pb.MatchNoneQuery())),
    ])
    def test_each_query_is_its_own_message(self, query: search.SearchQuery, expected: pb.Query) -> None:
        assert encode_query(query) == expected

    def test_a_boost_is_sent(self) -> None:
        assert encode_query(_boosted(search.TermQuery('a'))).term_query.boost == 2.0

    def test_a_point_is_longitude_then_latitude(self) -> None:
        distance = encode_query(search.GeoDistanceQuery('10mi', (1.0, 2.0), field='g')).geo_distance_query
        box = encode_query(search.GeoBoundingBoxQuery({'lon': 0.0, 'lat': 1.0}, (2.0, 3.0))).geo_bounding_box_query
        polygon = encode_query(search.GeoPolygonQuery([(0.0, 0.0), (1.0, 1.0), (1.0, 0.0)])).geo_polygon_query
        assert (distance.distance, distance.center, distance.field) == ('10mi', LL(longitude=1.0, latitude=2.0), 'g')
        assert (box.top_left, box.bottom_right) == (LL(longitude=0.0, latitude=1.0), LL(longitude=2.0, latitude=3.0))
        assert list(polygon.vertices) == [LL(longitude=0.0, latitude=0.0), LL(longitude=1.0, latitude=1.0),
                                          LL(longitude=1.0, latitude=0.0)]

    @pytest.mark.parametrize('query', [search.NumericRangeQuery(1, 5), search.TermRangeQuery(min='a', max='c')],
                             ids=['numeric', 'term'])
    def test_a_minimum_is_inclusive_unless_said_otherwise(self, query: Any) -> None:
        sent = encode_query(query)
        assert getattr(sent, sent.WhichOneof('query')).inclusive_min is True
        query.inclusive_min = False
        sent = encode_query(query)
        assert getattr(sent, sent.WhichOneof('query')).inclusive_min is False

    def test_a_range_with_no_minimum_sends_no_inclusivity(self) -> None:
        sent = encode_query(search.NumericRangeQuery(max=5)).numeric_range_query
        assert (sent.HasField('min'), sent.HasField('inclusive_min'), sent.max) == (False, False, 5.0)

    def test_a_date_range(self) -> None:
        query = search.DateRangeQuery('2024-01-01', '2024-02-01', inclusive_start=False, inclusive_end=False,
                                      datetime_parser='dateTimeOptional', field='f')
        assert encode_query(query).date_range_query == pb.DateRangeQuery(start_date='2024-01-01',
                                                                         end_date='2024-02-01',
                                                                         date_time_parser='dateTimeOptional',
                                                                         field='f')

    def test_compound_queries(self) -> None:
        conjunction = encode_query(_boosted(search.ConjunctionQuery(search.TermQuery('a'))))
        disjunction = encode_query(search.DisjunctionQuery(search.TermQuery('a'), search.TermQuery('b')))
        assert conjunction.conjunction_query == pb.ConjunctionQuery(
            queries=[pb.Query(term_query=pb.TermQuery(term='a'))], boost=2.0)
        assert (len(disjunction.disjunction_query.queries), disjunction.disjunction_query.minimum) == (2, 1)

    def test_a_boolean_query_sends_each_clauses_minimum(self) -> None:
        sent = encode_query(search.BooleanQuery(must=search.TermQuery('a'), should=search.TermQuery('b'),
                                                must_not=search.TermQuery('c'))).boolean_query
        assert [q.term_query.term for q in sent.must.queries] == ['a']
        assert (sent.should.minimum, sent.must_not.minimum) == (1, 1)

    def test_a_clause_that_cannot_be_sent_fails_the_whole_query(self) -> None:
        with pytest.raises(FeatureUnavailableException, match='RawQuery'):
            encode_query(search.ConjunctionQuery(search.TermQuery('a'), search.RawQuery({'term': 'b'})))

    def test_the_classes_own_checks_come_first(self) -> None:
        with pytest.raises(NoChildrenException):
            encode_query(search.ConjunctionQuery())

    def test_a_value_its_field_cannot_hold_is_an_invalid_argument(self) -> None:
        with pytest.raises(InvalidArgumentException, match='TermQuery'):
            encode_query(search.TermQuery('a', fuzziness=-1))


class UnsendableUnitTests:

    @pytest.mark.parametrize('obj, name', [
        (search.RawQuery({'term': 'a'}), 'RawQuery'),
        (_boosted(search.MatchAllQuery()), 'MatchAllQuery.boost'),
        (_boosted(search.MatchNoneQuery()), 'MatchNoneQuery.boost'),
        (search.DateRangeQuery('2024-01-01'), 'DateRangeQuery.start'),
        (search.DateRangeQuery('2024-01-01', inclusive_start=True), 'DateRangeQuery.start'),
        (search.DateRangeQuery(end='2024-01-01', inclusive_end=True), 'DateRangeQuery.inclusive_end'),
        (search.NumericRangeQuery(16777217), 'NumericRangeQuery.min'),
        (search.NumericRangeQuery(max=0.1 + 0.2), 'NumericRangeQuery.max'),
        (search.TermQuery('a', boost=1.23456789), 'TermQuery.boost'),
        (search.SortField('f', mode='min'), 'SortField.mode'),
        (search.Sort('field'), 'Sort.by'),
        (search.NumericFacet('f').add_range('r', min=16777217), 'NumericFacet.numeric_ranges'),
    ])
    def test_what_the_gateway_cannot_send(self, obj: Any, name: str) -> None:
        assert unsendable(obj) is not None
        encode = encode_facet if isinstance(obj, search.Facet) else encode_sort if isinstance(
            obj, search.Sort) else encode_query
        with pytest.raises(FeatureUnavailableException, match=rf'<message={name} cannot be sent over the couchbase2'):
            encode(obj)

    @pytest.mark.parametrize('obj', [search.TermQuery('a'), search.SortID(), search.TermFacet('f')],
                             ids=['query', 'sort', 'facet'])
    def test_set_prop_cannot_be_sent(self, obj: Any) -> None:
        obj.set_prop('custom', 1)
        assert 'set_prop' in (unsendable(obj) or '')

    @pytest.mark.parametrize('obj', [
        search.DateRangeQuery('2024-01-01', inclusive_start=False),
        search.DateRangeQuery(end='2024-01-01', inclusive_end=False),
        search.NumericRangeQuery(1.1, 16777216),
        search.TermQuery('a', boost=0.3),
        search.Sort('score'),
        search.NumericFacet('f').add_range('r', min=1.5),
    ])
    def test_what_it_can(self, obj: Any) -> None:
        assert unsendable(obj) is None

    @pytest.mark.parametrize('value, exact', [
        (1.1, True), (0.1, True), (2.5, True), (16777216, True), (-3, True), (3.4e38, True),
        (16777217, False), (1.23456789, False), (0.1 + 0.2, False), (1e40, False), (10**400, False),
        (float('inf'), False), (float('nan'), False),
    ])
    def test_a_float_the_gateway_carries_unchanged(self, value: float, exact: bool) -> None:
        assert exact_float32(value) is exact


class EncodeSortUnitTests:

    @pytest.mark.parametrize('spec, expected', [
        ('rating', pb.Sorting(field_sorting=pb.FieldSorting(field='rating'))),
        ('-rating', pb.Sorting(field_sorting=pb.FieldSorting(field='rating', descending=True))),
        ('_score', pb.Sorting(score_sorting=pb.ScoreSorting())),
        ('-_score', pb.Sorting(score_sorting=pb.ScoreSorting(descending=True))),
        ('-_id', pb.Sorting(id_sorting=pb.IdSorting(descending=True))),
    ])
    def test_a_sort_named_the_search_services_way(self, spec: str, expected: pb.Sorting) -> None:
        assert encode_sort(spec) == expected

    def test_the_sort_classes(self) -> None:
        assert encode_sort(search.SortField('f', type='number', missing='last', desc=True)) == (
            pb.Sorting(field_sorting=pb.FieldSorting(field='f', type='number', missing='last', descending=True)))
        assert encode_sort(search.SortGeoDistance((1.0, 2.0), 'g', unit='km')) == (
            pb.Sorting(geo_distance_sorting=pb.GeoDistanceSorting(field='g', center=LL(longitude=1.0, latitude=2.0),
                                                                  unit='km')))
        assert encode_sort(search.SortScore(desc=True)) == pb.Sorting(score_sorting=pb.ScoreSorting(descending=True))
        assert encode_sort(search.SortID()) == pb.Sorting(id_sorting=pb.IdSorting())


class EncodeFacetUnitTests:

    def test_a_facet_with_no_limit_has_a_size_of_zero(self) -> None:
        assert encode_facet(search.TermFacet('f')) == pb.Facet(term_facet=pb.TermFacet(field='f', size=0))

    def test_the_range_facets(self) -> None:
        numeric = encode_facet(search.NumericFacet('n', 3).add_range('low', max=2.5).add_range('high', min=2.5))
        date = encode_facet(search.DateFacet('d', 4).add_range('early', end='2024-01-01'))
        assert numeric.numeric_range_facet == pb.NumericRangeFacet(
            field='n', size=3, numeric_ranges=[pb.NumericRange(name='low', max=2.5),
                                               pb.NumericRange(name='high', min=2.5)])
        assert date.date_range_facet == pb.DateRangeFacet(field='d', size=4,
                                                          date_ranges=[pb.DateRange(name='early', end='2024-01-01')])


# One sample of every SearchOptions key, and where each one goes over couchbase2.  A key added to the
# block without a sample here fails the walk.
_SENT: dict[str, Any] = {
    'limit': 3, 'skip': 2, 'explain': True, 'include_locations': True, 'fields': ['a'], 'highlight_fields': ['a'],
    'highlight_style': search.HighlightStyle.Ansi, 'collections': ['c'], 'disable_scoring': True,
    'scoring': ScoringNone(), 'sort': ['a'], 'facets': {'f': search.TermFacet('f')},
    'scan_consistency': search.SearchScanConsistency.NOT_BOUNDED,
}
_READ: dict[str, Any] = {'timeout': timedelta(seconds=3), 'serializer': DefaultJsonSerializer(), 'span': None,
                         'parent_span': None, 'scope_name': 's'}
_REFUSED: dict[str, Any] = {'consistent_with': MutationState()}
_INERT: dict[str, Any] = {'client_context_id': 'c', 'raw': {'x': 1}, 'show_request': True}
_LOGGED: dict[str, Any] = {'log_request': True, 'log_response': True}


class SearchRequestUnitTests:

    def test_every_option_is_accounted_for(self) -> None:
        assert set(BLOCKS['SearchOptions'].keys) == {*_SENT, *_READ, *_REFUSED, *_INERT, *_LOGGED}

    @pytest.mark.parametrize('key', sorted(_SENT))
    def test_each_option_sent_changes_the_request(self, core: ClusterCore, key: str) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', CouchbaseDeprecationWarning)
            sent = _request(core, None, SearchOptions(**{key: _SENT[key]})).message
        baseline = _request(core).message
        if key == 'scan_consistency':
            assert sent.scan_consistency == pb.SearchQueryRequest.SCAN_CONSISTENCY_NOT_BOUNDED
        else:
            assert sent != baseline

    @pytest.mark.parametrize('key', sorted(_INERT))
    def test_an_option_the_gateway_cannot_act_on_warns_and_is_not_sent(self, core: ClusterCore, key: str) -> None:
        with pytest.warns(CouchbaseApplicabilityWarning, match=f'Option {key} '):
            sent = _request(core, None, **{key: _INERT[key]}).message
        assert sent == _request(core).message

    def test_the_options(self, core: ClusterCore) -> None:
        sent = _request(core, None, SearchOptions(limit=3, skip=2, explain=True, include_locations=True,
                                                  fields=['a'], highlight_fields=['b'], collections=['c']),
                        highlight_style='html', sort='-_score').message
        assert (sent.limit, sent.skip, sent.include_explanation, sent.include_locations) == (3, 2, True, True)
        assert (list(sent.fields), list(sent.highlight_fields), list(sent.collections)) == (['a'], ['b'], ['c'])
        assert sent.highlight_style == pb.SearchQueryRequest.HIGHLIGHT_STYLE_HTML
        assert list(sent.sort) == [pb.Sorting(score_sorting=pb.ScoreSorting(descending=True))]

    def test_scoring_none_disables_scoring(self, core: ClusterCore) -> None:
        assert _request(core, None, scoring=ScoringNone()).message.disable_scoring is True

    def test_disable_scoring_is_sent_and_deprecated(self, core: ClusterCore) -> None:
        with pytest.warns(CouchbaseDeprecationWarning):
            assert _request(core, None, disable_scoring=True).message.disable_scoring is True

    @pytest.mark.parametrize('kwargs, match', [
        ({'consistent_with': MutationState()}, 'consistent_with'),
        ({'scan_consistency': search.SearchScanConsistency.REQUEST_PLUS}, 'REQUEST_PLUS scan consistency'),
        ({'scoring': ReciprocalRankFusion()}, 'ReciprocalRankFusion scoring'),
        ({'limit': 0}, 'A limit of 0'),
    ])
    def test_what_the_gateway_cannot_carry_is_refused(self, core: ClusterCore, kwargs: dict[str, Any],
                                                      match: str) -> None:
        with pytest.raises(FeatureUnavailableException, match=f'<message={match} is not available over the couchbase2'):
            _request(core, None, **kwargs)

    def test_a_vector_search_is_refused(self, core: ClusterCore) -> None:
        request = search.SearchRequest(VectorSearch.from_vector_query(VectorQuery('v', [1.0, 2.0])))
        with pytest.raises(FeatureUnavailableException, match='<message=Vector search'):
            _request(core, request)

    @pytest.mark.parametrize('kwargs', [
        {'scan_consistency': search.SearchScanConsistency.AT_PLUS},
        {'scoring': ScoringNone(), 'disable_scoring': True},
        {'limit': -1},
        {'skip': 2**32},
        {'fields': 'a'},
        {'sort': [1]},
        {'facets': {'f': search.DateFacet('d')}},
        {'timeout': 3},
    ])
    def test_arguments_refused_as_on_classic(self, core: ClusterCore, kwargs: dict[str, Any]) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', CouchbaseDeprecationWarning)
            with pytest.raises(InvalidArgumentException):
                _request(core, None, **kwargs)

    def test_search_query_takes_no_request(self, core: ClusterCore) -> None:
        with pytest.raises(InvalidArgumentException, match='Pass it to search'):
            request: Any = search.SearchRequest(search.MatchAllQuery())
            ClusterRequestBuilder(core).build_search_query_request('ix', request)

    def test_search_warns_on_a_bare_query(self, core: ClusterCore) -> None:
        with pytest.warns(CouchbaseDeprecationWarning):
            _request(core, search.TermQuery('a'))

    def test_the_timeout(self, core: ClusterCore) -> None:
        assert _request(core).timeout == 75.0
        assert _request(core, None, timeout=timedelta(seconds=3)).timeout == 3.0
        options = ClusterOptions(PasswordAuthenticator('Administrator', 'password'),
                                 timeout_options=ClusterTimeoutOptions(search_timeout=timedelta(seconds=9)))
        assert _request(ClusterCore('couchbase2://localhost', options)).timeout == 9.0

    def test_a_cluster_search_names_its_index_alone(self, core: ClusterCore) -> None:
        sent = _request(core)
        assert (sent.message.index_name, sent.message.HasField('bucket_name')) == ('ix', False)
        assert 'bucket_name' not in sent.span_attributes

    def test_a_scope_search_names_its_index_in_full(self, core: ClusterCore) -> None:
        sent = ScopeRequestBuilder(core, 'travel', 'inventory').build_search_query_request('ix', search.MatchAllQuery())
        assert (sent.message.index_name, sent.message.HasField('bucket_name')) == ('travel.inventory.ix', False)
        assert (sent.span_attributes.get('bucket_name'), sent.span_attributes.get('scope_name')) == ('travel',
                                                                                                     'inventory')

    def test_the_deprecated_scope_name_picks_the_scope(self, core: ClusterCore) -> None:
        with pytest.warns(CouchbaseDeprecationWarning):
            sent = ScopeRequestBuilder(core, 'travel', 'inventory').build_search_request(
                'ix', search.SearchRequest(search.MatchAllQuery()), scope_name='other')
        assert sent.message.index_name == 'travel.other.ix'

    def test_what_the_answer_keeps(self, core: ClusterCore) -> None:
        bare, located, highlighted = (_request(core), _request(core, None, include_locations=True),
                                      _request(core, None, highlight_fields=['a']))
        assert (bare.fragments, bare.locations) == (False, False)
        assert (located.fragments, located.locations) == (False, True)
        assert (highlighted.fragments, highlighted.locations) == (True, True)


def _hit(doc_id: str, **fields: Any) -> Response.SearchQueryRow:
    return Response.SearchQueryRow(id=doc_id, index='p1', score=1.5, **fields)


def _meta(total: int = 1) -> Response.MetaData:
    return Response.MetaData(metrics=Response.SearchMetrics(execution_time=Duration(seconds=1, nanos=500),
                                                            total_rows=total, max_score=2.5,
                                                            total_partition_count=3, success_partition_count=2,
                                                            error_partition_count=1),
                             errors={'p3': 'down'})


def _read(req: SearchQueryRequest, *messages: Response) -> tuple[list[Any], Any]:
    rows = SearchRows(req)
    for message in messages:
        rows.take(message)
    read = []
    while rows:
        read.append(rows.next_row())
    return read, rows.final_metadata()


class SearchAnswerUnitTests:

    def test_a_row(self, core: ClusterCore) -> None:
        req = _request(core, None, include_locations=True, highlight_style='html')
        location = Response.Location(field='f', term='t', position=1, start=0, end=5)
        hit = _hit('a', explanation=b'{"value": 1}', locations=[location],
                   fragments={'f': Response.Fragment(content=['<mark>t</mark>'])},
                   fields={'n': b'1', 's': b'"x"', 'o': b'{"k": [1]}'})
        (row,), _ = _read(req, Response(hits=[hit]))
        assert (row.id, row.index, row.score, row.explanation) == ('a', 'p1', 1.5, {'value': 1})
        assert row.fields == {'n': 1, 's': 'x', 'o': {'k': [1]}} and isinstance(row.fields, search.SearchRowFields)
        assert row.fragments == {'f': ['<mark>t</mark>']}
        assert row.locations.get_all() == [search.SearchRowLocation(field='f', term='t', position=1, start=0, end=5)]

    def test_what_the_request_did_not_ask_for_is_dropped(self, core: ClusterCore) -> None:
        hit = _hit('a', locations=[Response.Location(field='f', term='t')],
                   fragments={'f': Response.Fragment(content=['x'])})
        (row,), _ = _read(_request(core), Response(hits=[hit]))
        assert (row.fields, row.locations, row.fragments, row.explanation) == (None, None, {}, {})

    def test_the_fields_are_read_by_the_requests_serializer(self, core: ClusterCore) -> None:
        class Upper(DefaultJsonSerializer):
            def deserialize(self, value: bytes) -> Any:
                return {k.upper(): v for k, v in super().deserialize(value).items()}

        (row,), _ = _read(_request(core, None, serializer=Upper()), Response(hits=[_hit('a', fields={'n': b'1'})]))
        assert row.fields == {'N': 1}

    def test_the_metadata(self, core: ClusterCore) -> None:
        rows, final = _read(_request(core), Response(hits=[_hit('a')]), Response(hits=[_hit('b')], meta_data=_meta(2)))
        metadata = final.metadata
        metrics = metadata.metrics()
        assert [row.id for row in rows] == ['a', 'b']
        assert (metadata.client_context_id(), metadata.errors()) == (None, {'p3': 'down'})
        assert (metrics.took(), metrics.total_rows(), metrics.max_score()) == (timedelta(seconds=1), 2, 2.5)
        assert (metrics.total_partition_count(), metrics.success_partition_count(),
                metrics.error_partition_count()) == (3, 2, 1)
        assert final.facets is None

    def test_the_request_is_logged_at_info_when_asked(self, core: ClusterCore,
                                                      caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.INFO, logger='couchbase.couchbase2'):
            _request(core)
            _request(core, None, log_request=True)
        (record,) = caplog.records
        assert record.levelno == logging.INFO
        assert record.getMessage() == 'SEARCH: {"index_name": "ix", "query": {"match_all_query": {}}}'

    def test_the_logged_request_is_tagged_as_user_data(self, core: ClusterCore,
                                                       caplog: pytest.LogCaptureFixture,
                                                       monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_redaction, '_enabled', True)
        with caplog.at_level(logging.INFO, logger='couchbase.couchbase2'):
            _request(core, None, log_request=True)
        (record,) = caplog.records
        assert record.getMessage() == 'SEARCH: <ud>{"index_name": "ix", "query": {"match_all_query": {}}}</ud>'

    def test_the_request_is_logged_at_debug_otherwise(self, core: ClusterCore,
                                                      caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.DEBUG, logger='couchbase.couchbase2'):
            _request(core)
        assert [(r.levelno, r.getMessage().split(':')[0]) for r in caplog.records] == [(logging.DEBUG, 'SEARCH')]

    def test_the_response_is_logged_without_its_hits_when_asked(self, core: ClusterCore,
                                                                caplog: pytest.LogCaptureFixture) -> None:
        answer = (Response(hits=[_hit('a')]), Response(meta_data=_meta(), facets={'f': Response.FacetResult()}))
        quiet, asked = _request(core), _request(core, None, log_response=True)
        with caplog.at_level(logging.DEBUG, logger='couchbase.couchbase2'):
            _read(quiet, *answer)
            assert caplog.records == []
            _read(asked, *answer)
        (record,) = caplog.records
        logged = json.loads(record.getMessage().removeprefix('SEARCH RESPONSE: '))
        assert record.levelno == logging.INFO
        assert set(logged) == {'meta_data', 'facets'}
        assert logged['meta_data']['errors'] == {'p3': 'down'}

    def test_a_stream_with_no_metadata_has_none(self, core: ClusterCore) -> None:
        _, final = _read(_request(core), Response(hits=[_hit('a')]))
        assert (final.metadata, final.facets) == (None, None)

    def test_a_term_facet(self, core: ClusterCore) -> None:
        result = Response.FacetResult(term_facet=Response.TermFacetResult(
            field='f', total=5, missing=1, other=2, terms=[Response.TermResult(name='x', size=3)]))
        empty = Response.FacetResult(term_facet=Response.TermFacetResult(field='g', total=5))
        _, final = _read(_request(core), Response(facets={'t': result, 'e': empty}))
        assert final.facets['t'] == search.SearchFacetResult(name='t', field='f', total=5, missing=1, other=2,
                                                             terms=[search.SearchTermFacet(term='x', count=3)],
                                                             numeric_ranges=None, date_ranges=None)
        assert final.facets['e'].terms is None

    def test_the_range_bounds_are_the_requests(self, core: ClusterCore) -> None:
        req = _request(core, None, facets={
            'n': search.NumericFacet('price', 5).add_range('cheap', 0.5, 2.75),
            'd': search.DateFacet('when', 5).add_range('late', start='2024-01-15T10:30:00.5+02:00')})
        numeric = Response.FacetResult(numeric_range_facet=Response.NumericRangeFacetResult(
            field='price', total=2, numeric_ranges=[Response.NumericRangeResult(name='cheap', size=2, max=2)]))
        date = Response.FacetResult(date_range_facet=Response.DateRangeFacetResult(
            field='when', total=2, date_ranges=[Response.DateRangeResult(name='late', size=2,
                                                                         start=Timestamp(seconds=1705307400,
                                                                                         nanos=500000000))]))
        _, final = _read(req, Response(facets={'n': numeric}), Response(facets={'d': date}, meta_data=_meta()))
        assert final.facets['n'].numeric_ranges == [search.SearchNumericRangeFacet(name='cheap', count=2, min=0.5,
                                                                                   max=2.75)]
        assert final.facets['d'].date_ranges == [search.SearchDateRangeFacet(name='late', count=2,
                                                                             start='2024-01-15T10:30:00.5+02:00')]


@pytest.fixture(scope='module')
def certificates(tmp_path_factory: pytest.TempPathFactory) -> Certificates:
    made = make_certificates(tmp_path_factory.mktemp('pki'))
    if made is None:
        pytest.skip('openssl is not on PATH, so no certificates can be made for a TLS server')
    return made


@pytest.fixture
def gateway() -> FakeGateway:
    return new_gateway()


@pytest.fixture
def connect(gateway: FakeGateway, certificates: Certificates) -> Iterator[Callable[[], tuple[str, ClusterOptions]]]:
    with serving(gateway, certificates) as port:
        yield lambda: (f'couchbase2://localhost:{port}',
                       ClusterOptions(PasswordAuthenticator('Administrator', 'password'),
                                      trust_store_path=str(certificates.ca)))


@pytest.fixture
def cluster(connect: Callable[[], tuple[str, ClusterOptions]]) -> Iterator[ClusterImpl]:
    impl = ClusterImpl(*connect())
    try:
        yield impl
    finally:
        impl.close_connection()


def _answer(gateway: FakeGateway) -> None:
    facet = Response.FacetResult(term_facet=Response.TermFacetResult(field='f', total=2,
                                                                     terms=[Response.TermResult(name='x', size=2)]))
    gateway.search_messages = [Response(hits=[_hit('a'), _hit('b')]),
                               Response(hits=[_hit('c')], facets={'t': facet}, meta_data=_meta(3))]


def _total_rows(metadata: search.SearchMetaData | None) -> int | None:
    metrics = None if metadata is None else metadata.metrics()
    return None if metrics is None else metrics.total_rows()


def _run(coroutine: Callable[[], Coroutine[Any, Any, Any]]) -> Any:
    return run_on_own_loop(coroutine())


class SearchImplUnitTests:

    def test_the_rows_stream_then_the_metadata_and_facets(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        _answer(gateway)
        result = cluster.search_query('ix', search.TermQuery('a'), SearchOptions(facets={'t': search.TermFacet('f')}))
        assert [row.id for row in result.rows()] == ['a', 'b', 'c']
        metadata, facets = result.metadata(), result.facets()
        assert _total_rows(metadata) == 3
        assert facets is not None and facets['t'].terms == [search.SearchTermFacet(term='x', count=2)]
        sent = gateway.calls[0].request
        assert (sent.index_name, sent.query.term_query.term) == ('ix', 'a')

    def test_nothing_is_sent_until_the_rows_are_read(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        result = cluster.search('ix', search.SearchRequest(search.MatchAllQuery()))
        assert gateway.calls == []
        list(result.rows())
        assert [call.method for call in gateway.calls] == ['SearchQuery']

    def test_a_scope_search_names_its_index_in_full(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        list(cluster.bucket('travel').scope('inventory').search_query('ix', search.MatchAllQuery()).rows())
        assert gateway.calls[0].request.index_name == 'travel.inventory.ix'

    def test_a_missing_index_raises_the_query_class(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        gateway.failures['SearchQuery'] = [Fail(grpc.StatusCode.NOT_FOUND, 'searchindex')]
        with pytest.raises(QueryIndexNotFoundException):
            list(cluster.search_query('missing', search.MatchAllQuery()).rows())

    def test_a_search_is_retried_before_its_first_message(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        _answer(gateway)
        gateway.failures['SearchQuery'] = [Fail(grpc.StatusCode.UNAVAILABLE)]
        assert len(list(cluster.search_query('ix', search.MatchAllQuery()).rows())) == 3
        assert len(gateway.calls) == 2

    def test_the_search_timeout_defaults_to_seventy_five_seconds(self, cluster: ClusterImpl,
                                                                 gateway: FakeGateway) -> None:
        list(cluster.search_query('ix', search.MatchAllQuery()).rows())
        assert 74.0 < gateway.calls[-1].remaining <= 75.0

    def test_an_error_ends_the_rows(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        gateway.search_messages = [Response(hits=[_hit('a')])]
        gateway.search_end = Fail(grpc.StatusCode.INVALID_ARGUMENT)
        with pytest.raises(InvalidArgumentException):
            list(cluster.search_query('ix', search.MatchAllQuery()).rows())

    def test_an_argument_is_refused_before_anything_is_sent(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        with pytest.raises(FeatureUnavailableException):
            cluster.search_query('ix', search.RawQuery({'term': 'a'}))
        assert gateway.calls == []


class AsyncSearchImplUnitTests:

    def test_a_cluster_search(self, connect: Callable[[], tuple[str, ClusterOptions]], gateway: FakeGateway) -> None:
        _answer(gateway)

        async def run() -> Any:
            impl = AsyncClusterImpl(*connect())
            try:
                result = impl.search_query('ix', search.MatchAllQuery())
                rows = [row.id async for row in result]
                return rows, result.metadata(), result.facets()
            finally:
                await impl.close_connection()

        rows, metadata, facets = _run(run)
        assert rows == ['a', 'b', 'c']
        assert _total_rows(metadata) == 3
        assert facets is not None and 't' in facets

    def test_a_scope_search(self, connect: Callable[[], tuple[str, ClusterOptions]], gateway: FakeGateway) -> None:
        async def run() -> None:
            impl = AsyncClusterImpl(*connect())
            try:
                scope = impl.bucket('travel').scope('inventory')
                [row async for row in scope.search('ix', search.SearchRequest(search.MatchAllQuery()))]
            finally:
                await impl.close_connection()

        _run(run)
        assert gateway.calls[0].request.index_name == 'travel.inventory.ix'

    def test_a_missing_index(self, connect: Callable[[], tuple[str, ClusterOptions]], gateway: FakeGateway) -> None:
        gateway.failures['SearchQuery'] = [Fail(grpc.StatusCode.NOT_FOUND, 'searchindex')]

        async def run() -> None:
            impl = AsyncClusterImpl(*connect())
            try:
                [row async for row in impl.search_query('missing', search.MatchAllQuery())]
            finally:
                await impl.close_connection()

        with pytest.raises(QueryIndexNotFoundException):
            _run(run)

    def test_the_whole_stream_is_read_for_twisted(self, connect: Callable[[], tuple[str, ClusterOptions]],
                                                  gateway: FakeGateway) -> None:
        _answer(gateway)

        async def run() -> SearchResult:
            impl = AsyncClusterImpl(*connect())
            try:
                prepared = impl.bucket('travel').scope('inventory').prepare_search_query(
                    'ix', search.MatchAllQuery(), None, {})
                return SearchResult(await buffered_rows(lambda: impl.open_search(prepared, None), SearchRows(prepared)))
            finally:
                await impl.close_connection()

        result = _run(run)
        assert [row.id for row in result.rows()] == ['a', 'b', 'c']
        metadata = result.metadata()
        assert _total_rows(metadata) == 3
