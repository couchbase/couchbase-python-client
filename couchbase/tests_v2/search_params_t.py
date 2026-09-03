#  Copyright 2016-2024. Couchbase, Inc.
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
import warnings
from datetime import timedelta
from typing import (Any,
                    Callable,
                    Dict,
                    List,
                    Literal,
                    Optional,
                    Set,
                    Tuple,
                    get_args,
                    get_origin,
                    get_type_hints)

import pytest

import couchbase.search as search
from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.pycbc_core.binding_cpp_types import CppSearchRequest
from couchbase.mutation_state import MutationState
from couchbase.options import SearchOptions, VectorSearchOptions
from couchbase.result import MutationToken
from couchbase.search import (HighlightStyle,
                              MatchOperator,
                              SearchRequest)
from couchbase.search_scoring import (ReciprocalRankFusion,
                                      RelativeScoreFusion,
                                      ScoringNone,
                                      SearchScoring)
from couchbase.serializer import DefaultJsonSerializer
from couchbase.vector_search import (VectorQuery,
                                     VectorQueryCombination,
                                     VectorSearch)

# A label rather than a claim about a cluster: nothing here connects.
INDEX_NAME = 'test-search-index'


def _encode(builder: search.SearchQueryBuilder) -> Dict[str, Any]:
    # `as_encodable()` serializes the query, the facets and the sort specs to JSON text, so
    # comparing the raw result would depend on key order and float formatting.  `facets` is
    # a dict of name to JSON string and `sort_specs` a list of them, so neither is the
    # single string `json.loads` accepts and neither parses the way `query` does.
    encoded = dict(builder.as_encodable())
    encoded['query'] = json.loads(encoded['query'])
    if 'facets' in encoded:
        encoded['facets'] = {name: json.loads(facet) for name, facet in encoded['facets'].items()}
    if 'sort_specs' in encoded:
        encoded['sort_specs'] = [json.loads(spec) for spec in encoded['sort_specs']]
    if 'vector_search' in encoded:
        encoded['vector_search'] = json.loads(encoded['vector_search'])
    return encoded


def _encoded(query: search.SearchQuery,
             options: Optional[SearchOptions] = None) -> Dict[str, Any]:
    if options is None:
        builder = search.SearchQueryBuilder.create_search_query_object(INDEX_NAME, query)
    else:
        builder = search.SearchQueryBuilder.create_search_query_object(INDEX_NAME, query, options)
    return _encode(builder)


def _params(query: search.SearchQuery,
            options: SearchOptions) -> search.SearchQueryBuilder:
    return search.SearchQueryBuilder.create_search_query_object(INDEX_NAME, query, options)


def _expected_request(query_body: Any, **overrides: Any) -> Dict[str, Any]:
    # `metrics` is in the base because create_search_query_object writes it unconditionally,
    # defaulting to True.  Query and analytics both default the same field the other way.
    return {'query': query_body, 'index_name': INDEX_NAME, 'metrics': True, **overrides}


def _expected_params(**overrides: Any) -> Dict[str, Any]:
    # Deliberately not the shape of _expected_request(): `params` is the options half only.
    # It never carries `index_name` or `query`, and never `facets` or `sort` either, which
    # is the easy mistake, since both are set through SearchOptions like everything else.
    return {'metrics': True, **overrides}


def _rejects(build: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    # Called through a `Callable[..., Any]` so the argument types are erased at the call
    # site.  These tests have to pass values the annotations forbid, and the alternative is
    # fourteen arg-type suppressions no fix would ever retire.  Written without the literal
    # suppression syntax: tools/audit_type_ignores.py matches raw file text, so quoting the
    # syntax in prose registers as an unaccountable suppression and fails the hook.
    with pytest.raises(InvalidArgumentException):
        build(*args, **kwargs)


def _assert_query_repr(query: search.SearchQuery, encoded: Dict[str, Any]) -> None:
    # Reads the body back off the encoded request on purpose: that holds `__repr__` to the
    # encoding rather than to a hand-copied literal.
    assert str(query) == f'{type(query).__name__}(query={encoded["query"]})'


class SearchParamsUnitTests:
    # No cb_env and no TEST_MANIFEST: nothing here connects, and the manifest is a parity
    # contract between api trees.  Not generated either, since `couchbase.search` is the
    # only module defining these types and the async trees re-export the same objects.

    def test_boolean_query(self) -> None:
        prefix_q = search.PrefixQuery('someterm', boost=2)
        bool_q = search.BooleanQuery(must=prefix_q, must_not=prefix_q, should=prefix_q)
        encoded = _encoded(bool_q)

        exp = {'prefix': 'someterm', 'boost': 2.0}
        assert encoded['query']['must'] == {'conjuncts': [exp]}
        assert encoded['query']['must_not'] == {'disjuncts': [exp], 'min': 1}
        assert encoded['query']['should'] == {'disjuncts': [exp], 'min': 1}
        _assert_query_repr(bool_q, encoded)

        # Multiple criteria in a single clause stay in the order they were given.
        bool_q = search.BooleanQuery(must=[search.PrefixQuery('someterm', boost=2),
                                           search.PrefixQuery('otherterm')])
        encoded = _encoded(bool_q)
        assert encoded['query']['must'] == {'conjuncts': [{'prefix': 'someterm', 'boost': 2.0},
                                                          {'prefix': 'otherterm'}]}
        _assert_query_repr(bool_q, encoded)

    def test_booleanfield_query(self) -> None:
        expected = _expected_request({'bool': True})

        q = search.BooleanFieldQuery(True)
        encoded = _encoded(q)
        assert encoded == expected
        _assert_query_repr(q, encoded)

        q = search.BooleanFieldQuery(bool=True)
        encoded = _encoded(q)
        assert encoded == expected
        _assert_query_repr(q, encoded)

    def test_conjunction_query(self) -> None:
        # A conjunction with no children cannot encode, and says so at encode time rather than
        # at construction: the children can still be added after the object exists.
        with pytest.raises(search.NoChildrenException):
            _encoded(search.ConjunctionQuery())

        conjuncts = {'conjuncts': [{'prefix': 'somePrefix'}, {'term': 'sand'}]}

        q = search.ConjunctionQuery(search.PrefixQuery('somePrefix'), search.TermQuery('sand'))
        encoded = _encoded(q)
        assert encoded['query'] == conjuncts
        _assert_query_repr(q, encoded)

        q = search.ConjunctionQuery([search.PrefixQuery('somePrefix'), search.TermQuery('sand')])
        encoded = _encoded(q)
        assert encoded['query'] == conjuncts
        _assert_query_repr(q, encoded)

    def test_consistent_with(self) -> None:
        mt = MutationToken(token={'partition_id': 42,
                                  'partition_uuid': 3004,
                                  'sequence_number': 3,
                                  'bucket_name': 'default'})
        ms = MutationState()
        ms._add_scanvec(mt)
        builder = _params(search.TermQuery('someterm'), SearchOptions(consistent_with=ms))

        # The core infers AT_PLUS from the presence of a mutation state, so the encoded param
        # is absent while the property still reports it.  They disagree by design.
        assert builder.params.get('scan_consistency', None) is None
        assert builder.consistency == search.SearchScanConsistency.AT_PLUS

        q_mt = builder.params.get('mutation_state', None)
        assert isinstance(q_mt, list)
        assert len(q_mt) == 1
        assert q_mt[0] == mt.as_dict()

    def test_daterange_query(self) -> None:
        # At least one bound is required, and neither bound alone is.
        with pytest.raises(ValueError):
            search.DateRangeQuery()

        cases: List[Tuple[Dict[str, Any], Dict[str, Any]]]
        cases = [({'end': '2024-12-01'}, {'end': '2024-12-01'}),
                 ({'start': '2024-01-01'}, {'start': '2024-01-01'}),
                 ({'start': '2024-01-01', 'end': '2024-12-01'},
                  {'start': '2024-01-01', 'end': '2024-12-01'})]
        for kwargs, body in cases:
            q = search.DateRangeQuery(**kwargs)
            encoded = _encoded(q)
            assert encoded['query'] == body
            _assert_query_repr(q, encoded)

        # Empty strings are bounds, not absent bounds, so they survive to the request.
        q = search.DateRangeQuery('', '')
        encoded = _encoded(q)
        assert encoded['query'] == {'start': '', 'end': ''}
        _assert_query_repr(q, encoded)

        # start_inclusive/end_inclusive are the deprecated spelling of inclusive_start/
        # inclusive_end, and both must reach the request under the current names.
        inclusive = {'start': '2024-01-01',
                     'end': '2024-12-01',
                     'inclusive_start': True,
                     'inclusive_end': True}
        spellings: List[Dict[str, Any]] = [{'start_inclusive': True, 'end_inclusive': True},
                                           {'inclusive_start': True, 'inclusive_end': True}]
        for kwargs in spellings:
            q = search.DateRangeQuery('2024-01-01', '2024-12-01', **kwargs)
            encoded = _encoded(q)
            assert encoded['query'] == inclusive
            _assert_query_repr(q, encoded)

    def test_disjunction_query(self) -> None:
        q = search.DisjunctionQuery()
        assert q.min == 1
        with pytest.raises(search.NoChildrenException):
            _encoded(q)

        disjuncts = {'disjuncts': [{'prefix': 'somePrefix'}, {'term': 'sand'}], 'min': 1}

        q = search.DisjunctionQuery(search.PrefixQuery('somePrefix'), search.TermQuery('sand'))
        encoded = _encoded(q)
        assert encoded['query'] == disjuncts
        _assert_query_repr(q, encoded)

        with pytest.raises(InvalidArgumentException):
            q.min = -1

        # min above the number of children is rejected at encode time, not when set.
        q.min = 3
        with pytest.raises(InvalidArgumentException):
            _encoded(q)

        q = search.DisjunctionQuery([search.PrefixQuery('somePrefix'), search.TermQuery('sand')])
        encoded = _encoded(q)
        assert encoded['query'] == disjuncts
        _assert_query_repr(q, encoded)

    def test_docid_query(self) -> None:
        with pytest.raises(search.NoChildrenException):
            _encoded(search.DocIdQuery([]))

        expected = _expected_request({'ids': ['foo', 'bar', 'baz']})

        q = search.DocIdQuery(['foo', 'bar', 'baz'])
        encoded = _encoded(q)
        assert encoded == expected
        _assert_query_repr(q, encoded)

        q = search.DocIdQuery(ids=['foo', 'bar', 'baz'])
        encoded = _encoded(q)
        assert encoded == expected
        _assert_query_repr(q, encoded)

    def test_facets(self) -> None:
        q = search.TermQuery('someterm')

        # A range needs bounds; naming one without them is rejected where it is added.
        f = search.NumericFacet('numfield')
        with pytest.raises(InvalidArgumentException):
            f.add_range('range1')

        builder = _params(q, SearchOptions())
        # The facets mapping validates on assignment, so a facet with no ranges cannot be
        # smuggled in after the builder exists.
        with pytest.raises(InvalidArgumentException):
            builder.facets['facetName'] = f

        builder.facets['facetName'] = f.add_range('range1', min=123, max=321)
        assert 'facetName' in builder.facets

        date_facet = search.DateFacet('datefield')
        date_facet.add_range('r1', start='2012', end='2013')
        date_facet.add_range('r2', start='2014')
        date_facet.add_range('r3', end='2015')
        builder = _params(q, SearchOptions(facets={'facetName': date_facet}))
        assert builder.facets['facetName'].encodable == {
            'field': 'datefield',
            'date_ranges': [{'name': 'r1', 'start': '2012', 'end': '2013'},
                            {'name': 'r2', 'start': '2014'},
                            {'name': 'r3', 'end': '2015'}]
        }

        term_facet = search.TermFacet('termfield')
        term_facet.limit = 10
        builder = _params(q, SearchOptions(facets={'facetName': term_facet}))
        assert builder.facets['facetName'].encodable == {'field': 'termfield', 'size': 10}

    def test_geo_bounding_box_query(self) -> None:
        expected = _expected_request({'top_left': [-0.489, 51.686],
                                      'bottom_right': [0.236, 51.28],
                                      'field': 'geo_location'})

        # A corner is accepted as a tuple, a list, a dict or a keyword, and all four reach the
        # request as the same two-element [lon, lat] array.
        corners: list[tuple[Any, Any]] = [((-0.489, 51.686), (0.236, 51.28)),
                                          ([-0.489, 51.686], [0.236, 51.28]),
                                          ({'lon': -0.489, 'lat': 51.686}, {'lon': 0.236, 'lat': 51.28})]
        for top_left, bottom_right in corners:
            q = search.GeoBoundingBoxQuery(top_left, bottom_right, field='geo_location')
            encoded = _encoded(q)
            assert encoded == expected
            _assert_query_repr(q, encoded)

        q = search.GeoBoundingBoxQuery(top_left=(-0.489, 51.686),
                                       bottom_right=(0.236, 51.28),
                                       field='geo_location')
        encoded = _encoded(q)
        assert encoded == expected
        _assert_query_repr(q, encoded)

    def test_geo_distance_query(self) -> None:
        expected = _expected_request({'distance': '130mi',
                                      'location': [-115.1391, 36.1716],
                                      'field': 'geo_location'})

        locations: List[Any] = [(-115.1391, 36.1716),
                                [-115.1391, 36.1716],
                                {'lon': -115.1391, 'lat': 36.1716}]
        for location in locations:
            q = search.GeoDistanceQuery('130mi', location, field='geo_location')
            encoded = _encoded(q)
            assert encoded == expected
            _assert_query_repr(q, encoded)

        q = search.GeoDistanceQuery(distance='130mi',
                                    location=(-115.1391, 36.1716),
                                    field='geo_location')
        encoded = _encoded(q)
        assert encoded == expected
        _assert_query_repr(q, encoded)

    def test_geo_polygon_query(self) -> None:
        points = [(37.79393211306212, -122.44234633404847),
                  (37.77995881733997, -122.43977141339417),
                  (37.788031092020155, -122.42925715405579),
                  (37.79026946582319, -122.41149020154114),
                  (37.79571192027403, -122.40735054016113),
                  (37.79393211306212, -122.44234633404847)]
        expected = _expected_request({'polygon_points': [list(p) for p in points],
                                      'field': 'geo_location'})

        q = search.GeoPolygonQuery(points, field='geo_location')
        encoded = _encoded(q)
        assert encoded == expected
        _assert_query_repr(q, encoded)

        q = search.GeoPolygonQuery(polygon_points=points, field='geo_location')
        encoded = _encoded(q)
        assert encoded == expected
        _assert_query_repr(q, encoded)

    def test_match_all_query(self) -> None:
        q = search.MatchAllQuery()
        encoded = _encoded(q)
        assert encoded == _expected_request({'match_all': None})
        _assert_query_repr(q, encoded)

    def test_match_none_query(self) -> None:
        q = search.MatchNoneQuery()
        encoded = _encoded(q)
        assert encoded == _expected_request({'match_none': None})
        _assert_query_repr(q, encoded)

    def test_match_phrase_query(self) -> None:
        expected = _expected_request({'match_phrase': 'salty beers',
                                      'analyzer': 'analyzer',
                                      'boost': 1.5,
                                      'field': 'field'},
                                     limit=10)
        opts = SearchOptions(limit=10)

        q = search.MatchPhraseQuery('salty beers', boost=1.5, analyzer='analyzer', field='field')
        encoded = _encoded(q, opts)
        assert encoded == expected
        _assert_query_repr(q, encoded)

        q = search.MatchPhraseQuery(match_phrase='salty beers', boost=1.5, analyzer='analyzer',
                                    field='field')
        encoded = _encoded(q, opts)
        assert encoded == expected
        _assert_query_repr(q, encoded)

    def test_match_query(self) -> None:
        opts = SearchOptions(limit=10)

        def expected(operator: str) -> Dict[str, Any]:
            return _expected_request({'match': 'salty beers',
                                      'analyzer': 'analyzer',
                                      'boost': 1.5,
                                      'field': 'field',
                                      'fuzziness': 1234,
                                      'prefix_length': 4,
                                      'operator': operator},
                                     limit=10)

        q = search.MatchQuery('salty beers', boost=1.5, analyzer='analyzer', field='field',
                              fuzziness=1234, prefix_length=4, match_operator=MatchOperator.OR)
        encoded = _encoded(q, opts)
        assert encoded == expected('or')
        _assert_query_repr(q, encoded)

        q = search.MatchQuery('salty beers', boost=1.5, analyzer='analyzer', field='field',
                              fuzziness=1234, prefix_length=4, match_operator=MatchOperator.AND)
        encoded = _encoded(q, opts)
        assert encoded == expected('and')
        _assert_query_repr(q, encoded)

        q = search.MatchQuery(match='salty beers', boost=1.5, analyzer='analyzer', field='field',
                              fuzziness=1234, prefix_length=4, match_operator=MatchOperator.AND)
        encoded = _encoded(q, opts)
        assert encoded == expected('and')
        _assert_query_repr(q, encoded)

    def test_numrange_query(self) -> None:
        # At least one bound is required.
        with pytest.raises(ValueError):
            search.NumericRangeQuery()

        # Zero is a bound, not an absent bound.
        bounds: List[Tuple[Dict[str, Any], Dict[str, Any]]]
        bounds = [({'min': 0, 'max': 0}, {'min': 0, 'max': 0}),
                  ({'min': 0.1, 'max': 0.9}, {'min': 0.1, 'max': 0.9}),
                  ({'max': 0.9}, {'max': 0.9}),
                  ({'min': 0.1}, {'min': 0.1})]
        for kwargs, body in bounds:
            q = search.NumericRangeQuery(**kwargs)
            encoded = _encoded(q)
            assert encoded['query'] == body
            _assert_query_repr(q, encoded)

        # min_inclusive/max_inclusive are the deprecated spelling of inclusive_min/inclusive_max.
        inclusive = {'min': 0.1, 'max': 0.9, 'inclusive_min': True, 'inclusive_max': True}
        spellings: List[Dict[str, Any]] = [{'min_inclusive': True, 'max_inclusive': True},
                                           {'inclusive_min': True, 'inclusive_max': True}]
        for kwargs in spellings:
            q = search.NumericRangeQuery(0.1, 0.9, **kwargs)
            encoded = _encoded(q)
            assert encoded['query'] == inclusive
            _assert_query_repr(q, encoded)

    def test_params_base(self) -> None:
        builder = _params(search.TermQuery('someterm'), SearchOptions())
        assert builder.params == _expected_params()

    def test_params_client_context_id(self) -> None:
        builder = _params(search.TermQuery('someterm'), SearchOptions(client_context_id='test-id'))
        assert builder.params == _expected_params(client_context_id='test-id')

    def test_params_disable_scoring(self) -> None:
        warnings.resetwarnings()
        with warnings.catch_warnings(record=True) as caught:
            builder = _params(search.TermQuery('someterm'), SearchOptions(disable_scoring=True))
        assert builder.params == _expected_params(disable_scoring=True)
        assert len(caught) == 1
        assert 'Option disable_scoring is deprecated' in str(caught[0].message)

        # False keeps the default behavior, so it is not a use of the deprecated option.
        warnings.resetwarnings()
        with warnings.catch_warnings(record=True) as caught:
            builder = _params(search.TermQuery('someterm'), SearchOptions(disable_scoring=False))
        assert builder.params == _expected_params(disable_scoring=False)
        assert caught == []

    def test_params_explain(self) -> None:
        builder = _params(search.TermQuery('someterm'), SearchOptions(explain=True))
        assert builder.params == _expected_params(explain=True)

    def test_params_facets(self) -> None:
        # The typed property and the encoded request are checked separately, and the request
        # is where the JSON serialization happens; `params` carries no 'facets' key at all.
        opts = SearchOptions(facets={
            'term': search.TermFacet('somefield', limit=10),
            'dr': search.DateFacet('datefield').add_range('name', 'start', 'end'),
            'nr': search.NumericFacet('numfield').add_range('name2', 0.0, 99.99)
        })
        exp_facets = {
            'term': {'field': 'somefield', 'size': 10},
            'dr': {'field': 'datefield',
                   'date_ranges': [{'name': 'name', 'start': 'start', 'end': 'end'}]},
            'nr': {'field': 'numfield',
                   'numeric_ranges': [{'name': 'name2', 'min': 0.0, 'max': 99.99}]},
        }

        builder = _params(search.TermQuery('someterm'), opts)
        assert builder.params == _expected_params()
        assert {name: facet.encodable for name, facet in builder.facets.items()} == exp_facets
        assert _encode(builder)['facets'] == exp_facets

    def test_params_fields(self) -> None:
        builder = _params(search.TermQuery('someterm'), SearchOptions(fields=['foo', 'bar', 'baz']))
        assert builder.params == _expected_params(fields=['foo', 'bar', 'baz'])

    def test_params_highlight_style(self) -> None:
        builder = _params(search.TermQuery('someterm'),
                          SearchOptions(highlight_style=HighlightStyle.Html))
        assert builder.params == _expected_params(highlight_style=HighlightStyle.Html.value)
        assert builder.highlight_style == HighlightStyle.Html

    def test_params_highlight_style_fields(self) -> None:
        builder = _params(search.TermQuery('someterm'),
                          SearchOptions(highlight_style=HighlightStyle.Ansi,
                                        highlight_fields=['foo', 'bar', 'baz']))
        assert builder.params == _expected_params(highlight_style=HighlightStyle.Ansi.value,
                                                  highlight_fields=['foo', 'bar', 'baz'])
        assert builder.highlight_style == HighlightStyle.Ansi

    def test_params_include_locations(self) -> None:
        builder = _params(search.TermQuery('someterm'), SearchOptions(include_locations=True))
        assert builder.params == _expected_params(include_locations=True)

    def test_params_limit(self) -> None:
        builder = _params(search.TermQuery('someterm'), SearchOptions(limit=10))
        assert builder.params == _expected_params(limit=10)

    def test_params_logging(self) -> None:
        builder = _params(search.TermQuery('someterm'),
                          SearchOptions(log_request=True, log_response=True))
        assert builder.params == _expected_params(log_request=True, log_response=True)

    def test_params_scan_consistency(self) -> None:
        builder = _params(search.TermQuery('someterm'),
                          SearchOptions(scan_consistency=search.SearchScanConsistency.REQUEST_PLUS))
        assert builder.params == _expected_params(
            scan_consistency=search.SearchScanConsistency.REQUEST_PLUS.value)
        assert builder.consistency == search.SearchScanConsistency.REQUEST_PLUS

    def test_params_scope_collections(self) -> None:
        # scope_name is deprecated and never reaches the core, but it does stay in params until
        # the request is handed over, so both the surviving key and the warning are asserted.
        warnings.resetwarnings()
        with warnings.catch_warnings(record=True) as caught:
            builder = _params(search.TermQuery('someterm'),
                              SearchOptions(scope_name='test-scope',
                                            collections=['test-collection-1', 'test-collection-2']))

        assert builder.params == _expected_params(
            scope_name='test-scope',
            collections=['test-collection-1', 'test-collection-2'])
        assert len(caught) == 1
        assert 'The scope_name option is not used by the search API.' in str(caught[0].message)

    def test_params_scoring_disable_scoring_conflict(self) -> None:
        # Both options write the same request field, so only disable_scoring=True conflicts.
        for opts in (SearchOptions(scoring=ReciprocalRankFusion(), disable_scoring=True),
                     SearchOptions(scoring=ScoringNone(), disable_scoring=True)):
            _rejects(_params, search.TermQuery('someterm'), opts)

        builder = _params(search.TermQuery('someterm'),
                          SearchOptions(scoring=ReciprocalRankFusion(), disable_scoring=False))
        assert builder.params == _expected_params(
            scoring={'scoring_type': 'reciprocal_rank_fusion'}, disable_scoring=False)

    def test_params_scoring_invalid(self) -> None:
        # The option takes a SearchScoring, not the encoded dict or a bare strategy name.
        for bad in ('reciprocal_rank_fusion', {'scoring_type': 'none'}, 60):
            _rejects(lambda s: _params(search.TermQuery('someterm'), SearchOptions(scoring=s)), bad)

        # bool is an int subclass, so it has to be rejected explicitly.
        for not_int in (True, 1.5, '60'):
            _rejects(ReciprocalRankFusion, rank_constant=not_int)

        # Both parameters are uint32_t in the core request.
        for out_of_range in (-1, 0xFFFFFFFF + 1):
            _rejects(ReciprocalRankFusion, rank_constant=out_of_range)
            _rejects(RelativeScoreFusion, window_size=out_of_range)
        assert ReciprocalRankFusion(rank_constant=0, window_size=0xFFFFFFFF).as_encodable() == {
            'scoring_type': 'reciprocal_rank_fusion', 'rank_constant': 0, 'window_size': 0xFFFFFFFF}

    def test_params_scoring_matches_generated_bindings(self) -> None:
        # Holds the public scoring types to the generated core binding: each must encode as
        # exactly one alternative of the request's scoring union, using only keys it declares,
        # and every alternative must have a public type.
        alternatives: Dict[Any, Tuple[str, Set[str]]] = {}
        for alt in get_args(get_type_hints(CppSearchRequest)['scoring']):
            # NoneType has no hints, so it contributes nothing here.
            fields = get_type_hints(alt)
            for name, annotation in fields.items():
                if get_origin(annotation) is Literal:
                    for tag in get_args(annotation):
                        alternatives[tag] = (name, set(fields))

        scorings: List[SearchScoring] = [ReciprocalRankFusion(rank_constant=60, window_size=200),
                                         RelativeScoreFusion(window_size=200),
                                         ScoringNone()]
        for scoring in scorings:
            encoded = _params(search.TermQuery('someterm'),
                              SearchOptions(scoring=scoring)).as_encodable()['scoring']
            claimed = [fields for tag, (key, fields) in alternatives.items()
                       if encoded.get(key) == tag]
            assert len(claimed) == 1, (f'{type(scoring).__name__} encodes as {encoded}, which no '
                                       f'generated scoring alternative claims: {alternatives}')
            assert set(encoded) <= claimed[0], (f'{type(scoring).__name__} encodes keys the '
                                                f'generated alternative does not declare: '
                                                f'{set(encoded) - claimed[0]}')

        assert len(alternatives) == len(scorings)

    def test_params_scoring_none(self) -> None:
        builder = _params(search.TermQuery('someterm'), SearchOptions(scoring=ScoringNone()))
        assert builder.params == _expected_params(scoring={'scoring_type': 'none'})

        # The base class is abstract, so a strategy name alone does not make a scoring mode.
        abstract: Callable[..., Any] = SearchScoring
        with pytest.raises(TypeError):
            abstract('none')

    def test_params_scoring_reciprocal_rank_fusion(self) -> None:
        builder = _params(search.TermQuery('someterm'),
                          SearchOptions(scoring=ReciprocalRankFusion(rank_constant=60,
                                                                     window_size=200)))
        assert builder.params == _expected_params(scoring={'scoring_type': 'reciprocal_rank_fusion',
                                                           'rank_constant': 60,
                                                           'window_size': 200})

        # Unset parameters are omitted rather than sent as null.
        builder = _params(search.TermQuery('someterm'),
                          SearchOptions(scoring=ReciprocalRankFusion()))
        assert builder.params == _expected_params(scoring={'scoring_type': 'reciprocal_rank_fusion'})

    def test_params_scoring_relative_score_fusion(self) -> None:
        # Relative score fusion has no rank constant, and omits an unset window size.
        builder = _params(search.TermQuery('someterm'), SearchOptions(scoring=RelativeScoreFusion()))
        assert builder.params == _expected_params(scoring={'scoring_type': 'relative_score_fusion'})

        builder = _params(search.TermQuery('someterm'),
                          SearchOptions(scoring=RelativeScoreFusion(window_size=200)))
        assert builder.params == _expected_params(scoring={'scoring_type': 'relative_score_fusion',
                                                           'window_size': 200})

    def test_params_serializer(self) -> None:
        serializer = DefaultJsonSerializer()
        builder = _params(search.TermQuery('someterm'), SearchOptions(serializer=serializer))
        assert builder.params == _expected_params(serializer=serializer)

    def test_params_show_request(self) -> None:
        builder = _params(search.TermQuery('someterm'), SearchOptions(show_request=True))
        assert builder.params == _expected_params(show_request=True)

    def test_params_skip(self) -> None:
        builder = _params(search.TermQuery('someterm'), SearchOptions(skip=10))
        assert builder.params == _expected_params(skip=10)

    def test_params_sort(self) -> None:
        # `builder.params` carries no 'sort' key either, and each spec is serialized
        # individually rather than the list as a whole.
        specs = ['f1', 'f2', '-_score']
        builder = _params(search.TermQuery('someterm'), SearchOptions(sort=specs))

        assert builder.params == _expected_params()
        assert builder.sort == specs
        assert _encode(builder)['sort_specs'] == specs

    def test_params_sort_invalid(self) -> None:
        # A sort spec is a field name or a Sort; an int is neither, and the builder has to
        # reject the list rather than encode a spec the server will not understand.
        specs: Any = [1, 2]
        with pytest.raises(InvalidArgumentException):
            _params(search.TermQuery('someterm'), SearchOptions(sort=specs))

    def test_params_sort_mixed(self) -> None:
        # The string stays a string and the SortField becomes an object, in one list.
        # Encoding the list as a whole would make both take the form the first element implied.
        # The `sort` type comment is Union[Sequence[str], Sequence[Sort]], which cannot express
        # a mixed list, so the local is Any until sdk_reorg annotates it Sequence[Union[...]].
        specs: Any = ['f1', search.SortScore()]
        builder = _params(search.TermQuery('someterm'), SearchOptions(sort=specs))

        assert _encode(builder)['sort_specs'] == ['f1', {'by': 'score'}]

    def test_params_timeout(self) -> None:
        # A timedelta, a whole number of seconds, and a fractional one: the three input forms,
        # all converging on the same millisecond count, which is the unit the binding reads.
        timeouts: List[Tuple[Any, int]] = [(timedelta(seconds=20), 20000),
                                           (20, 20000),
                                           (25.5, 25500)]
        for timeout, expected_ms in timeouts:
            builder = _params(search.TermQuery('someterm'), SearchOptions(timeout=timeout))
            assert builder.params == _expected_params(timeout=expected_ms)

    def test_params_timeout_getter(self) -> None:
        # Regression guard (PYCBC-1894): the request carries milliseconds and the property
        # reports seconds, so the getter converts rather than handing back what is stored.  The
        # absent case has to stay None rather than becoming zero.
        builder = _params(search.TermQuery('someterm'), SearchOptions(timeout=timedelta(seconds=20)))
        assert builder.timeout == 20

        builder = _params(search.TermQuery('someterm'), SearchOptions())
        assert builder.timeout is None

    def test_params_uninterpretable_enum_option(self) -> None:
        # set_option writes into params without verification, so a value the getter cannot
        # turn into an enum member survives until it is read back.
        builder = _params(search.TermQuery('someterm'), SearchOptions())
        builder.set_option('scan_consistency', 5)
        with pytest.raises(InvalidArgumentException):
            builder.consistency

    def test_phrase_query(self) -> None:
        # Terms are required.
        with pytest.raises(ValueError):
            search.PhraseQuery()

        q = search.PhraseQuery('salty')
        encoded = _encoded(q)
        assert encoded == _expected_request({'terms': ['salty']})
        _assert_query_repr(q, encoded)

        expected = _expected_request({'terms': ['salty', 'beers']})

        q = search.PhraseQuery('salty', 'beers')
        encoded = _encoded(q)
        assert encoded == expected
        _assert_query_repr(q, encoded)

        q = search.PhraseQuery(terms=['salty', 'beers'])
        encoded = _encoded(q)
        assert encoded == expected
        _assert_query_repr(q, encoded)

    def test_prefix_query(self) -> None:
        expected = _expected_request({'prefix': 'someterm', 'boost': 1.5})

        q = search.PrefixQuery('someterm', boost=1.5)
        encoded = _encoded(q)
        assert encoded == expected
        _assert_query_repr(q, encoded)

        q = search.PrefixQuery(prefix='someterm', boost=1.5)
        encoded = _encoded(q)
        assert encoded == expected
        _assert_query_repr(q, encoded)

    def test_query_string_query(self) -> None:
        expected = _expected_request({'query': 'q*ry', 'boost': 2.0}, explain=True, limit=10)
        opts = SearchOptions(limit=10, explain=True)

        q = search.QueryStringQuery('q*ry', boost=2.0)
        encoded = _encoded(q, opts)
        assert encoded == expected
        _assert_query_repr(q, encoded)

        q = search.QueryStringQuery(query='q*ry', boost=2.0)
        encoded = _encoded(q, opts)
        assert encoded == expected
        _assert_query_repr(q, encoded)

    def test_raw_query(self) -> None:
        # No repr assertion: RawQuery passes the body through untouched and does not define
        # __repr__, so str() reports the default object repr rather than the query it carries.
        q = search.RawQuery({'foo': 'bar'})
        assert _encoded(q) == _expected_request({'foo': 'bar'})

    def test_regexp_query(self) -> None:
        expected = _expected_request({'regexp': 'some?regex'})

        q = search.RegexQuery('some?regex')
        encoded = _encoded(q)
        assert encoded == expected
        _assert_query_repr(q, encoded)

        q = search.RegexQuery(regexp='some?regex')
        encoded = _encoded(q)
        assert encoded == expected
        _assert_query_repr(q, encoded)

    def test_term_query(self) -> None:
        expected = _expected_request({'term': 'someterm',
                                      'boost': 1.5,
                                      'fuzziness': 12,
                                      'prefix_length': 23,
                                      'field': 'field'},
                                     explain=True)
        opts = SearchOptions(explain=True)

        q = search.TermQuery('someterm', field='field', boost=1.5, prefix_length=23, fuzziness=12)
        encoded = _encoded(q, opts)
        assert encoded == expected
        _assert_query_repr(q, encoded)

        q = search.TermQuery(term='someterm', field='field', boost=1.5, prefix_length=23,
                             fuzziness=12)
        encoded = _encoded(q, opts)
        assert encoded == expected
        _assert_query_repr(q, encoded)

    def test_termrange_query(self) -> None:
        # At least one bound is required.
        with pytest.raises(ValueError):
            search.TermRangeQuery()

        # `start`/`end` are the deprecated spelling of `min`/`max`, and both reach the request
        # under the current names.  Empty strings are bounds, not absent bounds.
        bounds: List[Tuple[Dict[str, Any], Dict[str, Any]]]
        bounds = [({'start': '', 'end': ''}, {'min': '', 'max': ''}),
                  ({'start': 'startTerm', 'end': 'endTerm'},
                   {'min': 'startTerm', 'max': 'endTerm'}),
                  ({'end': 'endTerm'}, {'max': 'endTerm'}),
                  ({'max': 'endTerm'}, {'max': 'endTerm'}),
                  ({'start': 'startTerm'}, {'min': 'startTerm'}),
                  ({'min': 'startTerm'}, {'min': 'startTerm'})]
        for kwargs, body in bounds:
            q = search.TermRangeQuery(**kwargs)
            encoded = _encoded(q)
            assert encoded['query'] == body
            _assert_query_repr(q, encoded)

        inclusive = {'min': 'startTerm',
                     'max': 'endTerm',
                     'inclusive_min': True,
                     'inclusive_max': True}
        spellings: List[Dict[str, Any]] = [{'start_inclusive': True, 'end_inclusive': True},
                                           {'inclusive_min': True, 'inclusive_max': True}]
        for kwargs in spellings:
            q = search.TermRangeQuery('startTerm', 'endTerm', **kwargs)
            encoded = _encoded(q)
            assert encoded['query'] == inclusive
            _assert_query_repr(q, encoded)

    def test_wildcard_query(self) -> None:
        expected = _expected_request({'wildcard': 'f*o', 'field': 'wc'})

        q = search.WildcardQuery('f*o', field='wc')
        encoded = _encoded(q)
        assert encoded == expected
        _assert_query_repr(q, encoded)

        q = search.WildcardQuery(wildcard='f*o', field='wc')
        encoded = _encoded(q)
        assert encoded == expected
        _assert_query_repr(q, encoded)


class VectorSearchParamsUnitTests:
    # Separate from SearchParamsUnitTests because a vector search is built through
    # SearchRequest rather than by handing a query to the builder.

    TEST_VECTOR = [-0.014653487130999565,
                   -0.008658270351588726,
                   0.017129190266132355,
                   -0.015563474968075752,
                   -0.02059517428278923,
                   0.019551364704966545]

    TEST_VECTOR_BASE64 = 'SSdtIGp1c3QgYSB0ZXN0IHN0cmluZw=='

    def _encoded_request(self, request: SearchRequest) -> Dict[str, Any]:
        builder = search.SearchQueryBuilder.create_search_query_from_request(INDEX_NAME, request)
        return _encode(builder)

    def _expected(self, *vector_queries: Dict[str, Any], **overrides: Any) -> Dict[str, Any]:
        # match_none rather than no query at all: a search request always carries one, so a
        # vector-only search gets one matching nothing and the vector half decides the result.
        return {'query': {'match_none': None},
                'index_name': INDEX_NAME,
                'metrics': True,
                'show_request': False,
                'vector_search': list(vector_queries),
                **overrides}

    def test_search_request_invalid(self) -> None:
        _rejects(SearchRequest, None)
        _rejects(SearchRequest.create, None)

        vector_search = VectorSearch.from_vector_query(VectorQuery('vector_field', self.TEST_VECTOR))
        search_query = search.MatchAllQuery()

        # `with_search_query` takes a SearchQuery, and only while the request carries none;
        # `with_vector_search` takes a VectorSearch on the same terms.  So each rejects the
        # other's kind outright, and rejects its own kind once the request already holds one.
        for half in (vector_search, search_query):
            _rejects(lambda h, o: SearchRequest(h).with_search_query(o), half, vector_search)
            _rejects(lambda h, o: SearchRequest(h).with_vector_search(o), half, search_query)
        _rejects(lambda h, o: SearchRequest(h).with_vector_search(o), vector_search, vector_search)
        _rejects(lambda h, o: SearchRequest(h).with_search_query(o), search_query, search_query)

        # That leaves exactly two legal orderings, and a request may hold both halves at once.
        # Asserting only the rejections cannot tell the rule from a blanket refusal.
        assert isinstance(SearchRequest(vector_search).with_search_query(search_query),
                          SearchRequest)
        assert isinstance(SearchRequest(search_query).with_vector_search(vector_search),
                          SearchRequest)

    def test_vector_query_invalid_boost(self) -> None:
        # boost is a float option; an int is not silently promoted.
        _rejects(VectorQuery, 'vector_field', self.TEST_VECTOR, boost=1)

    def test_vector_query_invalid_num_candidates(self) -> None:
        # cannot be < 1, and must be an int; both are rejected positionally and by keyword
        _rejects(VectorQuery, 'vector_field', self.TEST_VECTOR, 0)
        _rejects(VectorQuery, 'vector_field', self.TEST_VECTOR, num_candidates=0)
        _rejects(VectorQuery, 'vector_field', self.TEST_VECTOR, 3.14159)
        _rejects(VectorQuery, 'vector_field', self.TEST_VECTOR, num_candidates=3.14159)

    def test_vector_query_invalid_vector(self) -> None:
        # cannot be None or empty
        _rejects(VectorQuery, 'vector_field', [])
        _rejects(VectorQuery, 'vector_field', None)
        # a list vector must be all floats, so a bare int is rejected even among floats
        _rejects(VectorQuery, 'vector_field', [1])
        _rejects(VectorQuery, 'vector_field', [1.111, 2, 3.14159])
        # anything that is not a list must be the base64 str form
        _rejects(VectorQuery, 'vector_field', {})
        # the field name is required and must be non-empty
        _rejects(VectorQuery, None, self.TEST_VECTOR)
        _rejects(VectorQuery, '', self.TEST_VECTOR)

    def test_vector_search(self) -> None:
        request = SearchRequest.create(
            VectorSearch.from_vector_query(VectorQuery('vector_field', self.TEST_VECTOR)))
        assert self._encoded_request(request) == self._expected(
            {'field': 'vector_field', 'vector': self.TEST_VECTOR, 'k': 3})

    def test_vector_search_with_prefilter(self) -> None:
        prefilter = search.MatchQuery('salty beers', boost=1.5, analyzer='analyzer', field='field',
                                      fuzziness=1234, prefix_length=4,
                                      match_operator=MatchOperator.OR)
        request = SearchRequest.create(
            VectorSearch.from_vector_query(VectorQuery('vector_field',
                                                       self.TEST_VECTOR,
                                                       prefilter=prefilter)))
        assert self._encoded_request(request) == self._expected(
            {'field': 'vector_field',
             'vector': self.TEST_VECTOR,
             'k': 3,
             'filter': {'match': 'salty beers',
                        'analyzer': 'analyzer',
                        'boost': 1.5,
                        'field': 'field',
                        'fuzziness': 1234,
                        'prefix_length': 4,
                        'operator': 'or'}})

    def test_vector_search_with_scoring(self) -> None:
        request = SearchRequest.create(search.MatchQuery('salty beers')).with_vector_search(
            VectorSearch.from_vector_query(VectorQuery('vector_field', self.TEST_VECTOR)))
        builder = search.SearchQueryBuilder.create_search_query_from_request(
            INDEX_NAME, request, SearchOptions(scoring=ReciprocalRankFusion(rank_constant=60)))
        assert _encode(builder) == self._expected(
            {'field': 'vector_field', 'vector': self.TEST_VECTOR, 'k': 3},
            query={'match': 'salty beers'},
            scoring={'scoring_type': 'reciprocal_rank_fusion', 'rank_constant': 60})

        # The conflict is checked on the request path as well as the query path.
        _rejects(search.SearchQueryBuilder.create_search_query_from_request,
                 INDEX_NAME, request, SearchOptions(scoring=ScoringNone(), disable_scoring=True))

    def test_vector_query_combination_reads_back(self) -> None:
        # PYCBC-1929: the getter read another option's key.  The encoded request always
        # carried the right combination, and the property only answered wrongly once
        # something else had been set, because until then the key it read was absent.
        for combination in (VectorQueryCombination.AND, VectorQueryCombination.OR):
            request = SearchRequest.create(
                VectorSearch([VectorQuery('vector_field', self.TEST_VECTOR)],
                             VectorSearchOptions(vector_query_combination=combination)))
            builder = search.SearchQueryBuilder.create_search_query_from_request(INDEX_NAME,
                                                                                 request)
            assert builder.vector_query_combination == combination

            builder.highlight_style = HighlightStyle.Ansi
            assert builder.vector_query_combination == combination

        absent = search.SearchQueryBuilder.create_search_query_from_request(
            INDEX_NAME,
            SearchRequest.create(VectorSearch.from_vector_query(
                VectorQuery('vector_field', self.TEST_VECTOR))))
        absent.highlight_style = HighlightStyle.Ansi
        assert absent.vector_query_combination is None

    def test_vector_search_base64(self) -> None:
        # A str vector is passed through as base64 under a different key, not decoded here.
        request = SearchRequest.create(
            VectorSearch.from_vector_query(VectorQuery('vector_field', self.TEST_VECTOR_BASE64)))
        assert self._encoded_request(request) == self._expected(
            {'field': 'vector_field', 'vector_base64': self.TEST_VECTOR_BASE64, 'k': 3})

    def test_vector_search_invalid(self) -> None:
        _rejects(VectorSearch, [])
        _rejects(VectorSearch, None)
        # every element must be a VectorQuery, so one stray value rejects the whole list
        _rejects(VectorSearch, [1, VectorQuery('vector_field', self.TEST_VECTOR)])
        _rejects(VectorSearch.from_vector_query, None)
        _rejects(VectorSearch.from_vector_query, 1)

    def test_vector_search_multiple_queries(self) -> None:
        vector_queries = [VectorQuery('vector_field', self.TEST_VECTOR),
                          VectorQuery('vector_field', self.TEST_VECTOR)]
        encoded_query = {'field': 'vector_field', 'vector': self.TEST_VECTOR, 'k': 3}

        # The combination is only written once there is more than one query to combine, and it
        # is the one place a vector search carries a top-level key beyond the query list.
        for combination, expected in [(VectorQueryCombination.AND, 'and'),
                                      (VectorQueryCombination.OR, 'or')]:
            request = SearchRequest.create(
                VectorSearch(vector_queries,
                             VectorSearchOptions(vector_query_combination=combination)))
            assert self._encoded_request(request) == self._expected(
                encoded_query, encoded_query, vector_query_combination=expected)
