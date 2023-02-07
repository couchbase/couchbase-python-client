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


import threading
from copy import copy
from datetime import datetime, timedelta

import pytest

import couchbase.search as search
from couchbase.exceptions import InvalidArgumentException, QueryIndexNotFoundException
from couchbase.mutation_state import MutationState
from couchbase.options import SearchOptions
from couchbase.result import MutationToken
from couchbase.search import (HighlightStyle,
                              MatchOperator,
                              SearchDateRangeFacet,
                              SearchFacetResult,
                              SearchNumericRangeFacet,
                              SearchRow,
                              SearchTermFacet)
from tests.environments import CollectionType
from tests.environments.search_environment import SearchTestEnvironment
from tests.test_features import EnvironmentFeatures


class SearchCollectionTestSuite:
    TEST_MANIFEST = [
        'test_cluster_query_collections',
        'test_scope_query_collections',
        'test_scope_search_fields',
        'test_scope_search_highlight',
        'test_search_query_in_thread',
    ]

    def test_cluster_query_collections(self, cb_env):
        q = search.TermQuery('auto')
        res = cb_env.cluster.search_query(cb_env.TEST_COLLECTION_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10,
                                                        scope_name=cb_env.scope.name,
                                                        collections=[cb_env.collection.name]))
        rows = cb_env.assert_rows(res, 2, return_rows=True)

        collections = list(map(lambda r: r.fields['_$c'], rows))
        assert all([c for c in collections if c == cb_env.collection.name]) is True

    def test_scope_query_collections(self, cb_env):
        q = search.TermQuery('auto')
        res = cb_env.scope.search_query(cb_env.TEST_COLLECTION_INDEX_NAME,
                                        q,
                                        SearchOptions(limit=10, collections=[cb_env.collection.name]))
        rows = cb_env.assert_rows(res, 2, return_rows=True)

        collections = list(map(lambda r: r.fields['_$c'], rows))
        assert all([c for c in collections if c == cb_env.collection.name]) is True

        res = cb_env.scope.search_query(cb_env.TEST_COLLECTION_INDEX_NAME, q, SearchOptions(limit=10))
        rows = cb_env.assert_rows(res, 2, return_rows=True)

        collections = list(map(lambda r: r.fields['_$c'], rows))
        assert all([c for c in collections if c in [cb_env.collection.name, cb_env.OTHER_COLLECTION]]) is True

    def test_scope_search_fields(self, cb_env):
        test_fields = ['make', 'model']
        q = search.TermQuery('auto')
        # verify fields works w/in kwargs
        res = cb_env.scope.search_query(cb_env.TEST_COLLECTION_INDEX_NAME,
                                        q,
                                        SearchOptions(limit=10),
                                        fields=test_fields,
                                        collections=[cb_env.collection.name])

        fields_with_col = copy(test_fields)
        fields_with_col.append('_$c')
        rows = cb_env.assert_rows(res, 1, return_rows=True)
        first_entry = rows[0]
        assert isinstance(first_entry, SearchRow)
        assert isinstance(first_entry.fields, dict)
        assert first_entry.fields != {}
        assert all(map(lambda f: f in fields_with_col, first_entry.fields.keys())) is True
        collections = list(map(lambda r: r.fields['_$c'], rows))
        assert all([c for c in collections if c == cb_env.collection.name]) is True

        # verify fields works w/in options
        res = cb_env.scope.search_query(cb_env.TEST_COLLECTION_INDEX_NAME,
                                        q,
                                        SearchOptions(limit=10,
                                                      fields=test_fields,
                                                      collections=[cb_env.collection.name]))

        rows = cb_env.assert_rows(res, 1, return_rows=True)
        first_entry = rows[0]
        assert isinstance(first_entry, SearchRow)
        assert isinstance(first_entry.fields, dict)
        assert first_entry.fields != {}
        assert all(map(lambda f: f in fields_with_col, first_entry.fields.keys())) is True
        collections = list(map(lambda r: r.fields['_$c'], rows))
        assert all([c for c in collections if c == cb_env.collection.name]) is True

    def test_scope_search_highlight(self, cb_env):

        q = search.TermQuery('auto')
        # check w/in options
        res = cb_env.scope.search_query(cb_env.TEST_COLLECTION_INDEX_NAME,
                                        q,
                                        SearchOptions(limit=10, highlight_style=HighlightStyle.Html))
        rows = cb_env.assert_rows(res, 1, return_rows=True)
        locations = rows[0].locations
        fragments = rows[0].fragments
        assert isinstance(locations, search.SearchRowLocations)
        assert isinstance(fragments, dict)
        assert all(map(lambda l: isinstance(l, search.SearchRowLocation), locations.get_all())) is True
        collections = list(map(lambda r: r.fields['_$c'], rows))
        assert all([c for c in collections if c == cb_env.collection.name]) is True

        # check w/in kwargs
        res = cb_env.scope.search_query(cb_env.TEST_COLLECTION_INDEX_NAME, q, SearchOptions(
            limit=10), highlight_style=HighlightStyle.Html, collections=[cb_env.collection.name])
        rows = cb_env.assert_rows(res, 1, return_rows=True)
        locations = rows[0].locations
        fragments = rows[0].fragments
        assert isinstance(locations, search.SearchRowLocations)
        assert isinstance(fragments, dict)
        assert all(map(lambda l: isinstance(l, search.SearchRowLocation), locations.get_all())) is True
        collections = list(map(lambda r: r.fields['_$c'], rows))
        assert all([c for c in collections if c == cb_env.collection.name]) is True

    def test_search_query_in_thread(self, cb_env):
        results = [None]

        def run_test(scope, search_idx, assert_fn, results):
            try:
                q = search.TermQuery('auto')
                result = scope.search_query(search_idx, q, SearchOptions(limit=10))
                assert_fn(result, 2)
                assert result.metadata() is not None
            except AssertionError:
                results[0] = False
            except Exception as ex:
                results[0] = ex
            else:
                results[0] = True

        t = threading.Thread(target=run_test,
                             args=(cb_env.scope, cb_env.TEST_COLLECTION_INDEX_NAME, cb_env.assert_rows, results))
        t.start()
        t.join()

        assert len(results) == 1
        assert results[0] is True


class SearchParamTestSuite:
    TEST_MANIFEST = [
        'test_boolean_query',
        'test_booleanfield_query',
        'test_conjunction_query',
        'test_consistent_with',
        'test_daterange_query',
        'test_disjunction_query',
        'test_docid_query',
        'test_facets',
        'test_match_all_query',
        'test_match_none_query',
        'test_match_phrase',
        'test_match_query',
        'test_numrange_query',
        'test_params_base',
        'test_params_client_context_id',
        'test_params_disable_scoring',
        'test_params_explain',
        'test_params_facets',
        'test_params_fields',
        'test_params_highlight_style',
        'test_params_highlight_style_fields',
        'test_params_include_locations',
        'test_params_limit',
        'test_params_scan_consistency',
        'test_params_scope_collections',
        'test_params_serializer',
        'test_params_skip',
        'test_params_sort',
        'test_params_timeout',
        'test_phrase_query',
        'test_prefix_query',
        'test_raw_query',
        'test_regexp_query',
        'test_string_query',
        'test_term_search',
        'test_termrange_query',
        'test_wildcard_query',
    ]

    @pytest.fixture(scope='class')
    def base_query_opts(self):
        return search.TermQuery('someterm'), {'metrics': True}

    def test_boolean_query(self, cb_env):
        prefix_q = search.PrefixQuery('someterm', boost=2)
        bool_q = search.BooleanQuery(
            must=prefix_q, must_not=prefix_q, should=prefix_q)
        exp = {'prefix': 'someterm', 'boost': 2.0}
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, bool_q
        )
        encoded_q = cb_env.get_encoded_query(search_query)

        conjuncts = {
            'conjuncts': [exp]
        }
        disjuncts = {
            'disjuncts': [exp],
            'min': 1
        }
        assert encoded_q['query']['must'] == conjuncts
        assert encoded_q['query']['must_not'] == disjuncts
        assert encoded_q['query']['should'] == disjuncts

        # Test multiple criteria in must and must_not
        pq_1 = search.PrefixQuery('someterm', boost=2)
        pq_2 = search.PrefixQuery('otherterm')
        bool_q = search.BooleanQuery(must=[pq_1, pq_2])
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, bool_q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        conjuncts = {
            'conjuncts': [
                {'prefix': 'someterm', 'boost': 2.0},
                {'prefix': 'otherterm'}
            ]
        }
        assert encoded_q['query']['must'] == conjuncts

    def test_booleanfield_query(self, cb_env):
        exp_json = {
            'query': {
                'bool': True
            },
            'index_name': cb_env.TEST_INDEX_NAME
        }
        q = search.BooleanFieldQuery(True)
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert exp_json == encoded_q

    def test_conjunction_query(self, cb_env):
        q = search.ConjunctionQuery()
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        with pytest.raises(search.NoChildrenException):
            _ = cb_env.get_encoded_query(search_query)

        conjuncts = {
            'conjuncts': [{'prefix': 'somePrefix'}],
        }
        q.conjuncts.append(search.PrefixQuery('somePrefix'))
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert encoded_q['query'] == conjuncts

    def test_consistent_with(self, cb_env):
        q = search.TermQuery('someterm')

        ms = MutationState()
        mt = MutationToken(token={
            'partition_id': 42,
            'partition_uuid': 3004,
            'sequence_number': 3,
            'bucket_name': 'default'
        })
        ms._add_scanvec(mt)
        opts = SearchOptions(consistent_with=ms)
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )

        # couchbase++ will set scan_consistency, so params should be
        # None, but the prop should return AT_PLUS
        assert search_query.params.get('scan_consistency', None) is None
        assert search_query.consistency == search.SearchScanConsistency.AT_PLUS

        q_mt = search_query.params.get('mutation_state', None)
        assert isinstance(q_mt, list)
        assert len(q_mt) == 1
        assert q_mt[0] == mt

    def test_daterange_query(self, cb_env):
        with pytest.raises(TypeError):
            q = search.DateRangeQuery()

        q = search.DateRangeQuery(end='theEnd')
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert encoded_q['query'] == {'end': 'theEnd'}

        q = search.DateRangeQuery(start='theStart')
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert encoded_q['query'] == {'start': 'theStart'}

        q = search.DateRangeQuery(start='theStart', end='theEnd')
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert encoded_q['query'] == {'start': 'theStart', 'end': 'theEnd'}

        q = search.DateRangeQuery('', '')  # Empty strings should be ok
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert encoded_q['query'] == {'start': '', 'end': ''}

    def test_disjunction_query(self, cb_env):
        q = search.DisjunctionQuery()
        assert q.min == 1
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        with pytest.raises(search.NoChildrenException):
            _ = cb_env.get_encoded_query(search_query)

        disjuncts = {
            'disjuncts': [{'prefix': 'somePrefix'}],
            'min': 1
        }
        q.disjuncts.append(search.PrefixQuery('somePrefix'))
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert encoded_q['query'] == disjuncts

        with pytest.raises(InvalidArgumentException):
            q.min = 0

        q.min = 2
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        with pytest.raises(InvalidArgumentException):
            _ = cb_env.get_encoded_query(search_query)

    def test_docid_query(self, cb_env):
        q = search.DocIdQuery([])
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        with pytest.raises(search.NoChildrenException):
            _ = cb_env.get_encoded_query(search_query)

        exp_json = {
            'query': {
                'ids': ['foo', 'bar', 'baz']
            },
            'index_name': cb_env.TEST_INDEX_NAME
        }

        q.ids = ['foo', 'bar', 'baz']
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert exp_json == encoded_q

    def test_facets(self, cb_env):
        q = search.TermQuery('someterm')

        f = search.NumericFacet('numfield')
        with pytest.raises(InvalidArgumentException):
            f.add_range('range1')

        opts = SearchOptions()
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )
        with pytest.raises(InvalidArgumentException):
            search_query.facets['facetName'] = f

        search_query.facets['facetName'] = f.add_range('range1', min=123, max=321)
        assert 'facetName' in search_query.facets

        f = search.DateFacet('datefield')
        f.add_range('r1', start='2012', end='2013')
        f.add_range('r2', start='2014')
        f.add_range('r3', end='2015')
        opts = SearchOptions(facets={'facetName': f})
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )

        exp = {
            'field': 'datefield',
            'date_ranges': [
                {'name': 'r1', 'start': '2012', 'end': '2013'},
                {'name': 'r2', 'start': '2014'},
                {'name': 'r3', 'end': '2015'}
            ]
        }
        encoded_facets = {}
        for name, facet in search_query.facets.items():
            encoded_facets[name] = facet.encodable

        assert encoded_facets['facetName'] == exp
        # self.assertEqual(exp, f.encodable)

        f = search.TermFacet('termfield')
        f.limit = 10
        opts = SearchOptions(facets={'facetName': f})
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )
        encoded_facets = {}
        for name, facet in search_query.facets.items():
            encoded_facets[name] = facet.encodable

        assert encoded_facets['facetName'] == {'field': 'termfield', 'size': 10}

    def test_match_all_query(self, cb_env):
        exp_json = {
            'query': {
                'match_all': None
            },
            'index_name': cb_env.TEST_INDEX_NAME
        }
        q = search.MatchAllQuery()
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert exp_json == encoded_q

    def test_match_none_query(self, cb_env):
        exp_json = {
            'query': {
                'match_none': None
            },
            'index_name': cb_env.TEST_INDEX_NAME
        }
        q = search.MatchNoneQuery()
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert exp_json == encoded_q

    def test_match_phrase(self, cb_env):
        exp_json = {
            'query': {
                'match_phrase': 'salty beers',
                'analyzer': 'analyzer',
                'boost': 1.5,
                'field': 'field'
            },
            'limit': 10,
            'index_name': cb_env.TEST_INDEX_NAME
        }

        q = search.MatchPhraseQuery('salty beers', boost=1.5, analyzer='analyzer',
                                    field='field')
        opts = search.SearchOptions(limit=10)

        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )

        encoded_q = cb_env.get_encoded_query(search_query)

        assert exp_json == encoded_q

    def test_match_query(self, cb_env):
        exp_json = {
            'query': {
                'match': 'salty beers',
                'analyzer': 'analyzer',
                'boost': 1.5,
                'field': 'field',
                'fuzziness': 1234,
                'prefix_length': 4,
                'operator': 'or'
            },
            'limit': 10,
            'index_name': cb_env.TEST_INDEX_NAME
        }

        q = search.MatchQuery('salty beers', boost=1.5, analyzer='analyzer',
                              field='field', fuzziness=1234, prefix_length=4, match_operator=MatchOperator.OR)
        opts = search.SearchOptions(limit=10)

        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert exp_json == encoded_q

        exp_json["query"]["operator"] = "and"

        q = search.MatchQuery('salty beers', boost=1.5, analyzer='analyzer',
                              field='field', fuzziness=1234, prefix_length=4, match_operator=MatchOperator.AND)
        opts = search.SearchOptions(limit=10)

        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert exp_json == encoded_q

    def test_numrange_query(self, cb_env):
        with pytest.raises(TypeError):
            q = search.NumericRangeQuery()

        q = search.NumericRangeQuery(0, 0)  # Should be OK
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert encoded_q['query'] == {'min': 0, 'max': 0}

        q = search.NumericRangeQuery(0.1, 0.9)
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert encoded_q['query'] == {'min': 0.1, 'max': 0.9}

        q = search.NumericRangeQuery(max=0.9)
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert encoded_q['query'] == {'max': 0.9}

        q = search.NumericRangeQuery(min=0.1)
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert encoded_q['query'] == {'min': 0.1}

    def test_params_base(self, cb_env, base_query_opts):
        q, base_opts = base_query_opts
        opts = SearchOptions()
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )

        assert search_query.params == base_opts

    def test_params_client_context_id(self, cb_env, base_query_opts):
        q, base_opts = base_query_opts
        opts = SearchOptions(client_context_id='test-id')
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )
        exp_opts = base_opts.copy()
        exp_opts['client_context_id'] = 'test-id'
        assert search_query.params == exp_opts

    def test_params_disable_scoring(self, cb_env, base_query_opts):
        q, base_opts = base_query_opts
        opts = SearchOptions(disable_scoring=True)
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )
        exp_opts = base_opts.copy()
        exp_opts['disable_scoring'] = True
        assert search_query.params == exp_opts

    def test_params_explain(self, cb_env, base_query_opts):
        q, base_opts = base_query_opts
        opts = SearchOptions(explain=True)
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )
        exp_opts = base_opts.copy()
        exp_opts['explain'] = True
        assert search_query.params == exp_opts

    def test_params_facets(self, cb_env, base_query_opts):
        q, base_opts = base_query_opts
        opts = SearchOptions(facets={
            'term': search.TermFacet('somefield', limit=10),
            'dr': search.DateFacet('datefield').add_range('name', 'start', 'end'),
            'nr': search.NumericFacet('numfield').add_range('name2', 0.0, 99.99)
        })
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )
        exp_opts = base_opts.copy()
        exp_opts['facets'] = {
            'term': {
                'field': 'somefield',
                'size': 10
            },
            'dr': {
                'field': 'datefield',
                'date_ranges': [{
                    'name': 'name',
                    'start': 'start',
                    'end': 'end'
                }]
            },
            'nr': {
                'field': 'numfield',
                'numeric_ranges': [{
                    'name': 'name2',
                    'min': 0.0,
                    'max': 99.99
                }]
            },
        }

        params = search_query.params
        # handle encoded here
        encoded_facets = {}
        for name, facet in search_query.facets.items():
            encoded_facets[name] = facet.encodable
        params['facets'] = encoded_facets
        assert params == exp_opts

    def test_params_fields(self, cb_env, base_query_opts):
        q, base_opts = base_query_opts
        opts = SearchOptions(fields=['foo', 'bar', 'baz'])
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )
        exp_opts = base_opts.copy()
        exp_opts['fields'] = ['foo', 'bar', 'baz']
        assert search_query.params == exp_opts

    def test_params_highlight_style(self, cb_env, base_query_opts):
        q, base_opts = base_query_opts
        opts = SearchOptions(highlight_style=HighlightStyle.Html)
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )
        exp_opts = base_opts.copy()
        exp_opts['highlight_style'] = HighlightStyle.Html.value
        assert search_query.params == exp_opts
        assert search_query.highlight_style == HighlightStyle.Html

    def test_params_highlight_style_fields(self, cb_env, base_query_opts):
        q, base_opts = base_query_opts
        opts = SearchOptions(highlight_style=HighlightStyle.Ansi, highlight_fields=['foo', 'bar', 'baz'])
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )
        exp_opts = base_opts.copy()
        exp_opts['highlight_style'] = HighlightStyle.Ansi.value
        exp_opts['highlight_fields'] = ['foo', 'bar', 'baz']
        assert search_query.params == exp_opts
        assert search_query.highlight_style == HighlightStyle.Ansi

    def test_params_include_locations(self, cb_env, base_query_opts):
        q, base_opts = base_query_opts
        opts = SearchOptions(include_locations=True)
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )
        exp_opts = base_opts.copy()
        exp_opts['include_locations'] = True
        assert search_query.params == exp_opts

    def test_params_limit(self, cb_env, base_query_opts):
        q, base_opts = base_query_opts
        opts = SearchOptions(limit=10)
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )
        exp_opts = base_opts.copy()
        exp_opts['limit'] = 10
        assert search_query.params == exp_opts

    def test_params_scan_consistency(self, cb_env, base_query_opts):
        q, base_opts = base_query_opts
        opts = SearchOptions(scan_consistency=search.SearchScanConsistency.REQUEST_PLUS)
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )
        exp_opts = base_opts.copy()
        exp_opts['scan_consistency'] = search.SearchScanConsistency.REQUEST_PLUS.value
        assert search_query.params == exp_opts
        assert search_query.consistency == search.SearchScanConsistency.REQUEST_PLUS

    def test_params_scope_collections(self, cb_env, base_query_opts):
        q, base_opts = base_query_opts
        opts = SearchOptions(scope_name='test-scope', collections=['test-collection-1', 'test-collection-2'])
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )
        exp_opts = base_opts.copy()
        exp_opts['scope_name'] = 'test-scope'
        exp_opts['collections'] = ['test-collection-1', 'test-collection-2']
        assert search_query.params == exp_opts

    def test_params_serializer(self, cb_env, base_query_opts):
        q, base_opts = base_query_opts
        from couchbase.serializer import DefaultJsonSerializer

        # serializer
        serializer = DefaultJsonSerializer()
        opts = SearchOptions(serializer=serializer)
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )

        exp_opts = base_opts.copy()
        exp_opts['serializer'] = serializer
        assert search_query.params == exp_opts

    def test_params_skip(self, cb_env, base_query_opts):
        q, base_opts = base_query_opts
        opts = SearchOptions(skip=10)
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )
        exp_opts = base_opts.copy()
        exp_opts['skip'] = 10
        assert search_query.params == exp_opts

    def test_params_sort(self, cb_env, base_query_opts):
        q, base_opts = base_query_opts
        opts = SearchOptions(sort=['f1', 'f2', '-_score'])
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )
        exp_opts = base_opts.copy()
        exp_opts['sort'] = ['f1', 'f2', '-_score']
        params = search_query.params
        params['sort'] = search_query.sort
        assert params == exp_opts

    def test_params_timeout(self, cb_env, base_query_opts):
        q, base_opts = base_query_opts
        opts = SearchOptions(timeout=timedelta(seconds=20))
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )
        exp_opts = base_opts.copy()
        exp_opts['timeout'] = int(timedelta(seconds=20).total_seconds() * 1e6)
        assert search_query.params == exp_opts

        opts = SearchOptions(timeout=20)
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 20000000
        assert search_query.params == exp_opts

        opts = SearchOptions(timeout=25.5)
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 25500000
        assert search_query.params == exp_opts

    def test_phrase_query(self, cb_env):
        exp_json = {
            'query': {
                'terms': ['salty', 'beers']
            },
            'index_name': cb_env.TEST_INDEX_NAME
        }
        q = search.PhraseQuery('salty', 'beers')
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert exp_json == encoded_q

        q = search.PhraseQuery()
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        with pytest.raises(search.NoChildrenException):
            _ = cb_env.get_encoded_query(search_query)

        q.terms.append('salty')
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        exp_json['query']['terms'] = ['salty']
        assert exp_json == encoded_q

    def test_prefix_query(self, cb_env):
        exp_json = {
            'query': {
                'prefix': 'someterm',
                'boost': 1.5
            },
            'index_name': cb_env.TEST_INDEX_NAME
        }
        q = search.PrefixQuery('someterm', boost=1.5)
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert exp_json == encoded_q

    def test_raw_query(self, cb_env):
        exp_json = {
            'query': {
                'foo': 'bar'
            },
            'index_name': cb_env.TEST_INDEX_NAME
        }
        q = search.RawQuery({'foo': 'bar'})
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert exp_json == encoded_q

    def test_regexp_query(self, cb_env):
        exp_json = {
            'query': {
                'regex': 'some?regex'
            },
            'index_name': cb_env.TEST_INDEX_NAME
        }
        q = search.RegexQuery('some?regex')
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert exp_json == encoded_q

    def test_string_query(self, cb_env):
        exp_json = {
            'query': {
                'query': 'q*ry',
                'boost': 2.0,
            },
            'explain': True,
            'limit': 10,
            'index_name': cb_env.TEST_INDEX_NAME
        }
        q = search.QueryStringQuery('q*ry', boost=2.0)
        opts = search.SearchOptions(limit=10, explain=True)
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert exp_json == encoded_q

    def test_term_search(self, cb_env):
        q = search.TermQuery('someterm', field='field', boost=1.5,
                             prefix_length=23, fuzziness=12)
        opts = search.SearchOptions(explain=True)

        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q, opts
        )

        encoded_q = cb_env.get_encoded_query(search_query)

        exp_json = {
            'query': {
                'term': 'someterm',
                'boost': 1.5,
                'fuzziness': 12,
                'prefix_length': 23,
                'field': 'field'
            },
            'index_name': cb_env.TEST_INDEX_NAME,
            'explain': True
        }

        assert exp_json == encoded_q

    def test_termrange_query(self, cb_env):
        with pytest.raises(TypeError):
            q = search.TermRangeQuery()

        q = search.TermRangeQuery('', '')  # Should be OK
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert encoded_q['query'] == {'start': '', 'end': ''}

        q = search.TermRangeQuery('startTerm', 'endTerm')
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert encoded_q['query'] == {'start': 'startTerm', 'end': 'endTerm'}

        q = search.TermRangeQuery(end='endTerm')
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert encoded_q['query'] == {'end': 'endTerm'}

        q = search.TermRangeQuery(start='startTerm')
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert encoded_q['query'] == {'start': 'startTerm'}

    def test_wildcard_query(self, cb_env):
        exp_json = {
            'query': {
                'wildcard': 'f*o',
                'field': 'wc',
            },
            'index_name': cb_env.TEST_INDEX_NAME
        }
        q = search.WildcardQuery('f*o', field='wc')
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert exp_json == encoded_q


class SearchTestSuite:
    TEST_MANIFEST = [
        'test_bad_search_query',
        'test_cluster_search',
        'test_cluster_search_date_facets',
        'test_cluster_search_disable_scoring',
        'test_cluster_search_facets_fail',
        'test_cluster_search_fields',
        'test_cluster_search_highlight',
        'test_cluster_search_numeric_facets',
        'test_cluster_search_scan_consistency',
        'test_cluster_search_term_facets',
        'test_cluster_search_top_level_facets',
        'test_cluster_search_top_level_facets_kwargs',
        'test_cluster_sort_field',
        'test_cluster_sort_field_multi',
        'test_cluster_sort_geo',
        'test_cluster_sort_id',
        'test_cluster_sort_score',
        'test_cluster_sort_str',
        'test_search_include_locations',
        'test_search_match_operator',
        'test_search_match_operator_fail',
        'test_search_no_include_locations',
        'test_search_query_in_thread',
        'test_search_raw_query',
    ]

    @pytest.fixture(scope="class")
    def check_disable_scoring_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('search_disable_scoring',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    def test_bad_search_query(self, cb_env):
        res = cb_env.cluster.search_query('not-an-index', search.TermQuery('auto'))
        with pytest.raises(QueryIndexNotFoundException):
            [r for r in res]

    def test_cluster_search(self, cb_env):
        q = search.TermQuery('auto')
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME, q, SearchOptions(limit=10))
        cb_env.assert_rows(res, 2)

    def test_cluster_search_date_facets(self, cb_env):
        facet_name = 'last_updated'
        facet = search.DateFacet('last_updated', limit=3)
        now = datetime.now()
        today = datetime(year=now.year, month=now.month, day=now.day)
        early_end = (today - timedelta(days=241))
        mid_start = (today - timedelta(days=240))
        mid_end = (today - timedelta(days=121))
        late_start = (today - timedelta(days=120))
        facet.add_range('early', end=f"{early_end.strftime('%Y-%m-%dT%H:%M:%SZ')}")
        facet.add_range('mid', start=f"{mid_start.strftime('%Y-%m-%dT%H:%M:%SZ')}",
                        end=f"{mid_end.strftime('%Y-%m-%dT%H:%M:%SZ')}")
        facet.add_range('late', start=f"{late_start.strftime('%Y-%m-%dT%H:%M:%SZ')}")
        q = search.TermQuery('auto')
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, facets={facet_name: facet}))

        cb_env.assert_rows(res, 1)
        facets = res.facets()
        assert isinstance(facets, dict)
        result_facet = facets[facet_name]
        assert isinstance(result_facet, SearchFacetResult)
        assert result_facet.name == facet_name
        assert result_facet.field == facet_name
        assert all(map(lambda ft: isinstance(ft, SearchDateRangeFacet), result_facet.date_ranges)) is True
        assert len(result_facet.date_ranges) <= facet.limit

    @pytest.mark.usefixtures('check_disable_scoring_supported')
    def test_cluster_search_disable_scoring(self, cb_env):

        # verify disable scoring works w/in SearchOptions
        q = search.TermQuery('auto')
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, disable_scoring=True))
        rows = cb_env.assert_rows(res, 1, return_rows=True)
        assert all(map(lambda r: r.score == 0, rows)) is True

        # verify disable scoring works w/in kwargs
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10), disable_scoring=True)
        rows = cb_env.assert_rows(res, 1, return_rows=True)
        assert all(map(lambda r: r.score == 0, rows)) is True

        # verify setting disable_scoring to False works
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, disable_scoring=False))
        rows = cb_env.assert_rows(res, 1, return_rows=True)
        assert all(map(lambda r: r.score != 0, rows)) is True

        # verify default disable_scoring is False
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10))
        rows = cb_env.assert_rows(res, 1, return_rows=True)
        assert all(map(lambda r: r.score != 0, rows)) is True

    # @TODO: 3.x raises a SearchException...
    def test_cluster_search_facets_fail(self, cb_env):
        q = search.TermQuery('auto')
        with pytest.raises(ValueError):
            cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                        q,
                                        SearchOptions(limit=10, facets={'test-facet': None}))

    def test_cluster_search_fields(self, cb_env):
        test_fields = ['make', 'mode']
        q = search.TermQuery('auto')
        # verify fields works w/in kwargs
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10), fields=test_fields)

        rows = cb_env.assert_rows(res, 1, return_rows=True)
        first_entry = rows[0]
        assert isinstance(first_entry, SearchRow)
        assert isinstance(first_entry.fields, dict)
        assert first_entry.fields != {}
        assert all(map(lambda f: f in test_fields, first_entry.fields.keys())) is True

        # verify fields works w/in options
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, fields=test_fields))

        rows = cb_env.assert_rows(res, 1, return_rows=True)
        first_entry = rows[0]
        assert isinstance(first_entry, SearchRow)
        assert isinstance(first_entry.fields, dict)
        assert first_entry.fields != {}
        assert all(map(lambda f: f in test_fields, first_entry.fields.keys())) is True

    def test_cluster_search_highlight(self, cb_env):

        q = search.TermQuery('auto')
        # check w/in options
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, highlight_style=HighlightStyle.Html))
        rows = cb_env.assert_rows(res, 1, return_rows=True)
        locations = rows[0].locations
        fragments = rows[0].fragments
        assert isinstance(locations, search.SearchRowLocations)
        assert isinstance(fragments, dict)
        assert all(map(lambda l: isinstance(l, search.SearchRowLocation), locations.get_all())) is True

        # check w/in options
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME, q, SearchOptions(
            limit=10), highlight_style=HighlightStyle.Html)
        rows = cb_env.assert_rows(res, 1, return_rows=True)
        locations = rows[0].locations
        fragments = rows[0].fragments
        assert isinstance(locations, search.SearchRowLocations)
        assert isinstance(fragments, dict)
        assert all(map(lambda l: isinstance(l, search.SearchRowLocation), locations.get_all())) is True

    def test_cluster_search_numeric_facets(self, cb_env):

        facet_name = 'rating'
        facet = search.NumericFacet('rating', limit=3)
        facet.add_range('low', max=4)
        facet.add_range('med', min=4, max=7)
        facet.add_range('high', min=7)
        q = search.TermQuery('auto')
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, facets={facet_name: facet}))

        cb_env.assert_rows(res, 1)
        facets = res.facets()
        assert isinstance(facets, dict)
        result_facet = facets[facet_name]
        assert isinstance(result_facet, SearchFacetResult)
        assert result_facet.name == facet_name
        assert result_facet.field == facet_name
        assert all(map(lambda ft: isinstance(ft, SearchNumericRangeFacet), result_facet.numeric_ranges)) is True
        assert len(result_facet.numeric_ranges) <= facet.limit

    def test_cluster_search_term_facets(self, cb_env):

        facet_name = 'model'
        facet = search.TermFacet('model', 5)
        q = search.TermQuery('auto')
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, facets={facet_name: facet}))

        cb_env.assert_rows(res, 1)
        facets = res.facets()
        assert isinstance(facets, dict)
        result_facet = facets[facet_name]
        assert isinstance(result_facet, SearchFacetResult)
        assert result_facet.name == facet_name
        assert result_facet.field == facet_name
        assert all(map(lambda ft: isinstance(ft, SearchTermFacet), result_facet.terms)) is True
        assert len(result_facet.terms) <= facet.limit

    def test_cluster_search_scan_consistency(self, cb_env):
        q = search.TermQuery('auto')
        # check w/in options
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10,
                                                        scan_consistency=search.SearchScanConsistency.NOT_BOUNDED))
        cb_env.assert_rows(res, 1)

        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10,
                                                        scan_consistency=search.SearchScanConsistency.REQUEST_PLUS))
        cb_env.assert_rows(res, 1)

        with pytest.raises(InvalidArgumentException):
            cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                        q,
                                        SearchOptions(limit=10,
                                                      scan_consistency=search.SearchScanConsistency.AT_PLUS))

    def test_cluster_search_top_level_facets(self, cb_env):
        # if the facet limit is omitted, the details of the facets will not be provided
        # (i.e. SearchFacetResult.terms is None,
        #       SearchFacetResult.numeric_ranges is None and SearchFacetResult.date_ranges is None)
        facet_name = 'model'
        facet = search.TermFacet('model')
        q = search.TermQuery('auto')
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, facets={facet_name: facet}))

        cb_env.assert_rows(res, 1)
        facets = res.facets()
        assert isinstance(facets, dict)
        result_facet = facets[facet_name]
        assert isinstance(result_facet, SearchFacetResult)
        assert result_facet.name == facet_name
        assert result_facet.field == facet_name
        assert result_facet.terms is None
        assert result_facet.numeric_ranges is None
        assert result_facet.date_ranges is None

        facet_name = 'rating'
        facet = search.NumericFacet('rating')
        facet.add_range('low', max=4)
        facet.add_range('med', min=4, max=7)
        facet.add_range('high', min=7)
        q = search.TermQuery('auto')
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, facets={facet_name: facet}))

        cb_env.assert_rows(res, 1)
        facets = res.facets()
        assert isinstance(facets, dict)
        result_facet = facets[facet_name]
        assert isinstance(result_facet, SearchFacetResult)
        assert result_facet.name == facet_name
        assert result_facet.field == facet_name
        assert result_facet.terms is None
        assert result_facet.numeric_ranges is None
        assert result_facet.date_ranges is None

    def test_cluster_search_top_level_facets_kwargs(self, cb_env):
        # if the facet limit is omitted, the details of the facets will not be provided
        # (i.e. SearchFacetResult.terms is None,
        #       SearchFacetResult.numeric_ranges is None and SearchFacetResult.date_ranges is None)
        facet_name = 'model'
        facet = search.TermFacet('model')
        q = search.TermQuery('auto')
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10), facets={facet_name: facet})

        cb_env.assert_rows(res, 1)
        facets = res.facets()
        assert isinstance(facets, dict)
        result_facet = facets[facet_name]
        assert isinstance(result_facet, SearchFacetResult)
        assert result_facet.name == facet_name
        assert result_facet.field == facet_name
        assert result_facet.terms is None
        assert result_facet.numeric_ranges is None
        assert result_facet.date_ranges is None

        facet_name = 'rating'
        facet = search.NumericFacet('rating')
        facet.add_range('low', max=4)
        facet.add_range('med', min=4, max=7)
        facet.add_range('high', min=7)
        q = search.TermQuery('auto')
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10), facets={facet_name: facet})

        cb_env.assert_rows(res, 1)
        facets = res.facets()
        assert isinstance(facets, dict)
        result_facet = facets[facet_name]
        assert isinstance(result_facet, SearchFacetResult)
        assert result_facet.name == facet_name
        assert result_facet.field == facet_name
        assert result_facet.terms is None
        assert result_facet.numeric_ranges is None
        assert result_facet.date_ranges is None

    def test_cluster_sort_field(self, cb_env):
        sort_field = "rating"
        q = search.TermQuery('auto')
        # field - ascending
        sort = search.SortField(field=sort_field, type="number", mode="min", missing="last")
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=[sort], fields=[sort_field]))

        rows = cb_env.assert_rows(res, 1, return_rows=True)
        rating = rows[0].fields[sort_field]
        for row in rows[1:]:
            assert row.fields[sort_field] >= rating
            rating = row.fields[sort_field]

        # field - descending
        sort.desc = True
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=[sort], fields=[sort_field]))

        rows = cb_env.assert_rows(res, 1, return_rows=True)
        rating = rows[0].fields[sort_field]
        for row in rows[1:]:
            assert rating >= row.fields[sort_field]
            rating = row.fields[sort_field]

    def test_cluster_sort_field_multi(self, cb_env):
        sort_fields = [
            search.SortField(field="rating", type="number",
                             mode="min", missing="last"),
            search.SortField(field="last_updated", type="number",
                             mode="min", missing="last"),
            search.SortScore(),
        ]
        sort_field_names = ["rating", "last_updated"]
        q = search.TermQuery('auto')
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=sort_fields, fields=sort_field_names))
        cb_env.assert_rows(res, 1)

        sort_fields = [
            search.SortField(field="rating", type="number",
                             mode="min", missing="last", desc=True),
            search.SortField(field="last_updated", type="number",
                             mode="min", missing="last"),
            search.SortScore(desc=True),
        ]
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=sort_fields, fields=sort_field_names))
        cb_env.assert_rows(res, 1)

        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=["rating", "last_udpated", "-_score"]))
        cb_env.assert_rows(res, 1)

    def test_cluster_sort_geo(self, cb_env):
        # @TODO:  better confirmation on results?
        sort_field = "geo"
        q = search.TermQuery('auto')
        # geo - ascending
        sort = search.SortGeoDistance(field=sort_field, location=(37.7749, 122.4194), unit="meters")
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=[sort], fields=[sort_field]))
        cb_env.assert_rows(res, 1)

        # geo - descending
        sort.desc = True
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=[sort], fields=[sort_field]))
        cb_env.assert_rows(res, 1)

    def test_cluster_sort_id(self, cb_env):
        q = search.TermQuery('auto')
        # score - ascending
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=[search.SortID()]))
        rows = cb_env.assert_rows(res, 1, return_rows=True)

        id = rows[0].id
        for row in rows[1:]:
            assert row.id >= id
            id = row.id

        # score - descending
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=[search.SortID(desc=True)]))
        rows = cb_env.assert_rows(res, 1, return_rows=True)

        id = rows[0].id
        for row in rows[1:]:
            assert id >= row.id
            id = row.id

    def test_cluster_sort_score(self, cb_env):
        q = search.TermQuery('auto')
        # score - ascending
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=[search.SortScore()]))
        rows = cb_env.assert_rows(res, 1, return_rows=True)

        score = rows[0].score
        for row in rows[1:]:
            assert row.score >= score
            score = row.score

        # score - descending
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=[search.SortScore(desc=True)]))
        rows = cb_env.assert_rows(res, 1, return_rows=True)

        score = rows[0].score
        for row in rows[1:]:
            assert score >= row.score
            score = row.score

    def test_cluster_sort_str(self, cb_env):
        q = search.TermQuery('auto')
        # score - ascending
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=['_score']))
        rows = cb_env.assert_rows(res, 1, return_rows=True)
        score = rows[0].score
        for row in rows[1:]:
            assert row.score >= score
            score = row.score

        # score - descending
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=['-_score']))
        rows = cb_env.assert_rows(res, 1, return_rows=True)
        score = rows[0].score
        for row in rows[1:]:
            assert score >= row.score
            score = row.score

    def test_search_include_locations(self, cb_env):
        q = search.TermQuery('auto')
        # check w/in options
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, include_locations=True))
        rows = cb_env.assert_rows(res, 1, return_rows=True)
        locations = rows[0].locations
        assert isinstance(locations, search.SearchRowLocations)
        assert all(map(lambda l: isinstance(l, search.SearchRowLocation), locations.get_all())) is True

        # check w/in kwargs
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10), include_locations=True)
        rows = cb_env.assert_rows(res, 1, return_rows=True)
        locations = rows[0].locations
        assert isinstance(locations, search.SearchRowLocations)
        assert all(map(lambda l: isinstance(l, search.SearchRowLocation), locations.get_all())) is True

    @pytest.mark.parametrize('operator, query_terms, expect_rows',
                             [(search.MatchOperator.AND, "auto deal", True),
                              (search.MatchOperator.AND, "auto :random:", False),
                              (search.MatchOperator.OR, "auto deal", True),
                              (search.MatchOperator.OR, "auto :random:", True)])
    def test_search_match_operator(self, cb_env, operator, query_terms, expect_rows):
        import random
        import string

        random_query_term = "".join(random.choice(string.ascii_letters)
                                    for _ in range(10))

        if ':random:' in query_terms:
            query_terms.replace(':random:', random_query_term)

        q = search.MatchQuery(query_terms, match_operator=operator)

        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME, q, limit=10)
        rows = cb_env.assert_rows(res, 0, return_rows=True)

        if expect_rows:
            assert len(rows) > 0
        else:
            assert len(rows) == 0

    def test_search_match_operator_fail(self, cb_env):
        with pytest.raises(ValueError):
            q = search.MatchQuery('auto deal', match_operator='NOT')
            cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME, q, limit=10)

    # @TODO(PYCBC-1296):  DIFF between 3.x and 4.x, locations returns None
    def test_search_no_include_locations(self, cb_env):
        q = search.TermQuery('auto')
        # check w/in options
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, include_locations=False))
        rows = cb_env.assert_rows(res, 1, return_rows=True)
        locations = rows[0].locations
        assert locations is None

        # check w/in kwargs
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10), include_locations=False)
        rows = cb_env.assert_rows(res, 1, return_rows=True)
        locations = rows[0].locations
        assert locations is None

    def test_search_query_in_thread(self, cb_env):
        results = [None]

        def run_test(cluster, search_idx, assert_fn, results):
            try:
                q = search.TermQuery('auto')
                result = cluster.search_query(search_idx, q, SearchOptions(limit=10))
                assert_fn(result, 2)
                assert result.metadata() is not None
            except AssertionError:
                results[0] = False
            except Exception as ex:
                results[0] = ex
            else:
                results[0] = True

        t = threading.Thread(target=run_test,
                             args=(cb_env.cluster, cb_env.TEST_INDEX_NAME, cb_env.assert_rows, results))
        t.start()
        t.join()

        assert len(results) == 1
        assert results[0] is True

    def test_search_raw_query(self, cb_env):
        query_args = {"match": "auto deal",
                      "fuzziness": 2, "operator": "and"}
        q = search.RawQuery(query_args)
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME, q, limit=10)
        cb_env.assert_rows(res, 1)


class ClassicSearchCollectionTests(SearchCollectionTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicSearchCollectionTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicSearchCollectionTests) if valid_test_method(meth)]
        compare = set(SearchCollectionTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = SearchTestEnvironment.from_environment(cb_base_env)
        cb_env.enable_search_mgmt()
        cb_env.setup(request.param)
        yield cb_env
        cb_env.teardown(request.param)


class ClassicSearchParamTests(SearchParamTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicSearchParamTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicSearchParamTests) if valid_test_method(meth)]
        compare = set(SearchParamTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = SearchTestEnvironment.from_environment(cb_base_env)
        cb_env.enable_search_mgmt()
        cb_env.setup(request.param, test_suite=self.__class__.__name__)
        yield cb_env
        cb_env.teardown(request.param, test_suite=self.__class__.__name__)


class ClassicSearchTests(SearchTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicSearchTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicSearchTests) if valid_test_method(meth)]
        compare = set(SearchTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = SearchTestEnvironment.from_environment(cb_base_env)
        cb_env.enable_search_mgmt()
        cb_env.setup(request.param)
        yield cb_env
        cb_env.teardown(request.param)
