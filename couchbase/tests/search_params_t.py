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

import warnings
from datetime import timedelta

import pytest

import couchbase.search as search
from couchbase.exceptions import InvalidArgumentException
from couchbase.mutation_state import MutationState
from couchbase.options import SearchOptions
from couchbase.result import MutationToken
from couchbase.search import HighlightStyle, MatchOperator
from tests.environments import CollectionType
from tests.environments.search_environment import SearchTestEnvironment


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
            'index_name': cb_env.TEST_INDEX_NAME,
            'metrics': True,
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
        assert q_mt[0] == mt.as_dict()

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
            'index_name': cb_env.TEST_INDEX_NAME,
            'metrics': True,
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
            'index_name': cb_env.TEST_INDEX_NAME,
            'metrics': True,
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
            'index_name': cb_env.TEST_INDEX_NAME,
            'metrics': True,
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
            'index_name': cb_env.TEST_INDEX_NAME,
            'metrics': True,
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
            'index_name': cb_env.TEST_INDEX_NAME,
            'metrics': True,
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
        caught_warnings = []
        warnings.resetwarnings()
        with warnings.catch_warnings(record=True) as ws:
            search_query = search.SearchQueryBuilder.create_search_query_object(
                cb_env.TEST_INDEX_NAME, q, opts
            )
            caught_warnings = ws
        exp_opts = base_opts.copy()
        # We still need to check for the scope_name option until we actually remove the option.
        # The option is deprecated and not sent to the C++ client, but it still in the search_query.params
        # until we send the params over to the C++ client.
        exp_opts['scope_name'] = 'test-scope'
        exp_opts['collections'] = ['test-collection-1', 'test-collection-2']
        assert search_query.params == exp_opts
        assert len(caught_warnings) == 1
        assert 'The scope_name option is not used by the search API.' in caught_warnings[0].message.args[0]

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
            'index_name': cb_env.TEST_INDEX_NAME,
            'metrics': True,
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
            'index_name': cb_env.TEST_INDEX_NAME,
            'metrics': True,
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
            'index_name': cb_env.TEST_INDEX_NAME,
            'metrics': True,
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
                'regexp': 'some?regex'
            },
            'index_name': cb_env.TEST_INDEX_NAME,
            'metrics': True,
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
            'index_name': cb_env.TEST_INDEX_NAME,
            'metrics': True,
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
            'explain': True,
            'metrics': True,
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
            'index_name': cb_env.TEST_INDEX_NAME,
            'metrics': True,
        }
        q = search.WildcardQuery('f*o', field='wc')
        search_query = search.SearchQueryBuilder.create_search_query_object(
            cb_env.TEST_INDEX_NAME, q
        )
        encoded_q = cb_env.get_encoded_query(search_query)
        assert exp_json == encoded_q


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
        cb_env.setup(request.param, test_suite=self.__class__.__name__)
        yield cb_env
        cb_env.teardown(request.param, test_suite=self.__class__.__name__)
