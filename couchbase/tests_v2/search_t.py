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

# Two suite classes over two different FTS topologies, which is why this is not one class
# parametrized on CollectionStrategy: `SearchTestSuite` runs DEFAULT against one
# bucket-wide index, `SearchCollectionTestSuite` runs NAMED over a collection-aware index
# spanning two collections, which is what lets a scope-level query filter on
# `collections=[...]`.  Setup for both is in tests_v2/environments/search_environment.py.
#
# Row assertions come from `SearchEnvironment`, not from `cb_env`, and the choice between
# assert_rows_exact and assert_rows_at_least is not a style question: the bucket-wide index
# sees every `type: vehicle` document in a shared bucket, so `exact` is used only where the
# test wrote the document it is looking for, or expects to find nothing.
#
# `search()` is given a SearchRequest and `search_query()` a bare query, each the argument
# its own signature documents; the builder accepts a bare query from either, so passing one
# to both would drive the two parametrizations down a single code path.
#
# Where a search error surfaces differs by api.  Sync and acouchbase build a lazy request,
# so a missing index raises on iteration; Twisted executes at call time and errbacks, so it
# raises from the call.  A test asserting on that puts the call inside the raises block.

from __future__ import annotations

import random
import string
import threading
import uuid
from collections.abc import Callable, Generator
from datetime import datetime, timedelta
from typing import Any, cast

import pytest

import couchbase.search as search
from couchbase.exceptions import InvalidArgumentException, QueryIndexNotFoundException
from couchbase.mutation_state import MutationState
from couchbase.options import SearchOptions
from couchbase.result import SearchResult
from couchbase.search import (HighlightStyle,
                              SearchDateRangeFacet,
                              SearchFacetResult,
                              SearchNumericRangeFacet,
                              SearchRow,
                              SearchRowLocation,
                              SearchRowLocations,
                              SearchTermFacet)
from tests_v2.environments.managers import TestEnvironmentScopeSearchIndexManager, TestEnvironmentSearchIndexManager
from tests_v2.environments.search_environment import SearchEnvironment
from tests_v2.environments.test_environment import (CollectionStrategy,
                                                    DocType,
                                                    SuiteRequirements,
                                                    SyncTestEnvironment,
                                                    build_environment,
                                                    teardown_environment)
from tests_v2.manifest import validate_test_manifest
from tests_v2.test_features import EnvironmentFeatures, ServerFeatures


class SearchCollectionTestSuite(SuiteRequirements):
    # The scope-level manager, not the cluster-level one: a scope-level query resolves only
    # indexes created in that scope, so an index of the same name created through
    # cluster.search_indexes() answers `index not found` rather than returning no rows.
    MANAGERS = [TestEnvironmentScopeSearchIndexManager]
    COLLECTION_STRATEGY = CollectionStrategy.NAMED
    # The collection manager arrives on its own: setup() auto-registers it for NAMED.
    # DATA_SET is declared rather than inherited because the index maps the document type
    # `vehicle`, so the dependency is on the corpus itself and not just on there being one.
    DATA_SET = [DocType.VEHICLE]
    # Scope-level search is 7.6.  Narrower than the setup_search gate, and the DEFAULT suite
    # below runs happily without it.
    REQUIRED_FEATURES = [ServerFeatures.ScopeSearch]

    TEST_MANIFEST = [
        'test_scope_query_collections',
        'test_scope_search_fields',
        'test_scope_search_highlight',
        'test_search_query_in_thread',
    ]

    @pytest.mark.parametrize('query_op', ['search_query', 'search'])
    def test_scope_query_collections(self, cb_env: SyncTestEnvironment, query_op: str) -> None:
        idx = SearchEnvironment.index_name(SearchCollectionTestSuite.COLLECTION_STRATEGY)
        q = search.TermQuery('auto')

        opts = SearchOptions(limit=10, collections=[cb_env.collection.name])
        if query_op == 'search':
            res = cb_env.scope.search(idx, search.SearchRequest(q), opts)
        else:
            res = cb_env.scope.search_query(idx, q, opts)
        rows = SearchEnvironment.assert_rows_at_least(res, 2)
    # `_$c` comes back without being asked for: it is the collection name FTS stamps on every
    # row of a collection-aware index.  A generator over every row, because the
    # `all([c for c in cols if c == name])` idiom passes when nothing matched at all.
        assert all(r.fields['_$c'] == cb_env.collection.name for r in rows)

        opts = SearchOptions(limit=10)
        if query_op == 'search':
            res = cb_env.scope.search(idx, search.SearchRequest(q), opts)
        else:
            res = cb_env.scope.search_query(idx, q, opts)
        rows = SearchEnvironment.assert_rows_at_least(res, 2)
        both = [cb_env.collection.name, SearchEnvironment.OTHER_COLLECTION]
        assert all(r.fields['_$c'] in both for r in rows)

    @pytest.mark.parametrize('query_op', ['search_query', 'search'])
    def test_scope_search_fields(self, cb_env: SyncTestEnvironment, query_op: str) -> None:
        idx = SearchEnvironment.index_name(SearchCollectionTestSuite.COLLECTION_STRATEGY)
        q = search.TermQuery('auto')
        test_fields = ['make', 'model']
        fields_with_col = test_fields + ['_$c']

        opts = SearchOptions(limit=10)
        if query_op == 'search':
            res = cb_env.scope.search(idx, search.SearchRequest(q), opts,
                                      fields=test_fields, collections=[cb_env.collection.name])
        else:
            res = cb_env.scope.search_query(idx, q, opts,
                                            fields=test_fields, collections=[cb_env.collection.name])
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        first_entry = rows[0]
        assert isinstance(first_entry, SearchRow)
        assert isinstance(first_entry.fields, dict)
        assert first_entry.fields != {}
        assert all(f in fields_with_col for f in first_entry.fields.keys())
        assert all(r.fields['_$c'] == cb_env.collection.name for r in rows)

        opts = SearchOptions(limit=10, fields=test_fields, collections=[cb_env.collection.name])
        if query_op == 'search':
            res = cb_env.scope.search(idx, search.SearchRequest(q), opts)
        else:
            res = cb_env.scope.search_query(idx, q, opts)
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        first_entry = rows[0]
        assert isinstance(first_entry, SearchRow)
        assert isinstance(first_entry.fields, dict)
        assert first_entry.fields != {}
        assert all(f in fields_with_col for f in first_entry.fields.keys())
        assert all(r.fields['_$c'] == cb_env.collection.name for r in rows)

    @pytest.mark.parametrize('query_op', ['search_query', 'search'])
    def test_scope_search_highlight(self, cb_env: SyncTestEnvironment, query_op: str) -> None:
        idx = SearchEnvironment.index_name(SearchCollectionTestSuite.COLLECTION_STRATEGY)
        q = search.TermQuery('auto')

        opts = SearchOptions(limit=10, highlight_style=HighlightStyle.Html)
        if query_op == 'search':
            res = cb_env.scope.search(idx, search.SearchRequest(q), opts)
        else:
            res = cb_env.scope.search_query(idx, q, opts)
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        locations = rows[0].locations
        assert isinstance(locations, SearchRowLocations)
        assert isinstance(rows[0].fragments, dict)
        assert all(isinstance(loc, SearchRowLocation) for loc in locations.get_all())
        # Membership, not equality: this query carries no `collections` filter, so the
        # scope's other collection is a legitimate source for a row.
        both = [cb_env.collection.name, SearchEnvironment.OTHER_COLLECTION]
        assert all(r.fields['_$c'] in both for r in rows)

        opts = SearchOptions(limit=10)
        if query_op == 'search':
            res = cb_env.scope.search(idx, search.SearchRequest(q), opts,
                                      highlight_style=HighlightStyle.Html,
                                      collections=[cb_env.collection.name])
        else:
            res = cb_env.scope.search_query(idx, q, opts,
                                            highlight_style=HighlightStyle.Html,
                                            collections=[cb_env.collection.name])
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        locations = rows[0].locations
        assert isinstance(locations, SearchRowLocations)
        assert isinstance(rows[0].fragments, dict)
        assert all(isinstance(loc, SearchRowLocation) for loc in locations.get_all())
        assert all(r.fields['_$c'] == cb_env.collection.name for r in rows)

    # Driven by a plain threading.Thread, so it has no async analogue: acouchbase and
    # txcouchbase objects are bound to an event loop (a reactor under Twisted), and calling into
    # them from a foreign thread exercises the loop, not the SDK.
    @pytest.mark.parametrize('query_op', ['search_query', 'search'])
    @pytest.mark.testgen_exclude('acouchbase', 'txcouchbase',
                                 reason='drives a threading.Thread; no async analogue')
    def test_search_query_in_thread(self, cb_env: SyncTestEnvironment, query_op: str) -> None:
        results: list[Any] = [None]

        def run_test(cb_env: SyncTestEnvironment,
                     query_op: str,
                     search_idx: str,
                     assert_fn: Callable[[SearchResult, int], list[SearchRow]],
                     results: list[Any]) -> None:
            try:
                q = search.TermQuery('auto')
                opts = SearchOptions(limit=10)
                if query_op == 'search':
                    result = cb_env.scope.search(search_idx, search.SearchRequest(q), opts)
                else:
                    result = cb_env.scope.search_query(search_idx, q, opts)
                assert_fn(result, 2)
                assert result.metadata() is not None
            except AssertionError:
                results[0] = False
            except Exception as ex:
                results[0] = ex
            else:
                results[0] = True

        idx = SearchEnvironment.index_name(SearchCollectionTestSuite.COLLECTION_STRATEGY)
        t = threading.Thread(target=run_test,
                             args=(cb_env, query_op, idx, SearchEnvironment.assert_rows_at_least, results))
        t.start()
        t.join()

        assert len(results) == 1
        assert results[0] is True


class SearchTestSuite(SuiteRequirements):
    MANAGERS = [TestEnvironmentSearchIndexManager]
    COLLECTION_STRATEGY = CollectionStrategy.DEFAULT
    # Declared for the same reason as the collection suite above: the index maps the document
    # type `vehicle`, so the corpus, not just its size, is what this suite depends on.
    DATA_SET = [DocType.VEHICLE]

    TEST_MANIFEST = [
        'test_bad_search_query',
        'test_cluster_search',
        'test_cluster_search_date_facets',
        'test_cluster_search_disable_scoring',
        'test_cluster_search_facets_fail',
        'test_cluster_search_fields',
        'test_cluster_search_highlight',
        'test_cluster_search_numeric_facets',
        'test_cluster_search_ryow',
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

    @pytest.fixture(scope='class')
    def check_disable_scoring_supported(self, cb_env: SyncTestEnvironment) -> None:
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.SearchDisableScoring,
                                                       cb_env.server_context)

    def test_bad_search_query(self, cb_env: SyncTestEnvironment) -> None:
        # The call is inside the raises block on purpose: sync and acouchbase raise on
        # iteration, Twisted from the call.  The drain is the environment helper rather than
        # a comprehension so the same statement reads correctly under an async iterator.
        with pytest.raises(QueryIndexNotFoundException):
            res = cb_env.cluster.search_query('not-an-index', search.TermQuery('auto'))
            SearchEnvironment.assert_rows_exact(res, 0)

    def test_cluster_search(self, cb_env: SyncTestEnvironment) -> None:
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        q = search.TermQuery('auto')
        # PYCBC-1572 added show_request, log_request and log_response to SearchOptions.  Their
        # effect is on the log rather than on the result, so this test can only prove they are
        # accepted and do not change the rows.  Run it under PYCBC_LOG_LEVEL=info with output
        # captured (pytest --capture=tee-sys) to see them work: the log should carry SEARCH:
        # and SEARCH RESPONSE: lines, and the JSON on the response line should hold the
        # request JSON.
        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10,
                                                                show_request=True,
                                                                log_request=True,
                                                                log_response=True))
        SearchEnvironment.assert_rows_at_least(res, 2)

    def test_cluster_search_date_facets(self, cb_env: SyncTestEnvironment) -> None:
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        facet_name = 'last_updated'
        # The limit is held as a local and asserted against, rather than read back off the
        # facet: Facet.limit is Optional[int] because a facet may carry no limit at all, and
        # what this test means to compare against is the number it asked for.
        facet_limit = 3
        facet = search.DateFacet('last_updated', limit=facet_limit)
        now = datetime.now()
        today = datetime(year=now.year, month=now.month, day=now.day)
        early_end = today - timedelta(days=241)
        mid_start = today - timedelta(days=240)
        mid_end = today - timedelta(days=121)
        late_start = today - timedelta(days=120)
        facet.add_range('early', end=f"{early_end.strftime('%Y-%m-%dT%H:%M:%SZ')}")
        facet.add_range('mid',
                        start=f"{mid_start.strftime('%Y-%m-%dT%H:%M:%SZ')}",
                        end=f"{mid_end.strftime('%Y-%m-%dT%H:%M:%SZ')}")
        facet.add_range('late', start=f"{late_start.strftime('%Y-%m-%dT%H:%M:%SZ')}")
        q = search.TermQuery('auto')
        opts = SearchOptions(limit=10, facets={facet_name: facet})
        res = cb_env.cluster.search_query(idx, q, opts)

        SearchEnvironment.assert_rows_at_least(res, 1)
        facets = res.facets()
        assert isinstance(facets, dict)
        result_facet = facets[facet_name]
        assert isinstance(result_facet, SearchFacetResult)
        assert result_facet.name == facet_name
        assert result_facet.field == facet_name
        assert all(isinstance(ft, SearchDateRangeFacet) for ft in result_facet.date_ranges)
        assert len(result_facet.date_ranges) <= facet_limit

    @pytest.mark.usefixtures('check_disable_scoring_supported')
    def test_cluster_search_disable_scoring(self, cb_env: SyncTestEnvironment) -> None:
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        q = search.TermQuery('auto')

        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10, disable_scoring=True))
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        assert all(r.score == 0 for r in rows)

        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10), disable_scoring=True)
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        assert all(r.score == 0 for r in rows)

        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10, disable_scoring=False))
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        assert all(r.score != 0 for r in rows)

        # disable_scoring defaults to False
        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10))
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        assert all(r.score != 0 for r in rows)

    def test_cluster_search_facets_fail(self, cb_env: SyncTestEnvironment) -> None:
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        q = search.TermQuery('auto')
        # Raised while the options are validated, before any request leaves the client, so this
        # is a plain synchronous raise in all three apis.
        with pytest.raises(ValueError):
            bad_facets = cast('dict[str, search.Facet]', {'test-facet': None})
            cb_env.cluster.search_query(idx, q, SearchOptions(limit=10, facets=bad_facets))

    def test_cluster_search_fields(self, cb_env: SyncTestEnvironment) -> None:
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        # 'model', and the typo 'mode' is worth guarding against: it is not an indexed field,
        # so the server returns 'make' alone and the `f in test_fields` assertion below passes
        # over a result missing half of what was asked for.
        test_fields = ['make', 'model']
        q = search.TermQuery('auto')

        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10), fields=test_fields)
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        first_entry = rows[0]
        assert isinstance(first_entry, SearchRow)
        assert isinstance(first_entry.fields, dict)
        assert first_entry.fields != {}
        assert all(f in test_fields for f in first_entry.fields.keys())

        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10, fields=test_fields))
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        first_entry = rows[0]
        assert isinstance(first_entry, SearchRow)
        assert isinstance(first_entry.fields, dict)
        assert first_entry.fields != {}
        assert all(f in test_fields for f in first_entry.fields.keys())

    def test_cluster_search_highlight(self, cb_env: SyncTestEnvironment) -> None:
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        q = search.TermQuery('auto')

        opts = SearchOptions(limit=10, highlight_style=HighlightStyle.Html)
        res = cb_env.cluster.search_query(idx, q, opts)
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        locations = rows[0].locations
        assert isinstance(locations, SearchRowLocations)
        assert isinstance(rows[0].fragments, dict)
        assert all(isinstance(loc, SearchRowLocation) for loc in locations.get_all())

        opts = SearchOptions(limit=10)
        res = cb_env.cluster.search_query(idx, q, opts, highlight_style=HighlightStyle.Html)
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        locations = rows[0].locations
        assert isinstance(locations, SearchRowLocations)
        assert isinstance(rows[0].fragments, dict)
        assert all(isinstance(loc, SearchRowLocation) for loc in locations.get_all())

    def test_cluster_search_numeric_facets(self, cb_env: SyncTestEnvironment) -> None:
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        facet_name = 'rating'
        facet_limit = 3
        facet = search.NumericFacet('rating', limit=facet_limit)
        facet.add_range('low', max=4)
        facet.add_range('med', min=4, max=7)
        facet.add_range('high', min=7)
        q = search.TermQuery('auto')
        opts = SearchOptions(limit=10, facets={facet_name: facet})
        res = cb_env.cluster.search_query(idx, q, opts)

        SearchEnvironment.assert_rows_at_least(res, 1)
        facets = res.facets()
        assert isinstance(facets, dict)
        result_facet = facets[facet_name]
        assert isinstance(result_facet, SearchFacetResult)
        assert result_facet.name == facet_name
        assert result_facet.field == facet_name
        assert all(isinstance(ft, SearchNumericRangeFacet) for ft in result_facet.numeric_ranges)
        assert len(result_facet.numeric_ranges) <= facet_limit

    def test_cluster_search_ryow(self, cb_env: SyncTestEnvironment) -> None:
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.SearchConsistentWith,
                                                       cb_env.server_context)
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)

        # Exact, not at_least: the term is a fresh uuid fragment, so the only document that
        # can match is the one this test writes, and an at-least assertion of zero rows is
        # no assertion at all.
        key, value = cb_env.get_new_doc()
        content = str(uuid.uuid4())[:8]
        value['description'] = content
        q = search.TermQuery(content)
        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10))
        SearchEnvironment.assert_rows_exact(res, 0)

        mut_res = cb_env.collection.insert(key, value)
        ms = MutationState()
        ms.add_mutation_token(mut_res.mutation_token())
        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10, consistent_with=ms))
        SearchEnvironment.assert_rows_exact(res, 1)

        # Regression guard (PYCBC-1477): building the MutationState from the result object
        # itself, rather than from its token, used to be able to crash the SDK.
        key, value = cb_env.get_new_doc()
        content = str(uuid.uuid4())[:8]
        value['description'] = content
        q = search.TermQuery(content)
        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10))
        SearchEnvironment.assert_rows_exact(res, 0)

        mut_res = cb_env.collection.insert(key, value)
        ms = MutationState(mut_res)
        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10, consistent_with=ms))
        SearchEnvironment.assert_rows_exact(res, 1)

    def test_cluster_search_term_facets(self, cb_env: SyncTestEnvironment) -> None:
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        facet_name = 'model'
        facet_limit = 5
        facet = search.TermFacet('model', facet_limit)
        q = search.TermQuery('auto')
        opts = SearchOptions(limit=10, facets={facet_name: facet})
        res = cb_env.cluster.search_query(idx, q, opts)

        SearchEnvironment.assert_rows_at_least(res, 1)
        facets = res.facets()
        assert isinstance(facets, dict)
        result_facet = facets[facet_name]
        assert isinstance(result_facet, SearchFacetResult)
        assert result_facet.name == facet_name
        assert result_facet.field == facet_name
        assert all(isinstance(ft, SearchTermFacet) for ft in result_facet.terms)
        assert len(result_facet.terms) <= facet_limit

    def test_cluster_search_scan_consistency(self, cb_env: SyncTestEnvironment) -> None:
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        q = search.TermQuery('auto')
        res = cb_env.cluster.search_query(
            idx, q, SearchOptions(limit=10, scan_consistency=search.SearchScanConsistency.NOT_BOUNDED))
        SearchEnvironment.assert_rows_at_least(res, 1)

        res = cb_env.cluster.search_query(
            idx, q, SearchOptions(limit=10, scan_consistency=search.SearchScanConsistency.REQUEST_PLUS))
        SearchEnvironment.assert_rows_at_least(res, 1)

        # AT_PLUS is rejected client-side: consistent_with is the only way to express it.
        with pytest.raises(InvalidArgumentException):
            cb_env.cluster.search_query(
                idx, q, SearchOptions(limit=10, scan_consistency=search.SearchScanConsistency.AT_PLUS))

    def test_cluster_search_top_level_facets(self, cb_env: SyncTestEnvironment) -> None:
        # With the facet limit omitted the server returns the facet without its details, so
        # terms, numeric_ranges and date_ranges all come back None.
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        q = search.TermQuery('auto')

        facet_name = 'model'
        facet: search.Facet = search.TermFacet('model')
        opts = SearchOptions(limit=10, facets={facet_name: facet})
        res = cb_env.cluster.search_query(idx, q, opts)
        SearchEnvironment.assert_rows_at_least(res, 1)
        self._assert_facet_without_details(res, facet_name)

        facet_name = 'rating'
        facet = search.NumericFacet('rating')
        facet.add_range('low', max=4)
        facet.add_range('med', min=4, max=7)
        facet.add_range('high', min=7)
        opts = SearchOptions(limit=10, facets={facet_name: facet})
        res = cb_env.cluster.search_query(idx, q, opts)
        SearchEnvironment.assert_rows_at_least(res, 1)
        self._assert_facet_without_details(res, facet_name)

    def test_cluster_search_top_level_facets_kwargs(self, cb_env: SyncTestEnvironment) -> None:
        # Same as test_cluster_search_top_level_facets, with the facets passed as a kwarg
        # alongside a SearchOptions rather than inside it.
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        q = search.TermQuery('auto')

        facet_name = 'model'
        facet: search.Facet = search.TermFacet('model')
        opts = SearchOptions(limit=10)
        res = cb_env.cluster.search_query(idx, q, opts, facets={facet_name: facet})
        SearchEnvironment.assert_rows_at_least(res, 1)
        self._assert_facet_without_details(res, facet_name)

        facet_name = 'rating'
        facet = search.NumericFacet('rating')
        facet.add_range('low', max=4)
        facet.add_range('med', min=4, max=7)
        facet.add_range('high', min=7)
        opts = SearchOptions(limit=10)
        res = cb_env.cluster.search_query(idx, q, opts, facets={facet_name: facet})
        SearchEnvironment.assert_rows_at_least(res, 1)
        self._assert_facet_without_details(res, facet_name)

    def test_cluster_sort_field(self, cb_env: SyncTestEnvironment) -> None:
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        sort_field = 'rating'
        q = search.TermQuery('auto')

        sort = search.SortField(field=sort_field, type='number', mode='min', missing='last')
        # The list is annotated, and the spec object is reused rather than rebuilt.  Both are
        # deliberate: SearchOptions declares sort as List[str] | List[Sort], and list is
        # invariant, so a bare [SortField(...)] literal is a list[SortField] and does not match.
        sort_specs: list[search.Sort] = [sort]
        opts = SearchOptions(limit=10, sort=sort_specs, fields=[sort_field])
        res = cb_env.cluster.search_query(idx, q, opts)
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        rating = rows[0].fields[sort_field]
        for row in rows[1:]:
            assert row.fields[sort_field] >= rating
            rating = row.fields[sort_field]

        sort.desc = True
        opts = SearchOptions(limit=10, sort=sort_specs, fields=[sort_field])
        res = cb_env.cluster.search_query(idx, q, opts)
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        rating = rows[0].fields[sort_field]
        for row in rows[1:]:
            assert rating >= row.fields[sort_field]
            rating = row.fields[sort_field]

    def test_cluster_sort_field_multi(self, cb_env: SyncTestEnvironment) -> None:
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        sort_field_names = ['rating', 'last_updated']
        q = search.TermQuery('auto')

        sort_fields: list[search.Sort] = [
            search.SortField(field='rating', type='number', mode='min', missing='last'),
            search.SortField(field='last_updated', type='number', mode='min', missing='last'),
            search.SortScore(),
        ]
        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10,
                                                                sort=sort_fields,
                                                                fields=sort_field_names))
        SearchEnvironment.assert_rows_at_least(res, 1)

        sort_fields = [
            search.SortField(field='rating', type='number', mode='min', missing='last', desc=True),
            search.SortField(field='last_updated', type='number', mode='min', missing='last'),
            search.SortScore(desc=True),
        ]
        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10,
                                                                sort=sort_fields,
                                                                fields=sort_field_names))
        SearchEnvironment.assert_rows_at_least(res, 1)

        # 'last_updated', and the transposition 'last_udpated' is worth guarding against: a
        # sort on a field the index does not hold is accepted and ignored by the server, so the
        # typo costs the string-form sort spec its only multi-field coverage silently.
        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10,
                                                                sort=['rating', 'last_updated', '-_score']))
        SearchEnvironment.assert_rows_at_least(res, 1)

    def test_cluster_sort_geo(self, cb_env: SyncTestEnvironment) -> None:
        # This asserts that a SortGeoDistance round-trips and the server accepts it, and no
        # more: nothing in the corpus is geo-sortable, since the vehicle documents carry
        # coordinates at `manufacturer.geo` as lat/ln rather than the lat/lon an FTS
        # geopoint needs, and the index maps neither path.
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        sort_field = 'geo'
        q = search.TermQuery('auto')

        sort = search.SortGeoDistance(field=sort_field, location=(37.7749, 122.4194), unit='meters')
        sort_specs: list[search.Sort] = [sort]
        opts = SearchOptions(limit=10, sort=sort_specs, fields=[sort_field])
        res = cb_env.cluster.search_query(idx, q, opts)
        SearchEnvironment.assert_rows_at_least(res, 1)

        sort.desc = True
        opts = SearchOptions(limit=10, sort=sort_specs, fields=[sort_field])
        res = cb_env.cluster.search_query(idx, q, opts)
        SearchEnvironment.assert_rows_at_least(res, 1)

    def test_cluster_sort_id(self, cb_env: SyncTestEnvironment) -> None:
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        q = search.TermQuery('auto')

        sort_specs: list[search.Sort] = [search.SortID()]
        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10, sort=sort_specs))
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        doc_id = rows[0].id
        for row in rows[1:]:
            assert row.id >= doc_id
            doc_id = row.id

        sort_specs = [search.SortID(desc=True)]
        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10, sort=sort_specs))
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        doc_id = rows[0].id
        for row in rows[1:]:
            assert doc_id >= row.id
            doc_id = row.id

    def test_cluster_sort_score(self, cb_env: SyncTestEnvironment) -> None:
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        q = search.TermQuery('auto')

        sort_specs: list[search.Sort] = [search.SortScore()]
        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10, sort=sort_specs))
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        score = rows[0].score
        for row in rows[1:]:
            assert row.score >= score
            score = row.score

        sort_specs = [search.SortScore(desc=True)]
        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10, sort=sort_specs))
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        score = rows[0].score
        for row in rows[1:]:
            assert score >= row.score
            score = row.score

    def test_cluster_sort_str(self, cb_env: SyncTestEnvironment) -> None:
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        q = search.TermQuery('auto')

        # ascending
        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10, sort=['_score']))
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        score = rows[0].score
        for row in rows[1:]:
            assert row.score >= score
            score = row.score

        # descending
        res = cb_env.cluster.search_query(idx, q, SearchOptions(limit=10, sort=['-_score']))
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        score = rows[0].score
        for row in rows[1:]:
            assert score >= row.score
            score = row.score

    def test_search_include_locations(self, cb_env: SyncTestEnvironment) -> None:
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        q = search.TermQuery('auto')

        opts = SearchOptions(limit=10, include_locations=True)
        res = cb_env.cluster.search_query(idx, q, opts)
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        locations = rows[0].locations
        assert isinstance(locations, SearchRowLocations)
        assert all(isinstance(loc, SearchRowLocation) for loc in locations.get_all())

        opts = SearchOptions(limit=10)
        res = cb_env.cluster.search_query(idx, q, opts, include_locations=True)
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        locations = rows[0].locations
        assert isinstance(locations, SearchRowLocations)
        assert all(isinstance(loc, SearchRowLocation) for loc in locations.get_all())

    @pytest.mark.parametrize('operator, query_terms, expect_rows',
                             [(search.MatchOperator.AND, 'auto deal', True),
                              (search.MatchOperator.AND, 'auto :random:', False),
                              (search.MatchOperator.OR, 'auto deal', True),
                              (search.MatchOperator.OR, 'auto :random:', True)])
    def test_search_match_operator(self,
                                   cb_env: SyncTestEnvironment,
                                   operator: search.MatchOperator,
                                   query_terms: str,
                                   expect_rows: bool) -> None:
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        # A minted term, so no seeded document can match it and the AND case fails because
        # the operator requires every term rather than by coincidence of the corpus.
        random_query_term = ''.join(random.choice(string.ascii_letters) for _ in range(10))  # nosec
        if ':random:' in query_terms:
            query_terms = query_terms.replace(':random:', random_query_term)

        q = search.MatchQuery(query_terms, match_operator=operator)
        res = cb_env.cluster.search_query(idx, q, limit=10)

        if expect_rows:
            SearchEnvironment.assert_rows_at_least(res, 1)
        else:
            SearchEnvironment.assert_rows_exact(res, 0)

    def test_search_match_operator_fail(self, cb_env: SyncTestEnvironment) -> None:
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        with pytest.raises(ValueError):
            q = search.MatchQuery('auto deal', match_operator='NOT')
            cb_env.cluster.search_query(idx, q, limit=10)

    def test_search_no_include_locations(self, cb_env: SyncTestEnvironment) -> None:
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        q = search.TermQuery('auto')

        opts = SearchOptions(limit=10, include_locations=False)
        res = cb_env.cluster.search_query(idx, q, opts)
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        assert rows[0].locations is None

        opts = SearchOptions(limit=10)
        res = cb_env.cluster.search_query(idx, q, opts, include_locations=False)
        rows = SearchEnvironment.assert_rows_at_least(res, 1)
        assert rows[0].locations is None

    # Excluded from both async trees for the same reason as its twin in the collection suite
    # above: a foreign thread exercises the event loop, not the SDK.
    @pytest.mark.testgen_exclude('acouchbase', 'txcouchbase',
                                 reason='drives a threading.Thread; no async analogue')
    def test_search_query_in_thread(self, cb_env: SyncTestEnvironment) -> None:
        results: list[Any] = [None]

        def run_test(cluster: Any,
                     search_idx: str,
                     assert_fn: Callable[[SearchResult, int], list[SearchRow]],
                     results: list[Any]) -> None:
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

        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        t = threading.Thread(target=run_test,
                             args=(cb_env.cluster, idx, SearchEnvironment.assert_rows_at_least, results))
        t.start()
        t.join()

        assert len(results) == 1
        assert results[0] is True

    def test_search_raw_query(self, cb_env: SyncTestEnvironment) -> None:
        idx = SearchEnvironment.index_name(SearchTestSuite.COLLECTION_STRATEGY)
        query_args = {'match': 'auto deal', 'fuzziness': 2, 'operator': 'and'}
        q = search.RawQuery(query_args)
        res = cb_env.cluster.search_query(idx, q, limit=10)
        SearchEnvironment.assert_rows_at_least(res, 1)

    @staticmethod
    def _assert_facet_without_details(res: SearchResult, facet_name: str) -> None:
        facets = res.facets()
        assert isinstance(facets, dict)
        result_facet = facets[facet_name]
        assert isinstance(result_facet, SearchFacetResult)
        assert result_facet.name == facet_name
        assert result_facet.field == facet_name
        assert result_facet.terms is None
        assert result_facet.numeric_ranges is None
        assert result_facet.date_ranges is None


class SearchCollectionTests(SearchCollectionTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(SearchCollectionTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, SearchCollectionTestSuite)
        SearchEnvironment.setup_search(env, SearchCollectionTestSuite.COLLECTION_STRATEGY)
        yield env
        SearchEnvironment.teardown_search(env, SearchCollectionTestSuite.COLLECTION_STRATEGY)
        teardown_environment(env, SearchCollectionTestSuite)


class SearchTests(SearchTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(SearchTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, SearchTestSuite)
        SearchEnvironment.setup_search(env, SearchTestSuite.COLLECTION_STRATEGY)
        yield env
        SearchEnvironment.teardown_search(env, SearchTestSuite.COLLECTION_STRATEGY)
        teardown_environment(env, SearchTestSuite)
