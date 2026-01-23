#  Copyright 2016-2023. Couchbase, Inc.
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


import uuid
from copy import copy
from datetime import datetime, timedelta

import pytest
import pytest_asyncio

import couchbase.search as search
from couchbase.exceptions import InvalidArgumentException, QueryIndexNotFoundException
from couchbase.mutation_state import MutationState
from couchbase.options import SearchOptions
from couchbase.search import (HighlightStyle,
                              SearchDateRangeFacet,
                              SearchFacetResult,
                              SearchNumericRangeFacet,
                              SearchRow,
                              SearchTermFacet)
from tests.environments import CollectionType
from tests.environments.search_environment import AsyncSearchTestEnvironment
from tests.test_features import EnvironmentFeatures


class SearchCollectionTestSuite:
    TEST_MANIFEST = [
        'test_cluster_query_collections',
        'test_scope_query_collections',
        'test_scope_search_fields',
        'test_scope_search_highlight',
    ]

    @pytest.mark.asyncio
    async def test_cluster_query_collections(self, cb_env):
        q = search.TermQuery('auto')
        res = cb_env.cluster.search_query(cb_env.TEST_COLLECTION_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10,
                                                        scope_name=cb_env.scope.name,
                                                        collections=[cb_env.collection.name]))
        rows = await cb_env.assert_rows(res, 2, return_rows=True)

        collections = list(map(lambda r: r.fields['_$c'], rows))
        assert all([c for c in collections if c == cb_env.collection.name]) is True

    @pytest.mark.asyncio
    async def test_scope_query_collections(self, cb_env):
        q = search.TermQuery('auto')
        res = cb_env.scope.search_query(cb_env.TEST_COLLECTION_INDEX_NAME,
                                        q,
                                        SearchOptions(limit=10, collections=[cb_env.collection.name]))
        rows = await cb_env.assert_rows(res, 2, return_rows=True)

        collections = list(map(lambda r: r.fields['_$c'], rows))
        assert all([c for c in collections if c == cb_env.collection.name]) is True

        res = cb_env.scope.search_query(cb_env.TEST_COLLECTION_INDEX_NAME, q, SearchOptions(limit=10))
        rows = await cb_env.assert_rows(res, 2, return_rows=True)

        collections = list(map(lambda r: r.fields['_$c'], rows))
        assert all([c for c in collections if c in [cb_env.collection.name, cb_env.OTHER_COLLECTION]]) is True

    @pytest.mark.asyncio
    async def test_scope_search_fields(self, cb_env):
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
        rows = await cb_env.assert_rows(res, 1, return_rows=True)
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

        rows = await cb_env.assert_rows(res, 1, return_rows=True)
        first_entry = rows[0]
        assert isinstance(first_entry, SearchRow)
        assert isinstance(first_entry.fields, dict)
        assert first_entry.fields != {}
        assert all(map(lambda f: f in fields_with_col, first_entry.fields.keys())) is True
        collections = list(map(lambda r: r.fields['_$c'], rows))
        assert all([c for c in collections if c == cb_env.collection.name]) is True

    @pytest.mark.asyncio
    async def test_scope_search_highlight(self, cb_env):

        q = search.TermQuery('auto')
        # check w/in options
        res = cb_env.scope.search_query(cb_env.TEST_COLLECTION_INDEX_NAME,
                                        q,
                                        SearchOptions(limit=10, highlight_style=HighlightStyle.Html))
        rows = await cb_env.assert_rows(res, 1, return_rows=True)
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
        rows = await cb_env.assert_rows(res, 1, return_rows=True)
        locations = rows[0].locations
        fragments = rows[0].fragments
        assert isinstance(locations, search.SearchRowLocations)
        assert isinstance(fragments, dict)
        assert all(map(lambda l: isinstance(l, search.SearchRowLocation), locations.get_all())) is True
        collections = list(map(lambda r: r.fields['_$c'], rows))
        assert all([c for c in collections if c == cb_env.collection.name]) is True


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
        'test_search_raw_query',
    ]

    @pytest.fixture(scope="class")
    def check_disable_scoring_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('search_disable_scoring',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.mark.asyncio
    async def test_bad_search_query(self, cb_env):
        res = cb_env.cluster.search_query('not-an-index', search.TermQuery('auto'))
        with pytest.raises(QueryIndexNotFoundException):
            [r async for r in res]

    @pytest.mark.asyncio
    async def test_cluster_search(self, cb_env):
        q = search.TermQuery('auto')
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME, q, SearchOptions(limit=10))
        await cb_env.assert_rows(res, 2)

    @pytest.mark.asyncio
    async def test_cluster_search_date_facets(self, cb_env):
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

        await cb_env.assert_rows(res, 1)
        facets = res.facets()
        assert isinstance(facets, dict)
        result_facet = facets[facet_name]
        assert isinstance(result_facet, SearchFacetResult)
        assert result_facet.name == facet_name
        assert result_facet.field == facet_name
        assert all(map(lambda ft: isinstance(ft, SearchDateRangeFacet), result_facet.date_ranges)) is True
        assert len(result_facet.date_ranges) <= facet.limit

    @pytest.mark.usefixtures('check_disable_scoring_supported')
    @pytest.mark.asyncio
    async def test_cluster_search_disable_scoring(self, cb_env):

        # verify disable scoring works w/in SearchOptions
        q = search.TermQuery('auto')
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, disable_scoring=True))
        rows = await cb_env.assert_rows(res, 1, return_rows=True)
        assert all(map(lambda r: r.score == 0, rows)) is True

        # verify disable scoring works w/in kwargs
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10), disable_scoring=True)
        rows = await cb_env.assert_rows(res, 1, return_rows=True)
        assert all(map(lambda r: r.score == 0, rows)) is True

        # verify setting disable_scoring to False works
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, disable_scoring=False))
        rows = await cb_env.assert_rows(res, 1, return_rows=True)
        assert all(map(lambda r: r.score != 0, rows)) is True

        # verify default disable_scoring is False
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10))
        rows = await cb_env.assert_rows(res, 1, return_rows=True)
        assert all(map(lambda r: r.score != 0, rows)) is True

    # @TODO: 3.x raises a SearchException...

    def test_cluster_search_facets_fail(self, cb_env):
        q = search.TermQuery('auto')
        with pytest.raises(ValueError):
            cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                        q,
                                        SearchOptions(limit=10, facets={'test-facet': None}))

    @pytest.mark.asyncio
    async def test_cluster_search_fields(self, cb_env):
        test_fields = ['make', 'mode']
        q = search.TermQuery('auto')
        # verify fields works w/in kwargs
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10), fields=test_fields)

        rows = await cb_env.assert_rows(res, 1, return_rows=True)
        first_entry = rows[0]
        assert isinstance(first_entry, SearchRow)
        assert isinstance(first_entry.fields, dict)
        assert first_entry.fields != {}
        assert all(map(lambda f: f in test_fields, first_entry.fields.keys())) is True

        # verify fields works w/in options
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, fields=test_fields))

        rows = await cb_env.assert_rows(res, 1, return_rows=True)
        first_entry = rows[0]
        assert isinstance(first_entry, SearchRow)
        assert isinstance(first_entry.fields, dict)
        assert first_entry.fields != {}
        assert all(map(lambda f: f in test_fields, first_entry.fields.keys())) is True

    @pytest.mark.asyncio
    async def test_cluster_search_highlight(self, cb_env):

        q = search.TermQuery('auto')
        # check w/in options
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, highlight_style=HighlightStyle.Html))
        rows = await cb_env.assert_rows(res, 1, return_rows=True)
        locations = rows[0].locations
        fragments = rows[0].fragments
        assert isinstance(locations, search.SearchRowLocations)
        assert isinstance(fragments, dict)
        assert all(map(lambda l: isinstance(l, search.SearchRowLocation), locations.get_all())) is True

        # check w/in options
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME, q, SearchOptions(
            limit=10), highlight_style=HighlightStyle.Html)
        rows = await cb_env.assert_rows(res, 1, return_rows=True)
        locations = rows[0].locations
        fragments = rows[0].fragments
        assert isinstance(locations, search.SearchRowLocations)
        assert isinstance(fragments, dict)
        assert all(map(lambda l: isinstance(l, search.SearchRowLocation), locations.get_all())) is True

    @pytest.mark.asyncio
    async def test_cluster_search_numeric_facets(self, cb_env):

        facet_name = 'rating'
        facet = search.NumericFacet('rating', limit=3)
        facet.add_range('low', max=4)
        facet.add_range('med', min=4, max=7)
        facet.add_range('high', min=7)
        q = search.TermQuery('auto')
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, facets={facet_name: facet}))

        await cb_env.assert_rows(res, 1)
        facets = res.facets()
        assert isinstance(facets, dict)
        result_facet = facets[facet_name]
        assert isinstance(result_facet, SearchFacetResult)
        assert result_facet.name == facet_name
        assert result_facet.field == facet_name
        assert all(map(lambda ft: isinstance(ft, SearchNumericRangeFacet), result_facet.numeric_ranges)) is True
        assert len(result_facet.numeric_ranges) <= facet.limit

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.asyncio
    async def test_cluster_search_ryow(self, cb_env):
        key, value = cb_env.get_new_doc()
        # need to make sure content is unique
        content = str(uuid.uuid4())[:8]
        value['description'] = content
        q = search.TermQuery(content)
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10))
        await cb_env.assert_rows(res, 0)
        res = await cb_env.collection.insert(key, value)
        ms = MutationState()
        ms.add_mutation_token(res.mutation_token())
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, consistent_with=ms))
        await cb_env.assert_rows(res, 1)

        # prior to PYCBC-1477 the SDK _could_ crash w/ this this sort of MS creation
        key, value = cb_env.get_new_doc()
        # need to make sure content is unique
        content = str(uuid.uuid4())[:8]
        value['description'] = content
        q = search.TermQuery(content)
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10))
        await cb_env.assert_rows(res, 0)
        res = await cb_env.collection.insert(key, value)
        ms = MutationState(res)
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, consistent_with=ms))
        await cb_env.assert_rows(res, 1)

    @pytest.mark.asyncio
    async def test_cluster_search_term_facets(self, cb_env):

        facet_name = 'model'
        facet = search.TermFacet('model', 5)
        q = search.TermQuery('auto')
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, facets={facet_name: facet}))

        await cb_env.assert_rows(res, 1)
        facets = res.facets()
        assert isinstance(facets, dict)
        result_facet = facets[facet_name]
        assert isinstance(result_facet, SearchFacetResult)
        assert result_facet.name == facet_name
        assert result_facet.field == facet_name
        assert all(map(lambda ft: isinstance(ft, SearchTermFacet), result_facet.terms)) is True
        assert len(result_facet.terms) <= facet.limit

    @pytest.mark.asyncio
    async def test_cluster_search_scan_consistency(self, cb_env):
        q = search.TermQuery('auto')
        # check w/in options
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10,
                                                        scan_consistency=search.SearchScanConsistency.NOT_BOUNDED))
        await cb_env.assert_rows(res, 1)

        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10,
                                                        scan_consistency=search.SearchScanConsistency.REQUEST_PLUS))
        await cb_env.assert_rows(res, 1)

        with pytest.raises(InvalidArgumentException):
            cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                        q,
                                        SearchOptions(limit=10,
                                                      scan_consistency=search.SearchScanConsistency.AT_PLUS))

    @pytest.mark.asyncio
    async def test_cluster_search_top_level_facets(self, cb_env):
        # if the facet limit is omitted, the details of the facets will not be provided
        # (i.e. SearchFacetResult.terms is None,
        #       SearchFacetResult.numeric_ranges is None and SearchFacetResult.date_ranges is None)
        facet_name = 'model'
        facet = search.TermFacet('model')
        q = search.TermQuery('auto')
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, facets={facet_name: facet}))

        await cb_env.assert_rows(res, 1)
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

        await cb_env.assert_rows(res, 1)
        facets = res.facets()
        assert isinstance(facets, dict)
        result_facet = facets[facet_name]
        assert isinstance(result_facet, SearchFacetResult)
        assert result_facet.name == facet_name
        assert result_facet.field == facet_name
        assert result_facet.terms is None
        assert result_facet.numeric_ranges is None
        assert result_facet.date_ranges is None

    @pytest.mark.asyncio
    async def test_cluster_search_top_level_facets_kwargs(self, cb_env):
        # if the facet limit is omitted, the details of the facets will not be provided
        # (i.e. SearchFacetResult.terms is None,
        #       SearchFacetResult.numeric_ranges is None and SearchFacetResult.date_ranges is None)
        facet_name = 'model'
        facet = search.TermFacet('model')
        q = search.TermQuery('auto')
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10), facets={facet_name: facet})

        await cb_env.assert_rows(res, 1)
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

        await cb_env.assert_rows(res, 1)
        facets = res.facets()
        assert isinstance(facets, dict)
        result_facet = facets[facet_name]
        assert isinstance(result_facet, SearchFacetResult)
        assert result_facet.name == facet_name
        assert result_facet.field == facet_name
        assert result_facet.terms is None
        assert result_facet.numeric_ranges is None
        assert result_facet.date_ranges is None

    @pytest.mark.asyncio
    async def test_cluster_sort_field(self, cb_env):
        sort_field = "rating"
        q = search.TermQuery('auto')
        # field - ascending
        sort = search.SortField(field=sort_field, type="number", mode="min", missing="last")
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=[sort], fields=[sort_field]))

        rows = await cb_env.assert_rows(res, 1, return_rows=True)
        rating = rows[0].fields[sort_field]
        for row in rows[1:]:
            assert row.fields[sort_field] >= rating
            rating = row.fields[sort_field]

        # field - descending
        sort.desc = True
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=[sort], fields=[sort_field]))

        rows = await cb_env.assert_rows(res, 1, return_rows=True)
        rating = rows[0].fields[sort_field]
        for row in rows[1:]:
            assert rating >= row.fields[sort_field]
            rating = row.fields[sort_field]

    @pytest.mark.asyncio
    async def test_cluster_sort_field_multi(self, cb_env):
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
        await cb_env.assert_rows(res, 1)

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
        await cb_env.assert_rows(res, 1)

        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=["rating", "last_udpated", "-_score"]))
        await cb_env.assert_rows(res, 1)

    @pytest.mark.asyncio
    async def test_cluster_sort_geo(self, cb_env):
        # @TODO:  better confirmation on results?
        sort_field = "geo"
        q = search.TermQuery('auto')
        # geo - ascending
        sort = search.SortGeoDistance(field=sort_field, location=(37.7749, 122.4194), unit="meters")
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=[sort], fields=[sort_field]))
        await cb_env.assert_rows(res, 1)

        # geo - descending
        sort.desc = True
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=[sort], fields=[sort_field]))
        await cb_env.assert_rows(res, 1)

    @pytest.mark.asyncio
    async def test_cluster_sort_id(self, cb_env):
        q = search.TermQuery('auto')
        # score - ascending
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=[search.SortID()]))
        rows = await cb_env.assert_rows(res, 1, return_rows=True)

        id = rows[0].id
        for row in rows[1:]:
            assert row.id >= id
            id = row.id

        # score - descending
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=[search.SortID(desc=True)]))
        rows = await cb_env.assert_rows(res, 1, return_rows=True)

        id = rows[0].id
        for row in rows[1:]:
            assert id >= row.id
            id = row.id

    @pytest.mark.asyncio
    async def test_cluster_sort_score(self, cb_env):
        q = search.TermQuery('auto')
        # score - ascending
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=[search.SortScore()]))
        rows = await cb_env.assert_rows(res, 1, return_rows=True)

        score = rows[0].score
        for row in rows[1:]:
            assert row.score >= score
            score = row.score

        # score - descending
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=[search.SortScore(desc=True)]))
        rows = await cb_env.assert_rows(res, 1, return_rows=True)

        score = rows[0].score
        for row in rows[1:]:
            assert score >= row.score
            score = row.score

    @pytest.mark.asyncio
    async def test_cluster_sort_str(self, cb_env):
        q = search.TermQuery('auto')
        # score - ascending
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=['_score']))
        rows = await cb_env.assert_rows(res, 1, return_rows=True)
        score = rows[0].score
        for row in rows[1:]:
            assert row.score >= score
            score = row.score

        # score - descending
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, sort=['-_score']))
        rows = await cb_env.assert_rows(res, 1, return_rows=True)
        score = rows[0].score
        for row in rows[1:]:
            assert score >= row.score
            score = row.score

    @pytest.mark.asyncio
    async def test_search_include_locations(self, cb_env):
        q = search.TermQuery('auto')
        # check w/in options
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, include_locations=True))
        rows = await cb_env.assert_rows(res, 1, return_rows=True)
        locations = rows[0].locations
        assert isinstance(locations, search.SearchRowLocations)
        assert all(map(lambda l: isinstance(l, search.SearchRowLocation), locations.get_all())) is True

        # check w/in kwargs
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10), include_locations=True)
        rows = await cb_env.assert_rows(res, 1, return_rows=True)
        locations = rows[0].locations
        assert isinstance(locations, search.SearchRowLocations)
        assert all(map(lambda l: isinstance(l, search.SearchRowLocation), locations.get_all())) is True

    @pytest.mark.parametrize('operator, query_terms, expect_rows',
                             [(search.MatchOperator.AND, "auto deal", True),
                              (search.MatchOperator.AND, "auto :random:", False),
                              (search.MatchOperator.OR, "auto deal", True),
                              (search.MatchOperator.OR, "auto :random:", True)])
    @pytest.mark.asyncio
    async def test_search_match_operator(self, cb_env, operator, query_terms, expect_rows):
        import random
        import string

        random_query_term = "".join(random.choice(string.ascii_letters)
                                    for _ in range(10))

        if ':random:' in query_terms:
            query_terms.replace(':random:', random_query_term)

        q = search.MatchQuery(query_terms, match_operator=operator)

        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME, q, limit=10)
        rows = await cb_env.assert_rows(res, 0, return_rows=True)

        if expect_rows:
            assert len(rows) > 0
        else:
            assert len(rows) == 0

    def test_search_match_operator_fail(self, cb_env):
        with pytest.raises(ValueError):
            q = search.MatchQuery('auto deal', match_operator='NOT')
            cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME, q, limit=10)

    # @TODO(PYCBC-1296):  DIFF between 3.x and 4.x, locations returns None

    @pytest.mark.asyncio
    async def test_search_no_include_locations(self, cb_env):
        q = search.TermQuery('auto')
        # check w/in options
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10, include_locations=False))
        rows = await cb_env.assert_rows(res, 1, return_rows=True)
        locations = rows[0].locations
        assert locations is None

        # check w/in kwargs
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME,
                                          q,
                                          SearchOptions(limit=10), include_locations=False)
        rows = await cb_env.assert_rows(res, 1, return_rows=True)
        locations = rows[0].locations
        assert locations is None

    @pytest.mark.asyncio
    async def test_search_raw_query(self, cb_env):
        query_args = {"match": "auto deal",
                      "fuzziness": 2, "operator": "and"}
        q = search.RawQuery(query_args)
        res = cb_env.cluster.search_query(cb_env.TEST_INDEX_NAME, q, limit=10)
        await cb_env.assert_rows(res, 1)


class ClassicSearchCollectionTests(SearchCollectionTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicSearchCollectionTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicSearchCollectionTests) if valid_test_method(meth)]
        compare = set(SearchCollectionTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest_asyncio.fixture(scope='class', name='cb_env', params=[CollectionType.NAMED])
    async def couchbase_test_environment(self, acb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        acb_env = AsyncSearchTestEnvironment.from_environment(acb_base_env)
        acb_env.enable_search_mgmt()
        await acb_env.setup(request.param)
        yield acb_env
        await acb_env.teardown(request.param)


class ClassicSearchTests(SearchTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicSearchTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicSearchTests) if valid_test_method(meth)]
        compare = set(SearchTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest_asyncio.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT])
    async def couchbase_test_environment(self, acb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        acb_env = AsyncSearchTestEnvironment.from_environment(acb_base_env)
        acb_env.enable_search_mgmt()
        await acb_env.setup(request.param)
        yield acb_env
        await acb_env.teardown(request.param)
