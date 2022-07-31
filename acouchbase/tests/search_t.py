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

import asyncio
import json
import pathlib
from copy import copy
from os import path

import pytest
import pytest_asyncio

import couchbase.search as search
from acouchbase.cluster import get_event_loop
from couchbase.exceptions import InvalidArgumentException, SearchIndexNotFoundException
from couchbase.management.collections import CollectionSpec
from couchbase.management.search import SearchIndex
from couchbase.search import (HighlightStyle,
                              SearchDateRangeFacet,
                              SearchFacetResult,
                              SearchNumericRangeFacet,
                              SearchOptions,
                              SearchRow,
                              SearchTermFacet)

from ._test_utils import CouchbaseTestEnvironmentException, TestEnvironment


class SearchTests:
    TEST_INDEX_NAME = 'test-search-index'
    TEST_INDEX_PATH = path.join(pathlib.Path(__file__).parent.parent.parent,
                                'tests',
                                'test_cases',
                                f'{TEST_INDEX_NAME}-params.json')

    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest_asyncio.fixture(scope="class", name="cb_env")
    async def couchbase_test_environment(self, couchbase_config):
        cb_env = await TestEnvironment.get_environment(__name__,
                                                       couchbase_config,
                                                       manage_buckets=True,
                                                       manage_search_indexes=True)

        await cb_env.try_n_times(3, 5, cb_env.load_data)
        try:
            await cb_env.try_n_times(3, 5, self._load_search_index, cb_env)
        except CouchbaseTestEnvironmentException:
            pytest.skip('Search index would not load.')
        yield cb_env
        await cb_env.try_n_times_till_exception(3, 5,
                                                cb_env.purge_data,
                                                raise_if_no_exception=False)
        await self._drop_search_index(cb_env)

    async def _load_search_index(self, cb_env):
        await cb_env.try_n_times_till_exception(10, 3,
                                                cb_env.sixm.drop_index,
                                                self.TEST_INDEX_NAME,
                                                expected_exceptions=(SearchIndexNotFoundException, ))
        with open(self.TEST_INDEX_PATH) as params_file:
            input = params_file.read()
            params_json = json.loads(input)
            await cb_env.try_n_times(10, 3,
                                     cb_env.sixm.upsert_index,
                                     SearchIndex(name=self.TEST_INDEX_NAME,
                                                 idx_type='fulltext-index',
                                                 source_name='default',
                                                 source_type='couchbase',
                                                 params=params_json))
            # make sure the index loads...
            num_docs = await self._check_indexed_docs(cb_env, retries=30, delay=10)
            if num_docs == 0:
                raise CouchbaseTestEnvironmentException('No docs loaded into the index')

    async def _check_indexed_docs(self, cb_env, retries=20, delay=30, num_docs=20, idx='test-search-index'):
        indexed_docs = 0
        no_docs_cutoff = 300
        for i in range(retries):
            # if no docs after waiting for a period of time, exit
            if indexed_docs == 0 and i * delay >= no_docs_cutoff:
                return 0
            indexed_docs = await cb_env.try_n_times(
                10, 10, cb_env.sixm.get_indexed_documents_count, idx)
            if indexed_docs >= num_docs:
                break
            print(f'Found {indexed_docs} indexed docs, waiting a bit...')
            await asyncio.sleep(delay)

        return indexed_docs

    @pytest.fixture(scope="class")
    def check_disable_scoring_supported(self, cb_env):
        cb_env.check_if_feature_supported('search_disable_scoring')

    async def _drop_search_index(self, cb_env):
        try:
            await cb_env.sixm.drop_index(self.TEST_INDEX_NAME)
        except SearchIndexNotFoundException:
            pass
        except Exception as ex:
            raise ex

    @pytest.mark.asyncio
    async def test_cluster_search(self, cb_env):
        q = search.TermQuery('home')
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10))
        await cb_env.assert_search_rows_async(res, 2)

    @pytest.mark.asyncio
    async def test_cluster_search_fields(self, cb_env):
        test_fields = ['name', 'activity']
        q = search.TermQuery('home')
        # verify fields works w/in kwargs
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10), fields=test_fields)

        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        first_entry = rows[0]
        assert isinstance(first_entry, SearchRow)
        assert isinstance(first_entry.fields, dict)
        assert first_entry.fields != {}
        res = list(map(lambda f: f in test_fields, first_entry.fields.keys()))
        assert all(map(lambda f: f in test_fields, first_entry.fields.keys())) is True

        # verify fields works w/in options
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10, fields=test_fields))

        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        first_entry = rows[0]
        assert isinstance(first_entry, SearchRow)
        assert isinstance(first_entry.fields, dict)
        assert first_entry.fields != {}
        res = list(map(lambda f: f in test_fields, first_entry.fields.keys()))
        assert all(map(lambda f: f in test_fields, first_entry.fields.keys())) is True

    # @TODO: 3.x raises a SearchException...
    def test_cluster_search_facets_fail(self, cb_env):
        q = search.TermQuery('home')
        with pytest.raises(ValueError):
            cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10, facets={'test-facet': None}))

    @pytest.mark.asyncio
    async def test_cluster_search_top_level_facets(self, cb_env):
        # if the facet limit is omitted, the details of the facets will not be provided
        # (i.e. SearchFacetResult.terms is None,
        #       SearchFacetResult.numeric_ranges is None and SearchFacetResult.date_ranges is None)
        facet_name = 'activity'
        facet = search.TermFacet('activity')
        q = search.TermQuery('home')
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10, facets={facet_name: facet}))

        await cb_env.assert_search_rows_async(res, 1)
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
        facet.add_range('low', max=2)
        facet.add_range('med', min=2, max=4)
        facet.add_range('high', min=4)
        q = search.TermQuery('home')
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10, facets={facet_name: facet}))

        await cb_env.assert_search_rows_async(res, 1)
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
        facet_name = 'activity'
        facet = search.TermFacet('activity')
        q = search.TermQuery('home')
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10), facets={facet_name: facet})

        await cb_env.assert_search_rows_async(res, 1)
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
        facet.add_range('low', max=2)
        facet.add_range('med', min=2, max=4)
        facet.add_range('high', min=4)
        q = search.TermQuery('home')
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10), facets={facet_name: facet})

        await cb_env.assert_search_rows_async(res, 1)
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
    async def test_cluster_search_term_facets(self, cb_env):

        facet_name = 'activity'
        facet = search.TermFacet('activity', 5)
        q = search.TermQuery('home')
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10, facets={facet_name: facet}))

        await cb_env.assert_search_rows_async(res, 1)
        facets = res.facets()
        assert isinstance(facets, dict)
        result_facet = facets[facet_name]
        assert isinstance(result_facet, SearchFacetResult)
        assert result_facet.name == facet_name
        assert result_facet.field == facet_name
        assert all(map(lambda ft: isinstance(ft, SearchTermFacet), result_facet.terms)) is True
        assert len(result_facet.terms) <= facet.limit

    @pytest.mark.asyncio
    async def test_cluster_search_numeric_facets(self, cb_env):

        facet_name = 'rating'
        facet = search.NumericFacet('rating', limit=3)
        facet.add_range('low', max=2)
        facet.add_range('med', min=2, max=4)
        facet.add_range('high', min=4)
        q = search.TermQuery('home')
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10, facets={facet_name: facet}))

        await cb_env.assert_search_rows_async(res, 1)
        facets = res.facets()
        assert isinstance(facets, dict)
        result_facet = facets[facet_name]
        assert isinstance(result_facet, SearchFacetResult)
        assert result_facet.name == facet_name
        assert result_facet.field == facet_name
        assert all(map(lambda ft: isinstance(ft, SearchNumericRangeFacet), result_facet.numeric_ranges)) is True
        assert len(result_facet.numeric_ranges) <= facet.limit

    @pytest.mark.asyncio
    async def test_cluster_search_date_facets(self, cb_env):
        facet_name = 'updated'
        facet = search.DateFacet('updated', limit=3)
        facet.add_range('early', end='2022-02-02T00:00:00Z')
        facet.add_range('mid', start='2022-02-03T00:00:00Z',
                        end='2022-03-03T00:00:00Z')
        facet.add_range('late', start='2022-03-04T00:00:00Z')
        q = search.TermQuery('home')
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10, facets={facet_name: facet}))

        await cb_env.assert_search_rows_async(res, 1)
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
        q = search.TermQuery('home')
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10, disable_scoring=True))
        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        assert all(map(lambda r: r.score == 0, rows)) is True

        # verify disable scoring works w/in kwargs
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10), disable_scoring=True)
        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        assert all(map(lambda r: r.score == 0, rows)) is True

        # verify setting disable_scoring to False works
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10, disable_scoring=False))
        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        assert all(map(lambda r: r.score != 0, rows)) is True

        # verify default disable_scoring is False
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10))
        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        assert all(map(lambda r: r.score != 0, rows)) is True

    @pytest.mark.asyncio
    async def test_cluster_search_highlight(self, cb_env):

        q = search.TermQuery('home')
        # check w/in options
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(
            limit=10, highlight_style=HighlightStyle.Html))
        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        locations = rows[0].locations
        fragments = rows[0].fragments
        assert isinstance(locations, search.SearchRowLocations)
        assert isinstance(fragments, dict)
        assert all(map(lambda l: isinstance(l, search.SearchRowLocation), locations.get_all())) is True

        # check w/in options
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(
            limit=10), highlight_style=HighlightStyle.Html)
        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        locations = rows[0].locations
        fragments = rows[0].fragments
        assert isinstance(locations, search.SearchRowLocations)
        assert isinstance(fragments, dict)
        assert all(map(lambda l: isinstance(l, search.SearchRowLocation), locations.get_all())) is True

    # @TODO(PYCBC-1296):  DIFF between 3.x and 4.x, locations returns None
    @pytest.mark.asyncio
    async def test_search_no_include_locations(self, cb_env):
        q = search.TermQuery('home')
        # check w/in options
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10, include_locations=False))
        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        locations = rows[0].locations
        assert locations is None

        # check w/in kwargs
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10), include_locations=False)
        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        locations = rows[0].locations
        assert locations is None

    @pytest.mark.asyncio
    async def test_search_include_locations(self, cb_env):
        q = search.TermQuery('home')
        # check w/in options
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10, include_locations=True))
        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        locations = rows[0].locations
        assert isinstance(locations, search.SearchRowLocations)
        assert all(map(lambda l: isinstance(l, search.SearchRowLocation), locations.get_all())) is True

        # check w/in kwargs
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10), include_locations=True)
        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        locations = rows[0].locations
        assert isinstance(locations, search.SearchRowLocations)
        assert all(map(lambda l: isinstance(l, search.SearchRowLocation), locations.get_all())) is True

    @pytest.mark.asyncio
    async def test_cluster_search_scan_consistency(self, cb_env):
        q = search.TermQuery('home')
        # check w/in options
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(
            limit=10, scan_consistency=search.SearchScanConsistency.NOT_BOUNDED))
        await cb_env.assert_search_rows_async(res, 1)

        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(
            limit=10, scan_consistency=search.SearchScanConsistency.REQUEST_PLUS))
        await cb_env.assert_search_rows_async(res, 1)

        with pytest.raises(InvalidArgumentException):
            cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(
                limit=10, scan_consistency=search.SearchScanConsistency.AT_PLUS))

    @pytest.mark.parametrize('operator, query_terms, expect_rows',
                             [(search.MatchOperator.AND, "home hollywood", True),
                              (search.MatchOperator.AND, "home :random:", False),
                              (search.MatchOperator.OR, "home hollywood", True),
                              (search.MatchOperator.OR, "home :random:", True)])
    @pytest.mark.asyncio
    async def test_search_match_operator(self, cb_env, operator, query_terms, expect_rows):
        import random
        import string

        random_query_term = "".join(random.choice(string.ascii_letters)
                                    for _ in range(10))

        if ':random:' in query_terms:
            query_terms.replace(':random:', random_query_term)

        q = search.MatchQuery(query_terms, match_operator=operator)

        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, limit=10)
        rows = await cb_env.assert_search_rows_async(res, 0, return_rows=True)

        if expect_rows:
            assert len(rows) > 0
        else:
            assert len(rows) == 0

    @pytest.mark.asyncio
    async def test_search_match_operator_fail(self, cb_env):
        with pytest.raises(ValueError):
            q = search.MatchQuery('home hollywood', match_operator='NOT')
            cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, limit=10)

    @pytest.mark.asyncio
    async def test_search_raw_query(self, cb_env):
        query_args = {"match": "home hollywood",
                      "fuzziness": 2, "operator": "and"}
        q = search.RawQuery(query_args)
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, limit=10)
        await cb_env.assert_search_rows_async(res, 1)

    @pytest.mark.asyncio
    async def test_cluster_sort_str(self, cb_env):
        q = search.TermQuery('home')
        # score - ascending
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10, sort=['_score']))
        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        score = rows[0].score
        for row in rows[1:]:
            assert row.score >= score
            score = row.score

        # score - descending
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10, sort=['-_score']))
        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        score = rows[0].score
        for row in rows[1:]:
            assert score >= row.score
            score = row.score

    @pytest.mark.asyncio
    async def test_cluster_sort_score(self, cb_env):
        q = search.TermQuery('home')
        # score - ascending
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10, sort=[search.SortScore()]))
        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)

        score = rows[0].score
        for row in rows[1:]:
            assert row.score >= score
            score = row.score

        # score - descending
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(
            limit=10, sort=[search.SortScore(desc=True)]))
        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)

        score = rows[0].score
        for row in rows[1:]:
            assert score >= row.score
            score = row.score

    @pytest.mark.asyncio
    async def test_cluster_sort_id(self, cb_env):
        q = search.TermQuery('home')
        # score - ascending
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10, sort=[search.SortID()]))
        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)

        id = rows[0].id
        for row in rows[1:]:
            assert row.id >= id
            id = row.id

        # score - descending
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(
            limit=10, sort=[search.SortID(desc=True)]))
        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)

        id = rows[0].id
        for row in rows[1:]:
            assert id >= row.id
            id = row.id

    @pytest.mark.asyncio
    async def test_cluster_sort_field(self, cb_env):
        sort_field = "rating"
        q = search.TermQuery('home')
        # field - ascending
        sort = search.SortField(field=sort_field, type="number", mode="min", missing="last")
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(
            limit=10, sort=[sort], fields=[sort_field]))

        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        rating = rows[0].fields[sort_field]
        for row in rows[1:]:
            assert row.fields[sort_field] >= rating
            rating = row.fields[sort_field]

        # field - descending
        sort.desc = True
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(
            limit=10, sort=[sort], fields=[sort_field]))

        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        rating = rows[0].fields[sort_field]
        for row in rows[1:]:
            assert rating >= row.fields[sort_field]
            rating = row.fields[sort_field]

    @pytest.mark.asyncio
    async def test_cluster_sort_geo(self, cb_env):
        # @TODO:  better confirmation on results?
        sort_field = "geo"
        q = search.TermQuery('home')
        # geo - ascending
        sort = search.SortGeoDistance(field=sort_field, location=(37.7749, 122.4194), unit="meters")
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(
            limit=10, sort=[sort], fields=[sort_field]))
        await cb_env.assert_search_rows_async(res, 1)

        # geo - descending
        sort.desc = True
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(
            limit=10, sort=[sort], fields=[sort_field]))
        await cb_env.assert_search_rows_async(res, 1)

    @pytest.mark.asyncio
    async def test_cluster_sort_field_multi(self, cb_env):
        sort_fields = [
            search.SortField(field="rating", type="number",
                             mode="min", missing="last"),
            search.SortField(field="updated", type="number",
                             mode="min", missing="last"),
            search.SortScore(),
        ]
        sort_field_names = ["rating", "updated"]
        q = search.TermQuery('home')
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(
            limit=10, sort=sort_fields, fields=sort_field_names))
        await cb_env.assert_search_rows_async(res, 1)

        sort_fields = [
            search.SortField(field="rating", type="number",
                             mode="min", missing="last", desc=True),
            search.SortField(field="updated", type="number",
                             mode="min", missing="last"),
            search.SortScore(desc=True),
        ]
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(
            limit=10, sort=sort_fields, fields=sort_field_names))
        await cb_env.assert_search_rows_async(res, 1)

        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(
            limit=10, sort=["abv", "udpated", "-_score"]))
        await cb_env.assert_search_rows_async(res, 1)


class SearchCollectionTests:
    TEST_INDEX_NAME = 'test-search-coll-index'
    TEST_INDEX_PATH = path.join(pathlib.Path(__file__).parent.parent.parent,
                                'tests',
                                'test_cases',
                                f'{TEST_INDEX_NAME}-params.json')
    OTHER_COLLECTION = 'other-collection'

    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest_asyncio.fixture(scope="class", name="cb_env")
    async def couchbase_test_environment(self, couchbase_config):
        cb_env = await TestEnvironment.get_environment(__name__,
                                                       couchbase_config,
                                                       manage_buckets=True,
                                                       manage_collections=True,
                                                       manage_search_indexes=True)

        await cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        await cb_env.try_n_times(5, 3, cb_env.load_data)
        # lets add another collection and load data there
        await self._create_and_load_other_collection(cb_env)

        try:
            await cb_env.try_n_times(3, 5, self._load_search_index, cb_env)
        except CouchbaseTestEnvironmentException:
            pytest.skip('Search index would not load.')
        yield cb_env
        await cb_env.try_n_times_till_exception(3, 5,
                                                cb_env.purge_data,
                                                raise_if_no_exception=False)
        await cb_env.try_n_times_till_exception(5, 3,
                                                cb_env.teardown_named_collections,
                                                raise_if_no_exception=False)
        await self._drop_search_index(cb_env)

    async def _create_and_load_other_collection(self, cb_env):
        # lets add another collection and load data there
        collection_spec = CollectionSpec(self.OTHER_COLLECTION, cb_env.TEST_SCOPE)
        await cb_env.cm.create_collection(collection_spec)
        collection = None
        for i in range(5):
            collection = await cb_env.get_collection(cb_env.TEST_SCOPE,
                                                     self.OTHER_COLLECTION,
                                                     bucket_name=cb_env.bucket.name)
            if collection:
                break
            cb_env.sleep(5)

        if not collection:
            raise CouchbaseTestEnvironmentException("Unabled to create other-collection for FTS collection testing")

        coll = cb_env.scope.collection(self.OTHER_COLLECTION)
        data = cb_env.get_json_data_by_type('landmarks')
        for d in data:
            key = f"{d['type']}_{d['id']}"
            await coll.upsert(key, d)

    async def _load_search_index(self, cb_env):
        await cb_env.try_n_times_till_exception(10, 3,
                                                cb_env.sixm.drop_index,
                                                self.TEST_INDEX_NAME,
                                                expected_exceptions=(SearchIndexNotFoundException, ))
        with open(self.TEST_INDEX_PATH) as params_file:
            input = params_file.read()
            params_json = json.loads(input)

            await cb_env.try_n_times(10, 3,
                                     cb_env.sixm.upsert_index,
                                     SearchIndex(name=self.TEST_INDEX_NAME,
                                                 idx_type='fulltext-index',
                                                 source_name='default',
                                                 source_type='couchbase',
                                                 params=params_json))
            # make sure the index loads...
            num_docs = await self._check_indexed_docs(cb_env, retries=30, delay=10)
            if num_docs == 0:
                raise CouchbaseTestEnvironmentException('No docs loaded into the index')

    async def _check_indexed_docs(self, cb_env, retries=20, delay=30, num_docs=20, idx='test-search-coll-index'):
        indexed_docs = 0
        no_docs_cutoff = 300
        for i in range(retries):
            # if no docs after waiting for a period of time, exit
            if indexed_docs == 0 and i * delay >= no_docs_cutoff:
                return 0
            indexed_docs = await cb_env.try_n_times(
                10, 10, cb_env.sixm.get_indexed_documents_count, idx)
            if indexed_docs >= num_docs:
                break
            print(f'Found {indexed_docs} indexed docs, waiting a bit...')
            await asyncio.sleep(delay)

        return indexed_docs

    async def _drop_search_index(self, cb_env):
        try:
            await cb_env.sixm.drop_index(self.TEST_INDEX_NAME)
        except SearchIndexNotFoundException:
            pass
        except Exception as ex:
            raise ex

    @pytest.mark.asyncio
    async def test_cluster_query_collections(self, cb_env):
        q = search.TermQuery('home')
        res = cb_env.cluster.search_query(self.TEST_INDEX_NAME, q, SearchOptions(
            limit=10, scope_name=cb_env.scope.name, collections=[cb_env.collection.name]))
        rows = await cb_env.assert_search_rows_async(res, 2, return_rows=True)

        collections = list(map(lambda r: r.fields['_$c'], rows))
        assert all([c for c in collections if c == cb_env.collection.name]) is True

    @pytest.mark.asyncio
    async def test_scope_query_collections(self, cb_env):
        q = search.TermQuery('home')
        res = cb_env.scope.search_query(self.TEST_INDEX_NAME, q, SearchOptions(
            limit=10, collections=[cb_env.collection.name]))
        rows = await cb_env.assert_search_rows_async(res, 2, return_rows=True)

        collections = list(map(lambda r: r.fields['_$c'], rows))
        assert all([c for c in collections if c == cb_env.collection.name]) is True

        res = cb_env.scope.search_query(self.TEST_INDEX_NAME, q, SearchOptions(limit=10))
        rows = await cb_env.assert_search_rows_async(res, 2, return_rows=True)

        collections = list(map(lambda r: r.fields['_$c'], rows))
        assert all([c for c in collections if c in [cb_env.collection.name, self.OTHER_COLLECTION]]) is True

    @pytest.mark.asyncio
    async def test_scope_search_fields(self, cb_env):
        test_fields = ['name', 'activity']
        q = search.TermQuery('home')
        # verify fields works w/in kwargs
        res = cb_env.scope.search_query(self.TEST_INDEX_NAME,
                                        q,
                                        SearchOptions(limit=10),
                                        fields=test_fields,
                                        collections=[cb_env.collection.name])

        fields_with_col = copy(test_fields)
        fields_with_col.append('_$c')
        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        first_entry = rows[0]
        assert isinstance(first_entry, SearchRow)
        assert isinstance(first_entry.fields, dict)
        assert first_entry.fields != {}
        assert all(map(lambda f: f in fields_with_col, first_entry.fields.keys())) is True
        collections = list(map(lambda r: r.fields['_$c'], rows))
        assert all([c for c in collections if c == cb_env.collection.name]) is True

        # verify fields works w/in options
        res = cb_env.scope.search_query(self.TEST_INDEX_NAME,
                                        q,
                                        SearchOptions(limit=10,
                                                      fields=test_fields,
                                                      collections=[cb_env.collection.name]))

        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        first_entry = rows[0]
        assert isinstance(first_entry, SearchRow)
        assert isinstance(first_entry.fields, dict)
        assert first_entry.fields != {}
        assert all(map(lambda f: f in fields_with_col, first_entry.fields.keys())) is True
        collections = list(map(lambda r: r.fields['_$c'], rows))
        assert all([c for c in collections if c == cb_env.collection.name]) is True

    @pytest.mark.asyncio
    async def test_scope_search_highlight(self, cb_env):

        q = search.TermQuery('home')
        # check w/in options
        res = cb_env.scope.search_query(self.TEST_INDEX_NAME, q, SearchOptions(
            limit=10, highlight_style=HighlightStyle.Html))
        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        locations = rows[0].locations
        fragments = rows[0].fragments
        assert isinstance(locations, search.SearchRowLocations)
        assert isinstance(fragments, dict)
        assert all(map(lambda l: isinstance(l, search.SearchRowLocation), locations.get_all())) is True
        collections = list(map(lambda r: r.fields['_$c'], rows))
        assert all([c for c in collections if c == cb_env.collection.name]) is True

        # check w/in kwargs
        res = cb_env.scope.search_query(self.TEST_INDEX_NAME, q, SearchOptions(
            limit=10), highlight_style=HighlightStyle.Html, collections=[cb_env.collection.name])
        rows = await cb_env.assert_search_rows_async(res, 1, return_rows=True)
        locations = rows[0].locations
        fragments = rows[0].fragments
        assert isinstance(locations, search.SearchRowLocations)
        assert isinstance(fragments, dict)
        assert all(map(lambda l: isinstance(l, search.SearchRowLocation), locations.get_all())) is True
        collections = list(map(lambda r: r.fields['_$c'], rows))
        assert all([c for c in collections if c == cb_env.collection.name]) is True
