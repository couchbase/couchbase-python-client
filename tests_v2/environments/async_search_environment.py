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

"""FTS index setup, teardown and row validation for the acouchbase search suites.

The async counterpart of search_environment.py, and the same shape.  Three things differ
from the sync module, all forced by the SDK rather than chosen:

  * every manager call is awaited, since AsyncTestEnvironmentSearchIndexManager returns
    awaitables where the sync wrapper returns values.  Dropping one on upsert_index leaves
    the index uncreated while setup carries on to wait for documents in it.
  * `cluster.search_query()` still returns its result object synchronously; it is the
    *rows* that are an async iterator.  So the drain is `async for`, and the two assertion
    helpers become coroutines with it, since the count they assert on is only known once
    the stream has been consumed.  `metadata()` and `facets()` stay synchronous in all
    three api styles.
  * the drain cannot be `for row in result.rows()`.  That does not raise over an async
    iterator, it iterates nothing, so every assertion would compare against an empty list
    and `assert_rows_exact(result, 0)` in particular would pass on any result at all.
    Same trap as async_query_environment._wait_for_query_index; see its docstring.

Everything else, including which of the two assertions a given call site wants, is settled
in the sync module's docstring and not restated here.
"""

from __future__ import annotations

import time
from datetime import timedelta
from typing import Any, Union

from couchbase.exceptions import SearchIndexNotFoundException
from couchbase.management.search import SearchIndex
from couchbase.result import SearchResult
from couchbase.search import (SearchMetaData,
                              SearchMetrics,
                              SearchRow,
                              SearchRowFields,
                              SearchRowLocations)
from tests_v2.assert_helpers import (cb_assert_eq,
                                     cb_assert_gte,
                                     cb_assert_isinstance)
from tests_v2.environments.async_environment import AsyncTestEnvironment
from tests_v2.environments.managers import (AsyncTestEnvironmentCollectionManager,
                                            AsyncTestEnvironmentScopeSearchIndexManager,
                                            AsyncTestEnvironmentSearchIndexManager)
from tests_v2.environments.test_environment import CollectionStrategy
from tests_v2.search_index_defs import (collection_index_name,
                                        collection_index_params,
                                        default_index_name,
                                        default_index_params)
from tests_v2.skips import CouchbaseTestEnvironmentException
from tests_v2.test_features import EnvironmentFeatures, ServerFeatures

_SearchIndexManager = Union[AsyncTestEnvironmentSearchIndexManager,
                            AsyncTestEnvironmentScopeSearchIndexManager]


class AsyncSearchEnvironment:
    """Asyncio FTS index setup, teardown and row validation."""

    OTHER_COLLECTION = 'other-collection'
    OTHER_COLLECTION_DOCS = 25
    MIN_INDEXED_DOCS = 20
    INDEX_WAIT_ROUNDS = 10
    INDEX_WAIT_DELAY = 3
    NO_DOCS_CUTOFF = 300

    # ------------------------------------------------------------------
    # Row and metadata validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_row(row: SearchRow) -> None:
        cb_assert_isinstance(row, SearchRow, what='search row')
        cb_assert_isinstance(row.index, str, what='search row index')
        cb_assert_isinstance(row.id, str, what='search row id')
        cb_assert_isinstance(row.score, float, what='search row score')
        cb_assert_isinstance(row.explanation, dict, what='search row explanation')
        cb_assert_isinstance(row.fragments, dict, what='search row fragments')
        if row.locations:
            cb_assert_isinstance(row.locations, SearchRowLocations, what='search row locations')
        if row.fields:
            cb_assert_isinstance(row.fields, SearchRowFields, what='search row fields')

    @staticmethod
    def _validate_metadata(result: SearchResult, expected_count: int) -> None:
        meta = result.metadata()
        cb_assert_isinstance(meta, SearchMetaData, what='search result metadata')
        metrics = meta.metrics()
        cb_assert_isinstance(metrics, SearchMetrics, what='search result metrics')
        errored = metrics.error_partition_count()
        succeeded = metrics.success_partition_count()
        cb_assert_isinstance(errored, int, what='search metrics error_partition_count')
        cb_assert_isinstance(succeeded, int, what='search metrics success_partition_count')
        cb_assert_isinstance(metrics.max_score(), float, what='search metrics max_score')
        cb_assert_isinstance(metrics.total_partition_count(), int, what='search metrics total_partition_count')
        cb_assert_eq(errored + succeeded, metrics.total_partition_count(),
                     what='search metrics partition counts',
                     ctx='every partition has to be counted as one or the other')
        cb_assert_isinstance(metrics.took(), timedelta, what='search metrics took')
        cb_assert_gte(metrics.took().total_seconds(), 0, what='search metrics took, in seconds')
        cb_assert_gte(metrics.total_rows(), expected_count, what='search metrics total_rows')

    @staticmethod
    async def _drain(result: SearchResult, expected_count: int) -> list[SearchRow]:
        """Validate every row and the metadata in one pass, and return the rows."""
        cb_assert_isinstance(result, SearchResult, what='result passed to _drain')
        rows: list[SearchRow] = []
        async for row in result.rows():
            assert row is not None, f'search result yielded a None row at index {len(rows)}'
            AsyncSearchEnvironment._validate_row(row)
            rows.append(row)
        AsyncSearchEnvironment._validate_metadata(result, expected_count)
        return rows

    @staticmethod
    async def assert_rows_exact(result: SearchResult, expected_count: int) -> list[SearchRow]:
        """Assert the result yields exactly expected_count validated rows, and return them."""
        rows = await AsyncSearchEnvironment._drain(result, expected_count)
        cb_assert_eq(len(rows), expected_count, what='search rows returned')
        return rows

    @staticmethod
    async def assert_rows_at_least(result: SearchResult, expected_count: int) -> list[SearchRow]:
        """Assert the result yields at least expected_count validated rows, and return them."""
        rows = await AsyncSearchEnvironment._drain(result, expected_count)
        cb_assert_gte(len(rows), expected_count, what='search rows returned')
        return rows

    # ------------------------------------------------------------------
    # Index lifecycle
    # ------------------------------------------------------------------

    @staticmethod
    def index_name(collection_strategy: CollectionStrategy) -> str:
        """The index the suite queries, worker-suffixed under xdist.

        Pure string arithmetic, no I/O, so it stays synchronous in all three api styles.
        """
        if collection_strategy == CollectionStrategy.DEFAULT:
            return default_index_name()
        return collection_index_name()

    @staticmethod
    def _index_params(cb_env: AsyncTestEnvironment, collection_strategy: CollectionStrategy) -> dict[str, Any]:
        if collection_strategy == CollectionStrategy.DEFAULT:
            return default_index_params()
        return collection_index_params(cb_env.scope.name,
                                       [cb_env.collection.name, AsyncSearchEnvironment.OTHER_COLLECTION])

    @staticmethod
    async def _create_and_load_other_collection(cb_env: AsyncTestEnvironment) -> None:
        """Add a second collection to the suite's scope and seed it with FTS-matchable docs."""
        coll_mgr = cb_env.get_manager(AsyncTestEnvironmentCollectionManager)
        await AsyncTestEnvironment.try_n_times(5, 3, coll_mgr.add_collection,
                                               cb_env.scope.name, AsyncSearchEnvironment.OTHER_COLLECTION)

        coll = cb_env.scope.collection(AsyncSearchEnvironment.OTHER_COLLECTION)
        for doc in cb_env.data_provider.get_new_vehicles(AsyncSearchEnvironment.OTHER_COLLECTION_DOCS):
            await AsyncTestEnvironment.try_n_times(3, 3, coll.upsert, f'{doc["id"]}', doc)

    @staticmethod
    def _index_manager(cb_env: AsyncTestEnvironment,
                       collection_strategy: CollectionStrategy) -> _SearchIndexManager:
        """The manager that owns the index for this strategy; see the sync module docstring."""
        if collection_strategy == CollectionStrategy.DEFAULT:
            return cb_env.get_manager(AsyncTestEnvironmentSearchIndexManager)
        return cb_env.get_manager(AsyncTestEnvironmentScopeSearchIndexManager)

    @staticmethod
    def _required_feature(collection_strategy: CollectionStrategy) -> ServerFeatures:
        if collection_strategy == CollectionStrategy.DEFAULT:
            return ServerFeatures.SearchIndexManagement
        return ServerFeatures.ScopeSearchIndexManagement

    @staticmethod
    async def _wait_for_indexing(sixm: _SearchIndexManager, idx_name: str) -> int:
        """Poll until the index reports MIN_INDEXED_DOCS documents; return what it reported."""
        deadline = time.monotonic() + AsyncSearchEnvironment.NO_DOCS_CUTOFF
        rounds_with_docs = 0
        while True:
            indexed_docs = await AsyncTestEnvironment.try_n_times(10, 10, sixm.get_indexed_documents_count, idx_name)
            if indexed_docs >= AsyncSearchEnvironment.MIN_INDEXED_DOCS:
                break
            if indexed_docs == 0:
                if time.monotonic() >= deadline:
                    break
            else:
                rounds_with_docs += 1
                if rounds_with_docs >= AsyncSearchEnvironment.INDEX_WAIT_ROUNDS:
                    break
            print(f'Found {indexed_docs} indexed docs, waiting a bit...')
            await AsyncTestEnvironment.sleep(AsyncSearchEnvironment.INDEX_WAIT_DELAY)

        return indexed_docs

    @staticmethod
    async def setup_search(cb_env: AsyncTestEnvironment, collection_strategy: CollectionStrategy) -> None:
        """Create the index the suite queries and wait for it to hold documents."""
        EnvironmentFeatures.check_if_feature_supported(AsyncSearchEnvironment._required_feature(collection_strategy),
                                                       cb_env.server_context)
        if collection_strategy != CollectionStrategy.DEFAULT:
            await AsyncSearchEnvironment._create_and_load_other_collection(cb_env)

        sixm = AsyncSearchEnvironment._index_manager(cb_env, collection_strategy)
        idx_name = AsyncSearchEnvironment.index_name(collection_strategy)
        params = AsyncSearchEnvironment._index_params(cb_env, collection_strategy)
        await AsyncTestEnvironment.try_n_times(10, 3, sixm.upsert_index,
                                               SearchIndex(name=idx_name,
                                                           idx_type='fulltext-index',
                                                           source_name=cb_env.bucket.name,
                                                           source_type='couchbase',
                                                           params=params))

        if await AsyncSearchEnvironment._wait_for_indexing(sixm, idx_name) == 0:
            raise CouchbaseTestEnvironmentException(f'No docs indexed into {idx_name}')

    @staticmethod
    async def teardown_search(cb_env: AsyncTestEnvironment, collection_strategy: CollectionStrategy) -> None:
        sixm = AsyncSearchEnvironment._index_manager(cb_env, collection_strategy)
        await AsyncTestEnvironment.try_n_times_till_exception(
            10, 3, sixm.drop_index,
            AsyncSearchEnvironment.index_name(collection_strategy),
            expected_exceptions=(SearchIndexNotFoundException,))
