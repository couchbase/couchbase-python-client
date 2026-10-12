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

"""FTS index setup, teardown and row validation for the txcouchbase search suites.

The third of the trio.  The Twisted search surface is complete, checked rather than
assumed: Cluster.search_query, Scope.search_query, Cluster.search, Scope.search, and both
index managers with the same fourteen methods the other two api styles carry.

The asynchrony sits in a different place from acouchbase, and that is the whole of the
difference between this module and the async one:

  * `cluster.search_query()` returns a Deferred, so the *call* goes through the reactor
    and the suite bridges it.  `result.rows()` is then a plain iterator, so the drain here
    is an ordinary `for` and the two assertion helpers stay synchronous.  acouchbase is the
    mirror image: the call returns immediately and the rows are async.  Neither module is a
    relabelling of the other.
  * the manager wrapper already bridges its own calls, so it is passed to try_n_times with
    `is_deferred=False`.  Leaving the default would have the retry helper wait on a value
    that is not a Deferred.  Same as TwistedAnalyticsEnvironment.

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
from tests.assert_helpers import (cb_assert_eq,
                                  cb_assert_gte,
                                  cb_assert_isinstance)
from tests.environments.managers import (TwistedTestEnvironmentCollectionManager,
                                         TwistedTestEnvironmentScopeSearchIndexManager,
                                         TwistedTestEnvironmentSearchIndexManager)
from tests.environments.test_environment import CollectionStrategy
from tests.environments.twisted_environment import TwistedTestEnvironment
from tests.search_index_defs import (collection_index_name,
                                     collection_index_params,
                                     default_index_name,
                                     default_index_params)
from tests.skips import CouchbaseTestEnvironmentException
from tests.test_features import EnvironmentFeatures, ServerFeatures

_SearchIndexManager = Union[TwistedTestEnvironmentSearchIndexManager,
                            TwistedTestEnvironmentScopeSearchIndexManager]


class TwistedSearchEnvironment:
    """Twisted FTS index setup, teardown and row validation."""

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
    def _drain(result: SearchResult, expected_count: int) -> list[SearchRow]:
        """Validate every row and the metadata in one pass, and return the rows."""
        cb_assert_isinstance(result, SearchResult, what='result passed to _drain')
        rows: list[SearchRow] = []
        for row in result.rows():
            assert row is not None, f'search result yielded a None row at index {len(rows)}'
            TwistedSearchEnvironment._validate_row(row)
            rows.append(row)
        TwistedSearchEnvironment._validate_metadata(result, expected_count)
        return rows

    @staticmethod
    def assert_rows_exact(result: SearchResult, expected_count: int) -> list[SearchRow]:
        """Assert the result yields exactly expected_count validated rows, and return them."""
        rows = TwistedSearchEnvironment._drain(result, expected_count)
        cb_assert_eq(len(rows), expected_count, what='search rows returned')
        return rows

    @staticmethod
    def assert_rows_at_least(result: SearchResult, expected_count: int) -> list[SearchRow]:
        """Assert the result yields at least expected_count validated rows, and return them."""
        rows = TwistedSearchEnvironment._drain(result, expected_count)
        cb_assert_gte(len(rows), expected_count, what='search rows returned')
        return rows

    # ------------------------------------------------------------------
    # Index lifecycle
    # ------------------------------------------------------------------

    @staticmethod
    def index_name(collection_strategy: CollectionStrategy) -> str:
        """The index the suite queries, worker-suffixed under xdist."""
        if collection_strategy == CollectionStrategy.DEFAULT:
            return default_index_name()
        return collection_index_name()

    @staticmethod
    def _index_params(cb_env: TwistedTestEnvironment, collection_strategy: CollectionStrategy) -> dict[str, Any]:
        if collection_strategy == CollectionStrategy.DEFAULT:
            return default_index_params()
        return collection_index_params(cb_env.scope.name,
                                       [cb_env.collection.name, TwistedSearchEnvironment.OTHER_COLLECTION])

    @staticmethod
    def _create_and_load_other_collection(cb_env: TwistedTestEnvironment) -> None:
        """Add a second collection to the suite's scope and seed it with FTS-matchable docs.

        The collection manager bridges its own calls, so it goes to try_n_times with
        `is_deferred=False`.  `collection.upsert` does not: it returns a Deferred, so the
        default is right there and the two calls below are deliberately not the same.
        """
        coll_mgr = cb_env.get_manager(TwistedTestEnvironmentCollectionManager)
        TwistedTestEnvironment.try_n_times(5, 3, coll_mgr.add_collection,
                                           cb_env.scope.name, TwistedSearchEnvironment.OTHER_COLLECTION,
                                           is_deferred=False)

        coll = cb_env.scope.collection(TwistedSearchEnvironment.OTHER_COLLECTION)
        for doc in cb_env.data_provider.get_new_vehicles(TwistedSearchEnvironment.OTHER_COLLECTION_DOCS):
            TwistedTestEnvironment.try_n_times(3, 3, coll.upsert, f'{doc["id"]}', doc)

    @staticmethod
    def _index_manager(cb_env: TwistedTestEnvironment,
                       collection_strategy: CollectionStrategy) -> _SearchIndexManager:
        """The manager that owns the index for this strategy; see the sync module docstring."""
        if collection_strategy == CollectionStrategy.DEFAULT:
            return cb_env.get_manager(TwistedTestEnvironmentSearchIndexManager)
        return cb_env.get_manager(TwistedTestEnvironmentScopeSearchIndexManager)

    @staticmethod
    def _required_feature(collection_strategy: CollectionStrategy) -> ServerFeatures:
        if collection_strategy == CollectionStrategy.DEFAULT:
            return ServerFeatures.SearchIndexManagement
        return ServerFeatures.ScopeSearchIndexManagement

    @staticmethod
    def _wait_for_indexing(sixm: _SearchIndexManager, idx_name: str) -> int:
        """Poll until the index reports MIN_INDEXED_DOCS documents; return what it reported."""
        deadline = time.monotonic() + TwistedSearchEnvironment.NO_DOCS_CUTOFF
        rounds_with_docs = 0
        while True:
            indexed_docs = TwistedTestEnvironment.try_n_times(10, 10, sixm.get_indexed_documents_count, idx_name,
                                                              is_deferred=False)
            if indexed_docs >= TwistedSearchEnvironment.MIN_INDEXED_DOCS:
                break
            if indexed_docs == 0:
                if time.monotonic() >= deadline:
                    break
            else:
                rounds_with_docs += 1
                if rounds_with_docs >= TwistedSearchEnvironment.INDEX_WAIT_ROUNDS:
                    break
            print(f'Found {indexed_docs} indexed docs, waiting a bit...')
            TwistedTestEnvironment.sleep(TwistedSearchEnvironment.INDEX_WAIT_DELAY)

        return indexed_docs

    @staticmethod
    def setup_search(cb_env: TwistedTestEnvironment, collection_strategy: CollectionStrategy) -> None:
        """Create the index the suite queries and wait for it to hold documents."""
        EnvironmentFeatures.check_if_feature_supported(
            TwistedSearchEnvironment._required_feature(collection_strategy), cb_env.server_context)
        if collection_strategy != CollectionStrategy.DEFAULT:
            TwistedSearchEnvironment._create_and_load_other_collection(cb_env)

        sixm = TwistedSearchEnvironment._index_manager(cb_env, collection_strategy)
        idx_name = TwistedSearchEnvironment.index_name(collection_strategy)
        params = TwistedSearchEnvironment._index_params(cb_env, collection_strategy)
        TwistedTestEnvironment.try_n_times(10, 3, sixm.upsert_index,
                                           SearchIndex(name=idx_name,
                                                       idx_type='fulltext-index',
                                                       source_name=cb_env.bucket.name,
                                                       source_type='couchbase',
                                                       params=params),
                                           is_deferred=False)

        if TwistedSearchEnvironment._wait_for_indexing(sixm, idx_name) == 0:
            raise CouchbaseTestEnvironmentException(f'No docs indexed into {idx_name}')

    @staticmethod
    def teardown_search(cb_env: TwistedTestEnvironment, collection_strategy: CollectionStrategy) -> None:
        sixm = TwistedSearchEnvironment._index_manager(cb_env, collection_strategy)
        TwistedTestEnvironment.try_n_times_till_exception(
            10, 3, sixm.drop_index,
            TwistedSearchEnvironment.index_name(collection_strategy),
            is_deferred=False,
            expected_exceptions=(SearchIndexNotFoundException,))
