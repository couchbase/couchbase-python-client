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

"""FTS index setup, teardown and row validation for the classic/sync search suites.

One module per api style, one class of static helpers, no TestEnvironment subclass.
Named-collection creation, data loading and purging are the framework's, not this module's.

Two setup paths, chosen by the suite's CollectionStrategy rather than decided here:

  * DEFAULT builds a bucket-wide index keyed on the documents' own `type` field, and the
    suite queries it through cluster.search_query.
  * NAMED creates a second collection beside the suite's own, seeds it, and builds a
    collection-aware index over both, which is what lets a scope-level query filter on
    `collections=[...]` and read back `_$c`.

**The two paths also differ in which manager creates the index, and that is not a style
choice.** A scope-level query goes to /api/bucket/<b>/scope/<s>/index/<name>/query, which
resolves only indexes created *in that scope*; a cluster-level index of the same name is
invisible there and the server answers `index not found` rather than an empty result.  So
the NAMED path upserts through TestEnvironmentScopeSearchIndexManager and DEFAULT through
the cluster-level one, and _index_manager() is the single place that decides.

The index definitions are built in tests_v2/search_index_defs.py rather than read from a
checked-in JSON fixture; that module's docstring has the reason, and it is xdist, not
tidiness.

**Row assertions live here, not on TestEnvironment.** A search result cannot be validated
the way a query result is: every row carries an index name, a score, locations and
fragments worth checking, and the metadata carries partition counts that must add up.  All
of that has to happen in the single pass that drains the stream, so it cannot be a
validate() called beside cb_env.assert_rows_exact().  The two names mirror that pair
deliberately, and carry the same rule: there is no bare assert_rows and no `exact=` flag,
because a default is a silent policy.

Which of the two a suite wants is not a style question here.  The bucket-wide index sees
every `type: vehicle` document in the shared bucket, including whatever another suite
seeded, so a term query's match count is genuinely outside the test's control and
at_least is the honest assertion.  The counts that *are* knowable are the ones where the
test wrote the document it is looking for, or where it expects to find nothing at all.
"""

from __future__ import annotations

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
from tests_v2.environments.managers import (TestEnvironmentCollectionManager,
                                            TestEnvironmentScopeSearchIndexManager,
                                            TestEnvironmentSearchIndexManager)
from tests_v2.environments.test_environment import (CollectionStrategy,
                                                    SyncTestEnvironment,
                                                    TestEnvironment)
from tests_v2.search_index_defs import (collection_index_name,
                                        collection_index_params,
                                        default_index_name,
                                        default_index_params)
from tests_v2.skips import CouchbaseTestEnvironmentException
from tests_v2.test_features import EnvironmentFeatures, ServerFeatures

# Both classes wrap the same five methods the index lifecycle here calls, but they are
# siblings rather than one subclassing the other, so the union is what names them.
_SearchIndexManager = Union[TestEnvironmentSearchIndexManager, TestEnvironmentScopeSearchIndexManager]


class SearchEnvironment:
    """Classic/sync FTS index setup, teardown and row validation."""

    OTHER_COLLECTION = 'other-collection'

    # Documents seeded into OTHER_COLLECTION.  Only the NAMED path creates it, and the only
    # thing the suite asks of it is that a scope-level query without a `collections` filter
    # returns rows from both collections, so this needs to be big enough that the keyword
    # fraction lands rather than large.
    OTHER_COLLECTION_DOCS = 25

    # How many indexed documents count as "the index is live".  A completeness check would
    # be a race: FTS indexing is asynchronous and the bucket is shared, so the true total is
    # neither knowable nor stable.  Twenty is enough to prove the index is serving.
    MIN_INDEXED_DOCS = 20

    # Rounds and seconds between them while waiting for the index to report documents, and
    # the point at which a still-empty index is called a failure rather than a slow one.
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
        # Both are None unless the query asked for them, which is what the
        # include_locations and fields suites assert on directly.
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
        # total_rows() counts matches, not rows returned, so it stays >= even where the row
        # count itself is exact: every query in these suites carries a limit.
        cb_assert_gte(metrics.total_rows(), expected_count, what='search metrics total_rows')

    @staticmethod
    def _drain(result: SearchResult, expected_count: int) -> list[SearchRow]:
        """Validate every row and the metadata in one pass, and return the rows."""
        cb_assert_isinstance(result, SearchResult, what='result passed to _drain')
        rows: list[SearchRow] = []
        for row in result.rows():
            assert row is not None, f'search result yielded a None row at index {len(rows)}'
            SearchEnvironment._validate_row(row)
            rows.append(row)
        SearchEnvironment._validate_metadata(result, expected_count)
        return rows

    @staticmethod
    def assert_rows_exact(result: SearchResult, expected_count: int) -> list[SearchRow]:
        """Assert the result yields exactly expected_count validated rows, and return them.

        For the queries whose answer the test itself created: a term the test just wrote
        into a document, or a term nothing in the corpus can match.  The rows come back
        rather than being handed over behind a `return_rows=True` flag, so a caller that
        wants them and a caller that does not read the same at the call site.
        """
        rows = SearchEnvironment._drain(result, expected_count)
        cb_assert_eq(len(rows), expected_count, what='search rows returned')
        return rows

    @staticmethod
    def assert_rows_at_least(result: SearchResult, expected_count: int) -> list[SearchRow]:
        """Assert the result yields at least expected_count validated rows, and return them.

        The honest assertion for anything querying the bucket-wide index: it covers every
        `type: vehicle` document in a shared bucket, so another suite's corpus can legally
        add matches.  Use assert_rows_exact wherever the count is knowable.
        """
        rows = SearchEnvironment._drain(result, expected_count)
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
    def _index_params(cb_env: SyncTestEnvironment, collection_strategy: CollectionStrategy) -> dict[str, Any]:
        if collection_strategy == CollectionStrategy.DEFAULT:
            return default_index_params()
        return collection_index_params(cb_env.scope.name,
                                       [cb_env.collection.name, SearchEnvironment.OTHER_COLLECTION])

    @staticmethod
    def _create_and_load_other_collection(cb_env: SyncTestEnvironment) -> None:
        """Add a second collection to the suite's scope and seed it with FTS-matchable docs.

        The documents come from data_provider.get_new_vehicles(), plural, which applies the
        same deterministic keyword injection the seeded corpus gets.  The singular
        get_new_vehicle() does not, because the injection belongs to the corpus builder rather
        than the per-document factory: that is what fixes the keyword fraction instead of
        leaving it to a coin flip per document.  A document minted one at a time is therefore
        never FTS-matchable, and seeding this collection through the singular form yields an
        index the suite's queries cannot match.

        Nothing here needs undoing: teardown drops the scope, which takes both collections.
        """
        coll_mgr = cb_env.get_manager(TestEnvironmentCollectionManager)
        TestEnvironment.try_n_times(5, 3, coll_mgr.add_collection,
                                    cb_env.scope.name, SearchEnvironment.OTHER_COLLECTION)

        coll = cb_env.scope.collection(SearchEnvironment.OTHER_COLLECTION)
        for doc in cb_env.data_provider.get_new_vehicles(SearchEnvironment.OTHER_COLLECTION_DOCS):
            TestEnvironment.try_n_times(3, 3, coll.upsert, f'{doc["id"]}', doc)

    @staticmethod
    def _index_manager(cb_env: SyncTestEnvironment,
                       collection_strategy: CollectionStrategy) -> _SearchIndexManager:
        """The manager that owns the index for this strategy; see the module docstring."""
        if collection_strategy == CollectionStrategy.DEFAULT:
            return cb_env.get_manager(TestEnvironmentSearchIndexManager)
        return cb_env.get_manager(TestEnvironmentScopeSearchIndexManager)

    @staticmethod
    def _required_feature(collection_strategy: CollectionStrategy) -> ServerFeatures:
        """The feature the index build needs, which follows the manager it goes through."""
        if collection_strategy == CollectionStrategy.DEFAULT:
            return ServerFeatures.SearchIndexManagement
        return ServerFeatures.ScopeSearchIndexManagement

    @staticmethod
    def _wait_for_indexing(sixm: _SearchIndexManager, idx_name: str) -> int:
        """Poll until the index reports MIN_INDEXED_DOCS documents; return what it reported.

        Returns 0 when the index is still empty after NO_DOCS_CUTOFF seconds, which the
        caller treats as a failure.  An index that is merely *behind* still returns its
        count and the suite proceeds: the row assertions are the real check.
        """
        indexed_docs = 0
        for i in range(SearchEnvironment.INDEX_WAIT_ROUNDS):
            if indexed_docs == 0 and i * SearchEnvironment.INDEX_WAIT_DELAY >= SearchEnvironment.NO_DOCS_CUTOFF:
                return 0
            indexed_docs = TestEnvironment.try_n_times(10, 10, sixm.get_indexed_documents_count, idx_name)
            if indexed_docs >= SearchEnvironment.MIN_INDEXED_DOCS:
                break
            print(f'Found {indexed_docs} indexed docs, waiting a bit...')
            TestEnvironment.sleep(SearchEnvironment.INDEX_WAIT_DELAY)

        return indexed_docs

    @staticmethod
    def setup_search(cb_env: SyncTestEnvironment, collection_strategy: CollectionStrategy) -> None:
        """Create the index the suite queries and wait for it to hold documents.

        The feature check runs first and runs here rather than in the suite, so it takes the
        whole class with it on a mock or on a cluster with no search node.  Reaching
        upsert_index without it fails inside the manager instead, which reads as an SDK
        defect.
        """
        EnvironmentFeatures.check_if_feature_supported(SearchEnvironment._required_feature(collection_strategy),
                                                       cb_env.server_context)
        if collection_strategy != CollectionStrategy.DEFAULT:
            SearchEnvironment._create_and_load_other_collection(cb_env)

        sixm = SearchEnvironment._index_manager(cb_env, collection_strategy)
        idx_name = SearchEnvironment.index_name(collection_strategy)
        TestEnvironment.try_n_times(10, 3, sixm.upsert_index,
                                    SearchIndex(name=idx_name,
                                                idx_type='fulltext-index',
                                                source_name=cb_env.bucket.name,
                                                source_type='couchbase',
                                                params=SearchEnvironment._index_params(cb_env,
                                                                                       collection_strategy)))

        if SearchEnvironment._wait_for_indexing(sixm, idx_name) == 0:
            raise CouchbaseTestEnvironmentException(f'No docs indexed into {idx_name}')

    @staticmethod
    def teardown_search(cb_env: SyncTestEnvironment, collection_strategy: CollectionStrategy) -> None:
        sixm = SearchEnvironment._index_manager(cb_env, collection_strategy)
        TestEnvironment.try_n_times_till_exception(10, 3, sixm.drop_index,
                                                   SearchEnvironment.index_name(collection_strategy),
                                                   expected_exceptions=(SearchIndexNotFoundException,))
