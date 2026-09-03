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

"""Query-index setup/teardown helpers for the classic/sync query test suites.

One module per api style, mirroring the test_environment.py/async_environment.py
split for import hygiene: async_query_environment.py and twisted_query_environment.py
follow this same shape rather than living here.
Within a module, one class per wire protocol that actually exists. QueryEnvironment
is CLASSIC; a Couchbase2QueryEnvironment class would join it here if COUCHBASE2 needs
its own setup path. Not pre-built ahead of time.
"""

from __future__ import annotations

from datetime import timedelta

from couchbase.exceptions import QueryIndexAlreadyExistsException, QueryIndexNotFoundException
from tests_v2.environments.managers import TestEnvironmentQueryIndexManager
from tests_v2.environments.test_environment import (CollectionStrategy,
                                                    SyncTestEnvironment,
                                                    TestEnvironment)
from tests_v2.test_features import EnvironmentFeatures, ServerFeatures


class QueryEnvironment:
    """Classic/sync query-index setup and teardown."""

    # Rounds and seconds between them while creating or dropping the primary index, so thirty
    # seconds.  One budget covers the manager API and the raw statements below: they are two
    # ways of issuing the same DDL, and nothing would record why one had been given more
    # patience than the other.
    PRIMARY_INDEX_ROUNDS = 10
    PRIMARY_INDEX_DELAY = 3

    # Rounds and seconds between them while waiting for the new index to cover the batch this
    # run loaded, so twenty-five seconds.
    INDEX_LOAD_ROUNDS = 5
    INDEX_LOAD_DELAY = 5

    # Rows the wait loop needs to see before it calls the index loaded.  It is the query's LIMIT
    # as well, so the loop cannot end up waiting for more rows than the query can return.
    INDEX_LOAD_ROWS = 5

    @staticmethod
    def _create_primary_index_raw(cb_env: SyncTestEnvironment) -> None:
        q_str = f'CREATE PRIMARY INDEX `#primary` on `{cb_env.bucket.name}`'
        for _ in range(QueryEnvironment.PRIMARY_INDEX_ROUNDS):
            try:
                cb_env.cluster.query(q_str).execute()
            except QueryIndexAlreadyExistsException:
                break
            TestEnvironment.sleep(QueryEnvironment.PRIMARY_INDEX_DELAY)

    @staticmethod
    def _drop_primary_index_raw(cb_env: SyncTestEnvironment) -> None:
        q_str = f'DROP PRIMARY INDEX on `{cb_env.bucket.name}`'
        for _ in range(QueryEnvironment.PRIMARY_INDEX_ROUNDS):
            try:
                cb_env.cluster.query(q_str).execute()
            except QueryIndexNotFoundException:
                break
            TestEnvironment.sleep(QueryEnvironment.PRIMARY_INDEX_DELAY)

    @staticmethod
    def _wait_for_query_index(cb_env: SyncTestEnvironment, query_namespace: str) -> None:
        batch = cb_env.get_batch_id()
        rows = QueryEnvironment.INDEX_LOAD_ROWS
        for _ in range(QueryEnvironment.INDEX_LOAD_ROUNDS):
            q_str = f"SELECT * FROM {query_namespace} WHERE batch LIKE '{batch}%' LIMIT {rows}"
            result = cb_env.cluster.query(q_str)
            count = sum(1 for _ in result.rows())
            if count >= rows:
                return
            print('Waiting for index to load, sleeping a bit...')
            TestEnvironment.sleep(QueryEnvironment.INDEX_LOAD_DELAY)

    @staticmethod
    def _supports_manager_api(cb_env: SyncTestEnvironment) -> bool:
        """True when the query index manager API can be used instead of raw statements.

        Names the condition the four branches below share, rather than spelling
        `cb_env.server_version_short > 6.6` in each.  See ServerFeatures.QueryContext.
        """
        return EnvironmentFeatures.is_feature_supported(ServerFeatures.QueryContext, cb_env.server_context)

    @staticmethod
    def setup_query_indexes(cb_env: SyncTestEnvironment, collection_strategy: CollectionStrategy) -> None:
        # Called after build_environment(), so data is already loaded; the primary
        # index build therefore scans existing docs rather than an empty collection.
        # Runs inside a class-scoped fixture, so this skip takes the whole query
        # suite with it on the mock rather than failing test by test.
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.QueryIndexManagement, cb_env.server_context)
        if collection_strategy == CollectionStrategy.NAMED:
            qixm = cb_env.get_manager(TestEnvironmentQueryIndexManager)
            TestEnvironment.try_n_times(QueryEnvironment.PRIMARY_INDEX_ROUNDS,
                                        QueryEnvironment.PRIMARY_INDEX_DELAY,
                                        qixm.create_primary_index, cb_env.bucket.name,
                                        scope_name=cb_env.scope.name, collection_name=cb_env.collection.name,
                                        timeout=timedelta(seconds=60), ignore_if_exists=True)
            query_namespace = cb_env.fqdn
        else:
            if QueryEnvironment._supports_manager_api(cb_env):
                qixm = cb_env.get_manager(TestEnvironmentQueryIndexManager)
                TestEnvironment.try_n_times(QueryEnvironment.PRIMARY_INDEX_ROUNDS,
                                            QueryEnvironment.PRIMARY_INDEX_DELAY,
                                            qixm.create_primary_index, cb_env.bucket.name,
                                            timeout=timedelta(seconds=60), ignore_if_exists=True)
            else:
                QueryEnvironment._create_primary_index_raw(cb_env)
            query_namespace = f'`{cb_env.bucket.name}`'
        QueryEnvironment._wait_for_query_index(cb_env, query_namespace)

    @staticmethod
    def teardown_query_indexes(cb_env: SyncTestEnvironment, collection_strategy: CollectionStrategy) -> None:
        if collection_strategy == CollectionStrategy.NAMED:
            qixm = cb_env.get_manager(TestEnvironmentQueryIndexManager)
            TestEnvironment.try_n_times_till_exception(QueryEnvironment.PRIMARY_INDEX_ROUNDS,
                                                       QueryEnvironment.PRIMARY_INDEX_DELAY,
                                                       qixm.drop_primary_index, cb_env.bucket.name,
                                                       scope_name=cb_env.scope.name,
                                                       collection_name=cb_env.collection.name,
                                                       expected_exceptions=(QueryIndexNotFoundException,))
        else:
            if QueryEnvironment._supports_manager_api(cb_env):
                qixm = cb_env.get_manager(TestEnvironmentQueryIndexManager)
                TestEnvironment.try_n_times_till_exception(QueryEnvironment.PRIMARY_INDEX_ROUNDS,
                                                           QueryEnvironment.PRIMARY_INDEX_DELAY,
                                                           qixm.drop_primary_index, cb_env.bucket.name,
                                                           expected_exceptions=(QueryIndexNotFoundException,))
            else:
                QueryEnvironment._drop_primary_index_raw(cb_env)
