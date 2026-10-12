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

"""Query-index setup/teardown helpers for the txcouchbase query test suites.

The third of the trio, the same shape as query_environment.py and
async_query_environment.py.

Three things differ from the sync module, and each is forced by the SDK rather than
chosen:

  * `cluster.query()` returns a Deferred, so the *call* goes through the reactor.
    `result.rows()` is then a plain iterator and the drain is an ordinary `for`.
    acouchbase is the mirror image, and neither module is a relabelling of the other.
  * there is no `.execute()`.  `txcouchbase.n1ql.N1QLRequest` defines `execute_query()`
    instead, so the shared `QueryResult.execute()` raises AttributeError here.  Both raw
    statement helpers drain rows() instead, which is what `.execute()` did anyway.
  * the manager wrapper bridges its own calls, so it goes to try_n_times with
    `is_deferred=False`.  Leaving the default would have the retry helper wait on a value
    that is not a Deferred.  Same as TwistedAnalyticsEnvironment.

Both raw helpers exist for servers older than the QueryContext feature, where the manager
API is unavailable.  They are unreachable on any currently supported server and are
carried for parity with the other two modules rather than because they are expected to
run.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from couchbase.exceptions import (CouchbaseException,
                                  InternalServerFailureException,
                                  QueryIndexAlreadyExistsException,
                                  QueryIndexNotFoundException)
from tests.environments.managers import TwistedTestEnvironmentQueryIndexManager
from tests.environments.query_index_mgmt_environment import index_already_dropped, index_not_online
from tests.environments.test_environment import CollectionStrategy
from tests.environments.twisted_environment import TwistedTestEnvironment, run_in_reactor_thread
from tests.test_features import EnvironmentFeatures, ServerFeatures


class TwistedQueryEnvironment:
    """Twisted query-index setup and teardown."""

    # The same budgets the sync module states the reasoning for: thirty seconds of primary
    # index DDL, then twenty-five seconds waiting for the index to cover this run's batch.
    PRIMARY_INDEX_ROUNDS = 10
    PRIMARY_INDEX_DELAY = 3
    INDEX_LOAD_ROUNDS = 5
    INDEX_LOAD_DELAY = 5
    INDEX_LOAD_ROWS = 5

    @staticmethod
    def _drain(cb_env: TwistedTestEnvironment, statement: str) -> None:
        """Run a statement for its effect, draining the rows.

        Draining is what drives the query to completion.  It is also exactly what the sync
        module's `.execute()` does, which returns `[r for r in list(self)]` and discards it.
        """
        result = run_in_reactor_thread(cb_env.cluster.query, statement)
        for _ in result.rows():
            pass

    @staticmethod
    def _create_primary_index_raw(cb_env: TwistedTestEnvironment) -> None:
        q_str = f'CREATE PRIMARY INDEX `#primary` on `{cb_env.bucket.name}`'
        for _ in range(TwistedQueryEnvironment.PRIMARY_INDEX_ROUNDS):
            try:
                TwistedQueryEnvironment._drain(cb_env, q_str)
            except QueryIndexAlreadyExistsException:
                break
            TwistedTestEnvironment.sleep(TwistedQueryEnvironment.PRIMARY_INDEX_DELAY)

    @staticmethod
    def _drop_primary_index(qixm: TwistedTestEnvironmentQueryIndexManager, *args: Any, **kwargs: Any) -> None:
        # A drop repeated while the first is still being applied can come back as GSI 5000,
        # `Index does not exist`, instead of QueryIndexNotFoundException.  Both mean the index is
        # gone, and the teardown loop stops only on QueryIndexNotFoundException, so the GSI form
        # is raised as one.
        try:
            qixm.drop_primary_index(*args, **kwargs)
        except InternalServerFailureException as exc:
            if index_already_dropped(exc):
                raise QueryIndexNotFoundException(message=str(exc)) from exc
            raise

    @staticmethod
    def _drop_primary_index_raw(cb_env: TwistedTestEnvironment) -> None:
        q_str = f'DROP PRIMARY INDEX on `{cb_env.bucket.name}`'
        for _ in range(TwistedQueryEnvironment.PRIMARY_INDEX_ROUNDS):
            try:
                TwistedQueryEnvironment._drain(cb_env, q_str)
            except Exception as exc:
                if index_already_dropped(exc):
                    break
                raise
            TwistedTestEnvironment.sleep(TwistedQueryEnvironment.PRIMARY_INDEX_DELAY)

    @staticmethod
    def _wait_for_query_index(cb_env: TwistedTestEnvironment, query_namespace: str) -> None:
        batch = cb_env.get_batch_id()
        rows = TwistedQueryEnvironment.INDEX_LOAD_ROWS
        for _ in range(TwistedQueryEnvironment.INDEX_LOAD_ROUNDS):
            q_str = f"SELECT * FROM {query_namespace} WHERE batch LIKE '{batch}%' LIMIT {rows}"
            try:
                result = run_in_reactor_thread(cb_env.cluster.query, q_str)
                count = sum(1 for _ in result.rows())
            except CouchbaseException as exc:
                if not index_not_online(exc):
                    raise
                count = 0
            if count >= rows:
                return
            print('Waiting for index to load, sleeping a bit...')
            TwistedTestEnvironment.sleep(TwistedQueryEnvironment.INDEX_LOAD_DELAY)

    @staticmethod
    def _supports_manager_api(cb_env: TwistedTestEnvironment) -> bool:
        """True when the query index manager API can be used instead of raw statements.

        Pure version arithmetic, no I/O, so it stays synchronous in all three api styles.
        """
        return EnvironmentFeatures.is_feature_supported(ServerFeatures.QueryContext, cb_env.server_context)

    @staticmethod
    def setup_query_indexes(cb_env: TwistedTestEnvironment,
                            collection_strategy: CollectionStrategy) -> None:
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.QueryIndexManagement, cb_env.server_context)
        if collection_strategy == CollectionStrategy.NAMED:
            qixm = cb_env.get_manager(TwistedTestEnvironmentQueryIndexManager)
            TwistedTestEnvironment.try_n_times(TwistedQueryEnvironment.PRIMARY_INDEX_ROUNDS,
                                               TwistedQueryEnvironment.PRIMARY_INDEX_DELAY,
                                               qixm.create_primary_index, cb_env.bucket.name,
                                               is_deferred=False,
                                               scope_name=cb_env.scope.name,
                                               collection_name=cb_env.collection.name,
                                               timeout=timedelta(seconds=60), ignore_if_exists=True)
            query_namespace = cb_env.fqdn
        else:
            if TwistedQueryEnvironment._supports_manager_api(cb_env):
                qixm = cb_env.get_manager(TwistedTestEnvironmentQueryIndexManager)
                TwistedTestEnvironment.try_n_times(TwistedQueryEnvironment.PRIMARY_INDEX_ROUNDS,
                                                   TwistedQueryEnvironment.PRIMARY_INDEX_DELAY,
                                                   qixm.create_primary_index, cb_env.bucket.name,
                                                   is_deferred=False,
                                                   timeout=timedelta(seconds=60), ignore_if_exists=True)
            else:
                TwistedQueryEnvironment._create_primary_index_raw(cb_env)
            query_namespace = f'`{cb_env.bucket.name}`'
        TwistedQueryEnvironment._wait_for_query_index(cb_env, query_namespace)

    @staticmethod
    def teardown_query_indexes(cb_env: TwistedTestEnvironment,
                               collection_strategy: CollectionStrategy) -> None:
        if collection_strategy == CollectionStrategy.NAMED:
            qixm = cb_env.get_manager(TwistedTestEnvironmentQueryIndexManager)
            TwistedTestEnvironment.try_n_times_till_exception(TwistedQueryEnvironment.PRIMARY_INDEX_ROUNDS,
                                                              TwistedQueryEnvironment.PRIMARY_INDEX_DELAY,
                                                              TwistedQueryEnvironment._drop_primary_index,
                                                              qixm,
                                                              cb_env.bucket.name,
                                                              is_deferred=False,
                                                              scope_name=cb_env.scope.name,
                                                              collection_name=cb_env.collection.name,
                                                              expected_exceptions=(QueryIndexNotFoundException,))
        else:
            if TwistedQueryEnvironment._supports_manager_api(cb_env):
                qixm = cb_env.get_manager(TwistedTestEnvironmentQueryIndexManager)
                TwistedTestEnvironment.try_n_times_till_exception(TwistedQueryEnvironment.PRIMARY_INDEX_ROUNDS,
                                                                  TwistedQueryEnvironment.PRIMARY_INDEX_DELAY,
                                                                  TwistedQueryEnvironment._drop_primary_index,
                                                                  qixm,
                                                                  cb_env.bucket.name,
                                                                  is_deferred=False,
                                                                  expected_exceptions=(QueryIndexNotFoundException,))
            else:
                TwistedQueryEnvironment._drop_primary_index_raw(cb_env)
