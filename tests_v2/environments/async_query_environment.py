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

"""Query-index setup/teardown helpers for the acouchbase query test suites.

The async counterpart of query_environment.py, and the same shape.  Two things differ, and
both are forced by the SDK rather than chosen:

  * every manager call is awaited, since AsyncTestEnvironmentQueryIndexManager returns
    awaitables where the sync wrapper returns values.
  * `_wait_for_query_index` counts rows with `async for` rather than a generator
    expression.  `sum(1 for _ in result.rows())` over an async iterator does not raise, it
    yields 0, which would make the wait loop conclude the index is not ready and then give
    up after five rounds of sleeping.  It is the one line here that is not a mechanical
    counterpart of the sync body.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from couchbase.exceptions import (CouchbaseException,
                                  InternalServerFailureException,
                                  QueryIndexAlreadyExistsException,
                                  QueryIndexNotFoundException)
from tests_v2.environments.async_environment import AsyncTestEnvironment
from tests_v2.environments.managers import AsyncTestEnvironmentQueryIndexManager
from tests_v2.environments.query_index_mgmt_environment import index_already_dropped, index_not_online
from tests_v2.environments.test_environment import CollectionStrategy
from tests_v2.test_features import EnvironmentFeatures, ServerFeatures


class AsyncQueryEnvironment:
    """Asyncio query-index setup and teardown."""

    # The same budgets the sync module states the reasoning for: thirty seconds of primary
    # index DDL, then twenty-five seconds waiting for the index to cover this run's batch.
    PRIMARY_INDEX_ROUNDS = 10
    PRIMARY_INDEX_DELAY = 3
    INDEX_LOAD_ROUNDS = 5
    INDEX_LOAD_DELAY = 5
    INDEX_LOAD_ROWS = 5

    @staticmethod
    async def _create_primary_index_raw(cb_env: AsyncTestEnvironment) -> None:
        q_str = f'CREATE PRIMARY INDEX `#primary` on `{cb_env.bucket.name}`'
        for _ in range(AsyncQueryEnvironment.PRIMARY_INDEX_ROUNDS):
            try:
                await cb_env.cluster.query(q_str).execute()
            except QueryIndexAlreadyExistsException:
                break
            await AsyncTestEnvironment.sleep(AsyncQueryEnvironment.PRIMARY_INDEX_DELAY)

    @staticmethod
    async def _drop_primary_index(qixm: AsyncTestEnvironmentQueryIndexManager, *args: Any, **kwargs: Any) -> None:
        # A drop repeated while the first is still being applied can come back as GSI 5000,
        # `Index does not exist`, instead of QueryIndexNotFoundException.  Both mean the index is
        # gone, and the teardown loop stops only on QueryIndexNotFoundException, so the GSI form
        # is raised as one.
        try:
            await qixm.drop_primary_index(*args, **kwargs)
        except InternalServerFailureException as exc:
            if index_already_dropped(exc):
                raise QueryIndexNotFoundException(message=str(exc)) from exc
            raise

    @staticmethod
    async def _drop_primary_index_raw(cb_env: AsyncTestEnvironment) -> None:
        q_str = f'DROP PRIMARY INDEX on `{cb_env.bucket.name}`'
        for _ in range(AsyncQueryEnvironment.PRIMARY_INDEX_ROUNDS):
            try:
                await cb_env.cluster.query(q_str).execute()
            except Exception as exc:
                if index_already_dropped(exc):
                    break
                raise
            await AsyncTestEnvironment.sleep(AsyncQueryEnvironment.PRIMARY_INDEX_DELAY)

    @staticmethod
    async def _wait_for_query_index(cb_env: AsyncTestEnvironment, query_namespace: str) -> None:
        batch = cb_env.get_batch_id()
        rows = AsyncQueryEnvironment.INDEX_LOAD_ROWS
        for _ in range(AsyncQueryEnvironment.INDEX_LOAD_ROUNDS):
            q_str = f"SELECT * FROM {query_namespace} WHERE batch LIKE '{batch}%' LIMIT {rows}"
            try:
                result = cb_env.cluster.query(q_str)
                count = 0
                async for _ in result.rows():
                    count += 1
            except CouchbaseException as exc:
                if not index_not_online(exc):
                    raise
                count = 0
            if count >= rows:
                return
            print('Waiting for index to load, sleeping a bit...')
            await AsyncTestEnvironment.sleep(AsyncQueryEnvironment.INDEX_LOAD_DELAY)

    @staticmethod
    def _supports_manager_api(cb_env: AsyncTestEnvironment) -> bool:
        """True when the query index manager API can be used instead of raw statements.

        Pure version arithmetic, no I/O, so it stays synchronous in both api styles.
        """
        return EnvironmentFeatures.is_feature_supported(ServerFeatures.QueryContext, cb_env.server_context)

    @staticmethod
    async def setup_query_indexes(cb_env: AsyncTestEnvironment,
                                  collection_strategy: CollectionStrategy) -> None:
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.QueryIndexManagement, cb_env.server_context)
        if collection_strategy == CollectionStrategy.NAMED:
            qixm = cb_env.get_manager(AsyncTestEnvironmentQueryIndexManager)
            await AsyncTestEnvironment.try_n_times(
                AsyncQueryEnvironment.PRIMARY_INDEX_ROUNDS, AsyncQueryEnvironment.PRIMARY_INDEX_DELAY,
                qixm.create_primary_index, cb_env.bucket.name,
                scope_name=cb_env.scope.name, collection_name=cb_env.collection.name,
                timeout=timedelta(seconds=60), ignore_if_exists=True)
            query_namespace = cb_env.fqdn
        else:
            if AsyncQueryEnvironment._supports_manager_api(cb_env):
                qixm = cb_env.get_manager(AsyncTestEnvironmentQueryIndexManager)
                await AsyncTestEnvironment.try_n_times(
                    AsyncQueryEnvironment.PRIMARY_INDEX_ROUNDS, AsyncQueryEnvironment.PRIMARY_INDEX_DELAY,
                    qixm.create_primary_index, cb_env.bucket.name,
                    timeout=timedelta(seconds=60), ignore_if_exists=True)
            else:
                await AsyncQueryEnvironment._create_primary_index_raw(cb_env)
            query_namespace = f'`{cb_env.bucket.name}`'
        await AsyncQueryEnvironment._wait_for_query_index(cb_env, query_namespace)

    @staticmethod
    async def teardown_query_indexes(cb_env: AsyncTestEnvironment,
                                     collection_strategy: CollectionStrategy) -> None:
        if collection_strategy == CollectionStrategy.NAMED:
            qixm = cb_env.get_manager(AsyncTestEnvironmentQueryIndexManager)
            await AsyncTestEnvironment.try_n_times_till_exception(
                AsyncQueryEnvironment.PRIMARY_INDEX_ROUNDS, AsyncQueryEnvironment.PRIMARY_INDEX_DELAY,
                AsyncQueryEnvironment._drop_primary_index, qixm, cb_env.bucket.name,
                scope_name=cb_env.scope.name,
                collection_name=cb_env.collection.name,
                expected_exceptions=(QueryIndexNotFoundException,))
        else:
            if AsyncQueryEnvironment._supports_manager_api(cb_env):
                qixm = cb_env.get_manager(AsyncTestEnvironmentQueryIndexManager)
                await AsyncTestEnvironment.try_n_times_till_exception(
                    AsyncQueryEnvironment.PRIMARY_INDEX_ROUNDS, AsyncQueryEnvironment.PRIMARY_INDEX_DELAY,
                    AsyncQueryEnvironment._drop_primary_index, qixm, cb_env.bucket.name,
                    expected_exceptions=(QueryIndexNotFoundException,))
            else:
                await AsyncQueryEnvironment._drop_primary_index_raw(cb_env)
