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

"""Analytics dataverse/dataset setup and teardown for the acouchbase suites.

The async counterpart of analytics_environment.py, and the same shape.  Three things
differ from the sync module, all forced by the SDK rather than chosen:

  * every manager call is awaited, since AsyncTestEnvironmentAnalyticsIndexManager
    returns awaitables where the sync wrapper returns values.  Dropping one leaves the
    dataset uncreated while setup carries on to wait for rows in it.
  * `cluster.analytics_query()` still returns its result object synchronously; it is the
    *rows* that are an async iterator.  So the call is not awaited and the drain is
    `async for`, which is the same split acouchbase uses for query.
  * the row drain cannot be `for _ in result.rows()`.  That does not raise over an async
    iterator, it iterates nothing, so the DDL statement would never reach the server and
    the failure would surface later as a missing dataset.  Same trap as
    async_query_environment._wait_for_query_index; see its docstring.
"""

from __future__ import annotations

from typing import Any

from tests_v2.environments.async_environment import AsyncTestEnvironment
from tests_v2.environments.managers import AsyncTestEnvironmentAnalyticsIndexManager
from tests_v2.environments.test_environment import CollectionStrategy
from tests_v2.skips import env_degraded
from tests_v2.test_features import EnvironmentFeatures, ServerFeatures


class AsyncAnalyticsEnvironment:
    """Asyncio analytics dataverse, dataset and link setup."""

    DATASET_NAME = 'test-dataset'
    MIN_INGESTED_ROWS = 10

    @staticmethod
    async def _execute(cb_env: AsyncTestEnvironment, statement: str, **kwargs: Any) -> None:
        """Run an analytics statement for its effect, draining the rows."""
        result = cb_env.cluster.analytics_query(statement, **kwargs)
        async for _ in result.rows():
            pass

    @staticmethod
    def _dataverse(cb_env: AsyncTestEnvironment) -> str:
        """`bucket`.`scope`, the dataverse that shadows the suite's named scope."""
        return f'`{cb_env.bucket.name}`.`{cb_env.scope.name}`'

    @staticmethod
    async def _create_analytics_collections(cb_env: AsyncTestEnvironment) -> None:
        dv = AsyncAnalyticsEnvironment._dataverse(cb_env)
        await AsyncAnalyticsEnvironment._execute(cb_env, f'CREATE DATAVERSE {dv} IF NOT EXISTS;')
        await AsyncAnalyticsEnvironment._execute(
            cb_env, f'USE {dv}; CREATE DATASET IF NOT EXISTS `{cb_env.collection.name}` ON {cb_env.fqdn}')
        await AsyncAnalyticsEnvironment._execute(cb_env, f'USE {dv}; CONNECT LINK Local;')

    @staticmethod
    async def _drop_analytics_collections(cb_env: AsyncTestEnvironment) -> None:
        dv = AsyncAnalyticsEnvironment._dataverse(cb_env)
        await AsyncAnalyticsEnvironment._execute(cb_env, f'USE {dv}; DISCONNECT LINK Local;')
        await AsyncAnalyticsEnvironment._execute(
            cb_env, f'USE {dv}; DROP DATASET `{cb_env.collection.name}` IF EXISTS;')
        await AsyncAnalyticsEnvironment._execute(cb_env, f'DROP DATAVERSE {dv} IF EXISTS;')

    @staticmethod
    async def _row_count(cb_env: AsyncTestEnvironment,
                         query_namespace: str,
                         query_context: str | None) -> int:
        q_str = f'SELECT COUNT(1) AS doc_count FROM {query_namespace}'
        if query_context is not None:
            result = cb_env.cluster.analytics_query(q_str, query_context=query_context)
        else:
            result = cb_env.cluster.analytics_query(q_str)
        rows = [r async for r in result.rows()]
        if not rows:
            return 0
        return int(rows[0].get('doc_count', 0))

    @staticmethod
    async def _wait_for_ingestion(cb_env: AsyncTestEnvironment,
                                  query_namespace: str,
                                  query_context: str | None) -> None:
        wanted = min(AsyncAnalyticsEnvironment.MIN_INGESTED_ROWS, len(cb_env.loaded_keys))
        for _ in range(5):
            count = await AsyncAnalyticsEnvironment._row_count(cb_env, query_namespace, query_context)
            if count >= wanted:
                return
            print('Waiting for the analytics dataset to ingest, sleeping a bit...')
            await AsyncTestEnvironment.sleep(5)
        env_degraded(stage='analytics-ingest',
                     namespace=query_namespace,
                     wanted=wanted,
                     got=count,
                     note='the analytics dataset did not ingest the loaded documents')

    @staticmethod
    async def setup_analytics(cb_env: AsyncTestEnvironment,
                              collection_strategy: CollectionStrategy) -> None:
        """Create the dataset the suite queries and wait for it to hold rows."""
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.Analytics, cb_env.server_context)
        if collection_strategy == CollectionStrategy.DEFAULT:
            aixm = cb_env.get_manager(AsyncTestEnvironmentAnalyticsIndexManager)
            await AsyncTestEnvironment.try_n_times(10, 3, aixm.create_dataset,
                                                   AsyncAnalyticsEnvironment.DATASET_NAME,
                                                   cb_env.bucket.name, ignore_if_exists=True)
            await aixm.connect_link()
            query_namespace = f'`{AsyncAnalyticsEnvironment.DATASET_NAME}`'
            query_context = None
        else:
            await AsyncAnalyticsEnvironment._create_analytics_collections(cb_env)
            query_namespace = f'`{cb_env.collection.name}`'
            query_context = f'default:{AsyncAnalyticsEnvironment._dataverse(cb_env)}'
        await AsyncAnalyticsEnvironment._wait_for_ingestion(cb_env, query_namespace, query_context)

    @staticmethod
    async def teardown_analytics(cb_env: AsyncTestEnvironment,
                                 collection_strategy: CollectionStrategy) -> None:
        if collection_strategy == CollectionStrategy.DEFAULT:
            aixm = cb_env.get_manager(AsyncTestEnvironmentAnalyticsIndexManager)
            await aixm.disconnect_link()
            await AsyncTestEnvironment.try_n_times(10, 3, aixm.drop_dataset,
                                                   AsyncAnalyticsEnvironment.DATASET_NAME,
                                                   ignore_if_not_exists=True)
        else:
            await AsyncAnalyticsEnvironment._drop_analytics_collections(cb_env)
