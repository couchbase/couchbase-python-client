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

"""Analytics dataverse/dataset setup and teardown for the txcouchbase suites.

The third of the trio.  The Twisted surface is complete (Cluster.analytics_query,
Scope.analytics_query, Cluster.analytics_indexes and all sixteen manager methods).

The asynchrony sits in a different place from acouchbase, and that is the whole of the
difference between this module and the async one:

  * `cluster.analytics_query()` returns a Deferred, so the *call* goes through the
    reactor.  `result.rows()` is then a plain iterator, so the drain is an ordinary
    `for`.  acouchbase is the mirror image: the call returns immediately and the rows are
    async.  Neither module is a relabelling of the other.
  * the manager wrapper already bridges its own calls, so it is passed to try_n_times
    with `is_deferred=False`.  Leaving the default would have the retry helper wait on a
    value that is not a Deferred.
"""

from __future__ import annotations

from typing import Any

from tests.environments.managers import TwistedTestEnvironmentAnalyticsIndexManager
from tests.environments.test_environment import CollectionStrategy
from tests.environments.twisted_environment import TwistedTestEnvironment, run_in_reactor_thread
from tests.skips import env_degraded
from tests.test_features import EnvironmentFeatures, ServerFeatures


class TwistedAnalyticsEnvironment:
    """Twisted analytics dataverse, dataset and link setup."""

    DATASET_NAME = 'test-dataset'
    MIN_INGESTED_ROWS = 10

    @staticmethod
    def _execute(cb_env: TwistedTestEnvironment, statement: str, **kwargs: Any) -> None:
        """Run an analytics statement for its effect, draining the rows."""
        result = run_in_reactor_thread(cb_env.cluster.analytics_query, statement, **kwargs)
        for _ in result.rows():
            pass

    @staticmethod
    def _dataverse(cb_env: TwistedTestEnvironment) -> str:
        """`bucket`.`scope`, the dataverse that shadows the suite's named scope."""
        return f'`{cb_env.bucket.name}`.`{cb_env.scope.name}`'

    @staticmethod
    def _create_analytics_collections(cb_env: TwistedTestEnvironment) -> None:
        dv = TwistedAnalyticsEnvironment._dataverse(cb_env)
        TwistedAnalyticsEnvironment._execute(cb_env, f'CREATE DATAVERSE {dv} IF NOT EXISTS;')
        TwistedAnalyticsEnvironment._execute(
            cb_env, f'USE {dv}; CREATE DATASET IF NOT EXISTS `{cb_env.collection.name}` ON {cb_env.fqdn}')
        TwistedAnalyticsEnvironment._execute(cb_env, f'USE {dv}; CONNECT LINK Local;')

    @staticmethod
    def _drop_analytics_collections(cb_env: TwistedTestEnvironment) -> None:
        dv = TwistedAnalyticsEnvironment._dataverse(cb_env)
        TwistedAnalyticsEnvironment._execute(cb_env, f'USE {dv}; DISCONNECT LINK Local;')
        TwistedAnalyticsEnvironment._execute(
            cb_env, f'USE {dv}; DROP DATASET `{cb_env.collection.name}` IF EXISTS;')
        TwistedAnalyticsEnvironment._execute(cb_env, f'DROP DATAVERSE {dv} IF EXISTS;')

    @staticmethod
    def _row_count(cb_env: TwistedTestEnvironment,
                   query_namespace: str,
                   query_context: str | None) -> int:
        q_str = f'SELECT COUNT(1) AS doc_count FROM {query_namespace}'
        if query_context is not None:
            result = run_in_reactor_thread(cb_env.cluster.analytics_query, q_str, query_context=query_context)
        else:
            result = run_in_reactor_thread(cb_env.cluster.analytics_query, q_str)
        rows = list(result.rows())
        if not rows:
            return 0
        return int(rows[0].get('doc_count', 0))

    @staticmethod
    def _wait_for_ingestion(cb_env: TwistedTestEnvironment,
                            query_namespace: str,
                            query_context: str | None) -> None:
        wanted = min(TwistedAnalyticsEnvironment.MIN_INGESTED_ROWS, len(cb_env.loaded_keys))
        for _ in range(5):
            count = TwistedAnalyticsEnvironment._row_count(cb_env, query_namespace, query_context)
            if count >= wanted:
                return
            print('Waiting for the analytics dataset to ingest, sleeping a bit...')
            TwistedTestEnvironment.sleep(5)
        env_degraded(stage='analytics-ingest',
                     namespace=query_namespace,
                     wanted=wanted,
                     got=count,
                     note='the analytics dataset did not ingest the loaded documents')

    @staticmethod
    def setup_analytics(cb_env: TwistedTestEnvironment,
                        collection_strategy: CollectionStrategy) -> None:
        """Create the dataset the suite queries and wait for it to hold rows."""
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.Analytics, cb_env.server_context)
        if collection_strategy == CollectionStrategy.DEFAULT:
            aixm = cb_env.get_manager(TwistedTestEnvironmentAnalyticsIndexManager)
            TwistedTestEnvironment.try_n_times(10, 3, aixm.create_dataset,
                                               TwistedAnalyticsEnvironment.DATASET_NAME,
                                               cb_env.bucket.name,
                                               is_deferred=False, ignore_if_exists=True)
            aixm.connect_link()
            query_namespace = f'`{TwistedAnalyticsEnvironment.DATASET_NAME}`'
            query_context = None
        else:
            TwistedAnalyticsEnvironment._create_analytics_collections(cb_env)
            query_namespace = f'`{cb_env.collection.name}`'
            query_context = f'default:{TwistedAnalyticsEnvironment._dataverse(cb_env)}'
        TwistedAnalyticsEnvironment._wait_for_ingestion(cb_env, query_namespace, query_context)

    @staticmethod
    def teardown_analytics(cb_env: TwistedTestEnvironment,
                           collection_strategy: CollectionStrategy) -> None:
        if collection_strategy == CollectionStrategy.DEFAULT:
            aixm = cb_env.get_manager(TwistedTestEnvironmentAnalyticsIndexManager)
            aixm.disconnect_link()
            TwistedTestEnvironment.try_n_times(10, 3, aixm.drop_dataset,
                                               TwistedAnalyticsEnvironment.DATASET_NAME,
                                               is_deferred=False, ignore_if_not_exists=True)
        else:
            TwistedAnalyticsEnvironment._drop_analytics_collections(cb_env)
