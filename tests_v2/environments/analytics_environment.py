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

"""Analytics dataverse/dataset setup and teardown for the classic/sync suites.

Same shape as query_environment.py: one module per api style, one class of static
helpers, no TestEnvironment subclass.  build_environment() has already created the
collection and seeded it by the time anything here runs, so this module holds only what is
actually analytics.

Two setup paths, both inherited from the suite they serve rather than chosen here:

  * NAMED runs raw DDL, because a dataset in a scope-shadowing dataverse cannot be
    expressed through the manager API.  `create_dataset(name, bucket_name)` has no
    dataverse parameter, so the `USE <dataverse>;` prefix is the only way to say it.
  * DEFAULT runs through the manager wrapper, one shared bucket-wide dataset.

The dataset is created *after* load_data(), and the order does not matter: a dataset
ingests the documents already in its source collection rather than only watching for new
ones.  The row-count wait below is what makes that safe either way, since analytics
ingestion is asynchronous whichever comes first.
"""

from __future__ import annotations

from typing import Any

from tests_v2.environments.managers import TestEnvironmentAnalyticsIndexManager
from tests_v2.environments.test_environment import (CollectionStrategy,
                                                    SyncTestEnvironment,
                                                    TestEnvironment)
from tests_v2.skips import env_degraded
from tests_v2.test_features import EnvironmentFeatures, ServerFeatures


class AnalyticsEnvironment:
    """Classic/sync analytics dataverse, dataset and link setup."""

    DATASET_NAME = 'test-dataset'

    # Ten rows is a "the link is flowing" probe, not a completeness check: the suites
    # assert their own counts, and ingestion is asynchronous so an exact count would be a
    # race.  Capped against what load_data() actually seeded, so a suite that lowers
    # NUM_DOCS below ten does not spend five sleeping rounds on a threshold it can never
    # reach.  NAMED_EMPTY seeds nothing, and there the cap makes the wait a no-op, which
    # is right: there is no ingestion to wait for.
    MIN_INGESTED_ROWS = 10

    @staticmethod
    def _execute(cb_env: SyncTestEnvironment, statement: str, **kwargs: Any) -> None:
        """Run an analytics statement for its effect, draining the rows.

        The drain is not optional.  `analytics_query` builds a lazy request and the
        statement does not reach the server until the rows are iterated, so dropping the
        result silently skips the DDL.
        """
        result = cb_env.cluster.analytics_query(statement, **kwargs)
        for _ in result.rows():
            pass

    @staticmethod
    def _dataverse(cb_env: SyncTestEnvironment) -> str:
        """`bucket`.`scope`, the dataverse that shadows the suite's named scope."""
        return f'`{cb_env.bucket.name}`.`{cb_env.scope.name}`'

    @staticmethod
    def _create_analytics_collections(cb_env: SyncTestEnvironment) -> None:
        dv = AnalyticsEnvironment._dataverse(cb_env)
        AnalyticsEnvironment._execute(cb_env, f'CREATE DATAVERSE {dv} IF NOT EXISTS;')
        AnalyticsEnvironment._execute(
            cb_env, f'USE {dv}; CREATE DATASET IF NOT EXISTS `{cb_env.collection.name}` ON {cb_env.fqdn}')
        AnalyticsEnvironment._execute(cb_env, f'USE {dv}; CONNECT LINK Local;')

    @staticmethod
    def _drop_analytics_collections(cb_env: SyncTestEnvironment) -> None:
        dv = AnalyticsEnvironment._dataverse(cb_env)
        AnalyticsEnvironment._execute(cb_env, f'USE {dv}; DISCONNECT LINK Local;')
        AnalyticsEnvironment._execute(cb_env, f'USE {dv}; DROP DATASET `{cb_env.collection.name}` IF EXISTS;')
        AnalyticsEnvironment._execute(cb_env, f'DROP DATAVERSE {dv} IF EXISTS;')

    @staticmethod
    def _row_count(cb_env: SyncTestEnvironment,
                   query_namespace: str,
                   query_context: str | None) -> int:
        q_str = f'SELECT COUNT(1) AS doc_count FROM {query_namespace}'
        if query_context is not None:
            result = cb_env.cluster.analytics_query(q_str, query_context=query_context)
        else:
            result = cb_env.cluster.analytics_query(q_str)
        rows = list(result.rows())
        if not rows:
            return 0
        return int(rows[0].get('doc_count', 0))

    @staticmethod
    def _wait_for_ingestion(cb_env: SyncTestEnvironment,
                            query_namespace: str,
                            query_context: str | None) -> None:
        wanted = min(AnalyticsEnvironment.MIN_INGESTED_ROWS, len(cb_env.loaded_keys))
        for _ in range(5):
            count = AnalyticsEnvironment._row_count(cb_env, query_namespace, query_context)
            if count >= wanted:
                return
            print('Waiting for the analytics dataset to ingest, sleeping a bit...')
            TestEnvironment.sleep(5)
        env_degraded(stage='analytics-ingest',
                     namespace=query_namespace,
                     wanted=wanted,
                     got=count,
                     note='the analytics dataset did not ingest the loaded documents')

    @staticmethod
    def setup_analytics(cb_env: SyncTestEnvironment, collection_strategy: CollectionStrategy) -> None:
        """Create the dataset the suite queries and wait for it to hold rows.

        The feature check runs first and runs here rather than in the suite, so it takes
        the whole class with it on a mock or on a cluster with no analytics node, the same
        way setup_query_indexes gates the query suites.  Reaching create_dataset without it
        fails inside the manager instead, which reads as an SDK defect.
        """
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.Analytics, cb_env.server_context)
        # Keyed on DEFAULT rather than on NAMED, so NAMED_EMPTY takes the named path too.
        # The branch that matters is whether there is a named scope to shadow with a
        # dataverse, which both named strategies have and DEFAULT does not.
        if collection_strategy == CollectionStrategy.DEFAULT:
            aixm = cb_env.get_manager(TestEnvironmentAnalyticsIndexManager)
            TestEnvironment.try_n_times(10, 3, aixm.create_dataset, AnalyticsEnvironment.DATASET_NAME,
                                        cb_env.bucket.name, ignore_if_exists=True)
            aixm.connect_link()
            query_namespace = f'`{AnalyticsEnvironment.DATASET_NAME}`'
            query_context = None
        else:
            AnalyticsEnvironment._create_analytics_collections(cb_env)
            query_namespace = f'`{cb_env.collection.name}`'
            query_context = f'default:{AnalyticsEnvironment._dataverse(cb_env)}'
        AnalyticsEnvironment._wait_for_ingestion(cb_env, query_namespace, query_context)

    @staticmethod
    def teardown_analytics(cb_env: SyncTestEnvironment, collection_strategy: CollectionStrategy) -> None:
        if collection_strategy == CollectionStrategy.DEFAULT:
            aixm = cb_env.get_manager(TestEnvironmentAnalyticsIndexManager)
            aixm.disconnect_link()
            TestEnvironment.try_n_times(10, 3, aixm.drop_dataset, AnalyticsEnvironment.DATASET_NAME,
                                        ignore_if_not_exists=True)
        else:
            AnalyticsEnvironment._drop_analytics_collections(cb_env)
