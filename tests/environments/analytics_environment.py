#  Copyright 2016-2023. Couchbase, Inc.
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


from __future__ import annotations

import time
from typing import Optional

from couchbase.result import AnalyticsResult
from tests.environments import CollectionType
from tests.environments.test_environment import AsyncTestEnvironment, TestEnvironment


class AnalyticsTestEnvironment(TestEnvironment):
    DATASET_NAME = 'test-dataset'

    @property
    def fqdn(self) -> Optional[str]:
        return f'`{self.bucket.name}`.`{self.scope.name}`.`{self.collection.name}`'

    def assert_rows(self,
                    result,  # type: AnalyticsResult
                    expected_count):
        count = 0
        assert isinstance(result, (AnalyticsResult,))
        for row in result.rows():
            assert row is not None
            count += 1
        assert count >= expected_count

    def create_analytics_collections(self):
        """
            Setup queries:
                Create dataverse:
                    CREATE DATAVERSE `default`.`test-scope` IF NOT EXISTS;

                Create dataset:
                    USE `default`.`test-scope`;
                    CREATE DATASET IF NOT EXISTS `test-collection` ON `default`.`test-scope`.`test-collection`;

                Connect Link:
                    USE `default`.`test-scope`; CONNECT LINK Local;
        """

        dv_fqdn = f'`{self.bucket.name}`.`{self.scope.name}`'
        q_str = f'CREATE DATAVERSE {dv_fqdn} IF NOT EXISTS;'
        res = self.cluster.analytics_query(q_str)
        [_ for _ in res.rows()]

        q_str = f'USE {dv_fqdn}; CREATE DATASET IF NOT EXISTS `{self.collection.name}` ON {self.fqdn}'
        res = self.cluster.analytics_query(q_str)
        [_ for _ in res.rows()]

        q_str = f'USE {dv_fqdn}; CONNECT LINK Local;'
        res = self.cluster.analytics_query(q_str)
        [_ for _ in res.rows()]

    def get_batch_id(self):
        if hasattr(self, '_batch_id'):
            return self._batch_id

        doc = list(self._loaded_docs.values())[0]
        self._batch_id = doc['batch']
        return self._batch_id

    def setup(self,
              collection_type,  # type: CollectionType
              ):

        if collection_type == CollectionType.NAMED:
            self.enable_collection_mgmt().enable_named_collections()
            TestEnvironment.try_n_times(5, 3, self.setup_named_collections)
            self.create_analytics_collections()
            query_namespace = f'`{self.collection.name}`'
            query_context = f'default:`{self.bucket.name}`.`{self.scope.name}`'
        else:
            TestEnvironment.try_n_times(10,
                                        3,
                                        self.aixm.create_dataset,
                                        self.DATASET_NAME,
                                        self.bucket.name,
                                        ignore_if_exists=True)
            self.aixm.connect_link()
            query_namespace = f'`{self.DATASET_NAME}`'
            query_context = None

        TestEnvironment.try_n_times(5, 3, self.load_data)

        for _ in range(5):
            row_count_good = self._check_row_count(self.cluster,
                                                   query_namespace,
                                                   10,
                                                   query_context=query_context)

            if row_count_good:
                break
            print('Waiting for index to load, sleeping a bit...')
            time.sleep(5)

    def teardown(self,
                 collection_type,  # type: CollectionType
                 ):

        TestEnvironment.try_n_times(5, 3, self.purge_data)

        if collection_type == CollectionType.NAMED:
            self.teardown_analytics_collections()
            TestEnvironment.try_n_times(5, 3, self.teardown_named_collections)
        else:
            self.aixm.disconnect_link()
            TestEnvironment.try_n_times(10,
                                        3,
                                        self.aixm.drop_dataset,
                                        self.DATASET_NAME,
                                        ignore_if_not_exists=True)

    def teardown_analytics_collections(self):
        """
            Tear-down queries:
                Disconnect Link:
                    USE `default`.`test-scope`; DISCONNECT LINK Local;

                Droo dataset:
                    USE `default`.`test-scope`; DROP DATASET `test-collection` IF EXISTS;

                Drop dataverse:
                    DROP DATAVERSE `default`.`test-scope` IF EXISTS;
        """
        dv_fqdn = f'`{self.bucket.name}`.`{self.scope.name}`'
        q_str = f'USE {dv_fqdn}; DISCONNECT LINK Local;'
        res = self.cluster.analytics_query(q_str)
        [_ for _ in res.rows()]

        q_str = f'USE {dv_fqdn}; DROP DATASET `{self.collection.name}` IF EXISTS;'
        res = self.cluster.analytics_query(q_str)
        [_ for _ in res.rows()]

        q_str = f'DROP DATAVERSE {dv_fqdn} IF EXISTS;'
        res = self.cluster.analytics_query(q_str)
        [_ for _ in res.rows()]

    def _check_row_count(self,
                         cb,
                         query_namespace,  # type: str
                         min_count,  # type: int
                         query_context=None,  # type: Optional[str]
                         ) -> bool:

        q_str = f'SELECT COUNT(1) AS doc_count FROM {query_namespace}'
        if query_context is not None:
            result = cb.analytics_query(q_str, query_context=query_context)
        else:
            result = cb.analytics_query(q_str)

        rows = [r for r in result.rows()]
        return len(rows) > 0 and rows[0].get('doc_count', 0) > min_count

    @classmethod
    def from_environment(cls,
                         env  # type: TestEnvironment
                         ) -> AnalyticsTestEnvironment:

        env_args = {
            'bucket': env.bucket,
            'cluster': env.cluster,
            'default_collection': env.default_collection,
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        cb_env = cls(**env_args)
        return cb_env


class AsyncAnalyticsTestEnvironment(AsyncTestEnvironment):
    DATASET_NAME = 'test-dataset'

    @property
    def fqdn(self) -> Optional[str]:
        return f'`{self.bucket.name}`.`{self.scope.name}`.`{self.collection.name}`'

    async def assert_rows(self,
                          result,  # type: AnalyticsResult
                          expected_count):
        count = 0
        assert isinstance(result, (AnalyticsResult,))
        async for row in result.rows():
            assert row is not None
            count += 1
        assert count >= expected_count

    async def create_analytics_collections(self):
        """
            Setup queries:
                Create dataverse:
                    CREATE DATAVERSE `default`.`test-scope` IF NOT EXISTS;

                Create dataset:
                    USE `default`.`test-scope`;
                    CREATE DATASET IF NOT EXISTS `test-collection` ON `default`.`test-scope`.`test-collection`;

                Connect Link:
                    USE `default`.`test-scope`; CONNECT LINK Local;
        """

        dv_fqdn = f'`{self.bucket.name}`.`{self.scope.name}`'
        q_str = f'CREATE DATAVERSE {dv_fqdn} IF NOT EXISTS;'
        res = self.cluster.analytics_query(q_str)
        [_ async for _ in res.rows()]

        q_str = f'USE {dv_fqdn}; CREATE DATASET IF NOT EXISTS `{self.collection.name}` ON {self.fqdn}'
        res = self.cluster.analytics_query(q_str)
        [_ async for _ in res.rows()]

        q_str = f'USE {dv_fqdn}; CONNECT LINK Local;'
        res = self.cluster.analytics_query(q_str)
        [_ async for _ in res.rows()]

    def get_batch_id(self):
        if hasattr(self, '_batch_id'):
            return self._batch_id

        doc = list(self._loaded_docs.values())[0]
        self._batch_id = doc['batch']
        return self._batch_id

    async def setup(self,
                    collection_type,  # type: CollectionType
                    ):

        if collection_type == CollectionType.NAMED:
            self.enable_collection_mgmt().enable_named_collections()
            await AsyncTestEnvironment.try_n_times(5, 3, self.setup_named_collections)
            await self.create_analytics_collections()
            query_namespace = f'`{self.collection.name}`'
            query_context = f'default:`{self.bucket.name}`.`{self.scope.name}`'
        else:
            await AsyncTestEnvironment.try_n_times(10,
                                                   3,
                                                   self.aixm.create_dataset,
                                                   self.DATASET_NAME,
                                                   self.bucket.name,
                                                   ignore_if_exists=True)
            await self.aixm.connect_link()
            query_namespace = f'`{self.DATASET_NAME}`'
            query_context = None

        await AsyncTestEnvironment.try_n_times(5, 3, self.load_data)

        for _ in range(5):
            row_count_good = await self._check_row_count(self.cluster,
                                                         query_namespace,
                                                         10,
                                                         query_context=query_context)

            if row_count_good:
                break
            print('Waiting for index to load, sleeping a bit...')
            await AsyncTestEnvironment.sleep(5)

    async def teardown(self,
                       collection_type,  # type: CollectionType
                       ):

        await AsyncTestEnvironment.try_n_times(5, 3, self.purge_data)

        if collection_type == CollectionType.NAMED:
            await self.teardown_analytics_collections()
            await AsyncTestEnvironment.try_n_times(5, 3, self.teardown_named_collections)
        else:
            await self.aixm.disconnect_link()
            await AsyncTestEnvironment.try_n_times(10,
                                                   3,
                                                   self.aixm.drop_dataset,
                                                   self.DATASET_NAME,
                                                   ignore_if_not_exists=True)

    async def teardown_analytics_collections(self):
        """
            Tear-down queries:
                Disconnect Link:
                    USE `default`.`test-scope`; DISCONNECT LINK Local;

                Droo dataset:
                    USE `default`.`test-scope`; DROP DATASET `test-collection` IF EXISTS;

                Drop dataverse:
                    DROP DATAVERSE `default`.`test-scope` IF EXISTS;
        """
        dv_fqdn = f'`{self.bucket.name}`.`{self.scope.name}`'
        q_str = f'USE {dv_fqdn}; DISCONNECT LINK Local;'
        res = self.cluster.analytics_query(q_str)
        [_ async for _ in res.rows()]

        q_str = f'USE {dv_fqdn}; DROP DATASET `{self.collection.name}` IF EXISTS;'
        res = self.cluster.analytics_query(q_str)
        [_ async for _ in res.rows()]

        q_str = f'DROP DATAVERSE {dv_fqdn} IF EXISTS;'
        res = self.cluster.analytics_query(q_str)
        [_ async for _ in res.rows()]

    async def _check_row_count(self,
                               cb,
                               query_namespace,  # type: str
                               min_count,  # type: int
                               query_context=None,  # type: Optional[str]
                               ) -> bool:

        q_str = f'SELECT COUNT(1) AS doc_count FROM {query_namespace}'
        if query_context is not None:
            result = cb.analytics_query(q_str, query_context=query_context)
        else:
            result = cb.analytics_query(q_str)

        rows = [r async for r in result.rows()]
        return len(rows) > 0 and rows[0].get('doc_count', 0) > min_count

    @classmethod
    def from_environment(cls,
                         env  # type: TestEnvironment
                         ) -> AnalyticsTestEnvironment:

        env_args = {
            'bucket': env.bucket,
            'cluster': env.cluster,
            'default_collection': env.default_collection,
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        cb_env = cls(**env_args)
        return cb_env
