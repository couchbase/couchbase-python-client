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
from datetime import timedelta
from typing import Optional

import requests

from couchbase.exceptions import QueryIndexAlreadyExistsException, QueryIndexNotFoundException
from couchbase.result import QueryResult
from tests.environments import CollectionType, CouchbaseTestEnvironmentException
from tests.environments.test_environment import AsyncTestEnvironment, TestEnvironment


class QueryTestEnvironment(TestEnvironment):

    @property
    def fqdn(self) -> Optional[str]:
        return f'`{self.bucket.name}`.`{self.scope.name}`.`{self.collection.name}`'

    def assert_rows(self,
                    result,  # type: QueryResult
                    expected_count):
        count = 0
        assert isinstance(result, (QueryResult,))
        for row in result.rows():
            assert row is not None
            count += 1
        assert count >= expected_count

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
            TestEnvironment.try_n_times(10,
                                        3,
                                        self.qixm.create_primary_index,
                                        self.bucket.name,
                                        scope_name=self.scope.name,
                                        collection_name=self.collection.name,
                                        timeout=timedelta(seconds=60),
                                        ignore_if_exists=True)
            query_namespace = self.fqdn
        else:
            if self.server_version_short > 6.6:
                TestEnvironment.try_n_times(10,
                                            3,
                                            self.qixm.create_primary_index,
                                            self.bucket.name,
                                            timeout=timedelta(seconds=60),
                                            ignore_if_exists=True)
            else:
                self._create_primary_index()
            query_namespace = f'`{self.bucket.name}`'

        TestEnvironment.try_n_times(5, 3, self.load_data)

        for _ in range(5):
            row_count_good = self._check_row_count(self.cluster, query_namespace, 5)

            if row_count_good:
                break
            print('Waiting for index to load, sleeping a bit...')
            time.sleep(5)

    def teardown(self,
                 collection_type,  # type: CollectionType
                 ):

        TestEnvironment.try_n_times(5, 3, self.purge_data)

        if collection_type == CollectionType.NAMED:
            TestEnvironment.try_n_times_till_exception(10,
                                                       3,
                                                       self.qixm.drop_primary_index,
                                                       self.bucket.name,
                                                       scope_name=self.scope.name,
                                                       collection_name=self.collection.name,
                                                       expected_exceptions=(QueryIndexNotFoundException,))

            TestEnvironment.try_n_times(5, 3, self.teardown_named_collections)
        else:
            if self.server_version_short > 6.6:
                TestEnvironment.try_n_times_till_exception(10,
                                                           3,
                                                           self.qixm.drop_primary_index,
                                                           self.bucket.name,
                                                           expected_exceptions=(QueryIndexNotFoundException))
            else:
                self._drop_primary_index()

    def _check_row_count(self,
                         cb,
                         query_namespace,  # type: str
                         min_count  # type: int
                         ) -> bool:

        batch = self.get_batch_id()
        result = cb.query(f"SELECT * FROM {query_namespace} WHERE batch LIKE '{batch}%' LIMIT 5")
        count = 0
        for _ in result.rows():
            count += 1
        return count >= min_count

    def _create_primary_index(self):
        q_str = f'CREATE PRIMARY INDEX `#primary` on `{self.bucket.name}`'
        for _ in range(10):
            try:
                self.cluster.query(q_str).execute()
            except QueryIndexAlreadyExistsException:
                break

            TestEnvironment.sleep(3)

    def _drop_primary_index(self):
        q_str = f'DROP PRIMARY INDEX on `{self.bucket.name}`'
        for _ in range(10):
            try:
                self.cluster.query(q_str).execute()
            except QueryIndexNotFoundException:
                break

            TestEnvironment.sleep(3)

    @classmethod
    def from_environment(cls,
                         env  # type: TestEnvironment
                         ) -> QueryTestEnvironment:

        env_args = {
            'bucket': env.bucket,
            'cluster': env.cluster,
            'default_collection': env.default_collection,
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        cb_env = cls(**env_args)
        return cb_env


class AsyncQueryTestEnvironment(AsyncTestEnvironment):
    UDF = """
    function runLoop(num){
      let count = 0
      for(let i = 0; i < num; i++){
        count++
      }
      return count
    }
    """

    UDF_URI_PATH = 'evaluator/v1/libraries/simple'

    @property
    def fqdn(self) -> Optional[str]:
        return f'`{self.bucket.name}`.`{self.scope.name}`.`{self.collection.name}`'

    async def assert_rows(self,
                          result,  # type: QueryResult
                          expected_count) -> bool:
        count = 0
        assert isinstance(result, (QueryResult,))
        async for row in result.rows():
            assert row is not None
            count += 1
        assert count >= expected_count

    async def drop_udf(self):
        try:
            q_str = 'DROP FUNCTION loop IF EXISTS;'
            await self.cluster.query(q_str).execute()
        except Exception as ex:
            raise CouchbaseTestEnvironmentException(f'Failed to drop UDF: {ex}')

        username, pw = self.config.get_username_and_pw()
        url = f'http://{self.config.host}:8093/{self.UDF_URI_PATH}'
        r = requests.delete(url, auth=(username, pw))
        if r.status_code != 200:
            msg = f'Unable to delete UDF via REST. Status: {r.status_code}. Content: {r.content}'
            raise CouchbaseTestEnvironmentException(msg)

    def get_batch_id(self):
        if hasattr(self, '_batch_id'):
            return self._batch_id

        doc = list(self._loaded_docs.values())[0]
        self._batch_id = doc['batch']
        return self._batch_id

    def get_udf(self):
        username, pw = self.config.get_username_and_pw()
        url = f'http://{self.config.host}:8093/{self.UDF_URI_PATH}'
        r = requests.get(url, data=self.UDF.encode('-utf-8'), auth=(username, pw))
        if r.status_code != 200:
            msg = f'Unable to get UDF. Status: {r.status_code}. Content: {r.content}'
            raise CouchbaseTestEnvironmentException(msg)

        udf = None
        try:
            udf = r.json()
        except Exception as ex:
            raise CouchbaseTestEnvironmentException(f'Failed to serialize UDF: {ex}')

        if 'simple' not in udf:
            raise CouchbaseTestEnvironmentException(f'unexpected UDF content: {udf}')

    async def load_udf(self, get_retries=3):
        username, pw = self.config.get_username_and_pw()
        url = f'http://{self.config.host}:8093/{self.UDF_URI_PATH}'
        r = requests.post(url, data=self.UDF.encode('-utf-8'), auth=(username, pw))
        if r.status_code != 200:
            msg = f'Unable to load UDF via REST. Status: {r.status_code}. Content: {r.content}'
            raise CouchbaseTestEnvironmentException(msg)

        try:
            q_str = 'CREATE FUNCTION loop(num) IF NOT EXISTS LANGUAGE JAVASCRIPT AS "runLoop" AT "simple";'
            await self.cluster.query(q_str).execute()
        except Exception as ex:
            raise CouchbaseTestEnvironmentException(f'Failed to create UDF: {ex}')

        retry_count = 1
        while True:
            try:
                self.get_udf()
            except Exception as ex:
                if retry_count == get_retries:
                    raise ex
                retry_count += 1
            else:
                break

    async def setup(self,
                    collection_type,  # type: CollectionType
                    ):

        if collection_type == CollectionType.NAMED:
            self.enable_collection_mgmt().enable_named_collections()
            await AsyncTestEnvironment.try_n_times(5, 3, self.setup_named_collections)
            await AsyncTestEnvironment.try_n_times(10,
                                                   3,
                                                   self.qixm.create_primary_index,
                                                   self.bucket.name,
                                                   scope_name=self.scope.name,
                                                   collection_name=self.collection.name,
                                                   timeout=timedelta(seconds=60),
                                                   ignore_if_exists=True)
            query_namespace = self.fqdn
        else:
            if self.server_version_short > 6.6:
                await AsyncTestEnvironment.try_n_times(10,
                                                       3,
                                                       self.qixm.create_primary_index,
                                                       self.bucket.name,
                                                       timeout=timedelta(seconds=60),
                                                       ignore_if_exists=True)
            else:
                await self._create_primary_index()
            query_namespace = f'`{self.bucket.name}`'

        await AsyncTestEnvironment.try_n_times(5, 3, self.load_data)

        for _ in range(5):
            row_count_good = await self._check_row_count(self.cluster, query_namespace, 5)

            if row_count_good:
                break
            print('Waiting for index to load, sleeping a bit...')
            await AsyncTestEnvironment.sleep(5)

    async def teardown(self,
                       collection_type,  # type: CollectionType
                       ):

        await AsyncTestEnvironment.try_n_times(5, 3, self.purge_data)

        if collection_type == CollectionType.NAMED:
            await AsyncTestEnvironment.try_n_times_till_exception(10,
                                                                  3,
                                                                  self.qixm.drop_primary_index,
                                                                  self.bucket.name,
                                                                  scope_name=self.scope.name,
                                                                  collection_name=self.collection.name,
                                                                  expected_exceptions=(QueryIndexNotFoundException,))

            await AsyncTestEnvironment.try_n_times(5, 3, self.teardown_named_collections)
        else:
            if self.server_version_short > 6.6:
                await AsyncTestEnvironment.try_n_times_till_exception(10,
                                                                      3,
                                                                      self.qixm.drop_primary_index,
                                                                      self.bucket.name,
                                                                      expected_exceptions=(QueryIndexNotFoundException))
            else:
                await self._drop_primary_index()

    async def _check_row_count(self,
                               cb,
                               query_namespace,  # type: str
                               min_count  # type: int
                               ) -> bool:

        batch = self.get_batch_id()
        result = cb.query(f"SELECT * FROM {query_namespace} WHERE batch LIKE '{batch}%' LIMIT 5")
        count = 0
        async for _ in result.rows():
            count += 1
        return count >= min_count

    async def _create_primary_index(self):
        q_str = f'CREATE PRIMARY INDEX `#primary` on `{self.bucket.name}`'
        for _ in range(10):
            try:
                await self.cluster.query(q_str).execute()
            except QueryIndexAlreadyExistsException:
                break

            await AsyncTestEnvironment.sleep(3)

    async def _drop_primary_index(self):
        q_str = f'DROP PRIMARY INDEX on `{self.bucket.name}`'
        for _ in range(10):
            try:
                await self.cluster.query(q_str).execute()
            except QueryIndexNotFoundException:
                break

            await AsyncTestEnvironment.sleep(3)

    @classmethod
    def from_environment(cls,
                         env  # type: AsyncTestEnvironment
                         ) -> AsyncQueryTestEnvironment:

        env_args = {
            'bucket': env.bucket,
            'cluster': env.cluster,
            'default_collection': env.default_collection,
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        cb_env = cls(**env_args)
        return cb_env
