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

from couchbase.exceptions import QueryIndexAlreadyExistsException, QueryIndexNotFoundException
from couchbase.result import QueryResult
from tests.environments import CollectionType
from tests.environments.test_environment import TestEnvironment


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
