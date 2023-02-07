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

import random
import uuid

from couchbase.exceptions import (BucketDoesNotExistException,
                                  CollectionNotFoundException,
                                  ScopeNotFoundException)
from couchbase.management.collections import CollectionSpec
from tests.environments.test_environment import TestEnvironment
from tests.test_features import EnvironmentFeatures


class CollectionManagementTestEnvironment(TestEnvironment):
    TEST_BUCKET = 'test-bucket'

    def add_dropped_scope(self, scope_name  # type: str
                          ):
        self._dropped_scope_ids.add(scope_name)

    def get_collection_name(self):
        all_ids = set(self._collection_ids)
        available_ids = all_ids.difference(self._used_collection_ids)
        id = random.choice(list(available_ids))
        self._used_collection_ids.add(id)
        return id

    def get_scope_name(self):
        all_ids = set(self._scope_ids)
        available_ids = all_ids.difference(self._used_scope_ids)
        id = random.choice(list(available_ids))
        self._used_scope_ids.add(id)
        return id

    def get_scope_names(self):
        available_ids = self._used_scope_ids.difference(self._dropped_scope_ids)
        return list(available_ids)

    def setup(self):
        TestEnvironment.try_n_times(3, 5, self.setup_collection_mgmt, self.TEST_BUCKET)
        self._batch_id = str(uuid.uuid4())[:8]
        self._used_scope_ids = set()
        self._dropped_scope_ids = set()
        self._scope_ids = [f'{self._batch_id}_scope_{i}' for i in range(20)]
        self._used_collection_ids = set()
        self._dropped_collection_ids = set()
        self._collection_ids = [f'{self._batch_id}_collection_{i}' for i in range(20)]

    def teardown(self):
        for c in self._used_collection_ids:
            TestEnvironment.try_n_times_till_exception(10,
                                                       3,
                                                       self.cm.drop_collection,
                                                       CollectionSpec(c),
                                                       expected_exceptions=(CollectionNotFoundException, ))
        self._collection_ids.clear()
        for s in self._used_scope_ids:
            TestEnvironment.try_n_times_till_exception(10,
                                                       3,
                                                       self.cm.drop_scope,
                                                       s,
                                                       expected_exceptions=(ScopeNotFoundException, ))
        self._collection_ids.clear()
        if EnvironmentFeatures.is_feature_supported('bucket_mgmt', self.server_version_short, self.mock_server_type):
            TestEnvironment.try_n_times_till_exception(10,
                                                       3,
                                                       self.bm.drop_bucket,
                                                       self.TEST_BUCKET,
                                                       expected_exceptions=(BucketDoesNotExistException, ))

    @classmethod
    def from_environment(cls,
                         env  # type: TestEnvironment
                         ) -> CollectionManagementTestEnvironment:

        env_args = {
            'bucket': env.bucket,
            'cluster': env.cluster,
            'default_collection': env.default_collection,
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        cb_env = cls(**env_args)
        return cb_env
