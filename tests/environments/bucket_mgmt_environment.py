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

from couchbase.exceptions import BucketDoesNotExistException
from tests.environments.test_environment import TestEnvironment


class BucketManagementTestEnvironment(TestEnvironment):

    def drop_bucket(self):
        available_ids = self._used_ids.difference(self._dropped_ids)
        bucket_name = available_ids.pop()
        self._dropped_ids.add(bucket_name)
        TestEnvironment.try_n_times_till_exception(10,
                                                   3,
                                                   self.bm.drop_bucket,
                                                   bucket_name,
                                                   expected_exceptions=(BucketDoesNotExistException, ))

    def get_bucket_name(self):
        all_ids = set(self._bucket_ids)
        available_ids = all_ids.difference(self._used_ids)
        id = random.choice(list(available_ids))
        self._used_ids.add(id)
        return id

    def get_bucket_names(self, num=3):
        all_ids = set(self._bucket_ids)
        available_ids = all_ids.difference(self._used_ids)
        names = list(available_ids)[:num]
        self._used_ids.update(names)
        return names

    def setup(self):
        self._batch_id = str(uuid.uuid4())[:8]
        self._used_ids = set()
        self._dropped_ids = set()
        # 3 retries for 15 tests
        self._bucket_ids = [f'{self._batch_id}_bucket_{i}' for i in range(50)]

    def teardown(self):

        for b in self._used_ids:
            TestEnvironment.try_n_times_till_exception(10,
                                                       3,
                                                       self.bm.drop_bucket,
                                                       b,
                                                       expected_exceptions=(BucketDoesNotExistException, ))
        self._used_ids.clear()

    @classmethod
    def from_environment(cls,
                         env  # type: TestEnvironment
                         ) -> BucketManagementTestEnvironment:

        env_args = {
            'bucket': env.bucket,
            'cluster': env.cluster,
            'default_collection': env.default_collection,
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        cb_env = cls(**env_args)
        return cb_env
