#  Copyright 2016-2026. Couchbase, Inc.
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

import pytest

from couchbase.exceptions import (BucketDoesNotExistException,
                                  CollectionNotFoundException,
                                  ScopeNotFoundException)
from couchbase.management.collections import CollectionSpec
from couchbase.management.options import (DropPrimaryQueryIndexOptions,
                                          DropQueryIndexOptions,
                                          GetAllQueryIndexOptions)
from couchbase.management.queries import QueryIndex
from tests.environments.metrics.async_metrics_environment import AsyncMetricsEnvironment
from tests.test_features import EnvironmentFeatures


class AsyncManagementMetricsEnvironment(AsyncMetricsEnvironment):

    COLLECITON_MGMT_TEST_BUCKET = 'test-bucket'

    async def disable_metrics_bucket_mgmt(self) -> None:
        """Clean up bucket management test artifacts."""
        for b in self._used_bucket_ids:
            await self.try_n_times_till_exception(10,
                                                  3,
                                                  self.bm.drop_bucket,
                                                  b,
                                                  expected_exceptions=(BucketDoesNotExistException, ))
        self._used_bucket_ids.clear()
        self.disable_bucket_mgmt()

    async def disable_metrics_collection_mgmt(self) -> None:
        """Clean up collection management test artifacts."""
        for c in self._used_collection_ids:
            await self.try_n_times_till_exception(10,
                                                  3,
                                                  self.test_bucket_cm.drop_collection,
                                                  CollectionSpec(c),
                                                  expected_exceptions=(CollectionNotFoundException, ))
        self._collection_ids.clear()
        for s in self._used_scope_ids:
            await self.try_n_times_till_exception(10,
                                                  3,
                                                  self.test_bucket_cm.drop_scope,
                                                  s,
                                                  expected_exceptions=(ScopeNotFoundException, ))
        self._scope_ids.clear()
        if EnvironmentFeatures.is_feature_supported('bucket_mgmt', self.server_version_short, self.mock_server_type):
            await self.try_n_times_till_exception(10,
                                                  3,
                                                  self.bm.drop_bucket,
                                                  self.COLLECITON_MGMT_TEST_BUCKET,
                                                  expected_exceptions=(BucketDoesNotExistException, ))

    async def disable_metrics_query_index_mgmt(self) -> None:
        """Clean up query index management test artifacts."""
        success = await self._drop_all_indexes()
        if not success:
            pytest.xfail('Indexes were not dropped after waiting ~20 seconds.')
        self.disable_query_mgmt()

    def enable_metrics_bucket_mgmt(self) -> None:
        """Enable bucket management for tracing tests."""
        EnvironmentFeatures.check_if_feature_supported('bucket_mgmt', self.server_version_short, self.mock_server_type)
        self.enable_bucket_mgmt()
        self._used_bucket_ids = set()
        self._bucket_batch_id = str(uuid.uuid4())[:8]
        self._bucket_ids = [f'{self._bucket_batch_id}_bucket_{i}' for i in range(10)]

    async def enable_metrics_collection_mgmt(self) -> None:
        """Enable collection management for tracing tests."""
        self.enable_bucket_mgmt()
        await self.setup_collection_mgmt(self.COLLECITON_MGMT_TEST_BUCKET)
        self._collection_batch_id = str(uuid.uuid4())[:8]
        self._used_scope_ids = set()
        self._scope_ids = [f'{self._collection_batch_id}_scope_{i}' for i in range(10)]
        self._used_collection_ids = set()
        self._collection_ids = [f'{self._collection_batch_id}_collection_{i}' for i in range(10)]

    def enable_metrics_query_index_mgmt(self) -> None:
        """Enable query index management for tracing tests."""
        if self.server_version_short <= 6.6:
            pytest.skip((f'Query Index Management only supported on server versions > 6.6. '
                        f'Using server version: {self.server_version}.'))

        self.enable_query_mgmt()

    def get_bucket_name(self):
        """Get a unique bucket name for testing."""
        all_ids = set(self._bucket_ids)
        available_ids = all_ids.difference(self._used_bucket_ids)
        id = random.choice(list(available_ids))
        self._used_bucket_ids.add(id)
        return id

    def get_collection_name(self):
        """Get a unique collection name for testing."""
        all_ids = set(self._collection_ids)
        available_ids = all_ids.difference(self._used_collection_ids)
        id = random.choice(list(available_ids))
        self._used_collection_ids.add(id)
        return id

    def get_scope_name(self):
        """Get a unique scope name for testing."""
        all_ids = set(self._scope_ids)
        available_ids = all_ids.difference(self._used_scope_ids)
        id = random.choice(list(available_ids))
        self._used_scope_ids.add(id)
        return id

    async def _drop_all_indexes(self) -> None:
        """Drop all indexes in the default collection."""
        scope_name = '_default'
        collection_name = '_default'
        opts = GetAllQueryIndexOptions(scope_name=scope_name, collection_name=collection_name)
        indexes = await self.qixm.get_all_indexes(self.bucket.name, opts)
        for index in indexes:
            await self._drop_index(index, scope_name, collection_name)
        for _ in range(10):
            indexes = await self.qixm.get_all_indexes(self.bucket.name, opts)
            if 0 == len(indexes):
                return True
            await self.sleep(2)

        return False

    async def _drop_index(self, index: QueryIndex, scope_name: str, collection_name: str) -> None:
        """Drop a single index."""
        if index.is_primary:
            opts = DropPrimaryQueryIndexOptions(scope_name=scope_name, collection_name=collection_name)
            if index.name != '#primary':
                opts['index_name'] = index.name
            await self.qixm.drop_primary_index(self.bucket.name, opts)
        else:
            await self.qixm.drop_index(self.bucket.name,
                                       index.name,
                                       DropQueryIndexOptions(scope_name=scope_name, collection_name=collection_name))
