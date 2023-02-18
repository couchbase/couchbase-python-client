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

from typing import List, Optional

import pytest

from couchbase.management.options import (DropPrimaryQueryIndexOptions,
                                          DropQueryIndexOptions,
                                          GetAllQueryIndexOptions)
from couchbase.management.queries import QueryIndex
from tests.environments import CollectionType
from tests.environments.test_environment import TestEnvironment


class QueryIndexManagementTestEnvironment(TestEnvironment):

    def clear_all_indexes(self,
                          collection_type=None,  # type: Optional[CollectionType]
                          ignore_fail=False  # type: Optional[bool]
                          ):
        pairs = [('_default', '_default')]
        if not collection_type and self._collection_type:
            collection_type = self._collection_type

        if collection_type and collection_type == CollectionType.NAMED:
            pairs.append((self.TEST_SCOPE, self.TEST_COLLECTION))

        for scope_col in pairs:

            indexes = self._get_all_indexes(scope_col[0], scope_col[1])
            success = self.drop_all_indexes(indexes,
                                            scope_name=scope_col[0],
                                            collection_name=scope_col[1])
            if success:
                continue
            elif ignore_fail is True:
                continue
            else:
                pytest.xfail(
                    "Indexes were not dropped after {} waits of {} seconds each".format(10, 2))

    def drop_all_indexes(self,
                         indexes,  # type: List[QueryIndex]
                         scope_name='_default',  # type: Optional[str]
                         collection_name='_default',  # type: Optional[str]
                         ):
        for index in indexes:
            self._drop_index(index, scope_name, collection_name)
        for _ in range(10):
            indexes = self._get_all_indexes(scope_name, collection_name)
            if 0 == len(indexes):
                return True
            TestEnvironment.sleep(2)

        return False

    def get_fqdn(self):
        return f'`{self.bucket.name}`.`{self.scope.name}`.`{self.collection.name}`'

    def get_batch_id(self):
        if hasattr(self, '_batch_id'):
            return self._batch_id

        doc = list(self._loaded_docs.values())[0]
        self._batch_id = doc['batch']
        return self._batch_id

    def setup(self,
              collection_type,  # type: CollectionType
              test_suite=None,  # type: Optional[str]
              ):

        # TODO:  will change once updated query_context
        if self.server_version_short <= 6.6:
            pytest.skip((f'Query Index Management only supported on server versions > 6.6. '
                        f'Using server version: {self.server_version}.'))

        if collection_type == CollectionType.NAMED:
            self.enable_collection_mgmt().enable_named_collections()
            TestEnvironment.try_n_times(5, 3, self.setup_named_collections)

        self._collection_type = collection_type
        self._from_collection = test_suite == 'ClassicCollectionQueryIndexManagementTests'
        self.enable_query_mgmt(from_collection=self._from_collection)

    def teardown(self,
                 collection_type,  # type: CollectionType
                 ):

        TestEnvironment.try_n_times(5,
                                    3,
                                    self.clear_all_indexes,
                                    collection_type,
                                    ignore_fail=True)
        self.disable_query_mgmt()
        TestEnvironment.try_n_times(5, 3, self.purge_data)
        if collection_type == CollectionType.NAMED:
            TestEnvironment.try_n_times(5, 3, self.teardown_named_collections)
            self.disable_collection_mgmt()

    def _drop_index(self, index, scope_name, collection_name):
        if self._from_collection:
            if index.is_primary:
                opts = DropPrimaryQueryIndexOptions(collection_name=collection_name)
                if index.name != '#primary':
                    opts['index_name'] = index.name
                self.qixm.drop_primary_index(opts)
            else:
                self.qixm.drop_index(index.name, DropQueryIndexOptions(collection_name=collection_name))
        else:
            if index.is_primary:
                opts = DropPrimaryQueryIndexOptions(scope_name=scope_name, collection_name=collection_name)
                if index.name != '#primary':
                    opts['index_name'] = index.name
                self.qixm.drop_primary_index(self.bucket.name, opts)
            else:
                self.qixm.drop_index(self.bucket.name,
                                     index.name,
                                     DropQueryIndexOptions(scope_name=scope_name, collection_name=collection_name))

    def _get_all_indexes(self,
                         scope_name=None,  # type: Optional[str]
                         collection_name=None,  # type: Optional[str]
                         ) -> List[QueryIndex]:
        if self._from_collection:
            return self.qixm.get_all_indexes(GetAllQueryIndexOptions(scope_name=scope_name,
                                                                     collection_name=collection_name))
        else:
            return self.qixm.get_all_indexes(self.bucket.name,
                                             GetAllQueryIndexOptions(scope_name=scope_name,
                                                                     collection_name=collection_name))

    @classmethod
    def from_environment(cls,
                         env  # type: TestEnvironment
                         ) -> QueryIndexManagementTestEnvironment:

        env_args = {
            'bucket': env.bucket,
            'cluster': env.cluster,
            'default_collection': env.default_collection,
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        cb_env = cls(**env_args)
        return cb_env
