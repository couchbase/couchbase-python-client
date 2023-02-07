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
import time

from couchbase.exceptions import (AmbiguousTimeoutException,
                                  DocumentNotFoundException,
                                  UnAmbiguousTimeoutException)
from couchbase.result import GetResult
from tests.environments import CollectionType
from tests.environments.test_environment import TestEnvironment


class CollectionMultiTestEnvironment(TestEnvironment):

    FAKE_DOCS = {
        'not-a-key1': {'what': 'a fake test doc!', 'id': 'not-a-key1'},
        'not-a-key2': {'what': 'a fake test doc!', 'id': 'not-a-key2'},
        'not-a-key3': {'what': 'a fake test doc!', 'id': 'not-a-key3'},
        'not-a-key4': {'what': 'a fake test doc!', 'id': 'not-a-key4'}
    }

    def check_all_not_found(self, cb_env, keys, okay_key=None):
        not_found = 0
        for k in keys:
            try:
                cb_env.collection.get(k)
                if okay_key and k == okay_key:
                    not_found += 1  # this is okay, it shouldn't have expired
            except DocumentNotFoundException:
                not_found += 1

        if not_found != len(keys):
            raise Exception('Not all docs were expired')

    def get_docs(self, num_docs):
        filtered_keys = set(self._loaded_docs.keys())
        available_keys = filtered_keys.difference(self._used_docs)
        keys = random.choices(list(available_keys), k=num_docs)
        self._used_docs.update(keys)
        return {k: self._loaded_docs[k] for k in keys}

    def get_new_docs(self, num_docs):
        docs = {}
        for v in self.data_provider.get_simple_docs(num_docs):
            key = f'{v["id"]}'
            self._used_extras.add(key)
            docs[key] = v

        return docs

    def load_data(self):
        for v in self.data_provider.get_simple_docs(100):
            for _ in range(3):
                try:
                    key = f'{v["id"]}'
                    _ = self.collection.upsert(key, v)
                    self._loaded_docs[key] = v
                    break
                except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                    time.sleep(3)
                    continue
                except Exception as ex:
                    print(ex)
                    raise

        for k in self._loaded_docs.keys():
            TestEnvironment.try_n_times(5, 1, self.collection.get, k)

    def make_sure_docs_exists(self, cb_env, keys):
        found = 0
        for k in keys:
            doc = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, k)
            if isinstance(doc, GetResult):
                found += 1

        if len(keys) != found:
            raise Exception('Unable to find all docs.')

    def setup(self,
              collection_type,  # type: CollectionType
              ):

        if collection_type == CollectionType.NAMED:
            self.enable_collection_mgmt().enable_named_collections()
            TestEnvironment.try_n_times(5, 3, self.setup_named_collections)

        TestEnvironment.try_n_times(5, 3, self.load_data)

    def teardown(self,
                 collection_type,  # type: CollectionType
                 ):

        TestEnvironment.try_n_times(5, 3, self.purge_data)

        if collection_type == CollectionType.NAMED:
            TestEnvironment.try_n_times(5, 3, self.teardown_named_collections)

    @classmethod
    def from_environment(cls,
                         env  # type: TestEnvironment
                         ) -> CollectionMultiTestEnvironment:

        env_args = {
            'bucket': env.bucket,
            'cluster': env.cluster,
            'default_collection': env.default_collection,
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        cb_env = cls(**env_args)
        return cb_env
