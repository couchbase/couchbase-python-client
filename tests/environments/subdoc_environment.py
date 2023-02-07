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

from couchbase.exceptions import AmbiguousTimeoutException, UnAmbiguousTimeoutException
from tests.environments import CollectionType
from tests.environments.test_environment import TestEnvironment


class SubdocTestEnvironment(TestEnvironment):

    def get_existing_doc_by_type(self, doc_type, key_only=False):
        if not self._loaded_docs:
            self.load_data()

        if doc_type == 'array_only':
            filtered_keys = set([k for k, v in self._loaded_docs.items() if isinstance(v, list)])
        else:
            filtered_keys = set([k for k, v in self._loaded_docs.items()
                                if not isinstance(v, list) and v['type'] in doc_type])
        available_keys = filtered_keys.difference(self._used_docs)
        key = random.choice(list(available_keys))
        self._used_docs.add(key)
        if key_only is True:
            return key
        return key, self._loaded_docs[key]

    def get_new_doc(self, key_only=False):
        return self.get_new_doc_by_type('vehicle', key_only=key_only)

    def get_new_doc_by_type(self, doc_type, key_only=False):
        if doc_type == 'array':
            doc = self.data_provider.get_array_docs(1)[0]
        elif doc_type == 'array_only':
            docs = self.data_provider.get_array_only_docs(1)
            key = docs.keys()[0]
            self._used_extras.add(key)
            if key_only is True:
                return key
            return key, docs[key]
        elif doc_type == 'count':
            doc = self.data_provider.get_count_docs(1)[0]
        elif doc_type == 'vehicle':
            doc = self.data_provider.get_new_vehicle()
        self._used_extras.add(doc['id'])
        if key_only is True:
            return doc['id']
        return doc['id'], doc

    def load_data(self):  # noqa: C901
        for v in self.data_provider.get_array_docs(15):
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

        for k, v in self.data_provider.get_array_only_docs(5).items():
            for _ in range(3):
                try:
                    _ = self.collection.upsert(k, v)
                    self._loaded_docs[k] = v
                    break
                except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                    time.sleep(3)
                    continue
                except Exception as ex:
                    print(ex)
                    raise

        for v in self.data_provider.get_count_docs(15):
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

        for v in self.data_provider.get_vehicles()[:25]:
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

        self._doc_types = ['array', 'array_only', 'count', 'vehicle']

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
                         ) -> SubdocTestEnvironment:

        env_args = {
            'bucket': env.bucket,
            'cluster': env.cluster,
            'default_collection': env.default_collection,
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        cb_env = cls(**env_args)
        return cb_env
