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
from typing import Optional

from couchbase.exceptions import AmbiguousTimeoutException, UnAmbiguousTimeoutException
from couchbase.transcoder import RawBinaryTranscoder, RawStringTranscoder
from tests.environments import CollectionType
from tests.environments.test_environment import TestEnvironment


class BinaryTestEnvironment(TestEnvironment):

    def get_existing_doc_by_type(self, doc_type, key_only=False):
        if doc_type == 'bytes':
            filtered_keys = set([k for k in self._loaded_docs.keys() if '::bytes::' in k])
        elif doc_type == 'bytes_empty':
            filtered_keys = set([k for k in self._loaded_docs.keys() if 'bytes_empty' in k])
        elif doc_type == 'counter':
            filtered_keys = set([k for k in self._loaded_docs.keys() if '::counter::' in k])
        elif doc_type == 'counter_empty':
            # returns a key
            key = self.data_provider.get_counter_docs(1)
            self._used_extras.add(key)
            return key
        elif doc_type == 'utf8':
            filtered_keys = set([k for k in self._loaded_docs.keys() if '::utf8::' in k])
        elif doc_type == 'utf8_empty':
            filtered_keys = set([k for k in self._loaded_docs.keys() if 'utf8_empty' in k])

        available_keys = filtered_keys.difference(self._used_docs)
        key = random.choice(list(available_keys))
        self._used_docs.add(key)
        if key_only is True:
            return key
        return key, self._loaded_docs[key]

    def get_multiple_existing_docs_by_type(self, doc_type, num_docs):
        if doc_type == 'bytes':
            filtered_keys = set([k for k in self._loaded_docs.keys() if '::bytes::' in k])
        elif doc_type == 'bytes_empty':
            filtered_keys = set([k for k in self._loaded_docs.keys() if 'bytes_empty' in k])
        elif doc_type == 'counter':
            filtered_keys = set([k for k in self._loaded_docs.keys() if '::counter::' in k])
        elif doc_type == 'counter_empty':
            keys = [self.data_provider.get_counter_docs(1) for _ in range(num_docs)]
            self._used_extras.update(keys)
            return keys
        elif doc_type == 'utf8':
            filtered_keys = set([k for k in self._loaded_docs.keys() if '::utf8::' in k])
        elif doc_type == 'utf8_empty':
            filtered_keys = set([k for k in self._loaded_docs.keys() if 'utf8_empty' in k])

        available_keys = filtered_keys.difference(self._used_docs)
        keys = random.choices(list(available_keys), k=num_docs)
        self._used_docs.update(keys)
        return keys

    def load_data(self, multi_tests_suite=False):  # noqa: C901
        tc = RawBinaryTranscoder()
        num_docs = 10 if multi_tests_suite is False else 20
        for k, v in self.data_provider.get_bytes_docs(num_docs).items():
            for _ in range(3):
                try:
                    _ = self.collection.upsert(k, v, transcoder=tc)
                    self._loaded_docs[k] = v
                    break
                except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                    time.sleep(3)
                    continue
                except Exception as ex:
                    print(ex)
                    raise

        if multi_tests_suite is False:
            for k, v in self.data_provider.get_bytes_docs(num_docs, b'XXXX').items():
                for _ in range(3):
                    try:
                        _ = self.collection.upsert(k, v, transcoder=tc)
                        self._loaded_docs[k] = v
                        break
                    except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                        time.sleep(3)
                        continue
                    except Exception as ex:
                        print(ex)
                        raise

            for k, v in self.data_provider.get_counter_docs(num_docs, 100).items():
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

        tc = RawStringTranscoder()
        for k, v in self.data_provider.get_utf8_docs(num_docs).items():
            for _ in range(3):
                try:
                    _ = self.collection.upsert(k, v, transcoder=tc)
                    self._loaded_docs[k] = v
                    break
                except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                    time.sleep(3)
                    continue
                except Exception as ex:
                    print(ex)
                    raise

        if multi_tests_suite is False:
            for k, v in self.data_provider.get_utf8_docs(num_docs, 'XXXX').items():
                for _ in range(3):
                    try:
                        _ = self.collection.upsert(k, v, transcoder=tc)
                        self._loaded_docs[k] = v
                        break
                    except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                        time.sleep(3)
                        continue
                    except Exception as ex:
                        print(ex)
                        raise

        self._doc_types = ['bytes', 'bytes_empty', 'counter', 'counter_empty', 'utf8', 'utf8_empty']

    def setup(self,
              collection_type,  # type: CollectionType
              test_suite=None,  # type: Optional[str]
              ):

        if collection_type == CollectionType.NAMED:
            self.enable_collection_mgmt().enable_named_collections()
            TestEnvironment.try_n_times(5, 3, self.setup_named_collections)

        multi = test_suite.split('.')[-1] == 'binary_collection_multi_t'
        TestEnvironment.try_n_times(5, 3, self.load_data, multi_tests_suite=multi)

    def teardown(self,
                 collection_type,  # type: CollectionType
                 ):

        TestEnvironment.try_n_times(5, 3, self.purge_data)

        if collection_type == CollectionType.NAMED:
            TestEnvironment.try_n_times(5, 3, self.teardown_named_collections)

    @classmethod
    def from_environment(cls,
                         env  # type: TestEnvironment
                         ) -> BinaryTestEnvironment:

        env_args = {
            'bucket': env.bucket,
            'cluster': env.cluster,
            'default_collection': env.default_collection,
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        cb_env = cls(**env_args)
        return cb_env
