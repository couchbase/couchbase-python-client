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
from couchbase.transcoder import RawBinaryTranscoder, RawStringTranscoder
from tests.environments import CollectionType
from tests.environments.test_environment import TestEnvironment


class FakeTestObj:
    PROP = "fake prop"
    PROP1 = 12345


class TranscoderTestEnvironment(TestEnvironment):

    def get_existing_doc_by_type(self, doc_type, key_only=False):
        if doc_type == 'bytes':
            filtered_keys = set([k for k in self._loaded_docs.keys() if '::bytes::' in k])
        elif doc_type == 'json':
            filtered_keys = set([k for k, v in self._loaded_docs.items()
                                if isinstance(v, dict) and v['type'] == 'simple'])
        elif doc_type == 'utf8':
            filtered_keys = set([k for k in self._loaded_docs.keys() if '::utf8::' in k])

        available_keys = filtered_keys.difference(self._used_docs)
        key = random.choice(list(available_keys))
        self._used_docs.add(key)
        if key_only is True:
            return key
        return key, self._loaded_docs[key]

    def get_new_doc_by_type(self, doc_type, key_only=False):
        key = None
        doc = None
        if doc_type == 'bytes':
            for k, v in self.data_provider.get_bytes_docs(1, 'bytes content'.encode('utf-8')).items():
                key = k
                doc = v
                break
        if doc_type == 'hex':
            key = self.data_provider.generate_keys(1)[0]
            hex_arr = ['ff0102030405060708090a0b0c0d0e0f',
                       '101112131415161718191a1b1c1d1e1f',
                       '202122232425262728292a2b2c2d2e2f',
                       '303132333435363738393a3b3c3d3e3f']
            doc = bytes.fromhex(''.join(hex_arr))
        elif doc_type == 'json':
            doc = self.data_provider.get_simple_docs(1)[0]
            key = f'{doc["id"]}'
        elif doc_type == 'obj':
            key = self.data_provider.generate_keys(1)[0]
            doc = FakeTestObj()
        elif doc_type == 'utf8':
            for k, v in self.data_provider.get_utf8_docs(1, 'utf8 content').items():
                key = k
                doc = v
                break

        self._used_extras.add(key)
        if key_only is True:
            return key
        return key, doc

    def load_data(self):  # noqa: C901
        tc = RawBinaryTranscoder()
        for k, v in self.data_provider.get_bytes_docs(10, 'bytes content'.encode('utf-8')).items():
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

        tc = RawStringTranscoder()
        for k, v in self.data_provider.get_utf8_docs(10, 'utf8 content').items():
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

        for v in self.data_provider.get_simple_docs(10):
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

        self._doc_types = ['bytes', 'json', 'utf8']

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
                         ) -> TranscoderTestEnvironment:

        env_args = {
            'bucket': env.bucket,
            'cluster': env.cluster,
            'default_collection': env.default_collection,
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        cb_env = cls(**env_args)
        return cb_env
