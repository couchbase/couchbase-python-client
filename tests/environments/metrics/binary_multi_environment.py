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
from typing import List

from couchbase.exceptions import AmbiguousTimeoutException, UnAmbiguousTimeoutException
from couchbase.transcoder import RawBinaryTranscoder
from tests.environments.metrics.metrics_environment import MetricsEnvironment


class BinaryMultiMetricsEnvironment(MetricsEnvironment):

    def setup_binary_data(self) -> None:  # noqa: C901
        """Setup binary and counter documents for testing binary operations."""
        tc = RawBinaryTranscoder()
        num_docs = 30  # More docs for multi operations

        # Load empty bytes documents (b'') for append/prepend operations
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

        # Load counter documents
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

    def get_multiple_existing_binary_docs_by_type(self, doc_type: str, num_docs: int) -> List[str]:
        """Get multiple existing binary documents by type (bytes or counter).

        Args:
            doc_type: Either 'bytes' or 'counter'
            num_docs: Number of documents to retrieve

        Returns:
            List of keys
        """
        if doc_type == 'bytes_empty':
            filtered_keys = set([k for k in self._loaded_docs.keys() if 'bytes_empty' in k])
        elif doc_type == 'counter':
            filtered_keys = set([k for k in self._loaded_docs.keys() if '::counter::' in k])
        else:
            raise ValueError(f"Unsupported doc_type: {doc_type}. Must be 'bytes' or 'counter'")

        available_keys = filtered_keys.difference(self._used_docs)
        keys = random.choices(list(available_keys), k=num_docs)
        self._used_docs.update(keys)
        return keys
