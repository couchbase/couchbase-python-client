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
from typing import (Any,
                    Tuple,
                    Union)

from couchbase.exceptions import AmbiguousTimeoutException, UnAmbiguousTimeoutException
from couchbase.transcoder import RawBinaryTranscoder
from tests.environments.tracing.base_tracing_environment import BaseTracingEnvironment


class BinaryTracingEnvironment(BaseTracingEnvironment):
    """Environment for single binary operation tracing tests.

    This environment is used for testing individual binary operations like:
    - append, prepend
    - increment, decrement

    Documents: Loads 10 bytes documents + 10 counter documents (20 total)
    """

    def setup_binary_data(self) -> None:  # noqa: C901
        """Setup binary and counter documents for testing binary operations."""
        tc = RawBinaryTranscoder()
        num_docs = 10

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

    def get_existing_binary_doc_by_type(
        self,
        doc_type: str,
        key_only: bool = False
    ) -> Union[str, Tuple[str, Any]]:
        """Get a single existing binary document by type (bytes or counter).

        Args:
            doc_type: Either 'bytes' or 'counter'
            key_only: If True, return only the key; otherwise return (key, value) tuple

        Returns:
            Either key string (if key_only=True) or (key, value) tuple
        """
        if doc_type == 'bytes_empty':
            filtered_keys = set([k for k in self._loaded_docs.keys() if 'bytes_empty' in k])
        elif doc_type == 'counter':
            filtered_keys = set([k for k in self._loaded_docs.keys() if '::counter::' in k])
        else:
            raise ValueError(f"Unsupported doc_type: {doc_type}. Must be 'bytes' or 'counter'")

        available_keys = filtered_keys.difference(self._used_docs)
        key = random.choice(list(available_keys))
        self._used_docs.add(key)

        if key_only is True:
            return key
        return key, self._loaded_docs[key]
