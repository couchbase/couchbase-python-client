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

from tests.environments.tracing.async_base_tracing_environment import AsyncBaseTracingEnvironment


class AsyncKeyValueTracingEnvironment(AsyncBaseTracingEnvironment):
    """Async environment for single key-value operation tracing tests.

    This environment is used for testing individual KV operations like:
    - get, get_and_lock, get_and_touch
    - insert, upsert, replace, remove
    - touch, unlock, exists

    Also used for data structure operations like:
    - CouchbaseList operations (append, prepend, get_at, remove_at, etc.)
    - CouchbaseMap operations (add, remove, get, exists, etc.)
    - CouchbaseQueue operations (push, pop, size, etc.)
    - CouchbaseSet operations (add, remove, contains, size, etc.)

    Documents: Loads 50 standard JSON documents
    """
    pass
