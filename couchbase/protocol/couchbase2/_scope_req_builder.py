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

from typing import TYPE_CHECKING, Any

from couchbase.common.options import QueryOptions
from couchbase.protocol.couchbase2._query import create_query_request

if TYPE_CHECKING:
    from couchbase.protocol.couchbase2._cluster_types import QueryRequest
    from couchbase.protocol.couchbase2._core import ClusterCore


class ScopeRequestBuilder:

    def __init__(self, core: ClusterCore, bucket_name: str, scope_name: str) -> None:
        self._core = core
        self._bucket_name = bucket_name
        self._scope_name = scope_name

    def build_query_request(self, statement: str, *options: object, **kwargs: Any) -> QueryRequest:
        """A query runs in this scope unless the caller names another context."""
        stated = any('query_context' in o for o in options if isinstance(o, QueryOptions))
        if not (stated or 'query_context' in kwargs):
            kwargs['query_context'] = f'`{self._bucket_name}`.`{self._scope_name}`'
        return create_query_request(self._core, statement, *options, **kwargs)
