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

from datetime import timedelta
from typing import TYPE_CHECKING, Any

from couchbase.common._option_args import merge
from couchbase.common._search_args import (check_search_query_argument,
                                           search_args,
                                           search_argument)
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.protocol.couchbase2._cluster_types import ClusterOptionsBlock, WaitUntilReadyRequest
from couchbase.protocol.couchbase2._query import create_query_request
from couchbase.protocol.couchbase2._search import create_search_request

if TYPE_CHECKING:
    from couchbase.common.options import SearchOptions, WaitUntilReadyOptions
    from couchbase.common.search_queries import SearchQuery
    from couchbase.common.search_request import SearchRequest
    from couchbase.protocol.couchbase2._cluster_types import QueryRequest, SearchQueryRequest
    from couchbase.protocol.couchbase2._core import ClusterCore


class ClusterRequestBuilder:

    def __init__(self, core: ClusterCore) -> None:
        self._core = core

    def build_query_request(self, statement: str, *options: object, **kwargs: Any) -> QueryRequest:
        return create_query_request(self._core, statement, *options, **kwargs)

    def build_search_request(self,
                             index: str,
                             request: SearchRequest | SearchQuery,
                             options: SearchOptions | None = None,
                             **kwargs: Any) -> SearchQueryRequest:
        query, vector_search = search_argument(request)
        return create_search_request(self._core, index, query, vector_search, search_args(options, **kwargs))

    def build_search_query_request(self,
                                   index: str,
                                   query: SearchQuery,
                                   options: SearchOptions | None = None,
                                   **kwargs: Any) -> SearchQueryRequest:
        check_search_query_argument(query)
        return create_search_request(self._core, index, query, None, search_args(options, **kwargs))

    def build_wait_until_ready_request(self,
                                       timeout: timedelta,
                                       opts: WaitUntilReadyOptions | None = None,
                                       **kwargs: Any) -> WaitUntilReadyRequest:
        self._core.validate_options(ClusterOptionsBlock.WaitUntilReady, merge(kwargs, opts))
        if not isinstance(timeout, timedelta):
            raise InvalidArgumentException(f'Expected timeout to be a timedelta, got {timeout!r}.')
        return WaitUntilReadyRequest(timeout.total_seconds())
