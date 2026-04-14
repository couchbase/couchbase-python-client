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

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import StreamingOperationType

if TYPE_CHECKING:
    from couchbase.analytics import AnalyticsQuery
    from couchbase.n1ql import N1QLQuery
    from couchbase.search import SearchQueryBuilder


@dataclass
class AnalyticsQueryRequest:
    analytics_query: AnalyticsQuery
    obs_handler: ObservableRequestHandler
    num_workers: Optional[int] = None

    @property
    def op_name(self) -> str:
        return StreamingOperationType.AnalyticsQuery.value


@dataclass
class QueryRequest:
    n1ql_query: N1QLQuery
    obs_handler: ObservableRequestHandler
    num_workers: Optional[int] = None

    @property
    def op_name(self) -> str:
        return StreamingOperationType.Query.value


@dataclass
class SearchQueryRequest:
    query_builder: SearchQueryBuilder
    obs_handler: ObservableRequestHandler
    bucket_name: str
    scope_name: str
    num_workers: Optional[int] = None

    @property
    def op_name(self) -> str:
        return StreamingOperationType.ViewQuery.value
