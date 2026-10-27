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
from typing import TYPE_CHECKING

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._option_diagnostics import OptionsBlock
from couchbase.common.protocol import CouchbaseProtocol

if TYPE_CHECKING:
    from couchbase.protocol.classic._analytics import AnalyticsQuery
    from couchbase.protocol.classic._n1ql import N1QLQuery
    from couchbase.protocol.classic._search import SearchQueryBuilder


class ScopeOptionsBlock(OptionsBlock, protocol=CouchbaseProtocol.CLASSIC):
    Analytics = 'AnalyticsOptions'
    Query = 'QueryOptions'
    Search = 'SearchOptions'


@dataclass
class AnalyticsQueryRequest:
    analytics_query: AnalyticsQuery
    obs_handler: ObservableRequestHandler


@dataclass
class QueryRequest:
    n1ql_query: N1QLQuery
    obs_handler: ObservableRequestHandler


@dataclass
class SearchQueryRequest:
    query_builder: SearchQueryBuilder
    obs_handler: ObservableRequestHandler
    bucket_name: str
    scope_name: str
