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

"""The `txcouchbase.result` import path.

The same objects as `couchbase.result`, re-exported so Twisted code imports them from its own
package.
"""

__all__ = [
    'AnalyticsResult',
    'ClusterInfoResult',
    'ContentProxy',
    'ContentSubdocProxy',
    'CounterResult',
    'DiagnosticsResult',
    'ExistsResult',
    'GetReplicaResult',
    'GetResult',
    'LookupInReplicaResult',
    'LookupInResult',
    'MultiCounterResult',
    'MultiExistsResult',
    'MultiGetReplicaResult',
    'MultiGetResult',
    'MultiMutationResult',
    'MultiResult',
    'MultiResultType',
    'MutateInResult',
    'MutationResult',
    'MutationToken',
    'OperationResult',
    'PingResult',
    'QueryResult',
    'Result',
    'ScanResult',
    'SearchResult',
    'ValueResult',
    'ViewResult',
]

from couchbase.common.result import ClusterInfoResult  # noqa: F401
from couchbase.common.result import ContentProxy  # noqa: F401
from couchbase.common.result import ContentSubdocProxy  # noqa: F401
from couchbase.common.result import CounterResult  # noqa: F401
from couchbase.common.result import DiagnosticsResult  # noqa: F401
from couchbase.common.result import ExistsResult  # noqa: F401
from couchbase.common.result import GetReplicaResult  # noqa: F401
from couchbase.common.result import GetResult  # noqa: F401
from couchbase.common.result import LookupInReplicaResult  # noqa: F401
from couchbase.common.result import LookupInResult  # noqa: F401
from couchbase.common.result import MultiCounterResult  # noqa: F401
from couchbase.common.result import MultiExistsResult  # noqa: F401
from couchbase.common.result import MultiGetReplicaResult  # noqa: F401
from couchbase.common.result import MultiGetResult  # noqa: F401
from couchbase.common.result import MultiMutationResult  # noqa: F401
from couchbase.common.result import MultiResult  # noqa: F401
from couchbase.common.result import MultiResultType  # noqa: F401
from couchbase.common.result import MutateInResult  # noqa: F401
from couchbase.common.result import MutationResult  # noqa: F401
from couchbase.common.result import MutationToken  # noqa: F401
from couchbase.common.result import OperationResult  # noqa: F401
from couchbase.common.result import PingResult  # noqa: F401
from couchbase.common.result import Result  # noqa: F401
from couchbase.common.result import ScanResult  # noqa: F401
from couchbase.common.result import ValueResult  # noqa: F401
from couchbase.result import AnalyticsResult  # noqa: F401
from couchbase.result import QueryResult  # noqa: F401
from couchbase.result import SearchResult  # noqa: F401
from couchbase.result import ViewResult  # noqa: F401
