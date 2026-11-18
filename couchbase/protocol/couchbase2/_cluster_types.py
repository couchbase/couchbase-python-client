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

"""The requests the cluster's and a scope's builders return."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from couchbase.common._option_diagnostics import OptionsBlock
from couchbase.common.protocol import CouchbaseProtocol

if TYPE_CHECKING:
    from couchbase.common._observability.handler import OpAttributeOptions
    from couchbase.protocol.couchbase2._generated.query.v1 import query_pb2
    from couchbase.serializer import Serializer


class ClusterOptionsBlock(OptionsBlock, protocol=CouchbaseProtocol.COUCHBASE2):
    Query = 'QueryOptions'
    WaitUntilReady = 'WaitUntilReadyOptions'


@dataclass(frozen=True)
class QueryRequest:
    message: query_pb2.QueryRequest
    serializer: Serializer
    timeout: float
    readonly: bool
    span_attributes: OpAttributeOptions


@dataclass(frozen=True)
class WaitUntilReadyRequest:
    timeout: float
