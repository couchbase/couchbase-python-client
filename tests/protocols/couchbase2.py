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

"""couchbase2's answers to the shared environments, read off the cluster's impl.

The gateway reports no cluster info and no topology.  The version and the node count are the
ones the run's config declares: the version always, since the version gates cannot do without
one, and the node count when given.  The KV endpoints and the developer preview flag are
unknown, and the fixtures that need them skip.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from acouchbase.protocol.couchbase2._cluster_impl import AsyncClusterImpl
    from couchbase.common._observability import MeterProtocol, TracerProtocol
    from couchbase.protocol.couchbase2._cluster_impl import ClusterImpl


class Couchbase2ClusterView:
    def __init__(self, cluster: Any, declared_version: str | None, declared_num_nodes: int | None) -> None:
        # txcouchbase's impl is an AsyncClusterImpl.
        self._cluster_impl: ClusterImpl | AsyncClusterImpl = cluster._impl
        self._declared_version = declared_version
        self._declared_num_nodes = declared_num_nodes

    @property
    def reads_cluster_info(self) -> bool:
        return False

    @property
    def reports_node_count(self) -> bool:
        return self._declared_num_nodes is not None

    @property
    def reports_kv_endpoints(self) -> bool:
        return False

    @property
    def node_count(self) -> int | None:
        return self._declared_num_nodes

    @property
    def is_developer_preview(self) -> bool | None:
        return None

    @property
    def server_version(self) -> str | None:
        return self._declared_version

    @property
    def server_version_full(self) -> str | None:
        return self._declared_version

    @property
    def server_version_short(self) -> float | None:
        if self._declared_version is None:
            return None
        major, minor = self._declared_version.split('.')[:2]
        return float(f'{major}.{minor}')

    @property
    def meter(self) -> MeterProtocol:
        return self._cluster_impl.observability_instruments.meter

    @property
    def tracer(self) -> TracerProtocol:
        """The tracer as the caller passed it, unwrapped from the SDK's `WrappedTracer`."""
        return self._cluster_impl.observability_instruments.tracer.tracer
