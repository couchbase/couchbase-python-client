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

"""Classic's answers to the shared environments, read off the cluster's impl."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from acouchbase.protocol.classic._cluster_impl import AsyncClusterImpl
    from couchbase.common._observability import MeterProtocol, TracerProtocol
    from couchbase.protocol.classic._cluster_impl import ClusterImpl


class ClassicClusterView:
    def __init__(self, cluster: Any) -> None:
        # txcouchbase's impl is an AsyncClusterImpl.
        self._cluster_impl: ClusterImpl | AsyncClusterImpl = cluster._impl

    @property
    def reads_cluster_info(self) -> bool:
        # The impl caches the last `get_cluster_info()` and answers the version and node count from it.
        return True

    @property
    def reports_topology(self) -> bool:
        return True

    @property
    def node_count(self) -> int | None:
        cluster_info = self._cluster_impl.cluster_info
        if cluster_info is None or cluster_info.nodes is None:
            return None
        return len(cluster_info.nodes)

    @property
    def is_developer_preview(self) -> bool | None:
        return self._cluster_impl.is_developer_preview

    @property
    def server_version(self) -> str | None:
        return self._cluster_impl.server_version

    @property
    def server_version_full(self) -> str | None:
        return self._cluster_impl.server_version_full

    @property
    def server_version_short(self) -> float | None:
        return self._cluster_impl.server_version_short

    @property
    def meter(self) -> MeterProtocol:
        return self._cluster_impl.observability_instruments.meter

    @property
    def tracer(self) -> TracerProtocol:
        """The tracer as the caller passed it.

        The SDK wraps it in a `WrappedTracer`, its own record of which of the two tracer bases it
        found, so unwrapping it here reads the SDK's answer rather than repeating the question.
        """
        return self._cluster_impl.observability_instruments.tracer.tracer
