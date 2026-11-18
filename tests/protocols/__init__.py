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

"""What the shared environments need to know about a connected cluster that no public API gives them.

The environments need the server's version and node count without I/O, because they read them at
class scope in all three api styles, and the meter and tracer a cluster was built with, so a suite
cannot assert on instruments the cluster is not using.  `Cluster.cluster_info()` is a round trip
and awaited in two of the styles, and nothing public returns the instruments.  So each protocol
answers for its own clusters in its own module here, and the environments ask through
`view_of` and never learn which protocol answered.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import (TYPE_CHECKING,
                    Any,
                    Protocol)

from tests.protocols.classic import ClassicClusterView
from tests.protocols.couchbase2 import Couchbase2ClusterView
from tests.test_features import TestProtocol

if TYPE_CHECKING:
    from couchbase.common._observability import MeterProtocol, TracerProtocol
    from tests.couchbase_config import CouchbaseConfig


class ClusterView(Protocol):
    @property
    def reads_cluster_info(self) -> bool:
        """Whether the environments must call `cluster_info()` at connect for this view to answer."""
        ...

    @property
    def reports_node_count(self) -> bool:
        """Whether this view knows the cluster's node count.

        False means unknown by design, and the topology fixtures skip.  True with no answer
        means the environment is broken.
        """
        ...

    @property
    def reports_kv_endpoints(self) -> bool:
        """Whether the protocol can ping the bucket for its KV endpoints; False skips as above."""
        ...

    @property
    def node_count(self) -> int | None: ...

    @property
    def is_developer_preview(self) -> bool | None: ...

    @property
    def server_version(self) -> str | None: ...

    @property
    def server_version_full(self) -> str | None: ...

    @property
    def server_version_short(self) -> float | None: ...

    @property
    def meter(self) -> MeterProtocol: ...

    @property
    def tracer(self) -> TracerProtocol: ...


_VIEWS: dict[TestProtocol, Callable[[Any, CouchbaseConfig], ClusterView]] = {
    TestProtocol.CLASSIC: lambda cluster, _: ClassicClusterView(cluster),
    TestProtocol.COUCHBASE2: lambda cluster, config: Couchbase2ClusterView(cluster,
                                                                           config.server_version,
                                                                           config.num_nodes),
}


def view_of(cluster: Any, config: CouchbaseConfig) -> ClusterView:
    """`cluster` seen through the run's protocol's view, for any of the three api styles' clusters."""
    view = _VIEWS.get(config.protocol)
    if view is None:
        raise NotImplementedError(f'no cluster view for the {config.protocol.value} protocol')
    return view(cluster, config)
