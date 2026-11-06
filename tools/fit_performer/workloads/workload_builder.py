from __future__ import annotations

from typing import TYPE_CHECKING

from .sdk_workload import SdkWorkload
from .workload import Workload

if TYPE_CHECKING:
    from couchbase.cluster import Cluster

    from run import workloads_pb2 as run_workloads_pb

    from ..telemetry.span_owner import SpanOwner


class WorkloadBuilder:

    @staticmethod
    def build_workload(workload: run_workloads_pb.Workload,
                       run_id: str,
                       conn: Cluster | None = None,
                       span_owner: SpanOwner | None = None
                       ) -> Workload:
        """Factory function, builds and returns the appropriate workload instance.

        Args:
            workload (protobuf Workload): The Protocol Buffers workload object.
            run_id (str): The ID of the run request that is creating this workload.
            conn (Cluster, optional): The cluster instance.

        Returns:
            Workload: The workload object.

        Raises:
            NotImplementedError: If an unsupported workload type is requested.
        """
        workload_type = workload.WhichOneof('workload')
        if workload_type == 'sdk':
            return SdkWorkload(workload.sdk, run_id, conn, span_owner=span_owner)
        else:
            raise NotImplementedError(f"The workload type '{workload_type}' is not supported")
