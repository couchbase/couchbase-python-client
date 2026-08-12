import logging
from typing import (TYPE_CHECKING,
                    Optional,
                    Union)

from .multi_process_executor import MultiProcessExecutor
from .multi_thread_executor import MultiThreadExecutor

if TYPE_CHECKING:
    from ..generated.run import top_level_pb2 as run
    from ..streaming import StreamOwner
    from ..telemetry.span_owner import SpanOwner
    from ..utils import ConnectionCache
    from ..workloads import Counters


class WorkloadExecutorBuilder:

    @staticmethod
    def build_executor(run_request,  # type: run.Request
                       run_id,       # type: str
                       cached_conn,  # type: ConnectionCache
                       stream_owner,  # type: StreamOwner
                       counters,     # type: Counters
                       span_owner=None,  # type: Optional[SpanOwner]
                       ) -> Union[MultiProcessExecutor, MultiThreadExecutor]:
        """ Factory function, builds and returns the appropriate workload executor instance.

        Args:
            run_request (run.Request): The run request received from the driver.
            run_id (str): The ID of the run that will own this executor.
            cached_conn (ConnectionCache): The cached connection object for the cluster used for the executor.
            stream_owner (StreamOwner): The StreamOwner object that will manage any streams created by the executor.
            counters (Counters): The performer-level counter registry used for counter bounds.

        Returns:
            Union[MultiProcessExecutor, MultiThreadExecutor]: The workload executor object

        Raises:
            NotImplementedError: If an unsupported API is requested
        """
        logger = logging.getLogger(__name__)
        mechanism = run_request.tunables.get("concurrencyMechanism", "multithreading")
        if mechanism == "multithreading":
            logger.info('Using multi-thread executor')
            return MultiThreadExecutor.build_executor(
                run_id,
                run_request,
                cached_conn.cluster,
                counters,
                stream_owner,
                span_owner=span_owner)
        elif mechanism == "multiprocessing":
            logger.info('Using multi-process executor')
            return MultiProcessExecutor.build_executor(
                run_id,
                run_request,
                cached_conn.hostname,
                cached_conn.mp_cluster_options,
                counters,
                obs_config=cached_conn.obs_config,
                span_owner=span_owner
            )
        else:
            raise NotImplementedError(f"Concurrency mechanism {str(mechanism)} not supported")
