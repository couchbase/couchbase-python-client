from __future__ import annotations

import queue
from abc import ABC, abstractmethod
from multiprocessing.queues import SimpleQueue
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from couchbase.cluster import Cluster
    from couchbase.options import ClusterOptions

    from run import top_level_pb2 as run_pb

    # Where workloads put their results, and `None` once every workload has finished.  A thread
    # executor uses an in-process queue and a process executor one its workers can reach.
    ResultQueue: TypeAlias = queue.Queue[run_pb.Result | None] | SimpleQueue[run_pb.Result | None]


class WorkloadExecutor(ABC):

    @property
    @abstractmethod
    def workload_complete(self) -> bool:
        raise NotImplementedError('workload_complete property must be implemented by concrete class.')

    @property
    @abstractmethod
    def results(self) -> ResultQueue:
        raise NotImplementedError('results property must be implemented by concrete class.')

    @property
    @abstractmethod
    def supports_streaming(self) -> bool:
        raise NotImplementedError('supports_streaming property must be implemented by concrete class.')

    # @TODO:  Would this sort of method be useful?  Ponder...
    @abstractmethod
    def set_connection(self,
                       conn: Cluster | None = None,
                       hostname: str | None = None,
                       options: ClusterOptions | None = None,
                       ) -> None:
        raise NotImplementedError('set_connection method must be implemented by concrete class.')

    @abstractmethod
    def build_workloads(self, request: run_pb.Request) -> None:
        raise NotImplementedError('build_workloads method must be implemented by concrete class.')

    @abstractmethod
    def create_pool(self) -> None:
        raise NotImplementedError('create_pool method must be implemented by concrete class.')

    @abstractmethod
    def execute_workloads(self) -> None:
        raise NotImplementedError('execute_workloads method must be implemented by concrete class.')

    @abstractmethod
    def shutdown(self) -> None:
        raise NotImplementedError('shutdown method must be implemented by concrete class.')
