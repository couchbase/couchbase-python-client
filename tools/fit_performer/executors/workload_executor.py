import multiprocessing as mp
import queue
from abc import ABC, abstractmethod
from typing import (TYPE_CHECKING,
                    Optional,
                    Union)

if TYPE_CHECKING:

    from acouchbase.cluster import AsyncCluster
    from couchbase.cluster import Cluster
    from couchbase.options import ClusterOptions


class WorkloadExecutor(ABC):

    @property
    @abstractmethod
    def workload_complete(self) -> bool:
        raise NotImplementedError('workload_complete property must be implemented by concrete class.')

    @property
    @abstractmethod
    def results(self) -> Union[mp.Queue, queue.Queue]:
        raise NotImplementedError('results property must be implemented by concrete class.')

    @property
    @abstractmethod
    def supports_streaming(self) -> bool:
        raise NotImplementedError('supports_streaming property must be implemented by concrete class.')

    # @TODO:  Would this sort of method be useful?  Ponder...
    @abstractmethod
    def set_connection(self,
                       conn=None,  # type: Optional[Union[AsyncCluster, Cluster]]
                       hostname=None,  # type: Optional[str]
                       options=None,  # type: Optional[ClusterOptions]
                       ) -> None:
        raise NotImplementedError('set_connection method must be implemented by concrete class.')

    @abstractmethod
    def build_workloads(self, request) -> None:
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
