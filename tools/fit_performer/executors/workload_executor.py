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
