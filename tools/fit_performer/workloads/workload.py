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

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from .bounds import build_bounds_executor

if TYPE_CHECKING:
    from couchbase.cluster import Cluster

    from sdk import workload_pb2 as sdk_pb

    from ..executors.workload_executor import ResultQueue
    from ..streaming import StreamOwner
    from .bounds import BoundsExecutor, Counters


class Workload(ABC):

    def __init__(self,
                 workload: sdk_pb.Workload,
                 run_id: str,
                 conn: Cluster | None = None
                 ) -> None:
        """ Initialize a workload instance.

        Args:
            workload (protobuf Workload): The gRPC workload object.
            run_id (str): The ID of the run request that created this workload.
            conn (Cluster, optional): The cluster instance.
        """
        self._workload = workload
        self._run_id = run_id
        self._conn = conn
        self._num_commands = len(self._workload.command)

        self._logger = logging.getLogger(__name__)

        # The workload's counters & bounds, set via set_counters_and_bounds.
        self._bounds: BoundsExecutor | None = None
        self._counters: Counters | None = None

    @property
    @abstractmethod
    def num_commands(self) -> int:
        """ Return the number of commands in the workload
        """
        raise NotImplementedError('num_commands property must be implemented by concrete class.')

    @abstractmethod
    def set_connection(self,
                       connection: Cluster
                       ) -> None:
        """ Set a workload's connection.

        Since the workload is built prior to the execution of the workload, there needs to a
        way to set the workload connection dynamically.  Specifically important for multi-processing
        as we need to create the cluster w/in the child process.  This method allows each child process
        to pass in the it's own connection to the workload.

        Args:
            connection (Cluster): The cluster instance.
        """
        raise NotImplementedError('set_connection method must be implemented by concrete class.')

    @abstractmethod
    def execute(self,
                results: ResultQueue,
                stream_owner: StreamOwner | None = None,
                ) -> None:
        """ Execute a workload.

        Executing the workload means:
            1.  Checking to make sure that the workload is w/in the bounds and if so,
                executing commands w/in the workload until the workload is either finished
                or no long w/in the bounds.
            2.  Keeping track of the number of commands that have been executed.
            3.  Keeping track of which command w/in the workload's list of commands should
                be executed.

        Args:
            results (ResultQueue): Queue that stores results from the workload commands.
                If the workload is using multi-processing, the queue should be a multiprocessing.SimpleQueue,
                otherwise a queue.Queue.
            stream_owner (StreamOwner, optional): The stream owner object to submit any streams created by this
                workload.

        """
        raise NotImplementedError('execute method must be implemented by concrete class.')

    @abstractmethod
    def execute_command(self,
                        command: sdk_pb.Command,
                        results: ResultQueue,
                        stream_owner: StreamOwner | None = None,
                        ) -> None:
        """ Execute one command from a workload.

        Build the SdkCommand, execute it and place the result in the results queue.

        Args:
            command (protobuf Command): The protobuf command object.
            results (ResultQueue): Queue that stores results from the workload commands.
                If the workload is using multi-processing, the queue should be a multiprocessing.SimpleQueue,
                otherwise a queue.Queue.
            stream_owner (StreamOwner, optional): The stream owner object to submit any streams created by this
                command.
        """
        raise NotImplementedError('execute_command method must be implemented by concrete class.')

    def set_counters_and_bounds(self, counters: Counters) -> None:
        """Build the workload's bounds from its proto definition.

        For counter / counter_eq bounds the shared ``counters`` registry is used so the count is
        coordinated across all workloads, run requests, and (for multi-processing) worker
        processes.

        Args:
            counters (Counters): The performer-level counter registry.

        Raises:
            ValueError: If the counter type or bounds type is not recognized.
        """
        self._counters = counters
        self._bounds = build_bounds_executor(self._workload, counters)

    def within_bounds(self) -> bool:
        """ Check if the workload is still within its specified bounds.

        Returns:
            bool: True if the workload is still within its bounds.  False otherwise.
        """
        if self._bounds is None:
            raise RuntimeError('Bounds have not been set, call set_counters_and_bounds first.')
        return self._bounds.can_execute()
