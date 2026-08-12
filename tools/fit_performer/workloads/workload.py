from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import (TYPE_CHECKING,
                    Any,
                    Optional,
                    Union)

from .bounds import build_bounds_executor

if TYPE_CHECKING:
    import multiprocessing as mp
    import queue

    from acouchbase.cluster import AsyncCluster
    from couchbase.cluster import Cluster

    from ..streaming import StreamOwner
    from .bounds import BoundsExecutor, Counters


class Workload(ABC):

    def __init__(self,
                 workload: Any,
                 run_id: str,
                 conn: Union[Cluster, AsyncCluster] = None
                 ):
        """ Initialize a workload instance.

        Args:
            workload (protobuf Workload): The gRPC workload object.
            run_id (str): The ID of the run request that created this workload.
            conn (Union[Cluster, AsyncCluster], optional): The cluster instance.
        """
        self._workload = workload
        self._run_id = run_id
        self._conn = conn
        self._num_commands = len(self._workload.command)

        self._logger = logging.getLogger(__name__)

        # The workload's counters & bounds, set via set_counters_and_bounds.
        self._bounds: Optional[BoundsExecutor] = None
        self._counters: Optional[Counters] = None

    @property
    @abstractmethod
    def num_commands(self) -> int:
        """ Return the number of commands in the workload
        """
        raise NotImplementedError('num_commands property must be implemented by concrete class.')

    @abstractmethod
    def set_connection(self,
                       connection  # type: Union[Cluster, AsyncCluster]
                       ) -> None:
        """ Set a workload's connection.

        Since the workload is built prior to the execution of the workload, there needs to a
        way to set the workload connection dynamically.  Specifically important for multi-processing
        as we need to create the cluster w/in the child process.  This method allows each child process
        to pass in the it's own connection to the workload.

        Args:
            connection (Union[Cluster, AsyncCluster]): The cluster instance.
        """
        raise NotImplementedError('set_connection method must be implemented by concrete class.')

    @abstractmethod
    def execute(self,
                results,  # type:  Union[mp.Queue, queue.Queue]
                stream_owner=None,  # type: Optional[StreamOwner]
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
            results (Union[mp.Queue, queue.Queue]): Queue that stores results from the workload commands.
                If the workload is using multi-processing, the queue should be a multiprocessing.Queue,
                otherwise a queue.Queue.
            stream_owner (StreamOwner, optional): The stream owner object to submit any streams created by this
                workload.

        """
        raise NotImplementedError('execute method must be implemented by concrete class.')

    @abstractmethod
    def execute_command(self,
                        command,  # type: Any
                        command_count,  # type: int
                        results) -> None:
        """ Execute one command from a workload.

        Build the SdkCommand, execute it and place the result in the results queue.

        Args:
            command (Union[protobuf Command object, SdkCommand]): The protobuf command object.
            command_count (int): The number of commands in the workload that have been executed.
            results (Union[mp.Queue, queue.Queue]): Queue that stores results from the workload commands.
                If the workload is using multi-processing, the queue should be a multiprocessing.Queue,
                otherwise a queue.Queue.
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
        return self._bounds.can_execute()
