from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import TYPE_CHECKING

from ..commands import SdkCommandBuilder, SdkCommandResult
from ..streaming import Stream
from .workload import Workload

if TYPE_CHECKING:
    from couchbase.cluster import Cluster

    from sdk import workload_pb2 as sdk_pb

    from ..executors.workload_executor import ResultQueue
    from ..streaming import StreamOwner
    from ..telemetry.span_owner import SpanOwner


class SdkWorkload(Workload):
    def __init__(self,
                 workload: sdk_pb.Workload,
                 run_id: str,
                 conn: Cluster | None = None,
                 span_owner: SpanOwner | None = None,
                 ) -> None:
        self._logger = logging.getLogger(__name__)
        super().__init__(workload, run_id, conn=conn)
        self._span_owner = span_owner

    def set_connection(self, connection: Cluster) -> None:
        self._conn = connection

    def set_span_owner(self, span_owner: SpanOwner | None) -> None:
        self._span_owner = span_owner

    @property
    def num_commands(self) -> int:
        return self._num_commands

    def execute(self,
                results: ResultQueue,
                stream_owner: StreamOwner | None = None,
                ) -> None:

        executed_cmd_count = 0
        while self.within_bounds():
            cmd = self._workload.command[executed_cmd_count % self.num_commands]
            self.execute_command(cmd, results, stream_owner)
            executed_cmd_count += 1

    def execute_command(self,
                        command: sdk_pb.Command,
                        results: ResultQueue,
                        stream_owner: StreamOwner | None = None,
                        ) -> None:
        if self._conn is None or self._counters is None:
            raise RuntimeError('The workload has no connection or counters, set them before executing it.')
        try:
            sdk_command = SdkCommandBuilder.build_command(self._conn, command, self._counters, self._span_owner)
            result = sdk_command.execute_command()
        except (ValueError, NotImplementedError) as ex:
            raise ex
        except Exception as ex:
            results.put(SdkCommandResult.exception_as_result(ex))
        else:
            if isinstance(result, Iterator):
                if stream_owner is None:
                    raise NotImplementedError('Streaming is not supported by this executor.')
                stream = Stream.build_stream(self._run_id, results, sdk_command, result)
                stream_owner.register_stream(stream)
                stream.start()
            else:
                results.put(result)
