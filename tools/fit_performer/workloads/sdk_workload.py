import logging
from typing import (TYPE_CHECKING,
                    Any,
                    Iterator,
                    Optional,
                    Union)

from ..commands import SdkCommandBuilder, SdkCommandResult
from ..streaming import Stream
from .workload import Workload

if TYPE_CHECKING:
    import multiprocessing as mp
    import queue

    from ..streaming import StreamOwner


class SdkWorkload(Workload):
    def __init__(self, workload, run_id, conn=None, span_owner=None):
        self._logger = logging.getLogger(__name__)
        super().__init__(workload, run_id, conn=conn)
        self._span_owner = span_owner

    def set_connection(self, connection) -> None:
        self._conn = connection

    def set_span_owner(self, span_owner) -> None:
        self._span_owner = span_owner

    @property
    def num_commands(self) -> int:
        return self._num_commands

    def execute(self,
                results,  # type:  Union[mp.Queue, queue.Queue]
                stream_owner=None,  # type: Optional[StreamOwner]
                ) -> None:

        executed_cmd_count = 0
        while self.within_bounds():
            cmd = self._workload.command[executed_cmd_count % self.num_commands]
            self.execute_command(cmd, results, stream_owner)
            executed_cmd_count += 1

    def execute_command(self,
                        command,  # type: Any
                        results,  # type:  Union[mp.Queue, queue.Queue]
                        stream_owner=None,  # type: Optional[StreamOwner]
                        ) -> None:
        try:
            sdk_command = SdkCommandBuilder.build_command(self._conn, command, self._counters, self._span_owner)
            result = sdk_command.execute_command()
        except (ValueError, NotImplementedError) as ex:
            raise ex
        except Exception as ex:
            results.put(SdkCommandResult.exception_as_result(ex))
        else:
            if isinstance(result, Iterator):
                stream = Stream.build_stream(self._run_id, results, sdk_command, result)
                stream_owner.register_stream(stream)
                stream.start()
            else:
                results.put(result)
