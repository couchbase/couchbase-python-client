from __future__ import annotations

import logging
from threading import (Event,
                       Lock,
                       Thread)
from typing import (TYPE_CHECKING,
                    Iterator,
                    Union)

from ..generated.run import top_level_pb2 as run
from ..generated.shared import exceptions_pb2 as exceptions
from ..generated.streams import top_level_pb2 as streams

if TYPE_CHECKING:
    import multiprocessing as mp
    import queue

    from ..commands import SdkCommand


class Stream:
    def __init__(self,
                 run_id,  # type: str
                 results,  # type: Union[mp.SimpleQueue, queue.Queue]
                 stream_type,  # type: streams.Type
                 stream_id,  # type: str
                 on_demand,  # type: bool
                 iterator,  # type: Iterator
                 ):

        self._run_id = run_id
        self._stream_id = stream_id
        self._type = stream_type
        self._results = results
        self._on_demand = on_demand
        self._iterator = iterator
        self._thread = None
        self._stop = Event()
        self._requested_item_count = 0
        self._requested_item_count_lock = Lock()
        self._logger = logging.getLogger(__name__)

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def stream_id(self) -> str:
        return self._stream_id

    def create(self) -> None:
        self._thread = Thread(target=self._run)
        self._send_created_signal()

    def start(self) -> None:
        self._logger.info(f'Starting stream {self._stream_id}')
        self._thread.start()

    def _run(self) -> None:
        self._logger.info('Running stream...')
        self._logger.info(f'On demand? {self._on_demand}')
        while True:
            if self._stop.is_set():
                self._send_cancelled_signal()
                break
            if (not self._on_demand) or (self._requested_item_count > 0):
                try:
                    result = next(self._iterator)
                except StopIteration:
                    self._send_complete_signal()
                    return
                except Exception as e:
                    self._logger.info(str(e))
                    raise e

                if isinstance(result, exceptions.Exception):
                    self._send_error_signal(result)
                    return

                self._results.put(result)

                if self._on_demand:
                    with self._requested_item_count_lock:
                        self._requested_item_count -= 1

    def _send_created_signal(self) -> None:
        self._logger.info('Sending stream created signal')
        signal = streams.Signal(created=streams.Created(stream_id=self._stream_id, type=self._type))
        self._results.put(run.Result(stream=signal))

    def _send_cancelled_signal(self) -> None:
        self._logger.info('Sending stream cancelled signal')
        signal = streams.Signal(cancelled=streams.Cancelled(stream_id=self._stream_id))
        self._results.put(run.Result(stream=signal))

    def _send_complete_signal(self) -> None:
        self._logger.info('Sending stream complete signal')
        signal = streams.Signal(complete=streams.Complete(stream_id=self._stream_id))
        self._results.put(run.Result(stream=signal))

    def _send_error_signal(self,
                           exception,  # type: exceptions.Exception
                           ) -> None:
        self._logger.info('Sending stream error signal')
        signal = streams.Signal(error=streams.Error(stream_id=self._stream_id, exception=exception))
        self._results.put(run.Result(stream=signal))

    def cancel(self) -> None:
        self._stop.set()

    def wait(self) -> None:
        self._thread.join()

    def request_items(self, num_items) -> None:
        with self._requested_item_count_lock:
            self._requested_item_count += num_items
            self._logger.info(f'{num_items} requested from stream - {self._requested_item_count} total')

    @classmethod
    def build_stream(cls,
                     run_id,  # type: str
                     results,  # type: Union[mp.SimpleQueue, queue.Queue]
                     sdk_command,  # type: SdkCommand
                     iterator,  # type: Iterator
                     ) -> Stream:

        stream_strategy = sdk_command.stream_config.WhichOneof('stream_when')
        if stream_strategy == 'automatically':
            on_demand = False
        elif stream_strategy == 'on_demand':
            on_demand = True
        else:
            raise NotImplementedError(f"Stream strategy '{stream_strategy}' not supported")

        return cls(run_id, results, sdk_command.stream_type, sdk_command.stream_config.stream_id, on_demand, iterator)
