import logging
from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:
    from ..generated.streams.top_level_pb2 import CancelRequest, RequestItemsRequest
    from .stream import Stream


class StreamOwner:
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self._streams: Dict[str, Stream] = {}

    def cancel(self,
               request,  # type: CancelRequest
               ) -> None:
        self._logger.info('Cancelling stream')
        self._streams[request.stream_id].cancel()
        del self._streams[request.stream_id]

    def request_items(self,
                      request,  # type: RequestItemsRequest
                      ) -> None:
        self._logger.info('Requesting items')
        stream_id = request.stream_id
        self._streams[stream_id].request_items(request.num_items)

    def wait_for_all_streams_from_run(self,
                                      run_id,  # type: str
                                      ) -> None:
        self._logger.info(f'Waiting for streams from run {run_id}')

        streams = list(filter((lambda s: s.run_id == run_id), self._streams.values()))
        for stream in streams:
            stream.wait()
            del self._streams[stream.stream_id]

        self._logger.info(f'All streams from run {run_id} have finished')

    def register_stream(self,
                        stream,  # type: Stream
                        ) -> None:
        stream_id = stream.stream_id
        if stream_id in self._streams:
            raise RuntimeError(f'Stream with ID {stream_id} already exists')
        self._logger.info(f'Registering stream {stream_id}')
        self._streams[stream_id] = stream
        stream.create()
