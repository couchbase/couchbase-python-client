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
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streams.top_level_pb2 import CancelRequest, RequestItemsRequest

    from .stream import Stream


class StreamOwner:
    def __init__(self) -> None:
        self._logger = logging.getLogger(__name__)
        self._streams: dict[str, Stream] = {}

    def cancel(self,
               request: CancelRequest,
               ) -> None:
        self._logger.info('Cancelling stream')
        self._streams[request.stream_id].cancel()
        del self._streams[request.stream_id]

    def request_items(self,
                      request: RequestItemsRequest,
                      ) -> None:
        self._logger.info('Requesting items')
        stream_id = request.stream_id
        self._streams[stream_id].request_items(request.num_items)

    def wait_for_all_streams_from_run(self,
                                      run_id: str,
                                      ) -> None:
        self._logger.info(f'Waiting for streams from run {run_id}')

        streams = list(filter((lambda s: s.run_id == run_id), self._streams.values()))
        for stream in streams:
            stream.wait()
            del self._streams[stream.stream_id]

        self._logger.info(f'All streams from run {run_id} have finished')

    def register_stream(self,
                        stream: Stream,
                        ) -> None:
        stream_id = stream.stream_id
        if stream_id in self._streams:
            raise RuntimeError(f'Stream with ID {stream_id} already exists')
        self._logger.info(f'Registering stream {stream_id}')
        self._streams[stream_id] = stream
        stream.create()
