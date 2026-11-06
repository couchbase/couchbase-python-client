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
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from run.top_level_pb2 import (BatchedResult,
                               Request,
                               Result)

if TYPE_CHECKING:
    from .workload_executor import WorkloadExecutor


class RequestExecutor:
    def __init__(self, request: Request, executor: WorkloadExecutor) -> None:
        self._batch_size: int | None = None
        self._executor = executor
        self._request = request
        self._conn_id = self._request.workloads.cluster_connection_id
        self._logger = logging.getLogger(__name__)

    def set_batch_size(self) -> None:
        self._batch_size = 1
        if self._request.HasField("config") and self._request.config.HasField("streaming_config"):
            if self._request.config.streaming_config.HasField("batch_size"):
                self._batch_size = self._request.config.streaming_config.batch_size
        self._logger.info(f"Batch size is {self._batch_size}")

    def execute_request(self) -> None:
        if self._executor is None:
            raise RuntimeError('No horizontal scale executor has been set, cannot execute request.')

        self._executor.execute_workloads()

    def results(self) -> RequestExecutor:
        return self

    def __iter__(self) -> RequestExecutor:
        return self

    def __next__(self) -> Result:
        if self._executor.workload_complete and self._executor.results.empty():
            self._logger.info("All results have been sent")
            raise StopIteration

        batch: list[Result] = []
        start_time = datetime.now()
        while True:
            if len(batch) == self._batch_size:
                break

            if len(batch) > 0 and datetime.now() - start_time >= timedelta(milliseconds=10):
                break

            result = self._executor.results.get()

            if result:
                batch.append(result)
            elif result is None and self._executor.workload_complete is True:
                # None is executor done sentinel and workload_complete is set - we're done
                # If the current batch is not empty, break the loop to send it back to the client first
                if len(batch) > 0:
                    break
                raise StopIteration

        if len(batch) > 1:
            return Result(batched=BatchedResult(result=batch))
        return batch[0]

    @classmethod
    def build_request(cls, request: Request, executor: WorkloadExecutor) -> RequestExecutor:
        req = cls(request, executor)
        req.set_batch_size()
        return req
