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

from .multi_process_executor import MultiProcessExecutor
from .multi_thread_executor import MultiThreadExecutor

if TYPE_CHECKING:
    from run import top_level_pb2 as run

    from ..streaming import StreamOwner
    from ..telemetry.span_owner import SpanOwner
    from ..utils import ConnectionCache
    from ..workloads import Counters


class WorkloadExecutorBuilder:

    @staticmethod
    def build_executor(run_request: run.Request,
                       run_id: str,
                       cached_conn: ConnectionCache,
                       stream_owner: StreamOwner,
                       counters: Counters,
                       span_owner: SpanOwner | None = None,
                       ) -> MultiProcessExecutor | MultiThreadExecutor:
        """ Factory function, builds and returns the appropriate workload executor instance.

        Args:
            run_request (run.Request): The run request received from the driver.
            run_id (str): The ID of the run that will own this executor.
            cached_conn (ConnectionCache): The cached connection object for the cluster used for the executor.
            stream_owner (StreamOwner): The StreamOwner object that will manage any streams created by the executor.
            counters (Counters): The performer-level counter registry used for counter bounds.

        Returns:
            Union[MultiProcessExecutor, MultiThreadExecutor]: The workload executor object

        Raises:
            NotImplementedError: If an unsupported API is requested
        """
        logger = logging.getLogger(__name__)
        mechanism = run_request.tunables.get("concurrencyMechanism", "multithreading")
        if mechanism == "multithreading":
            logger.info('Using multi-thread executor')
            if cached_conn.cluster is None:
                raise RuntimeError('The cached connection has no cluster, cannot build a multi-thread executor.')
            return MultiThreadExecutor.build_executor(
                run_id,
                run_request,
                cached_conn.cluster,
                counters,
                stream_owner,
                span_owner=span_owner)
        elif mechanism == "multiprocessing":
            logger.info('Using multi-process executor')
            return MultiProcessExecutor.build_executor(
                run_id,
                run_request,
                cached_conn.hostname,
                cached_conn.mp_cluster_options,
                counters,
                obs_config=cached_conn.obs_config,
                span_owner=span_owner
            )
        else:
            raise NotImplementedError(f"Concurrency mechanism {str(mechanism)} not supported")
