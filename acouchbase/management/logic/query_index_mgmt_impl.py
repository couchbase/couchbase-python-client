#  Copyright 2016-2023. Couchbase, Inc.
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

import asyncio
import time
from typing import (TYPE_CHECKING,
                    Iterable,
                    List)

from couchbase.exceptions import (AmbiguousTimeoutException,
                                  QueryIndexNotFoundException,
                                  WatchQueryIndexTimeoutException)
from couchbase.logic.observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.logic.operation_types import QueryIndexMgmtOperationType
from couchbase.management.logic.query_index_mgmt_req_builder import QueryIndexMgmtRequestBuilder
from couchbase.management.logic.query_index_mgmt_req_types import (BuildDeferredIndexesRequest,
                                                                   CreateIndexRequest,
                                                                   DropIndexRequest,
                                                                   GetAllIndexesRequest,
                                                                   QueryIndex,
                                                                   WatchIndexesRequest)

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter


class AsyncQueryIndexMgmtImpl:
    def __init__(self,
                 client_adapter: AsyncClientAdapter,
                 observability_instruments: ObservabilityInstruments) -> None:
        self._client_adapter = client_adapter
        self._request_builder = QueryIndexMgmtRequestBuilder()
        self._observability_instruments = observability_instruments

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """**INTERNAL**"""
        return self._client_adapter.loop

    @property
    def request_builder(self) -> QueryIndexMgmtRequestBuilder:
        """**INTERNAL**"""
        return self._request_builder

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        """**INTERNAL**"""
        return self._observability_instruments

    async def create_index(self, req: CreateIndexRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def create_primary_index(self, req: CreateIndexRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def drop_index(self, req: DropIndexRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def drop_primary_index(self, req: DropIndexRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def get_all_indexes(self,
                              req: GetAllIndexesRequest,
                              obs_handler: ObservableRequestHandler) -> List[QueryIndex]:
        """**INTERNAL**"""
        res = await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_indexes = res.raw_result['indexes']
        return [QueryIndex.from_server(idx) for idx in raw_indexes]

    async def build_deferred_indexes(self,
                                     req: BuildDeferredIndexesRequest,
                                     obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def watch_indexes(self, req: WatchIndexesRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        current_time = time.monotonic()
        # timeout is converted to millisecs via options processing
        timeout = req.timeout / 1000
        deadline = current_time + timeout
        delay = 0.1  # seconds
        # needs to be int b/c req.timeout expects int (this is what the bindings want)
        delay_ms = int(delay * 1e3)

        op_type = QueryIndexMgmtOperationType.QueryIndexGetAll
        get_all_indexes_req = GetAllIndexesRequest(self._request_builder._error_map,
                                                   bucket_name=req.bucket_name,
                                                   scope_name=req.scope_name,
                                                   collection_name=req.collection_name,
                                                   timeout=req.timeout)

        while True:
            async with ObservableRequestHandler(op_type, self._observability_instruments) as sub_obs_handler:
                sub_obs_handler.create_http_span(parent_span=obs_handler.wrapped_span)
                try:
                    indexes = await self.get_all_indexes(get_all_indexes_req, sub_obs_handler)
                except AmbiguousTimeoutException:
                    pass  # go ahead and move on, raise WatchQueryIndexTimeoutException later if needed

                all_online = self._check_indexes(req.index_names, indexes)
                if all_online:
                    break

                current_time = time.monotonic()
                if deadline < (current_time + delay):
                    msg = 'Failed to find all indexes online within the alloted time.'
                    raise WatchQueryIndexTimeoutException(msg)
                await asyncio.sleep(delay)
                get_all_indexes_req.timeout -= delay_ms

    def _check_indexes(self, index_names: Iterable[str], indexes: Iterable[QueryIndex]):
        for idx_name in index_names:
            match = next((i for i in indexes if i.name == idx_name), None)
            if not match:
                raise QueryIndexNotFoundException(f'Cannot find index with name: {idx_name}')

        return all(map(lambda i: i.state == 'online', indexes))
