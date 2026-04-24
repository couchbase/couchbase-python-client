#  Copyright 2016-2022. Couchbase, Inc.
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

from typing import (TYPE_CHECKING,
                    Iterable,
                    Iterator,
                    Union)

from acouchbase.logic.client_adapter import AsyncClientAdapter
from couchbase.exceptions import ErrorMapper, UnAmbiguousTimeoutException
from couchbase.logic.collection_req_builder import CollectionRequestBuilder
from couchbase.logic.collection_types import CollectionDetails
from couchbase.logic.observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.logic.pycbc_core import pycbc_connection
from couchbase.logic.pycbc_core import pycbc_exception as PycbcCoreException
from couchbase.result import (CounterResult,
                              ExistsResult,
                              GetReplicaResult,
                              GetResult,
                              LookupInReplicaResult,
                              LookupInResult,
                              MutateInResult,
                              MutationResult,
                              ScanResultIterable)

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop

    from acouchbase.kv_range_scan import AsyncRangeScanRequest
    from acouchbase.scope import AsyncScope
    from couchbase.logic.pycbc_core import pycbc_kv_request as PycbcCoreKeyValueRequest
    from couchbase.transcoder import Transcoder
    from txcouchbase.scope import TxScope


class AsyncCollectionImpl:

    def __init__(self, collection_name: str, scope: Union[AsyncScope, TxScope]) -> None:
        self._scope = scope
        self._client_adapter = scope._impl._client_adapter
        self._collection_details = CollectionDetails(self._scope._impl.bucket_name,
                                                     self._scope._impl.name,
                                                     collection_name,
                                                     self._scope._impl.cluster_settings.default_transcoder)
        self._request_builder = CollectionRequestBuilder(self._collection_details, self._client_adapter.loop)

    @property
    def bucket_name(self) -> str:
        """
            str: The name of the bucket for this :class:`~.Collection` instance.
        """
        return self._collection_details.bucket_name

    @property
    def client_adapter(self) -> AsyncClientAdapter:
        return self._client_adapter

    @property
    def connected(self) -> bool:
        return self._scope._impl._bucket_impl.connected

    @property
    def connection(self) -> pycbc_connection:
        """
        **INTERNAL**
        """
        return self._client_adapter.connection

    @property
    def loop(self) -> AbstractEventLoop:
        return self._client_adapter.loop

    @property
    def name(self) -> str:
        """
            str: The name of this :class:`~.Collection` instance.
        """
        return self._collection_details.collection_name

    @property
    def request_builder(self) -> CollectionRequestBuilder:
        return self._request_builder

    @property
    def scope_name(self) -> str:
        """
            str: The name of the scope for this :class:`~.Collection` instance.
        """
        return self._collection_details.scope_name

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        """
        **INTERNAL**
        """
        return self._scope._impl.cluster_settings.observability_instruments

    async def append(self, req: PycbcCoreKeyValueRequest, obs_handler: ObservableRequestHandler) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(ret, key=req.key)

    async def decrement(self, req: PycbcCoreKeyValueRequest, obs_handler: ObservableRequestHandler) -> CounterResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return CounterResult(ret, key=req.key)

    async def exists(self, req: PycbcCoreKeyValueRequest, obs_handler: ObservableRequestHandler) -> ExistsResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return ExistsResult(ret, key=req.key)

    async def get_all_replicas(self,
                               req: PycbcCoreKeyValueRequest,
                               transcoder: Transcoder,
                               obs_handler: ObservableRequestHandler) -> Iterable[GetReplicaResult]:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)

        def _decode_replicas() -> Iterator[GetReplicaResult]:
            while True:
                try:
                    res = next(ret)
                except StopIteration:
                    # this is a timeout from pulling a result from the queue, kill the generator
                    raise UnAmbiguousTimeoutException('Timeout reached waiting for result in queue.') from None
                else:
                    if isinstance(res, PycbcCoreException):
                        raise ErrorMapper.build_exception(res)
                    # should only be None once all replicas have been retrieved
                    if res is None:
                        return

                    yield GetReplicaResult(res, transcoder=transcoder, key=req.key)

        return _decode_replicas()

    async def get_and_lock(self,
                           req: PycbcCoreKeyValueRequest,
                           transcoder: Transcoder,
                           obs_handler: ObservableRequestHandler) -> GetResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return GetResult(ret, transcoder=transcoder, key=req.key)

    async def get_and_touch(self,
                            req: PycbcCoreKeyValueRequest,
                            transcoder: Transcoder,
                            obs_handler: ObservableRequestHandler) -> GetResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return GetResult(ret, transcoder=transcoder, key=req.key)

    async def get_any_replica(self,
                              req: PycbcCoreKeyValueRequest,
                              transcoder: Transcoder,
                              obs_handler: ObservableRequestHandler) -> GetReplicaResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return GetReplicaResult(ret, transcoder=transcoder, key=req.key)

    async def get(self,
                  req: PycbcCoreKeyValueRequest,
                  transcoder: Transcoder,
                  obs_handler: ObservableRequestHandler) -> GetResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return GetResult(ret, transcoder=transcoder, key=req.key)

    async def increment(self, req: PycbcCoreKeyValueRequest, obs_handler: ObservableRequestHandler) -> CounterResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return CounterResult(ret, key=req.key)

    async def insert(self, req: PycbcCoreKeyValueRequest, obs_handler: ObservableRequestHandler) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(ret, key=req.key)

    async def lookup_in(self,
                        req: PycbcCoreKeyValueRequest,
                        transcoder: Transcoder,
                        obs_handler: ObservableRequestHandler) -> LookupInResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return LookupInResult(ret, transcoder=transcoder, is_subdoc=True, key=req.key)

    async def lookup_in_all_replicas(self,
                                     req: PycbcCoreKeyValueRequest,
                                     transcoder: Transcoder,
                                     obs_handler: ObservableRequestHandler) -> Iterable[LookupInReplicaResult]:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)

        def _decode_replicas() -> Iterator[LookupInReplicaResult]:
            while True:
                try:
                    res = next(ret)
                except StopIteration:
                    # this is a timeout from pulling a result from the queue, kill the generator
                    raise UnAmbiguousTimeoutException('Timeout reached waiting for result in queue.') from None
                else:
                    if isinstance(res, PycbcCoreException):
                        raise ErrorMapper.build_exception(res)
                    # should only be None once all replicas have been retrieved
                    if res is None:
                        return

                    yield LookupInReplicaResult(res, transcoder=transcoder, is_subdoc=True, key=req.key)
        return _decode_replicas()

    async def lookup_in_any_replica(self,
                                    req: PycbcCoreKeyValueRequest,
                                    transcoder: Transcoder,
                                    obs_handler: ObservableRequestHandler) -> LookupInReplicaResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return LookupInReplicaResult(ret, transcoder=transcoder, is_subdoc=True, key=req.key)

    async def mutate_in(self, req: PycbcCoreKeyValueRequest, obs_handler: ObservableRequestHandler) -> MutateInResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutateInResult(ret, key=req.key)

    async def prepend(self, req: PycbcCoreKeyValueRequest, obs_handler: ObservableRequestHandler) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(ret, key=req.key)

    def range_scan(self, req: AsyncRangeScanRequest) -> ScanResultIterable:
        return ScanResultIterable(req)

    async def remove(self, req: PycbcCoreKeyValueRequest, obs_handler: ObservableRequestHandler) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(ret, key=req.key)

    async def replace(self, req: PycbcCoreKeyValueRequest, obs_handler: ObservableRequestHandler) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(ret, key=req.key)

    async def touch(self, req: PycbcCoreKeyValueRequest, obs_handler: ObservableRequestHandler) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(ret, key=req.key)

    async def unlock(self, req: PycbcCoreKeyValueRequest, obs_handler: ObservableRequestHandler) -> None:
        await self.wait_until_bucket_connected()
        await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)

    async def upsert(self, req: PycbcCoreKeyValueRequest, obs_handler: ObservableRequestHandler) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(ret, key=req.key)

    async def wait_until_bucket_connected(self) -> None:
        if self.connected:
            return
        await self._scope._impl.wait_until_bucket_connected()
