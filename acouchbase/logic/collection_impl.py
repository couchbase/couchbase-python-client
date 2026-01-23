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
                    Optional,
                    Union)

from acouchbase.logic.client_adapter import AsyncClientAdapter
from couchbase.exceptions import ErrorMapper, UnAmbiguousTimeoutException
from couchbase.exceptions import exception as CouchbaseBaseException
from couchbase.logic.collection_req_builder import CollectionRequestBuilder
from couchbase.logic.collection_types import CollectionDetails
from couchbase.logic.top_level_types import PyCapsuleType
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
    from couchbase.logic.collection_types import (AppendRequest,
                                                  DecrementRequest,
                                                  ExistsRequest,
                                                  GetAllReplicasRequest,
                                                  GetAndLockRequest,
                                                  GetAndTouchRequest,
                                                  GetAnyReplicaRequest,
                                                  GetRequest,
                                                  IncrementRequest,
                                                  InsertRequest,
                                                  LookupInAllReplicasRequest,
                                                  LookupInAnyReplicaRequest,
                                                  LookupInRequest,
                                                  MutateInRequest,
                                                  PrependRequest,
                                                  RemoveRequest,
                                                  ReplaceRequest,
                                                  TouchRequest,
                                                  UnlockRequest,
                                                  UpsertRequest)
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
    def connection(self) -> Optional[PyCapsuleType]:
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

    async def append(self, req: AppendRequest) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req)
        return MutationResult(ret)

    async def decrement(self, req: DecrementRequest) -> CounterResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req)
        return CounterResult(ret)

    async def exists(self, req: ExistsRequest) -> ExistsResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req)
        return ExistsResult(ret)

    async def get_all_replicas(self, req: GetAllReplicasRequest) -> Iterable[GetReplicaResult]:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req)

        def _decode_replicas() -> Iterator[GetReplicaResult]:
            while True:
                try:
                    res = next(ret)
                except StopIteration:
                    # this is a timeout from pulling a result from the queue, kill the generator
                    raise UnAmbiguousTimeoutException('Timeout reached waiting for result in queue.') from None
                else:
                    if isinstance(res, CouchbaseBaseException):
                        raise ErrorMapper.build_exception(res)
                    # should only be None once all replicas have been retrieved
                    if res is None:
                        return

                    yield GetReplicaResult(res, transcoder=req.transcoder)

        return _decode_replicas()

    async def get_and_lock(self, req: GetAndLockRequest) -> GetResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req)
        return GetResult(ret, transcoder=req.transcoder)

    async def get_and_touch(self, req: GetAndTouchRequest) -> GetResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req)
        return GetResult(ret, transcoder=req.transcoder)

    async def get_any_replica(self, req: GetAnyReplicaRequest) -> GetReplicaResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req)
        return GetReplicaResult(ret, transcoder=req.transcoder)

    async def get(self, req: GetRequest) -> GetResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req)
        return GetResult(ret, transcoder=req.transcoder)

    async def increment(self, req: IncrementRequest) -> CounterResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req)
        return CounterResult(ret)

    async def insert(self, req: InsertRequest) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req)
        return MutationResult(ret)

    async def lookup_in(self, req: LookupInRequest) -> LookupInResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req)
        return LookupInResult(ret, transcoder=req.transcoder, is_subdoc=True)

    async def lookup_in_all_replicas(self, req: LookupInAllReplicasRequest) -> Iterable[LookupInReplicaResult]:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req)

        def _decode_replicas() -> Iterator[LookupInReplicaResult]:
            while True:
                try:
                    res = next(ret)
                except StopIteration:
                    # this is a timeout from pulling a result from the queue, kill the generator
                    raise UnAmbiguousTimeoutException('Timeout reached waiting for result in queue.') from None
                else:
                    if isinstance(res, CouchbaseBaseException):
                        raise ErrorMapper.build_exception(res)
                    # should only be None once all replicas have been retrieved
                    if res is None:
                        return

                    yield LookupInReplicaResult(res, transcoder=req.transcoder, is_subdoc=True)
        return _decode_replicas()

    async def lookup_in_any_replica(self, req: LookupInAnyReplicaRequest) -> LookupInReplicaResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req)
        return LookupInReplicaResult(ret, transcoder=req.transcoder, is_subdoc=True)

    async def mutate_in(self, req: MutateInRequest) -> MutateInResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req)
        return MutateInResult(ret)

    async def prepend(self, req: PrependRequest) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req)
        return MutationResult(ret)

    def range_scan(self, req: AsyncRangeScanRequest) -> ScanResultIterable:
        req.set_connection(self.client_adapter.connection)
        return ScanResultIterable(req)

    async def remove(self, req: RemoveRequest) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req)
        return MutationResult(ret)

    async def replace(self, req: ReplaceRequest) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req)
        return MutationResult(ret)

    async def touch(self, req: TouchRequest) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req)
        return MutationResult(ret)

    async def unlock(self, req: UnlockRequest) -> None:
        await self.wait_until_bucket_connected()
        await self.client_adapter.execute_collection_request(req)

    async def upsert(self, req: UpsertRequest) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req)
        return MutationResult(ret)

    async def wait_until_bucket_connected(self) -> None:
        if self.connected:
            return
        await self._scope._impl.wait_until_bucket_connected()
