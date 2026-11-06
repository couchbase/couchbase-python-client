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

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from acouchbase.protocol.classic._client_adapter import AsyncClientAdapter
from acouchbase.protocol.classic._management._query_index_mgmt_impl import AsyncQueryIndexMgmtImpl
from acouchbase.protocol.classic._range_scan import ClassicAsyncRangeScanExecutor
from acouchbase.result import AsyncScanResultIterable
from couchbase.common._observability import (KeyspaceDetails,
                                             ObservabilityInstruments,
                                             ObservableRequestHandler)
from couchbase.common._operations import Op
from couchbase.common._streaming import AsyncRowIterator
from couchbase.common.result import (CounterResult,
                                     ExistsResult,
                                     GetReplicaResult,
                                     GetResult,
                                     LookupInReplicaResult,
                                     LookupInResult,
                                     MutateInResult,
                                     MutationResult)
from couchbase.protocol.classic._collection_impl import decode_replicas
from couchbase.protocol.classic._collection_req_builder import CollectionRequestBuilder
from couchbase.protocol.classic._collection_types import CollectionDetails
from couchbase.protocol.classic._pycbc_core import pycbc_connection
from couchbase.protocol.classic._result import (_counter_from_core,
                                                _exists_from_core,
                                                _get_from_core,
                                                _get_replica_from_core,
                                                _lookup_in_from_core,
                                                _lookup_in_replica_from_core,
                                                _mutate_in_from_core,
                                                _mutation_from_core)

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop
    from datetime import timedelta

    from acouchbase.protocol.classic._scope_impl import AsyncScopeImpl
    from couchbase.common._utils import DocumentValue
    from couchbase.common.kv_range_scan import ScanType
    from couchbase.common.options import (AppendOptions,
                                          DecrementOptions,
                                          ExistsOptions,
                                          GetAllReplicasOptions,
                                          GetAndLockOptions,
                                          GetAndTouchOptions,
                                          GetAnyReplicaOptions,
                                          GetOptions,
                                          IncrementOptions,
                                          InsertOptions,
                                          LookupInAllReplicasOptions,
                                          LookupInAnyReplicaOptions,
                                          LookupInOptions,
                                          MutateInOptions,
                                          PrependOptions,
                                          RemoveOptions,
                                          ReplaceOptions,
                                          ScanOptions,
                                          TouchOptions,
                                          UnlockOptions,
                                          UpsertOptions)
    from couchbase.common.protocol import CouchbaseProtocol
    from couchbase.common.subdocument import Spec
    from couchbase.protocol.classic._pycbc_core import pycbc_kv_request as PycbcCoreKeyValueRequest
    from couchbase.serializer import Serializer
    from couchbase.transcoder import Transcoder


class AsyncCollectionImpl:

    def __init__(self, collection_name: str, scope_impl: AsyncScopeImpl) -> None:
        self._scope_impl = scope_impl
        self._client_adapter: AsyncClientAdapter = scope_impl._client_adapter
        cluster_settings = scope_impl.cluster_settings
        self._collection_details: CollectionDetails = CollectionDetails(
            scope_impl.bucket_name,
            scope_impl.name,
            collection_name,
            cluster_settings.default_transcoder,
            default_serializer=cluster_settings.default_serializer,
            option_validation=cluster_settings.option_validation,
            kv_durable_timeout=scope_impl.kv_durable_timeout)
        self._request_builder = CollectionRequestBuilder(self._collection_details)

    @property
    def couchbase_protocol(self) -> CouchbaseProtocol:
        """**INTERNAL**"""
        return self._client_adapter.couchbase_protocol

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
        return self._scope_impl.connected

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
        return self._scope_impl.observability_instruments

    @property
    def keyspace(self) -> KeyspaceDetails:
        return self._collection_details.get_details_as_dict()

    async def get(self,
                  key: str,
                  opts: GetOptions | None = None,
                  **kwargs: Any) -> GetResult:
        async with ObservableRequestHandler.create(Op.Get, self.observability_instruments) as obs_handler:
            req, transcoder = self._request_builder.build_get_request(key, obs_handler, opts, **kwargs)
            return await self._get(req, transcoder, obs_handler)

    async def get_any_replica(self,
                              key: str,
                              opts: GetAnyReplicaOptions | None = None,
                              **kwargs: Any) -> GetReplicaResult:
        async with ObservableRequestHandler.create(Op.GetAnyReplica, self.observability_instruments) as obs_handler:
            req, transcoder = self._request_builder.build_get_any_replica_request(key, obs_handler, opts, **kwargs)
            return await self._get_any_replica(req, transcoder, obs_handler)

    async def get_all_replicas(self,
                               key: str,
                               opts: GetAllReplicasOptions | None = None,
                               **kwargs: Any) -> Iterable[GetReplicaResult]:
        async with ObservableRequestHandler.create(Op.GetAllReplicas, self.observability_instruments) as obs_handler:
            req, transcoder = self._request_builder.build_get_all_replicas_request(key, obs_handler, opts, **kwargs)
            return await self._get_all_replicas(req, transcoder, obs_handler)

    async def exists(self,
                     key: str,
                     opts: ExistsOptions | None = None,
                     **kwargs: Any) -> ExistsResult:
        async with ObservableRequestHandler.create(Op.Exists, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_exists_request(key, obs_handler, opts, **kwargs)
            return await self._exists(req, obs_handler)

    async def insert(self,
                     key: str,
                     value: DocumentValue,
                     opts: InsertOptions | None = None,
                     **kwargs: Any) -> MutationResult:
        async with ObservableRequestHandler.create(Op.Insert, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_insert_request(key, value, obs_handler, opts, **kwargs)
            return await self._insert(req, obs_handler)

    async def upsert(self,
                     key: str,
                     value: DocumentValue,
                     opts: UpsertOptions | None = None,
                     **kwargs: Any) -> MutationResult:
        async with ObservableRequestHandler.create(Op.Upsert, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_upsert_request(key, value, obs_handler, opts, **kwargs)
            return await self._upsert(req, obs_handler)

    async def replace(self,
                      key: str,
                      value: DocumentValue,
                      opts: ReplaceOptions | None = None,
                      **kwargs: Any) -> MutationResult:
        async with ObservableRequestHandler.create(Op.Replace, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_replace_request(key, value, obs_handler, opts, **kwargs)
            return await self._replace(req, obs_handler)

    async def remove(self,
                     key: str,
                     opts: RemoveOptions | None = None,
                     **kwargs: Any) -> MutationResult:
        async with ObservableRequestHandler.create(Op.Remove, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_remove_request(key, obs_handler, opts, **kwargs)
            return await self._remove(req, obs_handler)

    async def touch(self,
                    key: str,
                    expiry: timedelta,
                    opts: TouchOptions | None = None,
                    **kwargs: Any) -> MutationResult:
        async with ObservableRequestHandler.create(Op.Touch, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_touch_request(key, expiry, obs_handler, opts, **kwargs)
            return await self._touch(req, obs_handler)

    async def get_and_touch(self,
                            key: str,
                            expiry: timedelta,
                            opts: GetAndTouchOptions | None = None,
                            **kwargs: Any) -> GetResult:
        async with ObservableRequestHandler.create(Op.GetAndTouch, self.observability_instruments) as obs_handler:
            req, transcoder = self._request_builder.build_get_and_touch_request(key,
                                                                                expiry,
                                                                                obs_handler,
                                                                                opts,
                                                                                **kwargs)
            return await self._get_and_touch(req, transcoder, obs_handler)

    async def get_and_lock(self,
                           key: str,
                           lock_time: timedelta,
                           opts: GetAndLockOptions | None = None,
                           **kwargs: Any) -> GetResult:
        async with ObservableRequestHandler.create(Op.GetAndLock, self.observability_instruments) as obs_handler:
            req, transcoder = self._request_builder.build_get_and_lock_request(key,
                                                                               lock_time,
                                                                               obs_handler,
                                                                               opts,
                                                                               **kwargs)
            return await self._get_and_lock(req, transcoder, obs_handler)

    async def unlock(self,
                     key: str,
                     cas: int,
                     opts: UnlockOptions | None = None,
                     **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.Unlock, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_unlock_request(key, cas, obs_handler, opts, **kwargs)
            await self._unlock(req, obs_handler)

    async def lookup_in(self,
                        key: str,
                        spec: Iterable[Spec],
                        opts: LookupInOptions | None = None,
                        **kwargs: Any) -> LookupInResult:
        async with ObservableRequestHandler.create(Op.LookupIn, self.observability_instruments) as obs_handler:
            req, serializer = self._request_builder.build_lookup_in_request(key, spec, obs_handler, opts, **kwargs)
            return await self._lookup_in(req, serializer, obs_handler)

    async def lookup_in_any_replica(self,
                                    key: str,
                                    spec: Iterable[Spec],
                                    opts: LookupInAnyReplicaOptions | None = None,
                                    **kwargs: Any) -> LookupInReplicaResult:
        async with ObservableRequestHandler.create(Op.LookupInAnyReplica,
                                                   self.observability_instruments) as obs_handler:
            req, serializer = self._request_builder.build_lookup_in_any_replica_request(key,
                                                                                        spec,
                                                                                        obs_handler,
                                                                                        opts,
                                                                                        **kwargs)
            return await self._lookup_in_any_replica(req, serializer, obs_handler)

    async def lookup_in_all_replicas(self,
                                     key: str,
                                     spec: Iterable[Spec],
                                     opts: LookupInAllReplicasOptions | None = None,
                                     **kwargs: Any) -> Iterable[LookupInReplicaResult]:
        async with ObservableRequestHandler.create(Op.LookupInAllReplicas,
                                                   self.observability_instruments) as obs_handler:
            req, serializer = self._request_builder.build_lookup_in_all_replicas_request(key,
                                                                                         spec,
                                                                                         obs_handler,
                                                                                         opts,
                                                                                         **kwargs)
            return await self._lookup_in_all_replicas(req, serializer, obs_handler)

    async def mutate_in(self,
                        key: str,
                        spec: Iterable[Spec],
                        opts: MutateInOptions | None = None,
                        **kwargs: Any) -> MutateInResult:
        async with ObservableRequestHandler.create(Op.MutateIn, self.observability_instruments) as obs_handler:
            req, serializer = self._request_builder.build_mutate_in_request(key, spec, obs_handler, opts, **kwargs)
            return await self._mutate_in(req, serializer, obs_handler)

    async def increment(self,
                        key: str,
                        opts: IncrementOptions | None = None,
                        **kwargs: Any) -> CounterResult:
        async with ObservableRequestHandler.create(Op.Increment, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_increment_request(key, obs_handler, opts, **kwargs)
            return await self._increment(req, obs_handler)

    async def decrement(self,
                        key: str,
                        opts: DecrementOptions | None = None,
                        **kwargs: Any) -> CounterResult:
        async with ObservableRequestHandler.create(Op.Decrement, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_decrement_request(key, obs_handler, opts, **kwargs)
            return await self._decrement(req, obs_handler)

    async def append(self,
                     key: str,
                     value: str | bytes | bytearray,
                     opts: AppendOptions | None = None,
                     **kwargs: Any) -> MutationResult:
        async with ObservableRequestHandler.create(Op.Append, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_append_request(key, value, obs_handler, opts, **kwargs)
            return await self._append(req, obs_handler)

    async def prepend(self,
                      key: str,
                      value: str | bytes | bytearray,
                      opts: PrependOptions | None = None,
                      **kwargs: Any) -> MutationResult:
        async with ObservableRequestHandler.create(Op.Prepend, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_prepend_request(key, value, obs_handler, opts, **kwargs)
            return await self._prepend(req, obs_handler)

    def scan(self,
             scan_type: ScanType,
             opts: ScanOptions | None = None,
             **kwargs: Any) -> AsyncScanResultIterable:
        scan_args = self._request_builder.build_range_scan_args(scan_type, opts, **kwargs)
        executor = ClassicAsyncRangeScanExecutor(self.connection, scan_args, self.loop)
        return AsyncScanResultIterable(AsyncRowIterator(executor), executor)

    def binary(self) -> AsyncCollectionImpl:
        """**INTERNAL** The binary operations are this impl's own."""
        return self

    def query_index_mgmt(self) -> AsyncQueryIndexMgmtImpl:
        """**INTERNAL**"""
        return AsyncQueryIndexMgmtImpl(self._client_adapter,
                                       self.observability_instruments,
                                       collection_context=(self.name, self.scope_name))

    # The execute half of each operation.  The txcouchbase impl builds the request itself, inside
    # the `start()` it hands to `defer()`, and returns one of these as the coroutine to run.
    async def _append(self,
                      req: PycbcCoreKeyValueRequest,
                      obs_handler: ObservableRequestHandler | None) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(_mutation_from_core(ret.raw_result), key=req.key)

    async def _decrement(self,
                         req: PycbcCoreKeyValueRequest,
                         obs_handler: ObservableRequestHandler | None) -> CounterResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return CounterResult(_counter_from_core(ret.raw_result), key=req.key)

    async def _exists(self,
                      req: PycbcCoreKeyValueRequest,
                      obs_handler: ObservableRequestHandler | None) -> ExistsResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return ExistsResult(_exists_from_core(ret.raw_result), key=req.key)

    async def _get_all_replicas(self,
                                req: PycbcCoreKeyValueRequest,
                                transcoder: Transcoder,
                                obs_handler: ObservableRequestHandler | None) -> Iterable[GetReplicaResult]:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return decode_replicas(ret, lambda res: GetReplicaResult(_get_replica_from_core(res.raw_result),
                                                                 transcoder=transcoder,
                                                                 key=req.key))

    async def _get_and_lock(self,
                            req: PycbcCoreKeyValueRequest,
                            transcoder: Transcoder,
                            obs_handler: ObservableRequestHandler | None) -> GetResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return GetResult(_get_from_core(ret.raw_result), transcoder=transcoder, key=req.key)

    async def _get_and_touch(self,
                             req: PycbcCoreKeyValueRequest,
                             transcoder: Transcoder,
                             obs_handler: ObservableRequestHandler | None) -> GetResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return GetResult(_get_from_core(ret.raw_result), transcoder=transcoder, key=req.key)

    async def _get_any_replica(self,
                               req: PycbcCoreKeyValueRequest,
                               transcoder: Transcoder,
                               obs_handler: ObservableRequestHandler | None) -> GetReplicaResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return GetReplicaResult(_get_replica_from_core(ret.raw_result), transcoder=transcoder, key=req.key)

    async def _get(self,
                   req: PycbcCoreKeyValueRequest,
                   transcoder: Transcoder,
                   obs_handler: ObservableRequestHandler | None) -> GetResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return GetResult(_get_from_core(ret.raw_result), transcoder=transcoder, key=req.key)

    async def _increment(self,
                         req: PycbcCoreKeyValueRequest,
                         obs_handler: ObservableRequestHandler | None) -> CounterResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return CounterResult(_counter_from_core(ret.raw_result), key=req.key)

    async def _insert(self,
                      req: PycbcCoreKeyValueRequest,
                      obs_handler: ObservableRequestHandler | None) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(_mutation_from_core(ret.raw_result), key=req.key)

    async def _lookup_in(self,
                         req: PycbcCoreKeyValueRequest,
                         serializer: Serializer,
                         obs_handler: ObservableRequestHandler | None) -> LookupInResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return LookupInResult(_lookup_in_from_core(ret.raw_result, req.key), serializer=serializer, key=req.key)

    async def _lookup_in_all_replicas(self,
                                      req: PycbcCoreKeyValueRequest,
                                      serializer: Serializer,
                                      obs_handler: ObservableRequestHandler | None) -> Iterable[LookupInReplicaResult]:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return decode_replicas(ret, lambda res: LookupInReplicaResult(
            _lookup_in_replica_from_core(res.raw_result, req.key), serializer=serializer, key=req.key))

    async def _lookup_in_any_replica(self,
                                     req: PycbcCoreKeyValueRequest,
                                     serializer: Serializer,
                                     obs_handler: ObservableRequestHandler | None) -> LookupInReplicaResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return LookupInReplicaResult(_lookup_in_replica_from_core(ret.raw_result, req.key),
                                     serializer=serializer,
                                     key=req.key)

    async def _mutate_in(self,
                         req: PycbcCoreKeyValueRequest,
                         serializer: Serializer,
                         obs_handler: ObservableRequestHandler | None) -> MutateInResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutateInResult(_mutate_in_from_core(ret.raw_result, req.key), serializer=serializer, key=req.key)

    async def _prepend(self,
                       req: PycbcCoreKeyValueRequest,
                       obs_handler: ObservableRequestHandler | None) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(_mutation_from_core(ret.raw_result), key=req.key)

    async def _remove(self,
                      req: PycbcCoreKeyValueRequest,
                      obs_handler: ObservableRequestHandler | None) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(_mutation_from_core(ret.raw_result), key=req.key)

    async def _replace(self,
                       req: PycbcCoreKeyValueRequest,
                       obs_handler: ObservableRequestHandler | None) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(_mutation_from_core(ret.raw_result), key=req.key)

    async def _touch(self,
                     req: PycbcCoreKeyValueRequest,
                     obs_handler: ObservableRequestHandler | None) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(_mutation_from_core(ret.raw_result), key=req.key)

    async def _unlock(self, req: PycbcCoreKeyValueRequest, obs_handler: ObservableRequestHandler | None) -> None:
        await self.wait_until_bucket_connected()
        await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)

    async def _upsert(self,
                      req: PycbcCoreKeyValueRequest,
                      obs_handler: ObservableRequestHandler | None) -> MutationResult:
        await self.wait_until_bucket_connected()
        ret = await self.client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(_mutation_from_core(ret.raw_result), key=req.key)

    async def wait_until_bucket_connected(self) -> None:
        if self.connected:
            return
        await self._scope_impl.wait_until_bucket_connected()
