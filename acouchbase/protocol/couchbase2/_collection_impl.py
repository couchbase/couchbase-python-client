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

"""An asyncio collection over the gateway, which is also its binary collection.

Each operation is prepared, then sent, so Twisted can prepare at the call and send in a coroutine.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from acouchbase.protocol.couchbase2._management._query_index_mgmt_impl import AsyncQueryIndexMgmtImpl
from couchbase.common._observability import (KeyspaceDetails,
                                             ObservabilityInstruments,
                                             ObservableRequestHandler)
from couchbase.common._operations import Op
from couchbase.common.result import (CounterResult,
                                     ExistsResult,
                                     GetResult,
                                     LookupInResult,
                                     MutateInResult,
                                     MutationResult)
from couchbase.protocol.couchbase2._collection_req_builder import CollectionRequestBuilder
from couchbase.protocol.couchbase2._collection_types import (GetRequest,
                                                             KvRequest,
                                                             LookupInRequest,
                                                             MutateInRequest,
                                                             ReadRequest,
                                                             counter_record,
                                                             exists_record,
                                                             get_record,
                                                             lookup_in_record,
                                                             mutate_in_record,
                                                             mutation_record)
from couchbase.protocol.couchbase2._core import unsupported
from couchbase.protocol.couchbase2._driver import call_unary_async

if TYPE_CHECKING:
    from datetime import timedelta

    from acouchbase.protocol.couchbase2._cluster_impl import AsyncClusterImpl
    from acouchbase.protocol.couchbase2._scope_impl import AsyncScopeImpl
    from acouchbase.result import AsyncScanResultIterable
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
                                          GetReplicaOptions,
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
    from couchbase.common.replica_reads import GetReplicaStrategy
    from couchbase.common.result import GetReplicaResult, LookupInReplicaResult
    from couchbase.common.subdocument import Spec
    from couchbase.protocol.couchbase2._generated.kv.v1 import kv_pb2


class AsyncCollectionImpl:

    def __init__(self, collection_name: str, scope_impl: AsyncScopeImpl) -> None:
        self._keyspace: KeyspaceDetails = {'bucket_name': scope_impl.bucket_name,
                                           'scope_name': scope_impl.name,
                                           'collection_name': collection_name}
        self._cluster_impl: AsyncClusterImpl = scope_impl.cluster_impl
        self._request_builder = CollectionRequestBuilder(self._cluster_impl.core, self._keyspace)

    @property
    def name(self) -> str:
        return self._keyspace['collection_name']

    @property
    def bucket_name(self) -> str:
        return self._keyspace['bucket_name']

    @property
    def scope_name(self) -> str:
        return self._keyspace['scope_name']

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        return self._cluster_impl.observability_instruments

    @property
    def keyspace(self) -> KeyspaceDetails:
        return KeyspaceDetails(**self._keyspace)

    async def get(self, key: str, opts: GetOptions | None = None, **kwargs: Any) -> GetResult:
        async with ObservableRequestHandler.create(Op.Get, self.observability_instruments) as obs_handler:
            return await self.send_get(self.prepare_get(key, obs_handler, opts, kwargs), key)

    def prepare_get(self,
                    key: str,
                    obs_handler: ObservableRequestHandler | None,
                    opts: GetOptions | None,
                    kwargs: dict[str, Any]) -> GetRequest:
        """**INTERNAL**"""
        self._cluster_impl.core.ensure_open()
        return self._request_builder.build_get_request(key, obs_handler, opts, **kwargs)

    async def send_get(self, req: GetRequest, key: str) -> GetResult:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        kv = await self._cluster_impl.kv()
        ctx = core.context(Op.Get, req.timeout, obs_handler=req.obs_handler)
        response = await call_unary_async(kv.Get, req.message, ctx, core.metadata)
        record = get_record(response, req.with_expiry)
        return GetResult(record, transcoder=req.transcoder, key=key)

    async def upsert(self, key: str, value: DocumentValue, opts: UpsertOptions | None = None,
                     **kwargs: Any) -> MutationResult:
        async with ObservableRequestHandler.create(Op.Upsert, self.observability_instruments) as obs_handler:
            return await self.send_upsert(self.prepare_upsert(key, value, obs_handler, opts, kwargs), key)

    def prepare_upsert(self,
                       key: str,
                       value: DocumentValue,
                       obs_handler: ObservableRequestHandler | None,
                       opts: UpsertOptions | None,
                       kwargs: dict[str, Any]) -> KvRequest[kv_pb2.UpsertRequest]:
        """**INTERNAL**"""
        self._cluster_impl.core.ensure_open()
        return self._request_builder.build_upsert_request(key, value, obs_handler, opts, **kwargs)

    async def send_upsert(self, req: KvRequest[kv_pb2.UpsertRequest], key: str) -> MutationResult:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        kv = await self._cluster_impl.kv()
        ctx = core.context(Op.Upsert, req.timeout, obs_handler=req.obs_handler)
        response = await call_unary_async(kv.Upsert, req.message, ctx, core.metadata)
        return MutationResult(mutation_record(response), key=key)

    async def get_any_replica(self,
                              key: str,
                              opts: GetAnyReplicaOptions | None = None,
                              **kwargs: Any) -> GetReplicaResult:
        raise unsupported('get_any_replica')

    async def get_replica(self,
                          key: str,
                          strategy: GetReplicaStrategy,
                          opts: GetReplicaOptions | None = None,
                          **kwargs: Any) -> GetReplicaResult:
        raise unsupported('get_replica')

    async def get_all_replicas(self,
                               key: str,
                               opts: GetAllReplicasOptions | None = None,
                               **kwargs: Any) -> Iterable[GetReplicaResult]:
        raise unsupported('get_all_replicas')

    async def exists(self,
                     key: str,
                     opts: ExistsOptions | None = None,
                     **kwargs: Any) -> ExistsResult:
        async with ObservableRequestHandler.create(Op.Exists, self.observability_instruments) as obs_handler:
            return await self.send_exists(self.prepare_exists(key, obs_handler, opts, kwargs), key)

    def prepare_exists(self,
                       key: str,
                       obs_handler: ObservableRequestHandler | None,
                       opts: ExistsOptions | None,
                       kwargs: dict[str, Any]) -> KvRequest[kv_pb2.ExistsRequest]:
        """**INTERNAL**"""
        self._cluster_impl.core.ensure_open()
        return self._request_builder.build_exists_request(key, obs_handler, opts, **kwargs)

    async def send_exists(self, req: KvRequest[kv_pb2.ExistsRequest], key: str) -> ExistsResult:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        kv = await self._cluster_impl.kv()
        ctx = core.context(Op.Exists, req.timeout, obs_handler=req.obs_handler)
        response = await call_unary_async(kv.Exists, req.message, ctx, core.metadata)
        return ExistsResult(exists_record(response), key=key)

    async def insert(self,
                     key: str,
                     value: DocumentValue,
                     opts: InsertOptions | None = None,
                     **kwargs: Any) -> MutationResult:
        async with ObservableRequestHandler.create(Op.Insert, self.observability_instruments) as obs_handler:
            return await self.send_insert(self.prepare_insert(key, value, obs_handler, opts, kwargs), key)

    def prepare_insert(self,
                       key: str,
                       value: DocumentValue,
                       obs_handler: ObservableRequestHandler | None,
                       opts: InsertOptions | None,
                       kwargs: dict[str, Any]) -> KvRequest[kv_pb2.InsertRequest]:
        """**INTERNAL**"""
        self._cluster_impl.core.ensure_open()
        return self._request_builder.build_insert_request(key, value, obs_handler, opts, **kwargs)

    async def send_insert(self, req: KvRequest[kv_pb2.InsertRequest], key: str) -> MutationResult:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        kv = await self._cluster_impl.kv()
        ctx = core.context(Op.Insert, req.timeout, obs_handler=req.obs_handler)
        response = await call_unary_async(kv.Insert, req.message, ctx, core.metadata)
        return MutationResult(mutation_record(response), key=key)

    async def replace(self,
                      key: str,
                      value: DocumentValue,
                      opts: ReplaceOptions | None = None,
                      **kwargs: Any) -> MutationResult:
        async with ObservableRequestHandler.create(Op.Replace, self.observability_instruments) as obs_handler:
            return await self.send_replace(self.prepare_replace(key, value, obs_handler, opts, kwargs), key)

    def prepare_replace(self,
                        key: str,
                        value: DocumentValue,
                        obs_handler: ObservableRequestHandler | None,
                        opts: ReplaceOptions | None,
                        kwargs: dict[str, Any]) -> KvRequest[kv_pb2.ReplaceRequest]:
        """**INTERNAL**"""
        self._cluster_impl.core.ensure_open()
        return self._request_builder.build_replace_request(key, value, obs_handler, opts, **kwargs)

    async def send_replace(self, req: KvRequest[kv_pb2.ReplaceRequest], key: str) -> MutationResult:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        kv = await self._cluster_impl.kv()
        ctx = core.context(Op.Replace, req.timeout, obs_handler=req.obs_handler)
        response = await call_unary_async(kv.Replace, req.message, ctx, core.metadata)
        return MutationResult(mutation_record(response), key=key)

    async def remove(self,
                     key: str,
                     opts: RemoveOptions | None = None,
                     **kwargs: Any) -> MutationResult:
        async with ObservableRequestHandler.create(Op.Remove, self.observability_instruments) as obs_handler:
            return await self.send_remove(self.prepare_remove(key, obs_handler, opts, kwargs), key)

    def prepare_remove(self,
                       key: str,
                       obs_handler: ObservableRequestHandler | None,
                       opts: RemoveOptions | None,
                       kwargs: dict[str, Any]) -> KvRequest[kv_pb2.RemoveRequest]:
        """**INTERNAL**"""
        self._cluster_impl.core.ensure_open()
        return self._request_builder.build_remove_request(key, obs_handler, opts, **kwargs)

    async def send_remove(self, req: KvRequest[kv_pb2.RemoveRequest], key: str) -> MutationResult:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        kv = await self._cluster_impl.kv()
        ctx = core.context(Op.Remove, req.timeout, obs_handler=req.obs_handler)
        response = await call_unary_async(kv.Remove, req.message, ctx, core.metadata)
        return MutationResult(mutation_record(response), key=key)

    async def touch(self,
                    key: str,
                    expiry: timedelta,
                    opts: TouchOptions | None = None,
                    **kwargs: Any) -> MutationResult:
        async with ObservableRequestHandler.create(Op.Touch, self.observability_instruments) as obs_handler:
            return await self.send_touch(self.prepare_touch(key, expiry, obs_handler, opts, kwargs), key)

    def prepare_touch(self,
                      key: str,
                      expiry: timedelta,
                      obs_handler: ObservableRequestHandler | None,
                      opts: TouchOptions | None,
                      kwargs: dict[str, Any]) -> KvRequest[kv_pb2.TouchRequest]:
        """**INTERNAL**"""
        self._cluster_impl.core.ensure_open()
        return self._request_builder.build_touch_request(key, expiry, obs_handler, opts, **kwargs)

    async def send_touch(self, req: KvRequest[kv_pb2.TouchRequest], key: str) -> MutationResult:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        kv = await self._cluster_impl.kv()
        ctx = core.context(Op.Touch, req.timeout, obs_handler=req.obs_handler)
        response = await call_unary_async(kv.Touch, req.message, ctx, core.metadata)
        return MutationResult(mutation_record(response), key=key)

    async def get_and_touch(self,
                            key: str,
                            expiry: timedelta,
                            opts: GetAndTouchOptions | None = None,
                            **kwargs: Any) -> GetResult:
        async with ObservableRequestHandler.create(Op.GetAndTouch, self.observability_instruments) as obs_handler:
            req = self.prepare_get_and_touch(key, expiry, obs_handler, opts, kwargs)
            return await self.send_get_and_touch(req, key)

    def prepare_get_and_touch(self,
                              key: str,
                              expiry: timedelta,
                              obs_handler: ObservableRequestHandler | None,
                              opts: GetAndTouchOptions | None,
                              kwargs: dict[str, Any]) -> ReadRequest[kv_pb2.GetAndTouchRequest]:
        """**INTERNAL**"""
        self._cluster_impl.core.ensure_open()
        return self._request_builder.build_get_and_touch_request(key, expiry, obs_handler, opts, **kwargs)

    async def send_get_and_touch(self, req: ReadRequest[kv_pb2.GetAndTouchRequest], key: str) -> GetResult:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        kv = await self._cluster_impl.kv()
        ctx = core.context(Op.GetAndTouch, req.timeout, obs_handler=req.obs_handler)
        response = await call_unary_async(kv.GetAndTouch, req.message, ctx, core.metadata)
        return GetResult(get_record(response), transcoder=req.transcoder, key=key)

    async def get_and_lock(self,
                           key: str,
                           lock_time: timedelta,
                           opts: GetAndLockOptions | None = None,
                           **kwargs: Any) -> GetResult:
        async with ObservableRequestHandler.create(Op.GetAndLock, self.observability_instruments) as obs_handler:
            req = self.prepare_get_and_lock(key, lock_time, obs_handler, opts, kwargs)
            return await self.send_get_and_lock(req, key)

    def prepare_get_and_lock(self,
                             key: str,
                             lock_time: timedelta,
                             obs_handler: ObservableRequestHandler | None,
                             opts: GetAndLockOptions | None,
                             kwargs: dict[str, Any]) -> ReadRequest[kv_pb2.GetAndLockRequest]:
        """**INTERNAL**"""
        self._cluster_impl.core.ensure_open()
        return self._request_builder.build_get_and_lock_request(key, lock_time, obs_handler, opts, **kwargs)

    async def send_get_and_lock(self, req: ReadRequest[kv_pb2.GetAndLockRequest], key: str) -> GetResult:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        kv = await self._cluster_impl.kv()
        ctx = core.context(Op.GetAndLock, req.timeout, obs_handler=req.obs_handler)
        response = await call_unary_async(kv.GetAndLock, req.message, ctx, core.metadata)
        return GetResult(get_record(response), transcoder=req.transcoder, key=key)

    async def unlock(self,
                     key: str,
                     cas: int,
                     opts: UnlockOptions | None = None,
                     **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.Unlock, self.observability_instruments) as obs_handler:
            return await self.send_unlock(self.prepare_unlock(key, cas, obs_handler, opts, kwargs), key)

    def prepare_unlock(self,
                       key: str,
                       cas: int,
                       obs_handler: ObservableRequestHandler | None,
                       opts: UnlockOptions | None,
                       kwargs: dict[str, Any]) -> KvRequest[kv_pb2.UnlockRequest]:
        """**INTERNAL**"""
        self._cluster_impl.core.ensure_open()
        return self._request_builder.build_unlock_request(key, cas, obs_handler, opts, **kwargs)

    async def send_unlock(self, req: KvRequest[kv_pb2.UnlockRequest], key: str) -> None:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        kv = await self._cluster_impl.kv()
        ctx = core.context(Op.Unlock, req.timeout, obs_handler=req.obs_handler)
        await call_unary_async(kv.Unlock, req.message, ctx, core.metadata)

    async def lookup_in(self,
                        key: str,
                        spec: Iterable[Spec],
                        opts: LookupInOptions | None = None,
                        **kwargs: Any) -> LookupInResult:
        async with ObservableRequestHandler.create(Op.LookupIn, self.observability_instruments) as obs_handler:
            return await self.send_lookup_in(self.prepare_lookup_in(key, spec, obs_handler, opts, kwargs), key)

    def prepare_lookup_in(self,
                          key: str,
                          spec: Iterable[Spec],
                          obs_handler: ObservableRequestHandler | None,
                          opts: LookupInOptions | None,
                          kwargs: dict[str, Any]) -> LookupInRequest:
        """**INTERNAL**"""
        self._cluster_impl.core.ensure_open()
        return self._request_builder.build_lookup_in_request(key, spec, obs_handler, opts, **kwargs)

    async def send_lookup_in(self, req: LookupInRequest, key: str) -> LookupInResult:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        kv = await self._cluster_impl.kv()
        ctx = core.context(Op.LookupIn, req.timeout, obs_handler=req.obs_handler)
        response = await call_unary_async(kv.LookupIn, req.message, ctx, core.metadata)
        return LookupInResult(lookup_in_record(response, req.specs, key), serializer=req.serializer, key=key)

    async def lookup_in_any_replica(self,
                                    key: str,
                                    spec: Iterable[Spec],
                                    opts: LookupInAnyReplicaOptions | None = None,
                                    **kwargs: Any) -> LookupInReplicaResult:
        raise unsupported('lookup_in_any_replica')

    async def lookup_in_all_replicas(self,
                                     key: str,
                                     spec: Iterable[Spec],
                                     opts: LookupInAllReplicasOptions | None = None,
                                     **kwargs: Any) -> Iterable[LookupInReplicaResult]:
        raise unsupported('lookup_in_all_replicas')

    async def mutate_in(self,
                        key: str,
                        spec: Iterable[Spec],
                        opts: MutateInOptions | None = None,
                        **kwargs: Any) -> MutateInResult:
        async with ObservableRequestHandler.create(Op.MutateIn, self.observability_instruments) as obs_handler:
            return await self.send_mutate_in(self.prepare_mutate_in(key, spec, obs_handler, opts, kwargs), key)

    def prepare_mutate_in(self,
                          key: str,
                          spec: Iterable[Spec],
                          obs_handler: ObservableRequestHandler | None,
                          opts: MutateInOptions | None,
                          kwargs: dict[str, Any]) -> MutateInRequest:
        """**INTERNAL**"""
        self._cluster_impl.core.ensure_open()
        return self._request_builder.build_mutate_in_request(key, spec, obs_handler, opts, **kwargs)

    async def send_mutate_in(self, req: MutateInRequest, key: str) -> MutateInResult:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        kv = await self._cluster_impl.kv()
        ctx = core.context(Op.MutateIn, req.timeout, obs_handler=req.obs_handler)
        response = await call_unary_async(kv.MutateIn, req.message, ctx, core.metadata)
        return MutateInResult(mutate_in_record(response, req.specs), serializer=req.serializer, key=key)

    def scan(self, scan_type: ScanType, opts: ScanOptions | None = None, **kwargs: Any) -> AsyncScanResultIterable:
        raise unsupported('scan')

    def binary(self) -> AsyncCollectionImpl:
        return self

    async def increment(self,
                        key: str,
                        opts: IncrementOptions | None = None,
                        **kwargs: Any) -> CounterResult:
        async with ObservableRequestHandler.create(Op.Increment, self.observability_instruments) as obs_handler:
            return await self.send_increment(self.prepare_increment(key, obs_handler, opts, kwargs), key)

    def prepare_increment(self,
                          key: str,
                          obs_handler: ObservableRequestHandler | None,
                          opts: IncrementOptions | None,
                          kwargs: dict[str, Any]) -> KvRequest[kv_pb2.IncrementRequest]:
        """**INTERNAL**"""
        self._cluster_impl.core.ensure_open()
        return self._request_builder.build_increment_request(key, obs_handler, opts, **kwargs)

    async def send_increment(self, req: KvRequest[kv_pb2.IncrementRequest], key: str) -> CounterResult:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        kv = await self._cluster_impl.kv()
        ctx = core.context(Op.Increment, req.timeout, obs_handler=req.obs_handler)
        response = await call_unary_async(kv.Increment, req.message, ctx, core.metadata)
        return CounterResult(counter_record(response), key=key)

    async def decrement(self,
                        key: str,
                        opts: DecrementOptions | None = None,
                        **kwargs: Any) -> CounterResult:
        async with ObservableRequestHandler.create(Op.Decrement, self.observability_instruments) as obs_handler:
            return await self.send_decrement(self.prepare_decrement(key, obs_handler, opts, kwargs), key)

    def prepare_decrement(self,
                          key: str,
                          obs_handler: ObservableRequestHandler | None,
                          opts: DecrementOptions | None,
                          kwargs: dict[str, Any]) -> KvRequest[kv_pb2.DecrementRequest]:
        """**INTERNAL**"""
        self._cluster_impl.core.ensure_open()
        return self._request_builder.build_decrement_request(key, obs_handler, opts, **kwargs)

    async def send_decrement(self, req: KvRequest[kv_pb2.DecrementRequest], key: str) -> CounterResult:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        kv = await self._cluster_impl.kv()
        ctx = core.context(Op.Decrement, req.timeout, obs_handler=req.obs_handler)
        response = await call_unary_async(kv.Decrement, req.message, ctx, core.metadata)
        return CounterResult(counter_record(response), key=key)

    async def append(self,
                     key: str,
                     value: str | bytes | bytearray,
                     opts: AppendOptions | None = None,
                     **kwargs: Any) -> MutationResult:
        async with ObservableRequestHandler.create(Op.Append, self.observability_instruments) as obs_handler:
            return await self.send_append(self.prepare_append(key, value, obs_handler, opts, kwargs), key)

    def prepare_append(self,
                       key: str,
                       value: str | bytes | bytearray,
                       obs_handler: ObservableRequestHandler | None,
                       opts: AppendOptions | None,
                       kwargs: dict[str, Any]) -> KvRequest[kv_pb2.AppendRequest]:
        """**INTERNAL**"""
        self._cluster_impl.core.ensure_open()
        return self._request_builder.build_append_request(key, value, obs_handler, opts, **kwargs)

    async def send_append(self, req: KvRequest[kv_pb2.AppendRequest], key: str) -> MutationResult:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        kv = await self._cluster_impl.kv()
        ctx = core.context(Op.Append, req.timeout, obs_handler=req.obs_handler)
        response = await call_unary_async(kv.Append, req.message, ctx, core.metadata)
        return MutationResult(mutation_record(response), key=key)

    async def prepend(self,
                      key: str,
                      value: str | bytes | bytearray,
                      opts: PrependOptions | None = None,
                      **kwargs: Any) -> MutationResult:
        async with ObservableRequestHandler.create(Op.Prepend, self.observability_instruments) as obs_handler:
            return await self.send_prepend(self.prepare_prepend(key, value, obs_handler, opts, kwargs), key)

    def prepare_prepend(self,
                        key: str,
                        value: str | bytes | bytearray,
                        obs_handler: ObservableRequestHandler | None,
                        opts: PrependOptions | None,
                        kwargs: dict[str, Any]) -> KvRequest[kv_pb2.PrependRequest]:
        """**INTERNAL**"""
        self._cluster_impl.core.ensure_open()
        return self._request_builder.build_prepend_request(key, value, obs_handler, opts, **kwargs)

    async def send_prepend(self, req: KvRequest[kv_pb2.PrependRequest], key: str) -> MutationResult:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        kv = await self._cluster_impl.kv()
        ctx = core.context(Op.Prepend, req.timeout, obs_handler=req.obs_handler)
        response = await call_unary_async(kv.Prepend, req.message, ctx, core.metadata)
        return MutationResult(mutation_record(response), key=key)

    def query_index_mgmt(self) -> AsyncQueryIndexMgmtImpl:
        """**INTERNAL**"""
        return AsyncQueryIndexMgmtImpl(self._cluster_impl, collection_context=(self.name, self.scope_name))
