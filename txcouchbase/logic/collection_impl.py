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
from typing import TYPE_CHECKING, Iterable

from twisted.internet.defer import Deferred

from acouchbase.logic.collection_impl import AsyncCollectionImpl
from couchbase.result import (CounterResult,
                              ExistsResult,
                              GetReplicaResult,
                              GetResult,
                              LookupInReplicaResult,
                              LookupInResult,
                              MutateInResult,
                              MutationResult)

if TYPE_CHECKING:
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


class TxCollectionImpl(AsyncCollectionImpl):

    def __init__(self, collection_name: str, scope: TxScope) -> None:
        super().__init__(collection_name, scope)

    def append_deferred(self, req: AppendRequest) -> Deferred[MutationResult]:
        coro = super().append(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def decrement_deferred(self, req: DecrementRequest) -> Deferred[CounterResult]:
        coro = super().decrement(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def exists_deferred(self, req: ExistsRequest) -> Deferred[ExistsResult]:
        coro = super().exists(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_replicas_deferred(self, req: GetAllReplicasRequest) -> Deferred[Iterable[GetReplicaResult]]:
        coro = super().get_all_replicas(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_and_lock_deferred(self, req: GetAndLockRequest) -> Deferred[GetResult]:
        coro = super().get_and_lock(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_and_touch_deferred(self, req: GetAndTouchRequest) -> Deferred[GetResult]:
        coro = super().get_and_touch(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_any_replica_deferred(self, req: GetAnyReplicaRequest) -> Deferred[GetReplicaResult]:
        coro = super().get_any_replica(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_deferred(self, req: GetRequest) -> Deferred[GetResult]:
        coro = super().get(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def increment_deferred(self, req: IncrementRequest) -> Deferred[CounterResult]:
        coro = super().increment(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def insert_deferred(self, req: InsertRequest) -> Deferred[MutationResult]:
        coro = super().insert(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def lookup_in_deferred(self, req: LookupInRequest) -> Deferred[LookupInResult]:
        coro = super().lookup_in(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def lookup_in_all_replicas_deferred(self,
                                        req: LookupInAllReplicasRequest) -> Deferred[Iterable[LookupInReplicaResult]]:
        coro = super().lookup_in_all_replicas(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def lookup_in_any_replica_deferred(self, req: LookupInAnyReplicaRequest) -> Deferred[LookupInReplicaResult]:
        coro = super().lookup_in_any_replica(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def mutate_in_deferred(self, req: MutateInRequest) -> Deferred[MutateInResult]:
        coro = super().mutate_in(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def prepend_deferred(self, req: PrependRequest) -> Deferred[MutationResult]:
        coro = super().prepend(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def remove_deferred(self, req: RemoveRequest) -> Deferred[MutationResult]:
        coro = super().remove(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def replace_deferred(self, req: ReplaceRequest) -> Deferred[MutationResult]:
        coro = super().replace(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def touch_deferred(self, req: TouchRequest) -> Deferred[MutationResult]:
        coro = super().touch(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def unlock_deferred(self, req: UnlockRequest) -> Deferred[None]:
        coro = super().unlock(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def upsert_deferred(self, req: UpsertRequest) -> Deferred[MutationResult]:
        coro = super().upsert(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def wait_until_bucket_connected_deferred(self) -> Deferred[None]:
        coro = super().wait_until_bucket_connected()
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d
