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
from twisted.python.failure import Failure

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
    from couchbase.logic.observability.handler import ObservableRequestHandler
    from couchbase.logic.pycbc_core import pycbc_kv_request as PycbcCoreKeyValueRequest
    from couchbase.transcoder import Transcoder
    from txcouchbase.scope import TxScope


class TxCollectionImpl(AsyncCollectionImpl):

    def __init__(self, collection_name: str, scope: TxScope) -> None:
        super().__init__(collection_name, scope)

    def _finish_span(self, result, obs_handler):
        """Callback to properly end the span on success or failure."""
        if obs_handler is None:
            return result
        if isinstance(result, Failure):
            exc = result.value
            obs_handler.__exit__(type(exc), exc, exc.__traceback__)
            return result
        else:
            obs_handler.__exit__(None, None, None)
            return result

    def append_deferred(self,
                        req: PycbcCoreKeyValueRequest,
                        obs_handler: ObservableRequestHandler) -> Deferred[MutationResult]:
        coro = super().append(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def decrement_deferred(self,
                           req: PycbcCoreKeyValueRequest,
                           obs_handler: ObservableRequestHandler) -> Deferred[CounterResult]:
        coro = super().decrement(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def exists_deferred(self,
                        req: PycbcCoreKeyValueRequest,
                        obs_handler: ObservableRequestHandler) -> Deferred[ExistsResult]:
        coro = super().exists(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_replicas_deferred(self,
                                  req: PycbcCoreKeyValueRequest,
                                  transcoder: Transcoder,
                                  obs_handler: ObservableRequestHandler) -> Deferred[Iterable[GetReplicaResult]]:
        coro = super().get_all_replicas(req, transcoder, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_and_lock_deferred(self,
                              req: PycbcCoreKeyValueRequest,
                              transcoder: Transcoder,
                              obs_handler: ObservableRequestHandler) -> Deferred[GetResult]:
        coro = super().get_and_lock(req, transcoder, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_and_touch_deferred(self,
                               req: PycbcCoreKeyValueRequest,
                               transcoder: Transcoder,
                               obs_handler: ObservableRequestHandler) -> Deferred[GetResult]:
        coro = super().get_and_touch(req, transcoder, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_any_replica_deferred(self,
                                 req: PycbcCoreKeyValueRequest,
                                 transcoder: Transcoder,
                                 obs_handler: ObservableRequestHandler) -> Deferred[GetReplicaResult]:
        coro = super().get_any_replica(req, transcoder, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_deferred(self,
                     req: PycbcCoreKeyValueRequest,
                     transcoder: Transcoder,
                     obs_handler: ObservableRequestHandler) -> Deferred[GetResult]:
        coro = super().get(req, transcoder, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def increment_deferred(self,
                           req: PycbcCoreKeyValueRequest,
                           obs_handler: ObservableRequestHandler) -> Deferred[CounterResult]:
        coro = super().increment(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def insert_deferred(self,
                        req: PycbcCoreKeyValueRequest,
                        obs_handler: ObservableRequestHandler) -> Deferred[MutationResult]:
        coro = super().insert(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def lookup_in_deferred(self,
                           req: PycbcCoreKeyValueRequest,
                           transcoder: Transcoder,
                           obs_handler: ObservableRequestHandler) -> Deferred[LookupInResult]:
        coro = super().lookup_in(req, transcoder, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def lookup_in_all_replicas_deferred(self,
                                        req: PycbcCoreKeyValueRequest,
                                        transcoder: Transcoder,
                                        obs_handler: ObservableRequestHandler
                                        ) -> Deferred[Iterable[LookupInReplicaResult]]:
        coro = super().lookup_in_all_replicas(req, transcoder, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def lookup_in_any_replica_deferred(self,
                                       req: PycbcCoreKeyValueRequest,
                                       transcoder: Transcoder,
                                       obs_handler: ObservableRequestHandler) -> Deferred[LookupInReplicaResult]:
        coro = super().lookup_in_any_replica(req, transcoder, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def mutate_in_deferred(self,
                           req: PycbcCoreKeyValueRequest,
                           obs_handler: ObservableRequestHandler) -> Deferred[MutateInResult]:
        coro = super().mutate_in(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def prepend_deferred(self,
                         req: PycbcCoreKeyValueRequest,
                         obs_handler: ObservableRequestHandler) -> Deferred[MutationResult]:
        coro = super().prepend(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def remove_deferred(self,
                        req: PycbcCoreKeyValueRequest,
                        obs_handler: ObservableRequestHandler) -> Deferred[MutationResult]:
        coro = super().remove(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def replace_deferred(self,
                         req: PycbcCoreKeyValueRequest,
                         obs_handler: ObservableRequestHandler) -> Deferred[MutationResult]:
        coro = super().replace(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def touch_deferred(self,
                       req: PycbcCoreKeyValueRequest,
                       obs_handler: ObservableRequestHandler) -> Deferred[MutationResult]:
        coro = super().touch(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def unlock_deferred(self,
                        req: PycbcCoreKeyValueRequest,
                        obs_handler: ObservableRequestHandler) -> Deferred[None]:
        coro = super().unlock(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def upsert_deferred(self,
                        req: PycbcCoreKeyValueRequest,
                        obs_handler: ObservableRequestHandler) -> Deferred[MutationResult]:
        coro = super().upsert(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def wait_until_bucket_connected_deferred(self) -> Deferred[None]:
        coro = super().wait_until_bucket_connected()
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d
