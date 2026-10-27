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
from collections.abc import Coroutine, Iterable
from typing import TYPE_CHECKING, Any

from twisted.internet.defer import Deferred

from acouchbase.protocol.classic._collection_impl import AsyncCollectionImpl
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from txcouchbase.protocol.classic._deferred import defer

if TYPE_CHECKING:
    from datetime import timedelta

    from couchbase.common._utils import DocumentValue
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
                                          TouchOptions,
                                          UnlockOptions,
                                          UpsertOptions)
    from couchbase.common.result import (CounterResult,
                                         ExistsResult,
                                         GetReplicaResult,
                                         GetResult,
                                         LookupInReplicaResult,
                                         LookupInResult,
                                         MutateInResult,
                                         MutationResult)
    from couchbase.common.subdocument import Spec


class TxCollectionImpl(AsyncCollectionImpl):

    def binary(self) -> TxCollectionImpl:
        """**INTERNAL** The binary operations are this impl's own."""
        return self

    def get_deferred(self,
                     key: str,
                     opts: GetOptions | None = None,
                     **kwargs: Any) -> Deferred[GetResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Get, self.observability_instruments)

        def start() -> Coroutine[Any, Any, GetResult]:
            req, transcoder = self._request_builder.build_get_request(key, obs_handler, opts, **kwargs)
            return self._get(req, transcoder, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_any_replica_deferred(self,
                                 key: str,
                                 opts: GetAnyReplicaOptions | None = None,
                                 **kwargs: Any) -> Deferred[GetReplicaResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.GetAnyReplica, self.observability_instruments)

        def start() -> Coroutine[Any, Any, GetReplicaResult]:
            req, transcoder = self._request_builder.build_get_any_replica_request(key, obs_handler, opts, **kwargs)
            return self._get_any_replica(req, transcoder, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_all_replicas_deferred(self,
                                  key: str,
                                  opts: GetAllReplicasOptions | None = None,
                                  **kwargs: Any) -> Deferred[Iterable[GetReplicaResult]]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.GetAllReplicas, self.observability_instruments)

        def start() -> Coroutine[Any, Any, Iterable[GetReplicaResult]]:
            req, transcoder = self._request_builder.build_get_all_replicas_request(key, obs_handler, opts, **kwargs)
            return self._get_all_replicas(req, transcoder, obs_handler)

        return defer(self.loop, obs_handler, start)

    def exists_deferred(self,
                        key: str,
                        opts: ExistsOptions | None = None,
                        **kwargs: Any) -> Deferred[ExistsResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Exists, self.observability_instruments)

        def start() -> Coroutine[Any, Any, ExistsResult]:
            req = self._request_builder.build_exists_request(key, obs_handler, opts, **kwargs)
            return self._exists(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def insert_deferred(self,
                        key: str,
                        value: DocumentValue,
                        opts: InsertOptions | None = None,
                        **kwargs: Any) -> Deferred[MutationResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Insert, self.observability_instruments)

        def start() -> Coroutine[Any, Any, MutationResult]:
            req = self._request_builder.build_insert_request(key, value, obs_handler, opts, **kwargs)
            return self._insert(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def upsert_deferred(self,
                        key: str,
                        value: DocumentValue,
                        opts: UpsertOptions | None = None,
                        **kwargs: Any) -> Deferred[MutationResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Upsert, self.observability_instruments)

        def start() -> Coroutine[Any, Any, MutationResult]:
            req = self._request_builder.build_upsert_request(key, value, obs_handler, opts, **kwargs)
            return self._upsert(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def replace_deferred(self,
                         key: str,
                         value: DocumentValue,
                         opts: ReplaceOptions | None = None,
                         **kwargs: Any) -> Deferred[MutationResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Replace, self.observability_instruments)

        def start() -> Coroutine[Any, Any, MutationResult]:
            req = self._request_builder.build_replace_request(key, value, obs_handler, opts, **kwargs)
            return self._replace(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def remove_deferred(self,
                        key: str,
                        opts: RemoveOptions | None = None,
                        **kwargs: Any) -> Deferred[MutationResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Remove, self.observability_instruments)

        def start() -> Coroutine[Any, Any, MutationResult]:
            req = self._request_builder.build_remove_request(key, obs_handler, opts, **kwargs)
            return self._remove(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def touch_deferred(self,
                       key: str,
                       expiry: timedelta,
                       opts: TouchOptions | None = None,
                       **kwargs: Any) -> Deferred[MutationResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Touch, self.observability_instruments)

        def start() -> Coroutine[Any, Any, MutationResult]:
            req = self._request_builder.build_touch_request(key, expiry, obs_handler, opts, **kwargs)
            return self._touch(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_and_touch_deferred(self,
                               key: str,
                               expiry: timedelta,
                               opts: GetAndTouchOptions | None = None,
                               **kwargs: Any) -> Deferred[GetResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.GetAndTouch, self.observability_instruments)

        def start() -> Coroutine[Any, Any, GetResult]:
            req, transcoder = self._request_builder.build_get_and_touch_request(key,
                                                                                expiry,
                                                                                obs_handler,
                                                                                opts,
                                                                                **kwargs)
            return self._get_and_touch(req, transcoder, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_and_lock_deferred(self,
                              key: str,
                              lock_time: timedelta,
                              opts: GetAndLockOptions | None = None,
                              **kwargs: Any) -> Deferred[GetResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.GetAndLock, self.observability_instruments)

        def start() -> Coroutine[Any, Any, GetResult]:
            req, transcoder = self._request_builder.build_get_and_lock_request(key,
                                                                               lock_time,
                                                                               obs_handler,
                                                                               opts,
                                                                               **kwargs)
            return self._get_and_lock(req, transcoder, obs_handler)

        return defer(self.loop, obs_handler, start)

    def unlock_deferred(self,
                        key: str,
                        cas: int,
                        opts: UnlockOptions | None = None,
                        **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Unlock, self.observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_unlock_request(key, cas, obs_handler, opts, **kwargs)
            return self._unlock(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def lookup_in_deferred(self,
                           key: str,
                           spec: Iterable[Spec],
                           opts: LookupInOptions | None = None,
                           **kwargs: Any) -> Deferred[LookupInResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.LookupIn, self.observability_instruments)

        def start() -> Coroutine[Any, Any, LookupInResult]:
            req, serializer = self._request_builder.build_lookup_in_request(key, spec, obs_handler, opts, **kwargs)
            return self._lookup_in(req, serializer, obs_handler)

        return defer(self.loop, obs_handler, start)

    def lookup_in_any_replica_deferred(self,
                                       key: str,
                                       spec: Iterable[Spec],
                                       opts: LookupInAnyReplicaOptions | None = None,
                                       **kwargs: Any) -> Deferred[LookupInReplicaResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.LookupInAnyReplica, self.observability_instruments)

        def start() -> Coroutine[Any, Any, LookupInReplicaResult]:
            req, serializer = self._request_builder.build_lookup_in_any_replica_request(key,
                                                                                        spec,
                                                                                        obs_handler,
                                                                                        opts,
                                                                                        **kwargs)
            return self._lookup_in_any_replica(req, serializer, obs_handler)

        return defer(self.loop, obs_handler, start)

    def lookup_in_all_replicas_deferred(self,
                                        key: str,
                                        spec: Iterable[Spec],
                                        opts: LookupInAllReplicasOptions | None = None,
                                        **kwargs: Any) -> Deferred[Iterable[LookupInReplicaResult]]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.LookupInAllReplicas, self.observability_instruments)

        def start() -> Coroutine[Any, Any, Iterable[LookupInReplicaResult]]:
            req, serializer = self._request_builder.build_lookup_in_all_replicas_request(key,
                                                                                         spec,
                                                                                         obs_handler,
                                                                                         opts,
                                                                                         **kwargs)
            return self._lookup_in_all_replicas(req, serializer, obs_handler)

        return defer(self.loop, obs_handler, start)

    def mutate_in_deferred(self,
                           key: str,
                           spec: Iterable[Spec],
                           opts: MutateInOptions | None = None,
                           **kwargs: Any) -> Deferred[MutateInResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.MutateIn, self.observability_instruments)

        def start() -> Coroutine[Any, Any, MutateInResult]:
            req, serializer = self._request_builder.build_mutate_in_request(key, spec, obs_handler, opts, **kwargs)
            return self._mutate_in(req, serializer, obs_handler)

        return defer(self.loop, obs_handler, start)

    def increment_deferred(self,
                           key: str,
                           opts: IncrementOptions | None = None,
                           **kwargs: Any) -> Deferred[CounterResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Increment, self.observability_instruments)

        def start() -> Coroutine[Any, Any, CounterResult]:
            req = self._request_builder.build_increment_request(key, obs_handler, opts, **kwargs)
            return self._increment(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def decrement_deferred(self,
                           key: str,
                           opts: DecrementOptions | None = None,
                           **kwargs: Any) -> Deferred[CounterResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Decrement, self.observability_instruments)

        def start() -> Coroutine[Any, Any, CounterResult]:
            req = self._request_builder.build_decrement_request(key, obs_handler, opts, **kwargs)
            return self._decrement(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def append_deferred(self,
                        key: str,
                        value: str | bytes | bytearray,
                        opts: AppendOptions | None = None,
                        **kwargs: Any) -> Deferred[MutationResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Append, self.observability_instruments)

        def start() -> Coroutine[Any, Any, MutationResult]:
            req = self._request_builder.build_append_request(key, value, obs_handler, opts, **kwargs)
            return self._append(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def prepend_deferred(self,
                         key: str,
                         value: str | bytes | bytearray,
                         opts: PrependOptions | None = None,
                         **kwargs: Any) -> Deferred[MutationResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Prepend, self.observability_instruments)

        def start() -> Coroutine[Any, Any, MutationResult]:
            req = self._request_builder.build_prepend_request(key, value, obs_handler, opts, **kwargs)
            return self._prepend(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def wait_until_bucket_connected_deferred(self) -> Deferred[None]:
        coro = super().wait_until_bucket_connected()
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d
