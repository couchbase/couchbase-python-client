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

from collections.abc import Coroutine, Iterable
from typing import TYPE_CHECKING, Any

from twisted.internet.defer import Deferred

from acouchbase.protocol.couchbase2._collection_impl import AsyncCollectionImpl
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.protocol.couchbase2._core import unsupported
from txcouchbase.protocol.couchbase2._deferred import defer

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
                                          TouchOptions,
                                          UnlockOptions,
                                          UpsertOptions)
    from couchbase.common.replica_reads import GetReplicaStrategy
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
        return self

    def get_deferred(self, key: str, opts: GetOptions | None = None, **kwargs: Any) -> Deferred[GetResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Get, self.observability_instruments)

        def start() -> Coroutine[Any, Any, GetResult]:
            return self.send_get(self.prepare_get(key, obs_handler, opts, kwargs), key)

        return defer(self._cluster_impl.loop, obs_handler, start)

    def upsert_deferred(self,
                        key: str,
                        value: DocumentValue,
                        opts: UpsertOptions | None = None,
                        **kwargs: Any) -> Deferred[MutationResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Upsert, self.observability_instruments)

        def start() -> Coroutine[Any, Any, MutationResult]:
            return self.send_upsert(self.prepare_upsert(key, value, obs_handler, opts, kwargs), key)

        return defer(self._cluster_impl.loop, obs_handler, start)

    def get_any_replica_deferred(self,
                                 key: str,
                                 opts: GetAnyReplicaOptions | None = None,
                                 **kwargs: Any) -> Deferred[GetReplicaResult]:
        raise unsupported('get_any_replica')

    def get_replica_deferred(self,
                             key: str,
                             strategy: GetReplicaStrategy,
                             opts: GetReplicaOptions | None = None,
                             **kwargs: Any) -> Deferred[GetReplicaResult]:
        raise unsupported('get_replica')

    def get_all_replicas_deferred(self,
                                  key: str,
                                  opts: GetAllReplicasOptions | None = None,
                                  **kwargs: Any) -> Deferred[Iterable[GetReplicaResult]]:
        raise unsupported('get_all_replicas')

    def exists_deferred(self,
                        key: str,
                        opts: ExistsOptions | None = None,
                        **kwargs: Any) -> Deferred[ExistsResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Exists, self.observability_instruments)

        def start() -> Coroutine[Any, Any, ExistsResult]:
            return self.send_exists(self.prepare_exists(key, obs_handler, opts, kwargs), key)

        return defer(self._cluster_impl.loop, obs_handler, start)

    def insert_deferred(self,
                        key: str,
                        value: DocumentValue,
                        opts: InsertOptions | None = None,
                        **kwargs: Any) -> Deferred[MutationResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Insert, self.observability_instruments)

        def start() -> Coroutine[Any, Any, MutationResult]:
            return self.send_insert(self.prepare_insert(key, value, obs_handler, opts, kwargs), key)

        return defer(self._cluster_impl.loop, obs_handler, start)

    def replace_deferred(self,
                         key: str,
                         value: DocumentValue,
                         opts: ReplaceOptions | None = None,
                         **kwargs: Any) -> Deferred[MutationResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Replace, self.observability_instruments)

        def start() -> Coroutine[Any, Any, MutationResult]:
            return self.send_replace(self.prepare_replace(key, value, obs_handler, opts, kwargs), key)

        return defer(self._cluster_impl.loop, obs_handler, start)

    def remove_deferred(self,
                        key: str,
                        opts: RemoveOptions | None = None,
                        **kwargs: Any) -> Deferred[MutationResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Remove, self.observability_instruments)

        def start() -> Coroutine[Any, Any, MutationResult]:
            return self.send_remove(self.prepare_remove(key, obs_handler, opts, kwargs), key)

        return defer(self._cluster_impl.loop, obs_handler, start)

    def touch_deferred(self,
                       key: str,
                       expiry: timedelta,
                       opts: TouchOptions | None = None,
                       **kwargs: Any) -> Deferred[MutationResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Touch, self.observability_instruments)

        def start() -> Coroutine[Any, Any, MutationResult]:
            return self.send_touch(self.prepare_touch(key, expiry, obs_handler, opts, kwargs), key)

        return defer(self._cluster_impl.loop, obs_handler, start)

    def get_and_touch_deferred(self,
                               key: str,
                               expiry: timedelta,
                               opts: GetAndTouchOptions | None = None,
                               **kwargs: Any) -> Deferred[GetResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.GetAndTouch, self.observability_instruments)

        def start() -> Coroutine[Any, Any, GetResult]:
            return self.send_get_and_touch(self.prepare_get_and_touch(key, expiry, obs_handler, opts, kwargs), key)

        return defer(self._cluster_impl.loop, obs_handler, start)

    def get_and_lock_deferred(self,
                              key: str,
                              lock_time: timedelta,
                              opts: GetAndLockOptions | None = None,
                              **kwargs: Any) -> Deferred[GetResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.GetAndLock, self.observability_instruments)

        def start() -> Coroutine[Any, Any, GetResult]:
            return self.send_get_and_lock(self.prepare_get_and_lock(key, lock_time, obs_handler, opts, kwargs), key)

        return defer(self._cluster_impl.loop, obs_handler, start)

    def unlock_deferred(self,
                        key: str,
                        cas: int,
                        opts: UnlockOptions | None = None,
                        **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Unlock, self.observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            return self.send_unlock(self.prepare_unlock(key, cas, obs_handler, opts, kwargs), key)

        return defer(self._cluster_impl.loop, obs_handler, start)

    def lookup_in_deferred(self,
                           key: str,
                           spec: Iterable[Spec],
                           opts: LookupInOptions | None = None,
                           **kwargs: Any) -> Deferred[LookupInResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.LookupIn, self.observability_instruments)

        def start() -> Coroutine[Any, Any, LookupInResult]:
            return self.send_lookup_in(self.prepare_lookup_in(key, spec, obs_handler, opts, kwargs), key)

        return defer(self._cluster_impl.loop, obs_handler, start)

    def lookup_in_any_replica_deferred(self,
                                       key: str,
                                       spec: Iterable[Spec],
                                       opts: LookupInAnyReplicaOptions | None = None,
                                       **kwargs: Any) -> Deferred[LookupInReplicaResult]:
        raise unsupported('lookup_in_any_replica')

    def lookup_in_all_replicas_deferred(self,
                                        key: str,
                                        spec: Iterable[Spec],
                                        opts: LookupInAllReplicasOptions | None = None,
                                        **kwargs: Any) -> Deferred[Iterable[LookupInReplicaResult]]:
        raise unsupported('lookup_in_all_replicas')

    def mutate_in_deferred(self,
                           key: str,
                           spec: Iterable[Spec],
                           opts: MutateInOptions | None = None,
                           **kwargs: Any) -> Deferred[MutateInResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.MutateIn, self.observability_instruments)

        def start() -> Coroutine[Any, Any, MutateInResult]:
            return self.send_mutate_in(self.prepare_mutate_in(key, spec, obs_handler, opts, kwargs), key)

        return defer(self._cluster_impl.loop, obs_handler, start)

    def increment_deferred(self,
                           key: str,
                           opts: IncrementOptions | None = None,
                           **kwargs: Any) -> Deferred[CounterResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Increment, self.observability_instruments)

        def start() -> Coroutine[Any, Any, CounterResult]:
            return self.send_increment(self.prepare_increment(key, obs_handler, opts, kwargs), key)

        return defer(self._cluster_impl.loop, obs_handler, start)

    def decrement_deferred(self,
                           key: str,
                           opts: DecrementOptions | None = None,
                           **kwargs: Any) -> Deferred[CounterResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Decrement, self.observability_instruments)

        def start() -> Coroutine[Any, Any, CounterResult]:
            return self.send_decrement(self.prepare_decrement(key, obs_handler, opts, kwargs), key)

        return defer(self._cluster_impl.loop, obs_handler, start)

    def append_deferred(self,
                        key: str,
                        value: str | bytes | bytearray,
                        opts: AppendOptions | None = None,
                        **kwargs: Any) -> Deferred[MutationResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Append, self.observability_instruments)

        def start() -> Coroutine[Any, Any, MutationResult]:
            return self.send_append(self.prepare_append(key, value, obs_handler, opts, kwargs), key)

        return defer(self._cluster_impl.loop, obs_handler, start)

    def prepend_deferred(self,
                         key: str,
                         value: str | bytes | bytearray,
                         opts: PrependOptions | None = None,
                         **kwargs: Any) -> Deferred[MutationResult]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.Prepend, self.observability_instruments)

        def start() -> Coroutine[Any, Any, MutationResult]:
            return self.send_prepend(self.prepare_prepend(key, value, obs_handler, opts, kwargs), key)

        return defer(self._cluster_impl.loop, obs_handler, start)
