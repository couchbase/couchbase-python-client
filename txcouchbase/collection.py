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

# used to allow for unquoted (i.e. forward reference, Python >= 3.7, PEP563)
from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    Union)

from twisted.internet.defer import Deferred

from couchbase.logic.collection import CollectionLogic
from couchbase.options import forward_args
from couchbase.result import (CounterResult,
                              ExistsResult,
                              GetReplicaResult,
                              GetResult,
                              LookupInReplicaResult,
                              LookupInResult,
                              MutateInResult,
                              MutationResult)
from txcouchbase.binary_collection import BinaryCollection
from txcouchbase.logic import TxWrapper

if TYPE_CHECKING:
    from datetime import timedelta

    from couchbase._utils import JSONType
    from couchbase.options import (AppendOptions,
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
    from couchbase.subdocument import Spec


class Collection(CollectionLogic):

    def __init__(self, scope, name):
        super().__init__(scope, name)
        self._loop = scope.loop

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._loop

    def get(self,
            key,  # type: str
            *opts,  # type: GetOptions
            **kwargs,  # type: Dict[str, Any]
            ) -> Deferred[GetResult]:
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_internal(key, **final_args)

    @TxWrapper.inject_callbacks_and_decode(GetResult)
    def _get_internal(
        self,
        key,  # type: str
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[GetResult]:
        super().get(key, **kwargs)

    def get_any_replica(self,
                        key,  # type: str
                        *opts,  # type: GetAnyReplicaOptions
                        **kwargs,  # type: Dict[str, Any]
                        ) -> Deferred[GetResult]:
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_any_replica_internal(key, **final_args)

    @TxWrapper.inject_callbacks_and_decode(GetReplicaResult)
    def _get_any_replica_internal(
        self,
        key,  # type: str
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[GetReplicaResult]:
        super().get_any_replica(key, **kwargs)

    def get_all_replicas(self,
                         key,  # type: str
                         *opts,  # type: GetAllReplicasOptions
                         **kwargs,  # type: Dict[str, Any]
                         ) -> Deferred[Iterable[GetReplicaResult]]:
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_all_replicas_internal(key, **final_args)

    @TxWrapper.inject_callbacks_and_decode(GetReplicaResult)
    def _get_all_replicas_internal(
        self,
        key,  # type: str
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[Iterable[GetReplicaResult]]:
        super().get_all_replicas(key, **kwargs)

    @TxWrapper.inject_callbacks(ExistsResult)
    def exists(
        self,
        key,  # type: str
        *opts,  # type: ExistsOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[ExistsResult]:
        super().exists(key, *opts, **kwargs)

    @TxWrapper.inject_callbacks(MutationResult)
    def insert(
        self,  # type: "Collection"
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: InsertOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[MutationResult]:
        super().insert(key, value, *opts, **kwargs)

    @TxWrapper.inject_callbacks(MutationResult)
    def upsert(
        self,
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: UpsertOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[MutationResult]:
        super().upsert(key, value, *opts, **kwargs)

    @TxWrapper.inject_callbacks(MutationResult)
    def replace(self,
                key,  # type: str
                value,  # type: JSONType
                *opts,  # type: ReplaceOptions
                **kwargs,  # type: Dict[str, Any]
                ) -> MutationResult:
        super().replace(key, value, *opts, **kwargs)

    @TxWrapper.inject_callbacks(MutationResult)
    def remove(self,
               key,  # type: str
               *opts,  # type: RemoveOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> MutationResult:
        super().remove(key, *opts, **kwargs)

    @TxWrapper.inject_callbacks(MutationResult)
    def touch(self,
              key,  # type: str
              expiry,  # type: timedelta
              *opts,  # type: TouchOptions
              **kwargs,  # type: Dict[str, Any]
              ) -> MutationResult:
        super().touch(key, expiry, *opts, **kwargs)

    def get_and_touch(self,
                      key,  # type: str
                      expiry,  # type: timedelta
                      *opts,  # type: GetAndTouchOptions
                      **kwargs,  # type: Dict[str, Any]
                      ) -> GetResult:
        # add to kwargs for conversion to int
        kwargs["expiry"] = expiry
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_and_touch_internal(key, **final_args)

    @TxWrapper.inject_callbacks_and_decode(GetResult)
    def _get_and_touch_internal(self,
                                key,  # type: str
                                **kwargs,  # type: Dict[str, Any]
                                ) -> GetResult:
        super().get_and_touch(key, **kwargs)

    def get_and_lock(
        self,
        key,  # type: str
        lock_time,  # type: timedelta
        *opts,  # type: GetAndLockOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> GetResult:
        # add to kwargs for conversion to int
        kwargs["lock_time"] = lock_time
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_and_lock_internal(key, **final_args)

    @TxWrapper.inject_callbacks_and_decode(GetResult)
    def _get_and_lock_internal(self,
                               key,  # type: str
                               **kwargs,  # type: Dict[str, Any]
                               ) -> GetResult:
        super().get_and_lock(key, **kwargs)

    @TxWrapper.inject_callbacks(None)
    def unlock(self,
               key,  # type: str
               cas,  # type: int
               *opts,  # type: UnlockOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> None:
        super().unlock(key, cas, *opts, **kwargs)

    def lookup_in(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: LookupInOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> LookupInResult:
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder
        return self._lookup_in_internal(key, spec, **final_args)

    @TxWrapper.inject_callbacks_and_decode(LookupInResult)
    def _lookup_in_internal(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        **kwargs,  # type: Dict[str, Any]
    ) -> LookupInResult:
        super().lookup_in(key, spec, **kwargs)

    def lookup_in_any_replica(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: LookupInAnyReplicaOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> LookupInReplicaResult:
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder
        return self._lookup_in_any_replica_internal(key, spec, **final_args)

    @TxWrapper.inject_callbacks_and_decode(LookupInReplicaResult)
    def _lookup_in_any_replica_internal(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        **kwargs,  # type: Dict[str, Any]
    ) -> LookupInReplicaResult:
        super().lookup_in_any_replica(key, spec, **kwargs)

    def lookup_in_all_replicas(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: LookupInAllReplicasOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Iterable[LookupInReplicaResult]:
        final_args = forward_args(kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder
        return self._lookup_in_all_replicas_internal(key, spec, **final_args)

    @TxWrapper.inject_callbacks_and_decode(LookupInReplicaResult)
    def _lookup_in_all_replicas_internal(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        **kwargs,  # type: Dict[str, Any]
    ) -> Iterable[LookupInReplicaResult]:
        super().lookup_in_all_replicas(key, spec, **kwargs)

    @TxWrapper.inject_callbacks(MutateInResult)
    def mutate_in(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: MutateInOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MutateInResult:
        super().mutate_in(key, spec, *opts, **kwargs)

    def binary(self) -> BinaryCollection:
        return BinaryCollection(self)

    @TxWrapper.inject_callbacks(MutationResult)
    def _append(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: AppendOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[MutationResult]:
        super().append(key, value, *opts, **kwargs)

    @TxWrapper.inject_callbacks(MutationResult)
    def _prepend(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: PrependOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[MutationResult]:
        super().prepend(key, value, *opts, **kwargs)

    @TxWrapper.inject_callbacks(CounterResult)
    def _increment(
        self,
        key,  # type: str
        *opts,  # type: IncrementOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[CounterResult]:
        super().increment(key, *opts, **kwargs)

    @TxWrapper.inject_callbacks(CounterResult)
    def _decrement(
        self,
        key,  # type: str
        *opts,  # type: DecrementOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[CounterResult]:
        super().decrement(key, *opts, **kwargs)

    @staticmethod
    def default_name():
        return "_default"


TxCollection = Collection
