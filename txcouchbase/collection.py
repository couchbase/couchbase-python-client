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
                    Iterable)

from twisted.internet.defer import Deferred

from couchbase.result import (ExistsResult,
                              GetReplicaResult,
                              GetResult,
                              LookupInReplicaResult,
                              LookupInResult,
                              MutateInResult,
                              MutationResult)
from txcouchbase.binary_collection import BinaryCollection
from txcouchbase.logic.collection_impl import TxCollectionImpl

if TYPE_CHECKING:
    from datetime import timedelta

    from couchbase._utils import JSONType
    from couchbase.options import (ExistsOptions,
                                   GetAllReplicasOptions,
                                   GetAndLockOptions,
                                   GetAndTouchOptions,
                                   GetAnyReplicaOptions,
                                   GetOptions,
                                   InsertOptions,
                                   LookupInAllReplicasOptions,
                                   LookupInAnyReplicaOptions,
                                   LookupInOptions,
                                   MutateInOptions,
                                   RemoveOptions,
                                   ReplaceOptions,
                                   TouchOptions,
                                   UnlockOptions,
                                   UpsertOptions)
    from couchbase.subdocument import Spec
    from txcouchbase.scope import TxScope


class Collection:

    def __init__(self, scope: TxScope, name: str) -> None:
        self._impl = TxCollectionImpl(name, scope)

    @property
    def name(self) -> str:
        """
            str: The name of this :class:`~.Collection` instance.
        """
        return self._impl.name

    def get(self,
            key,  # type: str
            *opts,  # type: GetOptions
            **kwargs,  # type: Dict[str, Any]
            ) -> Deferred[GetResult]:
        req = self._impl.request_builder.build_get_request(key, *opts, **kwargs)
        return self._impl.get_deferred(req)

    def get_any_replica(self,
                        key,  # type: str
                        *opts,  # type: GetAnyReplicaOptions
                        **kwargs,  # type: Dict[str, Any]
                        ) -> Deferred[GetResult]:
        req = self._impl.request_builder.build_get_any_replica_request(key, *opts, **kwargs)
        return self._impl.get_any_replica_deferred(req)

    def get_all_replicas(self,
                         key,  # type: str
                         *opts,  # type: GetAllReplicasOptions
                         **kwargs,  # type: Dict[str, Any]
                         ) -> Deferred[Iterable[GetReplicaResult]]:
        req = self._impl.request_builder.build_get_all_replicas_request(key, *opts, **kwargs)
        return self._impl.get_all_replicas_deferred(req)

    def exists(
        self,
        key,  # type: str
        *opts,  # type: ExistsOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[ExistsResult]:
        req = self._impl.request_builder.build_exists_request(key, *opts, **kwargs)
        return self._impl.exists_deferred(req)

    def insert(
        self,
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: InsertOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[MutationResult]:
        req = self._impl.request_builder.build_insert_request(key, value, *opts, **kwargs)
        return self._impl.insert_deferred(req)

    def upsert(
        self,
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: UpsertOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[MutationResult]:
        req = self._impl.request_builder.build_upsert_request(key, value, *opts, **kwargs)
        return self._impl.upsert_deferred(req)

    def replace(self,
                key,  # type: str
                value,  # type: JSONType
                *opts,  # type: ReplaceOptions
                **kwargs,  # type: Dict[str, Any]
                ) -> Deferred[MutationResult]:
        req = self._impl.request_builder.build_replace_request(key, value, *opts, **kwargs)
        return self._impl.replace_deferred(req)

    def remove(self,
               key,  # type: str
               *opts,  # type: RemoveOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> Deferred[MutationResult]:
        req = self._impl.request_builder.build_remove_request(key, *opts, **kwargs)
        return self._impl.remove_deferred(req)

    def touch(self,
              key,  # type: str
              expiry,  # type: timedelta
              *opts,  # type: TouchOptions
              **kwargs,  # type: Dict[str, Any]
              ) -> Deferred[MutationResult]:
        req = self._impl.request_builder.build_touch_request(key, expiry, *opts, **kwargs)
        return self._impl.touch_deferred(req)

    def get_and_touch(self,
                      key,  # type: str
                      expiry,  # type: timedelta
                      *opts,  # type: GetAndTouchOptions
                      **kwargs,  # type: Dict[str, Any]
                      ) -> Deferred[GetResult]:
        req = self._impl.request_builder.build_get_and_touch_request(key, expiry, *opts, **kwargs)
        return self._impl.get_and_touch_deferred(req)

    def get_and_lock(
        self,
        key,  # type: str
        lock_time,  # type: timedelta
        *opts,  # type: GetAndLockOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[GetResult]:
        req = self._impl.request_builder.build_get_and_lock_request(key, lock_time, *opts, **kwargs)
        return self._impl.get_and_lock_deferred(req)

    def unlock(self,
               key,  # type: str
               cas,  # type: int
               *opts,  # type: UnlockOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> Deferred[None]:
        req = self._impl.request_builder.build_unlock_request(key, cas, *opts, **kwargs)
        self._impl.unlock_deferred(req)

    def lookup_in(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: LookupInOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[LookupInResult]:
        req = self._impl.request_builder.build_lookup_in_request(key, spec, *opts, **kwargs)
        return self._impl.lookup_in_deferred(req)

    def lookup_in_any_replica(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: LookupInAnyReplicaOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[LookupInReplicaResult]:
        req = self._impl.request_builder.build_lookup_in_any_replica_request(key, spec, *opts, **kwargs)
        return self._impl.lookup_in_any_replica_deferred(req)

    def lookup_in_all_replicas(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: LookupInAllReplicasOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[Iterable[LookupInReplicaResult]]:
        req = self._impl.request_builder.build_lookup_in_all_replicas_request(key, spec, *opts, **kwargs)
        return self._impl.lookup_in_all_replicas_deferred(req)

    def mutate_in(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: MutateInOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[MutateInResult]:
        req = self._impl.request_builder.build_mutate_in_request(key, spec, *opts, **kwargs)
        return self._impl.mutate_in_deferred(req)

    def binary(self) -> BinaryCollection:
        return BinaryCollection(self._impl)

    @staticmethod
    def default_name() -> str:
        return "_default"


TxCollection = Collection
