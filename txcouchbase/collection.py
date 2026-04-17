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

from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import KeyValueOperationType
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
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_get_request(key, None, *opts, **kwargs)
            d = self._impl.get_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.Get, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_request(key, obs_handler, *opts, **kwargs)
            d = self._impl.get_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_any_replica(self,
                        key,  # type: str
                        *opts,  # type: GetAnyReplicaOptions
                        **kwargs,  # type: Dict[str, Any]
                        ) -> Deferred[GetResult]:
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_get_any_replica_request(key, None, *opts, **kwargs)
            d = self._impl.get_any_replica_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.GetAnyReplica, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_any_replica_request(key, obs_handler, *opts, **kwargs)
            d = self._impl.get_any_replica_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_all_replicas(self,
                         key,  # type: str
                         *opts,  # type: GetAllReplicasOptions
                         **kwargs,  # type: Dict[str, Any]
                         ) -> Deferred[Iterable[GetReplicaResult]]:
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_get_all_replicas_request(key, None, *opts, **kwargs)
            d = self._impl.get_all_replicas_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.GetAllReplicas, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_all_replicas_request(key, obs_handler, *opts, **kwargs)
            d = self._impl.get_all_replicas_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def exists(
        self,
        key,  # type: str
        *opts,  # type: ExistsOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[ExistsResult]:
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_exists_request(key, None, *opts, **kwargs)
            d = self._impl.exists_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.Exists, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_exists_request(key, obs_handler, *opts, **kwargs)
            d = self._impl.exists_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def insert(
        self,
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: InsertOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[MutationResult]:
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_insert_request(key, value, None, *opts, **kwargs)
            d = self._impl.insert_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.Insert, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_insert_request(key, value, obs_handler, *opts, **kwargs)
            d = self._impl.insert_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def upsert(
        self,
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: UpsertOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[MutationResult]:
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_upsert_request(key, value, None, *opts, **kwargs)
            d = self._impl.upsert_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.Upsert, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_upsert_request(key, value, obs_handler, *opts, **kwargs)
            d = self._impl.upsert_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def replace(self,
                key,  # type: str
                value,  # type: JSONType
                *opts,  # type: ReplaceOptions
                **kwargs,  # type: Dict[str, Any]
                ) -> Deferred[MutationResult]:
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_replace_request(key, value, None, *opts, **kwargs)
            d = self._impl.replace_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.Replace, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_replace_request(key, value, obs_handler, *opts, **kwargs)
            d = self._impl.replace_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def remove(self,
               key,  # type: str
               *opts,  # type: RemoveOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> Deferred[MutationResult]:
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_remove_request(key, None, *opts, **kwargs)
            d = self._impl.remove_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.Remove, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_remove_request(key, obs_handler, *opts, **kwargs)
            d = self._impl.remove_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def touch(self,
              key,  # type: str
              expiry,  # type: timedelta
              *opts,  # type: TouchOptions
              **kwargs,  # type: Dict[str, Any]
              ) -> Deferred[MutationResult]:
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_touch_request(key, expiry, None, *opts, **kwargs)
            d = self._impl.touch_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.Touch, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_touch_request(key, expiry, obs_handler, *opts, **kwargs)
            d = self._impl.touch_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_and_touch(self,
                      key,  # type: str
                      expiry,  # type: timedelta
                      *opts,  # type: GetAndTouchOptions
                      **kwargs,  # type: Dict[str, Any]
                      ) -> Deferred[GetResult]:
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_get_and_touch_request(key, expiry, None, *opts, **kwargs)
            d = self._impl.get_and_touch_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.GetAndTouch, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_and_touch_request(key, expiry, obs_handler, *opts, **kwargs)
            d = self._impl.get_and_touch_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_and_lock(
        self,
        key,  # type: str
        lock_time,  # type: timedelta
        *opts,  # type: GetAndLockOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[GetResult]:
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_get_and_lock_request(key, lock_time, None, *opts, **kwargs)
            d = self._impl.get_and_lock_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.GetAndLock, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_and_lock_request(key, lock_time, obs_handler, *opts, **kwargs)
            d = self._impl.get_and_lock_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def unlock(self,
               key,  # type: str
               cas,  # type: int
               *opts,  # type: UnlockOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> Deferred[None]:
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_unlock_request(key, cas, None, *opts, **kwargs)
            d = self._impl.unlock_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.Unlock, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_unlock_request(key, cas, obs_handler, *opts, **kwargs)
            d = self._impl.unlock_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def lookup_in(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: LookupInOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[LookupInResult]:
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_lookup_in_request(key, spec, None, *opts, **kwargs)
            d = self._impl.lookup_in_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.LookupIn, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_lookup_in_request(key, spec, obs_handler, *opts, **kwargs)
            d = self._impl.lookup_in_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def lookup_in_any_replica(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: LookupInAnyReplicaOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[LookupInReplicaResult]:
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_lookup_in_any_replica_request(
                key, spec, None, *opts, **kwargs)
            d = self._impl.lookup_in_any_replica_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.LookupInAnyReplica, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_lookup_in_any_replica_request(
                key, spec, obs_handler, *opts, **kwargs)
            d = self._impl.lookup_in_any_replica_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def lookup_in_all_replicas(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: LookupInAllReplicasOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[Iterable[LookupInReplicaResult]]:
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_lookup_in_all_replicas_request(
                key, spec, None, *opts, **kwargs)
            d = self._impl.lookup_in_all_replicas_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.LookupInAllReplicas, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_lookup_in_all_replicas_request(
                key, spec, obs_handler, *opts, **kwargs)
            d = self._impl.lookup_in_all_replicas_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def mutate_in(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: MutateInOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> Deferred[MutateInResult]:
        instruments = self._impl.observability_instruments
        if instruments.is_noop:
            req = self._impl.request_builder.build_mutate_in_request(key, spec, None, *opts, **kwargs)
            d = self._impl.mutate_in_deferred(req, None)
            d.addBoth(self._impl._finish_span, None)
            return d
        obs_handler = ObservableRequestHandler(KeyValueOperationType.MutateIn, instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_mutate_in_request(key, spec, obs_handler, *opts, **kwargs)
            d = self._impl.mutate_in_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def binary(self) -> BinaryCollection:
        return BinaryCollection(self._impl)

    @staticmethod
    def default_name() -> str:
        return "_default"


TxCollection = Collection
