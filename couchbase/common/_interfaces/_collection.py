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

"""What a protocol's impl must provide to the collection facades.

The deprecated datastructure methods are a capability, `SupportsLegacyDatastructures`, which the facade reaches only
through `require()`.  The SDK imports this module only under `TYPE_CHECKING`.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import (TYPE_CHECKING,
                    Any,
                    Protocol)

if TYPE_CHECKING:
    from datetime import timedelta

    from twisted.internet.defer import Deferred

    from acouchbase.result import AsyncScanResultIterable
    from couchbase.common._capabilities import Capability
    from couchbase.common._interfaces._binary_collection import (AsyncBinaryCollectionInterface,
                                                                 BinaryCollectionInterface,
                                                                 TxBinaryCollectionInterface)
    from couchbase.common._interfaces._query_index_mgmt import AsyncQueryIndexMgmtInterface, QueryIndexMgmtInterface
    from couchbase.common._observability import KeyspaceDetails, ObservabilityInstruments
    from couchbase.common._utils import DocumentValue, JSONType
    from couchbase.common.exceptions import CouchbaseException
    from couchbase.common.kv_range_scan import ScanType
    from couchbase.common.options import (ExistsMultiOptions,
                                          ExistsOptions,
                                          GetAllReplicasMultiOptions,
                                          GetAllReplicasOptions,
                                          GetAndLockMultiOptions,
                                          GetAndLockOptions,
                                          GetAndTouchOptions,
                                          GetAnyReplicaMultiOptions,
                                          GetAnyReplicaOptions,
                                          GetMultiOptions,
                                          GetOptions,
                                          GetReplicaOptions,
                                          InsertMultiOptions,
                                          InsertOptions,
                                          LookupInAllReplicasOptions,
                                          LookupInAnyReplicaOptions,
                                          LookupInOptions,
                                          MutateInOptions,
                                          RemoveMultiOptions,
                                          RemoveOptions,
                                          ReplaceMultiOptions,
                                          ReplaceOptions,
                                          ScanOptions,
                                          TouchMultiOptions,
                                          TouchOptions,
                                          UnlockMultiOptions,
                                          UnlockOptions,
                                          UpsertMultiOptions,
                                          UpsertOptions)
    from couchbase.common.replica_reads import GetReplicaStrategy
    from couchbase.common.result import (ExistsResult,
                                         GetReplicaResult,
                                         GetResult,
                                         LookupInReplicaResult,
                                         LookupInResult,
                                         MultiExistsResult,
                                         MultiGetReplicaResult,
                                         MultiGetResult,
                                         MultiMutationResult,
                                         MultiResultType,
                                         MutateInResult,
                                         MutationResult,
                                         OperationResult)
    from couchbase.common.subdocument import Spec
    from couchbase.result import ScanResultIterable


class CollectionInterface(Protocol):
    @property
    def capabilities(self) -> frozenset[Capability]: ...

    @property
    def name(self) -> str: ...

    @property
    def bucket_name(self) -> str: ...

    @property
    def scope_name(self) -> str: ...

    @property
    def observability_instruments(self) -> ObservabilityInstruments: ...

    @property
    def keyspace(self) -> KeyspaceDetails: ...

    def get(self,
            key: str,
            opts: GetOptions | None = None,
            **kwargs: Any) -> GetResult: ...

    def get_any_replica(self,
                        key: str,
                        opts: GetAnyReplicaOptions | None = None,
                        **kwargs: Any) -> GetReplicaResult: ...

    def get_replica(self,
                    key: str,
                    strategy: GetReplicaStrategy,
                    opts: GetReplicaOptions | None = None,
                    **kwargs: Any) -> GetReplicaResult: ...

    def get_all_replicas(self,
                         key: str,
                         opts: GetAllReplicasOptions | None = None,
                         **kwargs: Any) -> Iterable[GetReplicaResult]: ...

    def exists(self,
               key: str,
               opts: ExistsOptions | None = None,
               **kwargs: Any) -> ExistsResult: ...

    def insert(self,
               key: str,
               value: DocumentValue,
               opts: InsertOptions | None = None,
               **kwargs: Any) -> MutationResult: ...

    def upsert(self,
               key: str,
               value: DocumentValue,
               opts: UpsertOptions | None = None,
               **kwargs: Any) -> MutationResult: ...

    def replace(self,
                key: str,
                value: DocumentValue,
                opts: ReplaceOptions | None = None,
                **kwargs: Any) -> MutationResult: ...

    def remove(self,
               key: str,
               opts: RemoveOptions | None = None,
               **kwargs: Any) -> MutationResult: ...

    def touch(self,
              key: str,
              expiry: timedelta,
              opts: TouchOptions | None = None,
              **kwargs: Any) -> MutationResult: ...

    def get_and_touch(self,
                      key: str,
                      expiry: timedelta,
                      opts: GetAndTouchOptions | None = None,
                      **kwargs: Any) -> GetResult: ...

    def get_and_lock(self,
                     key: str,
                     lock_time: timedelta,
                     opts: GetAndLockOptions | None = None,
                     **kwargs: Any) -> GetResult: ...

    def unlock(self,
               key: str,
               cas: int,
               opts: UnlockOptions | None = None,
               **kwargs: Any) -> None: ...

    def lookup_in(self,
                  key: str,
                  spec: Iterable[Spec],
                  opts: LookupInOptions | None = None,
                  **kwargs: Any) -> LookupInResult: ...

    def lookup_in_any_replica(self,
                              key: str,
                              spec: Iterable[Spec],
                              opts: LookupInAnyReplicaOptions | None = None,
                              **kwargs: Any) -> LookupInReplicaResult: ...

    def lookup_in_all_replicas(self,
                               key: str,
                               spec: Iterable[Spec],
                               opts: LookupInAllReplicasOptions | None = None,
                               **kwargs: Any) -> Iterable[LookupInReplicaResult]: ...

    def mutate_in(self,
                  key: str,
                  spec: Iterable[Spec],
                  opts: MutateInOptions | None = None,
                  **kwargs: Any) -> MutateInResult: ...

    def get_multi(self,
                  keys: list[str],
                  opts: GetMultiOptions | None = None,
                  **kwargs: Any) -> MultiGetResult: ...

    def get_any_replica_multi(self,
                              keys: list[str],
                              opts: GetAnyReplicaMultiOptions | None = None,
                              **kwargs: Any) -> MultiGetReplicaResult[GetReplicaResult]: ...

    def get_all_replicas_multi(self,
                               keys: list[str],
                               opts: GetAllReplicasMultiOptions | None = None,
                               **kwargs: Any) -> MultiGetReplicaResult[list[GetReplicaResult]]: ...

    def get_and_lock_multi(self,
                           keys: list[str],
                           lock_time: timedelta,
                           opts: GetAndLockMultiOptions | None = None,
                           **kwargs: Any) -> MultiGetResult: ...

    def exists_multi(self,
                     keys: list[str],
                     opts: ExistsMultiOptions | None = None,
                     **kwargs: Any) -> MultiExistsResult: ...

    def insert_multi(self,
                     keys_and_docs: Mapping[str, DocumentValue],
                     opts: InsertMultiOptions | None = None,
                     **kwargs: Any) -> MultiMutationResult: ...

    def upsert_multi(self,
                     keys_and_docs: Mapping[str, DocumentValue],
                     opts: UpsertMultiOptions | None = None,
                     **kwargs: Any) -> MultiMutationResult: ...

    def replace_multi(self,
                      keys_and_docs: Mapping[str, DocumentValue],
                      opts: ReplaceMultiOptions | None = None,
                      **kwargs: Any) -> MultiMutationResult: ...

    def remove_multi(self,
                     keys: list[str],
                     opts: RemoveMultiOptions | None = None,
                     **kwargs: Any) -> MultiMutationResult: ...

    def touch_multi(self,
                    keys: list[str],
                    expiry: timedelta,
                    opts: TouchMultiOptions | None = None,
                    **kwargs: Any) -> MultiMutationResult: ...

    def unlock_multi(self,
                     keys: MultiResultType | dict[str, int],
                     opts: UnlockMultiOptions | None = None,
                     **kwargs: Any) -> dict[str, CouchbaseException | None]: ...

    def scan(self, scan_type: ScanType, opts: ScanOptions | None = None, **kwargs: Any) -> ScanResultIterable: ...

    def binary(self) -> BinaryCollectionInterface: ...

    def query_index_mgmt(self) -> QueryIndexMgmtInterface: ...


class AsyncCollectionInterface(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def bucket_name(self) -> str: ...

    @property
    def scope_name(self) -> str: ...

    @property
    def observability_instruments(self) -> ObservabilityInstruments: ...

    @property
    def keyspace(self) -> KeyspaceDetails: ...

    async def get(self,
                  key: str,
                  opts: GetOptions | None = None,
                  **kwargs: Any) -> GetResult: ...

    async def get_any_replica(self,
                              key: str,
                              opts: GetAnyReplicaOptions | None = None,
                              **kwargs: Any) -> GetReplicaResult: ...

    async def get_replica(self,
                          key: str,
                          strategy: GetReplicaStrategy,
                          opts: GetReplicaOptions | None = None,
                          **kwargs: Any) -> GetReplicaResult: ...

    async def get_all_replicas(self,
                               key: str,
                               opts: GetAllReplicasOptions | None = None,
                               **kwargs: Any) -> Iterable[GetReplicaResult]: ...

    async def exists(self,
                     key: str,
                     opts: ExistsOptions | None = None,
                     **kwargs: Any) -> ExistsResult: ...

    async def insert(self,
                     key: str,
                     value: DocumentValue,
                     opts: InsertOptions | None = None,
                     **kwargs: Any) -> MutationResult: ...

    async def upsert(self,
                     key: str,
                     value: DocumentValue,
                     opts: UpsertOptions | None = None,
                     **kwargs: Any) -> MutationResult: ...

    async def replace(self,
                      key: str,
                      value: DocumentValue,
                      opts: ReplaceOptions | None = None,
                      **kwargs: Any) -> MutationResult: ...

    async def remove(self,
                     key: str,
                     opts: RemoveOptions | None = None,
                     **kwargs: Any) -> MutationResult: ...

    async def touch(self,
                    key: str,
                    expiry: timedelta,
                    opts: TouchOptions | None = None,
                    **kwargs: Any) -> MutationResult: ...

    async def get_and_touch(self,
                            key: str,
                            expiry: timedelta,
                            opts: GetAndTouchOptions | None = None,
                            **kwargs: Any) -> GetResult: ...

    async def get_and_lock(self,
                           key: str,
                           lock_time: timedelta,
                           opts: GetAndLockOptions | None = None,
                           **kwargs: Any) -> GetResult: ...

    async def unlock(self,
                     key: str,
                     cas: int,
                     opts: UnlockOptions | None = None,
                     **kwargs: Any) -> None: ...

    async def lookup_in(self,
                        key: str,
                        spec: Iterable[Spec],
                        opts: LookupInOptions | None = None,
                        **kwargs: Any) -> LookupInResult: ...

    async def lookup_in_any_replica(self,
                                    key: str,
                                    spec: Iterable[Spec],
                                    opts: LookupInAnyReplicaOptions | None = None,
                                    **kwargs: Any) -> LookupInReplicaResult: ...

    async def lookup_in_all_replicas(self,
                                     key: str,
                                     spec: Iterable[Spec],
                                     opts: LookupInAllReplicasOptions | None = None,
                                     **kwargs: Any) -> Iterable[LookupInReplicaResult]: ...

    async def mutate_in(self,
                        key: str,
                        spec: Iterable[Spec],
                        opts: MutateInOptions | None = None,
                        **kwargs: Any) -> MutateInResult: ...

    def scan(self,
             scan_type: ScanType,
             opts: ScanOptions | None = None,
             **kwargs: Any) -> AsyncScanResultIterable: ...

    def binary(self) -> AsyncBinaryCollectionInterface: ...

    def query_index_mgmt(self) -> AsyncQueryIndexMgmtInterface: ...


class TxCollectionInterface(Protocol):
    @property
    def name(self) -> str: ...

    def get_deferred(self,
                     key: str,
                     opts: GetOptions | None = None,
                     **kwargs: Any) -> Deferred[GetResult]: ...

    def get_any_replica_deferred(self,
                                 key: str,
                                 opts: GetAnyReplicaOptions | None = None,
                                 **kwargs: Any) -> Deferred[GetReplicaResult]: ...

    def get_replica_deferred(self,
                             key: str,
                             strategy: GetReplicaStrategy,
                             opts: GetReplicaOptions | None = None,
                             **kwargs: Any) -> Deferred[GetReplicaResult]: ...

    def get_all_replicas_deferred(self,
                                  key: str,
                                  opts: GetAllReplicasOptions | None = None,
                                  **kwargs: Any) -> Deferred[Iterable[GetReplicaResult]]: ...

    def exists_deferred(self,
                        key: str,
                        opts: ExistsOptions | None = None,
                        **kwargs: Any) -> Deferred[ExistsResult]: ...

    def insert_deferred(self,
                        key: str,
                        value: DocumentValue,
                        opts: InsertOptions | None = None,
                        **kwargs: Any) -> Deferred[MutationResult]: ...

    def upsert_deferred(self,
                        key: str,
                        value: DocumentValue,
                        opts: UpsertOptions | None = None,
                        **kwargs: Any) -> Deferred[MutationResult]: ...

    def replace_deferred(self,
                         key: str,
                         value: DocumentValue,
                         opts: ReplaceOptions | None = None,
                         **kwargs: Any) -> Deferred[MutationResult]: ...

    def remove_deferred(self,
                        key: str,
                        opts: RemoveOptions | None = None,
                        **kwargs: Any) -> Deferred[MutationResult]: ...

    def touch_deferred(self,
                       key: str,
                       expiry: timedelta,
                       opts: TouchOptions | None = None,
                       **kwargs: Any) -> Deferred[MutationResult]: ...

    def get_and_touch_deferred(self,
                               key: str,
                               expiry: timedelta,
                               opts: GetAndTouchOptions | None = None,
                               **kwargs: Any) -> Deferred[GetResult]: ...

    def get_and_lock_deferred(self,
                              key: str,
                              lock_time: timedelta,
                              opts: GetAndLockOptions | None = None,
                              **kwargs: Any) -> Deferred[GetResult]: ...

    def unlock_deferred(self,
                        key: str,
                        cas: int,
                        opts: UnlockOptions | None = None,
                        **kwargs: Any) -> Deferred[None]: ...

    def lookup_in_deferred(self,
                           key: str,
                           spec: Iterable[Spec],
                           opts: LookupInOptions | None = None,
                           **kwargs: Any) -> Deferred[LookupInResult]: ...

    def lookup_in_any_replica_deferred(self,
                                       key: str,
                                       spec: Iterable[Spec],
                                       opts: LookupInAnyReplicaOptions | None = None,
                                       **kwargs: Any) -> Deferred[LookupInReplicaResult]: ...

    def lookup_in_all_replicas_deferred(self,
                                        key: str,
                                        spec: Iterable[Spec],
                                        opts: LookupInAllReplicasOptions | None = None,
                                        **kwargs: Any) -> Deferred[Iterable[LookupInReplicaResult]]: ...

    def mutate_in_deferred(self,
                           key: str,
                           spec: Iterable[Spec],
                           opts: MutateInOptions | None = None,
                           **kwargs: Any) -> Deferred[MutateInResult]: ...

    def binary(self) -> TxBinaryCollectionInterface: ...


class SupportsLegacyDatastructures(Protocol):
    @property
    def legacy_datastructures(self) -> LegacyDatastructuresInterface: ...


class LegacyDatastructuresInterface(Protocol):
    def list_append(self, key: str, value: JSONType, create: bool | None = False, **kwargs: object
                    ) -> OperationResult: ...

    def list_prepend(self, key: str, value: JSONType, create: bool | None = False, **kwargs: object
                     ) -> OperationResult: ...

    def list_set(self, key: str, index: int, value: JSONType, **kwargs: object) -> OperationResult: ...

    def list_get(self, key: str, index: int, **kwargs: object) -> Any: ...

    def list_remove(self, key: str, index: int, **kwargs: object) -> OperationResult: ...

    def list_size(self, key: str, **kwargs: object) -> int: ...

    def map_add(self, key: str, mapkey: str, value: Any, create: bool | None = False, **kwargs: object
                ) -> OperationResult: ...

    def map_get(self, key: str, mapkey: str, **kwargs: object) -> Any: ...

    def map_remove(self, key: str, mapkey: str, **kwargs: object) -> OperationResult: ...

    def map_size(self, key: str, **kwargs: object) -> int: ...

    def set_add(self, key: str, value: Any, create: bool | None = False, **kwargs: object
                ) -> OperationResult | None: ...

    def set_remove(self, key: str, value: Any, **kwargs: object) -> OperationResult | None: ...

    def set_size(self, key: str, **kwargs: object) -> int: ...

    def set_contains(self, key: str, value: Any, **kwargs: object) -> bool: ...

    def queue_push(self, key: str, value: Any, create: bool | None = False, **kwargs: object
                   ) -> OperationResult: ...

    def queue_pop(self, key: str, **kwargs: object) -> Any: ...

    def queue_size(self, key: str) -> int: ...
