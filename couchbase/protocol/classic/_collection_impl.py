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

from collections.abc import (Callable,
                             Iterable,
                             Iterator,
                             Mapping)
from typing import (TYPE_CHECKING,
                    Any,
                    TypeVar)

from couchbase.common._observability import (KeyspaceDetails,
                                             ObservabilityInstruments,
                                             ObservableRequestHandler)
from couchbase.common._operations import Op
from couchbase.common._streaming import RowIterator
from couchbase.common.exceptions import CouchbaseException, InvalidArgumentException
from couchbase.common.result import (CounterResult,
                                     ExistsResult,
                                     GetReplicaResult,
                                     GetResult,
                                     LookupInReplicaResult,
                                     LookupInResult,
                                     MultiCounterResult,
                                     MultiExistsResult,
                                     MultiGetReplicaResult,
                                     MultiGetResult,
                                     MultiMutationResult,
                                     MutateInResult,
                                     MutationResult)
from couchbase.protocol.classic._capabilities import CAPABILITIES
from couchbase.protocol.classic._collection_multi_req_builder import CollectionMultiRequestBuilder
from couchbase.protocol.classic._collection_req_builder import CollectionRequestBuilder
from couchbase.protocol.classic._collection_types import CollectionDetails
from couchbase.protocol.classic._error_mapping import ErrorMapper
from couchbase.protocol.classic._legacy_datastructures import LegacyDatastructures
from couchbase.protocol.classic._management._query_index_mgmt_impl import QueryIndexMgmtImpl
from couchbase.protocol.classic._pycbc_core import pycbc_exception as PycbcCoreException
from couchbase.protocol.classic._pycbc_core import pycbc_kv_request as PycbcCoreKeyValueRequest
from couchbase.protocol.classic._range_scan import ClassicRangeScanExecutor
from couchbase.protocol.classic._result import (_counter_from_core,
                                                _exists_from_core,
                                                _get_from_core,
                                                _get_replica_from_core,
                                                _lookup_in_from_core,
                                                _lookup_in_replica_from_core,
                                                _mutate_in_from_core,
                                                _mutation_from_core)
from couchbase.result import ScanResultIterable
from couchbase.serializer import Serializer
from couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from datetime import timedelta

    from couchbase.common._utils import DocumentValue
    from couchbase.common.kv_range_scan import ScanType
    from couchbase.common.options import (AppendMultiOptions,
                                          AppendOptions,
                                          DecrementMultiOptions,
                                          DecrementOptions,
                                          ExistsMultiOptions,
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
                                          IncrementMultiOptions,
                                          IncrementOptions,
                                          InsertMultiOptions,
                                          InsertOptions,
                                          LookupInAllReplicasOptions,
                                          LookupInAnyReplicaOptions,
                                          LookupInOptions,
                                          MutateInOptions,
                                          PrependMultiOptions,
                                          PrependOptions,
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
    from couchbase.common.protocol import CouchbaseProtocol
    from couchbase.common.result import MultiResultType
    from couchbase.common.subdocument import Spec
    from couchbase.protocol.classic._client_adapter import ClientAdapter
    from couchbase.protocol.classic._pycbc_core import pycbc_connection, pycbc_result
    from couchbase.protocol.classic._scope_impl import ScopeImpl


def _resolve_sub_op_error(value: Any,
                          obs_handler: ObservableRequestHandler | None,
                          return_exceptions: bool) -> CouchbaseException:
    exc = ErrorMapper.build_exception(value) if isinstance(value, PycbcCoreException) else value
    if obs_handler:
        obs_handler.process_multi_sub_op(value, exc_val=exc)
    if not return_exceptions:
        raise exc
    return exc


ResultT = TypeVar('ResultT')


def decode_replicas(stream: Iterable[Any], build: Callable[[Any], ResultT]) -> Iterator[ResultT]:
    """Build each replica from the binding's stream, which holds every entry and then a None."""
    for res in stream:
        if isinstance(res, PycbcCoreException):
            raise ErrorMapper.build_exception(res)
        if res is None:
            return
        yield build(res)


def _parse_sub_ops(ret: pycbc_result,
                   obs_handler: ObservableRequestHandler | None,
                   return_exceptions: bool,
                   build: Callable[[str, Any], ResultT]
                   ) -> tuple[dict[str, ResultT | CouchbaseException], bool]:
    """Record, map and build every key of a multi operation in one pass, in key order.

    A multi operation reports one outcome per key, so the core hands its errors back as values
    rather than raising. Each becomes its ``CouchbaseException`` here and each success is built
    with ``build``; with ``return_exceptions`` false the first error is raised, after every key
    before it has been recorded.
    """
    all_ok = ret.multi_resp_all_okay is True
    results: dict[str, ResultT | CouchbaseException] = {}
    for k, v in ret.raw_result.items():
        # pycbc_result and pycbc_exception have a core_span member
        if obs_handler and hasattr(v, 'core_span'):
            obs_handler.process_core_span(v.core_span)
        if isinstance(v, (CouchbaseException, PycbcCoreException)):
            results[k] = _resolve_sub_op_error(v, obs_handler, return_exceptions)
            continue
        if obs_handler:
            obs_handler.process_multi_sub_op(v)
        results[k] = build(k, v)
    return results, all_ok


class CollectionImpl:

    capabilities = CAPABILITIES

    def __init__(self, collection_name: str, scope_impl: ScopeImpl) -> None:
        self._scope_impl = scope_impl
        self._client_adapter: ClientAdapter = scope_impl._client_adapter
        cluster_settings = scope_impl.cluster_settings
        self._collection_details = CollectionDetails(scope_impl.bucket_name,
                                                     scope_impl.name,
                                                     collection_name,
                                                     cluster_settings.default_transcoder,
                                                     default_serializer=cluster_settings.default_serializer,
                                                     option_validation=cluster_settings.option_validation,
                                                     kv_durable_timeout=scope_impl.kv_durable_timeout)
        self._multi_request_builder = CollectionMultiRequestBuilder(self._collection_details)
        self._request_builder = CollectionRequestBuilder(self._collection_details)
        self._legacy_datastructures = LegacyDatastructures(self)

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
    def connection(self) -> pycbc_connection:
        """
        **INTERNAL**
        """
        return self._client_adapter.connection

    @property
    def legacy_datastructures(self) -> LegacyDatastructures:
        return self._legacy_datastructures

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
        """**INTERNAL**"""
        return self._scope_impl.observability_instruments

    @property
    def keyspace(self) -> KeyspaceDetails:
        return self._collection_details.get_details_as_dict()

    def get(self,
            key: str,
            opts: GetOptions | None = None,
            **kwargs: Any) -> GetResult:
        with ObservableRequestHandler.create(Op.Get, self.observability_instruments) as obs_handler:
            req, transcoder = self._request_builder.build_get_request(key, obs_handler, opts, **kwargs)
            return self._get(req, transcoder, obs_handler)

    def get_any_replica(self,
                        key: str,
                        opts: GetAnyReplicaOptions | None = None,
                        **kwargs: Any) -> GetReplicaResult:
        with ObservableRequestHandler.create(Op.GetAnyReplica, self.observability_instruments) as obs_handler:
            req, transcoder = self._request_builder.build_get_any_replica_request(key, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
            return GetReplicaResult(_get_replica_from_core(ret.raw_result), transcoder=transcoder, key=req.key)

    def get_all_replicas(self,
                         key: str,
                         opts: GetAllReplicasOptions | None = None,
                         **kwargs: Any) -> Iterable[GetReplicaResult]:
        with ObservableRequestHandler.create(Op.GetAllReplicas, self.observability_instruments) as obs_handler:
            req, transcoder = self._request_builder.build_get_all_replicas_request(key, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
            return decode_replicas(ret, lambda res: GetReplicaResult(_get_replica_from_core(res.raw_result),
                                                                     transcoder=transcoder,
                                                                     key=req.key))

    def exists(self,
               key: str,
               opts: ExistsOptions | None = None,
               **kwargs: Any) -> ExistsResult:
        with ObservableRequestHandler.create(Op.Exists, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_exists_request(key, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
            return ExistsResult(_exists_from_core(ret.raw_result), key=req.key)

    def insert(self,
               key: str,
               value: DocumentValue,
               opts: InsertOptions | None = None,
               **kwargs: Any) -> MutationResult:
        with ObservableRequestHandler.create(Op.Insert, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_insert_request(key, value, obs_handler, opts, **kwargs)
            return self._insert(req, obs_handler)

    def upsert(self,
               key: str,
               value: DocumentValue,
               opts: UpsertOptions | None = None,
               **kwargs: Any) -> MutationResult:
        with ObservableRequestHandler.create(Op.Upsert, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_upsert_request(key, value, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
            return MutationResult(_mutation_from_core(ret.raw_result), key=req.key)

    def replace(self,
                key: str,
                value: DocumentValue,
                opts: ReplaceOptions | None = None,
                **kwargs: Any) -> MutationResult:
        with ObservableRequestHandler.create(Op.Replace, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_replace_request(key, value, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
            return MutationResult(_mutation_from_core(ret.raw_result), key=req.key)

    def remove(self,
               key: str,
               opts: RemoveOptions | None = None,
               **kwargs: Any) -> MutationResult:
        with ObservableRequestHandler.create(Op.Remove, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_remove_request(key, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
            return MutationResult(_mutation_from_core(ret.raw_result), key=req.key)

    def touch(self,
              key: str,
              expiry: timedelta,
              opts: TouchOptions | None = None,
              **kwargs: Any) -> MutationResult:
        with ObservableRequestHandler.create(Op.Touch, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_touch_request(key, expiry, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
            return MutationResult(_mutation_from_core(ret.raw_result), key=req.key)

    def get_and_touch(self,
                      key: str,
                      expiry: timedelta,
                      opts: GetAndTouchOptions | None = None,
                      **kwargs: Any) -> GetResult:
        with ObservableRequestHandler.create(Op.GetAndTouch, self.observability_instruments) as obs_handler:
            req, transcoder = self._request_builder.build_get_and_touch_request(key,
                                                                                expiry,
                                                                                obs_handler,
                                                                                opts,
                                                                                **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
            return GetResult(_get_from_core(ret.raw_result), transcoder=transcoder, key=req.key)

    def get_and_lock(self,
                     key: str,
                     lock_time: timedelta,
                     opts: GetAndLockOptions | None = None,
                     **kwargs: Any) -> GetResult:
        with ObservableRequestHandler.create(Op.GetAndLock, self.observability_instruments) as obs_handler:
            req, transcoder = self._request_builder.build_get_and_lock_request(key,
                                                                               lock_time,
                                                                               obs_handler,
                                                                               opts,
                                                                               **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
            return GetResult(_get_from_core(ret.raw_result), transcoder=transcoder, key=req.key)

    def unlock(self,
               key: str,
               cas: int,
               opts: UnlockOptions | None = None,
               **kwargs: Any) -> None:
        with ObservableRequestHandler.create(Op.Unlock, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_unlock_request(key, cas, obs_handler, opts, **kwargs)
            self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)

    def lookup_in(self,
                  key: str,
                  spec: Iterable[Spec],
                  opts: LookupInOptions | None = None,
                  **kwargs: Any) -> LookupInResult:
        with ObservableRequestHandler.create(Op.LookupIn, self.observability_instruments) as obs_handler:
            req, serializer = self._request_builder.build_lookup_in_request(key, spec, obs_handler, opts, **kwargs)
            return self._lookup_in(req, serializer, obs_handler)

    def lookup_in_any_replica(self,
                              key: str,
                              spec: Iterable[Spec],
                              opts: LookupInAnyReplicaOptions | None = None,
                              **kwargs: Any) -> LookupInReplicaResult:
        with ObservableRequestHandler.create(Op.LookupInAnyReplica, self.observability_instruments) as obs_handler:
            req, serializer = self._request_builder.build_lookup_in_any_replica_request(key,
                                                                                        spec,
                                                                                        obs_handler,
                                                                                        opts,
                                                                                        **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
            return LookupInReplicaResult(_lookup_in_replica_from_core(ret.raw_result, req.key),
                                         serializer=serializer,
                                         key=req.key)

    def lookup_in_all_replicas(self,
                               key: str,
                               spec: Iterable[Spec],
                               opts: LookupInAllReplicasOptions | None = None,
                               **kwargs: Any) -> Iterable[LookupInReplicaResult]:
        with ObservableRequestHandler.create(Op.LookupInAllReplicas, self.observability_instruments) as obs_handler:
            req, serializer = self._request_builder.build_lookup_in_all_replicas_request(key,
                                                                                         spec,
                                                                                         obs_handler,
                                                                                         opts,
                                                                                         **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
            return decode_replicas(ret, lambda res: LookupInReplicaResult(
                _lookup_in_replica_from_core(res.raw_result, req.key), serializer=serializer, key=req.key))

    def mutate_in(self,
                  key: str,
                  spec: Iterable[Spec],
                  opts: MutateInOptions | None = None,
                  **kwargs: Any) -> MutateInResult:
        with ObservableRequestHandler.create(Op.MutateIn, self.observability_instruments) as obs_handler:
            req, serializer = self._request_builder.build_mutate_in_request(key, spec, obs_handler, opts, **kwargs)
            return self._mutate_in(req, serializer, obs_handler)

    def get_multi(self,
                  keys: list[str],
                  opts: GetMultiOptions | None = None,
                  **kwargs: Any) -> MultiGetResult:
        with ObservableRequestHandler.create(Op.GetMulti, self.observability_instruments) as obs_handler:
            req = self._multi_request_builder.build_get_multi_request(keys, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
            results, all_ok = _parse_sub_ops(ret, obs_handler, req.return_exceptions,
                                             lambda k, v: GetResult(_get_from_core(v.raw_result),
                                                                    transcoder=req.key_transcoders[k],
                                                                    key=k))
            return MultiGetResult(results, all_ok)

    def get_any_replica_multi(self,
                              keys: list[str],
                              opts: GetAnyReplicaMultiOptions | None = None,
                              **kwargs: Any) -> MultiGetReplicaResult[GetReplicaResult]:
        with ObservableRequestHandler.create(Op.GetAnyReplicaMulti, self.observability_instruments) as obs_handler:
            req = self._multi_request_builder.build_get_any_replica_multi_request(keys, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
            results, all_ok = _parse_sub_ops(ret, obs_handler, req.return_exceptions,
                                             lambda k, v: GetReplicaResult(_get_replica_from_core(v.raw_result),
                                                                           transcoder=req.key_transcoders[k],
                                                                           key=k))
            return MultiGetReplicaResult(results, all_ok)

    def get_all_replicas_multi(self,
                               keys: list[str],
                               opts: GetAllReplicasMultiOptions | None = None,
                               **kwargs: Any) -> MultiGetReplicaResult[list[GetReplicaResult]]:
        with ObservableRequestHandler.create(Op.GetAllReplicasMulti, self.observability_instruments) as obs_handler:
            req = self._multi_request_builder.build_get_all_replicas_multi_request(keys, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)

            # every successful key holds a stream of replicas, decoded below into a list[GetReplicaResult]
            all_ok = ret.multi_resp_all_okay is True
            failed = {}
            streams = {}
            for k, v in ret.raw_result.items():
                # pycbc_streamed_result and pycbc_exception have a core_span member
                if obs_handler and hasattr(v, 'core_span'):
                    obs_handler.process_core_span(v.core_span)
                if isinstance(v, PycbcCoreException):
                    failed[k] = v
                    continue
                # the start and end time are on the pycbc_streamed_result, so metrics for a successful key
                # have to be recorded before its replicas are decoded
                if obs_handler:
                    obs_handler.process_multi_sub_op(v)
                streams[k] = v

            def replicas(key: str, stream: Any) -> list[GetReplicaResult]:
                transcoder = req.key_transcoders[key]
                return list(decode_replicas(stream, lambda res: GetReplicaResult(
                    _get_replica_from_core(res.raw_result), transcoder=transcoder, key=key)))

            decoded = {k: replicas(k, v) for k, v in streams.items()}
            # A failed key is resolved only once every successful key has decoded, so an error inside a
            # replica stream is raised ahead of a per-key one.
            results: dict[str, list[GetReplicaResult] | CouchbaseException] = {
                k: _resolve_sub_op_error(v, obs_handler, req.return_exceptions) for k, v in failed.items()}
            results.update(decoded)
            return MultiGetReplicaResult(results, all_ok)

    def get_and_lock_multi(self,
                           keys: list[str],
                           lock_time: timedelta,
                           opts: GetAndLockMultiOptions | None = None,
                           **kwargs: Any) -> MultiGetResult:
        with ObservableRequestHandler.create(Op.GetAndLockMulti, self.observability_instruments) as obs_handler:
            req = self._multi_request_builder.build_get_and_lock_multi_request(keys,
                                                                               lock_time,
                                                                               obs_handler,
                                                                               opts,
                                                                               **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
            results, all_ok = _parse_sub_ops(ret, obs_handler, req.return_exceptions,
                                             lambda k, v: GetResult(_get_from_core(v.raw_result),
                                                                    transcoder=req.key_transcoders[k],
                                                                    key=k))
            return MultiGetResult(results, all_ok)

    def exists_multi(self,
                     keys: list[str],
                     opts: ExistsMultiOptions | None = None,
                     **kwargs: Any) -> MultiExistsResult:
        with ObservableRequestHandler.create(Op.ExistsMulti, self.observability_instruments) as obs_handler:
            req = self._multi_request_builder.build_exists_multi_request(keys, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
            results, all_ok = _parse_sub_ops(ret, obs_handler, req.return_exceptions,
                                             lambda k, v: ExistsResult(_exists_from_core(v.raw_result), key=k))
            return MultiExistsResult(results, all_ok)

    def insert_multi(self,
                     keys_and_docs: Mapping[str, DocumentValue],
                     opts: InsertMultiOptions | None = None,
                     **kwargs: Any) -> MultiMutationResult:
        with ObservableRequestHandler.create(Op.InsertMulti, self.observability_instruments) as obs_handler:
            req = self._multi_request_builder.build_insert_multi_request(keys_and_docs, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
            results, all_ok = _parse_sub_ops(ret, obs_handler, req.return_exceptions,
                                             lambda k, v: MutationResult(_mutation_from_core(v.raw_result), key=k))
            return MultiMutationResult(results, all_ok)

    def upsert_multi(self,
                     keys_and_docs: Mapping[str, DocumentValue],
                     opts: UpsertMultiOptions | None = None,
                     **kwargs: Any) -> MultiMutationResult:
        with ObservableRequestHandler.create(Op.UpsertMulti, self.observability_instruments) as obs_handler:
            req = self._multi_request_builder.build_upsert_multi_request(keys_and_docs, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
            results, all_ok = _parse_sub_ops(ret, obs_handler, req.return_exceptions,
                                             lambda k, v: MutationResult(_mutation_from_core(v.raw_result), key=k))
            return MultiMutationResult(results, all_ok)

    def replace_multi(self,
                      keys_and_docs: Mapping[str, DocumentValue],
                      opts: ReplaceMultiOptions | None = None,
                      **kwargs: Any) -> MultiMutationResult:
        with ObservableRequestHandler.create(Op.ReplaceMulti, self.observability_instruments) as obs_handler:
            req = self._multi_request_builder.build_replace_multi_request(keys_and_docs, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
            results, all_ok = _parse_sub_ops(ret, obs_handler, req.return_exceptions,
                                             lambda k, v: MutationResult(_mutation_from_core(v.raw_result), key=k))
            return MultiMutationResult(results, all_ok)

    def remove_multi(self,
                     keys: list[str],
                     opts: RemoveMultiOptions | None = None,
                     **kwargs: Any) -> MultiMutationResult:
        with ObservableRequestHandler.create(Op.RemoveMulti, self.observability_instruments) as obs_handler:
            req = self._multi_request_builder.build_remove_multi_request(keys, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
            results, all_ok = _parse_sub_ops(ret, obs_handler, req.return_exceptions,
                                             lambda k, v: MutationResult(_mutation_from_core(v.raw_result), key=k))
            return MultiMutationResult(results, all_ok)

    def touch_multi(self,
                    keys: list[str],
                    expiry: timedelta,
                    opts: TouchMultiOptions | None = None,
                    **kwargs: Any) -> MultiMutationResult:
        with ObservableRequestHandler.create(Op.TouchMulti, self.observability_instruments) as obs_handler:
            req = self._multi_request_builder.build_touch_multi_request(keys, expiry, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
            results, all_ok = _parse_sub_ops(ret, obs_handler, req.return_exceptions,
                                             lambda k, v: MutationResult(_mutation_from_core(v.raw_result), key=k))
            return MultiMutationResult(results, all_ok)

    def unlock_multi(self,
                     keys: MultiResultType | dict[str, int],
                     opts: UnlockMultiOptions | None = None,
                     **kwargs: Any) -> dict[str, CouchbaseException | None]:
        with ObservableRequestHandler.create(Op.UnlockMulti, self.observability_instruments) as obs_handler:
            req = self._multi_request_builder.build_unlock_multi_request(keys, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
            results, _ = _parse_sub_ops(ret, obs_handler, req.return_exceptions, lambda k, v: None)
            return results

    def increment(self,
                  key: str,
                  opts: IncrementOptions | None = None,
                  **kwargs: Any) -> CounterResult:
        with ObservableRequestHandler.create(Op.Increment, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_increment_request(key, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
            return CounterResult(_counter_from_core(ret.raw_result), key=req.key)

    def decrement(self,
                  key: str,
                  opts: DecrementOptions | None = None,
                  **kwargs: Any) -> CounterResult:
        with ObservableRequestHandler.create(Op.Decrement, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_decrement_request(key, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
            return CounterResult(_counter_from_core(ret.raw_result), key=req.key)

    def append(self,
               key: str,
               value: str | bytes | bytearray,
               opts: AppendOptions | None = None,
               **kwargs: Any) -> MutationResult:
        with ObservableRequestHandler.create(Op.Append, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_append_request(key, value, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
            return MutationResult(_mutation_from_core(ret.raw_result), key=req.key)

    def prepend(self,
                key: str,
                value: str | bytes | bytearray,
                opts: PrependOptions | None = None,
                **kwargs: Any) -> MutationResult:
        with ObservableRequestHandler.create(Op.Prepend, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_prepend_request(key, value, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
            return MutationResult(_mutation_from_core(ret.raw_result), key=req.key)

    def append_multi(self,
                     keys_and_values: Mapping[str, str | bytes | bytearray],
                     opts: AppendMultiOptions | None = None,
                     **kwargs: Any) -> MultiMutationResult:
        with ObservableRequestHandler.create(Op.AppendMulti, self.observability_instruments) as obs_handler:
            req = self._multi_request_builder.build_append_multi_request(keys_and_values, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
            results, all_ok = _parse_sub_ops(ret, obs_handler, req.return_exceptions,
                                             lambda k, v: MutationResult(_mutation_from_core(v.raw_result), key=k))
            return MultiMutationResult(results, all_ok)

    def prepend_multi(self,
                      keys_and_values: Mapping[str, str | bytes | bytearray],
                      opts: PrependMultiOptions | None = None,
                      **kwargs: Any) -> MultiMutationResult:
        with ObservableRequestHandler.create(Op.PrependMulti, self.observability_instruments) as obs_handler:
            req = self._multi_request_builder.build_prepend_multi_request(keys_and_values, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
            results, all_ok = _parse_sub_ops(ret, obs_handler, req.return_exceptions,
                                             lambda k, v: MutationResult(_mutation_from_core(v.raw_result), key=k))
            return MultiMutationResult(results, all_ok)

    def increment_multi(self,
                        keys: list[str],
                        opts: IncrementMultiOptions | None = None,
                        **kwargs: Any) -> MultiCounterResult:
        with ObservableRequestHandler.create(Op.IncrementMulti, self.observability_instruments) as obs_handler:
            req = self._multi_request_builder.build_increment_multi_request(keys, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
            results, all_ok = _parse_sub_ops(ret, obs_handler, req.return_exceptions,
                                             lambda k, v: CounterResult(_counter_from_core(v.raw_result), key=k))
            return MultiCounterResult(results, all_ok)

    def decrement_multi(self,
                        keys: list[str],
                        opts: DecrementMultiOptions | None = None,
                        **kwargs: Any) -> MultiCounterResult:
        with ObservableRequestHandler.create(Op.DecrementMulti, self.observability_instruments) as obs_handler:
            req = self._multi_request_builder.build_decrement_multi_request(keys, obs_handler, opts, **kwargs)
            ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
            results, all_ok = _parse_sub_ops(ret, obs_handler, req.return_exceptions,
                                             lambda k, v: CounterResult(_counter_from_core(v.raw_result), key=k))
            return MultiCounterResult(results, all_ok)

    def scan(self, scan_type: ScanType, opts: ScanOptions | None = None, **kwargs: Any) -> ScanResultIterable:
        scan_args = self._request_builder.build_range_scan_args(scan_type, opts, **kwargs)
        executor = ClassicRangeScanExecutor(self.connection, scan_args)
        return ScanResultIterable(RowIterator(executor), executor)

    def binary(self) -> CollectionImpl:
        """**INTERNAL** The binary operations are this impl's own."""
        return self

    def query_index_mgmt(self) -> QueryIndexMgmtImpl:
        """**INTERNAL**"""
        return QueryIndexMgmtImpl(self._client_adapter,
                                  self.observability_instruments,
                                  collection_context=(self.name, self.scope_name))

    # The execute halves the legacy datastructures call, with requests they build under their own span.

    def _get(self,
             req: PycbcCoreKeyValueRequest,
             transcoder: Transcoder,
             obs_handler: ObservableRequestHandler | None) -> GetResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return GetResult(_get_from_core(ret.raw_result), transcoder=transcoder, key=req.key)

    def _insert(self,
                req: PycbcCoreKeyValueRequest,
                obs_handler: ObservableRequestHandler | None) -> MutationResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(_mutation_from_core(ret.raw_result), key=req.key)

    def _lookup_in(self,
                   req: PycbcCoreKeyValueRequest,
                   serializer: Serializer,
                   obs_handler: ObservableRequestHandler | None) -> LookupInResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return LookupInResult(_lookup_in_from_core(ret.raw_result, req.key), serializer=serializer, key=req.key)

    def _mutate_in(self,
                   req: PycbcCoreKeyValueRequest,
                   serializer: Serializer,
                   obs_handler: ObservableRequestHandler | None) -> MutateInResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutateInResult(_mutate_in_from_core(ret.raw_result, req.key), serializer=serializer, key=req.key)

    def _set_default_transcoder(self, transcoder: Transcoder) -> None:
        if not issubclass(transcoder.__class__, Transcoder):
            raise InvalidArgumentException('Cannot set default transcoder to non Transcoder type.')

        self._collection_details.default_transcoder = transcoder
