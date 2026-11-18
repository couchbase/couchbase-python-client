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

"""A sync collection over the gateway, which is also its binary collection."""

from __future__ import annotations

from collections.abc import (Callable,
                             Iterable,
                             Mapping)
from typing import (TYPE_CHECKING,
                    Any,
                    NoReturn,
                    Protocol,
                    TypeVar)

from couchbase.common._multi_args import unlock_keys_and_cas
from couchbase.common._observability import (KeyspaceDetails,
                                             ObservabilityInstruments,
                                             ObservableRequestHandler)
from couchbase.common._observability._clock import now_ns
from couchbase.common._operations import Op
from couchbase.common.exceptions import CouchbaseException, InvalidArgumentException
from couchbase.common.result import (CounterResult,
                                     ExistsResult,
                                     GetResult,
                                     LookupInResult,
                                     MultiCounterResult,
                                     MultiExistsResult,
                                     MultiGetResult,
                                     MultiMutationResult,
                                     MutateInResult,
                                     MutationResult)
from couchbase.protocol.couchbase2._capabilities import CAPABILITIES
from couchbase.protocol.couchbase2._collection_req_builder import CollectionRequestBuilder
from couchbase.protocol.couchbase2._collection_types import (CollectionOptionsBlock,
                                                             KvRequest,
                                                             counter_record,
                                                             exists_record,
                                                             get_record,
                                                             lookup_in_record,
                                                             mutate_in_record,
                                                             mutation_record)
from couchbase.protocol.couchbase2._core import unsupported
from couchbase.protocol.couchbase2._driver import call_unary

if TYPE_CHECKING:
    from datetime import timedelta

    import grpc
    from google.protobuf.message import Message

    from couchbase.common._capabilities import Capability
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
                                          GetReplicaOptions,
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
    from couchbase.common.replica_reads import GetReplicaStrategy
    from couchbase.common.result import (GetReplicaResult,
                                         LookupInReplicaResult,
                                         MultiGetReplicaResult,
                                         MultiResultType)
    from couchbase.common.subdocument import Spec
    from couchbase.protocol.couchbase2._cluster_impl import ClusterImpl
    from couchbase.protocol.couchbase2._scope_impl import ScopeImpl
    from couchbase.result import ScanResultIterable

_M = TypeVar('_M', bound='Message')
_T = TypeVar('_T')


class _Request(Protocol):
    @property
    def message(self) -> Message: ...

    @property
    def timeout(self) -> float: ...

    @property
    def obs_handler(self) -> ObservableRequestHandler | None: ...


_Req = TypeVar('_Req', bound=_Request)


def _checked_keys(keys: object) -> list[str]:
    if not isinstance(keys, list):
        raise InvalidArgumentException('Expected keys to be a list.')
    return keys


def _checked_docs(keys_and_docs: object) -> Mapping[str, Any]:
    if not isinstance(keys_and_docs, Mapping):
        raise InvalidArgumentException('Expected keys_and_docs to be a mapping.')
    return keys_and_docs


class CollectionImpl:

    def __init__(self, collection_name: str, scope_impl: ScopeImpl) -> None:
        self._keyspace: KeyspaceDetails = {'bucket_name': scope_impl.bucket_name,
                                           'scope_name': scope_impl.name,
                                           'collection_name': collection_name}
        self._cluster_impl: ClusterImpl = scope_impl.cluster_impl
        self._request_builder = CollectionRequestBuilder(self._cluster_impl.core, self._keyspace)

    @property
    def capabilities(self) -> frozenset[Capability]:
        return CAPABILITIES

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

    def get(self, key: str, opts: GetOptions | None = None, **kwargs: Any) -> GetResult:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.Get, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_get_request(key, obs_handler, opts, **kwargs)
            ctx = core.context(Op.Get, req.timeout, obs_handler=req.obs_handler)
            response = call_unary(self._cluster_impl.kv.Get, req.message, ctx, core.metadata)
            record = get_record(response, req.with_expiry)
            return GetResult(record, transcoder=req.transcoder, key=key)

    def upsert(self,
               key: str,
               value: DocumentValue,
               opts: UpsertOptions | None = None,
               **kwargs: Any) -> MutationResult:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.Upsert, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_upsert_request(key, value, obs_handler, opts, **kwargs)
            ctx = core.context(Op.Upsert, req.timeout, obs_handler=req.obs_handler)
            response = call_unary(self._cluster_impl.kv.Upsert, req.message, ctx, core.metadata)
            return MutationResult(mutation_record(response), key=key)

    def get_any_replica(self, key: str, opts: GetAnyReplicaOptions | None = None, **kwargs: Any) -> GetReplicaResult:
        raise unsupported('get_any_replica')

    def get_replica(self,
                    key: str,
                    strategy: GetReplicaStrategy,
                    opts: GetReplicaOptions | None = None,
                    **kwargs: Any) -> GetReplicaResult:
        raise unsupported('get_replica')

    def get_all_replicas(self,
                         key: str,
                         opts: GetAllReplicasOptions | None = None,
                         **kwargs: Any) -> Iterable[GetReplicaResult]:
        raise unsupported('get_all_replicas')

    def exists(self, key: str, opts: ExistsOptions | None = None, **kwargs: Any) -> ExistsResult:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.Exists, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_exists_request(key, obs_handler, opts, **kwargs)
            ctx = core.context(Op.Exists, req.timeout, obs_handler=req.obs_handler)
            response = call_unary(self._cluster_impl.kv.Exists, req.message, ctx, core.metadata)
            return ExistsResult(exists_record(response), key=key)

    def insert(self,
               key: str,
               value: DocumentValue,
               opts: InsertOptions | None = None,
               **kwargs: Any) -> MutationResult:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.Insert, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_insert_request(key, value, obs_handler, opts, **kwargs)
            ctx = core.context(Op.Insert, req.timeout, obs_handler=req.obs_handler)
            response = call_unary(self._cluster_impl.kv.Insert, req.message, ctx, core.metadata)
            return MutationResult(mutation_record(response), key=key)

    def replace(self,
                key: str,
                value: DocumentValue,
                opts: ReplaceOptions | None = None,
                **kwargs: Any) -> MutationResult:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.Replace, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_replace_request(key, value, obs_handler, opts, **kwargs)
            ctx = core.context(Op.Replace, req.timeout, obs_handler=req.obs_handler)
            response = call_unary(self._cluster_impl.kv.Replace, req.message, ctx, core.metadata)
            return MutationResult(mutation_record(response), key=key)

    def remove(self, key: str, opts: RemoveOptions | None = None, **kwargs: Any) -> MutationResult:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.Remove, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_remove_request(key, obs_handler, opts, **kwargs)
            ctx = core.context(Op.Remove, req.timeout, obs_handler=req.obs_handler)
            response = call_unary(self._cluster_impl.kv.Remove, req.message, ctx, core.metadata)
            return MutationResult(mutation_record(response), key=key)

    def touch(self, key: str, expiry: timedelta, opts: TouchOptions | None = None, **kwargs: Any) -> MutationResult:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.Touch, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_touch_request(key, expiry, obs_handler, opts, **kwargs)
            ctx = core.context(Op.Touch, req.timeout, obs_handler=req.obs_handler)
            response = call_unary(self._cluster_impl.kv.Touch, req.message, ctx, core.metadata)
            return MutationResult(mutation_record(response), key=key)

    def get_and_touch(self,
                      key: str,
                      expiry: timedelta,
                      opts: GetAndTouchOptions | None = None,
                      **kwargs: Any) -> GetResult:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.GetAndTouch, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_get_and_touch_request(key, expiry, obs_handler, opts, **kwargs)
            ctx = core.context(Op.GetAndTouch, req.timeout, obs_handler=req.obs_handler)
            response = call_unary(self._cluster_impl.kv.GetAndTouch, req.message, ctx, core.metadata)
            return GetResult(get_record(response), transcoder=req.transcoder, key=key)

    def get_and_lock(self,
                     key: str,
                     lock_time: timedelta,
                     opts: GetAndLockOptions | None = None,
                     **kwargs: Any) -> GetResult:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.GetAndLock, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_get_and_lock_request(key, lock_time, obs_handler, opts, **kwargs)
            ctx = core.context(Op.GetAndLock, req.timeout, obs_handler=req.obs_handler)
            response = call_unary(self._cluster_impl.kv.GetAndLock, req.message, ctx, core.metadata)
            return GetResult(get_record(response), transcoder=req.transcoder, key=key)

    def unlock(self, key: str, cas: int, opts: UnlockOptions | None = None, **kwargs: Any) -> None:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.Unlock, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_unlock_request(key, cas, obs_handler, opts, **kwargs)
            ctx = core.context(Op.Unlock, req.timeout, obs_handler=req.obs_handler)
            call_unary(self._cluster_impl.kv.Unlock, req.message, ctx, core.metadata)

    def lookup_in(self,
                  key: str,
                  spec: Iterable[Spec],
                  opts: LookupInOptions | None = None,
                  **kwargs: Any) -> LookupInResult:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.LookupIn, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_lookup_in_request(key, spec, obs_handler, opts, **kwargs)
            ctx = core.context(Op.LookupIn, req.timeout, obs_handler=req.obs_handler)
            response = call_unary(self._cluster_impl.kv.LookupIn, req.message, ctx, core.metadata)
            return LookupInResult(lookup_in_record(response, req.specs, key), serializer=req.serializer, key=key)

    def lookup_in_any_replica(self,
                              key: str,
                              spec: Iterable[Spec],
                              opts: LookupInAnyReplicaOptions | None = None,
                              **kwargs: Any) -> LookupInReplicaResult:
        raise unsupported('lookup_in_any_replica')

    def lookup_in_all_replicas(self,
                               key: str,
                               spec: Iterable[Spec],
                               opts: LookupInAllReplicasOptions | None = None,
                               **kwargs: Any) -> Iterable[LookupInReplicaResult]:
        raise unsupported('lookup_in_all_replicas')

    def mutate_in(self,
                  key: str,
                  spec: Iterable[Spec],
                  opts: MutateInOptions | None = None,
                  **kwargs: Any) -> MutateInResult:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.MutateIn, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_mutate_in_request(key, spec, obs_handler, opts, **kwargs)
            ctx = core.context(Op.MutateIn, req.timeout, obs_handler=req.obs_handler)
            response = call_unary(self._cluster_impl.kv.MutateIn, req.message, ctx, core.metadata)
            return MutateInResult(mutate_in_record(response, req.specs), serializer=req.serializer, key=key)

    def _each_key(self,
                  op: Op,
                  keys: object,
                  check: Callable[[object], Iterable[str]],
                  opts: dict[str, Any] | None,
                  kwargs: dict[str, Any],
                  build: Callable[[str, dict[str, Any]], _Req],
                  send: Callable[[str, _Req, ObservableRequestHandler | None], _T]
                  ) -> tuple[dict[str, _T | CouchbaseException], bool]:
        """A multi operation, as each key's single operation, one after another.

        Each key's calls are traced under the multi operation's span, as its dispatches.

        `check` reads the keys out of `keys` once the span has started, so a `keys` of the wrong
        shape fails the operation's span.  Every key's request is built before any is sent, so an
        invalid argument raises with nothing sent.  Every key is sent before an error is raised, so
        with `return_exceptions` false the keys that succeed are written as they are on classic,
        which sends the batch at once; the first key's error, in key order, is the one raised.
        """
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(op, self.observability_instruments) as obs_handler:
            block = CollectionOptionsBlock[op.name]
            multi = self._request_builder.build_multi_options(block, keys, check, obs_handler, opts, **kwargs)
            requests = {key: build(key, key_opts) for key, key_opts in multi.by_key.items()}
            results: dict[str, _T | CouchbaseException] = {}
            for key, req in requests.items():
                start = now_ns()
                failure: CouchbaseException | None = None
                try:
                    results[key] = send(key, req, obs_handler)
                except CouchbaseException as exc:
                    results[key] = failure = exc
                if obs_handler is not None:
                    obs_handler.record_sub_op(now_ns() - start, exc_val=failure)
            errors = [v for v in results.values() if isinstance(v, CouchbaseException)]
            if errors and not multi.return_exceptions:
                raise errors[0]
            return results, not errors

    def _send(self,
              op: Op,
              rpc: grpc.UnaryUnaryMultiCallable[Any, _M],
              req: _Request,
              obs_handler: ObservableRequestHandler | None) -> _M:
        core = self._cluster_impl.core
        ctx = core.context(op, req.timeout, obs_handler=obs_handler)
        return call_unary(rpc, req.message, ctx, core.metadata)

    def get_multi(self, keys: list[str], opts: GetMultiOptions | None = None, **kwargs: Any) -> MultiGetResult:
        kv = self._cluster_impl.kv
        results, all_ok = self._each_key(
            Op.GetMulti, keys, _checked_keys, opts, kwargs,
            lambda key, args: self._request_builder.build_get_request(key, None, args),
            lambda key, req, obs: GetResult(
                get_record(self._send(Op.Get, kv.Get, req, obs), req.with_expiry),
                transcoder=req.transcoder, key=key))
        return MultiGetResult(results, all_ok)

    def get_any_replica_multi(self,
                              keys: list[str],
                              opts: GetAnyReplicaMultiOptions | None = None,
                              **kwargs: Any) -> MultiGetReplicaResult[GetReplicaResult]:
        raise unsupported('get_any_replica_multi')

    def get_all_replicas_multi(self,
                               keys: list[str],
                               opts: GetAllReplicasMultiOptions | None = None,
                               **kwargs: Any) -> MultiGetReplicaResult[list[GetReplicaResult]]:
        raise unsupported('get_all_replicas_multi')

    def get_and_lock_multi(self,
                           keys: list[str],
                           lock_time: timedelta,
                           opts: GetAndLockMultiOptions | None = None,
                           **kwargs: Any) -> MultiGetResult:
        kv = self._cluster_impl.kv
        results, all_ok = self._each_key(
            Op.GetAndLockMulti, keys, _checked_keys, opts, kwargs,
            lambda key, args: self._request_builder.build_get_and_lock_request(key, lock_time, None, args),
            lambda key, req, obs: GetResult(
                get_record(self._send(Op.GetAndLock, kv.GetAndLock, req, obs)),
                transcoder=req.transcoder, key=key))
        return MultiGetResult(results, all_ok)

    def exists_multi(self, keys: list[str], opts: ExistsMultiOptions | None = None, **kwargs: Any) -> MultiExistsResult:
        kv = self._cluster_impl.kv
        results, all_ok = self._each_key(
            Op.ExistsMulti, keys, _checked_keys, opts, kwargs,
            lambda key, args: self._request_builder.build_exists_request(key, None, args),
            lambda key, req, obs: ExistsResult(
                exists_record(self._send(Op.Exists, kv.Exists, req, obs)),
                key=key))
        return MultiExistsResult(results, all_ok)

    def insert_multi(self,
                     keys_and_docs: Mapping[str, DocumentValue],
                     opts: InsertMultiOptions | None = None,
                     **kwargs: Any) -> MultiMutationResult:
        kv = self._cluster_impl.kv
        results, all_ok = self._each_key(
            Op.InsertMulti, keys_and_docs, _checked_docs, opts, kwargs,
            lambda key, args: self._request_builder.build_insert_request(key, keys_and_docs[key], None, args),
            lambda key, req, obs: MutationResult(
                mutation_record(self._send(Op.Insert, kv.Insert, req, obs)),
                key=key))
        return MultiMutationResult(results, all_ok)

    def upsert_multi(self,
                     keys_and_docs: Mapping[str, DocumentValue],
                     opts: UpsertMultiOptions | None = None,
                     **kwargs: Any) -> MultiMutationResult:
        kv = self._cluster_impl.kv
        results, all_ok = self._each_key(
            Op.UpsertMulti, keys_and_docs, _checked_docs, opts, kwargs,
            lambda key, args: self._request_builder.build_upsert_request(key, keys_and_docs[key], None, args),
            lambda key, req, obs: MutationResult(
                mutation_record(self._send(Op.Upsert, kv.Upsert, req, obs)),
                key=key))
        return MultiMutationResult(results, all_ok)

    def replace_multi(self,
                      keys_and_docs: Mapping[str, DocumentValue],
                      opts: ReplaceMultiOptions | None = None,
                      **kwargs: Any) -> MultiMutationResult:
        kv = self._cluster_impl.kv
        results, all_ok = self._each_key(
            Op.ReplaceMulti, keys_and_docs, _checked_docs, opts, kwargs,
            lambda key, args: self._request_builder.build_replace_request(key, keys_and_docs[key], None, args),
            lambda key, req, obs: MutationResult(
                mutation_record(self._send(Op.Replace, kv.Replace, req, obs)),
                key=key))
        return MultiMutationResult(results, all_ok)

    def remove_multi(self,
                     keys: list[str],
                     opts: RemoveMultiOptions | None = None,
                     **kwargs: Any) -> MultiMutationResult:
        kv = self._cluster_impl.kv
        results, all_ok = self._each_key(
            Op.RemoveMulti, keys, _checked_keys, opts, kwargs,
            lambda key, args: self._request_builder.build_remove_request(key, None, args),
            lambda key, req, obs: MutationResult(
                mutation_record(self._send(Op.Remove, kv.Remove, req, obs)),
                key=key))
        return MultiMutationResult(results, all_ok)

    def touch_multi(self,
                    keys: list[str],
                    expiry: timedelta,
                    opts: TouchMultiOptions | None = None,
                    **kwargs: Any) -> MultiMutationResult:
        kv = self._cluster_impl.kv
        results, all_ok = self._each_key(
            Op.TouchMulti, keys, _checked_keys, opts, kwargs,
            lambda key, args: self._request_builder.build_touch_request(key, expiry, None, args),
            lambda key, req, obs: MutationResult(
                mutation_record(self._send(Op.Touch, kv.Touch, req, obs)),
                key=key))
        return MultiMutationResult(results, all_ok)

    def unlock_multi(self,
                     keys: MultiResultType | dict[str, int],
                     opts: UnlockMultiOptions | None = None,
                     **kwargs: Any) -> dict[str, CouchbaseException | None]:
        keys_and_cas = unlock_keys_and_cas(keys)
        kv = self._cluster_impl.kv

        def unlock(key: str, req: KvRequest[Any], obs_handler: ObservableRequestHandler | None) -> None:
            self._send(Op.Unlock, kv.Unlock, req, obs_handler)

        results, _ = self._each_key(
            Op.UnlockMulti, keys_and_cas, _checked_docs, opts, kwargs,
            lambda key, args: self._request_builder.build_unlock_request(key, keys_and_cas[key], None, args),
            unlock)
        return results

    def scan(self, scan_type: ScanType, opts: ScanOptions | None = None, **kwargs: Any) -> ScanResultIterable:
        raise unsupported('scan')

    def binary(self) -> CollectionImpl:
        return self

    def increment(self, key: str, opts: IncrementOptions | None = None, **kwargs: Any) -> CounterResult:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.Increment, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_increment_request(key, obs_handler, opts, **kwargs)
            ctx = core.context(Op.Increment, req.timeout, obs_handler=req.obs_handler)
            response = call_unary(self._cluster_impl.kv.Increment, req.message, ctx, core.metadata)
            return CounterResult(counter_record(response), key=key)

    def decrement(self, key: str, opts: DecrementOptions | None = None, **kwargs: Any) -> CounterResult:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.Decrement, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_decrement_request(key, obs_handler, opts, **kwargs)
            ctx = core.context(Op.Decrement, req.timeout, obs_handler=req.obs_handler)
            response = call_unary(self._cluster_impl.kv.Decrement, req.message, ctx, core.metadata)
            return CounterResult(counter_record(response), key=key)

    def append(self,
               key: str,
               value: str | bytes | bytearray,
               opts: AppendOptions | None = None,
               **kwargs: Any) -> MutationResult:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.Append, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_append_request(key, value, obs_handler, opts, **kwargs)
            ctx = core.context(Op.Append, req.timeout, obs_handler=req.obs_handler)
            response = call_unary(self._cluster_impl.kv.Append, req.message, ctx, core.metadata)
            return MutationResult(mutation_record(response), key=key)

    def prepend(self,
                key: str,
                value: str | bytes | bytearray,
                opts: PrependOptions | None = None,
                **kwargs: Any) -> MutationResult:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.Prepend, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_prepend_request(key, value, obs_handler, opts, **kwargs)
            ctx = core.context(Op.Prepend, req.timeout, obs_handler=req.obs_handler)
            response = call_unary(self._cluster_impl.kv.Prepend, req.message, ctx, core.metadata)
            return MutationResult(mutation_record(response), key=key)

    def append_multi(self,
                     keys_and_values: Mapping[str, str | bytes | bytearray],
                     opts: AppendMultiOptions | None = None,
                     **kwargs: Any) -> MultiMutationResult:
        kv = self._cluster_impl.kv
        results, all_ok = self._each_key(
            Op.AppendMulti, keys_and_values, _checked_docs, opts, kwargs,
            lambda key, args: self._request_builder.build_append_request(key, keys_and_values[key], None, args),
            lambda key, req, obs: MutationResult(
                mutation_record(self._send(Op.Append, kv.Append, req, obs)),
                key=key))
        return MultiMutationResult(results, all_ok)

    def prepend_multi(self,
                      keys_and_values: Mapping[str, str | bytes | bytearray],
                      opts: PrependMultiOptions | None = None,
                      **kwargs: Any) -> MultiMutationResult:
        kv = self._cluster_impl.kv
        results, all_ok = self._each_key(
            Op.PrependMulti, keys_and_values, _checked_docs, opts, kwargs,
            lambda key, args: self._request_builder.build_prepend_request(key, keys_and_values[key], None, args),
            lambda key, req, obs: MutationResult(
                mutation_record(self._send(Op.Prepend, kv.Prepend, req, obs)),
                key=key))
        return MultiMutationResult(results, all_ok)

    def increment_multi(self,
                        keys: list[str],
                        opts: IncrementMultiOptions | None = None,
                        **kwargs: Any) -> MultiCounterResult:
        kv = self._cluster_impl.kv
        results, all_ok = self._each_key(
            Op.IncrementMulti, keys, _checked_keys, opts, kwargs,
            lambda key, args: self._request_builder.build_increment_request(key, None, args),
            lambda key, req, obs: CounterResult(
                counter_record(self._send(Op.Increment, kv.Increment, req, obs)), key=key))
        return MultiCounterResult(results, all_ok)

    def decrement_multi(self,
                        keys: list[str],
                        opts: DecrementMultiOptions | None = None,
                        **kwargs: Any) -> MultiCounterResult:
        kv = self._cluster_impl.kv
        results, all_ok = self._each_key(
            Op.DecrementMulti, keys, _checked_keys, opts, kwargs,
            lambda key, args: self._request_builder.build_decrement_request(key, None, args),
            lambda key, req, obs: CounterResult(
                counter_record(self._send(Op.Decrement, kv.Decrement, req, obs)), key=key))
        return MultiCounterResult(results, all_ok)

    def query_index_mgmt(self) -> NoReturn:
        raise unsupported('Query index management')
