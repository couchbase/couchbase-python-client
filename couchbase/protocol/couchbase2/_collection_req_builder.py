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

"""A collection's key-value requests to the gateway, shared by the sync, asyncio and Twisted impls.

A request is built at the call, so an invalid argument raises there rather than when a coroutine or
Deferred is resolved: its options are checked and read as the caller wrote them, its span started,
and its message built.
"""

from __future__ import annotations

from collections.abc import (Callable,
                             Iterable,
                             Mapping)
from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Any,
                    TypeAlias,
                    TypeVar)

from google.protobuf.message import Message

from couchbase.common._binary_args import binary_value, read_counter_args
from couchbase.common._multi_args import refuse_projection
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._option_args import merge, resolve_deprecated
from couchbase.common._option_spec import BLOCKS
from couchbase.common._subdoc_args import (check_preserve_expiry,
                                           encode_spec_value,
                                           read_store_semantics_keywords)
from couchbase.common.durability import (ClientDurability,
                                         DurabilityLevel,
                                         ServerDurability)
from couchbase.common.exceptions import FeatureUnavailableException, InvalidArgumentException
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.common.subdocument import StoreSemantics, SubDocOp
from couchbase.protocol.couchbase2._collection_types import (CollectionOptionsBlock,
                                                             GetRequest,
                                                             KvRequest,
                                                             LookupInRequest,
                                                             MultiOptions,
                                                             MutateInRequest,
                                                             ReadRequest,
                                                             SubdocSpec,
                                                             xattrs_first)
from couchbase.protocol.couchbase2._core import timeout_seconds
from couchbase.protocol.couchbase2._generated.kv.v1 import kv_pb2

if TYPE_CHECKING:
    from couchbase.common._observability import KeyspaceDetails
    from couchbase.common._utils import DocumentValue
    from couchbase.common.subdocument import Spec
    from couchbase.protocol.couchbase2._core import ClusterCore
    from couchbase.serializer import Serializer
    from couchbase.transcoder import Transcoder

_M = TypeVar('_M', bound=Message)
_Counter = TypeVar('_Counter', kv_pb2.IncrementRequest, kv_pb2.DecrementRequest)
_Binary = TypeVar('_Binary', kv_pb2.AppendRequest, kv_pb2.PrependRequest)

if TYPE_CHECKING:
    _Durable: TypeAlias = (kv_pb2.InsertRequest
                           | kv_pb2.UpsertRequest
                           | kv_pb2.ReplaceRequest
                           | kv_pb2.RemoveRequest
                           | kv_pb2.IncrementRequest
                           | kv_pb2.DecrementRequest
                           | kv_pb2.AppendRequest
                           | kv_pb2.PrependRequest
                           | kv_pb2.MutateInRequest)
    _WithCas: TypeAlias = (kv_pb2.ReplaceRequest
                           | kv_pb2.RemoveRequest
                           | kv_pb2.AppendRequest
                           | kv_pb2.PrependRequest
                           | kv_pb2.MutateInRequest)

_DURABILITY_LEVELS = {
    DurabilityLevel.MAJORITY: kv_pb2.DURABILITY_LEVEL_MAJORITY,
    DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE: kv_pb2.DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE,
    DurabilityLevel.PERSIST_TO_MAJORITY: kv_pb2.DURABILITY_LEVEL_PERSIST_TO_MAJORITY,
}

_MAX_EXPIRY_SECS = 2**32 - 1

_LookupOp = kv_pb2.LookupInRequest.Spec
_LOOKUP_IN_OPS = {
    SubDocOp.GET: _LookupOp.OPERATION_GET,
    SubDocOp.GET_DOC: _LookupOp.OPERATION_GET,
    SubDocOp.EXISTS: _LookupOp.OPERATION_EXISTS,
    SubDocOp.GET_COUNT: _LookupOp.OPERATION_COUNT,
}

# The gateway reads an empty path as the whole document, which is what SET_DOC and REMOVE_DOC are.
_MutateOp = kv_pb2.MutateInRequest.Spec
_MUTATE_IN_OPS = {
    SubDocOp.DICT_ADD: _MutateOp.OPERATION_INSERT,
    SubDocOp.DICT_UPSERT: _MutateOp.OPERATION_UPSERT,
    SubDocOp.REPLACE: _MutateOp.OPERATION_REPLACE,
    SubDocOp.SET_DOC: _MutateOp.OPERATION_REPLACE,
    SubDocOp.REMOVE: _MutateOp.OPERATION_REMOVE,
    SubDocOp.REMOVE_DOC: _MutateOp.OPERATION_REMOVE,
    SubDocOp.ARRAY_PUSH_LAST: _MutateOp.OPERATION_ARRAY_APPEND,
    SubDocOp.ARRAY_PUSH_FIRST: _MutateOp.OPERATION_ARRAY_PREPEND,
    SubDocOp.ARRAY_INSERT: _MutateOp.OPERATION_ARRAY_INSERT,
    SubDocOp.ARRAY_ADD_UNIQUE: _MutateOp.OPERATION_ARRAY_ADD_UNIQUE,
    SubDocOp.COUNTER: _MutateOp.OPERATION_COUNTER,
}

# A multi operation's own keys, which no key's single operation takes.
_MULTI_ONLY = frozenset({'per_key_options', 'return_exceptions'})
_INERT_HERE = CouchbaseProtocol.COUCHBASE2.name.lower()

_STORE_SEMANTICS = {
    StoreSemantics.REPLACE: kv_pb2.MutateInRequest.STORE_SEMANTIC_REPLACE,
    StoreSemantics.UPSERT: kv_pb2.MutateInRequest.STORE_SEMANTIC_UPSERT,
    StoreSemantics.INSERT: kv_pb2.MutateInRequest.STORE_SEMANTIC_INSERT,
}


class CollectionRequestBuilder:

    def __init__(self, core: ClusterCore, keyspace: KeyspaceDetails) -> None:
        self._core = core
        self._keyspace = keyspace

    def build_get_request(self,
                          key: str,
                          obs_handler: ObservableRequestHandler | None,
                          opts: dict[str, Any] | None = None,
                          **kwargs: Any) -> GetRequest:
        final_args = self._start(CollectionOptionsBlock.Get, obs_handler, opts, kwargs)
        projections = final_args.get('project')
        if projections is not None and not (isinstance(projections, list)
                                            and all(isinstance(p, str) for p in projections)):
            raise InvalidArgumentException('Project must be a list of strings.')

        return GetRequest(message=self._keyed(kv_pb2.GetRequest, key, project=projections or ()),
                          transcoder=self._transcoder(final_args),
                          with_expiry=bool(final_args.get('with_expiry')),
                          timeout=timeout_seconds(final_args, self._core.kv_timeout),
                          obs_handler=obs_handler)

    def build_get_and_touch_request(self,
                                    key: str,
                                    expiry: timedelta,
                                    obs_handler: ObservableRequestHandler | None,
                                    opts: dict[str, Any] | None = None,
                                    **kwargs: Any) -> ReadRequest[kv_pb2.GetAndTouchRequest]:
        kwargs['expiry'] = expiry
        final_args = self._start(CollectionOptionsBlock.GetAndTouch, obs_handler, opts, kwargs)
        message = self._keyed(kv_pb2.GetAndTouchRequest, key, expiry_secs=_expiry_secs(final_args['expiry']))
        return ReadRequest(message=message,
                           transcoder=self._transcoder(final_args),
                           timeout=timeout_seconds(final_args, self._core.kv_timeout),
                           obs_handler=obs_handler)

    def build_get_and_lock_request(self,
                                   key: str,
                                   lock_time: timedelta,
                                   obs_handler: ObservableRequestHandler | None,
                                   opts: dict[str, Any] | None = None,
                                   **kwargs: Any) -> ReadRequest[kv_pb2.GetAndLockRequest]:
        kwargs['lock_time'] = lock_time
        final_args = self._start(CollectionOptionsBlock.GetAndLock, obs_handler, opts, kwargs)
        message = self._keyed(kv_pb2.GetAndLockRequest, key, lock_time_secs=_lock_time_secs(final_args['lock_time']))
        return ReadRequest(message=message,
                           transcoder=self._transcoder(final_args),
                           timeout=timeout_seconds(final_args, self._core.kv_timeout),
                           obs_handler=obs_handler)

    def build_exists_request(self,
                             key: str,
                             obs_handler: ObservableRequestHandler | None,
                             opts: dict[str, Any] | None = None,
                             **kwargs: Any) -> KvRequest[kv_pb2.ExistsRequest]:
        final_args = self._start(CollectionOptionsBlock.Exists, obs_handler, opts, kwargs)
        return KvRequest(message=self._keyed(kv_pb2.ExistsRequest, key),
                         timeout=timeout_seconds(final_args, self._core.kv_timeout),
                         obs_handler=obs_handler)

    def build_insert_request(self,
                             key: str,
                             value: DocumentValue,
                             obs_handler: ObservableRequestHandler | None,
                             opts: dict[str, Any] | None = None,
                             **kwargs: Any) -> KvRequest[kv_pb2.InsertRequest]:
        final_args = self._start(CollectionOptionsBlock.Insert, obs_handler, opts, kwargs)
        content, flags = self._encode(value, final_args, obs_handler)
        message = self._keyed(kv_pb2.InsertRequest, key,
                              content_uncompressed=content,
                              content_flags=flags,
                              expiry_secs=_optional_expiry_secs(final_args))
        timeout = self._durability(message, final_args, obs_handler)
        return KvRequest(message=message, timeout=timeout_seconds(final_args, timeout), obs_handler=obs_handler)

    def build_upsert_request(self,
                             key: str,
                             value: DocumentValue,
                             obs_handler: ObservableRequestHandler | None,
                             opts: dict[str, Any] | None = None,
                             **kwargs: Any) -> KvRequest[kv_pb2.UpsertRequest]:
        """The expiry goes in the only shape the gateway reads as the caller meant it.

        An absent expiry tells the gateway to keep the document's existing one, so no expiry is an
        explicit zero, and keeping the existing one is the field left out.  `expiry_secs` is always a
        duration: the gateway turns one past 30 days into a timestamp itself.  With an expiry as well,
        `preserve_expiry` applies it to a new document only; the gateway refuses that flag beside an
        absent or zero expiry.
        """
        final_args = self._start(CollectionOptionsBlock.Upsert, obs_handler, opts, kwargs)
        content, flags = self._encode(value, final_args, obs_handler)
        message = self._keyed(kv_pb2.UpsertRequest, key, content_uncompressed=content, content_flags=flags)
        seconds = _optional_expiry_secs(final_args)
        preserve_expiry = bool(final_args.get('preserve_expiry'))
        if seconds or not preserve_expiry:
            message.expiry_secs = seconds
        if seconds and preserve_expiry:
            message.preserve_expiry_on_existing = True
        timeout = self._durability(message, final_args, obs_handler)
        return KvRequest(message=message, timeout=timeout_seconds(final_args, timeout), obs_handler=obs_handler)

    def build_replace_request(self,
                              key: str,
                              value: DocumentValue,
                              obs_handler: ObservableRequestHandler | None,
                              opts: dict[str, Any] | None = None,
                              **kwargs: Any) -> KvRequest[kv_pb2.ReplaceRequest]:
        """No expiry is an explicit zero, and `preserve_expiry` leaves the expiry out, as for `upsert`."""
        final_args = self._start(CollectionOptionsBlock.Replace, obs_handler, opts, kwargs)
        seconds = _optional_expiry_secs(final_args)
        preserve_expiry = final_args.get('preserve_expiry', False) is True
        if final_args.get('expiry') and preserve_expiry:
            raise InvalidArgumentException('The expiry and preserve_expiry options cannot both be set for replace '
                                           'operations.')
        content, flags = self._encode(value, final_args, obs_handler)
        message = self._keyed(kv_pb2.ReplaceRequest, key, content_uncompressed=content, content_flags=flags)
        if not preserve_expiry:
            message.expiry_secs = seconds
        _set_cas(message, final_args)
        timeout = self._durability(message, final_args, obs_handler)
        return KvRequest(message=message, timeout=timeout_seconds(final_args, timeout), obs_handler=obs_handler)

    def build_remove_request(self,
                             key: str,
                             obs_handler: ObservableRequestHandler | None,
                             opts: dict[str, Any] | None = None,
                             **kwargs: Any) -> KvRequest[kv_pb2.RemoveRequest]:
        final_args = self._start(CollectionOptionsBlock.Remove, obs_handler, opts, kwargs)
        message = self._keyed(kv_pb2.RemoveRequest, key)
        _set_cas(message, final_args)
        timeout = self._durability(message, final_args, obs_handler)
        return KvRequest(message=message, timeout=timeout_seconds(final_args, timeout), obs_handler=obs_handler)

    def build_touch_request(self,
                            key: str,
                            expiry: timedelta,
                            obs_handler: ObservableRequestHandler | None,
                            opts: dict[str, Any] | None = None,
                            **kwargs: Any) -> KvRequest[kv_pb2.TouchRequest]:
        kwargs['expiry'] = expiry
        final_args = self._start(CollectionOptionsBlock.Touch, obs_handler, opts, kwargs)
        return KvRequest(message=self._keyed(kv_pb2.TouchRequest, key, expiry_secs=_expiry_secs(final_args['expiry'])),
                         timeout=timeout_seconds(final_args, self._core.kv_timeout),
                         obs_handler=obs_handler)

    def build_unlock_request(self,
                             key: str,
                             cas: int,
                             obs_handler: ObservableRequestHandler | None,
                             opts: dict[str, Any] | None = None,
                             **kwargs: Any) -> KvRequest[kv_pb2.UnlockRequest]:
        kwargs['cas'] = cas
        final_args = self._start(CollectionOptionsBlock.Unlock, obs_handler, opts, kwargs)
        cas = final_args['cas']
        if not isinstance(cas, int):
            raise InvalidArgumentException('Expected value to be of type int.')
        return KvRequest(message=self._keyed(kv_pb2.UnlockRequest, key, cas=cas),
                         timeout=timeout_seconds(final_args, self._core.kv_timeout),
                         obs_handler=obs_handler)

    def build_increment_request(self,
                                key: str,
                                obs_handler: ObservableRequestHandler | None,
                                opts: dict[str, Any] | None = None,
                                **kwargs: Any) -> KvRequest[kv_pb2.IncrementRequest]:
        return self._counter_request(CollectionOptionsBlock.Increment, kv_pb2.IncrementRequest,
                                     key, obs_handler, opts, kwargs)

    def build_decrement_request(self,
                                key: str,
                                obs_handler: ObservableRequestHandler | None,
                                opts: dict[str, Any] | None = None,
                                **kwargs: Any) -> KvRequest[kv_pb2.DecrementRequest]:
        return self._counter_request(CollectionOptionsBlock.Decrement, kv_pb2.DecrementRequest,
                                     key, obs_handler, opts, kwargs)

    def build_append_request(self,
                             key: str,
                             value: str | bytes | bytearray,
                             obs_handler: ObservableRequestHandler | None,
                             opts: dict[str, Any] | None = None,
                             **kwargs: Any) -> KvRequest[kv_pb2.AppendRequest]:
        return self._binary_request(CollectionOptionsBlock.Append, kv_pb2.AppendRequest,
                                    key, value, obs_handler, opts, kwargs)

    def build_prepend_request(self,
                              key: str,
                              value: str | bytes | bytearray,
                              obs_handler: ObservableRequestHandler | None,
                              opts: dict[str, Any] | None = None,
                              **kwargs: Any) -> KvRequest[kv_pb2.PrependRequest]:
        return self._binary_request(CollectionOptionsBlock.Prepend, kv_pb2.PrependRequest,
                                    key, value, obs_handler, opts, kwargs)

    def build_lookup_in_request(self,
                                key: str,
                                specs: Iterable[Spec],
                                obs_handler: ObservableRequestHandler | None,
                                opts: dict[str, Any] | None = None,
                                **kwargs: Any) -> LookupInRequest:
        final_args = self._start(CollectionOptionsBlock.LookupIn, obs_handler, opts, kwargs)
        written = tuple(specs)
        order = xattrs_first([bool(spec[2]) for spec in written])
        sent = []
        for index in order:
            opcode, path, xattr = written[index][:3]
            operation = _LOOKUP_IN_OPS.get(opcode)
            if operation is None:
                raise InvalidArgumentException(f'{_spec_name(written[index])} cannot be used in lookup_in.')
            spec = _LookupOp(operation=operation, path=path)
            if xattr:
                spec.flags.xattr = True
            sent.append(spec)

        message = self._keyed(kv_pb2.LookupInRequest, key, specs=sent)
        if final_args.get('access_deleted'):
            message.flags.access_deleted = True
        return LookupInRequest(message=message,
                               specs=_sent_specs(written, order),
                               serializer=self._serializer(final_args),
                               timeout=timeout_seconds(final_args, self._core.kv_timeout),
                               obs_handler=obs_handler)

    def build_mutate_in_request(self,
                                key: str,
                                specs: Iterable[Spec],
                                obs_handler: ObservableRequestHandler | None,
                                opts: dict[str, Any] | None = None,
                                **kwargs: Any) -> MutateInRequest:
        """The expiry is read as `upsert` reads it, except beside `preserve_expiry`.

        The gateway keeps the document's expiry when the request carries none, so no expiry is an
        explicit zero, and preserving it is the field left out.  It has no way to apply an expiry to
        a new document only, so an expiry with `preserve_expiry` is unavailable.  Nor can it expand a
        mutation macro: the macro would be stored as written.
        """
        final_args = self._start(CollectionOptionsBlock.MutateIn, obs_handler, opts, kwargs)
        written = tuple(specs)
        check_preserve_expiry(written, final_args)
        read_store_semantics_keywords(final_args)
        serializer = self._serializer(final_args)
        order = xattrs_first([len(spec) > 3 and bool(spec[3]) for spec in written])
        sent = [self._mutate_in_spec(written[index], serializer, obs_handler) for index in order]
        message = self._keyed(kv_pb2.MutateInRequest, key, specs=sent)
        seconds = _optional_expiry_secs(final_args)
        if bool(final_args.get('preserve_expiry')):
            if seconds:
                raise FeatureUnavailableException('couchbase2 cannot apply an expiry to a new document only, so '
                                                  'mutate_in does not take an expiry with preserve_expiry.')
        else:
            message.expiry_secs = seconds
        _set_cas(message, final_args)
        store_semantics = final_args.get('store_semantics')
        if store_semantics is not None:
            message.store_semantic = _store_semantic(store_semantics)
        if final_args.get('access_deleted'):
            message.flags.access_deleted = True
        timeout = self._durability(message, final_args, obs_handler)
        return MutateInRequest(message=message,
                               specs=_sent_specs(written, order),
                               serializer=serializer,
                               timeout=timeout_seconds(final_args, timeout),
                               obs_handler=obs_handler)

    def build_multi_options(self,
                            block: CollectionOptionsBlock,
                            keys: object,
                            check: Callable[[object], Iterable[str]],
                            obs_handler: ObservableRequestHandler | None,
                            opts: dict[str, Any] | None = None,
                            **kwargs: Any) -> MultiOptions:
        """Each key's options for a multi operation, which sends every key as its single operation.

        A key's `per_key_options` are laid over the batch's, a key given as `None` leaving the batch's
        in place.  Both are judged against the multi block, and a key that block does not carry, or
        cannot act on, is dropped, as classic drops it.  The span is the batch's, so no key's request
        starts one.
        """
        final_args = merge(kwargs, opts)
        self._core.validate_options(block, final_args)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        if obs_handler:
            obs_handler.create_kv_multi_span(self._keyspace, parent_span=parent_span)
        checked_keys = check(keys)
        per_key = final_args.pop('per_key_options', None) or {}
        return_exceptions = final_args.pop('return_exceptions', True)
        if not isinstance(per_key, Mapping):
            raise InvalidArgumentException(f'Expected per_key_options to be a dict of {block}.')
        for key_opts in per_key.values():
            if not isinstance(key_opts, dict):
                raise InvalidArgumentException(f'Expected options to be of type {block} | dict')
            self._core.validate_options(block, key_opts)
        if block is CollectionOptionsBlock.GetMulti:
            refuse_projection(final_args, dict(per_key))

        entry = BLOCKS[block]

        def sent(stated: dict[str, Any]) -> dict[str, Any]:
            if entry.retired:
                stated = resolve_deprecated(entry, stated)
            return {k: v for k, v in stated.items()
                    if v is not None and k in entry.keys and k not in _MULTI_ONLY
                    and _INERT_HERE not in entry.keys[k].inert}

        batch = sent(final_args)
        by_key = {key: {**batch, **sent(per_key[key])} if key in per_key else batch for key in checked_keys}
        return MultiOptions(by_key=by_key, return_exceptions=bool(return_exceptions))

    def _mutate_in_spec(self,
                        spec: Spec,
                        serializer: Serializer,
                        obs_handler: ObservableRequestHandler | None) -> kv_pb2.MutateInRequest.Spec:
        operation = _MUTATE_IN_OPS.get(spec[0]) if len(spec) > 3 else None
        if operation is None:
            raise InvalidArgumentException(f'{_spec_name(spec)} cannot be used in mutate_in.')
        opcode, path, create_path, xattr, expand_macros = spec[:5]
        if expand_macros:
            raise FeatureUnavailableException('couchbase2 cannot expand a mutation macro; the gateway would '
                                              'store the macro as written.')
        message = _MutateOp(operation=operation, path=path)
        if create_path:
            message.flags.create_path = True
        if xattr:
            message.flags.xattr = True
        if len(spec) == 6:
            def encode() -> tuple[bytes, int]:
                return encode_spec_value(serializer, opcode, spec[5]), 0

            if not obs_handler or obs_handler.is_noop:
                message.content, _ = encode()
            else:
                message.content, _ = obs_handler.maybe_add_encoding_span(encode)
        return message

    def _start(self,
               block: CollectionOptionsBlock,
               obs_handler: ObservableRequestHandler | None,
               opts: dict[str, Any] | None,
               kwargs: dict[str, Any]) -> dict[str, Any]:
        """The caller's options, checked, with the span taken out of them and started."""
        final_args = merge(kwargs, opts)
        self._core.validate_options(block, final_args)
        # Checked before the span starts, so an operation they fail has no span.
        if final_args.get('expiry') is not None:
            _expiry_secs(final_args['expiry'])
        if 'lock_time' in final_args:
            _lock_time_secs(final_args['lock_time'])
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._keyspace, parent_span=parent_span)
        return final_args

    def _keyed(self, message_type: type[_M], key: str, **fields: Any) -> _M:
        return message_type(bucket_name=self._keyspace['bucket_name'],
                            scope_name=self._keyspace['scope_name'],
                            collection_name=self._keyspace['collection_name'],
                            key=key,
                            **fields)

    def _durability(self,
                    message: _Durable,
                    final_args: dict[str, Any],
                    obs_handler: ObservableRequestHandler | None) -> float:
        """Set the message's durability level, and return the timeout the request falls back on."""
        level = _durability_level(final_args.get('durability'))
        if level == DurabilityLevel.NONE:
            return self._core.kv_timeout
        if obs_handler:
            obs_handler.add_kv_durability_attribute(level)
        message.durability_level = _DURABILITY_LEVELS[level]
        return self._core.kv_durable_timeout

    def _encode(self,
                value: DocumentValue,
                final_args: dict[str, Any],
                obs_handler: ObservableRequestHandler | None) -> tuple[bytes, int]:
        transcoder = self._transcoder(final_args)
        if not obs_handler or obs_handler.is_noop:
            return transcoder.encode_value(value)
        return obs_handler.maybe_create_encoding_span(lambda: transcoder.encode_value(value))

    def _counter_request(self,
                         block: CollectionOptionsBlock,
                         message_type: type[_Counter],
                         key: str,
                         obs_handler: ObservableRequestHandler | None,
                         opts: dict[str, Any] | None,
                         kwargs: dict[str, Any]) -> KvRequest[_Counter]:
        final_args = self._start(block, obs_handler, opts, kwargs)
        delta, initial = read_counter_args(final_args)
        message = self._keyed(message_type, key, delta=delta, expiry_secs=_optional_expiry_secs(final_args))
        if initial is not None:
            message.initial = initial
        timeout = self._durability(message, final_args, obs_handler)
        return KvRequest(message=message, timeout=timeout_seconds(final_args, timeout), obs_handler=obs_handler)

    def _binary_request(self,
                        block: CollectionOptionsBlock,
                        message_type: type[_Binary],
                        key: str,
                        value: str | bytes | bytearray,
                        obs_handler: ObservableRequestHandler | None,
                        opts: dict[str, Any] | None,
                        kwargs: dict[str, Any]) -> KvRequest[_Binary]:
        final_args = self._start(block, obs_handler, opts, kwargs)
        message = self._keyed(message_type, key, content=binary_value(value))
        _set_cas(message, final_args)
        timeout = self._durability(message, final_args, obs_handler)
        return KvRequest(message=message, timeout=timeout_seconds(final_args, timeout), obs_handler=obs_handler)

    def _transcoder(self, final_args: dict[str, Any]) -> Transcoder:
        transcoder: Transcoder = final_args.get('transcoder') or self._core.settings.default_transcoder
        return transcoder

    def _serializer(self, final_args: dict[str, Any]) -> Serializer:
        serializer: Serializer = final_args.get('serializer') or self._core.settings.default_serializer
        return serializer


def _sent_specs(written: tuple[Spec, ...], order: list[int]) -> tuple[SubdocSpec, ...]:
    return tuple(SubdocSpec(opcode=SubDocOp(written[i][0]), path=written[i][1], original_index=i) for i in order)


def _spec_name(spec: Spec) -> str:
    try:
        return SubDocOp(spec[0]).name
    except (IndexError, ValueError):
        return repr(spec)


def _store_semantic(store_semantics: Any) -> kv_pb2.MutateInRequest.StoreSemantic:
    try:
        return _STORE_SEMANTICS[StoreSemantics(store_semantics)]
    except (ValueError, TypeError):
        raise InvalidArgumentException(f'Expected StoreSemantics instead of {store_semantics!r}') from None


def _set_cas(message: _WithCas, final_args: dict[str, Any]) -> None:
    """A CAS of zero is no CAS, as on classic."""
    cas = final_args.get('cas')
    if cas:
        message.cas = cas


def _optional_expiry_secs(final_args: dict[str, Any]) -> int:
    expiry = final_args.get('expiry')
    return 0 if expiry is None else _expiry_secs(expiry)


def _lock_time_secs(lock_time: timedelta) -> int:
    if not isinstance(lock_time, timedelta):
        raise InvalidArgumentException(f'Expected timedelta instead of {lock_time}')
    seconds = int(lock_time.total_seconds())
    if not 0 <= seconds <= _MAX_EXPIRY_SECS:
        raise InvalidArgumentException(f'Expected lock time seconds from 0 to {_MAX_EXPIRY_SECS}, got {seconds}.')
    return seconds


def _expiry_secs(expiry: timedelta) -> int:
    if not isinstance(expiry, timedelta):
        raise InvalidArgumentException(f'Expected timedelta instead of {expiry}')
    seconds = int(expiry.total_seconds())
    if not 0 <= seconds <= _MAX_EXPIRY_SECS:
        raise InvalidArgumentException(f'Expected expiry seconds from 0 (for no expiry) to {_MAX_EXPIRY_SECS}, '
                                       f'got {seconds}.')
    return seconds


def _durability_level(durability: object) -> DurabilityLevel:
    if durability is None:
        return DurabilityLevel.NONE
    if isinstance(durability, ClientDurability):
        raise FeatureUnavailableException('couchbase2 does not support ClientDurability; use ServerDurability.')
    if not isinstance(durability, ServerDurability):
        raise InvalidArgumentException(f'Expected ServerDurability instead of {durability}')
    return DurabilityLevel(durability.level)
