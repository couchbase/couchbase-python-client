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

from collections.abc import Iterable
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common._transforms import timedelta_as_milliseconds
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.kv_range_scan import (PrefixScan,
                                            RangeScan,
                                            SamplingScan,
                                            ScanType)
from couchbase.common.options import (AppendOptions,
                                      DecrementOptions,
                                      DeltaValue,
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
                                      ScanOptions,
                                      SignedInt64,
                                      TouchOptions,
                                      UnlockOptions,
                                      UpsertOptions)
from couchbase.common.subdocument import StoreSemantics, SubDocOp
from couchbase.constants import FMT_BYTES
from couchbase.durability import DurabilityLevel
from couchbase.protocol.classic._bindings import KV_OPCODE, KeyValueBinding
from couchbase.protocol.classic._collection_types import CollectionDetails, CollectionOptionsBlock
from couchbase.protocol.classic._pycbc_core import pycbc_kv_request as PycbcCoreKeyValueRequest
from couchbase.protocol.classic._subdocument import build_lookup_in_path_flags, build_mutate_in_path_flags
from couchbase.protocol.classic._wire import forward_args
from couchbase.serializer import Serializer
from couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from couchbase.common._observability.handler import WrappedSpan
    from couchbase.common._observability.observability_types import SpanProtocol
    from couchbase.common._utils import DocumentValue
    from couchbase.common.subdocument import Spec
    from couchbase.protocol.classic._pycbc_core.binding_cpp_types import CppSubdocCommand

# What `ClusterTimeoutOptions` documents for `kv_durable_timeout` when the cluster sets none.
DEFAULT_KV_DURABLE_TIMEOUT = timedelta_as_milliseconds(timedelta(seconds=10))

ALLOWED_MULTI_OP_LOOKUP = {
    SubDocOp.ARRAY_PUSH_FIRST: True,
    SubDocOp.ARRAY_PUSH_LAST: True,
    SubDocOp.ARRAY_INSERT: True,
}

MUTATE_IN_SEMANTICS = {
    'insert_doc': StoreSemantics.INSERT,
    'upsert_doc': StoreSemantics.UPSERT,
    'replace_doc': StoreSemantics.REPLACE
}


class CollectionRequestBuilder:

    def __init__(self, collection_details: CollectionDetails) -> None:
        self._collection_dtls = collection_details

    def _create_kv_request(self,
                           opcode: int,
                           key: str,
                           obs_handler: ObservableRequestHandler | None) -> PycbcCoreKeyValueRequest:
        req = PycbcCoreKeyValueRequest()
        req.opcode = opcode
        req.bucket = self._collection_dtls.bucket_name
        req.scope = self._collection_dtls.scope_name
        req.collection = self._collection_dtls.collection_name
        req.key = key

        if obs_handler:
            # TODO(PYCBC-1746): Update once legacy tracing logic is removed
            if obs_handler.is_legacy_tracer:
                legacy_request_span = obs_handler.legacy_request_span
                if legacy_request_span:
                    req.parent_span = legacy_request_span
            else:
                req.wrapper_span_name = obs_handler.wrapper_span_name
            req.with_metrics = obs_handler.with_metrics
        return req

    def _subdoc_serializer(self, op_args: dict[str, Any]) -> Serializer:
        # No subdocument options block carries `transcoder`, so a caller passing one has had the
        # unknown-option warning.  It is dropped here because the core request has no attribute
        # for it, and a subdocument value is never transcoded.
        op_args.pop('transcoder', None)
        return self._collection_dtls.get_request_serializer(op_args)

    def _maybe_update_durable_timeout(self, op_args: dict[str, Any], durability: Any = None) -> None:
        # A counter or a replace pops its durability before this runs, so it hands the level in.
        durability = op_args.get('durability', durability)
        if isinstance(durability, int) and 'timeout' not in op_args:
            durable_timeout = self._collection_dtls.kv_durable_timeout
            op_args['timeout'] = DEFAULT_KV_DURABLE_TIMEOUT if durable_timeout is None else durable_timeout

    def _process_counter_options(self, args: dict[str, Any]) -> None:
        """**INTERNAL**"""
        initial = args.pop('initial', None)
        delta = args.pop('delta', None)
        if not initial:
            initial = SignedInt64(0)
        if not delta:
            delta = DeltaValue(1)

        # @TODO: remove deprecation next .minor
        if delta is not None:
            if not isinstance(delta, DeltaValue):
                raise InvalidArgumentException("Argument is not valid DeltaValue")
        if initial is not None:
            if not isinstance(initial, SignedInt64):
                raise InvalidArgumentException("Argument is not valid SignedInt64")

        args['delta'] = int(delta)
        initial = int(initial)
        # Negative 'initial' means no initial value
        if initial >= 0:
            args['initial_value'] = initial  # C++ expects initial_value

    def _process_binary_value(self, value: str | bytes | bytearray) -> tuple[bytes, int]:
        if isinstance(value, str):
            value = value.encode('utf-8')
        elif isinstance(value, bytearray):
            value = bytes(value)

        if not isinstance(value, bytes):
            raise ValueError('The value provided must of type str, bytes or bytearray.')

        # we don't use flags for binary ops, but passing the flags around helps reduce logic for request types
        return value, FMT_BYTES

    def _spec_as_dict(self, spec: Spec, original_index: int, value: bytes | None = None) -> CppSubdocCommand:
        if len(spec) == 3:
            opcode, path, xattr = spec
            flags = build_lookup_in_path_flags(xattr, False)
        else:
            opcode, path, create_path, xattr, expand_macro = spec[:5]
            flags = build_mutate_in_path_flags(xattr, create_path, expand_macro, False)
        command: CppSubdocCommand = {
            'opcode': opcode.value,
            'path': path,
            'flags': flags.value,
            'original_index': original_index
        }
        if value is not None:
            command['value'] = value
        return command

    def _build_lookup_in_request(self,
                                 op: Op,
                                 key: str,
                                 specs: Iterable[Spec],
                                 obs_handler: ObservableRequestHandler | None,
                                 final_args: dict[str, Any],
                                 parent_span: SpanProtocol | WrappedSpan | None
                                 ) -> tuple[PycbcCoreKeyValueRequest, Serializer]:
        serializer = self._subdoc_serializer(final_args)
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        req = self._create_kv_request(KV_OPCODE[(op, False)], key, obs_handler)
        req.specs = [self._spec_as_dict(spec, idx) for idx, spec in enumerate(specs)]
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req, serializer

    def build_append_request(self,
                             key: str,
                             value: str | bytes | bytearray,
                             obs_handler: ObservableRequestHandler | None,
                             opts: AppendOptions | None = None,
                             **kwargs: object) -> PycbcCoreKeyValueRequest:
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.Append,
                                  mode=self._collection_dtls.option_validation)
        self._maybe_update_durable_timeout(final_args)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        value_bytes, flags = self._process_binary_value(value)

        opcode = KV_OPCODE[(Op.Append, isinstance(durability, dict))]
        if isinstance(durability, dict):
            final_args['persist_to'] = durability['persist_to']
            final_args['replicate_to'] = durability['replicate_to']
        else:
            if durability and obs_handler:
                obs_handler.add_kv_durability_attribute(DurabilityLevel(durability))
            final_args['durability_level'] = durability

        req = self._create_kv_request(opcode, key, obs_handler)
        req.value = value_bytes
        req.flags = flags
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req

    def build_decrement_request(self,
                                key: str,
                                obs_handler: ObservableRequestHandler | None,
                                opts: DecrementOptions | None = None,
                                **kwargs: object) -> PycbcCoreKeyValueRequest:
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.Decrement,
                                  mode=self._collection_dtls.option_validation)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        self._process_counter_options(final_args)
        self._maybe_update_durable_timeout(final_args, durability)

        opcode = KV_OPCODE[(Op.Decrement, isinstance(durability, dict))]
        if isinstance(durability, dict):
            final_args['persist_to'] = durability['persist_to']
            final_args['replicate_to'] = durability['replicate_to']
        else:
            if durability and obs_handler:
                obs_handler.add_kv_durability_attribute(DurabilityLevel(durability))
            final_args['durability_level'] = durability

        req = self._create_kv_request(opcode, key, obs_handler)
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req

    def build_exists_request(self,
                             key: str,
                             obs_handler: ObservableRequestHandler | None,
                             opts: ExistsOptions | None = None,
                             **kwargs: object) -> PycbcCoreKeyValueRequest:
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.Exists,
                                  mode=self._collection_dtls.option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        req = self._create_kv_request(KV_OPCODE[(Op.Exists, False)], key, obs_handler)
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req

    def build_get_all_replicas_request(self,
                                       key: str,
                                       obs_handler: ObservableRequestHandler | None,
                                       opts: GetAllReplicasOptions | None = None,
                                       **kwargs: object) -> tuple[PycbcCoreKeyValueRequest, Transcoder]:
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.GetAllReplicas,
                                  mode=self._collection_dtls.option_validation)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        req = self._create_kv_request(KV_OPCODE[(Op.GetAllReplicas, False)], key, obs_handler)
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req, transcoder

    def build_get_and_lock_request(self,
                                   key: str,
                                   lock_time: timedelta,
                                   obs_handler: ObservableRequestHandler | None,
                                   opts: GetAndLockOptions | None = None,
                                   **kwargs: object) -> tuple[PycbcCoreKeyValueRequest, Transcoder]:
        # add to kwargs for conversion to int
        kwargs['lock_time'] = lock_time
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.GetAndLock,
                                  mode=self._collection_dtls.option_validation)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        req = self._create_kv_request(KV_OPCODE[(Op.GetAndLock, False)], key, obs_handler)
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req, transcoder

    def build_get_and_touch_request(self,
                                    key: str,
                                    expiry: timedelta,
                                    obs_handler: ObservableRequestHandler | None,
                                    opts: GetAndTouchOptions | None = None,
                                    **kwargs: object) -> tuple[PycbcCoreKeyValueRequest, Transcoder]:
        # add to kwargs for conversion to int
        kwargs['expiry'] = expiry
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.GetAndTouch,
                                  mode=self._collection_dtls.option_validation)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        req = self._create_kv_request(KV_OPCODE[(Op.GetAndTouch, False)], key, obs_handler)
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req, transcoder

    def build_get_any_replica_request(self,
                                      key: str,
                                      obs_handler: ObservableRequestHandler | None,
                                      opts: GetAnyReplicaOptions | None = None,
                                      **kwargs: object) -> tuple[PycbcCoreKeyValueRequest, Transcoder]:
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.GetAnyReplica,
                                  mode=self._collection_dtls.option_validation)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        req = self._create_kv_request(KV_OPCODE[(Op.GetAnyReplica, False)], key, obs_handler)
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req, transcoder

    def build_get_request(self,
                          key: str,
                          obs_handler: ObservableRequestHandler | None,
                          opts: GetOptions | None = None,
                          **kwargs: object) -> tuple[PycbcCoreKeyValueRequest, Transcoder]:
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.Get,
                                  mode=self._collection_dtls.option_validation)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)

        opcode = KV_OPCODE[(Op.Get, False)]
        if final_args.get('with_expiry') or 'project' in final_args:
            opcode = KeyValueBinding.GetProjected
            projections = final_args.pop('project', None)
            if projections:
                if not (isinstance(projections, list) and all(map(lambda p: isinstance(p, str), projections))):
                    raise InvalidArgumentException('Project must be a list of strings.')
                final_args['projections'] = projections

        req = self._create_kv_request(opcode, key, obs_handler)
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req, transcoder

    def build_increment_request(self,
                                key: str,
                                obs_handler: ObservableRequestHandler | None,
                                opts: IncrementOptions | None = None,
                                **kwargs: object) -> PycbcCoreKeyValueRequest:
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.Increment,
                                  mode=self._collection_dtls.option_validation)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        self._process_counter_options(final_args)
        self._maybe_update_durable_timeout(final_args, durability)

        opcode = KV_OPCODE[(Op.Increment, isinstance(durability, dict))]
        if isinstance(durability, dict):
            final_args['persist_to'] = durability['persist_to']
            final_args['replicate_to'] = durability['replicate_to']
        else:
            if durability and obs_handler:
                obs_handler.add_kv_durability_attribute(DurabilityLevel(durability))
            final_args['durability_level'] = durability

        req = self._create_kv_request(opcode, key, obs_handler)
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req

    def build_insert_request(self,
                             key: str,
                             value: DocumentValue,
                             obs_handler: ObservableRequestHandler | None,
                             opts: InsertOptions | None = None,
                             **kwargs: object) -> PycbcCoreKeyValueRequest:
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.Insert,
                                  mode=self._collection_dtls.option_validation)
        self._maybe_update_durable_timeout(final_args)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        if not obs_handler or obs_handler.is_noop:
            transcoded_value, flags = transcoder.encode_value(value)
        else:
            transcoded_value, flags = obs_handler.maybe_create_encoding_span(lambda: transcoder.encode_value(value))

        opcode = KV_OPCODE[(Op.Insert, isinstance(durability, dict))]
        if isinstance(durability, dict):
            final_args['persist_to'] = durability['persist_to']
            final_args['replicate_to'] = durability['replicate_to']
        else:
            if durability and obs_handler:
                obs_handler.add_kv_durability_attribute(DurabilityLevel(durability))
            final_args['durability_level'] = durability

        req = self._create_kv_request(opcode, key, obs_handler)
        req.value = transcoded_value
        req.flags = flags
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req

    def build_lookup_in_all_replicas_request(self,
                                             key: str,
                                             specs: Iterable[Spec],
                                             obs_handler: ObservableRequestHandler | None,
                                             opts: LookupInAllReplicasOptions | None = None,
                                             **kwargs: object) -> tuple[PycbcCoreKeyValueRequest, Serializer]:
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.LookupInAllReplicas,
                                  mode=self._collection_dtls.option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        return self._build_lookup_in_request(Op.LookupInAllReplicas, key, specs, obs_handler, final_args, parent_span)

    def build_lookup_in_any_replica_request(self,
                                            key: str,
                                            specs: Iterable[Spec],
                                            obs_handler: ObservableRequestHandler | None,
                                            opts: LookupInAnyReplicaOptions | None = None,
                                            **kwargs: object) -> tuple[PycbcCoreKeyValueRequest, Serializer]:
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.LookupInAnyReplica,
                                  mode=self._collection_dtls.option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        return self._build_lookup_in_request(Op.LookupInAnyReplica, key, specs, obs_handler, final_args, parent_span)

    def build_lookup_in_request(self,
                                key: str,
                                specs: Iterable[Spec],
                                obs_handler: ObservableRequestHandler | None,
                                opts: LookupInOptions | None = None,
                                **kwargs: object) -> tuple[PycbcCoreKeyValueRequest, Serializer]:
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.LookupIn,
                                  mode=self._collection_dtls.option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        return self._build_lookup_in_request(Op.LookupIn, key, specs, obs_handler, final_args, parent_span)

    def build_mutate_in_request(self,  # noqa: C901
                                key: str,
                                specs: Iterable[Spec],
                                obs_handler: ObservableRequestHandler | None,
                                opts: MutateInOptions | None = None,
                                **kwargs: object) -> tuple[PycbcCoreKeyValueRequest, Serializer]:
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.MutateIn,
                                  mode=self._collection_dtls.option_validation)
        self._maybe_update_durable_timeout(final_args)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        expiry = final_args.get('expiry', None)
        preserve_expiry = final_args.get('preserve_expiry', False)
        # read twice when preserve_expiry is set, so a one-shot iterable has to be materialized first
        specs = tuple(specs)
        if preserve_expiry is True:
            spec_ops = {s[0] for s in specs}
            if SubDocOp.DICT_ADD in spec_ops:
                raise InvalidArgumentException(
                    'The preserve_expiry option cannot be set for mutate_in with insert operations.')
            if SubDocOp.REPLACE in spec_ops and expiry:
                raise InvalidArgumentException(
                    'The expiry and preserve_expiry options cannot both be set for mutate_in '
                    'with replace operations.')

        semantics = [v for k, v in MUTATE_IN_SEMANTICS.items() if final_args.pop(k, None) is not None]
        if len(semantics) > 1:
            raise InvalidArgumentException("Cannot set multiple store semantics.")
        elif len(semantics) == 1:
            final_args['store_semantics'] = semantics[0]

        final_specs = []
        serializer = self._subdoc_serializer(final_args)

        def _encode(value: Any, is_array_op: bool) -> tuple[bytes, int]:
            encoded = serializer.serialize(value)
            if is_array_op:
                # An array op carries its values as a bare list, without the enclosing brackets.
                encoded = encoded.strip()[1:-1]
            return encoded, FMT_BYTES  # a subdocument value carries no flags

        for idx, spec in enumerate(specs):
            encoded_value = None
            if len(spec) == 6:
                spec_value = spec[5]
                is_array_op = ALLOWED_MULTI_OP_LOOKUP.get(spec[0], False) is True
                if not obs_handler or obs_handler.is_noop:
                    encoded_value, _ = _encode(spec_value, is_array_op)
                else:
                    encoded_value, _ = obs_handler.maybe_add_encoding_span(
                        lambda v=spec_value, a=is_array_op: _encode(v, a)
                    )
            final_specs.append(self._spec_as_dict(spec, idx, encoded_value))

        opcode = KV_OPCODE[(Op.MutateIn, isinstance(durability, dict))]
        if isinstance(durability, dict):
            final_args['persist_to'] = durability['persist_to']
            final_args['replicate_to'] = durability['replicate_to']
        else:
            if durability and obs_handler:
                obs_handler.add_kv_durability_attribute(DurabilityLevel(durability))
            final_args['durability_level'] = durability

        req = self._create_kv_request(opcode, key, obs_handler)
        req.specs = final_specs
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req, serializer

    def build_prepend_request(self,
                              key: str,
                              value: str | bytes | bytearray,
                              obs_handler: ObservableRequestHandler | None,
                              opts: PrependOptions | None = None,
                              **kwargs: object) -> PycbcCoreKeyValueRequest:
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.Prepend,
                                  mode=self._collection_dtls.option_validation)
        self._maybe_update_durable_timeout(final_args)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        value_bytes, flags = self._process_binary_value(value)

        opcode = KV_OPCODE[(Op.Prepend, isinstance(durability, dict))]
        if isinstance(durability, dict):
            final_args['persist_to'] = durability['persist_to']
            final_args['replicate_to'] = durability['replicate_to']
        else:
            if durability and obs_handler:
                obs_handler.add_kv_durability_attribute(DurabilityLevel(durability))
            final_args['durability_level'] = durability

        req = self._create_kv_request(opcode, key, obs_handler)
        req.value = value_bytes
        req.flags = flags
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req

    def _process_scan_orchestrator_ops(self, orchestrator_opts: dict[str, Any]) -> None:
        if 'concurrency' in orchestrator_opts and orchestrator_opts['concurrency'] < 1:
            raise InvalidArgumentException('Concurrency option must be positive')

    def _get_scan_type(self, scan_type: ScanType) -> dict[str, Any]:
        # The binding layer reads the scan type as a tagged dict; the tag key matches the
        # discriminator configured for the variant in tools/autogen/config/bindings.yaml.
        if isinstance(scan_type, RangeScan):
            scan_type_dict: dict[str, Any] = {'scan_type': 'range_scan'}
            if scan_type.start is not None:
                scan_type_dict['from'] = scan_type.start.to_dict()
            if scan_type.end is not None:
                scan_type_dict['to'] = scan_type.end.to_dict()
        elif isinstance(scan_type, PrefixScan):
            scan_type_dict = {'scan_type': 'prefix_scan', 'prefix': scan_type.prefix}
        elif isinstance(scan_type, SamplingScan):
            if scan_type.limit <= 0:
                raise InvalidArgumentException('Sampling scan limit must be positive')
            scan_type_dict = {'scan_type': 'sampling_scan', 'limit': scan_type.limit}
            if scan_type.seed is not None:
                scan_type_dict['seed'] = scan_type.seed
        else:
            raise InvalidArgumentException('scan_type must be RangeScan | PrefixScan | SamplingScan')

        return scan_type_dict

    def build_range_scan_args(self,
                              scan_type: ScanType,
                              opts: ScanOptions | None = None,
                              **kwargs: object) -> dict[str, Any]:
        orchestrator_opts = forward_args(kwargs, opts, block=CollectionOptionsBlock.Scan,
                                         mode=self._collection_dtls.option_validation)
        transcoder = self._collection_dtls.get_request_transcoder(orchestrator_opts)
        self._process_scan_orchestrator_ops(orchestrator_opts)
        return {
            **self._collection_dtls.get_details_as_txn_dict(),
            'transcoder': transcoder,
            'scan_type': self._get_scan_type(scan_type),
            'orchestrator_options': orchestrator_opts,
        }

    def build_remove_request(self,
                             key: str,
                             obs_handler: ObservableRequestHandler | None,
                             opts: RemoveOptions | None = None,
                             **kwargs: object) -> PycbcCoreKeyValueRequest:
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.Remove,
                                  mode=self._collection_dtls.option_validation)
        self._maybe_update_durable_timeout(final_args)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)

        opcode = KV_OPCODE[(Op.Remove, isinstance(durability, dict))]
        if isinstance(durability, dict):
            final_args['persist_to'] = durability['persist_to']
            final_args['replicate_to'] = durability['replicate_to']
        else:
            if durability and obs_handler:
                obs_handler.add_kv_durability_attribute(DurabilityLevel(durability))
            final_args['durability_level'] = durability

        req = self._create_kv_request(opcode, key, obs_handler)
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req

    def build_replace_request(self,
                              key: str,
                              value: DocumentValue,
                              obs_handler: ObservableRequestHandler | None,
                              opts: ReplaceOptions | None = None,
                              **kwargs: object) -> PycbcCoreKeyValueRequest:
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.Replace,
                                  mode=self._collection_dtls.option_validation)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        expiry = final_args.get('expiry', None)
        preserve_expiry = final_args.get('preserve_expiry', False)
        if expiry and preserve_expiry is True:
            raise InvalidArgumentException(
                'The expiry and preserve_expiry options cannot both be set for replace operations.'
            )
        self._maybe_update_durable_timeout(final_args, durability)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        if not obs_handler or obs_handler.is_noop:
            transcoded_value, flags = transcoder.encode_value(value)
        else:
            transcoded_value, flags = obs_handler.maybe_create_encoding_span(lambda: transcoder.encode_value(value))

        opcode = KV_OPCODE[(Op.Replace, isinstance(durability, dict))]
        if isinstance(durability, dict):
            final_args['persist_to'] = durability['persist_to']
            final_args['replicate_to'] = durability['replicate_to']
        else:
            if durability and obs_handler:
                obs_handler.add_kv_durability_attribute(DurabilityLevel(durability))
            final_args['durability_level'] = durability

        req = self._create_kv_request(opcode, key, obs_handler)
        req.value = transcoded_value
        req.flags = flags
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req

    def build_touch_request(self,
                            key: str,
                            expiry: timedelta,
                            obs_handler: ObservableRequestHandler | None,
                            opts: TouchOptions | None = None,
                            **kwargs: object) -> PycbcCoreKeyValueRequest:
        kwargs['expiry'] = expiry
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.Touch,
                                  mode=self._collection_dtls.option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        req = self._create_kv_request(KV_OPCODE[(Op.Touch, False)], key, obs_handler)
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req

    def build_unlock_request(self,
                             key: str,
                             cas: int,
                             obs_handler: ObservableRequestHandler | None,
                             opts: UnlockOptions | None = None,
                             **kwargs: object) -> PycbcCoreKeyValueRequest:
        kwargs['cas'] = cas
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.Unlock,
                                  mode=self._collection_dtls.option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        req = self._create_kv_request(KV_OPCODE[(Op.Unlock, False)], key, obs_handler)
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req

    def build_upsert_request(self,
                             key: str,
                             value: DocumentValue,
                             obs_handler: ObservableRequestHandler | None,
                             opts: UpsertOptions | None = None,
                             **kwargs: object) -> PycbcCoreKeyValueRequest:
        final_args = forward_args(kwargs, opts, block=CollectionOptionsBlock.Upsert,
                                  mode=self._collection_dtls.option_validation)
        self._maybe_update_durable_timeout(final_args)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        if not obs_handler or obs_handler.is_noop:
            transcoded_value, flags = transcoder.encode_value(value)
        else:
            transcoded_value, flags = obs_handler.maybe_create_encoding_span(lambda: transcoder.encode_value(value))

        opcode = KV_OPCODE[(Op.Upsert, isinstance(durability, dict))]
        if isinstance(durability, dict):
            final_args['persist_to'] = durability['persist_to']
            final_args['replicate_to'] = durability['replicate_to']
        else:
            if durability and obs_handler:
                obs_handler.add_kv_durability_attribute(DurabilityLevel(durability))
            final_args['durability_level'] = durability

        req = self._create_kv_request(opcode, key, obs_handler)
        req.value = transcoded_value
        req.flags = flags
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req
