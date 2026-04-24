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

import json
from datetime import timedelta
from enum import IntEnum
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Optional,
                    Tuple,
                    Union)

from acouchbase.kv_range_scan import AsyncRangeScanRequest
from couchbase.constants import FMT_BYTES
from couchbase.durability import DurabilityLevel
from couchbase.exceptions import InvalidArgumentException
from couchbase.kv_range_scan import (PrefixScan,
                                     RangeScan,
                                     RangeScanRequest,
                                     SamplingScan,
                                     ScanType)
from couchbase.logic.collection_types import CollectionDetails
from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import KeyValueOperationCode
from couchbase.logic.options import DeltaValueBase, SignedInt64Base
from couchbase.logic.pycbc_core import pycbc_kv_request as PycbcCoreKeyValueRequest
from couchbase.logic.transforms import timedelta_as_milliseconds
from couchbase.mutation_state import MutationState
from couchbase.options import forward_args
from couchbase.subdocument import (StoreSemantics,
                                   SubDocOp,
                                   build_lookup_in_path_flags,
                                   build_mutate_in_path_flags)
from couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop

    from couchbase._utils import JSONType
    from couchbase.logic.pycbc_core import pycbc_connection
    from couchbase.subdocument import Spec

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


class RangeScanType(IntEnum):
    RangeScan = 1
    PrefixScan = 2
    SamplingScan = 3


class CollectionRequestBuilder:

    def __init__(self, collection_details: CollectionDetails, loop: Optional[AbstractEventLoop] = None) -> None:
        self._collection_dtls = collection_details
        self._loop = loop

    def _create_kv_request(self,
                           opcode: int,
                           key: str,
                           obs_handler: Optional[ObservableRequestHandler]) -> PycbcCoreKeyValueRequest:
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

    def _maybe_update_durable_timeout(self, op_args: Dict[str, Any]) -> None:
        if 'durability' in op_args and isinstance(op_args['durability'], int) and 'timeout' not in op_args:
            op_args['timeout'] = timedelta_as_milliseconds(timedelta(seconds=10))

    def _process_counter_options(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """**INTERNAL**"""
        initial = args.pop('initial', None)
        delta = args.pop('delta', None)
        if not initial:
            initial = SignedInt64Base(0)
        if not delta:
            delta = DeltaValueBase(1)

        # @TODO: remove deprecation next .minor
        if delta is not None:
            if not DeltaValueBase.is_valid(delta):
                raise InvalidArgumentException("Argument is not valid DeltaValue")
        if initial is not None:
            if not SignedInt64Base.is_valid(initial):
                raise InvalidArgumentException("Argument is not valid SignedInt64")

        args['delta'] = int(delta)
        initial = int(initial)
        # Negative 'initial' means no initial value
        if initial >= 0:
            args['initial_value'] = initial  # C++ expects initial_value

    def _process_binary_value(self, value: Union[str, bytes, bytearray]) -> Tuple[bytes, int]:
        if isinstance(value, str):
            value = value.encode('utf-8')
        elif isinstance(value, bytearray):
            value = bytes(value)

        if not isinstance(value, bytes):
            raise ValueError('The value provided must of type str, bytes or bytearray.')

        # we don't use flags for binary ops, but passing the flags around helps reduce logic for request types
        return value, FMT_BYTES

    def _spec_as_dict(self, spec: Union[List, Tuple], original_index: int) -> Dict[str, Any]:
        if len(spec) == 3:
            opcode, path, xattr = spec
            flags = build_lookup_in_path_flags(xattr, False)
            return {
                'opcode': opcode.value,
                'path': path,
                'value': None,
                'flags': flags.value,
                'original_index': original_index
            }
        elif len(spec) == 5:
            opcode, path, create_path, xattr, expand_macro = spec
            flags = build_mutate_in_path_flags(xattr, create_path, expand_macro, False)
            return {
                'opcode': opcode.value,
                'path': path,
                'value': None,
                'flags': flags.value,
                'original_index': original_index
            }
        else:
            opcode, path, create_path, xattr, expand_macro, value = spec
            flags = build_mutate_in_path_flags(xattr, create_path, expand_macro, False)
            return {
                'opcode': opcode.value,
                'path': path,
                'value': value,
                'flags': flags.value,
                'original_index': original_index
            }

    def build_append_request(self,
                             key: str,
                             value: Union[str, bytes, bytearray],
                             obs_handler: Optional[ObservableRequestHandler],
                             *opts: object,
                             **kwargs: object) -> PycbcCoreKeyValueRequest:
        final_args = forward_args(kwargs, *opts)
        self._maybe_update_durable_timeout(final_args)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        value_bytes, flags = self._process_binary_value(value)

        opcode = KeyValueOperationCode.Append.value
        if isinstance(durability, dict):
            opcode = KeyValueOperationCode.AppendWithLegacyDurability.value
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
                                obs_handler: Optional[ObservableRequestHandler],
                                *opts: object,
                                **kwargs: object) -> PycbcCoreKeyValueRequest:
        final_args = forward_args(kwargs, *opts)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        self._process_counter_options(final_args)

        opcode = KeyValueOperationCode.Decrement.value
        if isinstance(durability, dict):
            opcode = KeyValueOperationCode.DecrementWithLegacyDurability.value
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
                             obs_handler: Optional[ObservableRequestHandler],
                             *opts: object,
                             **kwargs: object) -> PycbcCoreKeyValueRequest:
        final_args = forward_args(kwargs, *opts)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        req = self._create_kv_request(KeyValueOperationCode.Exists.value, key, obs_handler)
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req

    def build_get_all_replicas_request(self,
                                       key: str,
                                       obs_handler: Optional[ObservableRequestHandler],
                                       *opts: object,
                                       **kwargs: object) -> Tuple[PycbcCoreKeyValueRequest, Transcoder]:
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        req = self._create_kv_request(KeyValueOperationCode.GetAllReplicas.value, key, obs_handler)
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req, transcoder

    def build_get_and_lock_request(self,
                                   key: str,
                                   lock_time: timedelta,
                                   obs_handler: Optional[ObservableRequestHandler],
                                   *opts: object,
                                   **kwargs: object) -> Tuple[PycbcCoreKeyValueRequest, Transcoder]:
        # add to kwargs for conversion to int
        kwargs['lock_time'] = lock_time
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        req = self._create_kv_request(KeyValueOperationCode.GetAndLock.value, key, obs_handler)
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req, transcoder

    def build_get_and_touch_request(self,
                                    key: str,
                                    expiry: timedelta,
                                    obs_handler: Optional[ObservableRequestHandler],
                                    *opts: object,
                                    **kwargs: object) -> Tuple[PycbcCoreKeyValueRequest, Transcoder]:
        # add to kwargs for conversion to int
        kwargs['expiry'] = expiry
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        req = self._create_kv_request(KeyValueOperationCode.GetAndTouch.value, key, obs_handler)
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req, transcoder

    def build_get_any_replica_request(self,
                                      key: str,
                                      obs_handler: Optional[ObservableRequestHandler],
                                      *opts: object,
                                      **kwargs: object) -> Tuple[PycbcCoreKeyValueRequest, Transcoder]:
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        req = self._create_kv_request(KeyValueOperationCode.GetAnyReplica.value, key, obs_handler)
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req, transcoder

    def build_get_request(self,
                          key: str,
                          obs_handler: Optional[ObservableRequestHandler],
                          *opts: object,
                          **kwargs: object) -> Tuple[PycbcCoreKeyValueRequest, Transcoder]:
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)

        opcode = KeyValueOperationCode.Get.value
        if final_args.get('with_expiry') or 'project' in final_args:
            opcode = KeyValueOperationCode.GetProjected.value
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
                                obs_handler: Optional[ObservableRequestHandler],
                                *opts: object,
                                **kwargs: object) -> PycbcCoreKeyValueRequest:
        final_args = forward_args(kwargs, *opts)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        self._process_counter_options(final_args)

        opcode = KeyValueOperationCode.Increment.value
        if isinstance(durability, dict):
            opcode = KeyValueOperationCode.IncrementWithLegacyDurability.value
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
                             value: JSONType,
                             obs_handler: Optional[ObservableRequestHandler],
                             *opts: object,
                             **kwargs: object) -> PycbcCoreKeyValueRequest:
        final_args = forward_args(kwargs, *opts)
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

        opcode = KeyValueOperationCode.Insert.value
        if isinstance(durability, dict):
            opcode = KeyValueOperationCode.InsertWithLegacyDurability.value
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
                                             specs: Union[List[Spec], Tuple[Spec]],
                                             obs_handler: Optional[ObservableRequestHandler],
                                             *opts: object,
                                             **kwargs: object) -> Tuple[PycbcCoreKeyValueRequest, Transcoder]:
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        final_specs = []
        for idx, spec in enumerate(specs):
            final_specs.append(self._spec_as_dict(spec, idx))
        req = self._create_kv_request(KeyValueOperationCode.LookupInAllReplicas.value, key, obs_handler)
        req.specs = final_specs
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req, transcoder

    def build_lookup_in_any_replica_request(self,
                                            key: str,
                                            specs: Union[List[Spec], Tuple[Spec]],
                                            obs_handler: Optional[ObservableRequestHandler],
                                            *opts: object,
                                            **kwargs: object) -> Tuple[PycbcCoreKeyValueRequest, Transcoder]:
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        final_specs = []
        for idx, spec in enumerate(specs):
            final_specs.append(self._spec_as_dict(spec, idx))
        req = self._create_kv_request(KeyValueOperationCode.LookupInAnyReplica.value, key, obs_handler)
        req.specs = final_specs
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req, transcoder

    def build_lookup_in_request(self,
                                key: str,
                                specs: Union[List[Spec], Tuple[Spec]],
                                obs_handler: Optional[ObservableRequestHandler],
                                *opts: object,
                                **kwargs: object) -> Tuple[PycbcCoreKeyValueRequest, Transcoder]:
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        final_specs = []
        for idx, spec in enumerate(specs):
            final_specs.append(self._spec_as_dict(spec, idx))
        req = self._create_kv_request(KeyValueOperationCode.LookupIn.value, key, obs_handler)
        req.specs = final_specs
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req, transcoder

    def build_mutate_in_request(self,  # noqa: C901
                                key: str,
                                specs: Union[List[Spec], Tuple[Spec]],
                                obs_handler: Optional[ObservableRequestHandler],
                                *opts: object,
                                **kwargs: object) -> PycbcCoreKeyValueRequest:
        final_args = forward_args(kwargs, *opts)
        self._maybe_update_durable_timeout(final_args)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        expiry = final_args.get('expiry', None)
        preserve_expiry = final_args.get('preserve_expiry', False)
        spec_ops = [s[0] for s in specs]
        if SubDocOp.DICT_ADD in spec_ops and preserve_expiry is True:
            raise InvalidArgumentException(
                'The preserve_expiry option cannot be set for mutate_in with insert operations.')
        if SubDocOp.REPLACE in spec_ops and expiry and preserve_expiry is True:
            raise InvalidArgumentException(
                'The expiry and preserve_expiry options cannot both be set for mutate_in with replace operations.')

        semantics = [v for k, v in MUTATE_IN_SEMANTICS.items() if final_args.pop(k, None) is not None]
        if len(semantics) > 1:
            raise InvalidArgumentException("Cannot set multiple store semantics.")
        elif len(semantics) == 1:
            final_args['store_semantics'] = semantics[0]

        final_specs = []
        transcoder = self._collection_dtls.get_request_transcoder(final_args)

        def _json_encode(value) -> Tuple[bytes, int]:
            new_value = json.dumps(value, ensure_ascii=False)
            # this is an array, need to remove brackets
            return new_value[1:len(new_value)-1].encode('utf-8'), FMT_BYTES  # flags are not used

        for idx, spec in enumerate(specs):
            if len(spec) == 6:
                tmp = list(spec[:5])
                spec_value = spec[5]
                # no need to propagate the flags for mutate_in specs
                if ALLOWED_MULTI_OP_LOOKUP.get(spec[0], False) is True:
                    if not obs_handler or obs_handler.is_noop:
                        transcoded_value, _ = _json_encode(spec_value)
                    else:
                        transcoded_value, _ = obs_handler.maybe_add_encoding_span(
                            lambda v=spec_value: _json_encode(v)
                        )
                else:
                    if not obs_handler or obs_handler.is_noop:
                        transcoded_value, _ = transcoder.encode_value(spec_value)
                    else:
                        transcoded_value, _ = obs_handler.maybe_add_encoding_span(
                            lambda v=spec_value: transcoder.encode_value(v)
                        )

                tmp.append(transcoded_value)
                final_specs.append(self._spec_as_dict(tmp, idx))
            else:
                final_specs.append(self._spec_as_dict(spec, idx))

        opcode = KeyValueOperationCode.MutateIn.value
        if isinstance(durability, dict):
            opcode = KeyValueOperationCode.MutateInWithLegacyDurability.value
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
        return req

    def build_prepend_request(self,
                              key: str,
                              value: Union[str, bytes, bytearray],
                              obs_handler: Optional[ObservableRequestHandler],
                              *opts: object,
                              **kwargs: object) -> PycbcCoreKeyValueRequest:
        final_args = forward_args(kwargs, *opts)
        self._maybe_update_durable_timeout(final_args)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        value_bytes, flags = self._process_binary_value(value)

        opcode = KeyValueOperationCode.Prepend.value
        if isinstance(durability, dict):
            opcode = KeyValueOperationCode.PrependWithLegacyDurability.value
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

    def _process_scan_orchestrator_ops(self, orchestrator_opts: Dict[str, Any]) -> None:
        if 'concurrency' in orchestrator_opts and orchestrator_opts['concurrency'] < 1:
            raise InvalidArgumentException('Concurrency option must be positive')

        consistent_with = orchestrator_opts.pop('consistent_with', None)
        if consistent_with:
            if not (isinstance(consistent_with, MutationState) and len(consistent_with._sv) > 0):
                raise InvalidArgumentException('Passed empty or invalid mutation state')
            else:
                orchestrator_opts['consistent_with'] = list(token.as_dict() for token in consistent_with._sv)

    def _get_scan_config(self, scan_type: ScanType) -> Dict[str, Any]:  # noqa: C901
        scan_config = {}
        if isinstance(scan_type, RangeScan):
            scan_type_val = RangeScanType.RangeScan.value
            if scan_type.start is not None:
                scan_config['from'] = scan_type.start.to_dict()
            if scan_type.end is not None:
                scan_config['to'] = scan_type.end.to_dict()
        elif isinstance(scan_type, PrefixScan):
            scan_type_val = RangeScanType.PrefixScan.value
            scan_config['prefix'] = scan_type.prefix
        elif isinstance(scan_type, SamplingScan):
            scan_type_val = RangeScanType.SamplingScan.value
            if scan_type.limit <= 0:
                raise InvalidArgumentException('Sampling scan limit must be positive')
            scan_config['limit'] = scan_type.limit
            if scan_type.seed is not None:
                scan_config['seed'] = scan_type.seed
        else:
            raise InvalidArgumentException('scan_type must be Union[RangeScan, PrefixScan, SamplingScan]')

        scan_config['scan_type'] = scan_type_val
        return scan_config

    def build_range_scan_request(self,
                                 connection: pycbc_connection,
                                 scan_type: ScanType,
                                 *opts: object,
                                 **kwargs: object) -> RangeScanRequest:
        orchestrator_opts = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(orchestrator_opts)
        self._process_scan_orchestrator_ops(orchestrator_opts)
        scan_config = self._get_scan_config(scan_type)
        scan_type = scan_config.pop('scan_type', None)
        if not scan_type:
            raise InvalidArgumentException('Cannot complete range scan operation with scan_type.')
        scan_args = self._collection_dtls.get_details_as_txn_dict()
        scan_args.update({
            'transcoder': transcoder,
            'scan_type': scan_type,
            'scan_config': scan_config,
            'orchestrator_options': orchestrator_opts,
        })
        return RangeScanRequest(connection, **scan_args)

    def build_range_scan_async_request(self,
                                       connection: pycbc_connection,
                                       scan_type: ScanType,
                                       *opts: object,
                                       **kwargs: object) -> AsyncRangeScanRequest:
        if not self._loop:
            raise RuntimeError('Cannot create a range scan request if an event loop is not running.')
        orchestrator_opts = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(orchestrator_opts)
        self._process_scan_orchestrator_ops(orchestrator_opts)
        scan_config = self._get_scan_config(scan_type)
        scan_type = scan_config.pop('scan_type', None)
        if not scan_type:
            raise InvalidArgumentException('Cannot complete range scan operation with scan_type.')
        scan_args = self._collection_dtls.get_details_as_txn_dict()
        scan_args.update({
            'transcoder': transcoder,
            'scan_type': scan_type,
            'scan_config': scan_config,
            'orchestrator_options': orchestrator_opts,
        })
        return AsyncRangeScanRequest(connection, self._loop, **scan_args)

    def build_remove_request(self,
                             key: str,
                             obs_handler: Optional[ObservableRequestHandler],
                             *opts: object,
                             **kwargs: object) -> PycbcCoreKeyValueRequest:
        final_args = forward_args(kwargs, *opts)
        self._maybe_update_durable_timeout(final_args)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)

        opcode = KeyValueOperationCode.Remove.value
        if isinstance(durability, dict):
            opcode = KeyValueOperationCode.RemoveWithLegacyDurability.value
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
                              value: JSONType,
                              obs_handler: Optional[ObservableRequestHandler],
                              *opts: object,
                              **kwargs: object) -> PycbcCoreKeyValueRequest:
        final_args = forward_args(kwargs, *opts)
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
        self._maybe_update_durable_timeout(final_args)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        if not obs_handler or obs_handler.is_noop:
            transcoded_value, flags = transcoder.encode_value(value)
        else:
            transcoded_value, flags = obs_handler.maybe_create_encoding_span(lambda: transcoder.encode_value(value))

        opcode = KeyValueOperationCode.Replace.value
        if isinstance(durability, dict):
            opcode = KeyValueOperationCode.ReplaceWithLegacyDurability.value
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
                            obs_handler: Optional[ObservableRequestHandler],
                            *opts: object,
                            **kwargs: object) -> PycbcCoreKeyValueRequest:
        kwargs['expiry'] = expiry
        final_args = forward_args(kwargs, *opts)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        req = self._create_kv_request(KeyValueOperationCode.Touch.value, key, obs_handler)
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req

    def build_unlock_request(self,
                             key: str,
                             cas: int,
                             obs_handler: Optional[ObservableRequestHandler],
                             *opts: object,
                             **kwargs: object) -> PycbcCoreKeyValueRequest:
        kwargs['cas'] = cas
        final_args = forward_args(kwargs, *opts)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)
        req = self._create_kv_request(KeyValueOperationCode.Unlock.value, key, obs_handler)
        for k, v in final_args.items():
            if v is not None:
                setattr(req, k, v)
        return req

    def build_upsert_request(self,
                             key: str,
                             value: JSONType,
                             obs_handler: Optional[ObservableRequestHandler],
                             *opts: object,
                             **kwargs: object) -> PycbcCoreKeyValueRequest:
        final_args = forward_args(kwargs, *opts)
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

        opcode = KeyValueOperationCode.Upsert.value
        if isinstance(durability, dict):
            opcode = KeyValueOperationCode.UpsertWithLegacyDurability.value
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
