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
from couchbase.exceptions import InvalidArgumentException
from couchbase.kv_range_scan import (PrefixScan,
                                     RangeScan,
                                     RangeScanRequest,
                                     SamplingScan,
                                     ScanType)
from couchbase.logic.collection_types import (AppendRequest,
                                              AppendWithLegacyDurabilityRequest,
                                              CollectionDetails,
                                              DecrementRequest,
                                              DecrementWithLegacyDurabilityRequest,
                                              ExistsRequest,
                                              GetAllReplicasRequest,
                                              GetAndLockRequest,
                                              GetAndTouchRequest,
                                              GetAnyReplicaRequest,
                                              GetProjectedRequest,
                                              GetRequest,
                                              IncrementRequest,
                                              IncrementWithLegacyDurabilityRequest,
                                              InsertRequest,
                                              InsertWithLegacyDurabilityRequest,
                                              LookupInAllReplicasRequest,
                                              LookupInAnyReplicaRequest,
                                              LookupInRequest,
                                              MutateInRequest,
                                              MutateInWithLegacyDurabilityRequest,
                                              PrependRequest,
                                              PrependWithLegacyDurabilityRequest,
                                              RemoveRequest,
                                              RemoveWithLegacyDurabilityRequest,
                                              ReplaceRequest,
                                              ReplaceWithLegacyDurabilityRequest,
                                              TouchRequest,
                                              UnlockRequest,
                                              UpsertRequest,
                                              UpsertWithLegacyDurabilityRequest)
from couchbase.logic.options import DeltaValueBase, SignedInt64Base
from couchbase.logic.transforms import timedelta_as_milliseconds
from couchbase.mutation_state import MutationState
from couchbase.options import forward_args
from couchbase.subdocument import (StoreSemantics,
                                   SubDocOp,
                                   build_lookup_in_path_flags,
                                   build_mutate_in_path_flags)

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

    def _maybe_update_durable_timeout(self, op_args: Dict[str, Any]) -> None:
        if 'durability' in op_args and isinstance(op_args['durability'], int) and 'timeout' not in op_args:
            op_args['timeout'] = timedelta_as_milliseconds(timedelta(seconds=10))

    def _process_counter_options(self, *opts: object, **kwargs: object) -> Dict[str, Any]:
        """**INTERNAL**"""
        args = forward_args(kwargs, *opts)
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

        return args

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
                             *opts: object,
                             **kwargs: object) -> Union[AppendRequest, AppendWithLegacyDurabilityRequest]:
        final_args = forward_args(kwargs, *opts)
        self._maybe_update_durable_timeout(final_args)
        value_bytes, flags = self._process_binary_value(value)
        durability = final_args.pop('durability', None)
        if isinstance(durability, dict):
            req = AppendWithLegacyDurabilityRequest(key,
                                                    *self._collection_dtls.get_details(),
                                                    value=value_bytes,
                                                    flags=flags,
                                                    persist_to=durability['persist_to'],
                                                    replicate_to=durability['replicate_to'],
                                                    **final_args)
        else:
            req = AppendRequest(key,
                                *self._collection_dtls.get_details(),
                                value=value_bytes,
                                flags=flags,
                                durability_level=durability,
                                **final_args)
        return req

    def build_decrement_request(self,
                                key: str,
                                *opts: object,
                                **kwargs: object) -> Union[DecrementRequest, DecrementWithLegacyDurabilityRequest]:
        final_args = self._process_counter_options(*opts, **kwargs)
        durability = final_args.pop('durability', None)
        if isinstance(durability, dict):
            req = DecrementWithLegacyDurabilityRequest(key,
                                                       *self._collection_dtls.get_details(),
                                                       persist_to=durability['persist_to'],
                                                       replicate_to=durability['replicate_to'],
                                                       **final_args)
        else:
            req = DecrementRequest(key,
                                   *self._collection_dtls.get_details(),
                                   durability_level=durability,
                                   **final_args)
        return req

    def build_exists_request(self, key: str, *opts: object, **kwargs: object) -> ExistsRequest:
        final_args = forward_args(kwargs, *opts)
        req = ExistsRequest(key,
                            *self._collection_dtls.get_details(),
                            **final_args)
        return req

    def build_get_all_replicas_request(self, key: str, *opts: object, **kwargs: object) -> GetAllReplicasRequest:
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        req = GetAllReplicasRequest(key,
                                    *self._collection_dtls.get_details(),
                                    transcoder=transcoder,
                                    **final_args)
        return req

    def build_get_and_lock_request(self,
                                   key: str,
                                   lock_time: timedelta,
                                   *opts: object,
                                   **kwargs: object) -> GetAndLockRequest:
        # add to kwargs for conversion to int
        kwargs['lock_time'] = lock_time
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        req = GetAndLockRequest(key,
                                *self._collection_dtls.get_details(),
                                transcoder=transcoder,
                                **final_args)
        return req

    def build_get_and_touch_request(self,
                                    key: str,
                                    expiry: timedelta,
                                    *opts: object,
                                    **kwargs: object) -> GetAndTouchRequest:
        # add to kwargs for conversion to int
        kwargs['expiry'] = expiry
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        req = GetAndTouchRequest(key,
                                 *self._collection_dtls.get_details(),
                                 transcoder=transcoder,
                                 **final_args)
        return req

    def build_get_any_replica_request(self, key: str, *opts: object, **kwargs: object) -> GetAnyReplicaRequest:
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        req = GetAnyReplicaRequest(key,
                                   *self._collection_dtls.get_details(),
                                   transcoder=transcoder,
                                   **final_args)
        return req

    def build_get_request(self, key: str, *opts: object, **kwargs: object) -> Union[GetProjectedRequest, GetRequest]:
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        if final_args.get('with_expiry') or 'project' in final_args:
            projections = final_args.pop('project', None)
            if projections:
                if not (isinstance(projections, list) and all(map(lambda p: isinstance(p, str), projections))):
                    raise InvalidArgumentException('Project must be a list of strings.')
                final_args['projections'] = projections

            req = GetProjectedRequest(key,
                                      *self._collection_dtls.get_details(),
                                      transcoder=transcoder,
                                      **final_args)
        else:
            req = GetRequest(key,
                             *self._collection_dtls.get_details(),
                             transcoder=transcoder,
                             **final_args)
        return req

    def build_increment_request(self,
                                key: str,
                                *opts: object,
                                **kwargs: object) -> Union[IncrementRequest, IncrementWithLegacyDurabilityRequest]:
        final_args = self._process_counter_options(*opts, **kwargs)
        durability = final_args.pop('durability', None)
        if isinstance(durability, dict):
            req = IncrementWithLegacyDurabilityRequest(key,
                                                       *self._collection_dtls.get_details(),
                                                       persist_to=durability['persist_to'],
                                                       replicate_to=durability['replicate_to'],
                                                       **final_args)
        else:
            req = IncrementRequest(key,
                                   *self._collection_dtls.get_details(),
                                   durability_level=durability,
                                   **final_args)
        return req

    def build_insert_request(self,
                             key: str,
                             value: JSONType,
                             *opts: object,
                             **kwargs: object) -> Union[InsertRequest, InsertWithLegacyDurabilityRequest]:
        final_args = forward_args(kwargs, *opts)
        self._maybe_update_durable_timeout(final_args)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        transcoded_value, flags = transcoder.encode_value(value)
        durability = final_args.pop('durability', None)
        if isinstance(durability, dict):
            req = InsertWithLegacyDurabilityRequest(key,
                                                    *self._collection_dtls.get_details(),
                                                    value=transcoded_value,
                                                    flags=flags,
                                                    persist_to=durability['persist_to'],
                                                    replicate_to=durability['replicate_to'],
                                                    **final_args)
        else:
            req = InsertRequest(key,
                                *self._collection_dtls.get_details(),
                                value=transcoded_value,
                                flags=flags,
                                durability_level=durability,
                                **final_args)
        return req

    def build_lookup_in_all_replicas_request(self,
                                             key: str,
                                             specs: Union[List[Spec], Tuple[Spec]],
                                             *opts: object,
                                             **kwargs: object) -> LookupInAllReplicasRequest:
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        final_specs = []
        for idx, spec in enumerate(specs):
            final_specs.append(self._spec_as_dict(spec, idx))
        req = LookupInAllReplicasRequest(key,
                                         *self._collection_dtls.get_details(),
                                         specs=final_specs,
                                         transcoder=transcoder,
                                         **final_args)
        return req

    def build_lookup_in_any_replica_request(self,
                                            key: str,
                                            specs: Union[List[Spec], Tuple[Spec]],
                                            *opts: object,
                                            **kwargs: object) -> LookupInAnyReplicaRequest:
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        final_specs = []
        for idx, spec in enumerate(specs):
            final_specs.append(self._spec_as_dict(spec, idx))
        req = LookupInAnyReplicaRequest(key,
                                        *self._collection_dtls.get_details(),
                                        specs=final_specs,
                                        transcoder=transcoder,
                                        **final_args)
        return req

    def build_lookup_in_request(self,
                                key: str,
                                specs: Union[List[Spec], Tuple[Spec]],
                                *opts: object,
                                **kwargs: object) -> LookupInRequest:
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        final_specs = []
        for idx, spec in enumerate(specs):
            final_specs.append(self._spec_as_dict(spec, idx))
        req = LookupInRequest(key,
                              *self._collection_dtls.get_details(),
                              specs=final_specs,
                              transcoder=transcoder,
                              **final_args)
        return req

    def build_mutate_in_request(self,
                                key: str,
                                specs: Union[List[Spec], Tuple[Spec]],
                                *opts: object,
                                **kwargs: object) -> Union[MutateInRequest, MutateInWithLegacyDurabilityRequest]:
        final_args = forward_args(kwargs, *opts)
        self._maybe_update_durable_timeout(final_args)
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
        for idx, spec in enumerate(specs):
            if len(spec) == 6:
                tmp = list(spec[:5])
                if ALLOWED_MULTI_OP_LOOKUP.get(spec[0], False) is True:
                    new_value = json.dumps(spec[5], ensure_ascii=False)
                    # this is an array, need to remove brackets
                    tmp.append(new_value[1:len(new_value)-1].encode('utf-8'))
                else:
                    # no need to propagate the flags
                    tmp.append(transcoder.encode_value(spec[5])[0])
                final_specs.append(self._spec_as_dict(tmp, idx))
                # final_specs.append(tuple(tmp))
            else:
                final_specs.append(self._spec_as_dict(spec, idx))
                # final_specs.append(s)

        durability = final_args.pop('durability', None)
        if isinstance(durability, dict):
            req = MutateInWithLegacyDurabilityRequest(key,
                                                      *self._collection_dtls.get_details(),
                                                      specs=final_specs,
                                                      persist_to=durability['persist_to'],
                                                      replicate_to=durability['replicate_to'],
                                                      **final_args)
        else:
            req = MutateInRequest(key,
                                  *self._collection_dtls.get_details(),
                                  specs=final_specs,
                                  durability_level=durability,
                                  **final_args)
        return req

    def build_prepend_request(self,
                              key: str,
                              value: Union[str, bytes, bytearray],
                              *opts: object,
                              **kwargs: object) -> Union[PrependRequest, PrependWithLegacyDurabilityRequest]:
        final_args = forward_args(kwargs, *opts)
        self._maybe_update_durable_timeout(final_args)
        value_bytes, flags = self._process_binary_value(value)
        durability = final_args.pop('durability', None)
        if isinstance(durability, dict):
            req = PrependWithLegacyDurabilityRequest(key,
                                                     *self._collection_dtls.get_details(),
                                                     value=value_bytes,
                                                     flags=flags,
                                                     persist_to=durability['persist_to'],
                                                     replicate_to=durability['replicate_to'],
                                                     **final_args)
        else:
            req = PrependRequest(key,
                                 *self._collection_dtls.get_details(),
                                 value=value_bytes,
                                 flags=flags,
                                 durability_level=durability,
                                 **final_args)
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
        scan_args = self._collection_dtls.get_details_as_dict()
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
        scan_args = self._collection_dtls.get_details_as_dict()
        scan_args.update({
            'transcoder': transcoder,
            'scan_type': scan_type,
            'scan_config': scan_config,
            'orchestrator_options': orchestrator_opts,
        })
        return AsyncRangeScanRequest(connection, self._loop, **scan_args)

    def build_remove_request(self,
                             key: str,
                             *opts: object,
                             **kwargs: object) -> Union[RemoveRequest, RemoveWithLegacyDurabilityRequest]:
        final_args = forward_args(kwargs, *opts)
        self._maybe_update_durable_timeout(final_args)
        durability = final_args.pop('durability', None)
        if isinstance(durability, dict):
            req = RemoveWithLegacyDurabilityRequest(key,
                                                    *self._collection_dtls.get_details(),
                                                    persist_to=durability['persist_to'],
                                                    replicate_to=durability['replicate_to'],
                                                    **final_args)
        else:
            req = RemoveRequest(key,
                                *self._collection_dtls.get_details(),
                                durability_level=durability,
                                **final_args)
        return req

    def build_replace_request(self,
                              key: str,
                              value: JSONType,
                              *opts: object,
                              **kwargs: object) -> Union[ReplaceRequest, ReplaceWithLegacyDurabilityRequest]:
        final_args = forward_args(kwargs, *opts)
        expiry = final_args.get('expiry', None)
        preserve_expiry = final_args.get('preserve_expiry', False)
        if expiry and preserve_expiry is True:
            raise InvalidArgumentException(
                'The expiry and preserve_expiry options cannot both be set for replace operations.'
            )
        self._maybe_update_durable_timeout(final_args)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        transcoded_value, flags = transcoder.encode_value(value)
        durability = final_args.pop('durability', None)
        if isinstance(durability, dict):
            req = ReplaceWithLegacyDurabilityRequest(key,
                                                     *self._collection_dtls.get_details(),
                                                     value=transcoded_value,
                                                     flags=flags,
                                                     persist_to=durability['persist_to'],
                                                     replicate_to=durability['replicate_to'],
                                                     **final_args)
        else:
            req = ReplaceRequest(key,
                                 *self._collection_dtls.get_details(),
                                 value=transcoded_value,
                                 flags=flags,
                                 durability_level=durability,
                                 **final_args)
        return req

    def build_touch_request(self,
                            key: str,
                            expiry: timedelta,
                            *opts: object,
                            **kwargs: object) -> TouchRequest:
        kwargs['expiry'] = expiry
        final_args = forward_args(kwargs, *opts)
        req = TouchRequest(key,
                           *self._collection_dtls.get_details(),
                           **final_args)
        return req

    def build_unlock_request(self,
                             key: str,
                             cas: int,
                             *opts: object,
                             **kwargs: object) -> UnlockRequest:
        kwargs['cas'] = cas
        final_args = forward_args(kwargs, *opts)
        req = UnlockRequest(key,
                            *self._collection_dtls.get_details(),
                            **final_args)
        return req

    def build_upsert_request(self,
                             key: str,
                             value: JSONType,
                             *opts: object,
                             **kwargs: object) -> Union[UpsertRequest, UpsertWithLegacyDurabilityRequest]:
        final_args = forward_args(kwargs, *opts)
        self._maybe_update_durable_timeout(final_args)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        transcoded_value, flags = transcoder.encode_value(value)
        durability = final_args.pop('durability', None)
        if isinstance(durability, dict):
            req = UpsertWithLegacyDurabilityRequest(key,
                                                    *self._collection_dtls.get_details(),
                                                    value=transcoded_value,
                                                    flags=flags,
                                                    persist_to=durability['persist_to'],
                                                    replicate_to=durability['replicate_to'],
                                                    **final_args)
        else:
            req = UpsertRequest(key,
                                *self._collection_dtls.get_details(),
                                value=transcoded_value,
                                flags=flags,
                                durability_level=durability,
                                **final_args)
        return req
