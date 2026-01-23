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
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Optional,
                    Tuple,
                    Union)

from acouchbase.kv_range_scan import AsyncRangeScanRequest
from couchbase.exceptions import InvalidArgumentException
from couchbase.kv_range_scan import (PrefixScan,
                                     RangeScan,
                                     RangeScanRequest,
                                     SamplingScan,
                                     ScanType)
from couchbase.logic.collection_types import (AppendRequest,
                                              CollectionDetails,
                                              DecrementRequest,
                                              ExistsRequest,
                                              GetAllReplicasRequest,
                                              GetAndLockRequest,
                                              GetAndTouchRequest,
                                              GetAnyReplicaRequest,
                                              GetRequest,
                                              IncrementRequest,
                                              InsertRequest,
                                              LookupInAllReplicasRequest,
                                              LookupInAnyReplicaRequest,
                                              LookupInRequest,
                                              MutateInRequest,
                                              PrependRequest,
                                              RemoveRequest,
                                              ReplaceRequest,
                                              TouchRequest,
                                              UnlockRequest,
                                              UpsertRequest)
from couchbase.logic.operation_types import KeyValueOperationType
from couchbase.logic.options import DeltaValueBase, SignedInt64Base
from couchbase.logic.transforms import timedelta_as_microseconds
from couchbase.mutation_state import MutationState
from couchbase.options import forward_args
from couchbase.pycbc_core import operations
from couchbase.subdocument import StoreSemantics, SubDocOp

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop

    from couchbase._utils import JSONType
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


class CollectionRequestBuilder:

    def __init__(self, collection_details: CollectionDetails, loop: Optional[AbstractEventLoop] = None) -> None:
        self._collection_dtls = collection_details
        self._loop = loop

    def _maybe_update_durable_timeout(self, op_args: Dict[str, Any]) -> None:
        if 'durability' in op_args and isinstance(op_args['durability'], int) and 'timeout' not in op_args:
            op_args['timeout'] = timedelta_as_microseconds(timedelta(seconds=10))

    def _process_counter_options(self, *opts: object, **kwargs: object) -> Dict[str, Any]:
        """**INTERNAL**"""
        args = forward_args(kwargs, *opts)
        initial = args.get('initial', None)
        delta = args.get('delta', None)
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
        if initial >= 0:
            args['initial'] = initial
        else:
            args.pop('initial')  # Negative 'initial' means no initial value

        return args

    def _process_binary_value(self, value: Union[str, bytes, bytearray]) -> bytes:
        if isinstance(value, str):
            value = value.encode('utf-8')
        elif isinstance(value, bytearray):
            value = bytes(value)

        if not isinstance(value, bytes):
            raise ValueError('The value provided must of type str, bytes or bytearray.')

        return value

    def build_append_request(self,
                             key: str,
                             value: Union[str, bytes, bytearray],
                             *opts: object,
                             **kwargs: object) -> AppendRequest:
        final_args = forward_args(kwargs, *opts)
        self._maybe_update_durable_timeout(final_args)
        value_bytes = self._process_binary_value(value)
        req = AppendRequest(operations.APPEND.value,
                            key,
                            *self._collection_dtls.get_details(),
                            value=value_bytes,
                            **final_args)
        return req

    def build_decrement_request(self, key: str, *opts: object, **kwargs: object) -> DecrementRequest:
        final_args = self._process_counter_options(*opts, **kwargs)
        req = DecrementRequest(operations.DECREMENT.value,
                               key,
                               *self._collection_dtls.get_details(),
                               **final_args)
        return req

    def build_exists_request(self, key: str, *opts: object, **kwargs: object) -> ExistsRequest:
        final_args = forward_args(kwargs, *opts)
        req = ExistsRequest(operations.EXISTS.value,
                            key,
                            *self._collection_dtls.get_details(),
                            **final_args)
        return req

    def build_get_all_replicas_request(self, key: str, *opts: object, **kwargs: object) -> GetAllReplicasRequest:
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        req = GetAllReplicasRequest(operations.GET_ALL_REPLICAS.value,
                                    key,
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
        req = GetAndLockRequest(operations.GET_AND_LOCK.value,
                                key,
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
        req = GetAndTouchRequest(operations.GET_AND_TOUCH.value,
                                 key,
                                 *self._collection_dtls.get_details(),
                                 transcoder=transcoder,
                                 **final_args)
        return req

    def build_get_any_replica_request(self, key: str, *opts: object, **kwargs: object) -> GetAnyReplicaRequest:
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        req = GetAnyReplicaRequest(operations.GET_ANY_REPLICA.value,
                                   key,
                                   *self._collection_dtls.get_details(),
                                   transcoder=transcoder,
                                   **final_args)
        return req

    def build_get_request(self, key: str, *opts: object, **kwargs: object) -> GetRequest:
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        req = GetRequest(operations.GET.value,
                         key,
                         *self._collection_dtls.get_details(),
                         transcoder=transcoder,
                         **final_args)
        if req.op_name == KeyValueOperationType.GetProject.value:
            req.op_type = operations.GET_PROJECTED.value
        return req

    def build_increment_request(self, key: str, *opts: object, **kwargs: object) -> IncrementRequest:
        final_args = self._process_counter_options(*opts, **kwargs)
        req = IncrementRequest(operations.INCREMENT.value,
                               key,
                               *self._collection_dtls.get_details(),
                               **final_args)
        return req

    def build_insert_request(self,
                             key: str,
                             value: JSONType,
                             *opts: object,
                             **kwargs: object) -> InsertRequest:
        final_args = forward_args(kwargs, *opts)
        self._maybe_update_durable_timeout(final_args)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        transcoded_value = transcoder.encode_value(value)
        req = InsertRequest(operations.INSERT.value,
                            key,
                            *self._collection_dtls.get_details(),
                            value=transcoded_value,
                            **final_args)
        return req

    def build_lookup_in_all_replicas_request(self,
                                             key: str,
                                             spec: Union[List[Spec], Tuple[Spec]],
                                             *opts: object,
                                             **kwargs: object) -> LookupInAllReplicasRequest:
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        req = LookupInAllReplicasRequest(operations.LOOKUP_IN_ALL_REPLICAS.value,
                                         key,
                                         *self._collection_dtls.get_details(),
                                         spec=spec,
                                         transcoder=transcoder,
                                         **final_args)
        return req

    def build_lookup_in_any_replica_request(self,
                                            key: str,
                                            spec: Union[List[Spec], Tuple[Spec]],
                                            *opts: object,
                                            **kwargs: object) -> LookupInAnyReplicaRequest:
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        req = LookupInAnyReplicaRequest(operations.LOOKUP_IN_ANY_REPLICA.value,
                                        key,
                                        *self._collection_dtls.get_details(),
                                        spec=spec,
                                        transcoder=transcoder,
                                        **final_args)
        return req

    def build_lookup_in_request(self,
                                key: str,
                                spec: Union[List[Spec], Tuple[Spec]],
                                *opts: object,
                                **kwargs: object) -> LookupInRequest:
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        req = LookupInRequest(operations.LOOKUP_IN.value,
                              key,
                              *self._collection_dtls.get_details(),
                              spec=spec,
                              transcoder=transcoder,
                              **final_args)
        return req

    def build_mutate_in_request(self,
                                key: str,
                                spec: Union[List[Spec], Tuple[Spec]],
                                *opts: object,
                                **kwargs: object) -> MutateInRequest:
        final_args = forward_args(kwargs, *opts)
        self._maybe_update_durable_timeout(final_args)
        expiry = final_args.get('expiry', None)
        preserve_expiry = final_args.get('preserve_expiry', False)
        spec_ops = [s[0] for s in spec]
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

        final_spec = []
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        for s in spec:
            if len(s) == 6:
                tmp = list(s[:5])
                if ALLOWED_MULTI_OP_LOOKUP.get(s[0], False) is True:
                    new_value = json.dumps(s[5], ensure_ascii=False)
                    # this is an array, need to remove brackets
                    tmp.append(new_value[1:len(new_value)-1].encode('utf-8'))
                else:
                    # no need to propagate the flags
                    tmp.append(transcoder.encode_value(s[5])[0])
                final_spec.append(tuple(tmp))
            else:
                final_spec.append(s)

        req = MutateInRequest(operations.MUTATE_IN.value,
                              key,
                              *self._collection_dtls.get_details(),
                              spec=final_spec,
                              **final_args)
        return req

    def build_prepend_request(self,
                              key: str,
                              value: Union[str, bytes, bytearray],
                              *opts: object,
                              **kwargs: object) -> PrependRequest:
        final_args = forward_args(kwargs, *opts)
        self._maybe_update_durable_timeout(final_args)
        value_bytes = self._process_binary_value(value)
        req = PrependRequest(operations.PREPEND.value,
                             key,
                             *self._collection_dtls.get_details(),
                             value=value_bytes,
                             **final_args)
        return req

    def _process_scan_args(self, scan_type: ScanType, final_args: Dict[str, Any]) -> None:  # noqa: C901
        op_type = None
        if 'concurrency' in final_args and final_args['concurrency'] < 1:
            raise InvalidArgumentException('Concurrency option must be positive')

        if isinstance(scan_type, RangeScan):
            op_type = operations.KV_RANGE_SCAN.value
            if scan_type.start is not None:
                final_args['start'] = scan_type.start.to_dict()
            if scan_type.end is not None:
                final_args['end'] = scan_type.end.to_dict()
        elif isinstance(scan_type, PrefixScan):
            op_type = operations.KV_PREFIX_SCAN.value
            final_args['prefix'] = scan_type.prefix
        elif isinstance(scan_type, SamplingScan):
            op_type = operations.KV_SAMPLING_SCAN.value
            if scan_type.limit <= 0:
                raise InvalidArgumentException('Sampling scan limit must be positive')
            final_args['limit'] = scan_type.limit
            if scan_type.seed is not None:
                final_args['seed'] = scan_type.seed
        else:
            raise InvalidArgumentException('scan_type must be Union[RangeScan, PrefixScan, SamplingScan]')

        final_args['op_type'] = op_type

        consistent_with = final_args.pop('consistent_with', None)
        if consistent_with:
            if not (isinstance(consistent_with, MutationState) and len(consistent_with._sv) > 0):
                raise InvalidArgumentException('Passed empty or invalid mutation state')
            else:
                final_args['consistent_with'] = list(token.as_dict() for token in consistent_with._sv)

    def build_range_scan_request(self, scan_type: ScanType, *opts: object, **kwargs: object) -> RangeScanRequest:
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        self._process_scan_args(scan_type, final_args)
        op_type = final_args.pop('op_type')
        scan_args = self._collection_dtls.get_details_as_dict()
        scan_args.update({
            'transcoder': transcoder,
            'op_type': op_type,
            'op_args': final_args,
        })
        return RangeScanRequest(**scan_args)

    def build_range_scan_async_request(self,
                                       scan_type: ScanType,
                                       *opts: object,
                                       **kwargs: object) -> AsyncRangeScanRequest:
        if not self._loop:
            raise RuntimeError('Cannot create a range scan request if an event loop is not running.')
        final_args = forward_args(kwargs, *opts)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        self._process_scan_args(scan_type, final_args)
        op_type = final_args.pop('op_type')
        scan_args = self._collection_dtls.get_details_as_dict()
        scan_args.update({
            'transcoder': transcoder,
            'op_type': op_type,
            'op_args': final_args,
        })
        return AsyncRangeScanRequest(self._loop, **scan_args)

    def build_remove_request(self,
                             key: str,
                             *opts: object,
                             **kwargs: object) -> RemoveRequest:
        final_args = forward_args(kwargs, *opts)
        self._maybe_update_durable_timeout(final_args)
        req = RemoveRequest(operations.REMOVE.value,
                            key,
                            *self._collection_dtls.get_details(),
                            **final_args)
        return req

    def build_replace_request(self,
                              key: str,
                              value: JSONType,
                              *opts: object,
                              **kwargs: object) -> ReplaceRequest:
        final_args = forward_args(kwargs, *opts)
        expiry = final_args.get('expiry', None)
        preserve_expiry = final_args.get('preserve_expiry', False)
        if expiry and preserve_expiry is True:
            raise InvalidArgumentException(
                'The expiry and preserve_expiry options cannot both be set for replace operations.'
            )
        self._maybe_update_durable_timeout(final_args)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        transcoded_value = transcoder.encode_value(value)
        req = ReplaceRequest(operations.REPLACE.value,
                             key,
                             *self._collection_dtls.get_details(),
                             value=transcoded_value,
                             **final_args)
        return req

    def build_touch_request(self,
                            key: str,
                            expiry: timedelta,
                            *opts: object,
                            **kwargs: object) -> TouchRequest:
        kwargs['expiry'] = expiry
        final_args = forward_args(kwargs, *opts)
        req = TouchRequest(operations.TOUCH.value,
                           key,
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
        req = UnlockRequest(operations.UNLOCK.value,
                            key,
                            *self._collection_dtls.get_details(),
                            **final_args)
        return req

    def build_upsert_request(self,
                             key: str,
                             value: JSONType,
                             *opts: object,
                             **kwargs: object) -> UpsertRequest:
        final_args = forward_args(kwargs, *opts)
        self._maybe_update_durable_timeout(final_args)
        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        transcoded_value = transcoder.encode_value(value)
        req = UpsertRequest(operations.UPSERT.value,
                            key,
                            *self._collection_dtls.get_details(),
                            value=transcoded_value,
                            **final_args)
        return req
