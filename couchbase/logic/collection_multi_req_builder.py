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

from copy import copy
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Optional,
                    Tuple,
                    Type,
                    Union)

from couchbase.constants import FMT_BYTES
from couchbase.durability import DurabilityLevel
from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.collection_multi_types import KeyValueMultiRequest, KeyValueMultiWithTranscoderRequest
from couchbase.logic.collection_types import CollectionDetails
from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import KeyValueMultiOperationCode
from couchbase.logic.options import DeltaValueBase, SignedInt64Base
from couchbase.logic.pycbc_core import pycbc_kv_request as PycbcCoreKeyValueRequest
from couchbase.options import (AppendMultiOptions,
                               DecrementMultiOptions,
                               ExistsMultiOptions,
                               GetAllReplicasMultiOptions,
                               GetAndLockMultiOptions,
                               GetAnyReplicaMultiOptions,
                               GetMultiOptions,
                               IncrementMultiOptions,
                               InsertMultiOptions,
                               MutationMultiOptions,
                               NoValueMultiOptions,
                               PrependMultiOptions,
                               RemoveMultiOptions,
                               ReplaceMultiOptions,
                               TouchMultiOptions,
                               UnlockMultiOptions,
                               UpsertMultiOptions,
                               get_valid_multi_args)
from couchbase.result import MultiGetResult, MultiMutationResult
from couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from datetime import timedelta

    from couchbase._utils import JSONType


_LEGACY_DURABILITY_LOOKUP = {
    KeyValueMultiOperationCode.AppendMulti: KeyValueMultiOperationCode.AppendWithLegacyDurabilityMulti,
    KeyValueMultiOperationCode.DecrementMulti: KeyValueMultiOperationCode.DecrementWithLegacyDurabilityMulti,
    KeyValueMultiOperationCode.IncrementMulti: KeyValueMultiOperationCode.IncrementWithLegacyDurabilityMulti,
    KeyValueMultiOperationCode.InsertMulti: KeyValueMultiOperationCode.InsertWithLegacyDurabilityMulti,
    KeyValueMultiOperationCode.PrependMulti: KeyValueMultiOperationCode.PrependWithLegacyDurabilityMulti,
    KeyValueMultiOperationCode.RemoveMulti: KeyValueMultiOperationCode.RemoveWithLegacyDurabilityMulti,
    KeyValueMultiOperationCode.ReplaceMulti: KeyValueMultiOperationCode.ReplaceWithLegacyDurabilityMulti,
    KeyValueMultiOperationCode.UpsertMulti: KeyValueMultiOperationCode.UpsertWithLegacyDurabilityMulti,
}

_NON_TRANSCODER_OP_LOOKUP = {
    KeyValueMultiOperationCode.RemoveMulti: True,
    KeyValueMultiOperationCode.RemoveWithLegacyDurabilityMulti: True,
    KeyValueMultiOperationCode.TouchMulti: True,
    KeyValueMultiOperationCode.UnlockMulti: True,
}


class CollectionMultiRequestBuilder:

    def __init__(self, collection_details: CollectionDetails) -> None:
        self._collection_dtls = collection_details

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

    def _get_delta_and_initial(self, args: Dict[str, Any]) -> Tuple[int, Optional[int]]:
        """**INTERNAL**"""
        initial = args.pop('initial', None)
        delta = args.pop('delta', None)
        if not initial:
            initial = SignedInt64Base(0)
        if not delta:
            delta = DeltaValueBase(1)

        self._validate_delta_initial(delta=delta, initial=initial)

        args['delta'] = int(delta)
        initial = int(initial)
        if initial >= 0:
            return delta, initial
        else:
            return delta, None

    def _get_multi_binary_mutation_req(self,  # noqa: C901
                                       keys_and_docs: Dict[str, Union[str, bytes, bytearray]],
                                       opts_type: Type[Union[AppendMultiOptions, PrependMultiOptions]],
                                       opcode: KeyValueMultiOperationCode,
                                       obs_handler: Optional[ObservableRequestHandler],
                                       *opts: object,
                                       **kwargs: object) -> KeyValueMultiRequest:

        final_args = get_valid_multi_args(opts_type, kwargs, *opts)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_multi_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)

        if not isinstance(keys_and_docs, dict):
            raise InvalidArgumentException(message='Expected keys_and_docs to be a dict.')

        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        if isinstance(durability, dict):
            opcode = _LEGACY_DURABILITY_LOOKUP[opcode]

        per_key_args = final_args.pop('per_key_options', None)
        return_exceptions = final_args.pop('return_exceptions', True)
        req_opcode = opcode.get_single_op_code()

        requests = []
        for k, v in keys_and_docs.items():
            if isinstance(v, str):
                value = v.encode('utf-8')
            elif isinstance(v, bytearray):
                value = bytes(v)
            else:
                value = v

            if not isinstance(value, bytes):
                raise ValueError('The value provided must of type str, bytes or bytearray.')

            req = self._create_kv_request(req_opcode, k, obs_handler)
            req.value = value
            req.flags = FMT_BYTES

            if isinstance(durability, dict):
                req.persist_to = durability['persist_to']
                req.replicate_to = durability['replicate_to']
            else:
                if durability and obs_handler:
                    obs_handler.add_kv_durability_attribute(DurabilityLevel(durability))
                req.durability_level = durability

            for arg_k, arg_v in final_args.items():
                if arg_v is not None:
                    setattr(req, arg_k, arg_v)
            if per_key_args and k in per_key_args:
                for arg_k, arg_v in per_key_args[k].items():
                    if arg_v is not None:
                        setattr(req, arg_k, arg_v)
            requests.append(req)

        return KeyValueMultiRequest(opcode, requests, return_exceptions)

    def _get_multi_counter_op_req(self,  # noqa: C901
                                  keys: List[str],
                                  opts_type: Type[Union[DecrementMultiOptions, IncrementMultiOptions]],
                                  opcode: KeyValueMultiOperationCode,
                                  obs_handler: Optional[ObservableRequestHandler],
                                  *opts: object,
                                  **kwargs: object) -> KeyValueMultiRequest:
        final_args = get_valid_multi_args(opts_type, kwargs, *opts)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_multi_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)

        if not isinstance(keys, list):
            raise InvalidArgumentException(message='Expected keys to be a list.')

        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        if isinstance(durability, dict):
            opcode = _LEGACY_DURABILITY_LOOKUP[opcode]

        per_key_args = final_args.pop('per_key_options', None)
        return_exceptions = final_args.pop('return_exceptions', True)
        req_opcode = opcode.get_single_op_code()

        global_delta, global_initial = self._get_delta_and_initial(final_args)
        final_args['delta'] = int(global_delta)
        if global_initial is not None:
            final_args['initial_value'] = int(global_initial)

        requests = []
        for k in keys:
            req = self._create_kv_request(req_opcode, k, obs_handler)
            if isinstance(durability, dict):
                req.persist_to = durability['persist_to']
                req.replicate_to = durability['replicate_to']
            else:
                if durability and obs_handler:
                    obs_handler.add_kv_durability_attribute(DurabilityLevel(durability))
                req.durability_level = durability

            for arg_k, arg_v in final_args.items():
                if arg_v is not None:
                    setattr(req, arg_k, arg_v)
            if per_key_args and k in per_key_args:
                pk_delta, pk_initial = self._get_delta_and_initial(per_key_args[k])
                per_key_args[k]['delta'] = int(pk_delta)
                if pk_initial is not None:
                    per_key_args[k]['initial_value'] = int(pk_initial)
                for arg_k, arg_v in per_key_args[k].items():
                    if arg_v is not None:
                        setattr(req, arg_k, arg_v)
            requests.append(req)

        return KeyValueMultiRequest(opcode, requests, return_exceptions)

    def _get_multi_op_mutation_req(self,  # noqa: C901
                                   keys_and_docs: Dict[str, JSONType],
                                   opts_type: Type[MutationMultiOptions],
                                   opcode: KeyValueMultiOperationCode,
                                   obs_handler: Optional[ObservableRequestHandler],
                                   *opts: object,
                                   **kwargs: object
                                   ) -> KeyValueMultiRequest:

        final_args = get_valid_multi_args(opts_type, kwargs, *opts)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_multi_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)

        if not isinstance(keys_and_docs, dict):
            raise InvalidArgumentException(message='Expected keys_and_docs to be a dict.')

        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        if isinstance(durability, dict):
            opcode = _LEGACY_DURABILITY_LOOKUP[opcode]

        per_key_args = final_args.pop('per_key_options', None)
        return_exceptions = final_args.pop('return_exceptions', True)
        req_opcode = opcode.get_single_op_code()

        transcoder = self._collection_dtls.get_request_transcoder(final_args)

        requests = []
        for key, value in keys_and_docs.items():
            req = self._create_kv_request(req_opcode, key, obs_handler)
            if isinstance(durability, dict):
                req.persist_to = durability['persist_to']
                req.replicate_to = durability['replicate_to']
            else:
                if durability and obs_handler:
                    obs_handler.add_kv_durability_attribute(DurabilityLevel(durability))
                req.durability_level = durability

            if per_key_args and key in per_key_args:
                key_transcoder: Transcoder = per_key_args[key].pop('transcoder', transcoder)
                if not obs_handler or obs_handler.is_noop:
                    transcoded_value, flags = key_transcoder.encode_value(value)
                else:
                    transcoded_value, flags = obs_handler.maybe_create_encoding_span(
                        lambda tc=key_transcoder, v=value: tc.encode_value(v)
                    )
            else:
                if not obs_handler or obs_handler.is_noop:
                    transcoded_value, flags = transcoder.encode_value(value)
                else:
                    transcoded_value, flags = obs_handler.maybe_create_encoding_span(
                        lambda tc=transcoder, v=value: tc.encode_value(v)
                    )

            req.value = transcoded_value
            req.flags = flags

            for arg_k, arg_v in final_args.items():
                if arg_v is not None:
                    setattr(req, arg_k, arg_v)
            if per_key_args and key in per_key_args:
                for arg_k, arg_v in per_key_args[key].items():
                    if arg_v is not None:
                        setattr(req, arg_k, arg_v)
            requests.append(req)

        return KeyValueMultiRequest(opcode, requests, return_exceptions)

    def _get_multi_op_non_value_req(self,  # noqa: C901
                                    keys: List[str],
                                    opts_type: Type[NoValueMultiOptions],
                                    opcode: KeyValueMultiOperationCode,
                                    obs_handler: Optional[ObservableRequestHandler],
                                    *opts: object,
                                    **kwargs: object) -> Union[KeyValueMultiRequest,
                                                               KeyValueMultiWithTranscoderRequest]:
        op_keys_cas = kwargs.pop('op_keys_cas', None)
        final_args = get_valid_multi_args(opts_type, kwargs, *opts)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            span=final_args.pop('span', None), parent_span=final_args.pop('parent_span', None)
        )
        if obs_handler:
            obs_handler.create_kv_multi_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)

        if not isinstance(keys, list):
            raise InvalidArgumentException(message='Expected keys to be a list.')

        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        if isinstance(durability, dict):
            opcode = _LEGACY_DURABILITY_LOOKUP[opcode]

        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        per_key_args = final_args.pop('per_key_options', None if op_keys_cas is None else {})
        if op_keys_cas:
            per_key_args.update({k: {'cas': v} for k, v in op_keys_cas.items()})
        return_exceptions = final_args.pop('return_exceptions', True)
        req_opcode = opcode.get_single_op_code()

        requests = []
        key_transcoders = {}
        for k in keys:
            req = self._create_kv_request(req_opcode, k, obs_handler)
            if isinstance(durability, dict):
                req.persist_to = durability['persist_to']
                req.replicate_to = durability['replicate_to']
            else:
                if durability and obs_handler:
                    obs_handler.add_kv_durability_attribute(DurabilityLevel(durability))
                req.durability_level = durability

            for arg_k, arg_v in final_args.items():
                if arg_v is not None:
                    setattr(req, arg_k, arg_v)
            if per_key_args and k in per_key_args:
                key_transcoder = per_key_args[k].pop('transcoder', transcoder)
                key_transcoders[k] = key_transcoder
                for arg_k, arg_v in per_key_args[k].items():
                    if arg_v is not None:
                        setattr(req, arg_k, arg_v)
            else:
                key_transcoders[k] = transcoder

            requests.append(req)

        if _NON_TRANSCODER_OP_LOOKUP.get(opcode, False) is True:
            return KeyValueMultiRequest(opcode, requests, return_exceptions)

        return KeyValueMultiWithTranscoderRequest(opcode, requests, return_exceptions, key_transcoders)

    def _validate_delta_initial(self,
                                delta: Optional[DeltaValueBase] = None,
                                initial: Optional[SignedInt64Base] = None) -> None:
        # @TODO: remove deprecation next .minor
        # from couchbase.collection import DeltaValueDeprecated, SignedInt64Deprecated
        if delta is not None:
            if not DeltaValueBase.is_valid(delta):
                raise InvalidArgumentException("Argument is not valid DeltaValue")
        if initial is not None:
            if not SignedInt64Base.is_valid(initial):
                raise InvalidArgumentException("Argument is not valid SignedInt64")

    def build_append_multi_request(self,
                                   keys_and_docs: Dict[str, Union[str, bytes, bytearray]],
                                   obs_handler: ObservableRequestHandler,
                                   *opts: object,
                                   **kwargs: object) -> KeyValueMultiRequest:
        return self._get_multi_binary_mutation_req(keys_and_docs,
                                                   AppendMultiOptions,
                                                   KeyValueMultiOperationCode.AppendMulti,
                                                   obs_handler,
                                                   *opts,
                                                   **kwargs)

    def build_decrement_multi_request(self,
                                      keys: List[str],
                                      obs_handler: ObservableRequestHandler,
                                      *opts: object,
                                      **kwargs: object) -> KeyValueMultiRequest:
        return self._get_multi_counter_op_req(keys,
                                              DecrementMultiOptions,
                                              KeyValueMultiOperationCode.DecrementMulti,
                                              obs_handler,
                                              *opts,
                                              **kwargs)

    def build_exists_multi_request(self,
                                   keys: List[str],
                                   obs_handler: ObservableRequestHandler,
                                   *opts: object,
                                   **kwargs: object) -> KeyValueMultiRequest:
        return self._get_multi_op_non_value_req(keys,
                                                ExistsMultiOptions,
                                                KeyValueMultiOperationCode.ExistsMulti,
                                                obs_handler,
                                                *opts,
                                                **kwargs)

    def build_get_all_replicas_multi_request(self,
                                             keys: List[str],
                                             obs_handler: ObservableRequestHandler,
                                             *opts: object,
                                             **kwargs: object) -> KeyValueMultiWithTranscoderRequest:
        return self._get_multi_op_non_value_req(keys,
                                                GetAllReplicasMultiOptions,
                                                KeyValueMultiOperationCode.GetAllReplicasMulti,
                                                obs_handler,
                                                *opts,
                                                **kwargs)

    def build_get_and_lock_multi_request(self,
                                         keys: List[str],
                                         lock_time: timedelta,
                                         obs_handler: ObservableRequestHandler,
                                         *opts: object,
                                         **kwargs: object) -> KeyValueMultiWithTranscoderRequest:
        kwargs['lock_time'] = lock_time
        return self._get_multi_op_non_value_req(keys,
                                                GetAndLockMultiOptions,
                                                KeyValueMultiOperationCode.GetAndLockMulti,
                                                obs_handler,
                                                *opts,
                                                **kwargs)

    def build_get_any_replica_multi_request(self,
                                            keys: List[str],
                                            obs_handler: ObservableRequestHandler,
                                            *opts: object,
                                            **kwargs: object) -> KeyValueMultiWithTranscoderRequest:
        return self._get_multi_op_non_value_req(keys,
                                                GetAnyReplicaMultiOptions,
                                                KeyValueMultiOperationCode.GetAnyReplicaMulti,
                                                obs_handler,
                                                *opts,
                                                **kwargs)

    def build_get_multi_request(self,
                                keys: List[str],
                                obs_handler: ObservableRequestHandler,
                                *opts: object,
                                **kwargs: object) -> KeyValueMultiWithTranscoderRequest:
        return self._get_multi_op_non_value_req(keys,
                                                GetMultiOptions,
                                                KeyValueMultiOperationCode.GetMulti,
                                                obs_handler,
                                                *opts,
                                                **kwargs)

    def build_increment_multi_request(self,
                                      keys: List[str],
                                      obs_handler: ObservableRequestHandler,
                                      *opts: object,
                                      **kwargs: object) -> KeyValueMultiRequest:
        return self._get_multi_counter_op_req(keys,
                                              IncrementMultiOptions,
                                              KeyValueMultiOperationCode.IncrementMulti,
                                              obs_handler,
                                              *opts,
                                              **kwargs)

    def build_insert_multi_request(self,
                                   keys_and_docs: Dict[str, JSONType],
                                   obs_handler: ObservableRequestHandler,
                                   *opts: object,
                                   **kwargs: object) -> KeyValueMultiRequest:
        return self._get_multi_op_mutation_req(keys_and_docs,
                                               InsertMultiOptions,
                                               KeyValueMultiOperationCode.InsertMulti,
                                               obs_handler,
                                               *opts,
                                               **kwargs)

    def build_prepend_multi_request(self,
                                    keys_and_docs: Dict[str, Union[str, bytes, bytearray]],
                                    obs_handler: ObservableRequestHandler,
                                    *opts: object,
                                    **kwargs: object) -> KeyValueMultiRequest:
        return self._get_multi_binary_mutation_req(keys_and_docs,
                                                   PrependMultiOptions,
                                                   KeyValueMultiOperationCode.PrependMulti,
                                                   obs_handler,
                                                   *opts,
                                                   **kwargs)

    def build_remove_multi_request(self,
                                   keys: List[str],
                                   obs_handler: ObservableRequestHandler,
                                   *opts: object,
                                   **kwargs: object) -> KeyValueMultiRequest:
        # use the non-mutation logic even though remove is a mutation
        return self._get_multi_op_non_value_req(keys,
                                                RemoveMultiOptions,
                                                KeyValueMultiOperationCode.RemoveMulti,
                                                obs_handler,
                                                *opts,
                                                **kwargs)

    def build_replace_multi_request(self,
                                    keys_and_docs: Dict[str, JSONType],
                                    obs_handler: ObservableRequestHandler,
                                    *opts: object,
                                    **kwargs: object) -> KeyValueMultiRequest:
        return self._get_multi_op_mutation_req(keys_and_docs,
                                               ReplaceMultiOptions,
                                               KeyValueMultiOperationCode.ReplaceMulti,
                                               obs_handler,
                                               *opts,
                                               **kwargs)

    def build_touch_multi_request(self,
                                  keys: List[str],
                                  expiry: timedelta,
                                  obs_handler: ObservableRequestHandler,
                                  *opts: object,
                                  **kwargs: object) -> KeyValueMultiRequest:
        kwargs['expiry'] = expiry
        # use the non-mutation logic even though touch is a mutation
        return self._get_multi_op_non_value_req(keys,
                                                TouchMultiOptions,
                                                KeyValueMultiOperationCode.TouchMulti,
                                                obs_handler,
                                                *opts,
                                                **kwargs)

    def build_unlock_multi_request(self,
                                   keys: List[str],
                                   obs_handler: ObservableRequestHandler,
                                   *opts: object,
                                   **kwargs: object) -> KeyValueMultiRequest:
        op_keys_cas = {}
        if isinstance(keys, dict):
            if not all(map(lambda k: isinstance(k, str), keys.keys())):
                raise InvalidArgumentException('If providing keys of type dict, all values must be type int.')
            if not all(map(lambda v: isinstance(v, int), keys.values())):
                raise InvalidArgumentException('If providing keys of type dict, all values must be type int.')
            op_keys_cas = copy(keys)
        elif isinstance(keys, (MultiGetResult, MultiMutationResult)):
            for k, v in keys.results.items():
                op_keys_cas[k] = v.cas
        else:
            raise InvalidArgumentException(
                'keys type must be Union[MultiGetResult, MultiMutationResult, Dict[str, int].')
        kwargs['op_keys_cas'] = op_keys_cas
        # use the non-mutation logic even though unlock is a mutation
        return self._get_multi_op_non_value_req(list(op_keys_cas.keys()),
                                                UnlockMultiOptions,
                                                KeyValueMultiOperationCode.UnlockMulti,
                                                obs_handler,
                                                *opts,
                                                **kwargs)

    def build_upsert_multi_request(self,
                                   keys_and_docs: Dict[str, JSONType],
                                   obs_handler: ObservableRequestHandler,
                                   *opts: object,
                                   **kwargs: object) -> KeyValueMultiRequest:
        return self._get_multi_op_mutation_req(keys_and_docs,
                                               UpsertMultiOptions,
                                               KeyValueMultiOperationCode.UpsertMulti,
                                               obs_handler,
                                               *opts,
                                               **kwargs)
