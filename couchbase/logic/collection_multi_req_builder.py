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
                    TypeVar,
                    Union)

from couchbase.constants import FMT_BYTES
from couchbase.durability import DurabilityLevel
from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.collection_multi_types import (AppendMultiRequest,
                                                    AppendWithLegacyDurabilityMultiRequest,
                                                    DecrementMultiRequest,
                                                    DecrementWithLegacyDurabilityMultiRequest,
                                                    ExistsMultiRequest,
                                                    GetAllReplicasMultiRequest,
                                                    GetAndLockMultiRequest,
                                                    GetAnyReplicaMultiRequest,
                                                    GetMultiRequest,
                                                    IncrementMultiRequest,
                                                    IncrementWithLegacyDurabilityMultiRequest,
                                                    InsertMultiRequest,
                                                    InsertWithLegacyDurabilityMultiRequest,
                                                    PrependMultiRequest,
                                                    PrependWithLegacyDurabilityMultiRequest,
                                                    RemoveMultiRequest,
                                                    RemoveWithLegacyDurabilityMultiRequest,
                                                    ReplaceMultiRequest,
                                                    ReplaceWithLegacyDurabilityMultiRequest,
                                                    TouchMultiRequest,
                                                    UnlockMultiRequest,
                                                    UpsertMultiRequest,
                                                    UpsertWithLegacyDurabilityMultiRequest)
from couchbase.logic.collection_types import CollectionDetails
from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.options import DeltaValueBase, SignedInt64Base
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

if TYPE_CHECKING:
    from datetime import timedelta

    from couchbase._utils import JSONType


ReqT = TypeVar('T')

LEGACY_DURABILITY_LOOKUP = {
    AppendMultiRequest: AppendWithLegacyDurabilityMultiRequest,
    DecrementMultiRequest: DecrementWithLegacyDurabilityMultiRequest,
    IncrementMultiRequest: IncrementWithLegacyDurabilityMultiRequest,
    InsertMultiRequest: InsertWithLegacyDurabilityMultiRequest,
    PrependMultiRequest: PrependWithLegacyDurabilityMultiRequest,
    RemoveMultiRequest: RemoveWithLegacyDurabilityMultiRequest,
    ReplaceMultiRequest: ReplaceWithLegacyDurabilityMultiRequest,
    UpsertMultiRequest: UpsertWithLegacyDurabilityMultiRequest
}

NON_TRANSCODER_OP_LOOKUP = {
    RemoveMultiRequest: True,
    RemoveWithLegacyDurabilityMultiRequest: True,
    TouchMultiRequest: True,
    UnlockMultiRequest: True,
}


class CollectionMultiRequestBuilder:

    def __init__(self, collection_details: CollectionDetails) -> None:
        self._collection_dtls = collection_details

    def _get_delta_and_initial(self, args: Dict[str, Any]) -> Tuple[int, Optional[int]]:
        """**INTERNAL**"""
        initial = args.get('initial', None)
        delta = args.get('delta', None)
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

    def _get_multi_binary_mutation_req(self,
                                       keys_and_docs: Dict[str, Union[str, bytes, bytearray]],
                                       opts_type: Type[Union[AppendMultiRequest, PrependMultiRequest]],
                                       return_type: Type[ReqT],
                                       obs_handler: ObservableRequestHandler,
                                       *opts: object,
                                       **kwargs: object) -> ReqT:

        final_args = get_valid_multi_args(opts_type, kwargs, *opts)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_kv_multi_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)

        if not isinstance(keys_and_docs, dict):
            raise InvalidArgumentException(message='Expected keys_and_docs to be a dict.')

        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        parsed_keys_and_docs = {}
        for k, v in keys_and_docs.items():
            if isinstance(v, str):
                value = v.encode('utf-8')
            elif isinstance(v, bytearray):
                value = bytes(v)
            else:
                value = v

            if not isinstance(value, bytes):
                raise ValueError('The value provided must of type str, bytes or bytearray.')

            # we don't use flags for binary ops, but passing the flags around helps reduce logic for request types
            parsed_keys_and_docs[k] = value, FMT_BYTES

        if isinstance(durability, dict):
            final_args['persist_to'] = durability['persist_to']
            final_args['replicate_to'] = durability['replicate_to']
            return_type = LEGACY_DURABILITY_LOOKUP[return_type]
        else:
            if durability:
                obs_handler.add_kv_durability_attribute(DurabilityLevel(durability))
            final_args['durability_level'] = durability

        per_key_args = final_args.pop('per_key_options', None)
        return_exceptions = final_args.pop('return_exceptions', True)
        return return_type(*self._collection_dtls.get_details(),
                           doc_list=list(parsed_keys_and_docs.items()),
                           op_args=final_args,
                           per_key_args=per_key_args,
                           return_exceptions=return_exceptions)

    def _get_multi_counter_op_req(self,
                                  keys: List[str],
                                  opts_type: Type[Union[DecrementMultiRequest, IncrementMultiRequest]],
                                  return_type: Type[ReqT],
                                  obs_handler: ObservableRequestHandler,
                                  *opts: object,
                                  **kwargs: object) -> ReqT:
        final_args = get_valid_multi_args(opts_type, kwargs, *opts)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_kv_multi_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)

        if not isinstance(keys, list):
            raise InvalidArgumentException(message='Expected keys to be a list.')

        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        if isinstance(durability, dict):
            final_args['persist_to'] = durability['persist_to']
            final_args['replicate_to'] = durability['replicate_to']
            return_type = LEGACY_DURABILITY_LOOKUP[return_type]
        else:
            if durability:
                obs_handler.add_kv_durability_attribute(DurabilityLevel(durability))
            final_args['durability_level'] = durability

        global_delta, global_initial = self._get_delta_and_initial(final_args)
        final_args['delta'] = int(global_delta)
        if global_initial is not None:
            final_args['initial_value'] = int(global_initial)

        per_key_args = final_args.pop('per_key_options', None)
        if per_key_args:
            for key in per_key_args.keys():
                delta, initial = self._get_delta_and_initial(per_key_args[key])
                if delta is not None:
                    per_key_args[key]['delta'] = int(delta)
                if initial is not None:
                    per_key_args[key]['initial_value'] = int(initial)

        return_exceptions = final_args.pop('return_exceptions', True)
        return return_type(*self._collection_dtls.get_details(),
                           doc_list=keys,
                           op_args=final_args,
                           per_key_args=per_key_args,
                           return_exceptions=return_exceptions)

    def _get_multi_op_mutation_req(self,  # noqa: C901
                                   keys_and_docs: Dict[str, JSONType],
                                   opts_type: Type[MutationMultiOptions],
                                   return_type: Type[ReqT],
                                   obs_handler: ObservableRequestHandler,
                                   *opts: object,
                                   **kwargs: object
                                   ) -> ReqT:

        final_args = get_valid_multi_args(opts_type, kwargs, *opts)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_kv_multi_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)

        if not isinstance(keys_and_docs, dict):
            raise InvalidArgumentException(message='Expected keys_and_docs to be a dict.')

        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        if isinstance(durability, dict):
            final_args['persist_to'] = durability['persist_to']
            final_args['replicate_to'] = durability['replicate_to']
            return_type = LEGACY_DURABILITY_LOOKUP[return_type]
        else:
            if durability:
                obs_handler.add_kv_durability_attribute(DurabilityLevel(durability))
            final_args['durability_level'] = durability

        per_key_args = final_args.pop('per_key_options', None)

        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        return_exceptions = final_args.pop('return_exceptions', True)

        transcoded_keys_and_docs = {}
        for key, value in keys_and_docs.items():
            if per_key_args and key in per_key_args:
                key_transcoder = per_key_args[key].pop('transcoder', transcoder)
                transcoded_value = obs_handler.maybe_add_encoding_span(lambda: key_transcoder.encode_value(value))
            else:
                transcoded_value = obs_handler.maybe_add_encoding_span(lambda: transcoder.encode_value(value))

            transcoded_keys_and_docs[key] = transcoded_value

        if opts_type.__name__ == ReplaceMultiOptions.__name__:
            global_expiry = final_args.get('expiry', None)
            global_preserve_expiry = final_args.get('preserve_expiry', None)
            if global_expiry and global_preserve_expiry is True:
                raise InvalidArgumentException(
                    'The expiry and preserve_expiry options cannot both be set for replace operations.'
                )

            for k, v in (per_key_args or {}).items():
                expiry = v.get('expiry', global_expiry)
                preserve_expiry = v.get('preserve_expiry', global_preserve_expiry)
                if expiry and preserve_expiry is True:
                    raise InvalidArgumentException(
                        message=("The expiry and preserve_expiry options cannot "
                                 f"both be set for replace operations.  Multi-op key: {k}.")
                    )

        return return_type(*self._collection_dtls.get_details(),
                           doc_list=list(transcoded_keys_and_docs.items()),
                           op_args=final_args,
                           per_key_args=per_key_args,
                           return_exceptions=return_exceptions)

    def _get_multi_op_non_value_req(self,
                                    keys: List[str],
                                    opts_type: Type[NoValueMultiOptions],
                                    return_type: Type[ReqT],
                                    obs_handler: ObservableRequestHandler,
                                    *opts: object,
                                    **kwargs: object) -> ReqT:
        op_keys_cas = kwargs.pop('op_keys_cas', None)
        final_args = get_valid_multi_args(opts_type, kwargs, *opts)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_kv_multi_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)

        if not isinstance(keys, list):
            raise InvalidArgumentException(message='Expected keys to be a list.')

        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        if isinstance(durability, dict):
            final_args['persist_to'] = durability['persist_to']
            final_args['replicate_to'] = durability['replicate_to']
            return_type = LEGACY_DURABILITY_LOOKUP[return_type]
        else:
            if durability:
                obs_handler.add_kv_durability_attribute(DurabilityLevel(durability))
            final_args['durability_level'] = durability

        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        return_exceptions = final_args.pop('return_exceptions', True)
        # unlock_multi needs a cas per doc, so we don't want per_key_args to be None for a unlock multi-op
        per_key_args = final_args.pop('per_key_options', None if op_keys_cas is None else {})
        if op_keys_cas:
            per_key_args.update({k: {'cas': v} for k, v in op_keys_cas.items()})
        key_transcoders = {}
        for key in keys:
            if per_key_args and key in per_key_args:
                key_transcoder = per_key_args[key].pop('transcoder', transcoder)
                key_transcoders[key] = key_transcoder
            else:
                key_transcoders[key] = transcoder

        if NON_TRANSCODER_OP_LOOKUP.get(return_type, False) is True:
            return return_type(*self._collection_dtls.get_details(),
                               doc_list=keys,
                               op_args=final_args,
                               per_key_args=per_key_args,
                               return_exceptions=return_exceptions)

        return return_type(*self._collection_dtls.get_details(),
                           doc_list=keys,
                           op_args=final_args,
                           per_key_args=per_key_args,
                           transcoders=key_transcoders,
                           return_exceptions=return_exceptions)

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
                                   **kwargs: object) -> AppendMultiRequest:
        return self._get_multi_binary_mutation_req(keys_and_docs,
                                                   AppendMultiOptions,
                                                   AppendMultiRequest,
                                                   obs_handler,
                                                   *opts,
                                                   **kwargs)

    def build_decrement_multi_request(self,
                                      keys: List[str],
                                      obs_handler: ObservableRequestHandler,
                                      *opts: object,
                                      **kwargs: object) -> DecrementMultiRequest:
        return self._get_multi_counter_op_req(keys,
                                              DecrementMultiOptions,
                                              DecrementMultiRequest,
                                              obs_handler,
                                              *opts,
                                              **kwargs)

    def build_exists_multi_request(self,
                                   keys: List[str],
                                   obs_handler: ObservableRequestHandler,
                                   *opts: object,
                                   **kwargs: object) -> ExistsMultiRequest:
        return self._get_multi_op_non_value_req(keys,
                                                ExistsMultiOptions,
                                                ExistsMultiRequest,
                                                obs_handler,
                                                *opts,
                                                **kwargs)

    def build_get_all_replicas_multi_request(self,
                                             keys: List[str],
                                             obs_handler: ObservableRequestHandler,
                                             *opts: object,
                                             **kwargs: object) -> GetAllReplicasMultiRequest:
        return self._get_multi_op_non_value_req(keys,
                                                GetAllReplicasMultiOptions,
                                                GetAllReplicasMultiRequest,
                                                obs_handler,
                                                *opts,
                                                **kwargs)

    def build_get_and_lock_multi_request(self,
                                         keys: List[str],
                                         lock_time: timedelta,
                                         obs_handler: ObservableRequestHandler,
                                         *opts: object,
                                         **kwargs: object) -> GetAndLockMultiRequest:
        kwargs['lock_time'] = lock_time
        return self._get_multi_op_non_value_req(keys,
                                                GetAndLockMultiOptions,
                                                GetAndLockMultiRequest,
                                                obs_handler,
                                                *opts,
                                                **kwargs)

    def build_get_any_replica_multi_request(self,
                                            keys: List[str],
                                            obs_handler: ObservableRequestHandler,
                                            *opts: object,
                                            **kwargs: object) -> GetAnyReplicaMultiRequest:
        return self._get_multi_op_non_value_req(keys,
                                                GetAnyReplicaMultiOptions,
                                                GetAnyReplicaMultiRequest,
                                                obs_handler,
                                                *opts,
                                                **kwargs)

    def build_get_multi_request(self,
                                keys: List[str],
                                obs_handler: ObservableRequestHandler,
                                *opts: object,
                                **kwargs: object) -> GetMultiRequest:
        return self._get_multi_op_non_value_req(keys, GetMultiOptions, GetMultiRequest, obs_handler, *opts, **kwargs)

    def build_increment_multi_request(self,
                                      keys: List[str],
                                      obs_handler: ObservableRequestHandler,
                                      *opts: object,
                                      **kwargs: object) -> IncrementMultiRequest:
        return self._get_multi_counter_op_req(keys,
                                              IncrementMultiOptions,
                                              IncrementMultiRequest,
                                              obs_handler,
                                              *opts,
                                              **kwargs)

    def build_insert_multi_request(self,
                                   keys_and_docs: Dict[str, JSONType],
                                   obs_handler: ObservableRequestHandler,
                                   *opts: object,
                                   **kwargs: object) -> InsertMultiRequest:
        return self._get_multi_op_mutation_req(keys_and_docs,
                                               InsertMultiOptions,
                                               InsertMultiRequest,
                                               obs_handler,
                                               *opts,
                                               **kwargs)

    def build_prepend_multi_request(self,
                                    keys_and_docs: Dict[str, Union[str, bytes, bytearray]],
                                    obs_handler: ObservableRequestHandler,
                                    *opts: object,
                                    **kwargs: object) -> PrependMultiRequest:
        return self._get_multi_binary_mutation_req(keys_and_docs,
                                                   PrependMultiOptions,
                                                   PrependMultiRequest,
                                                   obs_handler,
                                                   *opts,
                                                   **kwargs)

    def build_remove_multi_request(self,
                                   keys: List[str],
                                   obs_handler: ObservableRequestHandler,
                                   *opts: object,
                                   **kwargs: object) -> RemoveMultiRequest:
        # use the non-mutation logic even though remove is a mutation
        return self._get_multi_op_non_value_req(keys,
                                                RemoveMultiOptions,
                                                RemoveMultiRequest,
                                                obs_handler,
                                                *opts,
                                                **kwargs)

    def build_replace_multi_request(self,
                                    keys_and_docs: Dict[str, JSONType],
                                    obs_handler: ObservableRequestHandler,
                                    *opts: object,
                                    **kwargs: object) -> ReplaceMultiRequest:
        return self._get_multi_op_mutation_req(keys_and_docs,
                                               ReplaceMultiOptions,
                                               ReplaceMultiRequest,
                                               obs_handler,
                                               *opts,
                                               **kwargs)

    def build_touch_multi_request(self,
                                  keys: List[str],
                                  expiry: timedelta,
                                  obs_handler: ObservableRequestHandler,
                                  *opts: object,
                                  **kwargs: object) -> TouchMultiRequest:
        kwargs['expiry'] = expiry
        # use the non-mutation logic even though touch is a mutation
        return self._get_multi_op_non_value_req(keys,
                                                TouchMultiOptions,
                                                TouchMultiRequest,
                                                obs_handler,
                                                *opts,
                                                **kwargs)

    def build_unlock_multi_request(self,
                                   keys: List[str],
                                   obs_handler: ObservableRequestHandler,
                                   *opts: object,
                                   **kwargs: object) -> UnlockMultiRequest:
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
                                                UnlockMultiRequest,
                                                obs_handler,
                                                *opts,
                                                **kwargs)

    def build_upsert_multi_request(self,
                                   keys_and_docs: Dict[str, JSONType],
                                   obs_handler: ObservableRequestHandler,
                                   *opts: object,
                                   **kwargs: object) -> UpsertMultiRequest:
        return self._get_multi_op_mutation_req(keys_and_docs,
                                               UpsertMultiOptions,
                                               UpsertMultiRequest,
                                               obs_handler,
                                               *opts,
                                               **kwargs)
