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

from collections.abc import Mapping
from copy import copy
from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common.exceptions import InternalSDKException, InvalidArgumentException
from couchbase.common.options import (AppendMultiOptions,
                                      DecrementMultiOptions,
                                      DeltaValue,
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
                                      SignedInt64,
                                      TouchMultiOptions,
                                      UnlockMultiOptions,
                                      UpsertMultiOptions)
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.common.result import MultiGetResult, MultiMutationResult
from couchbase.constants import FMT_BYTES
from couchbase.durability import DurabilityLevel
from couchbase.protocol.classic._bindings import (KV_OPCODE,
                                                  KeyValueBinding,
                                                  KeyValueMultiBinding)
from couchbase.protocol.classic._collection_multi_types import KeyValueMultiRequest, KeyValueMultiWithTranscoderRequest
from couchbase.protocol.classic._collection_req_builder import DEFAULT_KV_DURABLE_TIMEOUT
from couchbase.protocol.classic._collection_types import CollectionDetails
from couchbase.protocol.classic._pycbc_core import pycbc_kv_request as PycbcCoreKeyValueRequest
from couchbase.protocol.classic._wire import get_valid_multi_args
from couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from datetime import timedelta

    from couchbase.common._utils import DocumentValue


def _without_transcoders(req: KeyValueMultiWithTranscoderRequest) -> KeyValueMultiRequest:
    """For an operation whose response has no document to decode."""
    return KeyValueMultiRequest(req.opcode, req.request_list, req.return_exceptions)


def _refuse_projection(final_args: dict[str, Any], per_key_args: dict[str, dict[str, Any]] | None) -> None:
    """Each key of a multi get goes out as a plain get, which carries no projection."""
    if any(args.get('project') is not None for args in (final_args, *(per_key_args or {}).values())):
        raise InvalidArgumentException('A multi get cannot project fields; use get for each key instead.')


def _multi_opcodes(op: Op, legacy: bool) -> tuple[KeyValueMultiBinding, KeyValueBinding]:
    """The binding for the whole batch, and the one each key's own request carries."""
    multi = KV_OPCODE[(op, legacy)]
    single = KV_OPCODE[(op.single, legacy)] if op.single is not None else None
    if not isinstance(multi, KeyValueMultiBinding) or not isinstance(single, KeyValueBinding):
        raise InternalSDKException(message=f'{op} has no multi and single binding pair.')
    return multi, single


class CollectionMultiRequestBuilder:

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

    def _maybe_update_durable_timeout(self, op_args: dict[str, Any], durability: Any) -> None:
        if isinstance(durability, int) and op_args.get('timeout') is None:
            durable_timeout = self._collection_dtls.kv_durable_timeout
            op_args['timeout'] = DEFAULT_KV_DURABLE_TIMEOUT if durable_timeout is None else durable_timeout

    def _get_delta_and_initial(self, args: dict[str, Any]) -> tuple[int, int | None]:
        """**INTERNAL**"""
        initial = args.pop('initial', None)
        delta = args.pop('delta', None)
        if not initial:
            initial = SignedInt64(0)
        if not delta:
            delta = DeltaValue(1)

        self._validate_delta_initial(delta=delta, initial=initial)

        args['delta'] = int(delta)
        initial = int(initial)
        if initial >= 0:
            return delta, initial
        else:
            return delta, None

    def _get_multi_binary_mutation_req(self,  # noqa: C901
                                       keys_and_docs: Mapping[str, str | bytes | bytearray],
                                       opts_type: type[AppendMultiOptions | PrependMultiOptions],
                                       op: Op,
                                       obs_handler: ObservableRequestHandler | None,
                                       opts: dict[str, Any] | None,
                                       **kwargs: object) -> KeyValueMultiRequest:

        final_args = get_valid_multi_args(opts_type, kwargs, opts,
                                          protocol=CouchbaseProtocol.CLASSIC,
                                          mode=self._collection_dtls.option_validation)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            parent_span=final_args.pop('parent_span', None))
        if obs_handler:
            obs_handler.create_kv_multi_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)

        if not isinstance(keys_and_docs, Mapping):
            raise InvalidArgumentException(message='Expected keys_and_docs to be a mapping.')

        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        per_key_args = final_args.pop('per_key_options', None)
        return_exceptions = final_args.pop('return_exceptions', True)
        legacy = isinstance(durability, dict)
        self._maybe_update_durable_timeout(final_args, durability)
        opcode, req_opcode = _multi_opcodes(op, legacy)

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
                                  keys: list[str],
                                  opts_type: type[DecrementMultiOptions | IncrementMultiOptions],
                                  op: Op,
                                  obs_handler: ObservableRequestHandler | None,
                                  opts: dict[str, Any] | None,
                                  **kwargs: object) -> KeyValueMultiRequest:
        final_args = get_valid_multi_args(opts_type, kwargs, opts,
                                          protocol=CouchbaseProtocol.CLASSIC,
                                          mode=self._collection_dtls.option_validation)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            parent_span=final_args.pop('parent_span', None))
        if obs_handler:
            obs_handler.create_kv_multi_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)

        if not isinstance(keys, list):
            raise InvalidArgumentException(message='Expected keys to be a list.')

        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        per_key_args = final_args.pop('per_key_options', None)
        return_exceptions = final_args.pop('return_exceptions', True)
        legacy = isinstance(durability, dict)
        self._maybe_update_durable_timeout(final_args, durability)
        opcode, req_opcode = _multi_opcodes(op, legacy)

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
                                   keys_and_docs: Mapping[str, DocumentValue],
                                   opts_type: type[MutationMultiOptions],
                                   op: Op,
                                   obs_handler: ObservableRequestHandler | None,
                                   opts: dict[str, Any] | None,
                                   **kwargs: object
                                   ) -> KeyValueMultiRequest:

        final_args = get_valid_multi_args(opts_type, kwargs, opts,
                                          protocol=CouchbaseProtocol.CLASSIC,
                                          mode=self._collection_dtls.option_validation)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            parent_span=final_args.pop('parent_span', None))
        if obs_handler:
            obs_handler.create_kv_multi_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)

        if not isinstance(keys_and_docs, Mapping):
            raise InvalidArgumentException(message='Expected keys_and_docs to be a mapping.')

        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        per_key_args = final_args.pop('per_key_options', None)
        return_exceptions = final_args.pop('return_exceptions', True)
        legacy = isinstance(durability, dict)
        self._maybe_update_durable_timeout(final_args, durability)
        opcode, req_opcode = _multi_opcodes(op, legacy)

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
                                    keys: list[str],
                                    opts_type: type[NoValueMultiOptions
                                                    | GetAllReplicasMultiOptions
                                                    | GetAnyReplicaMultiOptions],
                                    op: Op,
                                    obs_handler: ObservableRequestHandler | None,
                                    op_keys_cas: dict[str, int] | None,
                                    opts: dict[str, Any] | None,
                                    **kwargs: object) -> KeyValueMultiWithTranscoderRequest:
        final_args = get_valid_multi_args(opts_type, kwargs, opts,
                                          protocol=CouchbaseProtocol.CLASSIC,
                                          mode=self._collection_dtls.option_validation)
        durability = final_args.pop('durability', None)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(
            parent_span=final_args.pop('parent_span', None))
        if obs_handler:
            obs_handler.create_kv_multi_span(self._collection_dtls.get_details_as_dict(), parent_span=parent_span)

        if not isinstance(keys, list):
            raise InvalidArgumentException(message='Expected keys to be a list.')

        if not opts_type:
            raise InvalidArgumentException(message='Expected options type is missing.')

        transcoder = self._collection_dtls.get_request_transcoder(final_args)
        per_key_args = final_args.pop('per_key_options', None if op_keys_cas is None else {})
        if op_keys_cas:
            per_key_args = {k: {**per_key_args.get(k, {}), 'cas': v} for k, v in op_keys_cas.items()}
        if op is Op.GetMulti:
            _refuse_projection(final_args, per_key_args)
        return_exceptions = final_args.pop('return_exceptions', True)
        legacy = isinstance(durability, dict)
        self._maybe_update_durable_timeout(final_args, durability)
        opcode, req_opcode = _multi_opcodes(op, legacy)

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

        return KeyValueMultiWithTranscoderRequest(opcode, requests, return_exceptions, key_transcoders)

    def _validate_delta_initial(self,
                                delta: DeltaValue | None = None,
                                initial: SignedInt64 | None = None) -> None:
        # @TODO: remove deprecation next .minor
        # from couchbase.collection import DeltaValueDeprecated, SignedInt64Deprecated
        if delta is not None:
            if not isinstance(delta, DeltaValue):
                raise InvalidArgumentException("Argument is not valid DeltaValue")
        if initial is not None:
            if not isinstance(initial, SignedInt64):
                raise InvalidArgumentException("Argument is not valid SignedInt64")

    def build_append_multi_request(self,
                                   keys_and_docs: Mapping[str, str | bytes | bytearray],
                                   obs_handler: ObservableRequestHandler | None,
                                   opts: AppendMultiOptions | None = None,
                                   **kwargs: object) -> KeyValueMultiRequest:
        return self._get_multi_binary_mutation_req(keys_and_docs,
                                                   AppendMultiOptions,
                                                   Op.AppendMulti,
                                                   obs_handler,
                                                   opts,
                                                   **kwargs)

    def build_decrement_multi_request(self,
                                      keys: list[str],
                                      obs_handler: ObservableRequestHandler | None,
                                      opts: DecrementMultiOptions | None = None,
                                      **kwargs: object) -> KeyValueMultiRequest:
        return self._get_multi_counter_op_req(keys,
                                              DecrementMultiOptions,
                                              Op.DecrementMulti,
                                              obs_handler,
                                              opts,
                                              **kwargs)

    def build_exists_multi_request(self,
                                   keys: list[str],
                                   obs_handler: ObservableRequestHandler | None,
                                   opts: ExistsMultiOptions | None = None,
                                   **kwargs: object) -> KeyValueMultiRequest:
        return self._get_multi_op_non_value_req(keys,
                                                ExistsMultiOptions,
                                                Op.ExistsMulti,
                                                obs_handler,
                                                None,
                                                opts,
                                                **kwargs)

    def build_get_all_replicas_multi_request(self,
                                             keys: list[str],
                                             obs_handler: ObservableRequestHandler | None,
                                             opts: GetAllReplicasMultiOptions | None = None,
                                             **kwargs: object) -> KeyValueMultiWithTranscoderRequest:
        return self._get_multi_op_non_value_req(keys,
                                                GetAllReplicasMultiOptions,
                                                Op.GetAllReplicasMulti,
                                                obs_handler,
                                                None,
                                                opts,
                                                **kwargs)

    def build_get_and_lock_multi_request(self,
                                         keys: list[str],
                                         lock_time: timedelta,
                                         obs_handler: ObservableRequestHandler | None,
                                         opts: GetAndLockMultiOptions | None = None,
                                         **kwargs: object) -> KeyValueMultiWithTranscoderRequest:
        kwargs['lock_time'] = lock_time
        return self._get_multi_op_non_value_req(keys,
                                                GetAndLockMultiOptions,
                                                Op.GetAndLockMulti,
                                                obs_handler,
                                                None,
                                                opts,
                                                **kwargs)

    def build_get_any_replica_multi_request(self,
                                            keys: list[str],
                                            obs_handler: ObservableRequestHandler | None,
                                            opts: GetAnyReplicaMultiOptions | None = None,
                                            **kwargs: object) -> KeyValueMultiWithTranscoderRequest:
        return self._get_multi_op_non_value_req(keys,
                                                GetAnyReplicaMultiOptions,
                                                Op.GetAnyReplicaMulti,
                                                obs_handler,
                                                None,
                                                opts,
                                                **kwargs)

    def build_get_multi_request(self,
                                keys: list[str],
                                obs_handler: ObservableRequestHandler | None,
                                opts: GetMultiOptions | None = None,
                                **kwargs: object) -> KeyValueMultiWithTranscoderRequest:
        return self._get_multi_op_non_value_req(keys,
                                                GetMultiOptions,
                                                Op.GetMulti,
                                                obs_handler,
                                                None,
                                                opts,
                                                **kwargs)

    def build_increment_multi_request(self,
                                      keys: list[str],
                                      obs_handler: ObservableRequestHandler | None,
                                      opts: IncrementMultiOptions | None = None,
                                      **kwargs: object) -> KeyValueMultiRequest:
        return self._get_multi_counter_op_req(keys,
                                              IncrementMultiOptions,
                                              Op.IncrementMulti,
                                              obs_handler,
                                              opts,
                                              **kwargs)

    def build_insert_multi_request(self,
                                   keys_and_docs: Mapping[str, DocumentValue],
                                   obs_handler: ObservableRequestHandler | None,
                                   opts: InsertMultiOptions | None = None,
                                   **kwargs: object) -> KeyValueMultiRequest:
        return self._get_multi_op_mutation_req(keys_and_docs,
                                               InsertMultiOptions,
                                               Op.InsertMulti,
                                               obs_handler,
                                               opts,
                                               **kwargs)

    def build_prepend_multi_request(self,
                                    keys_and_docs: Mapping[str, str | bytes | bytearray],
                                    obs_handler: ObservableRequestHandler | None,
                                    opts: PrependMultiOptions | None = None,
                                    **kwargs: object) -> KeyValueMultiRequest:
        return self._get_multi_binary_mutation_req(keys_and_docs,
                                                   PrependMultiOptions,
                                                   Op.PrependMulti,
                                                   obs_handler,
                                                   opts,
                                                   **kwargs)

    def build_remove_multi_request(self,
                                   keys: list[str],
                                   obs_handler: ObservableRequestHandler | None,
                                   opts: RemoveMultiOptions | None = None,
                                   **kwargs: object) -> KeyValueMultiRequest:
        # use the non-mutation logic even though remove is a mutation
        req = self._get_multi_op_non_value_req(keys,
                                               RemoveMultiOptions,
                                               Op.RemoveMulti,
                                               obs_handler,
                                               None,
                                               opts,
                                               **kwargs)
        return _without_transcoders(req)

    def build_replace_multi_request(self,
                                    keys_and_docs: Mapping[str, DocumentValue],
                                    obs_handler: ObservableRequestHandler | None,
                                    opts: ReplaceMultiOptions | None = None,
                                    **kwargs: object) -> KeyValueMultiRequest:
        return self._get_multi_op_mutation_req(keys_and_docs,
                                               ReplaceMultiOptions,
                                               Op.ReplaceMulti,
                                               obs_handler,
                                               opts,
                                               **kwargs)

    def build_touch_multi_request(self,
                                  keys: list[str],
                                  expiry: timedelta,
                                  obs_handler: ObservableRequestHandler | None,
                                  opts: TouchMultiOptions | None = None,
                                  **kwargs: object) -> KeyValueMultiRequest:
        kwargs['expiry'] = expiry
        # use the non-mutation logic even though touch is a mutation
        req = self._get_multi_op_non_value_req(keys,
                                               TouchMultiOptions,
                                               Op.TouchMulti,
                                               obs_handler,
                                               None,
                                               opts,
                                               **kwargs)
        return _without_transcoders(req)

    def build_unlock_multi_request(self,
                                   keys: dict[str, int] | MultiGetResult | MultiMutationResult,
                                   obs_handler: ObservableRequestHandler | None,
                                   opts: UnlockMultiOptions | None = None,
                                   **kwargs: object) -> KeyValueMultiRequest:
        op_keys_cas: dict[str, int] = {}
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
                'keys type must be MultiGetResult | MultiMutationResult | dict[str, int].')
        # use the non-mutation logic even though unlock is a mutation
        req = self._get_multi_op_non_value_req(list(op_keys_cas.keys()),
                                               UnlockMultiOptions,
                                               Op.UnlockMulti,
                                               obs_handler,
                                               op_keys_cas,
                                               opts,
                                               **kwargs)
        return _without_transcoders(req)

    def build_upsert_multi_request(self,
                                   keys_and_docs: Mapping[str, DocumentValue],
                                   obs_handler: ObservableRequestHandler | None,
                                   opts: UpsertMultiOptions | None = None,
                                   **kwargs: object) -> KeyValueMultiRequest:
        return self._get_multi_op_mutation_req(keys_and_docs,
                                               UpsertMultiOptions,
                                               Op.UpsertMulti,
                                               obs_handler,
                                               opts,
                                               **kwargs)
