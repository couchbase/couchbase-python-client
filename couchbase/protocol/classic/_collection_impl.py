#  Copyright 2016-2022. Couchbase, Inc.
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

from collections.abc import Callable, Iterator
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    List,
                    Optional,
                    TypeVar)

from couchbase.common._observability import (KeyspaceDetails,
                                             ObservabilityInstruments,
                                             ObservableRequestHandler)
from couchbase.common.exceptions import (CouchbaseException,
                                         InvalidArgumentException,
                                         UnAmbiguousTimeoutException)
from couchbase.protocol.classic._capabilities import CAPABILITIES
from couchbase.protocol.classic._collection_multi_req_builder import CollectionMultiRequestBuilder
from couchbase.protocol.classic._collection_req_builder import CollectionRequestBuilder
from couchbase.protocol.classic._collection_types import CollectionDetails
from couchbase.protocol.classic._error_mapping import ErrorMapper
from couchbase.protocol.classic._pycbc_core import pycbc_exception as PycbcCoreException
from couchbase.protocol.classic._pycbc_core import pycbc_kv_request as PycbcCoreKeyValueRequest
from couchbase.result import (CounterResult,
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
                              MutationResult,
                              ScanResultIterable)
from couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from couchbase.common.protocol import CouchbaseProtocol
    from couchbase.kv_range_scan import RangeScanRequest
    from couchbase.protocol.classic._client_adapter import ClientAdapter
    from couchbase.protocol.classic._collection_multi_types import (KeyValueMultiRequest,
                                                                    KeyValueMultiWithTranscoderRequest)
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


def _decode_replicas(stream: Any, build: Callable[[Any], ResultT]) -> Iterator[ResultT]:
    """Build each replica as the core's stream yields it; the stream ends with a None."""
    while True:
        try:
            res = next(stream)
        except StopIteration:
            # this is a timeout from pulling a result from the queue, kill the generator
            raise UnAmbiguousTimeoutException('Timeout reached waiting for result in queue.') from None
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
                                                     option_validation=cluster_settings.option_validation)
        self._multi_request_builder = CollectionMultiRequestBuilder(self._collection_details)
        self._request_builder = CollectionRequestBuilder(self._collection_details)

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
            req: PycbcCoreKeyValueRequest,
            transcoder: Transcoder,
            obs_handler: ObservableRequestHandler) -> GetResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return GetResult(ret, transcoder=transcoder, key=req.key)

    def get_any_replica(self,
                        req: PycbcCoreKeyValueRequest,
                        transcoder: Transcoder,
                        obs_handler: ObservableRequestHandler) -> GetReplicaResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return GetReplicaResult(ret, transcoder=transcoder, key=req.key)

    def get_all_replicas(self,
                         req: PycbcCoreKeyValueRequest,
                         transcoder: Transcoder,
                         obs_handler: ObservableRequestHandler) -> Iterable[GetReplicaResult]:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)

        def _decode_replicas() -> Iterator[GetReplicaResult]:
            while True:
                try:
                    res = next(ret)
                except StopIteration:
                    # this is a timeout from pulling a result from the queue, kill the generator
                    raise UnAmbiguousTimeoutException('Timeout reached waiting for result in queue.') from None
                else:
                    if isinstance(res, PycbcCoreException):
                        raise ErrorMapper.build_exception(res)
                    # should only be None once all replicas have been retrieved
                    if res is None:
                        return

                    yield GetReplicaResult(res, transcoder=transcoder, key=req.key)

        return _decode_replicas()

    def exists(self, req: PycbcCoreKeyValueRequest, obs_handler: ObservableRequestHandler) -> ExistsResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return ExistsResult(ret, key=req.key)

    def insert(self,
               req: PycbcCoreKeyValueRequest,
               obs_handler: ObservableRequestHandler) -> MutationResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(ret, key=req.key)

    def upsert(self,
               req: PycbcCoreKeyValueRequest,
               obs_handler: ObservableRequestHandler) -> MutationResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(ret, key=req.key)

    def replace(self,
                req: PycbcCoreKeyValueRequest,
                obs_handler: ObservableRequestHandler) -> MutationResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(ret, key=req.key)

    def remove(self,
               req: PycbcCoreKeyValueRequest,
               obs_handler: ObservableRequestHandler) -> MutationResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(ret, key=req.key)

    def touch(self, req: PycbcCoreKeyValueRequest, obs_handler: ObservableRequestHandler) -> MutationResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(ret, key=req.key)

    def get_and_touch(self,
                      req: PycbcCoreKeyValueRequest,
                      transcoder: Transcoder,
                      obs_handler: ObservableRequestHandler) -> GetResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return GetResult(ret, transcoder=transcoder, key=req.key)

    def get_and_lock(self,
                     req: PycbcCoreKeyValueRequest,
                     transcoder: Transcoder,
                     obs_handler: ObservableRequestHandler) -> GetResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return GetResult(ret, transcoder=transcoder, key=req.key)

    def unlock(self, req: PycbcCoreKeyValueRequest, obs_handler: ObservableRequestHandler) -> None:
        self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)

    def lookup_in(self,
                  req: PycbcCoreKeyValueRequest,
                  transcoder: Transcoder,
                  obs_handler: ObservableRequestHandler) -> LookupInResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return LookupInResult(ret, transcoder=transcoder, is_subdoc=True, key=req.key)

    def lookup_in_any_replica(self,
                              req: PycbcCoreKeyValueRequest,
                              transcoder: Transcoder,
                              obs_handler: ObservableRequestHandler) -> LookupInReplicaResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return LookupInReplicaResult(ret, transcoder=transcoder, is_subdoc=True, key=req.key)

    def lookup_in_all_replicas(self,
                               req: PycbcCoreKeyValueRequest,
                               transcoder: Transcoder,
                               obs_handler: ObservableRequestHandler) -> Iterable[LookupInReplicaResult]:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)

        def _decode_replicas() -> Iterator[LookupInReplicaResult]:
            while True:
                try:
                    res = next(ret)
                except StopIteration:
                    # this is a timeout from pulling a result from the queue, kill the generator
                    raise UnAmbiguousTimeoutException('Timeout reached waiting for result in queue.') from None
                else:
                    if isinstance(res, PycbcCoreException):
                        raise ErrorMapper.build_exception(res)
                    # should only be None once all replicas have been retrieved
                    if res is None:
                        return

                    yield LookupInReplicaResult(res, transcoder=transcoder, is_subdoc=True, key=req.key)
        return _decode_replicas()

    def mutate_in(self,
                  req: PycbcCoreKeyValueRequest,
                  obs_handler: ObservableRequestHandler) -> MutateInResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        transcoder = self._collection_details.default_transcoder
        return MutateInResult(ret, transcoder=transcoder, is_subdoc=True, key=req.key)

    def get_multi(self,
                  req: KeyValueMultiWithTranscoderRequest,
                  obs_handler: ObservableRequestHandler) -> MultiGetResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
        return MultiGetResult(ret,
                              return_exceptions=req.return_exceptions,
                              transcoders=req.key_transcoders,
                              obs_handler=obs_handler)

    def get_any_replica_multi(self,
                              req: KeyValueMultiWithTranscoderRequest,
                              obs_handler: ObservableRequestHandler) -> MultiGetReplicaResult[GetReplicaResult]:
        ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
        return MultiGetReplicaResult(ret,
                                     return_exceptions=req.return_exceptions,
                                     transcoders=req.key_transcoders,
                                     obs_handler=obs_handler)

    def get_all_replicas_multi(self,  # noqa: C901
                               req: KeyValueMultiWithTranscoderRequest,
                               obs_handler: ObservableRequestHandler
                               ) -> MultiGetReplicaResult[List[GetReplicaResult]]:
        ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)

        def _decode_replicas(key: str, transcoder: Transcoder, value: Any) -> Iterator[GetReplicaResult]:
            while True:
                try:
                    res = next(value)
                except StopIteration:
                    # this is a timeout from pulling a result from the queue, kill the generator
                    raise UnAmbiguousTimeoutException('Timeout reached waiting for result in queue.') from None
                else:
                    if isinstance(res, PycbcCoreException):
                        raise ErrorMapper.build_exception(res)
                    # should only be None once all replicas have been retrieved
                    if res is None:
                        return

                    yield GetReplicaResult(res, transcoder=transcoder, key=key)

        # all the successful results will be streamed_results, so lets pop those off the main result dict and re-add
        # the key back transformed into a List[GetReplicaResult]
        result_keys = []
        pass_obs_handler = False
        for k, v in ret.raw_result.items():
            if k == 'all_okay':
                continue
            # pycbc_streamed_result and pycbc_exception have a core_span member
            if obs_handler and hasattr(v, 'core_span'):
                obs_handler.process_core_span(v.core_span, from_kv_get_all_replicas=True)
            # we don't need to process metrics here for exceptions, that will be handled when we process the
            # exceptions in the MultiResult class
            if isinstance(v, PycbcCoreException):
                pass_obs_handler = True if obs_handler else False
                continue

            # we do have to process metrics here for successful operations b/c the start and end time
            # are on the pycbc_streamed_result
            if obs_handler:
                obs_handler.process_multi_sub_op(v)

            result_keys.append(k)

        for k in result_keys:
            value = ret.raw_result.pop(k)
            tc = req.key_transcoders[k]
            ret.raw_result[k] = list(r for r in _decode_replicas(k, tc, value))

        if pass_obs_handler:
            return MultiGetReplicaResult(ret, return_exceptions=req.return_exceptions, obs_handler=obs_handler)
        return MultiGetReplicaResult(ret, return_exceptions=req.return_exceptions)

    def get_and_lock_multi(self,
                           req: KeyValueMultiWithTranscoderRequest,
                           obs_handler: ObservableRequestHandler) -> MultiGetResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
        return MultiGetResult(ret,
                              return_exceptions=req.return_exceptions,
                              transcoders=req.key_transcoders,
                              obs_handler=obs_handler)

    def exists_multi(self, req: KeyValueMultiRequest, obs_handler: ObservableRequestHandler) -> MultiExistsResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
        return MultiExistsResult(ret, return_exceptions=req.return_exceptions, obs_handler=obs_handler)

    def insert_multi(self, req: KeyValueMultiRequest, obs_handler: ObservableRequestHandler) -> MultiMutationResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
        return MultiMutationResult(ret, return_exceptions=req.return_exceptions, obs_handler=obs_handler)

    def upsert_multi(self,
                     req: KeyValueMultiRequest,
                     obs_handler: ObservableRequestHandler) -> MultiMutationResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
        return MultiMutationResult(ret, return_exceptions=req.return_exceptions, obs_handler=obs_handler)

    def replace_multi(self, req: KeyValueMultiRequest, obs_handler: ObservableRequestHandler) -> MultiMutationResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
        return MultiMutationResult(ret, return_exceptions=req.return_exceptions, obs_handler=obs_handler)

    def remove_multi(self, req: KeyValueMultiRequest, obs_handler: ObservableRequestHandler) -> MultiMutationResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
        return MultiMutationResult(ret, return_exceptions=req.return_exceptions, obs_handler=obs_handler)

    def touch_multi(self, req: KeyValueMultiRequest, obs_handler: ObservableRequestHandler) -> MultiMutationResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
        return MultiMutationResult(ret, return_exceptions=req.return_exceptions, obs_handler=obs_handler)

    def unlock_multi(self,
                     req: KeyValueMultiRequest,
                     obs_handler: ObservableRequestHandler) -> Dict[str, Optional[PycbcCoreException]]:
        ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
        output: Dict[str, Optional[PycbcCoreException]] = {}
        for k, v in ret.raw_result.items():
            if k == 'all_okay':
                continue
            # pycbc_streamed_result and pycbc_exception have a core_span member
            if obs_handler and hasattr(v, 'core_span'):
                obs_handler.process_core_span(v.core_span)
            if isinstance(v, PycbcCoreException):
                ex = ErrorMapper.build_exception(v)
                if obs_handler:
                    obs_handler.process_multi_sub_op(v, exc_val=ex)
                if not req.return_exceptions:
                    raise ex
                else:
                    output[k] = ex
            else:
                if obs_handler:
                    obs_handler.process_multi_sub_op(v)
                output[k] = None

        return output

    def increment(self,
                  req: PycbcCoreKeyValueRequest,
                  obs_handler: ObservableRequestHandler) -> CounterResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return CounterResult(ret, key=req.key)

    def decrement(self,
                  req: PycbcCoreKeyValueRequest,
                  obs_handler: ObservableRequestHandler) -> CounterResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return CounterResult(ret, key=req.key)

    def append(self,
               req: PycbcCoreKeyValueRequest,
               obs_handler: ObservableRequestHandler) -> MutationResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(ret, key=req.key)

    def prepend(self,
                req: PycbcCoreKeyValueRequest,
                obs_handler: ObservableRequestHandler) -> MutationResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req, obs_handler=obs_handler)
        return MutationResult(ret, key=req.key)

    def append_multi(self, req: KeyValueMultiRequest, obs_handler: ObservableRequestHandler) -> MultiMutationResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
        return MultiMutationResult(ret, return_exceptions=req.return_exceptions, obs_handler=obs_handler)

    def prepend_multi(self, req: KeyValueMultiRequest, obs_handler: ObservableRequestHandler) -> MultiMutationResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
        return MultiMutationResult(ret, return_exceptions=req.return_exceptions, obs_handler=obs_handler)

    def increment_multi(self,
                        req: KeyValueMultiRequest,
                        obs_handler: ObservableRequestHandler) -> MultiCounterResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
        return MultiCounterResult(ret, return_exceptions=req.return_exceptions, obs_handler=obs_handler)

    def decrement_multi(self, req: KeyValueMultiRequest, obs_handler: ObservableRequestHandler) -> MultiCounterResult:
        ret = self._client_adapter.execute_collection_request(req.opcode, req.request_list, obs_handler=obs_handler)
        return MultiCounterResult(ret, return_exceptions=req.return_exceptions, obs_handler=obs_handler)

    def _set_default_transcoder(self, transcoder: Transcoder) -> None:
        if not issubclass(transcoder.__class__, Transcoder):
            raise InvalidArgumentException('Cannot set default transcoder to non Transcoder type.')

        self._collection_details.default_transcoder = transcoder

    @property
    def multi_request_builder(self) -> CollectionMultiRequestBuilder:
        return self._multi_request_builder

    def range_scan(self, req: RangeScanRequest) -> ScanResultIterable:
        return ScanResultIterable(req)
