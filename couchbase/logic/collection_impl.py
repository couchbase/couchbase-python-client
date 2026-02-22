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

from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    Iterator,
                    Optional,
                    Union)

from couchbase.exceptions import (ErrorMapper,
                                  InvalidArgumentException,
                                  UnAmbiguousTimeoutException)
from couchbase.logic.collection_multi_req_builder import CollectionMultiRequestBuilder
from couchbase.logic.collection_req_builder import CollectionRequestBuilder
from couchbase.logic.collection_types import CollectionDetails
from couchbase.logic.pycbc_core import pycbc_exception as PycbcCoreException
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
    from couchbase.kv_range_scan import RangeScanRequest
    from couchbase.logic.collection_multi_types import (AppendMultiRequest,
                                                        DecrementMultiRequest,
                                                        ExistsMultiRequest,
                                                        GetAllReplicasMultiRequest,
                                                        GetAndLockMultiRequest,
                                                        GetAnyReplicaMultiRequest,
                                                        GetMultiRequest,
                                                        IncrementMultiRequest,
                                                        InsertMultiRequest,
                                                        PrependMultiRequest,
                                                        RemoveMultiRequest,
                                                        ReplaceMultiRequest,
                                                        TouchMultiRequest,
                                                        UnlockMultiRequest,
                                                        UpsertMultiRequest)
    from couchbase.logic.collection_types import (AppendRequest,
                                                  AppendWithLegacyDurabilityRequest,
                                                  DecrementRequest,
                                                  DecrementWithLegacyDurabilityRequest,
                                                  ExistsRequest,
                                                  GetAllReplicasRequest,
                                                  GetAndLockRequest,
                                                  GetAndTouchRequest,
                                                  GetAnyReplicaRequest,
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
    from couchbase.logic.pycbc_core import pycbc_connection
    from couchbase.scope import Scope


class CollectionImpl:

    def __init__(self, collection_name: str, scope: Scope) -> None:
        self._scope = scope
        self._client_adapter = scope._impl._client_adapter
        self._collection_details = CollectionDetails(self._scope._impl.bucket_name,
                                                     self._scope._impl.name,
                                                     collection_name,
                                                     self._scope._impl.cluster_settings.default_transcoder)
        self._multi_request_builder = CollectionMultiRequestBuilder(self._collection_details)
        self._request_builder = CollectionRequestBuilder(self._collection_details)

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
    def multi_request_builder(self) -> CollectionMultiRequestBuilder:
        return self._multi_request_builder

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

    def append(self, req: Union[AppendRequest, AppendWithLegacyDurabilityRequest]) -> MutationResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MutationResult(ret, key=req.key)

    def append_multi(self, req: AppendMultiRequest) -> MultiMutationResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MultiMutationResult(ret, return_exceptions=req.return_exceptions)

    def decrement(self, req: Union[DecrementRequest, DecrementWithLegacyDurabilityRequest]) -> CounterResult:
        ret = self._client_adapter.execute_collection_request(req)
        return CounterResult(ret, key=req.key)

    def decrement_multi(self, req: DecrementMultiRequest) -> MultiCounterResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MultiCounterResult(ret, return_exceptions=req.return_exceptions)

    def exists(self, req: ExistsRequest) -> ExistsResult:
        ret = self._client_adapter.execute_collection_request(req)
        return ExistsResult(ret, key=req.key)

    def exists_multi(self, req: ExistsMultiRequest) -> MultiExistsResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MultiExistsResult(ret, return_exceptions=req.return_exceptions)

    def get_all_replicas(self, req: GetAllReplicasRequest) -> Iterable[GetReplicaResult]:
        ret = self._client_adapter.execute_collection_request(req)

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

                    yield GetReplicaResult(res, transcoder=req.transcoder, key=req.key)

        return _decode_replicas()

    def get_all_replicas_multi(self, req: GetAllReplicasMultiRequest) -> MultiGetReplicaResult:
        ret = self._client_adapter.execute_collection_request(req)

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
        for k, v in ret.raw_result.items():
            if k == 'all_okay' or isinstance(v, PycbcCoreException):
                continue
            result_keys.append(k)

        for k in result_keys:
            value = ret.raw_result.pop(k)
            tc = req.transcoders[k]
            ret.raw_result[k] = list(r for r in _decode_replicas(k, tc, value))

        return MultiGetReplicaResult(ret, return_exceptions=req.return_exceptions)

    def get_and_lock(self, req: GetAndLockRequest) -> GetResult:
        ret = self._client_adapter.execute_collection_request(req)
        return GetResult(ret, transcoder=req.transcoder, key=req.key)

    def get_and_lock_multi(self, req: GetAndLockMultiRequest) -> MultiGetResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MultiGetResult(ret, return_exceptions=req.return_exceptions, transcoders=req.transcoders)

    def get_and_touch(self, req: GetAndTouchRequest) -> GetResult:
        ret = self._client_adapter.execute_collection_request(req)
        return GetResult(ret, transcoder=req.transcoder, key=req.key)

    def get_any_replica(self, req: GetAnyReplicaRequest) -> GetReplicaResult:
        ret = self._client_adapter.execute_collection_request(req)
        return GetReplicaResult(ret, transcoder=req.transcoder, key=req.key)

    def get_any_replica_multi(self, req: GetAnyReplicaMultiRequest) -> MultiGetReplicaResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MultiGetReplicaResult(ret, return_exceptions=req.return_exceptions, transcoders=req.transcoders)

    def get(self, req: GetRequest) -> GetResult:
        ret = self._client_adapter.execute_collection_request(req)
        return GetResult(ret, transcoder=req.transcoder, key=req.key)

    def get_multi(self, req: GetMultiRequest) -> MultiGetResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MultiGetResult(ret, return_exceptions=req.return_exceptions, transcoders=req.transcoders)

    def increment(self, req: Union[IncrementRequest, IncrementWithLegacyDurabilityRequest]) -> CounterResult:
        ret = self._client_adapter.execute_collection_request(req)
        return CounterResult(ret, key=req.key)

    def increment_multi(self, req: IncrementMultiRequest) -> MultiCounterResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MultiCounterResult(ret, return_exceptions=req.return_exceptions)

    def insert(self, req: Union[InsertRequest, InsertWithLegacyDurabilityRequest]) -> MutationResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MutationResult(ret, key=req.key)

    def insert_multi(self, req: InsertMultiRequest) -> MultiMutationResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MultiMutationResult(ret, return_exceptions=req.return_exceptions)

    def lookup_in(self, req: LookupInRequest) -> LookupInResult:
        ret = self._client_adapter.execute_collection_request(req)
        return LookupInResult(ret, transcoder=req.transcoder, is_subdoc=True, key=req.key)

    def lookup_in_all_replicas(self, req: LookupInAllReplicasRequest) -> Iterable[LookupInReplicaResult]:
        ret = self._client_adapter.execute_collection_request(req)

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

                    yield LookupInReplicaResult(res, transcoder=req.transcoder, is_subdoc=True, key=req.key)
        return _decode_replicas()

    def lookup_in_any_replica(self, req: LookupInAnyReplicaRequest) -> LookupInReplicaResult:
        ret = self._client_adapter.execute_collection_request(req)
        return LookupInReplicaResult(ret, transcoder=req.transcoder, is_subdoc=True, key=req.key)

    def mutate_in(self, req: Union[MutateInRequest, MutateInWithLegacyDurabilityRequest]) -> MutateInResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MutateInResult(ret, key=req.key)

    def prepend(self, req: Union[PrependRequest, PrependWithLegacyDurabilityRequest]) -> MutationResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MutationResult(ret, key=req.key)

    def prepend_multi(self, req: PrependMultiRequest) -> MultiMutationResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MultiMutationResult(ret, return_exceptions=req.return_exceptions)

    def range_scan(self, req: RangeScanRequest) -> ScanResultIterable:
        return ScanResultIterable(req)

    def remove(self, req: Union[RemoveRequest, RemoveWithLegacyDurabilityRequest]) -> MutationResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MutationResult(ret, key=req.key)

    def remove_multi(self, req: RemoveMultiRequest) -> MultiMutationResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MultiMutationResult(ret, return_exceptions=req.return_exceptions)

    def replace(self, req: Union[ReplaceRequest, ReplaceWithLegacyDurabilityRequest]) -> MutationResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MutationResult(ret, key=req.key)

    def replace_multi(self, req: ReplaceMultiRequest) -> MultiMutationResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MultiMutationResult(ret, return_exceptions=req.return_exceptions)

    def touch(self, req: TouchRequest) -> MutationResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MutationResult(ret, key=req.key)

    def touch_multi(self, req: TouchMultiRequest) -> MultiMutationResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MultiMutationResult(ret, return_exceptions=req.return_exceptions)

    def unlock(self, req: UnlockRequest) -> None:
        self._client_adapter.execute_collection_request(req)

    def unlock_multi(self, req: UnlockMultiRequest) -> Dict[str, Optional[PycbcCoreException]]:
        ret = self._client_adapter.execute_collection_request(req)
        output: Dict[str, Optional[PycbcCoreException]] = {}
        for k, v in ret.raw_result.items():
            if k == 'all_okay':
                continue
            if isinstance(v, PycbcCoreException):
                ex = ErrorMapper.build_exception(v)
                if not req.return_exceptions:
                    raise ex
                else:
                    output[k] = ex
            else:
                output[k] = None

        return output

    def upsert(self, req: Union[UpsertRequest, UpsertWithLegacyDurabilityRequest]) -> MutationResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MutationResult(ret, key=req.key)

    def upsert_multi(self, req: UpsertMultiRequest) -> MultiMutationResult:
        ret = self._client_adapter.execute_collection_request(req)
        return MultiMutationResult(ret, return_exceptions=req.return_exceptions)

    def _set_default_transcoder(self, transcoder: Transcoder) -> None:
        if not issubclass(transcoder.__class__, Transcoder):
            raise InvalidArgumentException('Cannot set default transcoder to non Transcoder type.')

        self._collection_details.default_transcoder = transcoder
