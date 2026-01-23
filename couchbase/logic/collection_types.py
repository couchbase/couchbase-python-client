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

from dataclasses import dataclass, fields
from typing import (TYPE_CHECKING,
                    Any,
                    Callable,
                    Dict,
                    Iterable,
                    List,
                    Optional,
                    Tuple,
                    Union)

from couchbase.logic.operation_types import KeyValueOperationType
from couchbase.serializer import Serializer
from couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from couchbase.logic.client_adapter import PyCapsuleType

# we have these params on the top-level pycbc_core request
OPARG_SKIP_LIST = ['bucket_name',
                   'collection_name',
                   'key',
                   'op_type',
                   'scope_name',
                   'timeout',
                   'transcoder',
                   'value']


@dataclass
class CollectionDetails:
    bucket_name: str
    scope_name: str
    collection_name: str
    default_transcoder: Transcoder

    def get_details(self) -> Tuple[str, str, str]:
        return self.bucket_name, self.scope_name, self.collection_name

    def get_details_as_dict(self) -> Dict[str, str]:
        return {
            'bucket': self.bucket_name,
            'scope': self.scope_name,
            'collection_name': self.collection_name
        }

    def get_request_transcoder(self, op_args: Dict[str, Any]) -> Transcoder:
        return op_args.pop('transcoder', self.default_transcoder)


@dataclass
class CollectionRequest:
    op_type: int
    key: str
    bucket_name: str
    scope_name: str
    collection_name: str

    def req_to_dict(self,
                    conn: PyCapsuleType,
                    callback: Optional[Callable[..., None]] = None,
                    errback: Optional[Callable[..., None]] = None) -> Dict[str, Any]:
        op_kwargs = {
            'conn': conn,
            'bucket': self.bucket_name,
            'scope': self.scope_name,
            'collection_name': self.collection_name,
            'key': self.key,
            'op_type': self.op_type,
        }

        op_args = {
            field.name: getattr(self, field.name)
            for field in fields(self)
            if field.name not in OPARG_SKIP_LIST and getattr(self, field.name) is not None
        }

        if callback is not None:
            op_args['callback'] = callback

        if errback is not None:
            op_args['errback'] = errback

        if hasattr(self, 'timeout') and getattr(self, 'timeout') is not None:
            op_args['timeout'] = getattr(self, 'timeout')

        op_kwargs['op_args'] = op_args

        return op_kwargs


@dataclass
class CollectionRequestWithEncoding(CollectionRequest):
    value: Union[bytes, Tuple[bytes, int]]

    def req_to_dict(self,
                    conn: PyCapsuleType,
                    callback: Optional[Callable[..., None]] = None,
                    errback: Optional[Callable[..., None]] = None) -> Dict[str, Any]:
        op_kwargs = super().req_to_dict(conn, callback=callback, errback=errback)
        op_kwargs['value'] = self.value
        return op_kwargs


@dataclass
class SubdocumentRequest(CollectionRequest):
    spec: Union[List[Tuple], Tuple[Tuple]]

    def req_to_dict(self,
                    conn: PyCapsuleType,
                    callback: Optional[Callable[..., None]] = None,
                    errback: Optional[Callable[..., None]] = None) -> Dict[str, Any]:
        op_kwargs = super().req_to_dict(conn, callback=callback, errback=errback)
        op_kwargs['spec'] = self.spec
        return op_kwargs


@dataclass
class AppendRequest(CollectionRequestWithEncoding):
    cas: Optional[int] = None
    durability: Optional[Union[int, Dict[str, int]]] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return KeyValueOperationType.Append.value


@dataclass
class DecrementRequest(CollectionRequest):
    delta: int
    durability: Optional[Union[int, Dict[str, int]]] = None
    expiry: Optional[int] = None
    initial: Optional[int] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return KeyValueOperationType.Decrement.value


@dataclass
class ExistsRequest(CollectionRequest):
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return KeyValueOperationType.Exists.value


@dataclass
class GetAllReplicasRequest(CollectionRequest):
    transcoder: Transcoder
    read_preference: Optional[int] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return KeyValueOperationType.GetAllReplicas.value


@dataclass
class GetAndLockRequest(CollectionRequest):
    lock_time: int
    transcoder: Transcoder
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return KeyValueOperationType.GetAndLock.value


@dataclass
class GetAndTouchRequest(CollectionRequest):
    expiry: int
    transcoder: Transcoder
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return KeyValueOperationType.GetAndTouch.value


@dataclass
class GetAnyReplicaRequest(CollectionRequest):
    transcoder: Transcoder
    read_preference: Optional[int] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return KeyValueOperationType.GetAnyReplica.value


@dataclass
class GetRequest(CollectionRequest):
    transcoder: Transcoder
    project: Optional[Iterable[str]] = None
    timeout: Optional[int] = None
    with_expiry: Optional[bool] = None

    @property
    def op_name(self) -> str:
        if self.project is not None or self.with_expiry:
            return KeyValueOperationType.GetProject.value
        return KeyValueOperationType.Get.value


@dataclass
class IncrementRequest(CollectionRequest):
    delta: int
    durability: Optional[Union[int, Dict[str, int]]] = None
    expiry: Optional[int] = None
    initial: Optional[int] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return KeyValueOperationType.Increment.value


@dataclass
class InsertRequest(CollectionRequestWithEncoding):
    durability: Optional[Union[int, Dict[str, int]]] = None
    expiry: Optional[int] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return KeyValueOperationType.Insert.value


@dataclass
class LookupInAllReplicasRequest(SubdocumentRequest):
    transcoder: Transcoder
    read_preference: Optional[int] = None
    serializer: Optional[Serializer] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return KeyValueOperationType.LookupInAllReplicas.value


@dataclass
class LookupInAnyReplicaRequest(SubdocumentRequest):
    transcoder: Transcoder
    read_preference: Optional[int] = None
    serializer: Optional[Serializer] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return KeyValueOperationType.LookupInAnyReplica.value


@dataclass
class LookupInRequest(SubdocumentRequest):
    transcoder: Transcoder
    access_deleted: Optional[bool] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return KeyValueOperationType.LookupIn.value


@dataclass
class MutateInRequest(SubdocumentRequest):
    access_deleted: Optional[bool] = None
    cas: Optional[int] = None
    durability: Optional[Union[int, Dict[str, int]]] = None
    expiry: Optional[int] = None
    preserve_expiry: Optional[bool] = None
    store_semantics: Optional[int] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return KeyValueOperationType.MutateIn.value


@dataclass
class PrependRequest(CollectionRequestWithEncoding):
    cas: Optional[int] = None
    durability: Optional[Union[int, Dict[str, int]]] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return KeyValueOperationType.Prepend.value


@dataclass
class RemoveRequest(CollectionRequest):
    cas: Optional[int] = None
    durability: Optional[Union[int, Dict[str, int]]] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return KeyValueOperationType.Remove.value


@dataclass
class ReplaceRequest(CollectionRequestWithEncoding):
    cas: Optional[int] = None
    durability: Optional[Union[int, Dict[str, int]]] = None
    expiry: Optional[int] = None
    preserve_expiry: Optional[bool] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return KeyValueOperationType.Replace.value


@dataclass
class TouchRequest(CollectionRequest):
    expiry: int
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return KeyValueOperationType.Touch.value


@dataclass
class UnlockRequest(CollectionRequest):
    cas: int
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return KeyValueOperationType.Unlock.value


@dataclass
class UpsertRequest(CollectionRequestWithEncoding):
    durability: Optional[Union[int, Dict[str, int]]] = None
    expiry: Optional[int] = None
    preserve_expiry: Optional[bool] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return KeyValueOperationType.Upsert.value
