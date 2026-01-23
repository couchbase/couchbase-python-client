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

from dataclasses import dataclass
from typing import (TYPE_CHECKING,
                    Any,
                    Dict)

from couchbase.logic.operation_types import KeyValueMultiOperationType
from couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from couchbase.logic.client_adapter import PyCapsuleType


@dataclass
class CollectionMultiRequest:
    op_type: int
    bucket_name: str
    scope_name: str
    collection_name: str
    op_args: Dict[str, Dict[str, Any]]

    def req_to_dict(self, conn: PyCapsuleType) -> Dict[str, Any]:
        op_kwargs = {
            'conn': conn,
            'bucket': self.bucket_name,
            'scope': self.scope_name,
            'collection_name': self.collection_name,
            'op_type': self.op_type,
            'op_args': self.op_args
        }

        return op_kwargs


@dataclass
class AppendMultiRequest(CollectionMultiRequest):
    return_exceptions: bool

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.AppendMulti.value


@dataclass
class DecrementMultiRequest(CollectionMultiRequest):
    return_exceptions: bool

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.DecrementMulti.value


@dataclass
class ExistsMultiRequest(CollectionMultiRequest):
    transcoders: Dict[str, Transcoder]
    return_exceptions: bool

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.ExistsMulti.value


@dataclass
class GetAllReplicasMultiRequest(CollectionMultiRequest):
    transcoders: Dict[str, Transcoder]
    return_exceptions: bool

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.GetAllReplicasMulti.value


@dataclass
class GetAndLockMultiRequest(CollectionMultiRequest):
    transcoders: Dict[str, Transcoder]
    return_exceptions: bool

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.GetAndLockMulti.value


@dataclass
class GetAnyReplicaMultiRequest(CollectionMultiRequest):
    transcoders: Dict[str, Transcoder]
    return_exceptions: bool

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.GetAnyReplicaMulti.value


@dataclass
class GetMultiRequest(CollectionMultiRequest):
    transcoders: Dict[str, Transcoder]
    return_exceptions: bool

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.GetMulti.value


@dataclass
class IncrementMultiRequest(CollectionMultiRequest):
    return_exceptions: bool

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.IncrementMulti.value


@dataclass
class InsertMultiRequest(CollectionMultiRequest):
    return_exceptions: bool

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.InsertMulti.value


@dataclass
class PrependMultiRequest(CollectionMultiRequest):
    return_exceptions: bool

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.PrependMulti.value


@dataclass
class RemoveMultiRequest(CollectionMultiRequest):
    return_exceptions: bool

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.RemoveMulti.value


@dataclass
class ReplaceMultiRequest(CollectionMultiRequest):
    return_exceptions: bool

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.ReplaceMulti.value


@dataclass
class TouchMultiRequest(CollectionMultiRequest):
    return_exceptions: bool

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.TouchMulti.value


@dataclass
class UnlockMultiRequest(CollectionMultiRequest):
    return_exceptions: bool

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.UnlockMulti.value


@dataclass
class UpsertMultiRequest(CollectionMultiRequest):
    return_exceptions: bool

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.UpsertMulti.value
