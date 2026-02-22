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

from dataclasses import dataclass, field
from typing import (Any,
                    Dict,
                    List,
                    Optional,
                    Tuple,
                    Union)

from couchbase.logic.operation_types import KeyValueMultiOperationType
from couchbase.transcoder import Transcoder


@dataclass
class CollectionMultiRequest:
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: Union[List[str], List[Tuple[str, Tuple[bytes, int]]]]
    op_args: Dict[str, Dict[str, Any]]
    return_exceptions: bool
    per_key_args: Optional[Dict[str, Dict[str, Any]]] = None

    def req_to_dict(self) -> Dict[str, Any]:
        op_kwargs = {
            'bucket': self.bucket_name,
            'scope': self.scope_name,
            'collection': self.collection_name,
            'doc_list': self.doc_list,
            'op_args': self.op_args
        }

        if self.per_key_args:
            op_kwargs['per_key_args'] = self.per_key_args

        return op_kwargs


@dataclass
class AppendMultiRequest(CollectionMultiRequest):

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.AppendMulti.value


@dataclass
class AppendWithLegacyDurabilityMultiRequest(CollectionMultiRequest):

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.AppendWithLegacyDurabilityMulti.value


@dataclass
class DecrementMultiRequest(CollectionMultiRequest):

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.DecrementMulti.value


@dataclass
class DecrementWithLegacyDurabilityMultiRequest(CollectionMultiRequest):

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.DecrementWithLegacyDurabilityMulti.value


@dataclass
class ExistsMultiRequest(CollectionMultiRequest):
    transcoders: Dict[str, Transcoder] = field(default_factory=dict)

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.ExistsMulti.value


@dataclass
class GetAllReplicasMultiRequest(CollectionMultiRequest):
    transcoders: Dict[str, Transcoder] = field(default_factory=dict)

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.GetAllReplicasMulti.value


@dataclass
class GetAndLockMultiRequest(CollectionMultiRequest):
    transcoders: Dict[str, Transcoder] = field(default_factory=dict)

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.GetAndLockMulti.value


@dataclass
class GetAnyReplicaMultiRequest(CollectionMultiRequest):
    transcoders: Dict[str, Transcoder] = field(default_factory=dict)

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.GetAnyReplicaMulti.value


@dataclass
class GetMultiRequest(CollectionMultiRequest):
    transcoders: Dict[str, Transcoder] = field(default_factory=dict)

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.GetMulti.value


@dataclass
class IncrementMultiRequest(CollectionMultiRequest):

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.IncrementMulti.value


@dataclass
class IncrementWithLegacyDurabilityMultiRequest(CollectionMultiRequest):

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.IncrementWithLegacyDurabilityMulti.value


@dataclass
class InsertMultiRequest(CollectionMultiRequest):

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.InsertMulti.value


@dataclass
class InsertWithLegacyDurabilityMultiRequest(CollectionMultiRequest):

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.InsertWithLegacyDurabilityMulti.value


@dataclass
class PrependMultiRequest(CollectionMultiRequest):

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.PrependMulti.value


@dataclass
class PrependWithLegacyDurabilityMultiRequest(CollectionMultiRequest):

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.PrependWithLegacyDurabilityMulti.value


@dataclass
class RemoveMultiRequest(CollectionMultiRequest):

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.RemoveMulti.value


@dataclass
class RemoveWithLegacyDurabilityMultiRequest(CollectionMultiRequest):

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.RemoveWithLegacyDurabilityMulti.value


@dataclass
class ReplaceMultiRequest(CollectionMultiRequest):

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.ReplaceMulti.value


@dataclass
class ReplaceWithLegacyDurabilityMultiRequest(CollectionMultiRequest):

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.ReplaceWithLegacyDurabilityMulti.value


@dataclass
class TouchMultiRequest(CollectionMultiRequest):

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.TouchMulti.value


@dataclass
class UnlockMultiRequest(CollectionMultiRequest):

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.UnlockMulti.value


@dataclass
class UpsertMultiRequest(CollectionMultiRequest):

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.UpsertMulti.value


@dataclass
class UpsertWithLegacyDurabilityMultiRequest(CollectionMultiRequest):

    @property
    def op_name(self) -> str:
        return KeyValueMultiOperationType.UpsertWithLegacyDurabilityMulti.value
