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
from typing import TYPE_CHECKING, Any

from couchbase.common._option_diagnostics import OptionsBlock
from couchbase.common.option_enums import OptionValidationMode
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.serializer import DefaultJsonSerializer, Serializer
from couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from couchbase.common._observability.handler import KeyspaceDetails


class CollectionOptionsBlock(OptionsBlock, protocol=CouchbaseProtocol.CLASSIC):
    Append = 'AppendOptions'
    Decrement = 'DecrementOptions'
    Exists = 'ExistsOptions'
    GetAllReplicas = 'GetAllReplicasOptions'
    GetAndLock = 'GetAndLockOptions'
    GetAndTouch = 'GetAndTouchOptions'
    GetAnyReplica = 'GetAnyReplicaOptions'
    Get = 'GetOptions'
    Increment = 'IncrementOptions'
    Insert = 'InsertOptions'
    LookupInAllReplicas = 'LookupInAllReplicasOptions'
    LookupInAnyReplica = 'LookupInAnyReplicaOptions'
    LookupIn = 'LookupInOptions'
    MutateIn = 'MutateInOptions'
    Prepend = 'PrependOptions'
    Remove = 'RemoveOptions'
    Replace = 'ReplaceOptions'
    Scan = 'ScanOptions'
    Touch = 'TouchOptions'
    Unlock = 'UnlockOptions'
    Upsert = 'UpsertOptions'


@dataclass
class CollectionDetails:
    """What an operation on this collection needs that the operation itself cannot say.

    The cluster's option validation mode travels here for the reason the default transcoder and
    serializer do: each is set once on `ClusterOptions` and read on every operation, and a request
    builder holds one of these and no route back to the cluster that made it.  The transcoder
    encodes and decodes whole documents; a subdocument value is always JSON and carries no flags,
    so the serializer encodes and decodes those.

    `kv_durable_timeout` is the cluster's, in milliseconds, for the same reason.  The binding
    builds each request itself, so the core's own durable default never reaches it and a durable
    request would otherwise go out under the plain key-value timeout.
    """
    bucket_name: str
    scope_name: str
    collection_name: str
    default_transcoder: Transcoder
    default_serializer: Serializer = field(default_factory=DefaultJsonSerializer)
    option_validation: OptionValidationMode | None = None
    kv_durable_timeout: int | None = None

    def get_details(self) -> tuple[str, str, str]:
        return self.bucket_name, self.scope_name, self.collection_name

    def get_details_as_dict(self) -> KeyspaceDetails:
        return {
            'bucket_name': self.bucket_name,
            'scope_name': self.scope_name,
            'collection_name': self.collection_name
        }

    def get_details_as_txn_dict(self) -> dict[str, str]:
        return {
            'bucket': self.bucket_name,
            'scope': self.scope_name,
            'collection_name': self.collection_name
        }

    def get_request_transcoder(self, op_args: dict[str, Any]) -> Transcoder:
        return op_args.pop('transcoder', self.default_transcoder)

    def get_request_serializer(self, op_args: dict[str, Any]) -> Serializer:
        return op_args.pop('serializer', self.default_serializer)
