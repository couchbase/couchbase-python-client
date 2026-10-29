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

__all__ = [
    'QueryIndex',
]

from dataclasses import dataclass, field
from typing import Any


@dataclass
class QueryIndex:
    name: str
    is_primary: bool
    type: str
    state: str
    # The classic core does not report the namespace or the datastore, so these are None there.
    namespace: str | None
    datastore_id: str | None
    keyspace: str | None
    index_key: list[str] = field(default_factory=list)
    condition: str | None = None
    bucket_name: str | None = None
    scope_name: str | None = None
    collection_name: str | None = None
    partition: str | None = None

    @classmethod
    def from_server(cls, json_data: dict[str, Any]) -> QueryIndex:

        bucket_name = json_data.get('bucket_name', None)
        if bucket_name is None:
            bucket_name = json_data.get('keyspace_id', None)
        return cls(json_data['name'],
                   bool(json_data.get('is_primary')),
                   json_data['type'],
                   json_data['state'],
                   json_data.get('namespace_id'),
                   json_data.get('datastore_id'),
                   # The core folds keyspace_id into collection_name, or into bucket_name when the
                   # index is on the bucket itself.
                   json_data.get('keyspace_id') or json_data.get('collection_name') or bucket_name,
                   json_data.get('index_key', []),
                   json_data.get('condition', None),
                   bucket_name,
                   json_data.get('scope_name', None),
                   json_data.get('collection_name', None),
                   json_data.get('partition', None)
                   )
