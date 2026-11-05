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
    """A query index, as the query service describes it.

    Attributes:
        name (str): The name of the index.
        is_primary (bool): Whether the index is a primary index.
        type (str): The kind of index, such as ``gsi``.
        state (str): The state of the index, such as ``online`` or ``deferred``.
        namespace (str | None): The namespace the index is in, if the service reports it.
        datastore_id (str | None): The datastore the index is in, if the service reports it.
        keyspace (str | None): What the index is on: its collection, or its bucket for an index on the bucket itself.
        index_key (list[str]): The expressions the index keys on.
        condition (str | None): The ``WHERE`` clause of a partial index.
        bucket_name (str | None): The name of the bucket the index is in.
        scope_name (str | None): The name of the scope the index is in.
        collection_name (str | None): The name of the collection the index is on.
        partition (str | None): The ``PARTITION BY`` clause of a partitioned index.
    """

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
        """**INTERNAL** The index the query service described."""
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
