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

from dataclasses import (dataclass,
                         field,
                         fields)
from typing import (Any,
                    Callable,
                    Dict,
                    List,
                    Optional,
                    Tuple,
                    Union)

from couchbase.exceptions import QueryIndexAlreadyExistsException, QueryIndexNotFoundException
from couchbase.logic.operation_types import QueryIndexMgmtOperationType
from couchbase.management.logic.mgmt_req import MgmtRequest


@dataclass
class QueryIndex:
    name: str
    is_primary: bool
    type: str
    state: str
    namespace: str
    datastore_id: str
    keyspace: str
    index_key: list = field(default_factory=list)
    condition: str = None
    bucket_name: str = None
    scope_name: str = None
    collection_name: str = None
    partition: str = None

    @classmethod
    def from_server(cls, json_data: Dict[str, Any]) -> QueryIndex:

        bucket_name = json_data.get('bucket_name', None)
        if bucket_name is None:
            bucket_name = json_data.get('keyspace_id', None)
        return cls(json_data.get('name'),
                   bool(json_data.get('is_primary')),
                   json_data.get('type'),
                   json_data.get('state'),
                   json_data.get('namespace_id'),
                   json_data.get('datastore_id'),
                   json_data.get('keyspace_id'),
                   json_data.get('index_key', []),
                   json_data.get('condition', None),
                   bucket_name,
                   json_data.get('scope_name', None),
                   json_data.get('collection_name', None),
                   json_data.get('partition', None)
                   )


# we have these params on the top-level pycbc_core request
OPARG_SKIP_LIST = ['mgmt_op', 'op_type', 'timeout', 'error_map']


@dataclass
class QueryIndexMgmtRequest(MgmtRequest):
    mgmt_op: str
    op_type: str
    # TODO: maybe timeout isn't optional, but defaults to default timeout?
    #       otherwise that makes inheritance tricky w/ child classes having required params

    def req_to_dict(self,
                    conn: Any,
                    callback: Optional[Callable[..., None]] = None,
                    errback: Optional[Callable[..., None]] = None) -> Dict[str, Any]:
        mgmt_kwargs = {
            'conn': conn,
            'mgmt_op': self.mgmt_op,
            'op_type': self.op_type,
        }

        if callback is not None:
            mgmt_kwargs['callback'] = callback

        if errback is not None:
            mgmt_kwargs['errback'] = errback

        if self.timeout is not None:
            mgmt_kwargs['timeout'] = self.timeout

        mgmt_kwargs['op_args'] = {
            field.name: getattr(self, field.name)
            for field in fields(self)
            if field.name not in OPARG_SKIP_LIST and getattr(self, field.name) is not None
        }

        return mgmt_kwargs


@dataclass
class CreateIndexRequest(QueryIndexMgmtRequest):
    bucket_name: str
    index_name: Optional[str] = None
    is_primary: Optional[bool] = None
    condition: Optional[str] = None
    keys: Optional[List[str]] = None
    ignore_if_exists: Optional[bool] = None
    scope_name: Optional[str] = None
    collection_name: Optional[str] = None
    deferred: Optional[bool] = None
    num_replicas: Optional[int] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return QueryIndexMgmtOperationType.CreateIndex.value


@dataclass
class DropIndexRequest(QueryIndexMgmtRequest):
    bucket_name: str
    index_name: Optional[str] = None
    is_primary: Optional[bool] = None
    ignore_if_does_not_exist: Optional[bool] = None
    scope_name: Optional[str] = None
    collection_name: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return QueryIndexMgmtOperationType.DropIndex.value


@dataclass
class GetAllIndexesRequest(QueryIndexMgmtRequest):
    bucket_name: str
    scope_name: Optional[str] = None
    collection_name: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return QueryIndexMgmtOperationType.GetAllIndexes.value


@dataclass
class BuildDeferredIndexesRequest(QueryIndexMgmtRequest):
    bucket_name: str
    scope_name: Optional[str] = None
    collection_name: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return QueryIndexMgmtOperationType.BuildDeferredIndexes.value


@dataclass
class WatchIndexesRequest(QueryIndexMgmtRequest):
    bucket_name: str
    index_names: Union[List[str], Tuple[str]]
    timeout: int
    scope_name: Optional[str] = None
    collection_name: Optional[str] = None

    @property
    def op_name(self) -> str:
        return QueryIndexMgmtOperationType.WatchIndexes.value


QUERY_INDEX_MGMT_ERROR_MAP: Dict[str, Exception] = {
    r'.*[iI]ndex.*already exists.*': QueryIndexAlreadyExistsException,
    r'.*[iI]ndex.*[nN]ot [fF]ound.*': QueryIndexNotFoundException
}
