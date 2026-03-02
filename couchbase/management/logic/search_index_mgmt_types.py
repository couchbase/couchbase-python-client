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

import json
from dataclasses import (dataclass,
                         field,
                         fields)
from typing import (Any,
                    Callable,
                    Dict,
                    Optional,
                    Union)

from couchbase._utils import is_null_or_empty
from couchbase.exceptions import (InvalidArgumentException,
                                  QuotaLimitedException,
                                  SearchIndexNotFoundException)
from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import SearchIndexMgmtOperationType
from couchbase.management.logic.mgmt_req import MgmtRequest


@dataclass
class SearchIndex:
    """Object representation for a Couchbase search index.

    Args:
        name (str): Name of the index.
        source_type (str, optional): Type of the data source.
        idx_type (str, optional): Type of the index.
        source_name (str): Name of the source of the data for the index (e.g. bucket name).
        uuid (str, optional): UUID of the index. It must be specified for an update.
        params (dict, optional): Index properties such as store type and mappings
        source_uuid (str, optional): The UUID of the data source, this can be used to more tightly tie the index to a
            source.
        source_params (dict, optional): Extra parameters for the source. These can include advanced connection settings
            and tuning.
        plan_params (dict, optional): Plan properties such as number of replicas and number of partitions.
    """

    name: str
    source_type: str = 'couchbase'
    idx_type: str = 'fulltext-index'
    source_name: str = None
    uuid: str = None
    params: dict = field(default_factory=dict)
    source_uuid: str = None
    source_params: dict = field(default_factory=dict)
    plan_params: dict = field(default_factory=dict)

    def is_valid(self):
        return not (is_null_or_empty(self.name)
                    or is_null_or_empty(self.idx_type)
                    or is_null_or_empty(self.source_type))

    def as_dict(self):
        output = {
            'name': self.name,
            'type': self.idx_type,
            'source_type': self.source_type
        }
        if self.uuid:
            output['uuid'] = self.uuid
        if len(self.params) > 0:
            output['params_json'] = json.dumps(self.params)
        if self.source_uuid:
            output['source_uuid'] = self.source_uuid
        if self.source_name:
            output['source_name'] = self.source_name
        if len(self.source_params) > 0:
            output['source_params_json'] = json.dumps(self.source_params)
        if len(self.plan_params) > 0:
            output['plan_params_json'] = json.dumps(self.plan_params)
        return output

    @classmethod
    def from_server(cls, json_data: Dict[str, Any]) -> SearchIndex:
        params = {}
        params_json = json_data.get('params_json', None)
        if params_json:
            params = json.loads(params_json)

        source_params = {}
        source_params_json = json_data.get('source_params_json', None)
        if source_params_json:
            source_params = json.loads(source_params_json)

        plan_params = {}
        plan_params_json = json_data.get('plan_params_json', None)
        if plan_params_json:
            plan_params = json.loads(plan_params_json)

        return cls(json_data.get('name'),
                   json_data.get('source_type'),
                   json_data.get('type'),
                   json_data.get('source_name', None),
                   json_data.get('uuid', None),
                   params,
                   json_data.get('source_uuid', None),
                   source_params,
                   plan_params
                   )

    @classmethod
    def from_json(cls, json_input: Union[str, Dict[str, Any]]) -> SearchIndex:
        """ Creates a :class:`.SearchIndex` from a provided JSON str or Python dict derived from JSON.

        Args:
            json_input (Union[str, Dict[str, Any]]): JSON representation of the search index.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the provided JSON is not str or dict.
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the provided JSON does not include an index name.
        """  # noqa: E501
        json_data = json_input
        if isinstance(json_input, str):
            json_data = json.loads(json_input)

        if not isinstance(json_data, dict):
            msg = 'Provided JSON input is either not a Python dict or a JSON str that produces a Python dict.'
            raise InvalidArgumentException(msg)

        name = json_data.get('name', None)
        if name is None:
            raise InvalidArgumentException('Provided JSON input does not contain an index name.')
        if not isinstance(name, str):
            raise InvalidArgumentException('Index name must be a str.')

        name_tokens = name.split('.')
        if len(name_tokens) > 1:
            name = name_tokens[len(name_tokens)-1]

        source_type = json_data.get('sourceType', 'couchbase')
        idx_type = json_data.get('type', 'fulltext-index')
        source_name = json_data.get('sourceName', None)
        # cannot pass in the UUID or sourceUUID
        uuid = None
        source_uuid = None
        params = json_data.get('params', None)
        if params is None:
            params = {}

        source_params = json_data.get('sourceParams', None)
        if source_params is None:
            source_params = {}
        plan_params = json_data.get('planParams', None)
        if plan_params is None:
            plan_params = {}

        return cls(name,
                   source_type,
                   idx_type,
                   source_name,
                   uuid,
                   params,
                   source_uuid,
                   source_params,
                   plan_params)


# we have these params on the top-level pycbc_core request
OPARG_SKIP_LIST = ['error_map']


@dataclass
class SearchIndexMgmtRequest(MgmtRequest):

    def req_to_dict(self,
                    obs_handler: Optional[ObservableRequestHandler] = None,
                    callback: Optional[Callable[..., None]] = None,
                    errback: Optional[Callable[..., None]] = None) -> Dict[str, Any]:
        mgmt_kwargs = {
            field.name: getattr(self, field.name)
            for field in fields(self)
            if field.name not in OPARG_SKIP_LIST and getattr(self, field.name) is not None
        }
        if callback is not None:
            mgmt_kwargs['callback'] = callback

        if errback is not None:
            mgmt_kwargs['errback'] = errback

        return mgmt_kwargs


@dataclass
class AllowQueryingRequest(SearchIndexMgmtRequest):
    index_name: str
    allow: bool
    bucket_name: Optional[str] = None
    scope_name: Optional[str] = None
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return SearchIndexMgmtOperationType.SearchIndexControlQuery.value


@dataclass
class AnalyzeDocumentRequest(SearchIndexMgmtRequest):
    index_name: str
    encoded_document: str
    bucket_name: Optional[str] = None
    scope_name: Optional[str] = None
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return SearchIndexMgmtOperationType.SearchIndexAnalyzeDocument.value


@dataclass
class DisallowQueryingRequest(SearchIndexMgmtRequest):
    index_name: str
    allow: bool
    bucket_name: Optional[str] = None
    scope_name: Optional[str] = None
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return SearchIndexMgmtOperationType.SearchIndexControlQuery.value


@dataclass
class DropIndexRequest(SearchIndexMgmtRequest):
    index_name: str
    bucket_name: Optional[str] = None
    scope_name: Optional[str] = None
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return SearchIndexMgmtOperationType.SearchIndexDrop.value


@dataclass
class FreezePlanRequest(SearchIndexMgmtRequest):
    index_name: str
    freeze: bool
    bucket_name: Optional[str] = None
    scope_name: Optional[str] = None
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return SearchIndexMgmtOperationType.SearchIndexControlPlanFreeze.value


@dataclass
class GetAllIndexesRequest(SearchIndexMgmtRequest):
    bucket_name: Optional[str] = None
    scope_name: Optional[str] = None
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return SearchIndexMgmtOperationType.SearchIndexGetAll.value


@dataclass
class GetAllIndexStatsRequest(SearchIndexMgmtRequest):
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return SearchIndexMgmtOperationType.SearchGetStats.value


@dataclass
class GetIndexedDocumentsCountRequest(SearchIndexMgmtRequest):
    index_name: str
    bucket_name: Optional[str] = None
    scope_name: Optional[str] = None
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return SearchIndexMgmtOperationType.SearchIndexGetDocumentsCount.value


@dataclass
class GetIndexRequest(SearchIndexMgmtRequest):
    index_name: str
    bucket_name: Optional[str] = None
    scope_name: Optional[str] = None
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return SearchIndexMgmtOperationType.SearchIndexGet.value


@dataclass
class GetIndexStatsRequest(SearchIndexMgmtRequest):
    index_name: str
    bucket_name: Optional[str] = None
    scope_name: Optional[str] = None
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return SearchIndexMgmtOperationType.SearchIndexGetStats.value


@dataclass
class PauseIngestRequest(SearchIndexMgmtRequest):
    index_name: str
    pause: bool
    bucket_name: Optional[str] = None
    scope_name: Optional[str] = None
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return SearchIndexMgmtOperationType.SearchIndexControlIngest.value


@dataclass
class ResumeIngestRequest(SearchIndexMgmtRequest):
    index_name: str
    pause: bool
    bucket_name: Optional[str] = None
    scope_name: Optional[str] = None
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return SearchIndexMgmtOperationType.SearchIndexControlIngest.value


@dataclass
class UnfreezePlanRequest(SearchIndexMgmtRequest):
    index_name: str
    freeze: bool
    bucket_name: Optional[str] = None
    scope_name: Optional[str] = None
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return SearchIndexMgmtOperationType.SearchIndexControlPlanFreeze.value


@dataclass
class UpsertIndexRequest(SearchIndexMgmtRequest):
    index: Dict[str, Any]
    bucket_name: Optional[str] = None
    scope_name: Optional[str] = None
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return SearchIndexMgmtOperationType.SearchIndexUpsert.value


SEARCH_INDEX_MGMT_ERROR_MAP: Dict[str, Exception] = {
    r'.*[iI]ndex not found.*': SearchIndexNotFoundException,
    r'.*[Cc]reate[Ii]ndex, [Pp]repare failed, err: [Ee]xceeds indexes limit': QuotaLimitedException
}
