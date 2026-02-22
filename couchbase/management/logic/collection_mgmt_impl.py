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

from datetime import timedelta
from typing import TYPE_CHECKING, List

from couchbase.management.logic.collection_mgmt_req_builder import CollectionMgmtRequestBuilder
from couchbase.management.logic.collection_mgmt_req_types import (CollectionSpec,
                                                                  CreateCollectionRequest,
                                                                  CreateScopeRequest,
                                                                  DropCollectionRequest,
                                                                  DropScopeRequest,
                                                                  GetAllScopesRequest,
                                                                  ScopeSpec,
                                                                  UpdateCollectionRequest)

if TYPE_CHECKING:
    from couchbase.logic.client_adapter import ClientAdapter


class CollectionMgmtImpl:
    def __init__(self, client_adapter: ClientAdapter) -> None:
        self._client_adapter = client_adapter
        self._request_builder = CollectionMgmtRequestBuilder()

    @property
    def request_builder(self) -> CollectionMgmtRequestBuilder:
        """**INTERNAL**"""
        return self._request_builder

    def create_collection(self, req: CreateCollectionRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def create_scope(self, req: CreateScopeRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def drop_collection(self, req: DropCollectionRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def drop_scope(self, req: DropScopeRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def get_all_scopes(self, req: GetAllScopesRequest) -> List[ScopeSpec]:
        """**INTERNAL**"""
        res = self._client_adapter.execute_mgmt_request(req)
        scopes = []
        raw_scopes = res.raw_result['manifest']['scopes']
        for s in raw_scopes:
            scope = ScopeSpec(s['name'], list())
            for c in s['collections']:
                scope.collections.append(
                    CollectionSpec(c['name'],
                                   s['name'],
                                   timedelta(seconds=c['max_expiry']),
                                   history=c.get('history')))
            scopes.append(scope)

        return scopes

    def update_collection(self, req: UpdateCollectionRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)
