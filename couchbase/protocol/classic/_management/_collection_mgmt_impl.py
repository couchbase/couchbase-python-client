#  Copyright 2016-2026. Couchbase, Inc.
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

from collections.abc import Iterable
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common.management.collections import CollectionSpec, ScopeSpec
from couchbase.protocol.classic._management._collection_mgmt_req_builder import CollectionMgmtRequestBuilder

if TYPE_CHECKING:
    from couchbase.common._observability import ObservabilityInstruments
    from couchbase.common.management.collections import UpdateCollectionSettings
    from couchbase.common.management.options import (CreateScopeOptions,
                                                     DropScopeOptions,
                                                     GetAllScopesOptions,
                                                     UpdateCollectionOptions)
    from couchbase.protocol.classic._client_adapter import ClientAdapter


class CollectionMgmtImpl:
    def __init__(self,
                 client_adapter: ClientAdapter,
                 bucket_name: str,
                 observability_instruments: ObservabilityInstruments) -> None:
        self._client_adapter = client_adapter
        self._bucket_name = bucket_name
        self._request_builder = CollectionMgmtRequestBuilder(client_adapter.option_validation)
        self._observability_instruments = observability_instruments

    def create_scope(self,
                     scope_name: str,
                     options: CreateScopeOptions | None = None,
                     **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.ScopeCreate, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_create_scope_request(self._bucket_name,
                                                                   scope_name,
                                                                   obs_handler,
                                                                   options,
                                                                   **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def drop_scope(self,
                   scope_name: str,
                   options: DropScopeOptions | None = None,
                   **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.ScopeDrop, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_scope_request(self._bucket_name,
                                                                 scope_name,
                                                                 obs_handler,
                                                                 options,
                                                                 **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def get_all_scopes(self,
                       options: GetAllScopesOptions | None = None,
                       **kwargs: Any) -> Iterable[ScopeSpec]:
        with ObservableRequestHandler(Op.ScopeGetAll, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_scopes_request(self._bucket_name, obs_handler, options, **kwargs)
            res = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            raw_scopes = res.raw_result['manifest']['scopes']
            return [ScopeSpec(s['name'],
                              [CollectionSpec(c['name'],
                                              s['name'],
                                              timedelta(seconds=c['max_expiry']),
                                              history=c.get('history'))
                               for c in s['collections']])
                    for s in raw_scopes]

    def update_collection(self,
                          scope_name: str,
                          collection_name: str,
                          settings: UpdateCollectionSettings,
                          options: UpdateCollectionOptions | None = None,
                          **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.CollectionUpdate, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_update_collection_request(self._bucket_name,
                                                                        scope_name,
                                                                        collection_name,
                                                                        settings,
                                                                        obs_handler,
                                                                        options,
                                                                        **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def create_collection(self, *args: Any, **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.CollectionCreate, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_create_collection_request(self._bucket_name,
                                                                        *args,
                                                                        obs_handler=obs_handler,
                                                                        **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def drop_collection(self, *args: Any, **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.CollectionDrop, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_collection_request(self._bucket_name,
                                                                      *args,
                                                                      obs_handler=obs_handler,
                                                                      **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
