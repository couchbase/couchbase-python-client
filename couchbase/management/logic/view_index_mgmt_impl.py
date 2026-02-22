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

from typing import TYPE_CHECKING, Iterable

from couchbase.logic.observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.logic.operation_types import ViewIndexMgmtOperationType
from couchbase.management.logic.view_index_mgmt_req_builder import ViewIndexMgmtRequestBuilder
from couchbase.management.logic.view_index_mgmt_types import DesignDocument, DesignDocumentNamespace

if TYPE_CHECKING:
    from couchbase.logic.client_adapter import ClientAdapter
    from couchbase.management.logic.view_index_mgmt_types import (DropDesignDocumentRequest,
                                                                  GetAllDesignDocumentsRequest,
                                                                  GetDesignDocumentRequest,
                                                                  PublishDesignDocumentRequest,
                                                                  UpsertDesignDocumentRequest)


class ViewIndexMgmtImpl:
    def __init__(self, client_adapter: ClientAdapter, observability_instruments: ObservabilityInstruments) -> None:
        self._client_adapter = client_adapter
        self._request_builder = ViewIndexMgmtRequestBuilder()
        self._observability_instruments = observability_instruments

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        """**INTERNAL**"""
        return self._observability_instruments

    @property
    def request_builder(self) -> ViewIndexMgmtRequestBuilder:
        """**INTERNAL**"""
        return self._request_builder

    def drop_design_document(
        self,
        req: DropDesignDocumentRequest,
        obs_handler: ObservableRequestHandler,
    ) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def get_all_design_documents(
        self,
        req: GetAllDesignDocumentsRequest,
        obs_handler: ObservableRequestHandler,
    ) -> Iterable[DesignDocument]:
        """**INTERNAL**"""
        ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_ddocs = ret.raw_result['design_documents']
        return [DesignDocument.from_json(ddoc) for ddoc in raw_ddocs]

    def get_design_document(
        self,
        req: GetDesignDocumentRequest,
        obs_handler: ObservableRequestHandler,
    ) -> DesignDocument:
        """**INTERNAL**"""
        ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_ddoc = ret.raw_result['document']
        return DesignDocument.from_json(raw_ddoc)

    def publish_design_document(
        self,
        req: PublishDesignDocumentRequest,
        obs_handler: ObservableRequestHandler,
    ) -> None:
        """**INTERNAL**"""
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=obs_handler.wrapped_span)

        op_type = ViewIndexMgmtOperationType.ViewIndexGet
        with ObservableRequestHandler(op_type, self._observability_instruments) as sub_obs_handler:
            sub_obs_handler.create_http_span(bucket_name=req.bucket_name, parent_span=parent_span)
            get_req = self._request_builder.build_get_design_document_request(
                req.bucket_name,
                req.design_doc_name,
                DesignDocumentNamespace.DEVELOPMENT,
                sub_obs_handler,
            )
            design_doc = self.get_design_document(get_req, sub_obs_handler)

        op_type = ViewIndexMgmtOperationType.ViewIndexUpsert
        with ObservableRequestHandler(op_type, self._observability_instruments) as sub_obs_handler:
            sub_obs_handler.create_http_span(bucket_name=req.bucket_name, parent_span=parent_span)
            up_req = self._request_builder.build_upsert_design_document_request(
                req.bucket_name,
                design_doc,
                DesignDocumentNamespace.PRODUCTION,
                sub_obs_handler,
            )
            self.upsert_design_document(up_req, sub_obs_handler)

    def upsert_design_document(
        self,
        req: UpsertDesignDocumentRequest,
        obs_handler: ObservableRequestHandler,
    ) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
