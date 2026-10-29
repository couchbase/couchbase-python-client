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

import time
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common.management.views import DesignDocument, DesignDocumentNamespace
from couchbase.protocol.classic._management._view_index_mgmt_req_builder import ViewIndexMgmtRequestBuilder

if TYPE_CHECKING:
    from couchbase.common.management.options import (DropDesignDocumentOptions,
                                                     GetAllDesignDocumentsOptions,
                                                     GetDesignDocumentOptions,
                                                     PublishDesignDocumentOptions,
                                                     UpsertDesignDocumentOptions)
    from couchbase.protocol.classic._client_adapter import ClientAdapter
    from couchbase.protocol.classic._management._view_index_mgmt_types import (GetDesignDocumentRequest,
                                                                               UpsertDesignDocumentRequest)


class ViewIndexMgmtImpl:
    def __init__(self,
                 client_adapter: ClientAdapter,
                 bucket_name: str,
                 observability_instruments: ObservabilityInstruments) -> None:
        self._client_adapter = client_adapter
        self._bucket_name = bucket_name
        self._request_builder = ViewIndexMgmtRequestBuilder(client_adapter.option_validation)
        self._observability_instruments = observability_instruments

    def drop_design_document(self,
                             design_doc_name: str,
                             namespace: DesignDocumentNamespace,
                             options: DropDesignDocumentOptions | None = None,
                             **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.ViewIndexDrop, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_design_document_request(self._bucket_name,
                                                                           design_doc_name,
                                                                           namespace,
                                                                           obs_handler,
                                                                           options,
                                                                           **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def get_all_design_documents(self,
                                 namespace: DesignDocumentNamespace,
                                 options: GetAllDesignDocumentsOptions | None = None,
                                 **kwargs: Any) -> Iterable[DesignDocument]:
        with ObservableRequestHandler(Op.ViewIndexGetAll, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_design_documents_request(self._bucket_name,
                                                                               namespace,
                                                                               obs_handler,
                                                                               options,
                                                                               **kwargs)
            ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            raw_ddocs = ret.raw_result['design_documents']
            return [DesignDocument.from_json(ddoc) for ddoc in raw_ddocs]

    def get_design_document(self,
                            design_doc_name: str,
                            namespace: DesignDocumentNamespace,
                            options: GetDesignDocumentOptions | None = None,
                            **kwargs: Any) -> DesignDocument:
        with ObservableRequestHandler(Op.ViewIndexGet, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_design_document_request(self._bucket_name,
                                                                          design_doc_name,
                                                                          namespace,
                                                                          obs_handler,
                                                                          options,
                                                                          **kwargs)
            return self._get_design_document(req, obs_handler)

    def publish_design_document(self,
                                design_doc_name: str,
                                options: PublishDesignDocumentOptions | None = None,
                                **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.ViewIndexPublish, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_publish_design_document_request(self._bucket_name,
                                                                              design_doc_name,
                                                                              obs_handler,
                                                                              options,
                                                                              **kwargs)
            parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=obs_handler.wrapped_span)
            # The get and the upsert share one deadline, the publish timeout.
            deadline = None if req.timeout is None else time.monotonic() + req.timeout / 1000

            with ObservableRequestHandler(Op.ViewIndexGet, self._observability_instruments) as sub_obs_handler:
                get_req = self._request_builder.build_get_design_document_request(
                    req.bucket_name,
                    req.design_doc_name,
                    DesignDocumentNamespace.DEVELOPMENT,
                    sub_obs_handler,
                    parent_span=parent_span,
                )
                get_req.timeout = req.timeout
                design_doc = self._get_design_document(get_req, sub_obs_handler)

            with ObservableRequestHandler(Op.ViewIndexUpsert, self._observability_instruments) as sub_obs_handler:
                up_req = self._request_builder.build_upsert_design_document_request(
                    req.bucket_name,
                    design_doc,
                    DesignDocumentNamespace.PRODUCTION,
                    sub_obs_handler,
                    parent_span=parent_span,
                )
                if deadline is not None:
                    up_req.timeout = int((deadline - time.monotonic()) * 1e3)
                self._upsert_design_document(up_req, sub_obs_handler)

    def upsert_design_document(self,
                               design_doc_data: DesignDocument,
                               namespace: DesignDocumentNamespace,
                               options: UpsertDesignDocumentOptions | None = None,
                               **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.ViewIndexUpsert, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_upsert_design_document_request(self._bucket_name,
                                                                             design_doc_data,
                                                                             namespace,
                                                                             obs_handler,
                                                                             options,
                                                                             **kwargs)
            self._upsert_design_document(req, obs_handler)

    def _get_design_document(self,
                             req: GetDesignDocumentRequest,
                             obs_handler: ObservableRequestHandler) -> DesignDocument:
        ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        return DesignDocument.from_json(ret.raw_result['document'])

    def _upsert_design_document(self,
                                req: UpsertDesignDocumentRequest,
                                obs_handler: ObservableRequestHandler) -> None:
        self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
