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

from collections.abc import Coroutine, Iterable
from typing import TYPE_CHECKING, Any

from acouchbase.protocol.classic._management._view_index_mgmt_impl import AsyncViewIndexMgmtImpl
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from txcouchbase.protocol.classic._deferred import defer

if TYPE_CHECKING:
    from twisted.internet.defer import Deferred

    from couchbase.common.management.options import (DropDesignDocumentOptions,
                                                     GetAllDesignDocumentsOptions,
                                                     GetDesignDocumentOptions,
                                                     PublishDesignDocumentOptions,
                                                     UpsertDesignDocumentOptions)
    from couchbase.common.management.views import DesignDocument, DesignDocumentNamespace


class TxViewIndexMgmtImpl(AsyncViewIndexMgmtImpl):

    def drop_design_document_deferred(self,
                                      design_doc_name: str,
                                      namespace: DesignDocumentNamespace,
                                      options: DropDesignDocumentOptions | None = None,
                                      **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.ViewIndexDrop, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_drop_design_document_request(self._bucket_name,
                                                                           design_doc_name,
                                                                           namespace,
                                                                           obs_handler,
                                                                           options,
                                                                           **kwargs)
            return self._drop_design_document(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_all_design_documents_deferred(self,
                                          namespace: DesignDocumentNamespace,
                                          options: GetAllDesignDocumentsOptions | None = None,
                                          **kwargs: Any) -> Deferred[Iterable[DesignDocument]]:
        obs_handler = ObservableRequestHandler(Op.ViewIndexGetAll, self._observability_instruments)

        def start() -> Coroutine[Any, Any, Iterable[DesignDocument]]:
            req = self._request_builder.build_get_all_design_documents_request(self._bucket_name,
                                                                               namespace,
                                                                               obs_handler,
                                                                               options,
                                                                               **kwargs)
            return self._get_all_design_documents(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_design_document_deferred(self,
                                     design_doc_name: str,
                                     namespace: DesignDocumentNamespace,
                                     options: GetDesignDocumentOptions | None = None,
                                     **kwargs: Any) -> Deferred[DesignDocument]:
        obs_handler = ObservableRequestHandler(Op.ViewIndexGet, self._observability_instruments)

        def start() -> Coroutine[Any, Any, DesignDocument]:
            req = self._request_builder.build_get_design_document_request(self._bucket_name,
                                                                          design_doc_name,
                                                                          namespace,
                                                                          obs_handler,
                                                                          options,
                                                                          **kwargs)
            return self._get_design_document(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def publish_design_document_deferred(self,
                                         design_doc_name: str,
                                         options: PublishDesignDocumentOptions | None = None,
                                         **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.ViewIndexPublish, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_publish_design_document_request(self._bucket_name,
                                                                              design_doc_name,
                                                                              obs_handler,
                                                                              options,
                                                                              **kwargs)
            return self._publish_design_document(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def upsert_design_document_deferred(self,
                                        design_doc_data: DesignDocument,
                                        namespace: DesignDocumentNamespace,
                                        options: UpsertDesignDocumentOptions | None = None,
                                        **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.ViewIndexUpsert, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_upsert_design_document_request(self._bucket_name,
                                                                             design_doc_data,
                                                                             namespace,
                                                                             obs_handler,
                                                                             options,
                                                                             **kwargs)
            return self._upsert_design_document(req, obs_handler)

        return defer(self.loop, obs_handler, start)
