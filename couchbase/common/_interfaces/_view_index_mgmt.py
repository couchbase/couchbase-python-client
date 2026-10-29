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

"""What a protocol's view index management impl must provide to the `ViewIndexManager` facades.

The impl is built by its bucket impl for one bucket, so no operation takes the bucket name.  The SDK
imports this module only under `TYPE_CHECKING`.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import (TYPE_CHECKING,
                    Any,
                    Protocol)

if TYPE_CHECKING:
    from twisted.internet.defer import Deferred

    from couchbase.common.management.options import (DropDesignDocumentOptions,
                                                     GetAllDesignDocumentsOptions,
                                                     GetDesignDocumentOptions,
                                                     PublishDesignDocumentOptions,
                                                     UpsertDesignDocumentOptions)
    from couchbase.common.management.views import DesignDocument, DesignDocumentNamespace


class ViewIndexMgmtInterface(Protocol):
    def get_design_document(self,
                            design_doc_name: str,
                            namespace: DesignDocumentNamespace,
                            options: GetDesignDocumentOptions | None = None,
                            **kwargs: Any) -> DesignDocument: ...

    def get_all_design_documents(self,
                                 namespace: DesignDocumentNamespace,
                                 options: GetAllDesignDocumentsOptions | None = None,
                                 **kwargs: Any) -> Iterable[DesignDocument]: ...

    def upsert_design_document(self,
                               design_doc_data: DesignDocument,
                               namespace: DesignDocumentNamespace,
                               options: UpsertDesignDocumentOptions | None = None,
                               **kwargs: Any) -> None: ...

    def drop_design_document(self,
                             design_doc_name: str,
                             namespace: DesignDocumentNamespace,
                             options: DropDesignDocumentOptions | None = None,
                             **kwargs: Any) -> None: ...

    def publish_design_document(self,
                                design_doc_name: str,
                                options: PublishDesignDocumentOptions | None = None,
                                **kwargs: Any) -> None: ...


class AsyncViewIndexMgmtInterface(Protocol):
    async def get_design_document(self,
                                  design_doc_name: str,
                                  namespace: DesignDocumentNamespace,
                                  options: GetDesignDocumentOptions | None = None,
                                  **kwargs: Any) -> DesignDocument: ...

    async def get_all_design_documents(self,
                                       namespace: DesignDocumentNamespace,
                                       options: GetAllDesignDocumentsOptions | None = None,
                                       **kwargs: Any) -> Iterable[DesignDocument]: ...

    async def upsert_design_document(self,
                                     design_doc_data: DesignDocument,
                                     namespace: DesignDocumentNamespace,
                                     options: UpsertDesignDocumentOptions | None = None,
                                     **kwargs: Any) -> None: ...

    async def drop_design_document(self,
                                   design_doc_name: str,
                                   namespace: DesignDocumentNamespace,
                                   options: DropDesignDocumentOptions | None = None,
                                   **kwargs: Any) -> None: ...

    async def publish_design_document(self,
                                      design_doc_name: str,
                                      options: PublishDesignDocumentOptions | None = None,
                                      **kwargs: Any) -> None: ...


class TxViewIndexMgmtInterface(Protocol):
    def get_design_document_deferred(self,
                                     design_doc_name: str,
                                     namespace: DesignDocumentNamespace,
                                     options: GetDesignDocumentOptions | None = None,
                                     **kwargs: Any) -> Deferred[DesignDocument]: ...

    def get_all_design_documents_deferred(self,
                                          namespace: DesignDocumentNamespace,
                                          options: GetAllDesignDocumentsOptions | None = None,
                                          **kwargs: Any) -> Deferred[Iterable[DesignDocument]]: ...

    def upsert_design_document_deferred(self,
                                        design_doc_data: DesignDocument,
                                        namespace: DesignDocumentNamespace,
                                        options: UpsertDesignDocumentOptions | None = None,
                                        **kwargs: Any) -> Deferred[None]: ...

    def drop_design_document_deferred(self,
                                      design_doc_name: str,
                                      namespace: DesignDocumentNamespace,
                                      options: DropDesignDocumentOptions | None = None,
                                      **kwargs: Any) -> Deferred[None]: ...

    def publish_design_document_deferred(self,
                                         design_doc_name: str,
                                         options: PublishDesignDocumentOptions | None = None,
                                         **kwargs: Any) -> Deferred[None]: ...
