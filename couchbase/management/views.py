#  Copyright 2016-2022. Couchbase, Inc.
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

from typing import (TYPE_CHECKING,
                    Any,
                    Iterable)

from couchbase.management.logic.view_index_mgmt_impl import ViewIndexMgmtImpl
from couchbase.management.logic.view_index_mgmt_types import View  # noqa: F401
from couchbase.management.logic.view_index_mgmt_types import DesignDocument, DesignDocumentNamespace

# @TODO:  lets deprecate import of options from couchbase.management.views
from couchbase.management.options import (DropDesignDocumentOptions,
                                          GetAllDesignDocumentsOptions,
                                          GetDesignDocumentOptions,
                                          PublishDesignDocumentOptions,
                                          UpsertDesignDocumentOptions)

if TYPE_CHECKING:
    from couchbase.logic.client_adapter import ClientAdapter


class ViewIndexManager:

    def __init__(self, client_adapter: ClientAdapter, bucket_name: str) -> str:
        self._impl = ViewIndexMgmtImpl(client_adapter)
        self._bucket_name = bucket_name

    def get_design_document(self,
                            design_doc_name,  # type: str
                            namespace,  # type: DesignDocumentNamespace
                            *options,   # type: GetDesignDocumentOptions
                            **kwargs    # type: Any
                            ) -> DesignDocument:
        req = self._impl.request_builder.build_get_design_document_request(self._bucket_name,
                                                                           design_doc_name,
                                                                           namespace,
                                                                           *options,
                                                                           **kwargs)
        return self._impl.get_design_document(req)

    def get_all_design_documents(self,
                                 namespace,     # type: DesignDocumentNamespace
                                 *options,      # type: GetAllDesignDocumentsOptions
                                 **kwargs       # type: Any
                                 ) -> Iterable[DesignDocument]:
        req = self._impl.request_builder.build_get_all_design_documents_request(self._bucket_name,
                                                                                namespace,
                                                                                *options,
                                                                                **kwargs)
        return self._impl.get_all_design_documents(req)

    def upsert_design_document(self,
                               design_doc_data,     # type: DesignDocument
                               namespace,           # type: DesignDocumentNamespace
                               *options,            # type: UpsertDesignDocumentOptions
                               **kwargs             # type: Any
                               ) -> None:
        req = self._impl.request_builder.build_upsert_design_document_request(self._bucket_name,
                                                                              design_doc_data,
                                                                              namespace,
                                                                              *options,
                                                                              **kwargs)
        self._impl.upsert_design_document(req)

    def drop_design_document(self,
                             design_doc_name,   # type: str
                             namespace,         # type: DesignDocumentNamespace
                             *options,          # type: DropDesignDocumentOptions
                             **kwargs           # type: Any
                             ) -> None:
        req = self._impl.request_builder.build_drop_design_document_request(self._bucket_name,
                                                                            design_doc_name,
                                                                            namespace,
                                                                            *options,
                                                                            **kwargs)
        self._impl.drop_design_document(req)

    def publish_design_document(self,
                                design_doc_name,    # type: str
                                *options,           # type: PublishDesignDocumentOptions
                                **kwargs            # type: Any
                                ) -> None:
        self._impl.publish_design_document(self._bucket_name, design_doc_name, *options, **kwargs)
