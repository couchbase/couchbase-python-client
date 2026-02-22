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

from acouchbase.management.logic.view_index_mgmt_impl import AsyncViewIndexMgmtImpl
from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import MgmtOperationType, ViewIndexMgmtOperationType
from couchbase.management.logic.view_index_mgmt_types import DesignDocument, DesignDocumentNamespace

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.logic.observability import ObservabilityInstruments
    from couchbase.management.options import (DropDesignDocumentOptions,
                                              GetAllDesignDocumentsOptions,
                                              GetDesignDocumentOptions,
                                              PublishDesignDocumentOptions,
                                              UpsertDesignDocumentOptions)


class ViewIndexManager:

    def __init__(
        self,
        client_adapter: AsyncClientAdapter,
        bucket_name: str,
        observability_instruments: ObservabilityInstruments,
    ) -> None:
        self._impl = AsyncViewIndexMgmtImpl(client_adapter, observability_instruments)
        self._bucket_name = bucket_name

    async def get_design_document(self,
                                  design_doc_name,  # type: str
                                  namespace,  # type: DesignDocumentNamespace
                                  *options,   # type: GetDesignDocumentOptions
                                  **kwargs    # type: Any
                                  ) -> DesignDocument:
        op_type = ViewIndexMgmtOperationType.ViewIndexGet
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_design_document_request(self._bucket_name,
                                                                               design_doc_name,
                                                                               namespace,
                                                                               obs_handler,
                                                                               *options,
                                                                               **kwargs)
            return await self._impl.get_design_document(req, obs_handler)

    async def get_all_design_documents(self,
                                       namespace,     # type: DesignDocumentNamespace
                                       *options,      # type: GetAllDesignDocumentsOptions
                                       **kwargs       # type: Any
                                       ) -> Iterable[DesignDocument]:
        op_type = ViewIndexMgmtOperationType.ViewIndexGetAll
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_all_design_documents_request(self._bucket_name,
                                                                                    namespace,
                                                                                    obs_handler,
                                                                                    *options,
                                                                                    **kwargs)
            return await self._impl.get_all_design_documents(req, obs_handler)

    async def upsert_design_document(self,
                                     design_doc_data,     # type: DesignDocument
                                     namespace,           # type: DesignDocumentNamespace
                                     *options,            # type: UpsertDesignDocumentOptions
                                     **kwargs             # type: Any
                                     ) -> None:
        op_type = ViewIndexMgmtOperationType.ViewIndexUpsert
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_upsert_design_document_request(self._bucket_name,
                                                                                  design_doc_data,
                                                                                  namespace,
                                                                                  obs_handler,
                                                                                  *options,
                                                                                  **kwargs)
            await self._impl.upsert_design_document(req, obs_handler)

    async def drop_design_document(self,
                                   design_doc_name,   # type: str
                                   namespace,         # type: DesignDocumentNamespace
                                   *options,          # type: DropDesignDocumentOptions
                                   **kwargs           # type: Any
                                   ) -> None:
        op_type = ViewIndexMgmtOperationType.ViewIndexDrop
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_drop_design_document_request(self._bucket_name,
                                                                                design_doc_name,
                                                                                namespace,
                                                                                obs_handler,
                                                                                *options,
                                                                                **kwargs)
            await self._impl.drop_design_document(req, obs_handler)

    async def publish_design_document(
        self,
        design_doc_name,    # type: str
        *options,           # type: PublishDesignDocumentOptions
        **kwargs,           # type: Any
    ) -> None:
        op_type = MgmtOperationType.ViewIndexPublish
        async with ObservableRequestHandler(
            op_type, self._impl.observability_instruments
        ) as obs_handler:
            req = self._impl.request_builder.build_publish_design_document_request(
                self._bucket_name,
                design_doc_name,
                obs_handler,
                *options,
                **kwargs,
            )
            await self._impl.publish_design_document(req, obs_handler)
