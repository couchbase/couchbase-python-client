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

from typing import (TYPE_CHECKING,
                    Any,
                    Awaitable,
                    Dict,
                    Iterable)

from acouchbase.management.logic.wrappers import AsyncMgmtWrapper
from couchbase.management.logic import ManagementType
from couchbase.management.logic.view_index_logic import (DesignDocument,
                                                         DesignDocumentNamespace,
                                                         ViewIndexManagerLogic)

if TYPE_CHECKING:
    from couchbase.management.options import (DropDesignDocumentOptions,
                                              GetAllDesignDocumentsOptions,
                                              GetDesignDocumentOptions,
                                              PublishDesignDocumentOptions,
                                              UpsertDesignDocumentOptions)


class ViewIndexManager(ViewIndexManagerLogic):
    def __init__(self, connection, loop, bucket_name):
        super().__init__(connection, bucket_name)
        self._loop = loop

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._loop

    @AsyncMgmtWrapper.inject_callbacks(DesignDocument, ManagementType.ViewIndexMgmt,
                                       ViewIndexManagerLogic._ERROR_MAPPING)
    def get_design_document(self,
                            design_doc_name,  # type: str
                            namespace,  # type: DesignDocumentNamespace
                            *options,   # type: GetDesignDocumentOptions
                            **kwargs    # type: Dict[str, Any]
                            ) -> Awaitable[DesignDocument]:
        super().get_design_document(design_doc_name, namespace, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(DesignDocument, ManagementType.ViewIndexMgmt,
                                       ViewIndexManagerLogic._ERROR_MAPPING)
    def get_all_design_documents(self,
                                 namespace,     # type: DesignDocumentNamespace
                                 *options,      # type: GetAllDesignDocumentsOptions
                                 **kwargs       # type: Dict[str, Any]
                                 ) -> Awaitable[Iterable[DesignDocument]]:
        super().get_all_design_documents(namespace, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.ViewIndexMgmt, ViewIndexManagerLogic._ERROR_MAPPING)
    def upsert_design_document(self,
                               design_doc_data,     # type: DesignDocument
                               namespace,           # type: DesignDocumentNamespace
                               *options,            # type: UpsertDesignDocumentOptions
                               **kwargs             # type: Dict[str, Any]
                               ) -> Awaitable[None]:
        super().upsert_design_document(design_doc_data, namespace, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.ViewIndexMgmt, ViewIndexManagerLogic._ERROR_MAPPING)
    def drop_design_document(self,
                             design_doc_name,   # type: str
                             namespace,         # type: DesignDocumentNamespace
                             *options,          # type: DropDesignDocumentOptions
                             **kwargs           # type: Dict[str, Any]
                             ) -> Awaitable[None]:
        super().drop_design_document(design_doc_name, namespace, *options, **kwargs)

    async def publish_design_document(self,
                                      design_doc_name,    # type: str
                                      *options,           # type: PublishDesignDocumentOptions
                                      **kwargs            # type: Dict[str, Any]
                                      ) -> Awaitable[None]:
        doc = await self.get_design_document(
            design_doc_name, DesignDocumentNamespace.DEVELOPMENT, *options, **kwargs)
        await self.upsert_design_document(
            doc, DesignDocumentNamespace.PRODUCTION, *options, **kwargs)
