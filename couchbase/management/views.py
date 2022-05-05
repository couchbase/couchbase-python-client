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

from typing import (Any,
                    Dict,
                    Iterable)

from couchbase.management.logic.view_index_logic import View  # noqa: F401
from couchbase.management.logic.view_index_logic import (DesignDocument,
                                                         DesignDocumentNamespace,
                                                         ViewIndexManagerLogic)
from couchbase.management.logic.wrappers import BlockingMgmtWrapper, ManagementType

# @TODO:  lets deprecate import of options from couchbase.management.views
from couchbase.management.options import (DropDesignDocumentOptions,
                                          GetAllDesignDocumentsOptions,
                                          GetDesignDocumentOptions,
                                          PublishDesignDocumentOptions,
                                          UpsertDesignDocumentOptions)


class ViewIndexManager(ViewIndexManagerLogic):
    def __init__(self, connection, bucket_name):
        super().__init__(connection, bucket_name)

    @BlockingMgmtWrapper.block(DesignDocument, ManagementType.ViewIndexMgmt, ViewIndexManagerLogic._ERROR_MAPPING)
    def get_design_document(self,
                            design_doc_name,  # type: str
                            namespace,  # type: DesignDocumentNamespace
                            *options,   # type: GetDesignDocumentOptions
                            **kwargs    # type: Dict[str, Any]
                            ) -> DesignDocument:
        return super().get_design_document(design_doc_name, namespace, *options, **kwargs)

    @BlockingMgmtWrapper.block(DesignDocument, ManagementType.ViewIndexMgmt, ViewIndexManagerLogic._ERROR_MAPPING)
    def get_all_design_documents(self,
                                 namespace,     # type: DesignDocumentNamespace
                                 *options,      # type: GetAllDesignDocumentsOptions
                                 **kwargs       # type: Dict[str, Any]
                                 ) -> Iterable[DesignDocument]:
        return super().get_all_design_documents(namespace, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.ViewIndexMgmt, ViewIndexManagerLogic._ERROR_MAPPING)
    def upsert_design_document(self,
                               design_doc_data,     # type: DesignDocument
                               namespace,           # type: DesignDocumentNamespace
                               *options,            # type: UpsertDesignDocumentOptions
                               **kwargs             # type: Dict[str, Any]
                               ) -> None:
        return super().upsert_design_document(design_doc_data, namespace, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.ViewIndexMgmt, ViewIndexManagerLogic._ERROR_MAPPING)
    def drop_design_document(self,
                             design_doc_name,   # type: str
                             namespace,         # type: DesignDocumentNamespace
                             *options,          # type: DropDesignDocumentOptions
                             **kwargs           # type: Dict[str, Any]
                             ) -> None:
        return super().drop_design_document(design_doc_name, namespace, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.ViewIndexMgmt, ViewIndexManagerLogic._ERROR_MAPPING)
    def publish_design_document(self,
                                design_doc_name,    # type: str
                                *options,           # type: PublishDesignDocumentOptions
                                **kwargs            # type: Dict[str, Any]
                                ) -> None:
        doc = self.get_design_document(
            design_doc_name, DesignDocumentNamespace.DEVELOPMENT, *options, **kwargs)
        return self.upsert_design_document(
            doc, DesignDocumentNamespace.PRODUCTION, *options, **kwargs)
