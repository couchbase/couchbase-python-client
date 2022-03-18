from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable)

from couchbase.management.logic.view_index_logic import View  # noqa: F401
from couchbase.management.logic.view_index_logic import (DesignDocument,
                                                         DesignDocumentNamespace,
                                                         ViewIndexManagerLogic)
from couchbase.management.logic.wrappers import ViewIndexMgmtWrapper

if TYPE_CHECKING:
    from couchbase.management.options import (DropDesignDocumentOptions,
                                              GetAllDesignDocumentsOptions,
                                              GetDesignDocumentOptions,
                                              PublishDesignDocumentOptions,
                                              UpsertDesignDocumentOptions)


class ViewIndexManager(ViewIndexManagerLogic):
    def __init__(self, connection, bucket_name):
        super().__init__(connection, bucket_name)

    @ViewIndexMgmtWrapper.block(DesignDocument, ViewIndexManagerLogic._ERROR_MAPPING)
    def get_design_document(self,
                            design_doc_name,  # type: str
                            namespace,  # type: DesignDocumentNamespace
                            *options,   # type: GetDesignDocumentOptions
                            **kwargs    # type: Dict[str, Any]
                            ) -> DesignDocument:
        return super().get_design_document(design_doc_name, namespace, *options, **kwargs)

    @ViewIndexMgmtWrapper.block(DesignDocument, ViewIndexManagerLogic._ERROR_MAPPING)
    def get_all_design_documents(self,
                                 namespace,     # type: DesignDocumentNamespace
                                 *options,      # type: GetAllDesignDocumentsOptions
                                 **kwargs       # type: Dict[str, Any]
                                 ) -> Iterable[DesignDocument]:
        return super().get_all_design_documents(namespace, *options, **kwargs)

    @ViewIndexMgmtWrapper.block(None, ViewIndexManagerLogic._ERROR_MAPPING)
    def upsert_design_document(self,
                               design_doc_data,     # type: DesignDocument
                               namespace,           # type: DesignDocumentNamespace
                               *options,            # type: UpsertDesignDocumentOptions
                               **kwargs             # type: Dict[str, Any]
                               ) -> None:
        super().upsert_design_document(design_doc_data, namespace, *options, **kwargs)

    @ViewIndexMgmtWrapper.block(None, ViewIndexManagerLogic._ERROR_MAPPING)
    def drop_design_document(self,
                             design_doc_name,   # type: str
                             namespace,         # type: DesignDocumentNamespace
                             *options,          # type: DropDesignDocumentOptions
                             **kwargs           # type: Dict[str, Any]
                             ) -> None:
        super().drop_design_document(design_doc_name, namespace, *options, **kwargs)

    @ViewIndexMgmtWrapper.block(None, ViewIndexManagerLogic._ERROR_MAPPING)
    def publish_design_document(self,
                                design_doc_name,    # type: str
                                *options,           # type: PublishDesignDocumentOptions
                                **kwargs            # type: Dict[str, Any]
                                ) -> None:
        doc = self.get_design_document(
            design_doc_name, DesignDocumentNamespace.DEVELOPMENT, *options, **kwargs)
        self.upsert_design_document(
            doc, DesignDocumentNamespace.PRODUCTION, *options, **kwargs)
