from typing import (TYPE_CHECKING,
                    Any,
                    Awaitable,
                    Dict,
                    Iterable)

from acouchbase.management.logic import ViewIndexMgmtWrapper
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

    @ViewIndexMgmtWrapper.inject_callbacks(DesignDocument, ViewIndexManagerLogic._ERROR_MAPPING)
    def get_design_document(self,
                            design_doc_name,  # type: str
                            namespace,  # type: DesignDocumentNamespace
                            *options,   # type: GetDesignDocumentOptions
                            **kwargs    # type: Dict[str, Any]
                            ) -> Awaitable[DesignDocument]:
        super().get_design_document(design_doc_name, namespace, *options, **kwargs)

    @ViewIndexMgmtWrapper.inject_callbacks(DesignDocument, ViewIndexManagerLogic._ERROR_MAPPING)
    def get_all_design_documents(self,
                                 namespace,     # type: DesignDocumentNamespace
                                 *options,      # type: GetAllDesignDocumentsOptions
                                 **kwargs       # type: Dict[str, Any]
                                 ) -> Awaitable[Iterable[DesignDocument]]:
        super().get_all_design_documents(namespace, *options, **kwargs)

    @ViewIndexMgmtWrapper.inject_callbacks(None, ViewIndexManagerLogic._ERROR_MAPPING)
    def upsert_design_document(self,
                               design_doc_data,     # type: DesignDocument
                               namespace,           # type: DesignDocumentNamespace
                               *options,            # type: UpsertDesignDocumentOptions
                               **kwargs             # type: Dict[str, Any]
                               ) -> Awaitable[None]:
        super().upsert_design_document(design_doc_data, namespace, *options, **kwargs)

    @ViewIndexMgmtWrapper.inject_callbacks(None, ViewIndexManagerLogic._ERROR_MAPPING)
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
