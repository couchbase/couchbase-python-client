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
                    Dict,
                    Iterable)

from twisted.internet.defer import Deferred, inlineCallbacks

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

    def get_design_document(self,
                            design_doc_name,  # type: str
                            namespace,  # type: DesignDocumentNamespace
                            *options,   # type: GetDesignDocumentOptions
                            **kwargs    # type: Dict[str, Any]
                            ) -> Deferred[DesignDocument]:
        return Deferred.fromFuture(super().get_design_document(design_doc_name, namespace, *options, **kwargs))

    def get_all_design_documents(self,
                                 namespace,     # type: DesignDocumentNamespace
                                 *options,      # type: GetAllDesignDocumentsOptions
                                 **kwargs       # type: Dict[str, Any]
                                 ) -> Deferred[Iterable[DesignDocument]]:
        return Deferred.fromFuture(super().get_all_design_documents(namespace, *options, **kwargs))

    def upsert_design_document(self,
                               design_doc_data,     # type: DesignDocument
                               namespace,           # type: DesignDocumentNamespace
                               *options,            # type: UpsertDesignDocumentOptions
                               **kwargs             # type: Dict[str, Any]
                               ) -> Deferred[None]:
        return Deferred.fromFuture(super().upsert_design_document(design_doc_data, namespace, *options, **kwargs))

    def drop_design_document(self,
                             design_doc_name,   # type: str
                             namespace,         # type: DesignDocumentNamespace
                             *options,          # type: DropDesignDocumentOptions
                             **kwargs           # type: Dict[str, Any]
                             ) -> Deferred[None]:
        return Deferred.fromFuture(super().drop_design_document(design_doc_name, namespace, *options, **kwargs))

    @inlineCallbacks
    def _publish_design_document(self,
                                 design_doc_name,    # type: str
                                 *options,           # type: PublishDesignDocumentOptions
                                 **kwargs            # type: Dict[str, Any]
                                 ) -> Deferred[None]:

        doc = yield self.get_design_document(
            design_doc_name, DesignDocumentNamespace.DEVELOPMENT, *options, **kwargs)

        yield self.upsert_design_document(
            doc, DesignDocumentNamespace.PRODUCTION, *options, **kwargs)

    def publish_design_document(self,
                                design_doc_name,    # type: str
                                *options,           # type: PublishDesignDocumentOptions
                                **kwargs            # type: Dict[str, Any]
                                ) -> Deferred[None]:

        d = Deferred()

        def _on_ok(_):
            d.callback(None)

        def _on_err(exc):
            d.errback(exc)

        pub_ddod_d = self._publish_design_document(design_doc_name, *options, **kwargs)
        pub_ddod_d.addCallback(_on_ok)
        pub_ddod_d.addErrback(_on_err)

        return d
