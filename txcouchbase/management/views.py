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

from twisted.internet.defer import Deferred

from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import MgmtOperationType, ViewIndexMgmtOperationType
from couchbase.management.logic.view_index_mgmt_types import DesignDocument, DesignDocumentNamespace
from txcouchbase.management.logic.view_index_mgmt_impl import TxViewIndexMgmtImpl

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.logic.observability import ObservabilityInstruments
    from couchbase.management.options import (DropDesignDocumentOptions,
                                              GetAllDesignDocumentsOptions,
                                              GetDesignDocumentOptions,
                                              PublishDesignDocumentOptions,
                                              UpsertDesignDocumentOptions)


class ViewIndexManager:

    def __init__(self,
                 client_adapter: AsyncClientAdapter,
                 bucket_name: str,
                 observability_instruments: ObservabilityInstruments) -> None:
        self._impl = TxViewIndexMgmtImpl(client_adapter, observability_instruments)
        self._bucket_name = bucket_name

    def get_design_document(self,
                            design_doc_name,  # type: str
                            namespace,  # type: DesignDocumentNamespace
                            *options,   # type: GetDesignDocumentOptions
                            **kwargs    # type: Any
                            ) -> Deferred[DesignDocument]:
        op_type = ViewIndexMgmtOperationType.ViewIndexGet
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_design_document_request(self._bucket_name,
                                                                               design_doc_name,
                                                                               namespace,
                                                                               obs_handler,
                                                                               *options,
                                                                               **kwargs)
            d = self._impl.get_design_document_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_all_design_documents(self,
                                 namespace,     # type: DesignDocumentNamespace
                                 *options,      # type: GetAllDesignDocumentsOptions
                                 **kwargs       # type: Any
                                 ) -> Deferred[Iterable[DesignDocument]]:
        op_type = ViewIndexMgmtOperationType.ViewIndexGetAll
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_all_design_documents_request(self._bucket_name,
                                                                                    namespace,
                                                                                    obs_handler,
                                                                                    *options,
                                                                                    **kwargs)
            d = self._impl.get_all_design_documents_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def upsert_design_document(self,
                               design_doc_data,     # type: DesignDocument
                               namespace,           # type: DesignDocumentNamespace
                               *options,            # type: UpsertDesignDocumentOptions
                               **kwargs             # type: Any
                               ) -> Deferred[None]:
        op_type = ViewIndexMgmtOperationType.ViewIndexUpsert
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_upsert_design_document_request(self._bucket_name,
                                                                                  design_doc_data,
                                                                                  namespace,
                                                                                  obs_handler,
                                                                                  *options,
                                                                                  **kwargs)
            d = self._impl.upsert_design_document_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def drop_design_document(self,
                             design_doc_name,   # type: str
                             namespace,         # type: DesignDocumentNamespace
                             *options,          # type: DropDesignDocumentOptions
                             **kwargs           # type: Any
                             ) -> Deferred[None]:
        op_type = ViewIndexMgmtOperationType.ViewIndexDrop
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_drop_design_document_request(self._bucket_name,
                                                                                design_doc_name,
                                                                                namespace,
                                                                                obs_handler,
                                                                                *options,
                                                                                **kwargs)
            d = self._impl.drop_design_document_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def publish_design_document(self,
                                design_doc_name,    # type: str
                                *options,           # type: PublishDesignDocumentOptions
                                **kwargs            # type: Any
                                ) -> Deferred[None]:
        op_type = MgmtOperationType.ViewIndexPublish
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            d = self._impl.publish_design_document_deferred(
                self._bucket_name, design_doc_name, obs_handler, *options, **kwargs)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise
