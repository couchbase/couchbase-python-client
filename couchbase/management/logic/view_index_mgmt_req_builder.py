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

from typing import TYPE_CHECKING

from couchbase.exceptions import InvalidArgumentException
from couchbase.management.logic.view_index_mgmt_types import (VIEW_INDEX_MGMT_ERROR_MAP,
                                                              DropDesignDocumentRequest,
                                                              GetAllDesignDocumentsRequest,
                                                              GetDesignDocumentRequest,
                                                              UpsertDesignDocumentRequest)
from couchbase.options import forward_args
from couchbase.pycbc_core import mgmt_operations, view_index_mgmt_operations

if TYPE_CHECKING:
    from couchbase.management.logic.view_index_mgmt_types import DesignDocument, DesignDocumentNamespace


class ViewIndexMgmtRequestBuilder:

    def __init__(self) -> None:
        self._error_map = VIEW_INDEX_MGMT_ERROR_MAP

    def _get_valid_namespace(self, namespace: DesignDocumentNamespace) -> str:
        if not namespace:
            raise InvalidArgumentException('Expected design document namespace to not be None')

        return namespace.to_str()

    def _validate_design_document_name(self, design_doc_name: str) -> None:
        if not design_doc_name:
            raise InvalidArgumentException('Expected design document name to not be None')

    def _validate_design_document(self, design_doc: DesignDocument) -> None:
        if not design_doc:
            raise InvalidArgumentException('Expected design document to not be None')

    def build_drop_design_document_request(self,
                                           bucket_name: str,
                                           design_doc_name: str,
                                           namespace: DesignDocumentNamespace,
                                           *options: object,
                                           **kwargs: object) -> DropDesignDocumentRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        self._validate_design_document_name(design_doc_name)
        namespace_str = self._get_valid_namespace(namespace)
        req = DropDesignDocumentRequest(self._error_map,
                                        mgmt_operations.VIEW_INDEX.value,
                                        view_index_mgmt_operations.DROP_INDEX.value,
                                        bucket_name=bucket_name,
                                        document_name=design_doc_name,
                                        namespace=namespace_str,
                                        **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_all_design_documents_request(self,
                                               bucket_name: str,
                                               namespace: DesignDocumentNamespace,
                                               *options: object,
                                               **kwargs: object) -> GetAllDesignDocumentsRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        namespace_str = self._get_valid_namespace(namespace)
        req = GetAllDesignDocumentsRequest(self._error_map,
                                           mgmt_operations.VIEW_INDEX.value,
                                           view_index_mgmt_operations.GET_ALL_INDEXES.value,
                                           bucket_name=bucket_name,
                                           namespace=namespace_str,
                                           **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_design_document_request(self,
                                          bucket_name: str,
                                          design_doc_name: str,
                                          namespace: DesignDocumentNamespace,
                                          *options: object,
                                          **kwargs: object) -> GetDesignDocumentRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        self._validate_design_document_name(design_doc_name)
        namespace_str = self._get_valid_namespace(namespace)
        req = GetDesignDocumentRequest(self._error_map,
                                       mgmt_operations.VIEW_INDEX.value,
                                       view_index_mgmt_operations.GET_INDEX.value,
                                       bucket_name=bucket_name,
                                       document_name=design_doc_name,
                                       namespace=namespace_str,
                                       **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_upsert_design_document_request(self,
                                             bucket_name: str,
                                             design_doc: DesignDocument,
                                             namespace: DesignDocumentNamespace,
                                             *options: object,
                                             **kwargs: object) -> UpsertDesignDocumentRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        self._get_valid_namespace(namespace)
        design_doc_dict = design_doc.as_dict(namespace)
        req = UpsertDesignDocumentRequest(self._error_map,
                                          mgmt_operations.VIEW_INDEX.value,
                                          view_index_mgmt_operations.UPSERT_INDEX.value,
                                          bucket_name=bucket_name,
                                          design_document=design_doc_dict,
                                          **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req
