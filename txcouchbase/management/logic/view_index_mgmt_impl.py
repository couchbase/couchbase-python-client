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

import asyncio
from typing import TYPE_CHECKING, Iterable

from twisted.internet.defer import Deferred

from acouchbase.management.logic.view_index_mgmt_impl import AsyncViewIndexMgmtImpl
from couchbase.management.logic.view_index_mgmt_types import DesignDocument

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.management.logic.view_index_mgmt_types import (DropDesignDocumentRequest,
                                                                  GetAllDesignDocumentsRequest,
                                                                  GetDesignDocumentRequest,
                                                                  UpsertDesignDocumentRequest)


class TxViewIndexMgmtImpl(AsyncViewIndexMgmtImpl):
    def __init___(self, client_adapter: AsyncClientAdapter) -> None:
        super().__init__(client_adapter)

    def drop_design_document_deferred(self, req: DropDesignDocumentRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_design_document(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_design_documents_deferred(self,
                                          req: GetAllDesignDocumentsRequest) -> Deferred[Iterable[DesignDocument]]:
        """**INTERNAL**"""
        coro = super().get_all_design_documents(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_design_document_deferred(self, req: GetDesignDocumentRequest) -> Deferred[DesignDocument]:
        """**INTERNAL**"""
        coro = super().get_design_document(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def publish_design_document_deferred(self,
                                         bucket_name: str,
                                         design_doc_name: str,
                                         *options: object,
                                         **kwargs: object) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().publish_design_document(bucket_name, design_doc_name, *options, **kwargs)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def upsert_design_document_deferred(self, req: UpsertDesignDocumentRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().upsert_design_document(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d
