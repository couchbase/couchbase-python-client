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
from twisted.python.failure import Failure

from acouchbase.management.logic.view_index_mgmt_impl import AsyncViewIndexMgmtImpl
from couchbase.logic.observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.management.logic.view_index_mgmt_types import DesignDocument

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.management.logic.view_index_mgmt_types import (DropDesignDocumentRequest,
                                                                  GetAllDesignDocumentsRequest,
                                                                  GetDesignDocumentRequest,
                                                                  UpsertDesignDocumentRequest)


class TxViewIndexMgmtImpl(AsyncViewIndexMgmtImpl):
    def __init__(self,
                 client_adapter: AsyncClientAdapter,
                 observability_instruments: ObservabilityInstruments) -> None:
        super().__init__(client_adapter, observability_instruments)

    def _finish_span(self, result, obs_handler: ObservableRequestHandler):
        """Callback to properly end the span on success or failure."""
        if isinstance(result, Failure):
            exc = result.value
            obs_handler.__exit__(type(exc), exc, exc.__traceback__)
            return result
        else:
            obs_handler.__exit__(None, None, None)
            return result

    def drop_design_document_deferred(self,
                                      req: DropDesignDocumentRequest,
                                      obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_design_document(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_design_documents_deferred(self,
                                          req: GetAllDesignDocumentsRequest,
                                          obs_handler: ObservableRequestHandler) -> Deferred[Iterable[DesignDocument]]:
        """**INTERNAL**"""
        coro = super().get_all_design_documents(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_design_document_deferred(self,
                                     req: GetDesignDocumentRequest,
                                     obs_handler: ObservableRequestHandler) -> Deferred[DesignDocument]:
        """**INTERNAL**"""
        coro = super().get_design_document(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def publish_design_document_deferred(self,
                                         bucket_name: str,
                                         design_doc_name: str,
                                         obs_handler: ObservableRequestHandler,
                                         *options: object,
                                         **kwargs: object) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().publish_design_document(bucket_name, design_doc_name, obs_handler, *options, **kwargs)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def upsert_design_document_deferred(self,
                                        req: UpsertDesignDocumentRequest,
                                        obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().upsert_design_document(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d
