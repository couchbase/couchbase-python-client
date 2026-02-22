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
from typing import (TYPE_CHECKING,
                    Dict,
                    Iterable)

from twisted.internet.defer import Deferred
from twisted.python.failure import Failure

from acouchbase.management.logic.analytics_mgmt_impl import AsyncAnalyticsMgmtImpl
from couchbase.logic.observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.management.logic.analytics_mgmt_types import (AnalyticsDataset,
                                                             AnalyticsIndex,
                                                             AnalyticsLink)

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.management.logic.analytics_mgmt_types import (ConnectLinkRequest,
                                                                 CreateDatasetRequest,
                                                                 CreateDataverseRequest,
                                                                 CreateIndexRequest,
                                                                 CreateLinkRequest,
                                                                 DisconnectLinkRequest,
                                                                 DropDatasetRequest,
                                                                 DropDataverseRequest,
                                                                 DropIndexRequest,
                                                                 DropLinkRequest,
                                                                 GetAllDatasetsRequest,
                                                                 GetAllIndexesRequest,
                                                                 GetLinksRequest,
                                                                 GetPendingMutationsRequest,
                                                                 ReplaceLinkRequest)


class TxAnalyticsMgmtImpl(AsyncAnalyticsMgmtImpl):
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

    def connect_link_deferred(self,
                              req: ConnectLinkRequest,
                              obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().connect_link(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def create_dataset_deferred(self,
                                req: CreateDatasetRequest,
                                obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().create_dataset(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def create_dataverse_deferred(self,
                                  req: CreateDataverseRequest,
                                  obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().create_dataverse(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def create_index_deferred(self,
                              req: CreateIndexRequest,
                              obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().create_index(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def create_link_deferred(self,
                             req: CreateLinkRequest,
                             obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().create_link(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def disconnect_link_deferred(self,
                                 req: DisconnectLinkRequest,
                                 obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().disconnect_link(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_dataset_deferred(self,
                              req: DropDatasetRequest,
                              obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_dataset(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_dataverse_deferred(self,
                                req: DropDataverseRequest,
                                obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_dataverse(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_index_deferred(self,
                            req: DropIndexRequest,
                            obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_index(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_link_deferred(self,
                           req: DropLinkRequest,
                           obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_link(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_datasets_deferred(self,
                                  req: GetAllDatasetsRequest,
                                  obs_handler: ObservableRequestHandler) -> Deferred[Iterable[AnalyticsDataset]]:
        """**INTERNAL**"""
        coro = super().get_all_datasets(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_indexes_deferred(self,
                                 req: GetAllIndexesRequest,
                                 obs_handler: ObservableRequestHandler) -> Deferred[Iterable[AnalyticsIndex]]:
        """**INTERNAL**"""
        coro = super().get_all_indexes(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_links_deferred(self,
                           req: GetLinksRequest,
                           obs_handler: ObservableRequestHandler) -> Deferred[Iterable[AnalyticsLink]]:
        """**INTERNAL**"""
        coro = super().get_links(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_pending_mutations_deferred(self,
                                       req: GetPendingMutationsRequest,
                                       obs_handler: ObservableRequestHandler) -> Deferred[Dict[str, int]]:
        """**INTERNAL**"""
        coro = super().get_pending_mutations(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def replace_link_deferred(self,
                              req: ReplaceLinkRequest,
                              obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().replace_link(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d
