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

from acouchbase.management.logic.analytics_mgmt_impl import AsyncAnalyticsMgmtImpl
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
    def __init___(self, client_adapter: AsyncClientAdapter) -> None:
        super().__init__(client_adapter)

    def connect_link_deferred(self, req: ConnectLinkRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().connect_link(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def create_dataset_deferred(self, req: CreateDatasetRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().create_dataset(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def create_dataverse_deferred(self, req: CreateDataverseRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().create_dataverse(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def create_index_deferred(self, req: CreateIndexRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().create_index(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def create_link_deferred(self, req: CreateLinkRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().create_link(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def disconnect_link_deferred(self, req: DisconnectLinkRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().discconnect_link(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_dataset_deferred(self, req: DropDatasetRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_dataset(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_dataverse_deferred(self, req: DropDataverseRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_dataverse(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_index_deferred(self, req: DropIndexRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_index(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_link_deferred(self, req: DropLinkRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_link(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_datasets_deferred(self, req: GetAllDatasetsRequest) -> Deferred[Iterable[AnalyticsDataset]]:
        """**INTERNAL**"""
        coro = super().get_all_datasets(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_indexes_deferred(self, req: GetAllIndexesRequest) -> Deferred[Iterable[AnalyticsIndex]]:
        """**INTERNAL**"""
        coro = super().get_all_indexes(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_links_deferred(self, req: GetLinksRequest) -> Deferred[Iterable[AnalyticsLink]]:
        """**INTERNAL**"""
        coro = super().get_links(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_pending_mutations_deferred(self, req: GetPendingMutationsRequest) -> Deferred[Dict[str, int]]:
        """**INTERNAL**"""
        coro = super().get_pending_mutations(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def replace_link_deferred(self, req: ReplaceLinkRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().replace_link(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d
