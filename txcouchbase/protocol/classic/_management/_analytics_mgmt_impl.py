#  Copyright 2016-2026. Couchbase, Inc.
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

from collections.abc import Coroutine, Iterable
from typing import TYPE_CHECKING, Any

from acouchbase.protocol.classic._management._analytics_mgmt_impl import AsyncAnalyticsMgmtImpl
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from txcouchbase.protocol.classic._deferred import defer

if TYPE_CHECKING:
    from twisted.internet.defer import Deferred

    from couchbase.common.management.analytics import (AnalyticsDataset,
                                                       AnalyticsDataType,
                                                       AnalyticsIndex,
                                                       AnalyticsLink)
    from couchbase.common.management.options import (ConnectLinkOptions,
                                                     CreateAnalyticsIndexOptions,
                                                     CreateDatasetOptions,
                                                     CreateDataverseOptions,
                                                     CreateLinkAnalyticsOptions,
                                                     DisconnectLinkOptions,
                                                     DropAnalyticsIndexOptions,
                                                     DropDatasetOptions,
                                                     DropDataverseOptions,
                                                     DropLinkAnalyticsOptions,
                                                     GetAllAnalyticsIndexesOptions,
                                                     GetAllDatasetOptions,
                                                     GetLinksAnalyticsOptions,
                                                     GetPendingMutationsOptions,
                                                     ReplaceLinkAnalyticsOptions)


class TxAnalyticsMgmtImpl(AsyncAnalyticsMgmtImpl):

    def create_dataverse_deferred(self,
                                  dataverse_name: str,
                                  options: CreateDataverseOptions | None = None,
                                  **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.AnalyticsDataverseCreate, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_create_dataverse_request(dataverse_name, obs_handler, options, **kwargs)
            return self._create_dataverse(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def drop_dataverse_deferred(self,
                                dataverse_name: str,
                                options: DropDataverseOptions | None = None,
                                **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.AnalyticsDataverseDrop, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_drop_dataverse_request(dataverse_name, obs_handler, options, **kwargs)
            return self._drop_dataverse(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def create_dataset_deferred(self,
                                dataset_name: str,
                                bucket_name: str,
                                options: CreateDatasetOptions | None = None,
                                **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.AnalyticsDatasetCreate, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_create_dataset_request(dataset_name,
                                                                     bucket_name,
                                                                     obs_handler,
                                                                     options,
                                                                     **kwargs)
            return self._create_dataset(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def drop_dataset_deferred(self,
                              dataset_name: str,
                              options: DropDatasetOptions | None = None,
                              **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.AnalyticsDatasetDrop, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_drop_dataset_request(dataset_name, obs_handler, options, **kwargs)
            return self._drop_dataset(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_all_datasets_deferred(self,
                                  options: GetAllDatasetOptions | None = None,
                                  **kwargs: Any) -> Deferred[Iterable[AnalyticsDataset]]:
        obs_handler = ObservableRequestHandler(Op.AnalyticsDatasetGetAll, self._observability_instruments)

        def start() -> Coroutine[Any, Any, Iterable[AnalyticsDataset]]:
            req = self._request_builder.build_get_all_datasets_request(obs_handler, options, **kwargs)
            return self._get_all_datasets(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def create_index_deferred(self,
                              index_name: str,
                              dataset_name: str,
                              fields: dict[str, AnalyticsDataType],
                              options: CreateAnalyticsIndexOptions | None = None,
                              **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.AnalyticsIndexCreate, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_create_index_request(index_name,
                                                                   dataset_name,
                                                                   fields,
                                                                   obs_handler,
                                                                   options,
                                                                   **kwargs)
            return self._create_index(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def drop_index_deferred(self,
                            index_name: str,
                            dataset_name: str,
                            options: DropAnalyticsIndexOptions | None = None,
                            **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.AnalyticsIndexDrop, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_drop_index_request(index_name,
                                                                 dataset_name,
                                                                 obs_handler,
                                                                 options,
                                                                 **kwargs)
            return self._drop_index(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_all_indexes_deferred(self,
                                 options: GetAllAnalyticsIndexesOptions | None = None,
                                 **kwargs: Any) -> Deferred[Iterable[AnalyticsIndex]]:
        obs_handler = ObservableRequestHandler(Op.AnalyticsIndexGetAll, self._observability_instruments)

        def start() -> Coroutine[Any, Any, Iterable[AnalyticsIndex]]:
            req = self._request_builder.build_get_all_indexes_request(obs_handler, options, **kwargs)
            return self._get_all_indexes(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def connect_link_deferred(self, options: ConnectLinkOptions | None = None, **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.AnalyticsLinkConnect, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_connect_link_request(obs_handler, options, **kwargs)
            return self._connect_link(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def disconnect_link_deferred(self, options: DisconnectLinkOptions | None = None, **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.AnalyticsLinkDisconnect, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_disconnect_link_request(obs_handler, options, **kwargs)
            return self._disconnect_link(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_pending_mutations_deferred(self,
                                       options: GetPendingMutationsOptions | None = None,
                                       **kwargs: Any) -> Deferred[dict[str, int]]:
        obs_handler = ObservableRequestHandler(Op.AnalyticsGetPendingMutations, self._observability_instruments)

        def start() -> Coroutine[Any, Any, dict[str, int]]:
            req = self._request_builder.build_get_pending_mutations_request(obs_handler, options, **kwargs)
            return self._get_pending_mutations(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def create_link_deferred(self,
                             link: AnalyticsLink,
                             options: CreateLinkAnalyticsOptions | None = None,
                             **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.AnalyticsLinkCreate, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_create_link_request(link, obs_handler, options, **kwargs)
            return self._create_link(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def replace_link_deferred(self,
                              link: AnalyticsLink,
                              options: ReplaceLinkAnalyticsOptions | None = None,
                              **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.AnalyticsLinkReplace, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_replace_link_request(link, obs_handler, options, **kwargs)
            return self._replace_link(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def drop_link_deferred(self,
                           link_name: str,
                           dataverse_name: str,
                           options: DropLinkAnalyticsOptions | None = None,
                           **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.AnalyticsLinkDrop, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_drop_link_request(link_name,
                                                                dataverse_name,
                                                                obs_handler,
                                                                options,
                                                                **kwargs)
            return self._drop_link(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_links_deferred(self,
                           options: GetLinksAnalyticsOptions | None = None,
                           **kwargs: Any) -> Deferred[Iterable[AnalyticsLink]]:
        obs_handler = ObservableRequestHandler(Op.AnalyticsLinkGetAll, self._observability_instruments)

        def start() -> Coroutine[Any, Any, Iterable[AnalyticsLink]]:
            req = self._request_builder.build_get_links_request(obs_handler, options, **kwargs)
            return self._get_links(req, obs_handler)

        return defer(self.loop, obs_handler, start)
