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
                    Dict,
                    Iterable,
                    Optional)

from twisted.internet.defer import Deferred

from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import AnalyticsMgmtOperationType
from couchbase.management.logic.analytics_mgmt_types import (AnalyticsDataset,
                                                             AnalyticsDataType,
                                                             AnalyticsIndex,
                                                             AnalyticsLink)
from txcouchbase.management.logic.analytics_mgmt_impl import TxAnalyticsMgmtImpl

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.logic.observability import ObservabilityInstruments
    from couchbase.management.options import (ConnectLinkOptions,
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


class AnalyticsIndexManager:

    def __init__(self,
                 client_adapter: AsyncClientAdapter,
                 observability_instruments: ObservabilityInstruments) -> None:
        self._impl = TxAnalyticsMgmtImpl(client_adapter, observability_instruments)

    def create_dataverse(self,
                         dataverse_name,    # type: str
                         options=None,      # type: Optional[CreateDataverseOptions]
                         **kwargs           # type: Any
                         ) -> Deferred[None]:
        op_type = AnalyticsMgmtOperationType.AnalyticsDataverseCreate
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_create_dataverse_request(dataverse_name, obs_handler, options, **kwargs)  # noqa: E501
            d = self._impl.create_dataverse_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def drop_dataverse(self,
                       dataverse_name,    # type: str
                       options=None,      # type: Optional[DropDataverseOptions]
                       **kwargs           # type: Any
                       ) -> Deferred[None]:
        op_type = AnalyticsMgmtOperationType.AnalyticsDataverseDrop
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_drop_dataverse_request(dataverse_name, obs_handler, options, **kwargs)  # noqa: E501
            d = self._impl.drop_dataverse_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def create_dataset(self,
                       dataset_name,    # type: str
                       bucket_name,     # type: str
                       options=None,    # type: Optional[CreateDatasetOptions]
                       **kwargs         # type: Any
                       ) -> Deferred[None]:
        op_type = AnalyticsMgmtOperationType.AnalyticsDatasetCreate
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_create_dataset_request(dataset_name, bucket_name, obs_handler, options, **kwargs)  # noqa: E501
            d = self._impl.create_dataset_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def drop_dataset(self,
                     dataset_name,  # type: str
                     options=None,  # type: Optional[DropDatasetOptions]
                     **kwargs       # type: Any
                     ) -> Deferred[None]:
        op_type = AnalyticsMgmtOperationType.AnalyticsDatasetDrop
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_drop_dataset_request(dataset_name, obs_handler, options, **kwargs)
            d = self._impl.drop_dataset_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_all_datasets(self,
                         options=None,   # type: Optional[GetAllDatasetOptions]
                         **kwargs   # type: Any
                         ) -> Deferred[Iterable[AnalyticsDataset]]:
        op_type = AnalyticsMgmtOperationType.AnalyticsDatasetGetAll
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_all_datasets_request(obs_handler, options, **kwargs)
            d = self._impl.get_all_datasets_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def create_index(self,
                     index_name,    # type: str
                     dataset_name,  # type: str
                     fields,        # type: Dict[str, AnalyticsDataType]
                     options=None,  # type: Optional[CreateAnalyticsIndexOptions]
                     **kwargs       # type: Any
                     ) -> Deferred[None]:
        op_type = AnalyticsMgmtOperationType.AnalyticsIndexCreate
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_create_index_request(index_name, dataset_name, fields, obs_handler, options, **kwargs)  # noqa: E501
            d = self._impl.create_index_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def drop_index(self,
                   index_name,    # type: str
                   dataset_name,  # type: str
                   options=None,  # type: Optional[DropAnalyticsIndexOptions]
                   **kwargs       # type: Any
                   ) -> Deferred[None]:
        op_type = AnalyticsMgmtOperationType.AnalyticsIndexDrop
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_drop_index_request(index_name, dataset_name, obs_handler, options, **kwargs)  # noqa: E501
            d = self._impl.drop_index_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_all_indexes(self,
                        options=None,   # type: Optional[GetAllAnalyticsIndexesOptions]
                        **kwargs   # type: Any
                        ) -> Deferred[Iterable[AnalyticsIndex]]:
        op_type = AnalyticsMgmtOperationType.AnalyticsIndexGetAll
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_all_indexes_request(obs_handler, options, **kwargs)
            d = self._impl.get_all_indexes_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def connect_link(self,
                     options=None,  # type: Optional[ConnectLinkOptions]
                     **kwargs   # type: Any
                     ) -> Deferred[None]:
        op_type = AnalyticsMgmtOperationType.AnalyticsLinkConnect
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_connect_link_request(obs_handler, options, **kwargs)
            d = self._impl.connect_link_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def disconnect_link(self,
                        options=None,  # type: Optional[DisconnectLinkOptions]
                        **kwargs   # type: Any
                        ) -> Deferred[None]:
        op_type = AnalyticsMgmtOperationType.AnalyticsLinkDisconnect
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_disconnect_link_request(obs_handler, options, **kwargs)
            d = self._impl.disconnect_link_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_pending_mutations(self,
                              options=None,     # type: Optional[GetPendingMutationsOptions]
                              **kwargs     # type: Any
                              ) -> Deferred[Dict[str, int]]:
        op_type = AnalyticsMgmtOperationType.AnalyticsGetPendingMutations
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_pending_mutations_request(obs_handler, options, **kwargs)
            d = self._impl.get_pending_mutations_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def create_link(self,
                    link,  # type: AnalyticsLink
                    options=None,     # type: Optional[CreateLinkAnalyticsOptions]
                    **kwargs          # type: Any
                    ) -> Deferred[None]:
        # We choose AnalyticsLinkCreateCouchbaseRemoteLink arbitrarily b/c the ObservableRequestHandler will
        # translate all LinkCreate options in AnalyticsMgmtOperationType to the appropriate op name and
        # the request builder will build the appropriate request type based on the link passed in.
        op_type = AnalyticsMgmtOperationType.AnalyticsLinkCreateCouchbaseRemoteLink
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_create_link_request(link, obs_handler, options, **kwargs)
            d = self._impl.create_link_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def replace_link(self,
                     link,  # type: AnalyticsLink
                     options=None,     # type: Optional[ReplaceLinkAnalyticsOptions]
                     **kwargs          # type: Any
                     ) -> Deferred[None]:
        # We choose AnalyticsLinkReplaceCouchbaseRemoteLink arbitrarily b/c the ObservableRequestHandler will
        # translate all LinkReplace options in AnalyticsMgmtOperationType to the appropriate op name and
        # the request builder will build the appropriate request type based on the link passed in.
        op_type = AnalyticsMgmtOperationType.AnalyticsLinkReplaceCouchbaseRemoteLink
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_replace_link_request(link, obs_handler, options, **kwargs)
            d = self._impl.replace_link_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def drop_link(self,
                  link_name,  # type: str
                  dataverse_name,  # type: str
                  options=None,     # type: Optional[DropLinkAnalyticsOptions]
                  **kwargs          # type: Any
                  ) -> Deferred[None]:
        op_type = AnalyticsMgmtOperationType.AnalyticsLinkDrop
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_drop_link_request(link_name, dataverse_name, obs_handler, options, **kwargs)  # noqa: E501
            d = self._impl.drop_link_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_links(self,
                  options=None,  # type: Optional[GetLinksAnalyticsOptions]
                  **kwargs       # type: Any
                  ) -> Deferred[Iterable[AnalyticsLink]]:
        op_type = AnalyticsMgmtOperationType.AnalyticsLinkGetAll
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_links_request(obs_handler, options, **kwargs)
            d = self._impl.get_links_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise
