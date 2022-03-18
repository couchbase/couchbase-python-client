from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    Awaitable,
                    Dict,
                    Iterable,
                    Optional)

from acouchbase.management.logic import AnalyticsMgmtWrapper
from couchbase.exceptions import InvalidArgumentException
from couchbase.management.logic.analytics_logic import (AnalyticsDataset,
                                                        AnalyticsDataType,
                                                        AnalyticsIndex,
                                                        AnalyticsLink,
                                                        AnalyticsManagerLogic,
                                                        AzureBlobExternalAnalyticsLink,
                                                        CouchbaseRemoteAnalyticsLink,
                                                        S3ExternalAnalyticsLink)

if TYPE_CHECKING:
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


class AnalyticsIndexManager(AnalyticsManagerLogic):

    def __init__(self, connection, loop):
        super().__init__(connection)
        self._loop = loop

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._loop

    @AnalyticsMgmtWrapper.inject_callbacks(None, AnalyticsManagerLogic._ERROR_MAPPING)
    def create_dataverse(self,
                         dataverse_name,    # type: str
                         options=None,      # type: Optional[CreateDataverseOptions]
                         **kwargs           # type: Dict[str, Any]
                         ) -> Awaitable[None]:

        if not isinstance(dataverse_name, str):
            raise ValueError("dataverse_name must be provided when creating an analytics dataverse.")

        super().create_dataverse(dataverse_name, options, **kwargs)

    @AnalyticsMgmtWrapper.inject_callbacks(None, AnalyticsManagerLogic._ERROR_MAPPING)
    def drop_dataverse(self,
                       dataverse_name,    # type: str
                       options=None,      # type: Optional[DropDataverseOptions]
                       **kwargs           # type: Dict[str, Any]
                       ) -> Awaitable[None]:

        if not isinstance(dataverse_name, str):
            raise ValueError("dataverse_name must be provided when dropping an analytics dataverse.")

        super().drop_dataverse(dataverse_name, options, **kwargs)

    @AnalyticsMgmtWrapper.inject_callbacks(None, AnalyticsManagerLogic._ERROR_MAPPING)
    def create_dataset(self,
                       dataset_name,    # type: str
                       bucket_name,     # type: str
                       options=None,    # type: Optional[CreateDatasetOptions]
                       **kwargs         # type: Dict[str, Any]
                       ) -> Awaitable[None]:

        if not isinstance(dataset_name, str):
            raise ValueError("dataset_name must be provided when creating an analytics dataset.")

        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when creating an analytics dataset.")

        super().create_dataset(dataset_name, bucket_name, options, **kwargs)

    @AnalyticsMgmtWrapper.inject_callbacks(None, AnalyticsManagerLogic._ERROR_MAPPING)
    def drop_dataset(self,
                     dataset_name,  # type: str
                     options=None,  # type: Optional[DropDatasetOptions]
                     **kwargs       # type: Dict[str, Any]
                     ) -> Awaitable[None]:

        if not isinstance(dataset_name, str):
            raise ValueError("dataset_name must be provided when dropping an analytics dataset.")

        super().drop_dataset(dataset_name, options, **kwargs)

    @AnalyticsMgmtWrapper.inject_callbacks(AnalyticsDataset, AnalyticsManagerLogic._ERROR_MAPPING)
    def get_all_datasets(self,
                         options=None,   # type: Optional[GetAllDatasetOptions]
                         **kwargs   # type: Dict[str, Any]
                         ) -> Awaitable[Iterable[AnalyticsDataset]]:

        super().get_all_datasets(options, **kwargs)

    @AnalyticsMgmtWrapper.inject_callbacks(None, AnalyticsManagerLogic._ERROR_MAPPING)
    def create_index(self,
                     index_name,    # type: str
                     dataset_name,  # type: str
                     fields,        # type: Dict[str, AnalyticsDataType]
                     options=None,  # type: Optional[CreateAnalyticsIndexOptions]
                     **kwargs       # type: Dict[str, Any]
                     ) -> Awaitable[None]:

        if not isinstance(index_name, str):
            raise ValueError("index_name must be provided when creating an analytics index.")

        if not isinstance(dataset_name, str):
            raise ValueError("dataset_name must be provided when creating an analytics index.")

        if fields is not None:
            if not isinstance(fields, dict):
                raise ValueError("fields must be provided when creating an analytics index.")

            if not all(map(lambda v: isinstance(v, AnalyticsDataType), fields.values())):
                raise InvalidArgumentException("fields must all be an AnalyticsDataType.")

        super().create_index(index_name, dataset_name, fields, options, **kwargs)

    @AnalyticsMgmtWrapper.inject_callbacks(None, AnalyticsManagerLogic._ERROR_MAPPING)
    def drop_index(self,
                   index_name,    # type: str
                   dataset_name,  # type: str
                   options=None,  # type: Optional[DropAnalyticsIndexOptions]
                   **kwargs       # type: Dict[str, Any]
                   ) -> Awaitable[None]:

        if not isinstance(index_name, str):
            raise ValueError("index_name must be provided when dropping an analytics index.")

        if not isinstance(dataset_name, str):
            raise ValueError("dataset_name must be provided when dropping an analytics index.")

        super().drop_index(index_name, dataset_name, options, **kwargs)

    @AnalyticsMgmtWrapper.inject_callbacks(AnalyticsIndex, AnalyticsManagerLogic._ERROR_MAPPING)
    def get_all_indexes(self,
                        options=None,   # type: Optional[GetAllAnalyticsIndexesOptions]
                        **kwargs   # type: Dict[str, Any]
                        ) -> Awaitable[Iterable[AnalyticsIndex]]:

        super().get_all_indexes(options, **kwargs)

    @AnalyticsMgmtWrapper.inject_callbacks(None, AnalyticsManagerLogic._ERROR_MAPPING)
    def connect_link(self,
                     options=None,  # type: Optional[ConnectLinkOptions]
                     **kwargs   # type: Dict[str, Any]
                     ) -> Awaitable[None]:
        super().connect_link(options, **kwargs)

    @AnalyticsMgmtWrapper.inject_callbacks(None, AnalyticsManagerLogic._ERROR_MAPPING)
    def disconnect_link(self,
                        options=None,  # type: Optional[DisconnectLinkOptions]
                        **kwargs   # type: Dict[str, Any]
                        ) -> Awaitable[None]:
        super().disconnect_link(options, **kwargs)

    @AnalyticsMgmtWrapper.inject_callbacks(dict, AnalyticsManagerLogic._ERROR_MAPPING)
    def get_pending_mutations(self,
                              options=None,     # type: Optional[GetPendingMutationsOptions]
                              **kwargs     # type: Dict[str, Any]
                              ) -> Dict[str, int]:

        super().get_pending_mutations(options, **kwargs)

    @AnalyticsMgmtWrapper.inject_callbacks(None, AnalyticsManagerLogic._ERROR_MAPPING)
    def create_link(
        self,
        link,  # type: AnalyticsLink
        options=None,     # type: Optional[CreateLinkAnalyticsOptions]
        **kwargs
    ) -> Awaitable[None]:
        super().create_link(link, options, **kwargs)

    @AnalyticsMgmtWrapper.inject_callbacks(None, AnalyticsManagerLogic._ERROR_MAPPING)
    def replace_link(
        self,
        link,  # type: AnalyticsLink
        options=None,     # type: Optional[ReplaceLinkAnalyticsOptions]
        **kwargs
    ) -> Awaitable[None]:
        super().replace_link(link, options, **kwargs)

    @AnalyticsMgmtWrapper.inject_callbacks(None, AnalyticsManagerLogic._ERROR_MAPPING)
    def drop_link(
        self,
        link_name,  # type: str
        dataverse_name,  # type: str
        options=None,     # type: Optional[DropLinkAnalyticsOptions]
        **kwargs
    ) -> Awaitable[None]:

        if not isinstance(link_name, str):
            raise ValueError("link_name must be provided when dropping an analytics link.")

        if not isinstance(dataverse_name, str):
            raise ValueError("dataverse_name must be provided when dropping an analytics link.")

        super().drop_link(link_name, dataverse_name, options, **kwargs)

    @AnalyticsMgmtWrapper.inject_callbacks((CouchbaseRemoteAnalyticsLink,
                                            S3ExternalAnalyticsLink,
                                            AzureBlobExternalAnalyticsLink), AnalyticsManagerLogic._ERROR_MAPPING)
    def get_links(
        self,
        options=None,  # type: Optional[GetLinksAnalyticsOptions]
        **kwargs
    ) -> Awaitable[Iterable[AnalyticsLink]]:
        super().get_links(options, **kwargs)
