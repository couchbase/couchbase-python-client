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

__all__ = [
    'AnalyticsDataType',
    'AnalyticsDataset',
    'AnalyticsEncryptionLevel',
    'AnalyticsIndex',
    'AnalyticsIndexManager',
    'AnalyticsLink',
    'AnalyticsLinkType',
    'AzureBlobExternalAnalyticsLink',
    'CouchbaseAnalyticsEncryptionSettings',
    'CouchbaseRemoteAnalyticsLink',
    'S3ExternalAnalyticsLink',
]

from collections.abc import Iterable
from typing import TYPE_CHECKING

from couchbase.common.management.analytics import AnalyticsEncryptionLevel  # noqa: F401
from couchbase.common.management.analytics import AnalyticsLinkType  # noqa: F401
from couchbase.common.management.analytics import AzureBlobExternalAnalyticsLink  # noqa: F401
from couchbase.common.management.analytics import CouchbaseAnalyticsEncryptionSettings  # noqa: F401
from couchbase.common.management.analytics import CouchbaseRemoteAnalyticsLink  # noqa: F401
from couchbase.common.management.analytics import S3ExternalAnalyticsLink  # noqa: F401
from couchbase.common.management.analytics import (AnalyticsDataset,
                                                   AnalyticsDataType,
                                                   AnalyticsIndex,
                                                   AnalyticsLink)

# @TODO:  lets deprecate import of options from couchbase.management.analytics
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

if TYPE_CHECKING:
    from typing_extensions import Unpack

    from couchbase.common._interfaces._analytics_mgmt import AnalyticsMgmtInterface
    from couchbase.common.management.options import (ConnectLinkOptionsKwargs,
                                                     CreateAnalyticsIndexOptionsKwargs,
                                                     CreateDatasetOptionsKwargs,
                                                     CreateDataverseOptionsKwargs,
                                                     CreateLinkAnalyticsOptionsKwargs,
                                                     DisconnectLinkOptionsKwargs,
                                                     DropAnalyticsIndexOptionsKwargs,
                                                     DropDatasetOptionsKwargs,
                                                     DropDataverseOptionsKwargs,
                                                     DropLinkAnalyticsOptionsKwargs,
                                                     GetAllAnalyticsIndexesOptionsKwargs,
                                                     GetAllDatasetOptionsKwargs,
                                                     GetLinksAnalyticsOptionsKwargs,
                                                     GetPendingMutationsOptionsKwargs,
                                                     ReplaceLinkAnalyticsOptionsKwargs)


class AnalyticsIndexManager:
    """Performs management operations on analytics dataverses, datasets, indexes and links."""

    def __init__(self, impl: AnalyticsMgmtInterface) -> None:
        self._impl = impl

    def create_dataverse(self,
                         dataverse_name: str,
                         options: CreateDataverseOptions | None = None,
                         **kwargs: Unpack[CreateDataverseOptionsKwargs]) -> None:
        """Creates a new analytics dataverse.

        Args:
            dataverse_name (str): The name of the dataverse to create.
            options (:class:`~couchbase.management.options.CreateDataverseOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the dataverse_name is not a str.
            :class:`~couchbase.exceptions.DataverseAlreadyExistsException`: If the dataverse already exists and
                ignore_if_exists is not set.
        """
        self._impl.create_dataverse(dataverse_name, options, **kwargs)

    def drop_dataverse(self,
                       dataverse_name: str,
                       options: DropDataverseOptions | None = None,
                       **kwargs: Unpack[DropDataverseOptionsKwargs]) -> None:
        """Drops an existing analytics dataverse.

        Args:
            dataverse_name (str): The name of the dataverse to drop.
            options (:class:`~couchbase.management.options.DropDataverseOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the dataverse_name is not a str.
            :class:`~couchbase.exceptions.DataverseNotFoundException`: If the dataverse does not exist and
                ignore_if_not_exists is not set.
        """
        self._impl.drop_dataverse(dataverse_name, options, **kwargs)

    def create_dataset(self,
                       dataset_name: str,
                       bucket_name: str,
                       options: CreateDatasetOptions | None = None,
                       **kwargs: Unpack[CreateDatasetOptionsKwargs]) -> None:
        """Creates a new analytics dataset over a bucket.

        Args:
            dataset_name (str): The name of the dataset to create.
            bucket_name (str): The name of the bucket the dataset reads from.
            options (:class:`~couchbase.management.options.CreateDatasetOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the dataset_name or bucket_name is not a str.
            :class:`~couchbase.exceptions.DatasetAlreadyExistsException`: If the dataset already exists and
                ignore_if_exists is not set.
        """
        self._impl.create_dataset(dataset_name, bucket_name, options, **kwargs)

    def drop_dataset(self,
                     dataset_name: str,
                     options: DropDatasetOptions | None = None,
                     **kwargs: Unpack[DropDatasetOptionsKwargs]) -> None:
        """Drops an existing analytics dataset.

        Args:
            dataset_name (str): The name of the dataset to drop.
            options (:class:`~couchbase.management.options.DropDatasetOptions`): Optional parameters for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the dataset_name is not a str.
            :class:`~couchbase.exceptions.DatasetNotFoundException`: If the dataset does not exist and
                ignore_if_not_exists is not set.
        """
        self._impl.drop_dataset(dataset_name, options, **kwargs)

    def get_all_datasets(self,
                         options: GetAllDatasetOptions | None = None,
                         **kwargs: Unpack[GetAllDatasetOptionsKwargs]) -> Iterable[AnalyticsDataset]:
        """Returns every analytics dataset.

        Args:
            options (:class:`~couchbase.management.options.GetAllDatasetOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Iterable[:class:`.AnalyticsDataset`]: The datasets.
        """
        return self._impl.get_all_datasets(options, **kwargs)

    def create_index(self,
                     index_name: str,
                     dataset_name: str,
                     fields: dict[str, AnalyticsDataType],
                     options: CreateAnalyticsIndexOptions | None = None,
                     **kwargs: Unpack[CreateAnalyticsIndexOptionsKwargs]) -> None:
        """Creates a new analytics index on a dataset.

        Args:
            index_name (str): The name of the index to create.
            dataset_name (str): The name of the dataset to index.
            fields (dict[str, :class:`.AnalyticsDataType`]): The fields the index covers, each mapped to its type.
            options (:class:`~couchbase.management.options.CreateAnalyticsIndexOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index_name or dataset_name is not a str, or
                fields is not a dict of :class:`.AnalyticsDataType` values.
        """
        self._impl.create_index(index_name, dataset_name, fields, options, **kwargs)

    def drop_index(self,
                   index_name: str,
                   dataset_name: str,
                   options: DropAnalyticsIndexOptions | None = None,
                   **kwargs: Unpack[DropAnalyticsIndexOptionsKwargs]) -> None:
        """Drops an existing analytics index.

        Args:
            index_name (str): The name of the index to drop.
            dataset_name (str): The name of the dataset the index is on.
            options (:class:`~couchbase.management.options.DropAnalyticsIndexOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index_name or dataset_name is not a str.
        """
        self._impl.drop_index(index_name, dataset_name, options, **kwargs)

    def get_all_indexes(self,
                        options: GetAllAnalyticsIndexesOptions | None = None,
                        **kwargs: Unpack[GetAllAnalyticsIndexesOptionsKwargs]) -> Iterable[AnalyticsIndex]:
        """Returns every analytics index.

        Args:
            options (:class:`~couchbase.management.options.GetAllAnalyticsIndexesOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Iterable[:class:`.AnalyticsIndex`]: The indexes.
        """
        return self._impl.get_all_indexes(options, **kwargs)

    def connect_link(self,
                     options: ConnectLinkOptions | None = None,
                     **kwargs: Unpack[ConnectLinkOptionsKwargs]) -> None:
        """Connects a link, so that its datasets start receiving data.

        Args:
            options (:class:`~couchbase.management.options.ConnectLinkOptions`): Optional parameters for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.
        """
        self._impl.connect_link(options, **kwargs)

    def disconnect_link(self,
                        options: DisconnectLinkOptions | None = None,
                        **kwargs: Unpack[DisconnectLinkOptionsKwargs]) -> None:
        """Disconnects a link, so that its datasets stop receiving data.

        Args:
            options (:class:`~couchbase.management.options.DisconnectLinkOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.
        """
        self._impl.disconnect_link(options, **kwargs)

    def get_pending_mutations(self,
                              options: GetPendingMutationsOptions | None = None,
                              **kwargs: Unpack[GetPendingMutationsOptionsKwargs]) -> dict[str, int]:
        """Returns how many mutations each dataset has yet to process.

        Args:
            options (:class:`~couchbase.management.options.GetPendingMutationsOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            dict[str, int]: The number of pending mutations, keyed by dataverse and dataset name.
        """
        return self._impl.get_pending_mutations(options, **kwargs)

    def create_link(self,
                    link: AnalyticsLink,
                    options: CreateLinkAnalyticsOptions | None = None,
                    **kwargs: Unpack[CreateLinkAnalyticsOptionsKwargs]) -> None:
        """Creates a new analytics link.

        Args:
            link (:class:`.AnalyticsLink`): The link to create: a :class:`.CouchbaseRemoteAnalyticsLink`,
                :class:`.S3ExternalAnalyticsLink` or :class:`.AzureBlobExternalAnalyticsLink`.
            options (:class:`~couchbase.management.options.CreateLinkAnalyticsOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the link is not an :class:`.AnalyticsLink`, or
                is not valid.
            :class:`~couchbase.exceptions.AnalyticsLinkExistsException`: If the link already exists.
            :class:`~couchbase.exceptions.DataverseNotFoundException`: If the link's dataverse does not exist.
        """
        self._impl.create_link(link, options, **kwargs)

    def replace_link(self,
                     link: AnalyticsLink,
                     options: ReplaceLinkAnalyticsOptions | None = None,
                     **kwargs: Unpack[ReplaceLinkAnalyticsOptionsKwargs]) -> None:
        """Replaces an existing analytics link.

        Args:
            link (:class:`.AnalyticsLink`): The link to replace: a :class:`.CouchbaseRemoteAnalyticsLink`,
                :class:`.S3ExternalAnalyticsLink` or :class:`.AzureBlobExternalAnalyticsLink`.
            options (:class:`~couchbase.management.options.ReplaceLinkAnalyticsOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the link is not an :class:`.AnalyticsLink`, or
                is not valid.
            :class:`~couchbase.exceptions.AnalyticsLinkNotFoundException`: If the link does not exist.
            :class:`~couchbase.exceptions.DataverseNotFoundException`: If the link's dataverse does not exist.
        """
        self._impl.replace_link(link, options, **kwargs)

    def drop_link(self,
                  link_name: str,
                  dataverse_name: str,
                  options: DropLinkAnalyticsOptions | None = None,
                  **kwargs: Unpack[DropLinkAnalyticsOptionsKwargs]) -> None:
        """Drops an existing analytics link.

        Args:
            link_name (str): The name of the link to drop.
            dataverse_name (str): The name of the dataverse the link is in.
            options (:class:`~couchbase.management.options.DropLinkAnalyticsOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the link_name or dataverse_name is not a str.
            :class:`~couchbase.exceptions.AnalyticsLinkNotFoundException`: If the link does not exist.
            :class:`~couchbase.exceptions.DataverseNotFoundException`: If the dataverse does not exist.
        """
        self._impl.drop_link(link_name, dataverse_name, options, **kwargs)

    def get_links(self,
                  options: GetLinksAnalyticsOptions | None = None,
                  **kwargs: Unpack[GetLinksAnalyticsOptionsKwargs]) -> Iterable[AnalyticsLink]:
        """Returns the analytics links, all of them unless the options narrow the search.

        Args:
            options (:class:`~couchbase.management.options.GetLinksAnalyticsOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Iterable[:class:`.AnalyticsLink`]: The links.

        Raises:
            :class:`~couchbase.exceptions.DataverseNotFoundException`: If the dataverse named in the options does not
                exist.
        """
        return self._impl.get_links(options, **kwargs)
