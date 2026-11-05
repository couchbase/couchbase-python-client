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

__all__ = [
    'AllowQueryingSearchIndexOptions',
    'AllowQueryingSearchIndexOptionsKwargs',
    'AnalyzeDocumentSearchIndexOptions',
    'AnalyzeDocumentSearchIndexOptionsKwargs',
    'BucketDescribeOptions',
    'BucketDescribeOptionsKwargs',
    'BuildDeferredQueryIndexOptions',
    'BuildDeferredQueryIndexOptionsKwargs',
    'ChangePasswordOptions',
    'ChangePasswordOptionsKwargs',
    'ConnectLinkOptions',
    'ConnectLinkOptionsKwargs',
    'CreateAnalyticsIndexOptions',
    'CreateAnalyticsIndexOptionsKwargs',
    'CreateBucketOptions',
    'CreateBucketOptionsKwargs',
    'CreateCollectionOptions',
    'CreateCollectionOptionsKwargs',
    'CreateDatasetOptions',
    'CreateDatasetOptionsKwargs',
    'CreateDataverseOptions',
    'CreateDataverseOptionsKwargs',
    'CreateLinkAnalyticsOptions',
    'CreateLinkAnalyticsOptionsKwargs',
    'CreatePrimaryQueryIndexOptions',
    'CreatePrimaryQueryIndexOptionsKwargs',
    'CreateQueryIndexOptions',
    'CreateQueryIndexOptionsKwargs',
    'CreateScopeOptions',
    'CreateScopeOptionsKwargs',
    'DeployFunctionOptions',
    'DeployFunctionOptionsKwargs',
    'DisallowQueryingSearchIndexOptions',
    'DisallowQueryingSearchIndexOptionsKwargs',
    'DisconnectLinkOptions',
    'DisconnectLinkOptionsKwargs',
    'DropAnalyticsIndexOptions',
    'DropAnalyticsIndexOptionsKwargs',
    'DropBucketOptions',
    'DropBucketOptionsKwargs',
    'DropCollectionOptions',
    'DropCollectionOptionsKwargs',
    'DropDatasetOptions',
    'DropDatasetOptionsKwargs',
    'DropDataverseOptions',
    'DropDataverseOptionsKwargs',
    'DropDesignDocumentOptions',
    'DropDesignDocumentOptionsKwargs',
    'DropFunctionOptions',
    'DropFunctionOptionsKwargs',
    'DropGroupOptions',
    'DropGroupOptionsKwargs',
    'DropLinkAnalyticsOptions',
    'DropLinkAnalyticsOptionsKwargs',
    'DropPrimaryQueryIndexOptions',
    'DropPrimaryQueryIndexOptionsKwargs',
    'DropQueryIndexOptions',
    'DropQueryIndexOptionsKwargs',
    'DropScopeOptions',
    'DropScopeOptionsKwargs',
    'DropSearchIndexOptions',
    'DropSearchIndexOptionsKwargs',
    'DropUserOptions',
    'DropUserOptionsKwargs',
    'EventingFunctionOptions',
    'EventingFunctionOptionsKwargs',
    'FlushBucketOptions',
    'FlushBucketOptionsKwargs',
    'FreezePlanSearchIndexOptions',
    'FreezePlanSearchIndexOptionsKwargs',
    'FunctionsStatusOptions',
    'FunctionsStatusOptionsKwargs',
    'GetAllAnalyticsIndexesOptions',
    'GetAllAnalyticsIndexesOptionsKwargs',
    'GetAllBucketOptions',
    'GetAllBucketOptionsKwargs',
    'GetAllDatasetOptions',
    'GetAllDatasetOptionsKwargs',
    'GetAllDesignDocumentsOptions',
    'GetAllDesignDocumentsOptionsKwargs',
    'GetAllFunctionOptions',
    'GetAllFunctionOptionsKwargs',
    'GetAllGroupsOptions',
    'GetAllGroupsOptionsKwargs',
    'GetAllQueryIndexOptions',
    'GetAllQueryIndexOptionsKwargs',
    'GetAllScopesOptions',
    'GetAllScopesOptionsKwargs',
    'GetAllSearchIndexStatsOptions',
    'GetAllSearchIndexStatsOptionsKwargs',
    'GetAllSearchIndexesOptions',
    'GetAllSearchIndexesOptionsKwargs',
    'GetAllUsersOptions',
    'GetAllUsersOptionsKwargs',
    'GetBucketOptions',
    'GetBucketOptionsKwargs',
    'GetDesignDocumentOptions',
    'GetDesignDocumentOptionsKwargs',
    'GetFunctionOptions',
    'GetFunctionOptionsKwargs',
    'GetGroupOptions',
    'GetGroupOptionsKwargs',
    'GetLinksAnalyticsOptions',
    'GetLinksAnalyticsOptionsKwargs',
    'GetPendingMutationsOptions',
    'GetPendingMutationsOptionsKwargs',
    'GetRolesOptions',
    'GetRolesOptionsKwargs',
    'GetSearchIndexOptions',
    'GetSearchIndexOptionsKwargs',
    'GetSearchIndexStatsOptions',
    'GetSearchIndexStatsOptionsKwargs',
    'GetSearchIndexedDocumentsCountOptions',
    'GetSearchIndexedDocumentsCountOptionsKwargs',
    'GetUserOptions',
    'GetUserOptionsKwargs',
    'PauseFunctionOptions',
    'PauseFunctionOptionsKwargs',
    'PauseIngestSearchIndexOptions',
    'PauseIngestSearchIndexOptionsKwargs',
    'PublishDesignDocumentOptions',
    'PublishDesignDocumentOptionsKwargs',
    'ReplaceLinkAnalyticsOptions',
    'ReplaceLinkAnalyticsOptionsKwargs',
    'ResumeFunctionOptions',
    'ResumeFunctionOptionsKwargs',
    'ResumeIngestSearchIndexOptions',
    'ResumeIngestSearchIndexOptionsKwargs',
    'UndeployFunctionOptions',
    'UndeployFunctionOptionsKwargs',
    'UnfreezePlanSearchIndexOptions',
    'UnfreezePlanSearchIndexOptionsKwargs',
    'UpdateBucketOptions',
    'UpdateBucketOptionsKwargs',
    'UpdateCollectionOptions',
    'UpdateCollectionOptionsKwargs',
    'UpsertDesignDocumentOptions',
    'UpsertDesignDocumentOptionsKwargs',
    'UpsertFunctionOptions',
    'UpsertFunctionOptionsKwargs',
    'UpsertGroupOptions',
    'UpsertGroupOptionsKwargs',
    'UpsertSearchIndexOptions',
    'UpsertSearchIndexOptionsKwargs',
    'UpsertUserOptions',
    'UpsertUserOptionsKwargs',
    'WatchQueryIndexOptions',
    'WatchQueryIndexOptionsKwargs',
]

from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from datetime import timedelta

    from typing_extensions import Unpack

    from couchbase.common.auth import AuthDomain
    from couchbase.common.management.analytics import AnalyticsLinkType
    from couchbase.common.observability.tracing import RequestSpan


# ==========================================================================================
# Analytics Management
# ==========================================================================================


class ConnectLinkOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.ConnectLinkOptions`."""

    timeout: timedelta | None
    dataverse_name: str | None
    link_name: str | None
    force: bool | None
    parent_span: RequestSpan | None


class ConnectLinkOptions(dict):
    """Available options for :meth:`~couchbase.management.analytics.AnalyticsIndexManager.connect_link`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``analytics_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        dataverse_name (str, optional): The name of the dataverse the operation applies to.
        link_name (str, optional): The name of the link.
        force (bool, optional): Whether to connect the link even if the bucket's UUID has changed, as it does when a
            bucket is dropped and created again.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[ConnectLinkOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def dataverse_name(self) -> str:
        """The value of the ``dataverse_name`` option, or ``'Default'`` if it was not set."""
        return self.get("dataverse_name", 'Default')

    @property
    def link_name(self) -> str:
        """The value of the ``link_name`` option, or ``'Local'`` if it was not set."""
        return self.get("link_name", 'Local')

    @property
    def force(self) -> bool:
        """The value of the ``force`` option, or ``False`` if it was not set."""
        return self.get("force", False)


class CreateAnalyticsIndexOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.CreateAnalyticsIndexOptions`."""

    timeout: timedelta | None
    ignore_if_exists: bool | None
    dataverse_name: str | None
    parent_span: RequestSpan | None


class CreateAnalyticsIndexOptions(dict):
    """Available options for :meth:`~couchbase.management.analytics.AnalyticsIndexManager.create_index`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``analytics_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        ignore_if_exists (bool, optional): Whether or not the call should ignore the index already existing when
            determining whether the call was successful.
        dataverse_name (str, optional): The name of the dataverse the operation applies to.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[CreateAnalyticsIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def ignore_if_exists(self) -> bool:
        """The value of the ``ignore_if_exists`` option, or ``False`` if it was not set."""
        return self.get("ignore_if_exists", False)

    @property
    def dataverse_name(self) -> str:
        """The value of the ``dataverse_name`` option, or ``'Default'`` if it was not set."""
        return self.get("dataverse_name", 'Default')


class CreateDatasetOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.CreateDatasetOptions`."""

    timeout: timedelta | None
    ignore_if_exists: bool | None
    condition: str | None
    dataverse_name: str | None
    parent_span: RequestSpan | None


class CreateDatasetOptions(dict):
    """Available options for :meth:`~couchbase.management.analytics.AnalyticsIndexManager.create_dataset`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``analytics_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        ignore_if_exists (bool, optional): Whether or not the call should ignore the index already existing when
            determining whether the call was successful.
        condition (str, optional): Specifies the 'where' condition for partial index creation.
        dataverse_name (str, optional): The name of the dataverse the operation applies to.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[CreateDatasetOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def ignore_if_exists(self) -> bool:
        """The value of the ``ignore_if_exists`` option, or ``False`` if it was not set."""
        return self.get("ignore_if_exists", False)

    @property
    def condition(self) -> str | None:
        """The value of the ``condition`` option, or ``None`` if it was not set."""
        return self.get("condition", None)

    @property
    def dataverse_name(self) -> str:
        """The value of the ``dataverse_name`` option, or ``'Default'`` if it was not set."""
        return self.get("dataverse_name", 'Default')


class CreateDataverseOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.CreateDataverseOptions`."""

    timeout: timedelta | None
    ignore_if_exists: bool | None
    parent_span: RequestSpan | None


class CreateDataverseOptions(dict):
    """Available options for :meth:`~couchbase.management.analytics.AnalyticsIndexManager.create_dataverse`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``analytics_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        ignore_if_exists (bool, optional): Whether or not the call should ignore the index already existing when
            determining whether the call was successful.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[CreateDataverseOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def ignore_if_exists(self) -> bool:
        """The value of the ``ignore_if_exists`` option, or ``False`` if it was not set."""
        return self.get("ignore_if_exists", False)


class CreateLinkAnalyticsOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.CreateLinkAnalyticsOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class CreateLinkAnalyticsOptions(dict):
    """Available options for :meth:`~couchbase.management.analytics.AnalyticsIndexManager.create_link`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``analytics_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[CreateLinkAnalyticsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class DisconnectLinkOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.DisconnectLinkOptions`."""

    timeout: timedelta | None
    dataverse_name: str | None
    link_name: str | None
    parent_span: RequestSpan | None


class DisconnectLinkOptions(dict):
    """Available options for :meth:`~couchbase.management.analytics.AnalyticsIndexManager.disconnect_link`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``analytics_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        dataverse_name (str, optional): The name of the dataverse the operation applies to.
        link_name (str, optional): The name of the link.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DisconnectLinkOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def dataverse_name(self) -> str:
        """The value of the ``dataverse_name`` option, or ``'Default'`` if it was not set."""
        return self.get("dataverse_name", 'Default')

    @property
    def link_name(self) -> str:
        """The value of the ``link_name`` option, or ``'Local'`` if it was not set."""
        return self.get("link_name", 'Local')


class DropAnalyticsIndexOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.DropAnalyticsIndexOptions`."""

    timeout: timedelta | None
    ignore_if_not_exists: bool | None
    dataverse_name: str | None
    parent_span: RequestSpan | None


class DropAnalyticsIndexOptions(dict):
    """Available options for :meth:`~couchbase.management.analytics.AnalyticsIndexManager.drop_index`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``analytics_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        ignore_if_not_exists (bool, optional): Whether or not the call should ignore the index not existing when
            determining whether the call was successful.
        dataverse_name (str, optional): The name of the dataverse the operation applies to.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DropAnalyticsIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def ignore_if_not_exists(self) -> bool:
        """The value of the ``ignore_if_not_exists`` option, or ``False`` if it was not set."""
        return self.get("ignore_if_not_exists", False)

    @property
    def dataverse_name(self) -> str:
        """The value of the ``dataverse_name`` option, or ``'Default'`` if it was not set."""
        return self.get("dataverse_name", 'Default')


class DropDatasetOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.DropDatasetOptions`."""

    timeout: timedelta | None
    ignore_if_not_exists: bool | None
    dataverse_name: str | None
    parent_span: RequestSpan | None


class DropDatasetOptions(dict):
    """Available options for :meth:`~couchbase.management.analytics.AnalyticsIndexManager.drop_dataset`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``analytics_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        ignore_if_not_exists (bool, optional): Whether or not the call should ignore the index not existing when
            determining whether the call was successful.
        dataverse_name (str, optional): The name of the dataverse the operation applies to.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DropDatasetOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def ignore_if_not_exists(self) -> bool:
        """The value of the ``ignore_if_not_exists`` option, or ``False`` if it was not set."""
        return self.get("ignore_if_not_exists", False)

    @property
    def dataverse_name(self) -> str:
        """The value of the ``dataverse_name`` option, or ``'Default'`` if it was not set."""
        return self.get("dataverse_name", 'Default')


class DropDataverseOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.DropDataverseOptions`."""

    timeout: timedelta | None
    ignore_if_not_exists: bool | None
    parent_span: RequestSpan | None


class DropDataverseOptions(dict):
    """Available options for :meth:`~couchbase.management.analytics.AnalyticsIndexManager.drop_dataverse`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``analytics_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        ignore_if_not_exists (bool, optional): Whether or not the call should ignore the index not existing when
            determining whether the call was successful.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DropDataverseOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def ignore_if_not_exists(self) -> bool:
        """The value of the ``ignore_if_not_exists`` option, or ``False`` if it was not set."""
        return self.get("ignore_if_not_exists", False)


class DropLinkAnalyticsOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.DropLinkAnalyticsOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class DropLinkAnalyticsOptions(dict):
    """Available options for :meth:`~couchbase.management.analytics.AnalyticsIndexManager.drop_link`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``analytics_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DropLinkAnalyticsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllAnalyticsIndexesOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetAllAnalyticsIndexesOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetAllAnalyticsIndexesOptions(dict):
    """Available options for :meth:`~couchbase.management.analytics.AnalyticsIndexManager.get_all_indexes`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``analytics_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAllAnalyticsIndexesOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllDatasetOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetAllDatasetOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetAllDatasetOptions(dict):
    """Available options for :meth:`~couchbase.management.analytics.AnalyticsIndexManager.get_all_datasets`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``analytics_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAllDatasetOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetLinksAnalyticsOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetLinksAnalyticsOptions`."""

    timeout: timedelta | None
    dataverse_name: str | None
    name: str | None
    link_type: AnalyticsLinkType | None
    parent_span: RequestSpan | None


class GetLinksAnalyticsOptions(dict):
    """Available options for :meth:`~couchbase.management.analytics.AnalyticsIndexManager.get_links`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``analytics_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        dataverse_name (str, optional): The name of the dataverse the operation applies to.
        name (str, optional): Only get the link with this name, which also needs ``dataverse_name``.
        link_type (:class:`~couchbase.management.analytics.AnalyticsLinkType`, optional): Only get the links of this
            type.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetLinksAnalyticsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def dataverse_name(self) -> str:
        """The value of the ``dataverse_name`` option, or ``'Default'`` if it was not set."""
        return self.get("dataverse_name", 'Default')

    @property
    def name(self) -> str | None:
        """The value of the ``name`` option, or ``None`` if it was not set."""
        return self.get("name", None)

    @property
    def link_type(self) -> AnalyticsLinkType | None:
        """The value of the ``link_type`` option, or ``None`` if it was not set."""
        return self.get("link_type", None)


class GetPendingMutationsOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetPendingMutationsOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetPendingMutationsOptions(dict):
    """Available options for :meth:`~couchbase.management.analytics.AnalyticsIndexManager.get_pending_mutations`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``analytics_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetPendingMutationsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class ReplaceLinkAnalyticsOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.ReplaceLinkAnalyticsOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class ReplaceLinkAnalyticsOptions(dict):
    """Available options for :meth:`~couchbase.management.analytics.AnalyticsIndexManager.replace_link`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``analytics_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[ReplaceLinkAnalyticsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


# ==========================================================================================
# Bucket Management
# ==========================================================================================


class BucketDescribeOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.BucketDescribeOptions`."""

    parent_span: RequestSpan | None
    timeout: timedelta | None


class BucketDescribeOptions(dict):
    """Available options for a :class:`~couchbase.management.buckets.BucketManager`'s bucket describe operation.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
    """

    def __init__(self, **kwargs: Unpack[BucketDescribeOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class CreateBucketOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.CreateBucketOptions`."""

    parent_span: RequestSpan | None
    timeout: timedelta | None


class CreateBucketOptions(dict):
    """Available options for a :class:`~couchbase.management.buckets.BucketManager`'s create bucket operation.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
    """

    def __init__(self, **kwargs: Unpack[CreateBucketOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class DropBucketOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.DropBucketOptions`."""

    parent_span: RequestSpan | None
    timeout: timedelta | None


class DropBucketOptions(dict):
    """Available options for a :class:`~couchbase.management.buckets.BucketManager`'s drop bucket operation.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
    """

    def __init__(self, **kwargs: Unpack[DropBucketOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class FlushBucketOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.FlushBucketOptions`."""

    parent_span: RequestSpan | None
    timeout: timedelta | None


class FlushBucketOptions(dict):
    """Available options for a :class:`~couchbase.management.buckets.BucketManager`'s flush bucket operation.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
    """

    def __init__(self, **kwargs: Unpack[FlushBucketOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllBucketOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetAllBucketOptions`."""

    parent_span: RequestSpan | None
    timeout: timedelta | None


class GetAllBucketOptions(dict):
    """Available options for a :class:`~couchbase.management.buckets.BucketManager`'s get all buckets operation.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
    """

    def __init__(self, **kwargs: Unpack[GetAllBucketOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetBucketOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetBucketOptions`."""

    parent_span: RequestSpan | None
    timeout: timedelta | None


class GetBucketOptions(dict):
    """Available options for a :class:`~couchbase.management.buckets.BucketManager`'s get bucket operation.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
    """

    def __init__(self, **kwargs: Unpack[GetBucketOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UpdateBucketOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.UpdateBucketOptions`."""

    parent_span: RequestSpan | None
    timeout: timedelta | None


class UpdateBucketOptions(dict):
    """Available options for a :class:`~couchbase.management.buckets.BucketManager`'s update bucket operation.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
    """

    def __init__(self, **kwargs: Unpack[UpdateBucketOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


# ==========================================================================================
# Collection Management
# ==========================================================================================


class CreateCollectionOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.CreateCollectionOptions`."""

    parent_span: RequestSpan | None
    timeout: timedelta | None


class CreateCollectionOptions(dict):
    """Available options for :meth:`~couchbase.management.collections.CollectionManager.create_collection`.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
    """

    def __init__(self, **kwargs: Unpack[CreateCollectionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class CreateScopeOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.CreateScopeOptions`."""

    parent_span: RequestSpan | None
    timeout: timedelta | None


class CreateScopeOptions(dict):
    """Available options for a :class:`~couchbase.management.collections.CollectionManager`'s create scope operation.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
    """

    def __init__(self, **kwargs: Unpack[CreateScopeOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class DropCollectionOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.DropCollectionOptions`."""

    parent_span: RequestSpan | None
    timeout: timedelta | None


class DropCollectionOptions(dict):
    """Available options for a :class:`~couchbase.management.collections.CollectionManager`'s drop collection operation.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
    """

    def __init__(self, **kwargs: Unpack[DropCollectionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class DropScopeOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.DropScopeOptions`."""

    parent_span: RequestSpan | None
    timeout: timedelta | None


class DropScopeOptions(dict):
    """Available options for a :class:`~couchbase.management.collections.CollectionManager`'s drop scope operation.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
    """

    def __init__(self, **kwargs: Unpack[DropScopeOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllScopesOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetAllScopesOptions`."""

    parent_span: RequestSpan | None
    timeout: timedelta | None


class GetAllScopesOptions(dict):
    """Available options for a :class:`~couchbase.management.collections.CollectionManager`'s get all scopes operation.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
    """

    def __init__(self, **kwargs: Unpack[GetAllScopesOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UpdateCollectionOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.UpdateCollectionOptions`."""

    parent_span: RequestSpan | None
    timeout: timedelta | None


class UpdateCollectionOptions(dict):
    """Available options for :meth:`~couchbase.management.collections.CollectionManager.update_collection`.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
    """

    def __init__(self, **kwargs: Unpack[UpdateCollectionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


# ==========================================================================================
# Eventing Function Management
# ==========================================================================================


class DeployFunctionOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.DeployFunctionOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class DeployFunctionOptions(dict):
    """Available options for :meth:`~couchbase.management.eventing.EventingFunctionManager.deploy_function`.

    The same options apply to :meth:`~couchbase.management.eventing.ScopeEventingFunctionManager.deploy_function`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DeployFunctionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class DropFunctionOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.DropFunctionOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class DropFunctionOptions(dict):
    """Available options for :meth:`~couchbase.management.eventing.EventingFunctionManager.drop_function`.

    The same options apply to :meth:`~couchbase.management.eventing.ScopeEventingFunctionManager.drop_function`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DropFunctionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class EventingFunctionOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.EventingFunctionOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class EventingFunctionOptions(dict):
    """Available options for a :class:`~couchbase.management.eventing.EventingFunctionManager` operation.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[EventingFunctionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class FunctionsStatusOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.FunctionsStatusOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class FunctionsStatusOptions(dict):
    """Available options for :meth:`~couchbase.management.eventing.EventingFunctionManager.functions_status`.

    The same options apply to :meth:`~couchbase.management.eventing.ScopeEventingFunctionManager.functions_status`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[FunctionsStatusOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllFunctionOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetAllFunctionOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetAllFunctionOptions(dict):
    """Available options for :meth:`~couchbase.management.eventing.EventingFunctionManager.get_all_functions`.

    The same options apply to :meth:`~couchbase.management.eventing.ScopeEventingFunctionManager.get_all_functions`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAllFunctionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetFunctionOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetFunctionOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetFunctionOptions(dict):
    """Available options for :meth:`~couchbase.management.eventing.EventingFunctionManager.get_function`.

    The same options apply to :meth:`~couchbase.management.eventing.ScopeEventingFunctionManager.get_function`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetFunctionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class PauseFunctionOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.PauseFunctionOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class PauseFunctionOptions(dict):
    """Available options for :meth:`~couchbase.management.eventing.EventingFunctionManager.pause_function`.

    The same options apply to :meth:`~couchbase.management.eventing.ScopeEventingFunctionManager.pause_function`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[PauseFunctionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class ResumeFunctionOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.ResumeFunctionOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class ResumeFunctionOptions(dict):
    """Available options for :meth:`~couchbase.management.eventing.EventingFunctionManager.resume_function`.

    The same options apply to :meth:`~couchbase.management.eventing.ScopeEventingFunctionManager.resume_function`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[ResumeFunctionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UndeployFunctionOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.UndeployFunctionOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class UndeployFunctionOptions(dict):
    """Available options for :meth:`~couchbase.management.eventing.EventingFunctionManager.undeploy_function`.

    The same options apply to :meth:`~couchbase.management.eventing.ScopeEventingFunctionManager.undeploy_function`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[UndeployFunctionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UpsertFunctionOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.UpsertFunctionOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class UpsertFunctionOptions(dict):
    """Available options for :meth:`~couchbase.management.eventing.EventingFunctionManager.upsert_function`.

    The same options apply to :meth:`~couchbase.management.eventing.ScopeEventingFunctionManager.upsert_function`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[UpsertFunctionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


# ==========================================================================================
# Query Index Management
# ==========================================================================================


class BuildDeferredQueryIndexOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.BuildDeferredQueryIndexOptions`."""

    timeout: timedelta | None
    scope_name: str | None
    collection_name: str | None
    parent_span: RequestSpan | None


class BuildDeferredQueryIndexOptions(dict):
    """Available options for :meth:`~couchbase.management.queries.QueryIndexManager.build_deferred_indexes`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        scope_name (str, optional): Specifies the scope of the index.

            .. deprecated:: 4.1.4
                Use :class:`~couchbase.management.queries.CollectionQueryIndexManager` instead.
        collection_name (str, optional): Specifies the collection of the index.

            .. deprecated:: 4.1.4
                Use :class:`~couchbase.management.queries.CollectionQueryIndexManager` instead.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[BuildDeferredQueryIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class CreatePrimaryQueryIndexOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.CreatePrimaryQueryIndexOptions`."""

    index_name: str | None
    timeout: timedelta | None
    ignore_if_exists: bool | None
    num_replicas: int | None
    deferred: bool | None
    condition: str | None
    scope_name: str | None
    collection_name: str | None
    parent_span: RequestSpan | None


class CreatePrimaryQueryIndexOptions(dict):
    """Available options for :meth:`~couchbase.management.queries.QueryIndexManager.create_primary_index`.

    Args:
        index_name (str, optional): Specifies the name of the primary index.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        ignore_if_exists (bool, optional): Whether or not the call should ignore the index already existing when
            determining whether the call was successful.
        num_replicas (int, optional): The number of replicas of this index that should be created.
        deferred (bool, optional): Specifies whether this index creation should be deferred until a later point in time
            when they can be explicitly built together.
        condition (str, optional): Specifies the 'where' condition for partial index creation.
        scope_name (str, optional): Specifies the scope of the index.

            .. deprecated:: 4.1.4
                Use :class:`~couchbase.management.queries.CollectionQueryIndexManager` instead.
        collection_name (str, optional): Specifies the collection of the index.

            .. deprecated:: 4.1.4
                Use :class:`~couchbase.management.queries.CollectionQueryIndexManager` instead.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[CreatePrimaryQueryIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def ignore_if_exists(self) -> bool:
        """The value of the ``ignore_if_exists`` option, or ``False`` if it was not set."""
        return self.get("ignore_if_exists", False)

    @property
    def condition(self) -> str | None:
        """The value of the ``condition`` option, or ``None`` if it was not set."""
        return self.get("condition", None)


class CreateQueryIndexOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.CreateQueryIndexOptions`."""

    timeout: timedelta | None
    ignore_if_exists: bool | None
    num_replicas: int | None
    deferred: bool | None
    condition: str | None
    scope_name: str | None
    collection_name: str | None
    parent_span: RequestSpan | None


class CreateQueryIndexOptions(dict):
    """Available options for a :class:`~couchbase.management.queries.QueryIndexManager`'s create index operation.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        ignore_if_exists (bool, optional): Whether or not the call should ignore the index already existing when
            determining whether the call was successful.
        num_replicas (int, optional): The number of replicas of this index that should be created.
        deferred (bool, optional): Specifies whether this index creation should be deferred until a later point in time
            when they can be explicitly built together.
        condition (str, optional): Specifies the 'where' condition for partial index creation.
        scope_name (str, optional): Specifies the scope of the index.

            .. deprecated:: 4.1.4
                Use :class:`~couchbase.management.queries.CollectionQueryIndexManager` instead.
        collection_name (str, optional): Specifies the collection of the index.

            .. deprecated:: 4.1.4
                Use :class:`~couchbase.management.queries.CollectionQueryIndexManager` instead.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[CreateQueryIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def ignore_if_exists(self) -> bool:
        """The value of the ``ignore_if_exists`` option, or ``False`` if it was not set."""
        return self.get("ignore_if_exists", False)

    @property
    def condition(self) -> str | None:
        """The value of the ``condition`` option, or ``None`` if it was not set."""
        return self.get("condition", None)


class DropPrimaryQueryIndexOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.DropPrimaryQueryIndexOptions`."""

    index_name: str | None
    ignore_if_not_exists: bool | None
    timeout: timedelta | None
    scope_name: str | None
    collection_name: str | None
    parent_span: RequestSpan | None


class DropPrimaryQueryIndexOptions(dict):
    """Available options for a :class:`~couchbase.management.queries.QueryIndexManager`'s drop primary index operation.

    Args:
        index_name (str, optional): Specifies the name of the primary index.
        ignore_if_not_exists (bool, optional): Whether or not the call should ignore the index not existing when
            determining whether the call was successful.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        scope_name (str, optional): Specifies the scope of the index.

            .. deprecated:: 4.1.4
                Use :class:`~couchbase.management.queries.CollectionQueryIndexManager` instead.
        collection_name (str, optional): Specifies the collection of the index.

            .. deprecated:: 4.1.4
                Use :class:`~couchbase.management.queries.CollectionQueryIndexManager` instead.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DropPrimaryQueryIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def ignore_if_not_exists(self) -> bool:
        """The value of the ``ignore_if_not_exists`` option, or ``False`` if it was not set."""
        return self.get("ignore_if_not_exists", False)


class DropQueryIndexOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.DropQueryIndexOptions`."""

    ignore_if_not_exists: bool | None
    timeout: timedelta | None
    scope_name: str | None
    collection_name: str | None
    parent_span: RequestSpan | None


class DropQueryIndexOptions(dict):
    """Available options for a :class:`~couchbase.management.queries.QueryIndexManager`'s drop index operation.

    Args:
        ignore_if_not_exists (bool, optional): Whether or not the call should ignore the index not existing when
            determining whether the call was successful.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        scope_name (str, optional): Specifies the scope of the index.

            .. deprecated:: 4.1.4
                Use :class:`~couchbase.management.queries.CollectionQueryIndexManager` instead.
        collection_name (str, optional): Specifies the collection of the index.

            .. deprecated:: 4.1.4
                Use :class:`~couchbase.management.queries.CollectionQueryIndexManager` instead.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DropQueryIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def ignore_if_not_exists(self) -> bool:
        """The value of the ``ignore_if_not_exists`` option, or ``False`` if it was not set."""
        return self.get("ignore_if_not_exists", False)


class GetAllQueryIndexOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetAllQueryIndexOptions`."""

    timeout: timedelta | None
    scope_name: str | None
    collection_name: str | None
    parent_span: RequestSpan | None


class GetAllQueryIndexOptions(dict):
    """Available options for a :class:`~couchbase.management.queries.QueryIndexManager`'s get all indexes operation.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        scope_name (str, optional): Specifies the scope of the index.

            .. deprecated:: 4.1.4
                Use :class:`~couchbase.management.queries.CollectionQueryIndexManager` instead.
        collection_name (str, optional): Specifies the collection of the index.

            .. deprecated:: 4.1.4
                Use :class:`~couchbase.management.queries.CollectionQueryIndexManager` instead.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAllQueryIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class WatchQueryIndexOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.WatchQueryIndexOptions`."""

    watch_primary: bool | None
    timeout: timedelta | None
    scope_name: str | None
    collection_name: str | None
    parent_span: RequestSpan | None


class WatchQueryIndexOptions(dict):
    """Available options for a :class:`~couchbase.management.queries.QueryIndexManager`'s watch indexes operation.

    Args:
        watch_primary (bool, optional): Specifies whether the primary indexes should be watched as well.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        scope_name (str, optional): Specifies the scope of the index.

            .. deprecated:: 4.1.4
                Use :class:`~couchbase.management.queries.CollectionQueryIndexManager` instead.
        collection_name (str, optional): Specifies the collection of the index.

            .. deprecated:: 4.1.4
                Use :class:`~couchbase.management.queries.CollectionQueryIndexManager` instead.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[WatchQueryIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


# ==========================================================================================
# Search Index Management
# ==========================================================================================


class AllowQueryingSearchIndexOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.AllowQueryingSearchIndexOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class AllowQueryingSearchIndexOptions(dict):
    """Available options for :meth:`~couchbase.management.search.SearchIndexManager.allow_querying`.

    The same options apply to :meth:`~couchbase.management.search.ScopeSearchIndexManager.allow_querying`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[AllowQueryingSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class AnalyzeDocumentSearchIndexOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.AnalyzeDocumentSearchIndexOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class AnalyzeDocumentSearchIndexOptions(dict):
    """Available options for :meth:`~couchbase.management.search.SearchIndexManager.analyze_document`.

    The same options apply to :meth:`~couchbase.management.search.ScopeSearchIndexManager.analyze_document`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[AnalyzeDocumentSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class DisallowQueryingSearchIndexOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.DisallowQueryingSearchIndexOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class DisallowQueryingSearchIndexOptions(dict):
    """Available options for :meth:`~couchbase.management.search.SearchIndexManager.disallow_querying`.

    The same options apply to :meth:`~couchbase.management.search.ScopeSearchIndexManager.disallow_querying`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DisallowQueryingSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class DropSearchIndexOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.DropSearchIndexOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class DropSearchIndexOptions(dict):
    """Available options for :meth:`~couchbase.management.search.SearchIndexManager.drop_index`.

    The same options apply to :meth:`~couchbase.management.search.ScopeSearchIndexManager.drop_index`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DropSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class FreezePlanSearchIndexOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.FreezePlanSearchIndexOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class FreezePlanSearchIndexOptions(dict):
    """Available options for :meth:`~couchbase.management.search.SearchIndexManager.freeze_plan`.

    The same options apply to :meth:`~couchbase.management.search.ScopeSearchIndexManager.freeze_plan`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[FreezePlanSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllSearchIndexStatsOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetAllSearchIndexStatsOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetAllSearchIndexStatsOptions(dict):
    """Available options for :meth:`~couchbase.management.search.SearchIndexManager.get_all_index_stats`.

    The same options apply to :meth:`~couchbase.management.search.ScopeSearchIndexManager.get_all_index_stats`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAllSearchIndexStatsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllSearchIndexesOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetAllSearchIndexesOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetAllSearchIndexesOptions(dict):
    """Available options for :meth:`~couchbase.management.search.SearchIndexManager.get_all_indexes`.

    The same options apply to :meth:`~couchbase.management.search.ScopeSearchIndexManager.get_all_indexes`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAllSearchIndexesOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetSearchIndexOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetSearchIndexOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetSearchIndexOptions(dict):
    """Available options for :meth:`~couchbase.management.search.SearchIndexManager.get_index`.

    The same options apply to :meth:`~couchbase.management.search.ScopeSearchIndexManager.get_index`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetSearchIndexStatsOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetSearchIndexStatsOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetSearchIndexStatsOptions(dict):
    """Available options for :meth:`~couchbase.management.search.SearchIndexManager.get_index_stats`.

    The same options apply to :meth:`~couchbase.management.search.ScopeSearchIndexManager.get_index_stats`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetSearchIndexStatsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetSearchIndexedDocumentsCountOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetSearchIndexedDocumentsCountOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetSearchIndexedDocumentsCountOptions(dict):
    """Available options for :meth:`~couchbase.management.search.SearchIndexManager.get_indexed_documents_count`.

    The same options apply to :meth:`~couchbase.management.search.ScopeSearchIndexManager.get_indexed_documents_count`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetSearchIndexedDocumentsCountOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class PauseIngestSearchIndexOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.PauseIngestSearchIndexOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class PauseIngestSearchIndexOptions(dict):
    """Available options for :meth:`~couchbase.management.search.SearchIndexManager.pause_ingest`.

    The same options apply to :meth:`~couchbase.management.search.ScopeSearchIndexManager.pause_ingest`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[PauseIngestSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class ResumeIngestSearchIndexOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.ResumeIngestSearchIndexOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class ResumeIngestSearchIndexOptions(dict):
    """Available options for :meth:`~couchbase.management.search.SearchIndexManager.resume_ingest`.

    The same options apply to :meth:`~couchbase.management.search.ScopeSearchIndexManager.resume_ingest`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[ResumeIngestSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UnfreezePlanSearchIndexOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.UnfreezePlanSearchIndexOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class UnfreezePlanSearchIndexOptions(dict):
    """Available options for :meth:`~couchbase.management.search.SearchIndexManager.unfreeze_plan`.

    The same options apply to :meth:`~couchbase.management.search.ScopeSearchIndexManager.unfreeze_plan`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[UnfreezePlanSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UpsertSearchIndexOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.UpsertSearchIndexOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class UpsertSearchIndexOptions(dict):
    """Available options for :meth:`~couchbase.management.search.SearchIndexManager.upsert_index`.

    The same options apply to :meth:`~couchbase.management.search.ScopeSearchIndexManager.upsert_index`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[UpsertSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


# ==========================================================================================
# User Management
# ==========================================================================================


class ChangePasswordOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.ChangePasswordOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class ChangePasswordOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s change password operation.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[ChangePasswordOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class DropGroupOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.DropGroupOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class DropGroupOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s drop group operation.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DropGroupOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class DropUserOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.DropUserOptions`."""

    domain_name: str | AuthDomain | None
    timeout: timedelta | None
    parent_span: RequestSpan | None


class DropUserOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s drop user operation.

    Args:
        domain_name (str | :class:`~couchbase.auth.AuthDomain`, optional): The user's domain name (either ``local`` or
            ``external``). Defaults to ``local``.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DropUserOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllGroupsOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetAllGroupsOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetAllGroupsOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s get all groups operation.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAllGroupsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllUsersOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetAllUsersOptions`."""

    domain_name: str | AuthDomain | None
    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetAllUsersOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s get all users operation.

    Args:
        domain_name (str | :class:`~couchbase.auth.AuthDomain`, optional): The user's domain name (either ``local`` or
            ``external``). Defaults to ``local``.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAllUsersOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetGroupOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetGroupOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetGroupOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s get group operation.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetGroupOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetRolesOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetRolesOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetRolesOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s get roles operation.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetRolesOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetUserOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetUserOptions`."""

    domain_name: str | AuthDomain | None
    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetUserOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s get user operation.

    Args:
        domain_name (str | :class:`~couchbase.auth.AuthDomain`, optional): The user's domain name (either ``local`` or
            ``external``). Defaults to ``local``.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetUserOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UpsertGroupOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.UpsertGroupOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class UpsertGroupOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s upsert group operation.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[UpsertGroupOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UpsertUserOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.UpsertUserOptions`."""

    domain_name: str | AuthDomain | None
    timeout: timedelta | None
    parent_span: RequestSpan | None


class UpsertUserOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s upsert user operation.

    Args:
        domain_name (str | :class:`~couchbase.auth.AuthDomain`, optional): The user's domain name (either ``local`` or
            ``external``). Defaults to ``local``.
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[UpsertUserOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


# ==========================================================================================
# View Management
# ==========================================================================================


class DropDesignDocumentOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.DropDesignDocumentOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class DropDesignDocumentOptions(dict):
    """Available options for :meth:`~couchbase.management.views.ViewIndexManager.drop_design_document`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``views_timeout``, from
            :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DropDesignDocumentOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllDesignDocumentsOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetAllDesignDocumentsOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetAllDesignDocumentsOptions(dict):
    """Available options for :meth:`~couchbase.management.views.ViewIndexManager.get_all_design_documents`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``management_timeout``,
            from :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAllDesignDocumentsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetDesignDocumentOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.GetDesignDocumentOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetDesignDocumentOptions(dict):
    """Available options for :meth:`~couchbase.management.views.ViewIndexManager.get_design_document`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``views_timeout``, from
            :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetDesignDocumentOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class PublishDesignDocumentOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.PublishDesignDocumentOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class PublishDesignDocumentOptions(dict):
    """Available options for :meth:`~couchbase.management.views.ViewIndexManager.publish_design_document`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``views_timeout``, from
            :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[PublishDesignDocumentOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UpsertDesignDocumentOptionsKwargs(TypedDict, total=False):
    """The keyword arguments of :class:`~couchbase.management.options.UpsertDesignDocumentOptions`."""

    timeout: timedelta | None
    parent_span: RequestSpan | None


class UpsertDesignDocumentOptions(dict):
    """Available options for :meth:`~couchbase.management.views.ViewIndexManager.upsert_design_document`.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to the cluster's ``views_timeout``, from
            :class:`~couchbase.options.ClusterTimeoutOptions`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[UpsertDesignDocumentOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)
