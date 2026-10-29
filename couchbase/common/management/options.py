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
    timeout: timedelta | None
    dataverse_name: str | None
    link_name: str | None
    force: bool | None
    parent_span: RequestSpan | None


class ConnectLinkOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.analytics_timeout`.
        dataverse_name (str, optional):
        link_name (str, optional):
        force (bool, optional):
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[ConnectLinkOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def dataverse_name(self) -> str:
        return self.get("dataverse_name", 'Default')

    @property
    def link_name(self) -> str:
        return self.get("link_name", 'Local')

    @property
    def force(self) -> bool:
        return self.get("force", False)


class CreateAnalyticsIndexOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    ignore_if_exists: bool | None
    dataverse_name: str | None
    parent_span: RequestSpan | None


class CreateAnalyticsIndexOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.analytics_timeout`.
        ignore_if_exists (bool, optional): Whether or not the call should ignore the index already existing when
            determining whether the call was successful.
        dataverse_name (str, optional):
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[CreateAnalyticsIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def ignore_if_exists(self) -> bool:
        return self.get("ignore_if_exists", False)

    @property
    def dataverse_name(self) -> str:
        return self.get("dataverse_name", 'Default')


class CreateDatasetOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    ignore_if_exists: bool | None
    condition: str | None
    dataverse_name: str | None
    parent_span: RequestSpan | None


class CreateDatasetOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.analytics_timeout`.
        ignore_if_exists (bool, optional): Whether or not the call should ignore the index already existing when
            determining whether the call was successful.
        condition (str, optional): Specifies the 'where' condition for partial index creation.
        dataverse_name (str, optional):
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[CreateDatasetOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def ignore_if_exists(self) -> bool:
        return self.get("ignore_if_exists", False)

    @property
    def condition(self) -> str | None:
        return self.get("condition", None)

    @property
    def dataverse_name(self) -> str:
        return self.get("dataverse_name", 'Default')


class CreateDataverseOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    ignore_if_exists: bool | None
    parent_span: RequestSpan | None


class CreateDataverseOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.analytics_timeout`.
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
        return self.get("ignore_if_exists", False)


class CreateLinkAnalyticsOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class CreateLinkAnalyticsOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.analytics_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[CreateLinkAnalyticsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class DisconnectLinkOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    dataverse_name: str | None
    link_name: str | None
    parent_span: RequestSpan | None


class DisconnectLinkOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.analytics_timeout`.
        dataverse_name (str, optional):
        link_name (str, optional):
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DisconnectLinkOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def dataverse_name(self) -> str:
        return self.get("dataverse_name", 'Default')

    @property
    def link_name(self) -> str:
        return self.get("link_name", 'Local')


class DropAnalyticsIndexOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    ignore_if_not_exists: bool | None
    dataverse_name: str | None
    parent_span: RequestSpan | None


class DropAnalyticsIndexOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.analytics_timeout`.
        ignore_if_not_exists (bool, optional): Whether or not the call should ignore the index not existing when
            determining whether the call was successful.
        dataverse_name (str, optional):
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DropAnalyticsIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def ignore_if_not_exists(self) -> bool:
        return self.get("ignore_if_not_exists", False)

    @property
    def dataverse_name(self) -> str:
        return self.get("dataverse_name", 'Default')


class DropDatasetOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    ignore_if_not_exists: bool | None
    dataverse_name: str | None
    parent_span: RequestSpan | None


class DropDatasetOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.analytics_timeout`.
        ignore_if_not_exists (bool, optional): Whether or not the call should ignore the index not existing when
            determining whether the call was successful.
        dataverse_name (str, optional):
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DropDatasetOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def ignore_if_not_exists(self) -> bool:
        return self.get("ignore_if_not_exists", False)

    @property
    def dataverse_name(self) -> str:
        return self.get("dataverse_name", 'Default')


class DropDataverseOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    ignore_if_not_exists: bool | None
    parent_span: RequestSpan | None


class DropDataverseOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.analytics_timeout`.
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
        return self.get("ignore_if_not_exists", False)


class DropLinkAnalyticsOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class DropLinkAnalyticsOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.analytics_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DropLinkAnalyticsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllAnalyticsIndexesOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetAllAnalyticsIndexesOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.analytics_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAllAnalyticsIndexesOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllDatasetOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetAllDatasetOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.analytics_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAllDatasetOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetLinksAnalyticsOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    dataverse_name: str | None
    name: str | None
    link_type: AnalyticsLinkType | None
    parent_span: RequestSpan | None


class GetLinksAnalyticsOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.analytics_timeout`.
        dataverse_name (str, optional):
        name (str, optional):
        link_type (:class:`~couchbase.management.analytics.AnalyticsLinkType`, optional):
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetLinksAnalyticsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def dataverse_name(self) -> str:
        return self.get("dataverse_name", 'Default')

    @property
    def name(self) -> str | None:
        return self.get("name", None)

    @property
    def link_type(self) -> AnalyticsLinkType | None:
        return self.get("link_type", None)


class GetPendingMutationsOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetPendingMutationsOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.analytics_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetPendingMutationsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class ReplaceLinkAnalyticsOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class ReplaceLinkAnalyticsOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.analytics_timeout`.
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
    parent_span: RequestSpan | None
    timeout: timedelta | None


class BucketDescribeOptions(dict):
    """Available options for a :class:`~couchbase.management.buckets.BucketManager`'s bucket describe operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
    """

    def __init__(self, **kwargs: Unpack[BucketDescribeOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class CreateBucketOptionsKwargs(TypedDict, total=False):
    parent_span: RequestSpan | None
    timeout: timedelta | None


class CreateBucketOptions(dict):
    """Available options for a :class:`~couchbase.management.buckets.BucketManager`'s create bucket operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
    """

    def __init__(self, **kwargs: Unpack[CreateBucketOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class DropBucketOptionsKwargs(TypedDict, total=False):
    parent_span: RequestSpan | None
    timeout: timedelta | None


class DropBucketOptions(dict):
    """Available options for a :class:`~couchbase.management.buckets.BucketManager`'s drop bucket operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
    """

    def __init__(self, **kwargs: Unpack[DropBucketOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class FlushBucketOptionsKwargs(TypedDict, total=False):
    parent_span: RequestSpan | None
    timeout: timedelta | None


class FlushBucketOptions(dict):
    """Available options for a :class:`~couchbase.management.buckets.BucketManager`'s flush bucket operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
    """

    def __init__(self, **kwargs: Unpack[FlushBucketOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllBucketOptionsKwargs(TypedDict, total=False):
    parent_span: RequestSpan | None
    timeout: timedelta | None


class GetAllBucketOptions(dict):
    """Available options for a :class:`~couchbase.management.buckets.BucketManager`'s get all buckets operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
    """

    def __init__(self, **kwargs: Unpack[GetAllBucketOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetBucketOptionsKwargs(TypedDict, total=False):
    parent_span: RequestSpan | None
    timeout: timedelta | None


class GetBucketOptions(dict):
    """Available options for a :class:`~couchbase.management.buckets.BucketManager`'s get bucket operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
    """

    def __init__(self, **kwargs: Unpack[GetBucketOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UpdateBucketOptionsKwargs(TypedDict, total=False):
    parent_span: RequestSpan | None
    timeout: timedelta | None


class UpdateBucketOptions(dict):
    """Available options for a :class:`~couchbase.management.buckets.BucketManager`'s update bucket operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
    """

    def __init__(self, **kwargs: Unpack[UpdateBucketOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


# ==========================================================================================
# Collection Management
# ==========================================================================================


class CreateCollectionOptionsKwargs(TypedDict, total=False):
    parent_span: RequestSpan | None
    timeout: timedelta | None


class CreateCollectionOptions(dict):
    """Available options for a :class:`~couchbase.management.collections.CollectionManager`'s create collection operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
    """  # noqa: E501

    def __init__(self, **kwargs: Unpack[CreateCollectionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class CreateScopeOptionsKwargs(TypedDict, total=False):
    parent_span: RequestSpan | None
    timeout: timedelta | None


class CreateScopeOptions(dict):
    """Available options for a :class:`~couchbase.management.collections.CollectionManager`'s create scope operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
    """

    def __init__(self, **kwargs: Unpack[CreateScopeOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class DropCollectionOptionsKwargs(TypedDict, total=False):
    parent_span: RequestSpan | None
    timeout: timedelta | None


class DropCollectionOptions(dict):
    """Available options for a :class:`~couchbase.management.collections.CollectionManager`'s drop collection operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
    """

    def __init__(self, **kwargs: Unpack[DropCollectionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class DropScopeOptionsKwargs(TypedDict, total=False):
    parent_span: RequestSpan | None
    timeout: timedelta | None


class DropScopeOptions(dict):
    """Available options for a :class:`~couchbase.management.collections.CollectionManager`'s drop scope operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
    """

    def __init__(self, **kwargs: Unpack[DropScopeOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllScopesOptionsKwargs(TypedDict, total=False):
    parent_span: RequestSpan | None
    timeout: timedelta | None


class GetAllScopesOptions(dict):
    """Available options for a :class:`~couchbase.management.collections.CollectionManager`'s get all scopes operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
    """

    def __init__(self, **kwargs: Unpack[GetAllScopesOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UpdateCollectionOptionsKwargs(TypedDict, total=False):
    parent_span: RequestSpan | None
    timeout: timedelta | None


class UpdateCollectionOptions(dict):
    """Available options for a :class:`~couchbase.management.collections.CollectionManager`'s update collection operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
    """  # noqa: E501

    def __init__(self, **kwargs: Unpack[UpdateCollectionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


# ==========================================================================================
# Eventing Function Management
# ==========================================================================================


class DeployFunctionOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class DeployFunctionOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DeployFunctionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class DropFunctionOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class DropFunctionOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DropFunctionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class EventingFunctionOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class EventingFunctionOptions(dict):
    """Available options for a :class:`~couchbase.management.eventing.EventingFunctionManager` operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[EventingFunctionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class FunctionsStatusOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class FunctionsStatusOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[FunctionsStatusOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllFunctionOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetAllFunctionOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAllFunctionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetFunctionOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetFunctionOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetFunctionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class PauseFunctionOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class PauseFunctionOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[PauseFunctionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class ResumeFunctionOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class ResumeFunctionOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[ResumeFunctionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UndeployFunctionOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class UndeployFunctionOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[UndeployFunctionOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UpsertFunctionOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class UpsertFunctionOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
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
    timeout: timedelta | None
    scope_name: str | None
    collection_name: str | None
    parent_span: RequestSpan | None


class BuildDeferredQueryIndexOptions(dict):
    """Available options for a :class:`~couchbase.management.queries.QueryIndexManager`'s build deferred indexes operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        scope_name (str, optional): Specifies the scope of the index.

            .. deprecated:: 4.1.4
                Use :class:`~couchbase.management.queries.CollectionQueryIndexManager` instead.
        collection_name (str, optional): Specifies the collection of the index.

            .. deprecated:: 4.1.4
                Use :class:`~couchbase.management.queries.CollectionQueryIndexManager` instead.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """  # noqa: E501

    def __init__(self, **kwargs: Unpack[BuildDeferredQueryIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class CreatePrimaryQueryIndexOptionsKwargs(TypedDict, total=False):
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
    """Available options for a :class:`~couchbase.management.queries.QueryIndexManager`'s create primary index operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        index_name (str, optional): Specifies the name of the primary index.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
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
    """  # noqa: E501

    def __init__(self, **kwargs: Unpack[CreatePrimaryQueryIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def ignore_if_exists(self) -> bool:
        return self.get("ignore_if_exists", False)

    @property
    def condition(self) -> str | None:
        return self.get("condition", None)


class CreateQueryIndexOptionsKwargs(TypedDict, total=False):
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

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
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
        return self.get("ignore_if_exists", False)

    @property
    def condition(self) -> str | None:
        return self.get("condition", None)


class DropPrimaryQueryIndexOptionsKwargs(TypedDict, total=False):
    index_name: str | None
    ignore_if_not_exists: bool | None
    timeout: timedelta | None
    scope_name: str | None
    collection_name: str | None
    parent_span: RequestSpan | None


class DropPrimaryQueryIndexOptions(dict):
    """Available options for a :class:`~couchbase.management.queries.QueryIndexManager`'s drop primary index operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        index_name (str, optional): Specifies the name of the primary index.
        ignore_if_not_exists (bool, optional): Whether or not the call should ignore the index not existing when
            determining whether the call was successful.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
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
        return self.get("ignore_if_not_exists", False)


class DropQueryIndexOptionsKwargs(TypedDict, total=False):
    ignore_if_not_exists: bool | None
    timeout: timedelta | None
    scope_name: str | None
    collection_name: str | None
    parent_span: RequestSpan | None


class DropQueryIndexOptions(dict):
    """Available options for a :class:`~couchbase.management.queries.QueryIndexManager`'s drop index operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        ignore_if_not_exists (bool, optional): Whether or not the call should ignore the index not existing when
            determining whether the call was successful.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
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
        return self.get("ignore_if_not_exists", False)


class GetAllQueryIndexOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    scope_name: str | None
    collection_name: str | None
    parent_span: RequestSpan | None


class GetAllQueryIndexOptions(dict):
    """Available options for a :class:`~couchbase.management.queries.QueryIndexManager`'s get all indexes operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
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
    watch_primary: bool | None
    timeout: timedelta | None
    scope_name: str | None
    collection_name: str | None
    parent_span: RequestSpan | None


class WatchQueryIndexOptions(dict):
    """Available options for a :class:`~couchbase.management.queries.QueryIndexManager`'s watch indexes operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        watch_primary (bool, optional): Specifies whether the primary indexes should be watched as well.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
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
    timeout: timedelta | None
    parent_span: RequestSpan | None


class AllowQueryingSearchIndexOptions(dict):
    """Available options for a :meth:`.ScopeSearchIndexManager.allow_querying` or a
    :meth:`.SearchIndexManager.allow_querying` operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[AllowQueryingSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class AnalyzeDocumentSearchIndexOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class AnalyzeDocumentSearchIndexOptions(dict):
    """Available options for a :meth:`.ScopeSearchIndexManager.analyze_document` or a
    :meth:`.SearchIndexManager.analyze_document` operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[AnalyzeDocumentSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class DisallowQueryingSearchIndexOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class DisallowQueryingSearchIndexOptions(dict):
    """Available options for a :meth:`.ScopeSearchIndexManager.disallow_querying` or a
    :meth:`.SearchIndexManager.disallow_querying` operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DisallowQueryingSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class DropSearchIndexOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class DropSearchIndexOptions(dict):
    """Available options for a :meth:`.ScopeSearchIndexManager.drop_index` or a :meth:`.SearchIndexManager.drop_index`
    operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DropSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class FreezePlanSearchIndexOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class FreezePlanSearchIndexOptions(dict):
    """Available options for a :meth:`.ScopeSearchIndexManager.freeze_plan` or a :meth:`.SearchIndexManager.freeze_plan`
    operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[FreezePlanSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllSearchIndexStatsOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetAllSearchIndexStatsOptions(dict):
    """Available options for a :meth:`.ScopeSearchIndexManager.get_all_index_stats` or a
    :meth:`.SearchIndexManager.get_all_index_stats` operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAllSearchIndexStatsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllSearchIndexesOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetAllSearchIndexesOptions(dict):
    """Available options for a :meth:`.ScopeSearchIndexManager.get_all_indexes` or a
    :meth:`.SearchIndexManager.get_all_indexes` operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAllSearchIndexesOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetSearchIndexOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetSearchIndexOptions(dict):
    """Available options for a :meth:`.ScopeSearchIndexManager.get_index` or a :meth:`.SearchIndexManager.get_index`
    operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetSearchIndexStatsOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetSearchIndexStatsOptions(dict):
    """Available options for a :meth:`.ScopeSearchIndexManager.get_index_stats` or a
    :meth:`.SearchIndexManager.get_index_stats` operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetSearchIndexStatsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetSearchIndexedDocumentsCountOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetSearchIndexedDocumentsCountOptions(dict):
    """Available options for a :meth:`.ScopeSearchIndexManager.get_indexed_documents_count` or a
    :meth:`.SearchIndexManager.get_indexed_documents_count` operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetSearchIndexedDocumentsCountOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class PauseIngestSearchIndexOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class PauseIngestSearchIndexOptions(dict):
    """Available options for a :meth:`.ScopeSearchIndexManager.pause_ingest` or a :meth:`.SearchIndexManager.pause_ingest`
    operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """  # noqa: E501

    def __init__(self, **kwargs: Unpack[PauseIngestSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class ResumeIngestSearchIndexOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class ResumeIngestSearchIndexOptions(dict):
    """Available options for a :meth:`.ScopeSearchIndexManager.resume_ingest` or a
    :meth:`.SearchIndexManager.resume_ingest` operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[ResumeIngestSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UnfreezePlanSearchIndexOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class UnfreezePlanSearchIndexOptions(dict):
    """Available options for a :meth:`.ScopeSearchIndexManager.unfreeze_plan` or a
    :meth:`.SearchIndexManager.unfreeze_plan` operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[UnfreezePlanSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UpsertSearchIndexOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class UpsertSearchIndexOptions(dict):
    """Available options for a :meth:`.ScopeSearchIndexManager.upsert_index` or a :meth:`.SearchIndexManager.upsert_index`
    operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """  # noqa: E501

    def __init__(self, **kwargs: Unpack[UpsertSearchIndexOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


# ==========================================================================================
# User Management
# ==========================================================================================


class ChangePasswordOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class ChangePasswordOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s change password operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[ChangePasswordOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class DropGroupOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class DropGroupOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s drop group operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DropGroupOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class DropUserOptionsKwargs(TypedDict, total=False):
    domain_name: str | AuthDomain | None
    timeout: timedelta | None
    parent_span: RequestSpan | None


class DropUserOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s drop user operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        domain_name (str | :class:`~couchbase.auth.AuthDomain`, optional): The user's domain name (either ``local`` or
            ``external``). Defaults to ``local``.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DropUserOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllGroupsOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetAllGroupsOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s get all groups operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAllGroupsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllUsersOptionsKwargs(TypedDict, total=False):
    domain_name: str | AuthDomain | None
    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetAllUsersOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s get all users operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        domain_name (str | :class:`~couchbase.auth.AuthDomain`, optional): The user's domain name (either ``local`` or
            ``external``). Defaults to ``local``.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAllUsersOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetGroupOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetGroupOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s get group operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetGroupOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetRolesOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetRolesOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s get roles operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetRolesOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetUserOptionsKwargs(TypedDict, total=False):
    domain_name: str | AuthDomain | None
    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetUserOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s get user operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        domain_name (str | :class:`~couchbase.auth.AuthDomain`, optional): The user's domain name (either ``local`` or
            ``external``). Defaults to ``local``.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetUserOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UpsertGroupOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class UpsertGroupOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s upsert group operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[UpsertGroupOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UpsertUserOptionsKwargs(TypedDict, total=False):
    domain_name: str | AuthDomain | None
    timeout: timedelta | None
    parent_span: RequestSpan | None


class UpsertUserOptions(dict):
    """Available options for a :class:`~couchbase.management.users.UserManager`'s upsert user operation.

    .. note::
        All options should be imported from ``couchbase.management.options``.

    Args:
        domain_name (str | :class:`~couchbase.auth.AuthDomain`, optional): The user's domain name (either ``local`` or
            ``external``). Defaults to ``local``.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
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
    timeout: timedelta | None
    parent_span: RequestSpan | None


class DropDesignDocumentOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.views_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DropDesignDocumentOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllDesignDocumentsOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetAllDesignDocumentsOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.management_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAllDesignDocumentsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetDesignDocumentOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class GetDesignDocumentOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.views_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetDesignDocumentOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class PublishDesignDocumentOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class PublishDesignDocumentOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.views_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[PublishDesignDocumentOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UpsertDesignDocumentOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class UpsertDesignDocumentOptions(dict):
    """Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.views_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[UpsertDesignDocumentOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)
