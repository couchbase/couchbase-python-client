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

from enum import Enum
from typing import (List,
                    Optional,
                    Union)

import pytest

from tests.mock_server import MockServerType


class ServerFeatures(Enum):
    KeyValue = 'kv'
    SSL = 'ssl'
    Views = 'views'
    SpatialViews = 'spatial_views'
    Diagnostics = 'diagnostics'
    SynchronousDurability = 'sync_durability'
    Query = 'query'
    Subdoc = 'subdoc'
    Xattr = 'xattr'
    Search = 'search'
    Analytics = 'analytics'
    Collections = 'collections'
    Replicas = 'replicas'
    UserManagement = 'user_mgmt'
    BasicBucketManagement = 'basic_bucket_mgmt'
    BucketManagement = 'bucket_mgmt'
    BucketMinDurability = 'bucket_min_durability'
    BucketStorageBackend = 'bucket_storage_backend'
    CustomConflictResolution = 'custom_conflict_resolution'
    QueryIndexManagement = 'query_index_mgmt'
    QueryUserDefinedFunctions = 'query_user_defined_functions'
    SearchIndexManagement = 'search_index_mgmt'
    ViewIndexManagement = 'view_index_mgmt'
    GetMeta = 'get_meta'
    AnalyticsPendingMutations = 'analytics_pending_mutations'
    AnalyticsLinkManagement = 'analytics_link_mgmt'
    UserGroupManagement = 'user_group_mgmt'
    PreserveExpiry = 'preserve_expiry'
    SearchDisableScoring = 'search_disable_scoring'
    Eventing = 'eventing'
    EventingFunctionManagement = 'eventing_function_mgmt'
    RateLimiting = 'rate_limiting'
    Txns = 'txns'
    TxnQueries = 'txn_queries'


class EnvironmentFeatures:
    # mock and real server (all versions) should have these features
    BASIC_FEATURES = [ServerFeatures.KeyValue,
                      ServerFeatures.Diagnostics,
                      ServerFeatures.SSL,
                      ServerFeatures.SpatialViews,
                      ServerFeatures.Subdoc,
                      ServerFeatures.Views,
                      ServerFeatures.Replicas]

    # mock related feature lists
    FEATURES_NOT_IN_MOCK = [ServerFeatures.Analytics,
                            ServerFeatures.BucketManagement,
                            ServerFeatures.EventingFunctionManagement,
                            ServerFeatures.GetMeta,
                            ServerFeatures.Query,
                            ServerFeatures.QueryIndexManagement,
                            ServerFeatures.QueryUserDefinedFunctions,
                            ServerFeatures.RateLimiting,
                            ServerFeatures.Search,
                            ServerFeatures.SearchIndexManagement,
                            ServerFeatures.TxnQueries,
                            ServerFeatures.UserGroupManagement,
                            ServerFeatures.UserManagement,
                            ServerFeatures.ViewIndexManagement]

    FEATURES_IN_MOCK = [ServerFeatures.Txns]

    # separate features into CBS versions, lets make 5.5 the earliest
    AT_LEAST_V5_5_0_FEATURES = [ServerFeatures.BucketManagement,
                                ServerFeatures.GetMeta,
                                ServerFeatures.Query,
                                ServerFeatures.QueryIndexManagement,
                                ServerFeatures.Search,
                                ServerFeatures.SearchIndexManagement,
                                ServerFeatures.ViewIndexManagement]

    AT_LEAST_V6_0_0_FEATURES = [ServerFeatures.Analytics,
                                ServerFeatures.UserManagement]

    AT_LEAST_V6_5_0_FEATURES = [ServerFeatures.AnalyticsPendingMutations,
                                ServerFeatures.UserGroupManagement,
                                ServerFeatures.SynchronousDurability,
                                ServerFeatures.SearchDisableScoring]

    AT_LEAST_V6_6_0_FEATURES = [ServerFeatures.BucketMinDurability,
                                ServerFeatures.Txns]

    AT_LEAST_V7_0_0_FEATURES = [ServerFeatures.Collections,
                                ServerFeatures.AnalyticsLinkManagement,
                                ServerFeatures.TxnQueries]

    AT_LEAST_V7_1_0_FEATURES = [ServerFeatures.RateLimiting,
                                ServerFeatures.BucketStorageBackend,
                                ServerFeatures.CustomConflictResolution,
                                ServerFeatures.EventingFunctionManagement,
                                ServerFeatures.PreserveExpiry,
                                ServerFeatures.QueryUserDefinedFunctions]

    @staticmethod
    def is_feature_supported(feature,  # type: str
                             server_version,  # type: float
                             mock_server_type=None  # type: Optional[MockServerType]
                             ) -> bool:
        try:
            supported = EnvironmentFeatures.supports_feature(feature, server_version, mock_server_type)
            return supported is None
        except Exception:
            return False

    @staticmethod
    def check_if_feature_supported(features,  # type: Union[str, List[str]]
                                   server_version,  # type: float
                                   mock_server_type=None  # type: Optional[MockServerType]
                                   ) -> None:

        features_list = []
        if isinstance(features, str):
            features_list.append(features)
        else:
            features_list.extend(features)

        for feature in features_list:
            try:
                supported = EnvironmentFeatures.supports_feature(feature, server_version, mock_server_type)
                if supported is not None:
                    pytest.skip(supported)
            except TypeError:
                pytest.skip("Unable to determine server version")
            except Exception:
                raise

    @staticmethod
    def supports_feature(feature,  # type: str  # noqa: C901
                         server_version,  # type: float
                         mock_server_type=None  # type: Optional[MockServerType]
                         ) -> Optional[str]:

        is_mock_server = mock_server_type is not None
        is_real_server = is_mock_server is False

        if feature in map(lambda f: f.value, EnvironmentFeatures.BASIC_FEATURES):
            return None

        if is_mock_server and feature in map(lambda f: f.value, EnvironmentFeatures.FEATURES_NOT_IN_MOCK):
            return f'Mock server does not support feature: {feature}'

        if is_mock_server and feature in map(lambda f: f.value, EnvironmentFeatures.FEATURES_IN_MOCK):
            return None

        if feature in [ServerFeatures.Diagnostics.value, ServerFeatures.BasicBucketManagement.value]:
            if is_real_server or mock_server_type == MockServerType.GoCAVES:
                return None

            return f'LegacyMockServer does not support feature: {feature}'

        if feature in [ServerFeatures.Diagnostics.value, ServerFeatures.BasicBucketManagement.value]:
            if is_real_server or mock_server_type == MockServerType.GoCAVES:
                return None

            return f'LegacyMockServer does not support feature: {feature}'

        if feature in map(lambda f: f.value, EnvironmentFeatures.AT_LEAST_V5_5_0_FEATURES):
            if is_mock_server:
                return f'Mock server does not support feature: {feature}'

            if server_version < 5.5:
                return (f'Feature: {feature} only supported on server versions >= 5.5. '
                        f'Using server version: {server_version}.')

            return None

        if feature in map(lambda f: f.value, EnvironmentFeatures.AT_LEAST_V6_0_0_FEATURES):
            if is_mock_server:
                return f'Mock server does not support feature: {feature}'

            if server_version < 6.0:
                return (f'Feature: {feature} only supported on server versions >= 6.0. '
                        f'Using server version: {server_version}.')
            # @TODO: couchbase++ looks to choke w/ CAVES
            # if feature == ServerFeatures.UserManagement.value:
            #     return self.mock_server_type == MockServerType.GoCAVES
            return None

        if feature in map(lambda f: f.value, EnvironmentFeatures.AT_LEAST_V6_5_0_FEATURES):
            if is_mock_server:
                return f'Mock server does not support feature: {feature}'

            if server_version < 6.5:
                return (f'Feature: {feature} only supported on server versions >= 6.5. '
                        f'Using server version: {server_version}.')

            return None

        if feature in map(lambda f: f.value, EnvironmentFeatures.AT_LEAST_V6_6_0_FEATURES):
            if is_mock_server:
                return f'Mock server does not support feature: {feature}'

            if server_version < 6.6:
                return (f'Feature: {feature} only supported on server versions >= 6.6. '
                        f'Using server version: {server_version}.')

            return None

        if feature in map(lambda f: f.value, EnvironmentFeatures.AT_LEAST_V7_0_0_FEATURES):
            if is_mock_server:
                if mock_server_type == MockServerType.GoCAVES:
                    return None
                return f'Mock server does not support feature: {feature}'

            if server_version < 7.0:
                return (f'Feature: {feature} only supported on server versions >= 7.0. '
                        f'Using server version: {server_version}.')

            return None

        if feature in map(lambda f: f.value, EnvironmentFeatures.AT_LEAST_V7_1_0_FEATURES):
            if is_mock_server:
                return f'Mock server does not support feature: {feature}'

            if server_version < 7.1:
                return (f'Feature: {feature} only supported on server versions >= 7.1. '
                        f'Using server version: {server_version}.')

            return None
