
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

import json
import os
import pathlib
import time
from collections import namedtuple
from configparser import ConfigParser
from dataclasses import dataclass, fields
from datetime import datetime, timedelta
from enum import Enum, IntEnum
from typing import (List,
                    Optional,
                    Union,
                    get_type_hints)

import pytest

import couchbase.search as search
from couchbase.auth import AuthDomain
from couchbase.management.eventing import (EventingFunction,
                                           EventingFunctionBucketAccess,
                                           EventingFunctionBucketBinding,
                                           EventingFunctionConstantBinding,
                                           EventingFunctionDcpBoundary,
                                           EventingFunctionDeploymentStatus,
                                           EventingFunctionKeyspace,
                                           EventingFunctionLanguageCompatibility,
                                           EventingFunctionLogLevel,
                                           EventingFunctionProcessingStatus,
                                           EventingFunctionSettings,
                                           EventingFunctionUrlAuthBasic,
                                           EventingFunctionUrlAuthBearer,
                                           EventingFunctionUrlAuthDigest,
                                           EventingFunctionUrlBinding,
                                           EventingFunctionUrlNoAuth)
from couchbase.management.logic.eventing_logic import QueryScanConsistency
from couchbase.management.users import (Role,
                                        RoleAndOrigins,
                                        User)
from couchbase.result import SearchResult
from couchbase.search import SearchRow

from .mock_server import (LegacyMockBucketSpec,
                          MockServer,
                          MockServerType)

BASEDIR = pathlib.Path(__file__).parent.parent

CONFIG_FILE = os.path.join(pathlib.Path(__file__).parent, "test_config.ini")

KVPair = namedtuple("KVPair", "key value")


class CouchbaseTestEnvironmentException(Exception):
    """Raised when something with the test environment is incorrect."""


class EventingFunctionManagementTestStatusException(Exception):
    """Raised when waiting for a certained status does not happen within a specified timeframe"""


class DataSize(IntEnum):
    """Determine amount of documents to load
    for testing.

    Increases in 20% increments
    """
    EXTRA_SMALL = 1
    SMALL = 2
    MEDIUM = 3
    LARGE = 4
    EXTRA_LARGE = 5


class CollectionType(IntEnum):
    DEFAULT = 1
    NAMED = 2


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
                            ServerFeatures.PreserveExpiry]

# Only set the baseline needed
TEST_SUITE_MAP = {
    'analytics_t': [ServerFeatures.Analytics],
    'analyticsmgmt_t': [ServerFeatures.Analytics],
    'binary_collection_multi_t': [ServerFeatures.KeyValue],
    'binary_collection_t': [ServerFeatures.KeyValue],
    'binary_durability_t': [ServerFeatures.KeyValue],
    'bucket_t': [ServerFeatures.Diagnostics],
    'bucketmgmt_t': [ServerFeatures.BucketManagement],
    'cluster_t': [ServerFeatures.Diagnostics],
    'collection_multi_t': [ServerFeatures.KeyValue],
    'collection_t': [ServerFeatures.KeyValue],
    'collectionmgmt_t': [ServerFeatures.Collections],
    'connection_t': [ServerFeatures.Diagnostics],
    'datastructures_t': [ServerFeatures.Subdoc],
    'durability_t': [ServerFeatures.KeyValue],
    'eventingmgmt_t': [ServerFeatures.EventingFunctionManagement],
    'metrics_t': [ServerFeatures.Collections],
    'mutation_tokens_t': [ServerFeatures.KeyValue],
    'query_t': [ServerFeatures.Query, ServerFeatures.QueryIndexManagement],
    'querymgmt_t': [ServerFeatures.QueryIndexManagement],
    'rate_limit_t': [ServerFeatures.RateLimiting,
                     ServerFeatures.BucketManagement,
                     ServerFeatures.UserManagement,
                     ServerFeatures.Collections],
    'search_t': [ServerFeatures.Search, ServerFeatures.SearchIndexManagement],
    'searchmgmt_t': [ServerFeatures.SearchIndexManagement],
    'subdoc_t': [ServerFeatures.Subdoc],
    'tracing_t': [ServerFeatures.Collections],
    'transactions_t': [ServerFeatures.Txns],
    'transcoder_t': [ServerFeatures.KeyValue],
    'usermgmt_t': [ServerFeatures.UserManagement],
    'viewmgmt_t': [ServerFeatures.ViewIndexManagement],
    'views_t': [ServerFeatures.Views, ServerFeatures.ViewIndexManagement]
}


class FakeTestObj:
    PROP = "fake prop"
    PROP1 = 12345


class CouchbaseTestEnvironment():
    KEY = "airport_3830"
    CONTENT = {
        "airportname": "Chicago Ohare Intl",
        "city": "Chicago",
        "country": "United States",
        "faa": "ORD",
        "geo":
        {
            "alt": 668,
            "lat": 41.978603,
            "lon": -87.904842
        },
        "icao": "KORD",
        "id": 3830,
        "type": "airport",
        "tz": "America/Chicago"
    }
    DURABLE_KEY = "airport_3876"
    DURABLE_CONTENT = {
        "airportname": "Charlotte Douglas Intl",
        "city": "Charlotte",
        "country": "United States",
        "faa": "CLT",
        "geo":
        {
            "alt": 748,
            "lat": 35.214,
            "lon": -80.943139
        },
        "icao": "KCLT",
        "id": 3876,
        "type": "airport",
        "tz": "America/New_York"
    }
    NEW_KEY = "airport_3469"
    NEW_CONTENT = {
        "airportname": "San Francisco Intl",
        "city": "San Francisco",
        "country": "United States",
        "faa": "SFO",
        "geo": {
            "alt": 13,
            "lat": 37.618972,
            "lon": -122.374889
        },
        "icao": "KSFO",
        "id": 3469,
        "type": "airport",
        "tz": "America/Los_Angeles"
    }
    UTF8_KEY = "bc_tests_utf8"
    BYTES_KEY = "bc_tests_bytes"
    COUNTER_KEY = "bc_tests_counter"

    def __init__(self, cluster, bucket, collection, cluster_config):
        self._cluster = cluster
        self._bucket = bucket
        self._collection = collection
        self._loaded_keys = None
        self._cluster_config = cluster_config

    @property
    def cluster(self):
        return self._cluster

    @property
    def bucket(self):
        return self._bucket

    @property
    def collection(self):
        return self._collection

    @property
    def loaded_keys(self):
        return self._loaded_keys

    @property
    def is_mock_server(self) -> MockServer:
        return self._cluster_config.is_mock_server

    @property
    def mock_server_type(self) -> MockServerType:
        return self._cluster_config.mock_server.mock_type

    @property
    def is_real_server(self):
        return not self._cluster_config.is_mock_server

    @property
    def server_version(self) -> Optional[str]:
        return self._cluster.server_version

    @property
    def server_version_short(self) -> Optional[float]:
        return self._cluster.server_version_short

    @property
    def server_version_full(self) -> Optional[str]:
        return self._cluster.server_version_full

    @property
    def is_developer_preview(self) -> Optional[bool]:
        return self._cluster.is_developer_preview

    def get_default_key_value(self):
        return self.KEY, self.CONTENT

    def default_durable_key_value(self):
        return self.DURABLE_KEY, self.DURABLE_CONTENT

    def get_binary_keys(self):
        return self.UTF8_KEY, self.BYTES_KEY, self.COUNTER_KEY

    def get_binary_key(self, type_):
        if type_ == "UTF8":
            return self.UTF8_KEY
        if type_ == "BYTES":
            return self.BYTES_KEY
        if type_ == "COUNTER":
            return self.COUNTER_KEY

    def load_data_from_file(self):
        # TODO:  config # of documents loaded and set default key/doc
        data_types = ["airports", "airlines", "routes", "hotels", "landmarks"]
        if not self._loaded_keys:
            self._loaded_keys = []
        sample_json = []
        with open(os.path.join(pathlib.Path(__file__).parent, "travel_sample_data.json")) as data_file:
            sample_data = data_file.read()
            sample_json = json.loads(sample_data)

        return data_types, sample_json

    def is_feature_supported(self, feature  # type: str
                             ) -> bool:
        try:
            supported = self.supports_feature(feature)
            return supported
        except Exception:
            return False

    def check_if_feature_supported(self, features  # type: Union[str,List[str]]
                                   ) -> None:

        features_list = []
        if isinstance(features, str):
            features_list.append(features)
        else:
            features_list.extend(features)

        for feature in features_list:
            try:
                supported = self.supports_feature(feature)
                if not supported:
                    pytest.skip(self.feature_not_supported_text(feature))
            except TypeError:
                pytest.skip("Unable to determine server version")
            except Exception:
                raise

    def supports_feature(self, feature  # type: str  # noqa: C901
                         ) -> bool:

        if feature in map(lambda f: f.value, BASIC_FEATURES):
            return True

        if self.is_mock_server and feature in map(lambda f: f.value, FEATURES_NOT_IN_MOCK):
            return False

        if self.is_mock_server and feature in map(lambda f: f.value, FEATURES_IN_MOCK):
            return True

        if feature == ServerFeatures.Diagnostics.value:
            if self.is_real_server:
                return True

            return self.mock_server_type == MockServerType.GoCAVES

        if feature == ServerFeatures.Xattr.value:
            if self.is_real_server:
                return True

            return self.mock_server_type == MockServerType.GoCAVES

        if feature == ServerFeatures.BasicBucketManagement.value:
            if self.is_real_server:
                return True

            return self.mock_server_type == MockServerType.GoCAVES

        if feature in map(lambda f: f.value, AT_LEAST_V5_5_0_FEATURES):
            if self.is_real_server:
                return self.server_version_short >= 5.5
            return not self.is_mock_server

        if feature in map(lambda f: f.value, AT_LEAST_V6_0_0_FEATURES):
            if self.is_real_server:
                return self.server_version_short >= 6.0
            # @TODO: couchbase++ looks to choke w/ CAVES
            # if feature == ServerFeatures.UserManagement.value:
            #     return self.mock_server_type == MockServerType.GoCAVES
            return not self.is_mock_server

        if feature in map(lambda f: f.value, AT_LEAST_V6_5_0_FEATURES):
            if self.is_real_server:
                return self.server_version_short >= 6.5
            return not self.is_mock_server

        if feature in map(lambda f: f.value, AT_LEAST_V6_6_0_FEATURES):
            if self.is_real_server:
                return self.server_version_short >= 6.6
            return not self.is_mock_server

        if feature in map(lambda f: f.value, AT_LEAST_V7_0_0_FEATURES):
            if self.is_real_server:
                return self.server_version_short >= 7.0
            if feature == ServerFeatures.Collections.value:
                return self.mock_server_type == MockServerType.GoCAVES
            return not self.is_mock_server

        if feature in map(lambda f: f.value, AT_LEAST_V7_1_0_FEATURES):
            if self.is_real_server:
                return self.server_version_short >= 7.1
            return not self.is_mock_server

        raise CouchbaseTestEnvironmentException(f"Unable to determine if server has provided feature: {feature}")

    def feature_not_supported_text(self, feature  # type: str  # noqa: C901
                                   ) -> str:

        if self.is_mock_server and feature in map(lambda f: f.value, FEATURES_NOT_IN_MOCK):
            return f'Mock server does not support feature: {feature}'

        if feature == ServerFeatures.Diagnostics.value:
            if self.mock_server_type == MockServerType.Legacy:
                return f'LegacyMockServer does not support feature: {feature}'

        if feature == ServerFeatures.Xattr.value:
            if self.mock_server_type == MockServerType.Legacy:
                return f'LegacyMockServer does not support feature: {feature}'

        if feature == ServerFeatures.BucketManagement.value:
            if self.mock_server_type == MockServerType.Legacy:
                return f'LegacyMockServer does not support feature: {feature}'

        if feature in map(lambda f: f.value, AT_LEAST_V5_5_0_FEATURES):
            if self.is_real_server:
                return (f'Feature: {feature} only supported on server versions >= 5.5. '
                        f'Using server version: {self.server_version}.')
            return f'Mock server does not support feature: {feature}'

        if feature in map(lambda f: f.value, AT_LEAST_V6_0_0_FEATURES):
            if self.is_real_server:
                return (f'Feature: {feature} only supported on server versions >= 6.0. '
                        f'Using server version: {self.server_version}.')
            return f'Mock server does not support feature: {feature}'

        if feature in map(lambda f: f.value, AT_LEAST_V6_5_0_FEATURES):
            if self.is_real_server:
                return (f'Feature: {feature} only supported on server versions >= 6.5. '
                        f'Using server version: {self.server_version}.')
            return f'Mock server does not support feature: {feature}'

        if feature in map(lambda f: f.value, AT_LEAST_V6_6_0_FEATURES):
            if self.is_real_server:
                return (f'Feature: {feature} only supported on server versions >= 6.6. '
                        f'Using server version: {self.server_version}.')
            return f'Mock server does not support feature: {feature}'

        if feature in map(lambda f: f.value, AT_LEAST_V7_0_0_FEATURES):
            if self.is_real_server:
                return (f'Feature: {feature} only supported on server versions >= 7.0. '
                        f'Using server version: {self.server_version}.')
            return f'Mock server does not support feature: {feature}'

        if feature in map(lambda f: f.value, AT_LEAST_V7_1_0_FEATURES):
            if self.is_real_server:
                return (f'Feature: {feature} only supported on server versions >= 7.1. '
                        f'Using server version: {self.server_version}.')
            return f'Mock server does not support feature: {feature}'

    @staticmethod
    def mock_supports_feature(test_suite,  # type: str
                              is_mock  # type: bool
                              ) -> bool:
        if not is_mock:
            return True

        test_suite_features = TEST_SUITE_MAP.get(test_suite, None)
        if not test_suite_features:
            raise CouchbaseTestEnvironmentException(f"Unable to determine features for test suite: {test_suite}")

        for feature in test_suite_features:
            if feature.value in map(lambda f: f.value, FEATURES_NOT_IN_MOCK):
                return False

        return True

    def skip_if_mock_unstable(self, stable):
        if not stable and self.is_mock_server:
            pytest.skip(('CAVES does not seem to be happy. Skipping tests as failure is not'
                        ' an accurate representation of the state of the test, but rather'
                         ' there is an environment issue.'))

    # common Search validation

    def validate_search_row(self, row):
        assert isinstance(row, SearchRow)  # nosec
        assert isinstance(row.index, str)  # nosec
        assert isinstance(row.id, str)  # nosec
        assert isinstance(row.score, float)  # nosec
        assert isinstance(row.explanation, dict)  # nosec
        assert isinstance(row.fragments, dict)  # nosec

        if row.locations:
            assert isinstance(row.locations, search.SearchRowLocations)  # nosec

        if row.fields:
            assert isinstance(row.fields, search.SearchRowFields)  # nosec

    def validate_search_metadata(self,
                                 result,  # type: SearchResult
                                 expected_count  # type: int
                                 ) -> None:
        meta = result.metadata()
        assert isinstance(meta, search.SearchMetaData)  # nosec
        metrics = meta.metrics()
        assert isinstance(metrics, search.SearchMetrics)  # nosec
        assert isinstance(metrics.error_partition_count(), int)  # nosec
        assert isinstance(metrics.max_score(), float)  # nosec
        assert isinstance(metrics.success_partition_count(), int)  # nosec
        assert isinstance(metrics.total_partition_count(), int)  # nosec
        total_count = metrics.error_partition_count() + metrics.success_partition_count()
        assert total_count == metrics.total_partition_count()  # nosec
        assert isinstance(metrics.took(), timedelta)  # nosec
        assert metrics.took().total_seconds() >= 0  # nosec
        assert metrics.total_rows() >= expected_count  # nosec

    def assert_search_rows(self,
                           result,  # type: SearchResult
                           expected_count,  # type: int
                           return_rows=False  # type: bool
                           ) -> Optional[List[Union[SearchRow, dict]]]:
        rows = []
        assert isinstance(result, SearchResult)  # nosec
        for row in result.rows():
            assert row is not None  # nosec
            self.validate_search_row(row)
            rows.append(row)
        assert len(rows) >= expected_count  # nosec

        self.validate_search_metadata(result, expected_count)

        if return_rows is True:
            return rows

    async def assert_search_rows_async(self,
                                       result,  # type: SearchResult
                                       expected_count,  # type: int
                                       return_rows=False  # type: bool
                                       ) -> Optional[List[Union[SearchRow, dict]]]:
        rows = []
        assert isinstance(result, SearchResult)  # nosec
        async for row in result.rows():
            assert row is not None  # nosec
            self.validate_search_row(row)
            rows.append(row)
        assert len(rows) >= expected_count  # nosec

        self.validate_search_metadata(result, expected_count)

        if return_rows is True:
            return rows

    # common User mgmt validation

    def validate_user(self, user, user_roles=None):
        # password is write-only
        property_list = ['username', 'groups', 'roles']
        properties = list(n for n in dir(user) if n in property_list)
        for p in properties:
            value = getattr(user, p)
            if p == 'username':
                assert value is not None  # nosec
            elif p == 'groups' and value:
                assert isinstance(value, set)  # nosec
                if len(value) > 0:
                    assert all(map(lambda r: isinstance(r, str), value)) is True  # nosec

            elif p == 'roles':
                assert isinstance(value, set)  # nosec
                if len(value) > 0:
                    assert all(map(lambda r: isinstance(r, Role), value)) is True  # nosec

        if user_roles:
            assert len(user_roles) == len(user.roles)  # nosec
            diff = set(user_roles).difference(user.roles)
            assert diff == set()  # nosec

        return True

    def validate_user_and_metadata(self,  # noqa: C901
                                   user_metadata,
                                   user_roles=None,
                                   groups=None):
        property_list = [
            'domain', 'user', 'effective_roles', 'password_changed',
            'external_groups'
        ]
        properties = list(n for n in dir(user_metadata) if n in property_list)
        for p in properties:
            value = getattr(user_metadata, p)
            if p == 'domain':
                assert isinstance(value, AuthDomain)  # nosec
            elif p == 'user':
                assert isinstance(value, User)  # nosec
                # per RFC, user property should return a mutable User object
                #   that will not have an effect on the UserAndMetadata object
                assert id(value) != id(user_metadata._user)  # nosec
                self.validate_user(value, user_roles)
            elif p == 'effective_roles':
                assert isinstance(value, list)  # nosec
                if len(value) > 0:
                    assert all(map(lambda r: isinstance(r, RoleAndOrigins), value)) is True  # nosec
            elif p == 'password_changed' and value:
                assert isinstance(value, datetime)  # nosec
            elif p == 'external_groups' and value:
                assert isinstance(value, set)  # nosec
                if len(value) > 0:
                    assert all(map(lambda r: isinstance(r, str), value)) is True  # nosec

        if user_roles:
            actual_roles = set(
                r.role for r in user_metadata.effective_roles
                if any(map(lambda o: o.type == 'user', r.origins))
                or len(r.origins) == 0)
            assert len(user_roles) == len(actual_roles)  # nosec
            assert set(user_roles) == actual_roles  # nosec

        if groups:
            actual_roles = set(
                r.role for r in user_metadata.effective_roles
                if any(map(lambda o: o.type != 'user', r.origins)))
            group_roles = set()
            for g in groups:
                group_roles.update(g.roles)

            assert len(group_roles) == len(actual_roles)  # nosec

        return True

    def validate_group(self, group, roles=None):
        property_list = [
            'name', 'description', 'roles', 'ldap_group_reference'
        ]
        properties = list(n for n in dir(group) if n in property_list)
        for p in properties:
            value = getattr(group, p)
            if p == 'name':
                assert value is not None  # nosec
            elif p == 'description' and value:
                assert isinstance(value, str)  # nosec
            elif p == 'roles':
                assert isinstance(value, set)  # nosec
                if len(value) > 0:
                    assert all(map(lambda r: isinstance(r, Role), value)) is True  # nosec
            elif p == 'ldap_group_reference' and value:
                assert isinstance(value, str)  # nosec

        if roles:
            assert len(roles) == len(group.roles)  # nosec
            assert set(roles) == group.roles  # nosec

        return True

    # commong eventing function validation
    def validate_bucket_bindings(self, bindings  # type: List[EventingFunctionBucketBinding]
                                 ) -> None:
        binding_fields = fields(EventingFunctionBucketBinding)
        type_hints = get_type_hints(EventingFunctionBucketBinding)
        for binding in bindings:
            assert isinstance(binding, EventingFunctionBucketBinding)  # nosec
            for field in binding_fields:
                value = getattr(binding, field.name)
                if value is not None:
                    if field.name == "name":
                        assert isinstance(value, EventingFunctionKeyspace)  # nosec
                    elif field.name == "access":
                        assert isinstance(value, EventingFunctionBucketAccess)  # nosec
                    else:
                        assert isinstance(value, type_hints[field.name])  # nosec

    def validate_url_bindings(self, bindings  # type: List[EventingFunctionUrlBinding]
                              ) -> None:
        binding_fields = fields(EventingFunctionUrlBinding)
        type_hints = get_type_hints(EventingFunctionUrlBinding)
        for binding in bindings:
            assert isinstance(binding, EventingFunctionUrlBinding)  # nosec
            for field in binding_fields:
                value = getattr(binding, field.name)
                if value is not None:
                    if field.name == "auth":
                        if isinstance(value, EventingFunctionUrlAuthBasic):
                            assert isinstance(value.username, str)  # nosec
                            assert value.password is None  # nosec
                        elif isinstance(value, EventingFunctionUrlAuthDigest):
                            assert isinstance(value.username, str)  # nosec
                            assert value.password is None  # nosec
                        elif isinstance(value, EventingFunctionUrlAuthBearer):
                            assert value.key is None  # nosec
                        else:
                            assert isinstance(value, EventingFunctionUrlNoAuth)  # nosec
                    else:
                        assert isinstance(value, type_hints[field.name])  # nosec

    def validate_constant_bindings(self, bindings  # type: List[EventingFunctionConstantBinding]
                                   ) -> None:
        binding_fields = fields(EventingFunctionConstantBinding)
        type_hints = get_type_hints(EventingFunctionConstantBinding)
        for binding in bindings:
            assert isinstance(binding, EventingFunctionConstantBinding)  # nosec
            for field in binding_fields:
                value = getattr(binding, field.name)
                if value is not None:
                    assert isinstance(value, type_hints[field.name])  # nosec

    def validate_settings(self, settings  # type: EventingFunctionSettings  # noqa: C901
                          ) -> None:  # noqa: C901
        assert isinstance(settings, EventingFunctionSettings)  # nosec
        settings_fields = fields(EventingFunctionSettings)
        type_hints = get_type_hints(EventingFunctionSettings)
        for field in settings_fields:
            value = getattr(settings, field.name)
            if value is not None:
                if field.name == "dcp_stream_boundary":
                    assert isinstance(value, EventingFunctionDcpBoundary)  # nosec
                elif field.name == "deployment_status":
                    assert isinstance(value, EventingFunctionDeploymentStatus)  # nosec
                elif field.name == "processing_status":
                    assert isinstance(value, EventingFunctionProcessingStatus)  # nosec
                elif field.name == "language_compatibility":
                    assert isinstance(value, EventingFunctionLanguageCompatibility)  # nosec
                elif field.name == "log_level":
                    assert isinstance(value, EventingFunctionLogLevel)  # nosec
                elif field.name == "query_consistency":
                    assert isinstance(value, QueryScanConsistency)  # nosec
                elif field.name == "handler_headers":
                    assert isinstance(value, list)  # nosec
                elif field.name == "handler_footers":
                    assert isinstance(value, list)  # nosec
                else:
                    assert isinstance(value, type_hints[field.name])  # nosec

    def validate_eventing_function(self, func,  # type: EventingFunction
                                   shallow=False  # type: Optional[bool]
                                   ) -> None:
        assert isinstance(func, EventingFunction)  # nosec
        if shallow is False:
            func_fields = fields(EventingFunction)
            type_hints = get_type_hints(EventingFunction)
            for field in func_fields:
                value = getattr(func, field.name)
                if value is not None:
                    if field.name == "bucket_bindings":
                        self.validate_bucket_bindings(value)
                    elif field.name == "url_bindings":
                        self.validate_url_bindings(value)
                    elif field.name == "constant_bindings":
                        self.validate_constant_bindings(value)
                    elif field.name == "settings":
                        self.validate_settings(value)
                    else:
                        assert isinstance(value, type_hints[field.name])  # nosec


class ClusterInformation():
    def __init__(self):
        self.host = "localhost"
        self.port = 8091
        self.admin_username = "Administrator"
        self.admin_password = "password"
        self.bucket_name = "default"
        self.bucket_password = ""
        self.real_server_enabled = False
        self.mock_server_enabled = False
        self.mock_server = None
        # self.mock_path = ""
        # self.mock_url = None
        # self.mock_server = None
        # self.mock_version = None

    @property
    def is_mock_server(self):
        return self.mock_server_enabled

    @property
    def is_real_server(self):
        return self.real_server_enabled

    def get_connection_string(self):
        if self.mock_server_enabled:
            if self.mock_server.mock_type == MockServerType.Legacy:
                # What current client uses for mock:
                # http://127.0.0.1:49696?ipv6=disabled
                return f"http://{self.host}:{self.port}"

            return self.mock_server.connstr
        else:
            return f"couchbase://{self.host}"

    def get_username_and_pw(self):
        return self.admin_username, self.admin_password

    def shutdown(self):
        if self.mock_server_enabled and self.mock_server.mock_type == MockServerType.GoCAVES:
            self.mock_server.shutdown()


def create_mock_server(mock_type,  # type: MockServerType
                       mock_path,  # type: str
                       mock_download_url,  # type: Optional[str]
                       mock_version,  # type: Optional[str]
                       log_dir=None,  # type: Optional[str]
                       log_filename=None,  # type: Optional[str]
                       ) -> MockServer:

    if mock_type == MockServerType.Legacy:
        bspec_dfl = LegacyMockBucketSpec('default', 'couchbase')
        mock = MockServer.create_legacy_mock_server([bspec_dfl],
                                                    mock_path,
                                                    mock_download_url,
                                                    replicas=2,
                                                    nodes=4)
    else:
        mock = MockServer.create_caves_mock_server(mock_path,
                                                   mock_download_url,
                                                   mock_version,
                                                   log_dir,
                                                   log_filename)

    try:
        mock.start()
        if mock_type == MockServerType.GoCAVES:
            mock.create_cluster()
    except Exception as ex:
        raise CouchbaseTestEnvironmentException(
            f"Problem trying to start mock server:\n{ex}")

    return mock


def restart_mock(mock) -> None:
    try:
        print("\nR.I.P. mock...")
        mock.stop()
        time.sleep(3)
        mock.start()
        return mock
    except Exception:
        import traceback
        traceback.print_exc()
        raise CouchbaseTestEnvironmentException('Error trying to restart mock')


@dataclass
class RateLimitData:
    url: str = None
    username: str = None
    pw: str = None
    fts_indexes: List[str] = None


def load_config():  # noqa: C901

    cluster_info = ClusterInformation()
    try:
        config = ConfigParser()
        config.read(CONFIG_FILE)

        if config.getboolean('realserver', 'enabled'):
            cluster_info.real_server_enabled = True
            cluster_info.host = config.get('realserver', 'host')
            cluster_info.port = config.getint('realserver', 'port')
            cluster_info.admin_username = config.get(
                'realserver', 'admin_username')
            cluster_info.admin_password = config.get(
                'realserver', 'admin_password')
            cluster_info.bucket_name = config.get('realserver', 'bucket_name')
            cluster_info.bucket_password = config.get(
                'realserver', 'bucket_password')

        mock_path = ''
        mock_url = ''
        mock_version = ''
        # @TODO(jc):  allow override of log dir and filename
        # log_dir = ''
        # log_filename = ''
        if config.getboolean('gocaves', 'enabled'):
            if cluster_info.real_server_enabled:
                raise CouchbaseTestEnvironmentException(
                    "Both real and mock servers cannot be enabled at the same time.")

            cluster_info.mock_server_enabled = True

            if config.has_option('gocaves', 'path'):
                mock_path = str(
                    BASEDIR.joinpath(config.get('gocaves', 'path')))
            if config.has_option('gocaves', 'url'):
                mock_url = config.get('gocaves', 'url')
            if config.has_option('gocaves', 'version'):
                mock_version = config.get('gocaves', 'version')

            cluster_info.mock_server = create_mock_server(MockServerType.GoCAVES,
                                                          mock_path,
                                                          mock_url,
                                                          mock_version)
            cluster_info.bucket_name = "default"
            # cluster_info.port = cluster_info.mock_server.rest_port
            # cluster_info.host = "127.0.0.1"
            cluster_info.admin_username = "Administrator"
            cluster_info.admin_password = "password"

        if config.has_section('legacymockserver') and config.getboolean('legacymockserver', 'enabled'):
            if cluster_info.real_server_enabled:
                raise CouchbaseTestEnvironmentException(
                    "Both real and mock servers cannot be enabled at the same time.")

            if cluster_info.mock_server_enabled:
                raise CouchbaseTestEnvironmentException(
                    "Both java mock and gocaves mock cannot be enabled at the same time.")

            cluster_info.mock_server_enabled = True
            mock_path = str(
                BASEDIR.joinpath(config.get("mockserver", "path")))
            if config.has_option("mockserver", "url"):
                mock_url = config.get("mockserver", "url")

            cluster_info.mock_server = create_mock_server(MockServerType.Legacy,
                                                          mock_path,
                                                          mock_url)
            cluster_info.bucket_name = "default"
            cluster_info.port = cluster_info.mock_server.rest_port
            cluster_info.host = "127.0.0.1"
            cluster_info.admin_username = "Administrator"
            cluster_info.admin_password = "password"

    except CouchbaseTestEnvironmentException:
        raise
    except Exception as ex:
        raise CouchbaseTestEnvironmentException(
            f"Problem trying read/load test configuration:\n{ex}")

    return cluster_info


@pytest.fixture(scope="session")
def couchbase_test_config():
    config = load_config()
    yield config
    config.shutdown()
