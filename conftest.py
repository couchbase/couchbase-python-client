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

import pytest

pytest_plugins = [
    # "tests.helpers"
    'tests.couchbase_config',
    'tests.environments.test_environment'
]

_DIAGNOSTIC_TESTS = [
    "acouchbase/tests/bucket_t.py::BucketDiagnosticsTests",
    "acouchbase/tests/cluster_t.py::ClusterDiagnosticsTests",
    "couchbase/tests/bucket_t.py::ClassicBucketDiagnosticsTests",
    "couchbase/tests/cluster_t.py::ClassicClusterDiagnosticsTests",
]

_KV_TESTS = [
    "acouchbase/tests/collection_t.py::CollectionTests",
    "acouchbase/tests/subdoc_t.py::SubDocumentTests",
    "acouchbase/tests/mutation_tokens_t.py::MutationTokensEnabledTests",
    "acouchbase/tests/binary_collection_t.py::BinaryCollectionTests",
    "acouchbase/tests/datastructures_t.py::DatastructuresTests",
    "acouchbase/tests/transcoder_t.py::DefaultTranscoderTests",
    "couchbase/tests/collection_t.py::ClassicCollectionTests",
    "couchbase/tests/collection_multi_t.py::ClassicCollectionMultiTests",
    "couchbase/tests/subdoc_t.py::ClassicSubDocumentTests",
    "couchbase/tests/mutation_tokens_t.py::ClassicMutationTokensEnabledTests",
    "couchbase/tests/binary_collection_t.py::ClassicBinaryCollectionTests",
    "couchbase/tests/binary_collection_multi_t.py::ClassicBinaryCollectionMultiTests",
    "couchbase/tests/datastructures_t.py::ClassicDatastructuresTests",
    "couchbase/tests/datastructures_t.py::ClassicLegacyDatastructuresTests",
    "couchbase/tests/transcoder_t.py::ClassicDefaultTranscoderTests",
    "txcouchbase/tests/collection_t.py::CollectionTests",
    "txcouchbase/tests/subdoc_t.py::SubDocumentTests",
    "txcouchbase/tests/mutation_tokens_t.py::MutationTokensEnabledTests",
    "txcouchbase/tests/binary_collection_t.py::BinaryCollectionTests",
    "txcouchbase/tests/transcoder_t.py::DefaultTranscoderTests",
]

_STREAMING_TESTS = [
    "acouchbase/tests/query_t.py::ClassicQueryTests",
    "acouchbase/tests/query_t.py::ClassicQueryCollectionTests",
    "acouchbase/tests/analytics_t.py::ClassicAnalyticsTests",
    "acouchbase/tests/analytics_t.py::ClassicAnalyticsCollectionTests",
    "acouchbase/tests/search_t.py::ClassicSearchTests",
    "acouchbase/tests/search_t.py::ClassicSearchCollectionTests",
    "acouchbase/tests/views_t.py::ClassicViewsTests",
    "couchbase/tests/analytics_params_t.py::ClassicAnalyticsParamTests",
    "couchbase/tests/analytics_t.py::ClassicAnalyticsTests",
    "couchbase/tests/analytics_t.py::ClassicAnalyticsCollectionTests",
    "couchbase/tests/query_params_t.py::ClassicQueryParamTests",
    "couchbase/tests/query_t.py::ClassicQueryTests",
    "couchbase/tests/query_t.py::ClassicQueryCollectionTests",
    "couchbase/tests/search_params_t.py::ClassicSearchParamTests",
    "couchbase/tests/search_params_t.py::ClassicVectorSearchParamTests",
    "couchbase/tests/search_t.py::ClassicSearchTests",
    "couchbase/tests/search_t.py::ClassicSearchCollectionTests",
    "couchbase/tests/views_t.py::ClassicViewsTests",
]

_MGMT_TESTS = [
    "acouchbase/tests/analyticsmgmt_t.py::AnalyticsManagementTests",
    "acouchbase/tests/analyticsmgmt_t.py::AnalyticsManagementLinksTests",
    "acouchbase/tests/bucketmgmt_t.py::BucketManagementTests",
    "acouchbase/tests/collectionmgmt_t.py::CollectionManagementTests",
    "acouchbase/tests/eventingmgmt_t.py::EventingManagementTests",
    "acouchbase/tests/eventingmgmt_t.py::ScopeEventingManagementTests",
    "acouchbase/tests/querymgmt_t.py::QueryIndexManagementTests",
    "acouchbase/tests/querymgmt_t.py::QueryIndexCollectionManagementTests",
    "acouchbase/tests/searchmgmt_t.py::SearchIndexManagementTests",
    "acouchbase/tests/usermgmt_t.py::UserManagementTests",
    "acouchbase/tests/viewmgmt_t.py::ViewIndexManagementTests",
    "couchbase/tests/analyticsmgmt_t.py::ClassicAnalyticsManagementTests",
    "couchbase/tests/analyticsmgmt_t.py::ClassicAnalyticsManagementLinksTests",
    "couchbase/tests/bucketmgmt_t.py::ClassicBucketManagementTests",
    "couchbase/tests/collectionmgmt_t.py::ClassicCollectionManagementTests",
    "couchbase/tests/eventingmgmt_t.py::ClassicEventingManagementTests",
    "couchbase/tests/eventingmgmt_t.py::ClassicScopeEventingManagementTests",
    "couchbase/tests/querymgmt_t.py::ClassicQueryIndexManagementTests",
    "couchbase/tests/querymgmt_t.py::ClassicQueryIndexCollectionManagementTests",
    "couchbase/tests/searchmgmt_t.py::ClassicSearchIndexManagementTests",
    "couchbase/tests/usermgmt_t.py::ClassicUserManagementTests",
    "couchbase/tests/viewmgmt_t.py::ClassicViewIndexManagementTests"
]

_SLOW_MGMT_TESTS = [
    "acouchbase/tests/eventingmgmt_t.py::EventingManagementTests",
    "acouchbase/tests/eventingmgmt_t.py::ScopeEventingManagementTests",
    "couchbase/tests/eventingmgmt_t.py::ClassicEventingManagementTests",
    "couchbase/tests/eventingmgmt_t.py::ClassicScopeEventingManagementTests",
]

_MISC_TESTS = [
    "acouchbase/tests/rate_limit_t.py::RateLimitTests",
    "couchbase/tests/connection_t.py::ClassicConnectionTests"
    "couchbase/tests/rate_limit_t.py::ClassicRateLimitTests",
]

_TXNS_TESTS = [
    "acouchbase/tests/transactions_t.py::ClassicTransactionTests",
    "couchbase/tests/transactions_t.py:ClassicTransactionTests",
]


@pytest.fixture(name="couchbase_config", scope="session")
def get_config(couchbase_test_config):
    if couchbase_test_config.mock_server_enabled:
        print("Mock server enabled!")
    if couchbase_test_config.real_server_enabled:
        print("Real server enabled!")

    return couchbase_test_config


def pytest_addoption(parser):
    parser.addoption(
        "--txcouchbase", action="store_true", default=False, help="run txcouchbase tests"
    )


def pytest_collection_modifyitems(items):  # noqa: C901
    for item in items:
        item_details = item.nodeid.split('::')

        item_api = item_details[0].split('/')
        if item_api[0] == 'couchbase':
            item.add_marker(pytest.mark.pycbc_couchbase)
        elif item_api[0] == 'acouchbase':
            item.add_marker(pytest.mark.pycbc_acouchbase)
        elif item_api[0] == 'txcouchbase':
            item.add_marker(pytest.mark.pycbc_txcouchbase)

        test_class_path = '::'.join(item_details[:-1])
        if test_class_path in _DIAGNOSTIC_TESTS:
            item.add_marker(pytest.mark.pycbc_diag)
        elif test_class_path in _KV_TESTS:
            item.add_marker(pytest.mark.pycbc_kv)
        elif test_class_path in _STREAMING_TESTS:
            item.add_marker(pytest.mark.pycbc_streaming)
        elif test_class_path in _MGMT_TESTS:
            item.add_marker(pytest.mark.pycbc_mgmt)
        elif test_class_path in _MISC_TESTS:
            item.add_marker(pytest.mark.pycbc_misc)
        elif test_class_path in _TXNS_TESTS:
            item.add_marker(pytest.mark.pycbc_txn)

        if test_class_path in _SLOW_MGMT_TESTS:
            item.add_marker(pytest.mark.pycbc_slow_mgmt)
