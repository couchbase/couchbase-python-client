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

import asyncio
import time
import warnings
from collections.abc import Callable, Iterable
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from acouchbase.protocol.classic._management._analytics_mgmt_impl import AsyncAnalyticsMgmtImpl
from acouchbase.protocol.classic._management._query_index_mgmt_impl import AsyncQueryIndexMgmtImpl
from acouchbase.protocol.classic._management._view_index_mgmt_impl import AsyncViewIndexMgmtImpl
from couchbase.common._observability.handler import ObservableRequestHandler
from couchbase.common._observability.no_op import NoOpMeter, NoOpTracer
from couchbase.common._observability.observability_types import (ObservabilityInstruments,
                                                                 TracerProtocol,
                                                                 WrappedTracer)
from couchbase.common._operations import Op
from couchbase.common.exceptions import (AmbiguousTimeoutException,
                                         InvalidArgumentException,
                                         WatchQueryIndexTimeoutException)
from couchbase.common.management.analytics import (AnalyticsDataType,
                                                   AnalyticsEncryptionLevel,
                                                   AnalyticsLinkType,
                                                   AzureBlobExternalAnalyticsLink,
                                                   CouchbaseAnalyticsEncryptionSettings,
                                                   CouchbaseRemoteAnalyticsLink,
                                                   S3ExternalAnalyticsLink)
from couchbase.common.management.buckets import (BucketSettings,
                                                 ConflictResolutionType,
                                                 CreateBucketSettings)
from couchbase.common.management.collections import (CollectionSpec,
                                                     CreateCollectionSettings,
                                                     UpdateCollectionSettings)
from couchbase.common.management.options import (ChangePasswordOptions,
                                                 ConnectLinkOptions,
                                                 CreateAnalyticsIndexOptions,
                                                 CreateCollectionOptions,
                                                 CreateDatasetOptions,
                                                 CreateDataverseOptions,
                                                 CreatePrimaryQueryIndexOptions,
                                                 CreateQueryIndexOptions,
                                                 DropAnalyticsIndexOptions,
                                                 DropBucketOptions,
                                                 DropCollectionOptions,
                                                 DropDatasetOptions,
                                                 DropDataverseOptions,
                                                 DropPrimaryQueryIndexOptions,
                                                 DropQueryIndexOptions,
                                                 DropUserOptions,
                                                 GetAllQueryIndexOptions,
                                                 GetAllUsersOptions,
                                                 GetLinksAnalyticsOptions,
                                                 GetUserOptions,
                                                 PublishDesignDocumentOptions,
                                                 UpsertUserOptions,
                                                 WatchQueryIndexOptions)
from couchbase.common.management.queries import QueryIndex
from couchbase.common.management.users import (Group,
                                               Role,
                                               User)
from couchbase.common.management.views import DesignDocumentNamespace
from couchbase.common.option_enums import OptionValidationMode
from couchbase.common.warnings import CouchbaseDeprecationWarning, CouchbaseOptionWarning
from couchbase.observability.tracing import SpanStatusCode
from couchbase.protocol.classic._management._analytics_mgmt_impl import AnalyticsMgmtImpl
from couchbase.protocol.classic._management._analytics_mgmt_req_builder import AnalyticsMgmtRequestBuilder
from couchbase.protocol.classic._management._bucket_mgmt_req_builder import BucketMgmtRequestBuilder
from couchbase.protocol.classic._management._collection_mgmt_req_builder import CollectionMgmtRequestBuilder
from couchbase.protocol.classic._management._query_index_mgmt_impl import QueryIndexMgmtImpl
from couchbase.protocol.classic._management._query_index_mgmt_req_builder import QueryIndexMgmtRequestBuilder
from couchbase.protocol.classic._management._query_index_mgmt_types import WatchIndexesRequest
from couchbase.protocol.classic._management._user_mgmt_req_builder import UserMgmtRequestBuilder
from couchbase.protocol.classic._management._view_index_mgmt_impl import ViewIndexMgmtImpl
from couchbase.protocol.classic._management._view_index_mgmt_req_builder import ViewIndexMgmtRequestBuilder
from tests.observability_test_impls import RecordingTracer
from tests.test_features import TestProtocol

# These build classic requests and read the dict classic hands its binding.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

BUCKET = 'default'
SCOPE = 'test-scope'
COLLECTION = 'test-collection'
INDEX = 'test-index'
USER = 'test-user'
GROUP = 'test-group'
ADMIN = {'name': 'admin', 'bucket': None, 'scope': None, 'collection': None}
TWO_SECONDS = timedelta(seconds=2)
# On Windows, time.monotonic() can advance in 15.6ms ticks, so what is left of a timeout can read up
# to one tick long.
CLOCK_TICK_MS = round(time.get_clock_info('monotonic').resolution * 1000)
DATASET = 'test-dataset'
DATAVERSE = 'test-dataverse'
LINK = 'test-link'
DDOC = 'test-ddoc'


def _obs_handler(op: Op) -> ObservableRequestHandler:
    # See the twin helper in cluster_req_builder_t.py for why the tracer is cast.
    tracer = WrappedTracer(cast(TracerProtocol, NoOpTracer()), False)
    instruments = ObservabilityInstruments(tracer, NoOpMeter(), is_noop=True)
    return ObservableRequestHandler(op, instruments)


_BUCKET = BucketMgmtRequestBuilder()
_COLL = CollectionMgmtRequestBuilder()
_QUERY = QueryIndexMgmtRequestBuilder()
_USER = UserMgmtRequestBuilder()
_ANALYTICS = AnalyticsMgmtRequestBuilder()

_CollectionCase = tuple[str, Callable[[ObservableRequestHandler], Any], dict[str, Any]]

_COLLECTION_CASES: list[_CollectionCase] = [
    ('create, no settings',
     lambda h: _COLL.build_create_collection_request(BUCKET, SCOPE, COLLECTION, obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION}),
    ('create, settings None',
     lambda h: _COLL.build_create_collection_request(BUCKET, SCOPE, COLLECTION, None, obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION}),
    ('create, empty settings',
     lambda h: _COLL.build_create_collection_request(BUCKET, SCOPE, COLLECTION, CreateCollectionSettings(),
                                                     obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION}),
    ('create, both settings',
     lambda h: _COLL.build_create_collection_request(
         BUCKET, SCOPE, COLLECTION, CreateCollectionSettings(max_expiry=timedelta(seconds=10), history=False),
         obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION,
      'max_expiry': 10, 'history': False}),
    ('create, zero max_expiry is sent',
     lambda h: _COLL.build_create_collection_request(
         BUCKET, SCOPE, COLLECTION, CreateCollectionSettings(max_expiry=timedelta(0)), obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION, 'max_expiry': 0}),
    ('create, keywords and a timeout',
     lambda h: _COLL.build_create_collection_request(
         BUCKET, scope_name=SCOPE, collection_name=COLLECTION, settings=CreateCollectionSettings(history=True),
         timeout=timedelta(seconds=2), obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION,
      'history': True, 'timeout': 2000}),
    ('create, options and a timeout',
     lambda h: _COLL.build_create_collection_request(
         BUCKET, SCOPE, COLLECTION, CreateCollectionSettings(history=True),
         CreateCollectionOptions(timeout=timedelta(seconds=2)), obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION,
      'history': True, 'timeout': 2000}),
    ('create, options by keyword',
     lambda h: _COLL.build_create_collection_request(
         BUCKET, SCOPE, COLLECTION, options=CreateCollectionOptions(timeout=timedelta(seconds=2)), obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION, 'timeout': 2000}),
    ('create, CollectionSpec and options by keyword',
     lambda h: _COLL.build_create_collection_request(
         BUCKET, collection=CollectionSpec(COLLECTION, SCOPE),
         options=CreateCollectionOptions(timeout=timedelta(seconds=2)), obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION, 'timeout': 2000}),
    ('create, CollectionSpec',
     lambda h: _COLL.build_create_collection_request(
         BUCKET, CollectionSpec(COLLECTION, SCOPE, timedelta(seconds=5), history=True), obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION, 'max_expiry': 5,
      'history': True}),
    ('create, CollectionSpec carrying only history',
     lambda h: _COLL.build_create_collection_request(
         BUCKET, CollectionSpec(COLLECTION, SCOPE, history=False), obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION, 'history': False}),
    ('update, history only',
     lambda h: _COLL.build_update_collection_request(
         BUCKET, SCOPE, COLLECTION, UpdateCollectionSettings(history=True), h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION, 'history': True}),
    ('update, both settings',
     lambda h: _COLL.build_update_collection_request(
         BUCKET, SCOPE, COLLECTION, UpdateCollectionSettings(max_expiry=timedelta(minutes=1), history=False), h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION,
      'max_expiry': 60, 'history': False}),
    ('drop',
     lambda h: _COLL.build_drop_collection_request(BUCKET, SCOPE, COLLECTION, obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION}),
    ('drop, options by keyword',
     lambda h: _COLL.build_drop_collection_request(
         BUCKET, SCOPE, COLLECTION, options=DropCollectionOptions(timeout=timedelta(seconds=2)), obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION, 'timeout': 2000}),
    ('drop, CollectionSpec',
     lambda h: _COLL.build_drop_collection_request(
         BUCKET, CollectionSpec(COLLECTION, SCOPE, timedelta(seconds=5)), obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION}),
    ('create scope',
     lambda h: _COLL.build_create_scope_request(BUCKET, SCOPE, h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE}),
    ('drop scope',
     lambda h: _COLL.build_drop_scope_request(BUCKET, SCOPE, h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE}),
    ('get all scopes',
     lambda h: _COLL.build_get_all_scopes_request(BUCKET, h),
     {'bucket_name': BUCKET}),
]

_QueryCase = tuple[str, Callable[[ObservableRequestHandler], Any], dict[str, Any]]

_QUERY_CASES: list[_QueryCase] = [
    ('create',
     lambda h: _QUERY.build_create_index_request(BUCKET, INDEX, ['a', 'b'], h),
     {'bucket_name': BUCKET, 'index_name': INDEX, 'keys': ['a', 'b']}),
    ('create, tuple keys become a list',
     lambda h: _QUERY.build_create_index_request(BUCKET, INDEX, ('a', 'b'), h),
     {'bucket_name': BUCKET, 'index_name': INDEX, 'keys': ['a', 'b']}),
    # False and 0 are sent; an empty name or condition is not.
    ('create, falsy options',
     lambda h: _QUERY.build_create_index_request(
         BUCKET, '', ['a'], h, None,
         CreateQueryIndexOptions(condition='', ignore_if_exists=False, deferred=False, num_replicas=0)),
     {'bucket_name': BUCKET, 'keys': ['a'], 'ignore_if_exists': False, 'deferred': False, 'num_replicas': 0}),
    ('create, every option',
     lambda h: _QUERY.build_create_index_request(
         BUCKET, INDEX, ['a'], h, None,
         CreateQueryIndexOptions(condition='a > 1', ignore_if_exists=True, deferred=True, num_replicas=2,
                                 scope_name=SCOPE, collection_name=COLLECTION, timeout=timedelta(seconds=2))),
     {'bucket_name': BUCKET, 'index_name': INDEX, 'keys': ['a'], 'condition': 'a > 1', 'ignore_if_exists': True,
      'deferred': True, 'num_replicas': 2, 'scope_name': SCOPE, 'collection_name': COLLECTION, 'timeout': 2000}),
    # The collection context is (collection, scope), the reverse of the option order.
    ('create, collection context',
     lambda h: _QUERY.build_create_index_request(BUCKET, INDEX, ['a'], h, (COLLECTION, SCOPE)),
     {'bucket_name': BUCKET, 'index_name': INDEX, 'keys': ['a'], 'scope_name': SCOPE,
      'collection_name': COLLECTION}),
    ('create primary',
     lambda h: _QUERY.build_create_primary_index_request(BUCKET, h),
     {'bucket_name': BUCKET, 'is_primary': True}),
    ('create primary, named',
     lambda h: _QUERY.build_create_primary_index_request(
         BUCKET, h, None, CreatePrimaryQueryIndexOptions(index_name=INDEX, deferred=True)),
     {'bucket_name': BUCKET, 'is_primary': True, 'index_name': INDEX, 'deferred': True}),
    ('drop',
     lambda h: _QUERY.build_drop_index_request(BUCKET, INDEX, h),
     {'bucket_name': BUCKET, 'index_name': INDEX}),
    ('drop, ignore_if_not_exists False is not sent',
     lambda h: _QUERY.build_drop_index_request(
         BUCKET, INDEX, h, None, DropQueryIndexOptions(ignore_if_not_exists=False)),
     {'bucket_name': BUCKET, 'index_name': INDEX}),
    ('drop, ignore_if_not_exists',
     lambda h: _QUERY.build_drop_index_request(
         BUCKET, INDEX, h, None, DropQueryIndexOptions(ignore_if_not_exists=True, timeout=timedelta(seconds=2))),
     {'bucket_name': BUCKET, 'index_name': INDEX, 'ignore_if_does_not_exist': True, 'timeout': 2000}),
    ('drop, collection context',
     lambda h: _QUERY.build_drop_index_request(BUCKET, INDEX, h, (COLLECTION, SCOPE)),
     {'bucket_name': BUCKET, 'index_name': INDEX, 'scope_name': SCOPE, 'collection_name': COLLECTION}),
    ('drop primary',
     lambda h: _QUERY.build_drop_primary_index_request(BUCKET, h),
     {'bucket_name': BUCKET, 'is_primary': True}),
    ('drop primary, empty name is not sent',
     lambda h: _QUERY.build_drop_primary_index_request(BUCKET, h, None, DropPrimaryQueryIndexOptions(index_name='')),
     {'bucket_name': BUCKET, 'is_primary': True}),
    ('drop primary, named',
     lambda h: _QUERY.build_drop_primary_index_request(
         BUCKET, h, None, DropPrimaryQueryIndexOptions(index_name=INDEX, ignore_if_not_exists=True)),
     {'bucket_name': BUCKET, 'is_primary': True, 'index_name': INDEX, 'ignore_if_does_not_exist': True}),
    ('get all',
     lambda h: _QUERY.build_get_all_indexes_request(BUCKET, h),
     {'bucket_name': BUCKET}),
    ('get all, options',
     lambda h: _QUERY.build_get_all_indexes_request(
         BUCKET, h, None, GetAllQueryIndexOptions(scope_name=SCOPE, collection_name=COLLECTION)),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION}),
    ('build deferred',
     lambda h: _QUERY.build_build_deferred_indexes_request(BUCKET, h),
     {'bucket_name': BUCKET}),
    ('build deferred, collection context',
     lambda h: _QUERY.build_build_deferred_indexes_request(BUCKET, h, (COLLECTION, SCOPE)),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION}),
]

_BucketCase = tuple[str, Callable[[ObservableRequestHandler], Any], dict[str, Any]]

_BUCKET_CASES: list[_BucketCase] = [
    ('describe',
     lambda h: _BUCKET.build_bucket_describe_request(BUCKET, h),
     {'name': BUCKET}),
    ('create, minimal settings',
     lambda h: _BUCKET.build_create_bucket_request(CreateBucketSettings(name=BUCKET), h),
     {'bucket': {'name': BUCKET}}),
    ('create, conflict resolution and a timeout',
     lambda h: _BUCKET.build_create_bucket_request(
         CreateBucketSettings(name=BUCKET, conflict_resolution_type=ConflictResolutionType.CUSTOM,
                              num_replicas=0), h, timeout=TWO_SECONDS),
     {'bucket': {'name': BUCKET, 'num_replicas': 0, 'conflict_resolution_type': 'custom'}, 'timeout': 2000}),
    ('drop',
     lambda h: _BUCKET.build_drop_bucket_request(BUCKET, h),
     {'name': BUCKET}),
    ('drop, options',
     lambda h: _BUCKET.build_drop_bucket_request(BUCKET, h, DropBucketOptions(timeout=TWO_SECONDS)),
     {'name': BUCKET, 'timeout': 2000}),
    ('flush',
     lambda h: _BUCKET.build_flush_bucket_request(BUCKET, h),
     {'name': BUCKET}),
    ('get all',
     lambda h: _BUCKET.build_get_all_buckets_request(h),
     {}),
    ('get all, timeout',
     lambda h: _BUCKET.build_get_all_buckets_request(h, timeout=TWO_SECONDS),
     {'timeout': 2000}),
    ('get',
     lambda h: _BUCKET.build_get_bucket_request(BUCKET, h),
     {'name': BUCKET}),
    ('update, flush disabled',
     lambda h: _BUCKET.build_update_bucket_request(BucketSettings(name=BUCKET, flush_enabled=False), h),
     {'bucket': {'name': BUCKET, 'flush_enabled': False}}),
    ('update leaves conflict resolution out',
     lambda h: _BUCKET.build_update_bucket_request(
         CreateBucketSettings(name=BUCKET, conflict_resolution_type=ConflictResolutionType.CUSTOM), h),
     {'bucket': {'name': BUCKET}}),
]

_UserCase = tuple[str, Callable[[ObservableRequestHandler], Any], dict[str, Any]]

_USER_CASES: list[_UserCase] = [
    ('change password',
     lambda h: _USER.build_change_password_request('new-password', h),
     {'newPassword': 'new-password'}),
    ('change password, options',
     lambda h: _USER.build_change_password_request('new-password', h, ChangePasswordOptions(timeout=TWO_SECONDS)),
     {'newPassword': 'new-password', 'timeout': 2000}),
    ('drop group',
     lambda h: _USER.build_drop_group_request(GROUP, h),
     {'name': GROUP}),
    ('drop user, local by default',
     lambda h: _USER.build_drop_user_request(USER, h),
     {'username': USER, 'domain': 'local'}),
    ('drop user, external domain',
     lambda h: _USER.build_drop_user_request(USER, h, DropUserOptions(domain_name='external')),
     {'username': USER, 'domain': 'external'}),
    ('get all groups',
     lambda h: _USER.build_get_all_groups_request(h),
     {}),
    ('get all users',
     lambda h: _USER.build_get_all_users_request(h),
     {'domain': 'local'}),
    ('get all users, options',
     lambda h: _USER.build_get_all_users_request(h, GetAllUsersOptions(domain_name='external',
                                                                       timeout=TWO_SECONDS)),
     {'domain': 'external', 'timeout': 2000}),
    ('get group',
     lambda h: _USER.build_get_group_request(GROUP, h),
     {'name': GROUP}),
    ('get roles',
     lambda h: _USER.build_get_roles_request(h, timeout=TWO_SECONDS),
     {'timeout': 2000}),
    ('get user',
     lambda h: _USER.build_get_user_request(USER, h),
     {'username': USER, 'domain': 'local'}),
    ('get user, options',
     lambda h: _USER.build_get_user_request(USER, h, GetUserOptions(domain_name='external')),
     {'username': USER, 'domain': 'external'}),
    ('upsert group',
     lambda h: _USER.build_upsert_group_request(Group(GROUP, description='d', roles=Role('admin')), h),
     {'group': {'name': GROUP, 'description': 'd', 'ldap_group_reference': None, 'roles': [ADMIN]}}),
    ('upsert user, local',
     lambda h: _USER.build_upsert_user_request(User(USER, password='pw', roles=[Role('admin')]), h),
     {'user': {'username': USER, 'display_name': None, 'password': 'pw', 'roles': [ADMIN]}, 'domain': 'local'}),
    ('upsert user, external with groups only',
     lambda h: _USER.build_upsert_user_request(User(USER, groups='g'), h,
                                               UpsertUserOptions(domain_name='external', timeout=TWO_SECONDS)),
     {'user': {'username': USER, 'display_name': None, 'password': None, 'groups': {'g'}},
      'domain': 'external', 'timeout': 2000}),
]


_AnalyticsCase = tuple[str, Callable[[ObservableRequestHandler], Any], dict[str, Any]]

_FULL_ENCRYPTION = CouchbaseAnalyticsEncryptionSettings(AnalyticsEncryptionLevel.FULL, certificate=b'cert',
                                                        client_certificate=b'client-cert', client_key=b'client-key')

_ANALYTICS_CASES: list[_AnalyticsCase] = [
    ('connect link',
     lambda h: _ANALYTICS.build_connect_link_request(h),
     {}),
    ('connect link, options',
     lambda h: _ANALYTICS.build_connect_link_request(h, ConnectLinkOptions(dataverse_name=DATAVERSE, link_name=LINK,
                                                                           force=True, timeout=TWO_SECONDS)),
     {'dataverse_name': DATAVERSE, 'link_name': LINK, 'force': True, 'timeout': 2000}),
    ('create dataset',
     lambda h: _ANALYTICS.build_create_dataset_request(DATASET, BUCKET, h),
     {'dataset_name': DATASET, 'bucket_name': BUCKET}),
    ('create dataset, condition',
     lambda h: _ANALYTICS.build_create_dataset_request(DATASET, BUCKET, h,
                                                       CreateDatasetOptions(condition='`type` = "airline"',
                                                                            dataverse_name=DATAVERSE,
                                                                            ignore_if_exists=True)),
     {'dataset_name': DATASET, 'bucket_name': BUCKET, 'condition': '`type` = "airline"',
      'dataverse_name': DATAVERSE, 'ignore_if_exists': True}),
    ('create dataverse',
     lambda h: _ANALYTICS.build_create_dataverse_request(DATAVERSE, h, CreateDataverseOptions(ignore_if_exists=True)),
     {'dataverse_name': DATAVERSE, 'ignore_if_exists': True}),
    ('create index',
     lambda h: _ANALYTICS.build_create_index_request(INDEX, DATASET, {'f': AnalyticsDataType.STRING}, h,
                                                     CreateAnalyticsIndexOptions(dataverse_name=DATAVERSE)),
     {'index_name': INDEX, 'dataset_name': DATASET, 'fields': {'f': 'string'}, 'dataverse_name': DATAVERSE}),
    ('create couchbase remote link, full encryption',
     lambda h: _ANALYTICS.build_create_link_request(
         CouchbaseRemoteAnalyticsLink(DATAVERSE, LINK, 'host', _FULL_ENCRYPTION), h),
     {'link': {'link_name': LINK, 'dataverse': DATAVERSE, 'hostname': 'host', 'link_type': 'couchbase',
               'encryption': {'level': 'full', 'certificate': 'cert', 'client_certificate': 'client-cert',
                              'client_key': 'client-key'}}}),
    ('create s3 link',
     lambda h: _ANALYTICS.build_create_link_request(
         S3ExternalAnalyticsLink(DATAVERSE, LINK, 'key-id', 'region', secret_access_key='secret'), h),
     {'link': {'link_name': LINK, 'dataverse': DATAVERSE, 'access_key_id': 'key-id', 'secret_access_key': 'secret',
               'region': 'region', 'link_type': 's3'}}),
    ('create azure blob link',
     lambda h: _ANALYTICS.build_create_link_request(
         AzureBlobExternalAnalyticsLink(DATAVERSE, LINK, connection_string='conn'), h),
     {'link': {'link_name': LINK, 'dataverse': DATAVERSE, 'connection_string': 'conn'}}),
    ('disconnect link',
     lambda h: _ANALYTICS.build_disconnect_link_request(h, dataverse_name=DATAVERSE, link_name=LINK),
     {'dataverse_name': DATAVERSE, 'link_name': LINK}),
    ('drop dataset',
     lambda h: _ANALYTICS.build_drop_dataset_request(DATASET, h, DropDatasetOptions(ignore_if_not_exists=True,
                                                                                    dataverse_name=DATAVERSE)),
     {'dataset_name': DATASET, 'ignore_if_does_not_exist': True, 'dataverse_name': DATAVERSE}),
    ('drop dataverse',
     lambda h: _ANALYTICS.build_drop_dataverse_request(DATAVERSE, h, DropDataverseOptions(ignore_if_not_exists=True)),
     {'dataverse_name': DATAVERSE, 'ignore_if_does_not_exist': True}),
    ('drop index',
     lambda h: _ANALYTICS.build_drop_index_request(INDEX, DATASET, h, DropAnalyticsIndexOptions(timeout=TWO_SECONDS)),
     {'index_name': INDEX, 'dataset_name': DATASET, 'timeout': 2000}),
    ('drop link',
     lambda h: _ANALYTICS.build_drop_link_request(LINK, DATAVERSE, h),
     {'link_name': LINK, 'dataverse_name': DATAVERSE}),
    ('get all datasets',
     lambda h: _ANALYTICS.build_get_all_datasets_request(h),
     {}),
    ('get all indexes',
     lambda h: _ANALYTICS.build_get_all_indexes_request(h, timeout=TWO_SECONDS),
     {'timeout': 2000}),
    ('get links, options',
     lambda h: _ANALYTICS.build_get_links_request(h, GetLinksAnalyticsOptions(dataverse_name=DATAVERSE, name=LINK,
                                                                              link_type=AnalyticsLinkType.S3External)),
     {'dataverse_name': DATAVERSE, 'link_name': LINK, 'link_type': 's3'}),
    ('get pending mutations',
     lambda h: _ANALYTICS.build_get_pending_mutations_request(h),
     {}),
    ('replace couchbase remote link',
     lambda h: _ANALYTICS.build_replace_link_request(
         CouchbaseRemoteAnalyticsLink(DATAVERSE, LINK, 'host', CouchbaseAnalyticsEncryptionSettings(),
                                      username='u', password='p'), h),
     {'link': {'link_name': LINK, 'dataverse': DATAVERSE, 'hostname': 'host', 'link_type': 'couchbase',
               'username': 'u', 'password': 'p', 'encryption': {'level': 'none'}}}),
]


# Each argument is of a type its parameter does not take, so the builder refuses it before it
# builds a request.
_NOT_A_LINK: Any = LINK
_NO_FIELDS: Any = None

_REJECTED: list[tuple[str, Callable[[ObservableRequestHandler], Any], Op, str]] = [
    ('analytics create index, no fields',
     lambda h: _ANALYTICS.build_create_index_request(INDEX, DATASET, _NO_FIELDS, h),
     Op.AnalyticsIndexCreate,
     'fields must be provided when creating an analytics index.'),
    ('analytics create link, a str',
     lambda h: _ANALYTICS.build_create_link_request(_NOT_A_LINK, h),
     Op.AnalyticsLinkCreate,
     'An AnalyticsLink must be provided when creating an analytics link.'),
    ('analytics replace link, a str',
     lambda h: _ANALYTICS.build_replace_link_request(_NOT_A_LINK, h),
     Op.AnalyticsLinkReplace,
     'An AnalyticsLink must be provided when replacing an analytics link.'),
    ('query create index, keys a str',
     lambda h: _QUERY.build_create_index_request(BUCKET, INDEX, 'a', h),
     Op.QueryIndexCreate,
     'Index keys must be provided when creating a secondary index.'),
    ('query watch indexes, a str',
     lambda h: _QUERY.build_watch_indexes_request(BUCKET, INDEX, h, None,
                                                  WatchQueryIndexOptions(timeout=timedelta(seconds=2))),
     Op.QueryIndexWatchIndexes,
     'An iterable of index names must be provided when watching indexes.'),
]

# A keyword no options class carries, under each mode that does not refuse it.
_LENIENT_MODES = [None, OptionValidationMode.Ignore, OptionValidationMode.Warn]

_QueryKeywordBuild = Callable[..., Any]
_QUERY_KEYWORD_BUILDS: list[tuple[str, _QueryKeywordBuild]] = [
    ('create', lambda b, h, **kw: b.build_create_index_request(BUCKET, INDEX, ['a'], h, **kw)),
    ('create primary', lambda b, h, **kw: b.build_create_primary_index_request(BUCKET, h, **kw)),
    ('drop', lambda b, h, **kw: b.build_drop_index_request(BUCKET, INDEX, h, **kw)),
    ('drop primary', lambda b, h, **kw: b.build_drop_primary_index_request(BUCKET, h, **kw)),
]


def _quiet(build: Callable[[ObservableRequestHandler], Any], op: Op) -> Any:
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', CouchbaseDeprecationWarning)
        warnings.simplefilter('ignore', CouchbaseOptionWarning)
        return build(_obs_handler(op))


# No cb_env in any class below: each test builds a request and compares what req_to_dict hands
# the binding, including which keys are absent, since the binding treats a missing key
# differently from a falsy one.


class CollectionMgmtRequestBuilderUnitTests:

    @pytest.mark.parametrize('build, expected',
                             [pytest.param(b, e, id=i) for i, b, e in _COLLECTION_CASES])
    def test_request_dict(self, build: Callable[[ObservableRequestHandler], Any], expected: dict[str, Any]) -> None:
        assert _quiet(build, Op.CollectionCreate).req_to_dict() == expected

    @pytest.mark.parametrize('mode', _LENIENT_MODES)
    def test_unknown_keyword_is_not_sent(self, mode: OptionValidationMode | None) -> None:
        builder = CollectionMgmtRequestBuilder(mode)
        req = _quiet(lambda h: builder.build_drop_scope_request(BUCKET, SCOPE, h, not_an_option=1), Op.ScopeDrop)
        assert req.req_to_dict() == {'bucket_name': BUCKET, 'scope_name': SCOPE}

    def test_unknown_keyword_warns(self) -> None:
        builder = CollectionMgmtRequestBuilder(OptionValidationMode.Warn)
        with pytest.warns(CouchbaseOptionWarning, match='not_an_option'):
            builder.build_drop_scope_request(BUCKET, SCOPE, _obs_handler(Op.ScopeDrop), not_an_option=1)

    def test_a_second_options_object_raises(self) -> None:
        opts = CreateCollectionOptions()
        with pytest.raises(TypeError, match='takes one options object, 2 given'):
            _quiet(lambda h: _COLL.build_create_collection_request(BUCKET, SCOPE, COLLECTION, None, opts, opts,
                                                                   obs_handler=h),
                   Op.CollectionCreate)
        with pytest.raises(TypeError, match='takes one options object, 2 given'):
            _quiet(lambda h: _COLL.build_drop_collection_request(BUCKET, CollectionSpec(COLLECTION, SCOPE),
                                                                 DropCollectionOptions(), DropCollectionOptions(),
                                                                 obs_handler=h),
                   Op.CollectionDrop)

    def test_options_positionally_and_by_keyword_raises(self) -> None:
        opts = DropCollectionOptions()
        with pytest.raises(TypeError, match="multiple values for argument 'options'"):
            _quiet(lambda h: _COLL.build_drop_collection_request(BUCKET, SCOPE, COLLECTION, opts, options=opts,
                                                                 obs_handler=h),
                   Op.CollectionDrop)

    @pytest.mark.parametrize('validate, value, message', [
        (_COLL._validate_bucket_name, 7, 'The bucket_name must be str.'),
        (_COLL._validate_scope_name, 7, 'The scope_name must be str.'),
        (_COLL._validate_collection_name, 7, 'The collection_name must be str.'),
        (_COLL._validate_scope_name, None, 'The scope_name cannot be empty.'),
        (_COLL._validate_scope_name, ' ', 'The scope_name cannot be empty.'),
    ])
    def test_name_validator_message(self, validate: Callable[[Any], Any], value: Any, message: str) -> None:
        with pytest.raises(InvalidArgumentException) as exc_info:
            validate(value)
        assert exc_info.value.message == message


class _FakeViewAdapter:
    """Answers every request with an empty design document, `delay` seconds late, and records the requests."""

    option_validation = None

    def __init__(self, delay: float = 0) -> None:
        self.delay = delay
        self.requests: list[Any] = []

    def answer(self, req: Any) -> Any:
        self.requests.append(req)
        time.sleep(self.delay)
        return SimpleNamespace(raw_result={'document': {'name': DDOC, 'views': {}}})

    def execute_mgmt_request(self, req: Any, obs_handler: Any = None) -> Any:
        return self.answer(req)


class _AsyncFakeViewAdapter:

    def __init__(self, delay: float = 0) -> None:
        self.fake = _FakeViewAdapter(delay)
        self.option_validation = None

    async def execute_mgmt_request(self, req: Any, obs_handler: Any = None) -> Any:
        return self.fake.answer(req)


class ViewIndexMgmtRequestBuilderUnitTests:

    @pytest.mark.parametrize('mode', _LENIENT_MODES)
    def test_unknown_keyword_is_not_sent(self, mode: OptionValidationMode | None) -> None:
        builder = ViewIndexMgmtRequestBuilder(mode)
        req = _quiet(lambda h: builder.build_get_design_document_request(BUCKET, DDOC,
                                                                         DesignDocumentNamespace.PRODUCTION, h,
                                                                         not_an_option=1),
                     Op.ViewIndexGet)
        assert req.req_to_dict() == {'bucket_name': BUCKET, 'document_name': DDOC, 'ns': 'production'}

    def test_unknown_keyword_warns(self) -> None:
        builder = ViewIndexMgmtRequestBuilder(OptionValidationMode.Warn)
        with pytest.warns(CouchbaseOptionWarning, match='not_an_option'):
            builder.build_get_design_document_request(BUCKET, DDOC, DesignDocumentNamespace.PRODUCTION,
                                                      _obs_handler(Op.ViewIndexGet), not_an_option=1)

    @pytest.mark.parametrize('api', ['sync', 'async'])
    def test_publish_spans_and_timeout(self, api: str) -> None:
        # Publish is a get and an upsert under one publish span: one root with the two
        # sub-requests' spans as its ended children.  The two share the publish timeout, so the
        # upsert gets what the get left.
        tracer = RecordingTracer()
        instruments = ObservabilityInstruments(WrappedTracer(cast(TracerProtocol, tracer), False), NoOpMeter())
        opts = PublishDesignDocumentOptions(timeout=TWO_SECONDS)
        if api == 'sync':
            fake = _FakeViewAdapter(delay=0.2)
            ViewIndexMgmtImpl(cast(Any, fake), BUCKET, instruments).publish_design_document(DDOC, opts)
        else:
            adapter = _AsyncFakeViewAdapter(delay=0.2)
            fake = adapter.fake
            asyncio.run(AsyncViewIndexMgmtImpl(cast(Any, adapter), BUCKET, instruments)
                        .publish_design_document(DDOC, opts))

        assert [span.name for span in tracer.spans] == [Op.ViewIndexPublish.span_name]
        children = tracer.spans[0].children
        assert [span.name for span in children] == [Op.ViewIndexGet.span_name, Op.ViewIndexUpsert.span_name]
        assert all(span.end_time is not None for span in children)
        get_req, up_req = fake.requests
        assert get_req.timeout == 2000
        assert 0 < up_req.timeout <= 1800 + CLOCK_TICK_MS


class QueryIndexMgmtRequestBuilderUnitTests:

    @pytest.mark.parametrize('build, expected',
                             [pytest.param(b, e, id=i) for i, b, e in _QUERY_CASES])
    def test_request_dict(self, build: Callable[[ObservableRequestHandler], Any], expected: dict[str, Any]) -> None:
        assert _quiet(build, Op.QueryIndexCreate).req_to_dict() == expected

    def test_watch_request(self) -> None:
        req = _QUERY.build_watch_indexes_request(BUCKET, (INDEX,), _obs_handler(Op.QueryIndexCreate), None,
                                                 WatchQueryIndexOptions(timeout=timedelta(seconds=2),
                                                                        watch_primary=True,
                                                                        scope_name=SCOPE))
        assert req == WatchIndexesRequest(bucket_name=BUCKET, index_names=[INDEX, '#primary'], timeout=2000,
                                          scope_name=SCOPE)

    def test_watch_request_collection_context(self) -> None:
        req = _QUERY.build_watch_indexes_request(BUCKET, [INDEX], _obs_handler(Op.QueryIndexCreate),
                                                 (COLLECTION, SCOPE),
                                                 WatchQueryIndexOptions(timeout=timedelta(seconds=2)))
        assert req == WatchIndexesRequest(bucket_name=BUCKET, index_names=[INDEX], timeout=2000,
                                          scope_name=SCOPE, collection_name=COLLECTION)

    def test_scope_name_is_deprecated(self) -> None:
        with pytest.warns(CouchbaseDeprecationWarning, match='Option scope_name is deprecated'):
            _QUERY.build_get_all_indexes_request(BUCKET, _obs_handler(Op.QueryIndexGetAll), None,
                                                 GetAllQueryIndexOptions(scope_name=SCOPE))

    @pytest.mark.parametrize('row, keyspace', [
        ({'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION}, COLLECTION),
        ({'bucket_name': BUCKET}, BUCKET),
    ])
    def test_keyspace_from_the_core_row(self, row: dict[str, Any], keyspace: str) -> None:
        index = QueryIndex.from_server({'name': INDEX, 'type': 'gsi', 'state': 'online', **row})
        assert index.keyspace == keyspace

    # Watching polls the server and checks the names each time, so a one-shot iterable must be
    # read once, up front.
    @pytest.mark.parametrize('index_names', [pytest.param({INDEX}, id='set'),
                                             pytest.param((n for n in [INDEX]), id='generator')])
    def test_watch_takes_any_iterable(self, index_names: Iterable[str]) -> None:
        req = _QUERY.build_watch_indexes_request(BUCKET, index_names, _obs_handler(Op.QueryIndexWatchIndexes), None,
                                                 WatchQueryIndexOptions(timeout=timedelta(seconds=2)))
        assert req.index_names == [INDEX]

    @pytest.mark.parametrize('build, message', [
        (lambda h: _QUERY.build_create_index_request(BUCKET, INDEX, [], h), 'Keys required for non-primary index'),
        (lambda h: _QUERY.build_create_primary_index_request(BUCKET, h, None,
                                                             CreatePrimaryQueryIndexOptions(condition='a > 1')),
         'cannot specify condition for primary index'),
    ])
    def test_create_rejects_the_arguments(self, build: Callable[[ObservableRequestHandler], Any], message: str) -> None:
        with pytest.raises(InvalidArgumentException) as exc_info:
            build(_obs_handler(Op.QueryIndexCreate))
        assert exc_info.value.message == message

    def test_create_takes_any_iterable(self) -> None:
        req = _QUERY.build_create_index_request(BUCKET, INDEX, (k for k in ['a', 'b']),
                                                _obs_handler(Op.QueryIndexCreate))
        assert req.req_to_dict()['keys'] == ['a', 'b']

    @pytest.mark.parametrize('mode', _LENIENT_MODES)
    @pytest.mark.parametrize('build', [pytest.param(b, id=i) for i, b in _QUERY_KEYWORD_BUILDS])
    def test_unknown_keyword_is_not_sent(self, build: _QueryKeywordBuild, mode: OptionValidationMode | None) -> None:
        builder = QueryIndexMgmtRequestBuilder(mode)
        req = _quiet(lambda h: build(builder, h, not_an_option=1), Op.QueryIndexCreate)
        assert req.req_to_dict() == _quiet(lambda h: build(builder, h), Op.QueryIndexCreate).req_to_dict()

    @pytest.mark.parametrize('build', [pytest.param(b, id=i) for i, b in _QUERY_KEYWORD_BUILDS])
    def test_unknown_keyword_warns(self, build: _QueryKeywordBuild) -> None:
        builder = QueryIndexMgmtRequestBuilder(OptionValidationMode.Warn)
        with pytest.warns(CouchbaseOptionWarning, match='not_an_option'):
            build(builder, _obs_handler(Op.QueryIndexCreate), not_an_option=1)

    @pytest.mark.parametrize('build', [pytest.param(b, id=i) for i, b in _QUERY_KEYWORD_BUILDS])
    def test_unknown_keyword_raises_when_strict(self, build: _QueryKeywordBuild) -> None:
        builder = QueryIndexMgmtRequestBuilder(OptionValidationMode.Strict)
        with pytest.raises(InvalidArgumentException, match='not_an_option'):
            build(builder, _obs_handler(Op.QueryIndexCreate), not_an_option=1)


class _FakeQueryIndexAdapter:
    """Times out the first `timeouts` get-all polls, each `delay` seconds late, then reports INDEX online.

    Records each poll's timeout.
    """

    option_validation = None

    def __init__(self, timeouts: int, delay: float = 0) -> None:
        self.timeouts = timeouts
        self.delay = delay
        self.poll_timeouts: list[int] = []

    def answer(self, req: Any) -> Any:
        self.poll_timeouts.append(req.timeout)
        if len(self.poll_timeouts) <= self.timeouts:
            time.sleep(self.delay)
            raise AmbiguousTimeoutException()
        return SimpleNamespace(raw_result={'indexes': [{'name': INDEX, 'type': 'gsi', 'state': 'online'}]})

    def execute_mgmt_request(self, req: Any, obs_handler: Any = None) -> Any:
        return self.answer(req)


class _AsyncFakeQueryIndexAdapter:

    def __init__(self, timeouts: int, delay: float = 0) -> None:
        self.fake = _FakeQueryIndexAdapter(timeouts, delay)
        self.option_validation = None

    async def execute_mgmt_request(self, req: Any, obs_handler: Any = None) -> Any:
        return self.fake.answer(req)


def _watch(api: str,
           timeouts: int,
           timeout: timedelta,
           delay: float = 0,
           tracer: RecordingTracer | None = None) -> list[int]:
    """Watch INDEX against a fake that times out `timeouts` polls; each poll's timeout."""
    if tracer is None:
        instruments = ObservabilityInstruments(WrappedTracer(cast(TracerProtocol, NoOpTracer()), False), NoOpMeter(),
                                               is_noop=True)
    else:
        instruments = ObservabilityInstruments(WrappedTracer(cast(TracerProtocol, tracer), False), NoOpMeter())
    opts = WatchQueryIndexOptions(timeout=timeout)
    if api == 'sync':
        fake = _FakeQueryIndexAdapter(timeouts, delay)
        QueryIndexMgmtImpl(cast(Any, fake), instruments).watch_indexes(BUCKET, [INDEX], opts)
        return fake.poll_timeouts
    adapter = _AsyncFakeQueryIndexAdapter(timeouts, delay)
    asyncio.run(AsyncQueryIndexMgmtImpl(cast(Any, adapter), instruments).watch_indexes(BUCKET, [INDEX], opts))
    return adapter.fake.poll_timeouts


class QueryIndexWatchUnitTests:

    @pytest.mark.parametrize('api', ['sync', 'async'])
    def test_a_timed_out_poll_is_polled_again(self, api: str) -> None:
        assert len(_watch(api, 1, TWO_SECONDS)) == 2

    @pytest.mark.parametrize('api', ['sync', 'async'])
    def test_each_poll_gets_what_is_left_of_the_timeout(self, api: str) -> None:
        # The first poll takes 0.2s and the watch then waits 0.1s, so the second has at most 1.7s.
        first, second = _watch(api, 1, TWO_SECONDS, delay=0.2)
        assert first == 2000
        assert 0 < second <= 1700 + CLOCK_TICK_MS

    @pytest.mark.parametrize('api', ['sync', 'async'])
    def test_a_timed_out_poll_span_has_error_status(self, api: str) -> None:
        tracer = RecordingTracer()
        _watch(api, 1, TWO_SECONDS, tracer=tracer)
        [watch_span] = tracer.spans
        assert [span.name for span in watch_span.children] == [Op.QueryIndexGetAll.span_name] * 2
        assert [span.status for span in watch_span.children] == [SpanStatusCode.ERROR, SpanStatusCode.UNSET]

    @pytest.mark.parametrize('api', ['sync', 'async'])
    def test_every_poll_timed_out_raises_the_watch_timeout(self, api: str) -> None:
        with pytest.raises(WatchQueryIndexTimeoutException):
            _watch(api, 100, timedelta(milliseconds=300))


class BucketMgmtRequestBuilderUnitTests:

    @pytest.mark.parametrize('build, expected',
                             [pytest.param(b, e, id=i) for i, b, e in _BUCKET_CASES])
    def test_request_dict(self, build: Callable[[ObservableRequestHandler], Any], expected: dict[str, Any]) -> None:
        assert _quiet(build, Op.BucketCreate).req_to_dict() == expected

    @pytest.mark.parametrize('mode', _LENIENT_MODES)
    def test_unknown_keyword_is_not_sent(self, mode: OptionValidationMode | None) -> None:
        builder = BucketMgmtRequestBuilder(mode)
        req = _quiet(lambda h: builder.build_get_bucket_request(BUCKET, h, not_an_option=1), Op.BucketGet)
        assert req.req_to_dict() == {'name': BUCKET}

    @pytest.mark.parametrize('value, message', [
        (7, 'The bucket_name must be str.'),
        (None, 'The bucket_name cannot be empty.'),
        (' ', 'The bucket_name cannot be empty.'),
    ])
    def test_name_validator_message(self, value: Any, message: str) -> None:
        with pytest.raises(InvalidArgumentException) as exc_info:
            _BUCKET._validate_bucket_name(value)
        assert exc_info.value.message == message

    @pytest.mark.parametrize('value, message', [
        (7, 'The bucket_name must be str.'),
        (' ', 'The bucket_name cannot be empty.'),
    ])
    def test_settings_name_is_validated(self, value: Any, message: str) -> None:
        # BucketSettings is a dict, so a name set after construction reaches the builder unchecked.
        create_settings = CreateBucketSettings(name=BUCKET)
        create_settings['name'] = value
        settings = BucketSettings(name=BUCKET)
        settings['name'] = value
        builds = [(lambda h: _BUCKET.build_create_bucket_request(create_settings, h), Op.BucketCreate),
                  (lambda h: _BUCKET.build_update_bucket_request(settings, h), Op.BucketUpdate)]
        for build, op in builds:
            with pytest.raises(InvalidArgumentException) as exc_info:
                _quiet(build, op)
            assert exc_info.value.message == message


class UserMgmtRequestBuilderUnitTests:

    @pytest.mark.parametrize('build, expected',
                             [pytest.param(b, e, id=i) for i, b, e in _USER_CASES])
    def test_request_dict(self, build: Callable[[ObservableRequestHandler], Any], expected: dict[str, Any]) -> None:
        assert _quiet(build, Op.UserGet).req_to_dict() == expected

    @pytest.mark.parametrize('mode', _LENIENT_MODES)
    def test_unknown_keyword_is_not_sent(self, mode: OptionValidationMode | None) -> None:
        builder = UserMgmtRequestBuilder(mode)
        req = _quiet(lambda h: builder.build_get_user_request(USER, h, not_an_option=1), Op.UserGet)
        assert req.req_to_dict() == {'username': USER, 'domain': 'local'}


class AnalyticsMgmtRequestBuilderUnitTests:

    @pytest.mark.parametrize('build, expected',
                             [pytest.param(b, e, id=i) for i, b, e in _ANALYTICS_CASES])
    def test_request_dict(self, build: Callable[[ObservableRequestHandler], Any], expected: dict[str, Any]) -> None:
        assert _quiet(build, Op.AnalyticsDatasetCreate).req_to_dict() == expected

    @pytest.mark.parametrize('mode', _LENIENT_MODES)
    def test_unknown_keyword_is_not_sent(self, mode: OptionValidationMode | None) -> None:
        builder = AnalyticsMgmtRequestBuilder(mode)
        req = _quiet(lambda h: builder.build_get_all_datasets_request(h, not_an_option=1), Op.AnalyticsDatasetGetAll)
        assert req.req_to_dict() == {}

    def test_replace_link_validates_the_link(self) -> None:
        link = CouchbaseRemoteAnalyticsLink(DATAVERSE, LINK, '', CouchbaseAnalyticsEncryptionSettings(),
                                            username='u', password='p')
        with pytest.raises(InvalidArgumentException) as exc_info:
            _quiet(lambda h: _ANALYTICS.build_replace_link_request(link, h), Op.AnalyticsLinkReplace)
        assert exc_info.value.message == 'Hostname must be set for couchbase analytics links.'

    # A link is validated under its operation's span, whatever its type, so a rejected link
    # leaves one ended span with error status.  Nothing reaches the adapter.
    @pytest.mark.parametrize('api', ['sync', 'async'])
    @pytest.mark.parametrize('method, op', [('create_link', Op.AnalyticsLinkCreate),
                                            ('replace_link', Op.AnalyticsLinkReplace)])
    @pytest.mark.parametrize('link', [
        pytest.param(CouchbaseRemoteAnalyticsLink('', LINK, 'host', CouchbaseAnalyticsEncryptionSettings()),
                     id='couchbase'),
        pytest.param(S3ExternalAnalyticsLink('', LINK, 'key-id', 'us-east-1', secret_access_key='secret'), id='s3'),
        pytest.param(AzureBlobExternalAnalyticsLink('', LINK, connection_string='conn'), id='azure_blob'),
    ])
    def test_a_rejected_link_has_an_error_span(self, link: Any, method: str, op: Op, api: str) -> None:
        tracer = RecordingTracer()
        instruments = ObservabilityInstruments(WrappedTracer(cast(TracerProtocol, tracer), False), NoOpMeter())
        adapter = SimpleNamespace(option_validation=None)
        with pytest.raises(InvalidArgumentException, match='Dataverse must be set'):
            if api == 'sync':
                getattr(AnalyticsMgmtImpl(cast(Any, adapter), instruments), method)(link)
            else:
                asyncio.run(getattr(AsyncAnalyticsMgmtImpl(cast(Any, adapter), instruments), method)(link))
        [span] = tracer.spans
        assert span.name == op.span_name
        assert span.status == SpanStatusCode.ERROR
        assert span.end_time is not None


class MgmtArgumentTypeUnitTests:

    @pytest.mark.parametrize('build, op, message', [pytest.param(b, o, m, id=i) for i, b, o, m in _REJECTED])
    def test_rejects_an_argument_of_the_wrong_type(self,
                                                   build: Callable[[ObservableRequestHandler], Any],
                                                   op: Op,
                                                   message: str) -> None:
        with pytest.raises(InvalidArgumentException) as exc_info:
            _quiet(build, op)
        assert exc_info.value.message == message
