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

# !! AUTO-GENERATED from tools/autogen/config/operations.yaml: DO NOT EDIT MANUALLY !!
# Regenerate with `python -m tools.autogen operations generate --write`.

"""Every operation the SDK performs, one `Op` member each, with the facts both protocols read.

A member is what a user calls and a trace names, not a binding call.  Its `.value` is the
operation's canonical name; the span and metric name is `span_name`, and is None for an
operation that is not traced.
"""

from __future__ import annotations

from enum import Enum


class ServiceType(Enum):
    Analytics = 'analytics'
    Eventing = 'eventing'
    KeyValue = 'kv'
    Management = 'management'
    Query = 'query'
    Search = 'search'
    Transactions = 'transactions'
    Views = 'views'


class OpKind(Enum):
    # one key, one request
    KeyValue = 'key_value'
    # one KeyValue operation over many keys, metered per key
    KeyValueMulti = 'key_value_multi'
    # a list, map, queue or set operation, built from KeyValue operations and metered as those
    Datastructure = 'datastructure'
    # a request whose rows are streamed back
    Streaming = 'streaming'
    # a request to a management service, or a composite of them
    Management = 'management'
    # connection lifecycle and health, not traced
    Cluster = 'cluster'


class Op(Enum):
    span_name: str | None
    kind: OpKind
    service: ServiceType | None
    # Whether a couchbase2 request is read-only, so that its timeout is unambiguous and a retry
    # strategy may treat it as idempotent.  Classic's core decides for itself and reads nothing here.
    couchbase2_readonly: bool
    # On a KeyValueMulti member, the operation each key is; None on every other.
    single: Op | None

    def __new__(cls,
                value: str,
                span_name: str | None,
                kind: OpKind,
                service: ServiceType | None,
                couchbase2_readonly: bool,
                single: str | None = None) -> Op:
        member = object.__new__(cls)
        member._value_ = value
        member.span_name = span_name
        member.kind = kind
        member.service = service
        member.couchbase2_readonly = couchbase2_readonly
        # The table defines a single before any multi naming it, so it is already a member here.
        member.single = cls[single] if single is not None else None
        return member

    Append = ('append', 'append', OpKind.KeyValue, ServiceType.KeyValue, False)
    Decrement = ('decrement', 'decrement', OpKind.KeyValue, ServiceType.KeyValue, False)
    Exists = ('exists', 'exists', OpKind.KeyValue, ServiceType.KeyValue, True)
    Get = ('get', 'get', OpKind.KeyValue, ServiceType.KeyValue, True)
    GetAllReplicas = ('get_all_replicas', 'get_all_replicas', OpKind.KeyValue, ServiceType.KeyValue, True)
    GetAndLock = ('get_and_lock', 'get_and_lock', OpKind.KeyValue, ServiceType.KeyValue, False)
    GetAndTouch = ('get_and_touch', 'get_and_touch', OpKind.KeyValue, ServiceType.KeyValue, False)
    GetAnyReplica = ('get_any_replica', 'get_any_replica', OpKind.KeyValue, ServiceType.KeyValue, True)
    GetReplica = ('get_replica', 'get_replica', OpKind.KeyValue, ServiceType.KeyValue, True)
    Increment = ('increment', 'increment', OpKind.KeyValue, ServiceType.KeyValue, False)
    Insert = ('insert', 'insert', OpKind.KeyValue, ServiceType.KeyValue, False)
    LookupIn = ('lookup_in', 'lookup_in', OpKind.KeyValue, ServiceType.KeyValue, True)
    LookupInAllReplicas = ('lookup_in_all_replicas',
                           'lookup_in_all_replicas',
                           OpKind.KeyValue,
                           ServiceType.KeyValue,
                           True)
    LookupInAnyReplica = ('lookup_in_any_replica', 'lookup_in_any_replica', OpKind.KeyValue, ServiceType.KeyValue, True)
    MutateIn = ('mutate_in', 'mutate_in', OpKind.KeyValue, ServiceType.KeyValue, False)
    Prepend = ('prepend', 'prepend', OpKind.KeyValue, ServiceType.KeyValue, False)
    Remove = ('remove', 'remove', OpKind.KeyValue, ServiceType.KeyValue, False)
    Replace = ('replace', 'replace', OpKind.KeyValue, ServiceType.KeyValue, False)
    Touch = ('touch', 'touch', OpKind.KeyValue, ServiceType.KeyValue, False)
    Unlock = ('unlock', 'unlock', OpKind.KeyValue, ServiceType.KeyValue, False)
    Upsert = ('upsert', 'upsert', OpKind.KeyValue, ServiceType.KeyValue, False)
    AppendMulti = ('append_multi', 'append_multi', OpKind.KeyValueMulti, ServiceType.KeyValue, False, 'Append')
    DecrementMulti = ('decrement_multi',
                      'decrement_multi',
                      OpKind.KeyValueMulti,
                      ServiceType.KeyValue,
                      False,
                      'Decrement')
    ExistsMulti = ('exists_multi', 'exists_multi', OpKind.KeyValueMulti, ServiceType.KeyValue, True, 'Exists')
    GetMulti = ('get_multi', 'get_multi', OpKind.KeyValueMulti, ServiceType.KeyValue, True, 'Get')
    GetAllReplicasMulti = ('get_all_replicas_multi',
                           'get_all_replicas_multi',
                           OpKind.KeyValueMulti,
                           ServiceType.KeyValue,
                           True,
                           'GetAllReplicas')
    GetAndLockMulti = ('get_and_lock_multi',
                       'get_and_lock_multi',
                       OpKind.KeyValueMulti,
                       ServiceType.KeyValue,
                       False,
                       'GetAndLock')
    GetAnyReplicaMulti = ('get_any_replica_multi',
                          'get_any_replica_multi',
                          OpKind.KeyValueMulti,
                          ServiceType.KeyValue,
                          True,
                          'GetAnyReplica')
    IncrementMulti = ('increment_multi',
                      'increment_multi',
                      OpKind.KeyValueMulti,
                      ServiceType.KeyValue,
                      False,
                      'Increment')
    InsertMulti = ('insert_multi', 'insert_multi', OpKind.KeyValueMulti, ServiceType.KeyValue, False, 'Insert')
    PrependMulti = ('prepend_multi', 'prepend_multi', OpKind.KeyValueMulti, ServiceType.KeyValue, False, 'Prepend')
    RemoveMulti = ('remove_multi', 'remove_multi', OpKind.KeyValueMulti, ServiceType.KeyValue, False, 'Remove')
    ReplaceMulti = ('replace_multi', 'replace_multi', OpKind.KeyValueMulti, ServiceType.KeyValue, False, 'Replace')
    TouchMulti = ('touch_multi', 'touch_multi', OpKind.KeyValueMulti, ServiceType.KeyValue, False, 'Touch')
    UnlockMulti = ('unlock_multi', 'unlock_multi', OpKind.KeyValueMulti, ServiceType.KeyValue, False, 'Unlock')
    UpsertMulti = ('upsert_multi', 'upsert_multi', OpKind.KeyValueMulti, ServiceType.KeyValue, False, 'Upsert')
    ListAppend = ('list_append', 'list_append', OpKind.Datastructure, ServiceType.KeyValue, False)
    ListClear = ('list_clear', 'list_clear', OpKind.Datastructure, ServiceType.KeyValue, False)
    ListGetAll = ('list_get_all', 'list_get_all', OpKind.Datastructure, ServiceType.KeyValue, True)
    ListGetAt = ('list_get_at', 'list_get_at', OpKind.Datastructure, ServiceType.KeyValue, True)
    ListIndexOf = ('list_index_of', 'list_index_of', OpKind.Datastructure, ServiceType.KeyValue, True)
    ListPrepend = ('list_prepend', 'list_prepend', OpKind.Datastructure, ServiceType.KeyValue, False)
    ListRemoveAt = ('list_remove_at', 'list_remove_at', OpKind.Datastructure, ServiceType.KeyValue, False)
    ListSetAt = ('list_set_at', 'list_set_at', OpKind.Datastructure, ServiceType.KeyValue, False)
    ListSize = ('list_size', 'list_size', OpKind.Datastructure, ServiceType.KeyValue, True)
    MapAdd = ('map_add', 'map_add', OpKind.Datastructure, ServiceType.KeyValue, False)
    MapClear = ('map_clear', 'map_clear', OpKind.Datastructure, ServiceType.KeyValue, False)
    MapExists = ('map_exists', 'map_exists', OpKind.Datastructure, ServiceType.KeyValue, True)
    MapGet = ('map_get', 'map_get', OpKind.Datastructure, ServiceType.KeyValue, True)
    MapGetAll = ('map_get_all', 'map_get_all', OpKind.Datastructure, ServiceType.KeyValue, True)
    MapItems = ('map_items', 'map_items', OpKind.Datastructure, ServiceType.KeyValue, True)
    MapKeys = ('map_keys', 'map_keys', OpKind.Datastructure, ServiceType.KeyValue, True)
    MapRemove = ('map_remove', 'map_remove', OpKind.Datastructure, ServiceType.KeyValue, False)
    MapSize = ('map_size', 'map_size', OpKind.Datastructure, ServiceType.KeyValue, True)
    MapValues = ('map_values', 'map_values', OpKind.Datastructure, ServiceType.KeyValue, True)
    QueueClear = ('queue_clear', 'queue_clear', OpKind.Datastructure, ServiceType.KeyValue, False)
    QueuePop = ('queue_pop', 'queue_pop', OpKind.Datastructure, ServiceType.KeyValue, False)
    QueuePush = ('queue_push', 'queue_push', OpKind.Datastructure, ServiceType.KeyValue, False)
    QueueSize = ('queue_size', 'queue_size', OpKind.Datastructure, ServiceType.KeyValue, True)
    SetAdd = ('set_add', 'set_add', OpKind.Datastructure, ServiceType.KeyValue, False)
    SetClear = ('set_clear', 'set_clear', OpKind.Datastructure, ServiceType.KeyValue, False)
    SetContains = ('set_contains', 'set_contains', OpKind.Datastructure, ServiceType.KeyValue, True)
    SetRemove = ('set_remove', 'set_remove', OpKind.Datastructure, ServiceType.KeyValue, False)
    SetSize = ('set_size', 'set_size', OpKind.Datastructure, ServiceType.KeyValue, True)
    SetValues = ('set_values', 'set_values', OpKind.Datastructure, ServiceType.KeyValue, True)
    RangeScan = ('range_scan', None, OpKind.Streaming, ServiceType.KeyValue, True)
    AnalyticsQuery = ('analytics_query', 'analytics_query', OpKind.Streaming, ServiceType.Analytics, False)
    Query = ('query', 'query', OpKind.Streaming, ServiceType.Query, False)
    SearchQuery = ('search_query', 'search_query', OpKind.Streaming, ServiceType.Search, True)
    ViewQuery = ('view_query', 'view_query', OpKind.Streaming, ServiceType.Views, True)
    AnalyticsDatasetCreate = ('analytics_dataset_create',
                              'manager_analytics_create_dataset',
                              OpKind.Management,
                              ServiceType.Analytics,
                              False)
    AnalyticsDatasetDrop = ('analytics_dataset_drop',
                            'manager_analytics_drop_dataset',
                            OpKind.Management,
                            ServiceType.Analytics,
                            False)
    AnalyticsDatasetGetAll = ('analytics_dataset_get_all',
                              'manager_analytics_get_all_datasets',
                              OpKind.Management,
                              ServiceType.Analytics,
                              True)
    AnalyticsDataverseCreate = ('analytics_dataverse_create',
                                'manager_analytics_create_dataverse',
                                OpKind.Management,
                                ServiceType.Analytics,
                                False)
    AnalyticsDataverseDrop = ('analytics_dataverse_drop',
                              'manager_analytics_drop_dataverse',
                              OpKind.Management,
                              ServiceType.Analytics,
                              False)
    AnalyticsGetPendingMutations = ('analytics_get_pending_mutations',
                                    'manager_analytics_get_pending_mutations',
                                    OpKind.Management,
                                    ServiceType.Analytics,
                                    True)
    AnalyticsIndexCreate = ('analytics_index_create',
                            'manager_analytics_create_index',
                            OpKind.Management,
                            ServiceType.Analytics,
                            False)
    AnalyticsIndexDrop = ('analytics_index_drop',
                          'manager_analytics_drop_index',
                          OpKind.Management,
                          ServiceType.Analytics,
                          False)
    AnalyticsIndexGetAll = ('analytics_index_get_all',
                            'manager_analytics_get_all_indexes',
                            OpKind.Management,
                            ServiceType.Analytics,
                            True)
    AnalyticsLinkConnect = ('analytics_link_connect',
                            'manager_analytics_connectlink',
                            OpKind.Management,
                            ServiceType.Analytics,
                            False)
    AnalyticsLinkCreate = ('analytics_link_create',
                           'manager_analytics_create_link',
                           OpKind.Management,
                           ServiceType.Analytics,
                           False)
    AnalyticsLinkDisconnect = ('analytics_link_disconnect',
                               'manager_analytics_disconnect_link',
                               OpKind.Management,
                               ServiceType.Analytics,
                               False)
    AnalyticsLinkDrop = ('analytics_link_drop',
                         'manager_analytics_drop_link',
                         OpKind.Management,
                         ServiceType.Analytics,
                         False)
    AnalyticsLinkGetAll = ('analytics_link_get_all',
                           'manager_analytics_get_all_links',
                           OpKind.Management,
                           ServiceType.Analytics,
                           True)
    AnalyticsLinkReplace = ('analytics_link_replace',
                            'manager_analytics_replace_link',
                            OpKind.Management,
                            ServiceType.Analytics,
                            False)
    BucketCreate = ('bucket_create', 'manager_buckets_create_bucket', OpKind.Management, ServiceType.Management, False)
    BucketDescribe = ('bucket_describe',
                      'manager_buckets_describe_bucket',
                      OpKind.Management,
                      ServiceType.Management,
                      True)
    BucketDrop = ('bucket_drop', 'manager_buckets_drop_bucket', OpKind.Management, ServiceType.Management, False)
    BucketFlush = ('bucket_flush', 'manager_buckets_flush_bucket', OpKind.Management, ServiceType.Management, False)
    BucketGet = ('bucket_get', 'manager_buckets_get_bucket', OpKind.Management, ServiceType.Management, True)
    BucketGetAll = ('bucket_get_all',
                    'manager_buckets_get_all_buckets',
                    OpKind.Management,
                    ServiceType.Management,
                    True)
    BucketUpdate = ('bucket_update', 'manager_buckets_update_bucket', OpKind.Management, ServiceType.Management, False)
    ClusterDescribe = ('cluster_describe', None, OpKind.Management, ServiceType.Management, True)
    ClusterDeveloperPreviewEnable = ('cluster_developer_preview_enable',
                                     None,
                                     OpKind.Management,
                                     ServiceType.Management,
                                     False)
    CollectionCreate = ('collection_create',
                        'manager_collections_create_collection',
                        OpKind.Management,
                        ServiceType.Management,
                        False)
    CollectionDrop = ('collection_drop',
                      'manager_collections_drop_collection',
                      OpKind.Management,
                      ServiceType.Management,
                      False)
    CollectionUpdate = ('collection_update',
                        'manager_collections_update_collection',
                        OpKind.Management,
                        ServiceType.Management,
                        False)
    ScopeCreate = ('scope_create', 'manager_collections_create_scope', OpKind.Management, ServiceType.Management, False)
    ScopeDrop = ('scope_drop', 'manager_collections_drop_scope', OpKind.Management, ServiceType.Management, False)
    ScopeGetAll = ('scope_get_all',
                   'manager_collections_get_all_scopes',
                   OpKind.Management,
                   ServiceType.Management,
                   True)
    ChangePassword = ('change_password',
                      'manager_users_change_password',
                      OpKind.Management,
                      ServiceType.Management,
                      False)
    GroupDrop = ('group_drop', 'manager_users_drop_group', OpKind.Management, ServiceType.Management, False)
    GroupGet = ('group_get', 'manager_users_get_group', OpKind.Management, ServiceType.Management, True)
    GroupGetAll = ('group_get_all', 'manager_users_get_all_groups', OpKind.Management, ServiceType.Management, True)
    GroupUpsert = ('group_upsert', 'manager_users_upsert_group', OpKind.Management, ServiceType.Management, False)
    RoleGetAll = ('role_get_all', 'manager_users_get_all_roles', OpKind.Management, ServiceType.Management, True)
    UserDrop = ('user_drop', 'manager_users_drop_user', OpKind.Management, ServiceType.Management, False)
    UserGet = ('user_get', 'manager_users_get_user', OpKind.Management, ServiceType.Management, True)
    UserGetAll = ('user_get_all', 'manager_users_get_all_users', OpKind.Management, ServiceType.Management, True)
    UserUpsert = ('user_upsert', 'manager_users_upsert_user', OpKind.Management, ServiceType.Management, False)
    EventingDeployFunction = ('eventing_deploy_function',
                              'manager_eventing_deploy_function',
                              OpKind.Management,
                              ServiceType.Eventing,
                              False)
    EventingDropFunction = ('eventing_drop_function',
                            'manager_eventing_drop_function',
                            OpKind.Management,
                            ServiceType.Eventing,
                            False)
    EventingGetAllFunctions = ('eventing_get_all_functions',
                               'manager_eventing_get_all_functions',
                               OpKind.Management,
                               ServiceType.Eventing,
                               True)
    EventingGetFunction = ('eventing_get_function',
                           'manager_eventing_get_function',
                           OpKind.Management,
                           ServiceType.Eventing,
                           True)
    EventingGetStatus = ('eventing_get_status',
                         'manager_eventing_functions_status',
                         OpKind.Management,
                         ServiceType.Eventing,
                         True)
    EventingPauseFunction = ('eventing_pause_function',
                             'manager_eventing_pause_function',
                             OpKind.Management,
                             ServiceType.Eventing,
                             False)
    EventingResumeFunction = ('eventing_resume_function',
                              'manager_eventing_resume_function',
                              OpKind.Management,
                              ServiceType.Eventing,
                              False)
    EventingUndeployFunction = ('eventing_undeploy_function',
                                'manager_eventing_undeploy_function',
                                OpKind.Management,
                                ServiceType.Eventing,
                                False)
    EventingUpsertFunction = ('eventing_upsert_function',
                              'manager_eventing_upsert_function',
                              OpKind.Management,
                              ServiceType.Eventing,
                              False)
    QueryIndexBuild = ('query_index_build', 'manager_query_build_indexes', OpKind.Management, ServiceType.Query, False)
    QueryIndexBuildDeferred = ('query_index_build_deferred',
                               'manager_query_build_deferred_indexes',
                               OpKind.Management,
                               ServiceType.Query,
                               False)
    QueryIndexCreate = ('query_index_create', 'manager_query_create_index', OpKind.Management, ServiceType.Query, False)
    QueryIndexDrop = ('query_index_drop', 'manager_query_drop_index', OpKind.Management, ServiceType.Query, False)
    QueryIndexGetAll = ('query_index_get_all',
                        'manager_query_get_all_indexes',
                        OpKind.Management,
                        ServiceType.Query,
                        True)
    QueryIndexGetAllDeferred = ('query_index_get_all_deferred',
                                'manager_query_get_all_deferred_indexes',
                                OpKind.Management,
                                ServiceType.Query,
                                True)
    QueryIndexWaitForIndexOnline = ('query_index_wait_for_index_online',
                                    'manager_query_wait_for_index_online',
                                    OpKind.Management,
                                    ServiceType.Query,
                                    True)
    QueryIndexWatchIndexes = ('query_index_watch_indexes',
                              'manager_query_watch_indexes',
                              OpKind.Management,
                              ServiceType.Query,
                              True)
    SearchGetStats = ('search_get_stats', 'manager_search_get_stats', OpKind.Management, ServiceType.Search, True)
    SearchIndexAllowQuerying = ('search_index_allow_querying',
                                'manager_search_allow_querying',
                                OpKind.Management,
                                ServiceType.Search,
                                False)
    SearchIndexAnalyzeDocument = ('search_index_analyze_document',
                                  'manager_search_analyze_document',
                                  OpKind.Management,
                                  ServiceType.Search,
                                  True)
    SearchIndexDisallowQuerying = ('search_index_disallow_querying',
                                   'manager_search_disallow_querying',
                                   OpKind.Management,
                                   ServiceType.Search,
                                   False)
    SearchIndexDrop = ('search_index_drop', 'manager_search_drop_index', OpKind.Management, ServiceType.Search, False)
    SearchIndexFreezePlan = ('search_index_freeze_plan',
                             'manager_search_freeze_plan',
                             OpKind.Management,
                             ServiceType.Search,
                             False)
    SearchIndexGet = ('search_index_get', 'manager_search_get_index', OpKind.Management, ServiceType.Search, True)
    SearchIndexGetAll = ('search_index_get_all',
                         'manager_search_get_all_indexes',
                         OpKind.Management,
                         ServiceType.Search,
                         True)
    SearchIndexGetDocumentsCount = ('search_index_get_documents_count',
                                    'manager_search_get_indexed_documents_count',
                                    OpKind.Management,
                                    ServiceType.Search,
                                    True)
    SearchIndexGetStats = ('search_index_get_stats',
                           'manager_search_get_index_stats',
                           OpKind.Management,
                           ServiceType.Search,
                           True)
    SearchIndexPauseIngest = ('search_index_pause_ingest',
                              'manager_search_pause_ingest',
                              OpKind.Management,
                              ServiceType.Search,
                              False)
    SearchIndexResumeIngest = ('search_index_resume_ingest',
                               'manager_search_resume_ingest',
                               OpKind.Management,
                               ServiceType.Search,
                               False)
    SearchIndexUnfreezePlan = ('search_index_unfreeze_plan',
                               'manager_search_unfreeze_plan',
                               OpKind.Management,
                               ServiceType.Search,
                               False)
    SearchIndexUpsert = ('search_index_upsert',
                         'manager_search_upsert_index',
                         OpKind.Management,
                         ServiceType.Search,
                         False)
    ViewIndexDrop = ('view_index_drop',
                     'manager_views_drop_design_document',
                     OpKind.Management,
                     ServiceType.Views,
                     False)
    ViewIndexGet = ('view_index_get', 'manager_views_get_design_document', OpKind.Management, ServiceType.Views, True)
    ViewIndexGetAll = ('view_index_get_all',
                       'manager_views_get_all_design_documents',
                       OpKind.Management,
                       ServiceType.Views,
                       True)
    ViewIndexPublish = ('view_index_publish',
                        'manager_views_publish_design_document',
                        OpKind.Management,
                        ServiceType.Views,
                        False)
    ViewIndexUpsert = ('view_index_upsert',
                       'manager_views_upsert_design_document',
                       OpKind.Management,
                       ServiceType.Views,
                       False)
    Close = ('close', None, OpKind.Cluster, None, False)
    CloseBucket = ('close_bucket', None, OpKind.Cluster, None, False)
    Connect = ('connect', None, OpKind.Cluster, None, False)
    Diagnostics = ('diagnostics', None, OpKind.Cluster, None, False)
    GetClusterLabels = ('get_cluster_labels', None, OpKind.Cluster, None, False)
    GetConnectionInfo = ('get_connection_info', None, OpKind.Cluster, None, False)
    GetDefaultTimeouts = ('get_default_timeouts', None, OpKind.Cluster, None, False)
    OpenBucket = ('open_bucket', None, OpKind.Cluster, None, False)
    Ping = ('ping', None, OpKind.Cluster, None, True)
    UpdateCredentials = ('update_credentials', None, OpKind.Cluster, None, False)
    WaitUntilReady = ('wait_until_ready', None, OpKind.Cluster, None, True)
