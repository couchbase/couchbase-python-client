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


class AnalyticsIndexMgmtOperationType(Enum):
    ConnectLink = 'connect_link'
    CreateDataset = 'create_dataset'
    CreateDataverse = 'create_dataverse'
    CreateIndex = 'create_index'
    CreateLink = 'create_link'
    DisconnectLink = 'disconnect_link'
    DropDataset = 'drop_dataset'
    DropDataverse = 'drop_dataverse'
    DropLink = 'drop_link'
    DropIndex = 'drop_index'
    GetAllDatasets = 'get_all_datasets'
    GetAllIndexes = 'get_all_indexes'
    GetLinks = 'get_links'
    GetPendingMutations = 'get_pending_mutations'
    ReplaceLink = 'replace_link'


class BucketMgmtOperationType(Enum):
    BucketDescribe = 'bucket_describe'
    CreateBucket = 'create_bucket'
    DropBucket = 'drop_bucket'
    FlushBucket = 'flush_bucket'
    GetAllBuckets = 'get_all_buckets'
    GetBucket = 'get_bucket'
    UpdateBucket = 'update_bucket'


class BucketOperationType(Enum):
    CloseBucket = 'close_bucket'
    OpenBucket = 'open_bucket'
    Ping = 'ping'


class ClusterOperationType(Enum):
    AnalyicsQuery = 'analytics_query'
    CreateConnection = 'create_connection'
    CloseConnection = 'close_connection'
    Diagnostics = 'diagnostics'
    GetClusterInfo = 'get_cluster_info'
    GetConnectionInfo = 'get_connection_info'
    Ping = 'ping'
    Query = 'query'
    SearchQuery = 'search_query'
    UpdateCredentials = 'update_credentials'
    WaitUntilReady = 'wait_until_ready'


class CollectionMgmtOperationType(Enum):
    CreateCollection = 'create_collection'
    CreateScope = 'create_scope'
    DropCollection = 'drop_collection'
    DropScope = 'drop_scope'
    GetAllScopes = 'get_all_scopes'
    UpdateCollection = 'update_collection'


class EventingFunctionMgmtOperationType(Enum):
    DeployFunction = 'deploy_function'
    DropFunction = 'drop_function'
    FunctionsStatus = 'functions_status'
    GetAllFunctions = 'get_all_functions'
    GetFunction = 'get_function'
    PauseFunction = 'pause_function'
    ResumeFunction = 'resume_function'
    UndeployFunction = 'undeploy_function'
    UpsertFunction = 'upsert_function'


class KeyValueMultiOperationType(Enum):
    AppendMulti = 'append_multi'
    DecrementMulti = 'decrement_multi'
    ExistsMulti = 'exists_multi'
    GetAllReplicasMulti = 'get_all_replicas_multi'
    GetAndLockMulti = 'get_and_lock_multi'
    GetAnyReplicaMulti = 'get_any_replica_multi'
    GetMulti = 'get_multi'
    IncrementMulti = 'increment_multi'
    InsertMulti = 'insert_multi'
    PrependMulti = 'prepend_multi'
    RemoveMulti = 'remove_multi'
    ReplaceMulti = 'replace_multi'
    TouchMulti = 'touch_multi'
    UnlockMulti = 'unlock_multi'
    UpsertMulti = 'upsert_multi'


class KeyValueOperationType(Enum):
    Append = 'append'
    Decrement = 'decrement'
    Exists = 'exists'
    Get = 'get'
    GetAllReplicas = 'get_all_replicas'
    GetAndLock = 'get_and_lock'
    GetAndTouch = 'get_and_touch'
    GetAnyReplica = 'get_any_replica'
    GetProject = 'get_project'
    Increment = 'increment'
    Insert = 'insert'
    LookupIn = 'lookup_in'
    LookupInAllReplicas = 'lookup_in_all_replicas'
    LookupInAnyReplica = 'lookup_in_any_replica'
    MutateIn = 'mutate_in'
    Prepend = 'prepend'
    RangeScanCancel = 'range_scan_canel'
    RangeScanContinue = 'range_scan_continue'
    RangeScanCreate = 'range_scan_create'
    Remove = 'remove'
    Replace = 'replace'
    Touch = 'touch'
    Unlock = 'unlock'
    Upsert = 'upsert'


class QueryIndexMgmtOperationType(Enum):
    CreateIndex = 'create_index'
    CreatePrimaryIndex = 'create_primary_index'
    DropIndex = 'drop_index'
    DropPrimaryIndex = 'drop_primary_index'
    GetAllIndexes = 'get_all_indexes'
    BuildDeferredIndexes = 'build_deferred_indexes'
    WatchIndexes = 'watch_indexes'


class ScopeOperationType(Enum):
    AnalyicsQuery = 'analytics_query'
    Query = 'query'
    SearchQuery = 'search_query'


class SearchIndexMgmtOperationType(Enum):
    AllowQuerying = 'allow_querying'
    AnalyzeDocument = 'analyze_document'
    DisallowQuerying = 'disallow_querying'
    DropIndex = 'drop_index'
    FreezePlan = 'freeze_plan'
    GetAllIndexes = 'get_all_indexes'
    GetAllIndexStats = 'get_all_index_stats'
    GetIndex = 'get_index'
    GetIndexedDocumentsCount = 'get_indexed_documents_count'
    GetIndexStats = 'get_index_stats'
    PauseIngest = 'pause_ingest'
    ResumeIngest = 'resume_ingest'
    UnfreezePlan = 'unfreeze_plan'
    UpsertIndex = 'upsert_index'


class UserMgmtOperationType(Enum):
    ChangePassword = 'change_password'
    DropGroup = 'drop_group'
    DropUser = 'drop_user'
    GetAllGroups = 'get_all_groups'
    GetAllUsers = 'get_all_users'
    GetGroup = 'get_group'
    GetRoles = 'get_roles'
    GetUser = 'get_user'
    UpsertGroup = 'upsert_group'
    UpsertUser = 'upsert_user'


class ViewIndexMgmtOperationType(Enum):
    DropDesignDocument = 'drop_design_document'
    GetAllDesignDocuments = 'get_all_design_documents'
    GetDesignDocument = 'get_design_document'
    PublishDesignDocument = 'publish_design_document'
    UpsertDesignDocument = 'upsert_design_document'
