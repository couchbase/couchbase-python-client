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

from typing import (Any,
                    Callable,
                    Dict,
                    TypedDict)

from couchbase.logic.operation_types import (AnalyticsIndexMgmtOperationType,
                                             BucketMgmtOperationType,
                                             BucketOperationType,
                                             ClusterOperationType,
                                             CollectionMgmtOperationType,
                                             EventingFunctionMgmtOperationType,
                                             KeyValueMultiOperationType,
                                             KeyValueOperationType,
                                             QueryIndexMgmtOperationType,
                                             SearchIndexMgmtOperationType,
                                             UserMgmtOperationType,
                                             ViewIndexMgmtOperationType)
from couchbase.pycbc_core import (binary_multi_operation,
                                  binary_operation,
                                  close_connection,
                                  create_connection,
                                  diagnostics_operation,
                                  get_connection_info,
                                  kv_multi_operation,
                                  kv_operation,
                                  management_operation,
                                  open_or_close_bucket,
                                  subdoc_operation,
                                  update_credentials)


class AnalyticsIndexMgmtOperationMap(TypedDict):
    connect_link: Callable[..., Any]
    create_dataset: Callable[..., Any]
    create_dataverse: Callable[..., Any]
    create_index: Callable[..., Any]
    create_link: Callable[..., Any]
    disconnect_link: Callable[..., Any]
    drop_dataset: Callable[..., Any]
    drop_dataverse: Callable[..., Any]
    drop_link: Callable[..., Any]
    drop_index: Callable[..., Any]
    get_all_datasets: Callable[..., Any]
    get_all_indexes: Callable[..., Any]
    get_links: Callable[..., Any]
    get_pending_mutations: Callable[..., Any]
    replace_link: Callable[..., Any]


class BucketMgmtOperationMap(TypedDict):
    bucket_describe: Callable[..., Any]
    create_bucket: Callable[..., Any]
    drop_bucket: Callable[..., Any]
    flush_bucket: Callable[..., Any]
    get_all_buckets: Callable[..., Any]
    get_bucket: Callable[..., Any]
    update_bucket: Callable[..., Any]


class BucketOperationMap(TypedDict):
    close_bucket: Callable[..., Any]
    open_bucket: Callable[..., Any]
    ping: Callable[..., Any]


class ClusterOperationMap(TypedDict):
    create_connection: Callable[..., Any]
    close_connection: Callable[..., Any]
    diagnostics: Callable[..., Any]
    get_cluster_info: Callable[..., Any]
    get_connection_info: Callable[..., Any]
    ping: Callable[..., Any]
    update_credentials: Callable[..., Any]


class CollectionMgmtOperationMap(TypedDict):
    create_collection: Callable[..., Any]
    create_scope: Callable[..., Any]
    drop_collection: Callable[..., Any]
    drop_scope: Callable[..., Any]
    get_all_scopes: Callable[..., Any]
    update_collection: Callable[..., Any]


class EventingFunctionMgmtOperationMap(TypedDict):
    deploy_function: Callable[..., Any]
    drop_function: Callable[..., Any]
    functions_status: Callable[..., Any]
    get_all_functions: Callable[..., Any]
    get_function: Callable[..., Any]
    pause_function: Callable[..., Any]
    resume_function: Callable[..., Any]
    undeploy_function: Callable[..., Any]
    upsert_function: Callable[..., Any]


class KeyValueMultiOperationMap(TypedDict):
    append_multi: Callable[..., Any]
    decrement_multi: Callable[..., Any]
    exist_multis: Callable[..., Any]
    get_multi: Callable[..., Any]
    get_all_replicas_multi: Callable[..., Any]
    get_and_lock_multi: Callable[..., Any]
    get_and_touch_multi: Callable[..., Any]
    get_any_replica_multi: Callable[..., Any]
    incremen_multit: Callable[..., Any]
    insert_multi: Callable[..., Any]
    prepend_multi: Callable[..., Any]
    remove_multi: Callable[..., Any]
    replace_multi: Callable[..., Any]
    touch_multi: Callable[..., Any]
    unlock_multi: Callable[..., Any]
    upsert_multi: Callable[..., Any]


class KeyValueOperationMap(TypedDict):
    append: Callable[..., Any]
    decrement: Callable[..., Any]
    exists: Callable[..., Any]
    get: Callable[..., Any]
    get_all_replicas: Callable[..., Any]
    get_and_lock: Callable[..., Any]
    get_and_touch: Callable[..., Any]
    get_any_replica: Callable[..., Any]
    get_project: Callable[..., Any]
    increment: Callable[..., Any]
    insert: Callable[..., Any]
    lookup_in: Callable[..., Any]
    lookup_in_all_replicas: Callable[..., Any]
    lookup_in_any_replica: Callable[..., Any]
    mutate_in: Callable[..., Any]
    prepend: Callable[..., Any]
    remove: Callable[..., Any]
    replace: Callable[..., Any]
    touch: Callable[..., Any]
    unlock: Callable[..., Any]
    upsert: Callable[..., Any]


class QueryIndexMgmtOperationMap(TypedDict):
    create_index: Callable[..., Any]
    create_primary_index: Callable[..., Any]
    drop_index: Callable[..., Any]
    drop_primary_index: Callable[..., Any]
    get_all_indexes: Callable[..., Any]
    build_deferred_indexes: Callable[..., Any]


class SearchIndexMgmtOperationMap(TypedDict):
    allow_querying: Callable[..., Any]
    analyze_document: Callable[..., Any]
    disallow_querying: Callable[..., Any]
    drop_index: Callable[..., Any]
    freeze_plan: Callable[..., Any]
    get_all_indexes: Callable[..., Any]
    get_all_index_stats: Callable[..., Any]
    get_index: Callable[..., Any]
    get_indexed_documents_count: Callable[..., Any]
    get_index_stats: Callable[..., Any]
    pause_ingest: Callable[..., Any]
    resume_ingest: Callable[..., Any]
    unfreeze_plan: Callable[..., Any]
    upsert_index: Callable[..., Any]


class UserMgmtOperationMap(TypedDict):
    change_password: Callable[..., Any]
    drop_group: Callable[..., Any]
    drop_user: Callable[..., Any]
    get_all_groups: Callable[..., Any]
    get_all_users: Callable[..., Any]
    get_group: Callable[..., Any]
    get_roles: Callable[..., Any]
    get_user: Callable[..., Any]
    upsert_group: Callable[..., Any]
    upsert_user: Callable[..., Any]


class ViewIndexMgmtOperationMap(TypedDict):
    drop_design_document: Callable[..., Any]
    get_all_design_documents: Callable[..., Any]
    get_design_document: Callable[..., Any]
    publish_design_document: Callable[..., Any]
    upsert_design_document: Callable[..., Any]


class BindingMap:

    def __init__(self) -> None:
        self._op_map = {}
        self._load_op_map()

    def _load_op_map(self) -> None:
        self._load_analytics_index_mgmt_op_map()
        self._load_bucket_mgmt_op_map()
        self._load_bucket_op_map()
        self._load_cluster_op_map()
        self._load_collection_mgmt_op_map()
        self._load_eventing_function_mgmt_op_map()
        self._load_key_value_multi_op_map()
        self._load_key_value_op_map()
        self._load_query_index_mgmt_op_map()
        self._load_search_index_mgmt_op_map()
        self._load_user_mgmt_op_map()
        self._load_view_index_mgmt_op_map()

    @property
    def op_map(self) -> Dict[str, Callable[..., Any]]:
        return self._op_map

    def _load_analytics_index_mgmt_op_map(self) -> None:
        self._op_map.update(**{
            AnalyticsIndexMgmtOperationType.ConnectLink.value: management_operation,
            AnalyticsIndexMgmtOperationType.CreateDataset.value: management_operation,
            AnalyticsIndexMgmtOperationType.CreateDataverse.value: management_operation,
            AnalyticsIndexMgmtOperationType.CreateIndex.value: management_operation,
            AnalyticsIndexMgmtOperationType.CreateLink.value: management_operation,
            AnalyticsIndexMgmtOperationType.DisconnectLink.value: management_operation,
            AnalyticsIndexMgmtOperationType.DropDataset.value: management_operation,
            AnalyticsIndexMgmtOperationType.DropDataverse.value: management_operation,
            AnalyticsIndexMgmtOperationType.DropLink.value: management_operation,
            AnalyticsIndexMgmtOperationType.DropIndex.value: management_operation,
            AnalyticsIndexMgmtOperationType.GetAllDatasets.value: management_operation,
            AnalyticsIndexMgmtOperationType.GetAllIndexes.value: management_operation,
            AnalyticsIndexMgmtOperationType.GetLinks.value: management_operation,
            AnalyticsIndexMgmtOperationType.GetPendingMutations.value: management_operation,
            AnalyticsIndexMgmtOperationType.ReplaceLink.value: management_operation,
        })

    def _load_bucket_mgmt_op_map(self) -> None:
        self._op_map.update(**{
            BucketMgmtOperationType.BucketDescribe.value: management_operation,
            BucketMgmtOperationType.CreateBucket.value: management_operation,
            BucketMgmtOperationType.DropBucket.value: management_operation,
            BucketMgmtOperationType.FlushBucket.value: management_operation,
            BucketMgmtOperationType.GetAllBuckets.value: management_operation,
            BucketMgmtOperationType.GetBucket.value: management_operation,
            BucketMgmtOperationType.UpdateBucket.value: management_operation,
        })

    def _load_bucket_op_map(self) -> None:
        self._op_map.update(**{
            BucketOperationType.CloseBucket.value: open_or_close_bucket,
            BucketOperationType.OpenBucket.value: open_or_close_bucket,
            BucketOperationType.Ping.value: diagnostics_operation,
        })

    def _load_cluster_op_map(self) -> None:
        self._op_map.update(**{
            ClusterOperationType.CreateConnection.value: create_connection,
            ClusterOperationType.CloseConnection.value: close_connection,
            ClusterOperationType.Diagnostics.value: diagnostics_operation,
            ClusterOperationType.GetClusterInfo.value: management_operation,
            ClusterOperationType.GetConnectionInfo.value: get_connection_info,
            ClusterOperationType.Ping.value: diagnostics_operation,
            ClusterOperationType.UpdateCredentials.value: update_credentials,
        })

    def _load_collection_mgmt_op_map(self) -> None:
        self._op_map.update(**{
            CollectionMgmtOperationType.CreateCollection.value: management_operation,
            CollectionMgmtOperationType.CreateScope.value: management_operation,
            CollectionMgmtOperationType.DropCollection.value: management_operation,
            CollectionMgmtOperationType.DropScope.value: management_operation,
            CollectionMgmtOperationType.GetAllScopes.value: management_operation,
            CollectionMgmtOperationType.UpdateCollection.value: management_operation,
        })

    def _load_eventing_function_mgmt_op_map(self) -> None:
        self._op_map.update(**{
            EventingFunctionMgmtOperationType.DeployFunction.value: management_operation,
            EventingFunctionMgmtOperationType.DropFunction.value: management_operation,
            EventingFunctionMgmtOperationType.FunctionsStatus.value: management_operation,
            EventingFunctionMgmtOperationType.GetAllFunctions.value: management_operation,
            EventingFunctionMgmtOperationType.GetFunction.value: management_operation,
            EventingFunctionMgmtOperationType.PauseFunction.value: management_operation,
            EventingFunctionMgmtOperationType.ResumeFunction.value: management_operation,
            EventingFunctionMgmtOperationType.UndeployFunction.value: management_operation,
            EventingFunctionMgmtOperationType.UpsertFunction.value: management_operation,
        })

    def _load_key_value_multi_op_map(self) -> None:
        self._op_map.update(**{
            KeyValueMultiOperationType.AppendMulti.value: binary_multi_operation,
            KeyValueMultiOperationType.DecrementMulti.value: binary_multi_operation,
            KeyValueMultiOperationType.ExistsMulti.value: kv_multi_operation,
            KeyValueMultiOperationType.GetMulti.value: kv_multi_operation,
            KeyValueMultiOperationType.GetAllReplicasMulti.value: kv_multi_operation,
            KeyValueMultiOperationType.GetAndLockMulti.value: kv_multi_operation,
            KeyValueMultiOperationType.GetAnyReplicaMulti.value: kv_multi_operation,
            KeyValueMultiOperationType.IncrementMulti.value: binary_multi_operation,
            KeyValueMultiOperationType.InsertMulti.value: kv_multi_operation,
            KeyValueMultiOperationType.PrependMulti.value: binary_multi_operation,
            KeyValueMultiOperationType.RemoveMulti.value: kv_multi_operation,
            KeyValueMultiOperationType.ReplaceMulti.value: kv_multi_operation,
            KeyValueMultiOperationType.TouchMulti.value: kv_multi_operation,
            KeyValueMultiOperationType.UnlockMulti.value: kv_multi_operation,
            KeyValueMultiOperationType.UpsertMulti.value: kv_multi_operation
        })

    def _load_key_value_op_map(self) -> None:
        self._op_map.update(**{
            KeyValueOperationType.Append.value: binary_operation,
            KeyValueOperationType.Decrement.value: binary_operation,
            KeyValueOperationType.Exists.value: kv_operation,
            KeyValueOperationType.Get.value: kv_operation,
            KeyValueOperationType.GetAllReplicas.value: kv_operation,
            KeyValueOperationType.GetAndLock.value: kv_operation,
            KeyValueOperationType.GetAndTouch.value: kv_operation,
            KeyValueOperationType.GetAnyReplica.value: kv_operation,
            KeyValueOperationType.GetProject.value: kv_operation,
            KeyValueOperationType.Increment.value: binary_operation,
            KeyValueOperationType.Insert.value: kv_operation,
            KeyValueOperationType.LookupIn.value: subdoc_operation,
            KeyValueOperationType.LookupInAllReplicas.value: subdoc_operation,
            KeyValueOperationType.LookupInAnyReplica.value: subdoc_operation,
            KeyValueOperationType.MutateIn.value: subdoc_operation,
            KeyValueOperationType.Prepend.value: binary_operation,
            KeyValueOperationType.Remove.value: kv_operation,
            KeyValueOperationType.Replace.value: kv_operation,
            KeyValueOperationType.Touch.value: kv_operation,
            KeyValueOperationType.Unlock.value: kv_operation,
            KeyValueOperationType.Upsert.value: kv_operation
        })

    def _load_query_index_mgmt_op_map(self) -> None:
        self._op_map.update(**{
            QueryIndexMgmtOperationType.CreateIndex.value: management_operation,
            QueryIndexMgmtOperationType.CreatePrimaryIndex.value: management_operation,
            QueryIndexMgmtOperationType.DropIndex.value: management_operation,
            QueryIndexMgmtOperationType.DropPrimaryIndex.value: management_operation,
            QueryIndexMgmtOperationType.GetAllIndexes.value: management_operation,
            QueryIndexMgmtOperationType.BuildDeferredIndexes.value: management_operation,
        })

    def _load_search_index_mgmt_op_map(self) -> None:
        self._op_map.update(**{
            SearchIndexMgmtOperationType.AllowQuerying.value: management_operation,
            SearchIndexMgmtOperationType.AnalyzeDocument.value: management_operation,
            SearchIndexMgmtOperationType.DisallowQuerying.value: management_operation,
            SearchIndexMgmtOperationType.DropIndex.value: management_operation,
            SearchIndexMgmtOperationType.FreezePlan.value: management_operation,
            SearchIndexMgmtOperationType.GetAllIndexes.value: management_operation,
            SearchIndexMgmtOperationType.GetAllIndexStats.value: management_operation,
            SearchIndexMgmtOperationType.GetIndex.value: management_operation,
            SearchIndexMgmtOperationType.GetIndexedDocumentsCount.value: management_operation,
            SearchIndexMgmtOperationType.GetIndexStats.value: management_operation,
            SearchIndexMgmtOperationType.PauseIngest.value: management_operation,
            SearchIndexMgmtOperationType.ResumeIngest.value: management_operation,
            SearchIndexMgmtOperationType.UnfreezePlan.value: management_operation,
            SearchIndexMgmtOperationType.UpsertIndex.value: management_operation,
        })

    def _load_user_mgmt_op_map(self) -> None:
        self._op_map.update(**{
            UserMgmtOperationType.ChangePassword.value: management_operation,
            UserMgmtOperationType.DropGroup.value: management_operation,
            UserMgmtOperationType.DropUser.value: management_operation,
            UserMgmtOperationType.GetAllGroups.value: management_operation,
            UserMgmtOperationType.GetAllUsers.value: management_operation,
            UserMgmtOperationType.GetGroup.value: management_operation,
            UserMgmtOperationType.GetRoles.value: management_operation,
            UserMgmtOperationType.GetUser.value: management_operation,
            UserMgmtOperationType.UpsertGroup.value: management_operation,
            UserMgmtOperationType.UpsertUser.value: management_operation,
        })

    def _load_view_index_mgmt_op_map(self) -> None:
        self._op_map.update(**{
            ViewIndexMgmtOperationType.DropDesignDocument.value: management_operation,
            ViewIndexMgmtOperationType.GetAllDesignDocuments.value: management_operation,
            ViewIndexMgmtOperationType.GetDesignDocument.value: management_operation,
            ViewIndexMgmtOperationType.PublishDesignDocument.value: management_operation,
            ViewIndexMgmtOperationType.UpsertDesignDocument.value: management_operation,
        })
