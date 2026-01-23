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

from couchbase.logic.operation_types import (BucketMgmtOperationType,
                                             BucketOperationType,
                                             ClusterOperationType,
                                             CollectionMgmtOperationType,
                                             KeyValueMultiOperationType,
                                             KeyValueOperationType,
                                             QueryIndexMgmtOperationType)
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


class BindingMap:

    def __init__(self) -> None:
        self._op_map = {}
        self._load_op_map()

    def _load_op_map(self) -> None:
        self._load_bucket_mgmt_op_map()
        self._load_bucket_op_map()
        self._load_cluster_op_map()
        self._load_collection_mgmt_op_map()
        self._load_key_value_multi_op_map()
        self._load_key_value_op_map()
        self._load_query_index_mgmt_op_map()

    @property
    def op_map(self) -> Dict[str, Callable[..., Any]]:
        return self._op_map

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
