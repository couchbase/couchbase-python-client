/*
 *   Copyright 2016-2022. Couchbase, Inc.
 *   All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#pragma once

#include "../client.hxx"
#include <core/operations/management/cluster_describe.hxx>
#include <core/operations/management/cluster_developer_preview_enable.hxx>
#include "bucket_management.hxx"
#include "collection_management.hxx"
#include "user_management.hxx"
#include "query_index_management.hxx"
#include "analytics_management.hxx"
#include "search_index_management.hxx"
#include "view_index_management.hxx"
#include "eventing_function_management.hxx"

class ManagementOperations
{
  public:
    enum OperationType { UNKNOWN, CLUSTER, BUCKET, COLLECTION, QUERY_INDEX, SEARCH_INDEX, USER, ANALYTICS, VIEW_INDEX, EVENTING_FUNCTION };

    ManagementOperations()
      : ManagementOperations{ UNKNOWN }
    {
    }
    constexpr ManagementOperations(OperationType op)
      : operation{ op }
    {
    }

    operator OperationType() const
    {
        return operation;
    }
    // lets prevent the implicit promotion of bool to int
    explicit operator bool() = delete;
    constexpr bool operator==(ManagementOperations op) const
    {
        return operation == op.operation;
    }
    constexpr bool operator!=(ManagementOperations op) const
    {
        return operation != op.operation;
    }

    static const char* ALL_OPERATIONS(void)
    {
        const char* ops = "CLUSTER "
                          "BUCKET "
                          "COLLECTION "
                          "QUERY_INDEX "
                          "SEARCH_INDEX "
                          "USER "
                          "ANALYTICS "
                          "VIEW_INDEX "
                          "EVENTING_FUNCTION";

        return ops;
    }

  private:
    OperationType operation;
};

class ClusterManagementOperations
{
  public:
    enum OperationType { UNKNOWN, GET_CLUSTER_INFO, ENABLE_DP };

    ClusterManagementOperations()
      : ClusterManagementOperations{ UNKNOWN }
    {
    }
    constexpr ClusterManagementOperations(OperationType op)
      : operation{ op }
    {
    }

    operator OperationType() const
    {
        return operation;
    }
    // lets prevent the implicit promotion of bool to int
    explicit operator bool() = delete;
    constexpr bool operator==(ClusterManagementOperations op) const
    {
        return operation == op.operation;
    }
    constexpr bool operator!=(ClusterManagementOperations op) const
    {
        return operation != op.operation;
    }

    static const char* ALL_OPERATIONS(void)
    {
        const char* ops = "GET_CLUSTER_INFO "
                          "ENABLE_DP";

        return ops;
    }

  private:
    OperationType operation;
};

PyObject*
handle_mgmt_op(PyObject* self, PyObject* args, PyObject* kwargs);

void
add_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class);

void
add_cluster_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class);
