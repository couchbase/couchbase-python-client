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

#include <core/operations/management/query.hxx>
#include <couchbase/build_query_index_options.hxx>

class QueryIndexManagementOperations
{
  public:
    enum OperationType { UNKNOWN, CREATE_INDEX, DROP_INDEX, GET_ALL_INDEXES, BUILD_DEFERRED_INDEXES };

    QueryIndexManagementOperations()
      : QueryIndexManagementOperations{ UNKNOWN }
    {
    }
    constexpr QueryIndexManagementOperations(QueryIndexManagementOperations::OperationType op)
      : operation{ op }
    {
    }

    operator OperationType() const
    {
        return operation;
    }
    // lets prevent the implicit promotion of bool to int
    explicit operator bool() = delete;
    constexpr bool operator==(QueryIndexManagementOperations op) const
    {
        return operation == op.operation;
    }
    constexpr bool operator!=(QueryIndexManagementOperations op) const
    {
        return operation != op.operation;
    }

    static const char* ALL_OPERATIONS(void)
    {
        const char* ops = "CREATE_INDEX "
                          "DROP_INDEX "
                          "GET_ALL_INDEXES "
                          "BUILD_DEFERRED_INDEXES";

        return ops;
    }

  private:
    OperationType operation;
};

struct query_index_mgmt_options {
    PyObject* op_args;
    QueryIndexManagementOperations::OperationType op_type = QueryIndexManagementOperations::UNKNOWN;
    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::management_timeout;
};

PyObject*
handle_query_index_mgmt_op(connection* conn, struct query_index_mgmt_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback);

void
add_query_index_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class);
