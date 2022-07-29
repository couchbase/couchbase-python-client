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

class BucketManagementOperations
{
  public:
    enum OperationType { UNKNOWN, CREATE_BUCKET, UPDATE_BUCKET, DROP_BUCKET, GET_BUCKET, GET_ALL_BUCKETS, FLUSH_BUCKET, BUCKET_DESCRIBE };

    BucketManagementOperations()
      : BucketManagementOperations{ UNKNOWN }
    {
    }
    constexpr BucketManagementOperations(BucketManagementOperations::OperationType op)
      : operation{ op }
    {
    }

    operator OperationType() const
    {
        return operation;
    }
    // lets prevent the implicit promotion of bool to int
    explicit operator bool() = delete;
    constexpr bool operator==(BucketManagementOperations op) const
    {
        return operation == op.operation;
    }
    constexpr bool operator!=(BucketManagementOperations op) const
    {
        return operation != op.operation;
    }

    static const char* ALL_OPERATIONS(void)
    {
        const char* ops = "CREATE_BUCKET "
                          "UPDATE_BUCKET "
                          "DROP_BUCKET "
                          "GET_BUCKET "
                          "GET_ALL_BUCKETS "
                          "FLUSH_BUCKET "
                          "BUCKET_DESCRIBE";

        return ops;
    }

  private:
    OperationType operation;
};

struct bucket_mgmt_options {
    PyObject* op_args;
    BucketManagementOperations::OperationType op_type = BucketManagementOperations::UNKNOWN;
    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::management_timeout;
};

PyObject*
handle_bucket_mgmt_op(connection* conn, struct bucket_mgmt_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback);

void
add_bucket_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class);
