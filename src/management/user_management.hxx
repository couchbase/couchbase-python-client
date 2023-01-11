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
#include <core/operations/management/user.hxx>

class UserManagementOperations
{
  public:
    enum OperationType {
        UNKNOWN,
        UPSERT_USER,
        GET_USER,
        GET_ALL_USERS,
        DROP_USER,
        CHANGE_PASSWORD,
        GET_ROLES,
        UPSERT_GROUP,
        GET_GROUP,
        GET_ALL_GROUPS,
        DROP_GROUP
    };

    UserManagementOperations()
      : UserManagementOperations{ UNKNOWN }
    {
    }
    constexpr UserManagementOperations(UserManagementOperations::OperationType op)
      : operation{ op }
    {
    }

    operator OperationType() const
    {
        return operation;
    }
    // lets prevent the implicit promotion of bool to int
    explicit operator bool() = delete;
    constexpr bool operator==(UserManagementOperations op) const
    {
        return operation == op.operation;
    }
    constexpr bool operator!=(UserManagementOperations op) const
    {
        return operation != op.operation;
    }

    static const char* ALL_OPERATIONS(void)
    {
        const char* ops = "UPSERT_USER "
                          "GET_USER "
                          "GET_ALL_USERS "
                          "DROP_USER "
                          "CHANGE_PASSWORD "
                          "GET_ROLES "
                          "UPSERT_GROUP "
                          "GET_GROUP "
                          "GET_ALL_GROUPS "
                          "DROP_GROUP ";

        return ops;
    }

  private:
    OperationType operation;
};

struct user_mgmt_options {
    PyObject* op_args;
    UserManagementOperations::OperationType op_type = UserManagementOperations::UNKNOWN;
    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::management_timeout;
};

PyObject*
handle_user_mgmt_op(connection* conn, struct user_mgmt_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback);

void
add_user_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class);
