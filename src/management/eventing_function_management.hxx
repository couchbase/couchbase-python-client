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

#include <core/operations/management/eventing.hxx>
// #include <couchbase/operations/management/eventing_upsert_function.hxx>
// #include <couchbase/operations/management/eventing_deploy_function.hxx>
// #include <couchbase/operations/management/eventing_get_function.hxx>
// #include <couchbase/operations/management/eventing_pause_function.hxx>
// #include <couchbase/operations/management/eventing_resume_function.hxx>
// #include <couchbase/operations/management/eventing_undeploy_function.hxx>
// #include <couchbase/operations/management/eventing_drop_function.hxx>
// #include <couchbase/operations/management/eventing_get_all_functions.hxx>
// #include <couchbase/operations/management/eventing_get_status.hxx>

class EventingFunctionManagementOperations
{
  public:
    enum OperationType {
        UNKNOWN,
        UPSERT_FUNCTION,
        DEPLOY_FUNCTION,
        GET_FUNCTION,
        PAUSE_FUNCTION,
        RESUME_FUNCTION,
        UNDEPLOY_FUNCTION,
        DROP_FUNCTION,
        GET_ALL_FUNCTIONS,
        GET_STATUS
    };

    EventingFunctionManagementOperations()
      : EventingFunctionManagementOperations{ UNKNOWN }
    {
    }
    constexpr EventingFunctionManagementOperations(EventingFunctionManagementOperations::OperationType op)
      : operation{ op }
    {
    }

    operator OperationType() const
    {
        return operation;
    }
    // lets prevent the implicit promotion of bool to int
    explicit operator bool() = delete;
    constexpr bool operator==(EventingFunctionManagementOperations op) const
    {
        return operation == op.operation;
    }
    constexpr bool operator!=(EventingFunctionManagementOperations op) const
    {
        return operation != op.operation;
    }

    static const char* ALL_OPERATIONS(void)
    {
        const char* ops = "UPSERT_FUNCTION "
                          "DEPLOY_FUNCTION "
                          "GET_FUNCTION "
                          "PAUSE_FUNCTION "
                          "RESUME_FUNCTION "
                          "UNDEPLOY_FUNCTION "
                          "DROP_FUNCTION "
                          "GET_ALL_FUNCTIONS "
                          "GET_STATUS";

        return ops;
    }

  private:
    OperationType operation;
};

struct eventing_function_mgmt_options {
    PyObject* op_args;
    EventingFunctionManagementOperations::OperationType op_type = EventingFunctionManagementOperations::UNKNOWN;
    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::management_timeout;
};

PyObject*
handle_eventing_function_mgmt_op(connection* conn,
                                 struct eventing_function_mgmt_options* options,
                                 PyObject* pyObj_callback,
                                 PyObject* pyObj_errback);

void
add_eventing_function_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class);
