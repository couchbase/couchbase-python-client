#pragma once

#include "../client.hxx"

#include <couchbase/operations/management/query.hxx>

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
    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::management_timeout;
};

PyObject*
handle_query_index_mgmt_op(connection* conn, struct query_index_mgmt_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback);

void
add_query_index_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class);
