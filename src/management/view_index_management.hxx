#pragma once

#include "../client.hxx"

#include <couchbase/operations/management/view.hxx>

class ViewIndexManagementOperations
{
  public:
    enum OperationType { UNKNOWN, UPSERT_INDEX, GET_INDEX, DROP_INDEX, GET_ALL_INDEXES };

    ViewIndexManagementOperations()
      : ViewIndexManagementOperations{ UNKNOWN }
    {
    }
    constexpr ViewIndexManagementOperations(ViewIndexManagementOperations::OperationType op)
      : operation{ op }
    {
    }

    operator OperationType() const
    {
        return operation;
    }
    // lets prevent the implicit promotion of bool to int
    explicit operator bool() = delete;
    constexpr bool operator==(ViewIndexManagementOperations op) const
    {
        return operation == op.operation;
    }
    constexpr bool operator!=(ViewIndexManagementOperations op) const
    {
        return operation != op.operation;
    }

    static const char* ALL_OPERATIONS(void)
    {
        const char* ops = "UPSERT_INDEX "
                          "GET_INDEX "
                          "DROP_INDEX "
                          "GET_ALL_INDEXES";

        return ops;
    }

  private:
    OperationType operation;
};

struct view_index_mgmt_options {
    PyObject* op_args;
    ViewIndexManagementOperations::OperationType op_type = ViewIndexManagementOperations::UNKNOWN;
    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::management_timeout;
};

PyObject*
handle_view_index_mgmt_op(connection* conn, struct view_index_mgmt_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback);

void
add_view_index_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class);
