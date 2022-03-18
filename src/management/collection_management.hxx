#pragma once

#include "../client.hxx"

#include <couchbase/operations/management/collections.hxx>

class CollectionManagementOperations
{
  public:
    enum OperationType { UNKNOWN, CREATE_SCOPE, DROP_SCOPE, GET_ALL_SCOPES, CREATE_COLLECTION, DROP_COLLECTION };

    CollectionManagementOperations()
      : CollectionManagementOperations{ UNKNOWN }
    {
    }
    constexpr CollectionManagementOperations(CollectionManagementOperations::OperationType op)
      : operation{ op }
    {
    }

    operator OperationType() const
    {
        return operation;
    }
    // lets prevent the implicit promotion of bool to int
    explicit operator bool() = delete;
    constexpr bool operator==(CollectionManagementOperations op) const
    {
        return operation == op.operation;
    }
    constexpr bool operator!=(CollectionManagementOperations op) const
    {
        return operation != op.operation;
    }

    static const char* ALL_OPERATIONS(void)
    {
        const char* ops = "CREATE_SCOPE "
                          "DROP_SCOPE "
                          "GET_ALL_SCOPES "
                          "CREATE_COLLECTION "
                          "DROP_COLLECTION";

        return ops;
    }

  private:
    OperationType operation;
};

struct collection_mgmt_options {
    PyObject* op_args;
    CollectionManagementOperations::OperationType op_type = CollectionManagementOperations::UNKNOWN;
    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::management_timeout;
};

PyObject*
handle_collection_mgmt_op(connection* conn, struct collection_mgmt_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback);

void
add_collection_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class);
