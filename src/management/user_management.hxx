#pragma once

#include "../client.hxx"
#include <couchbase/operations/management/user.hxx>

class UserManagementOperations
{
  public:
    enum OperationType {
        UNKNOWN,
        UPSERT_USER,
        GET_USER,
        GET_ALL_USERS,
        DROP_USER,
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
                          "GET_ROLES "
                          "UPSERT_GROUP "
                          "GET_GROUP "
                          "GET_ALL_GROUPS "
                          "DROP_GROUP";

        return ops;
    }

  private:
    OperationType operation;
};

struct user_mgmt_options {
    PyObject* op_args;
    UserManagementOperations::OperationType op_type = UserManagementOperations::UNKNOWN;
    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::management_timeout;
};

PyObject*
handle_user_mgmt_op(connection* conn, struct user_mgmt_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback);

void
add_user_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class);
