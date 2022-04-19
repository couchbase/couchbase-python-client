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
    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::management_timeout;
};

PyObject*
handle_bucket_mgmt_op(connection* conn, struct bucket_mgmt_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback);

void
add_bucket_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class);
