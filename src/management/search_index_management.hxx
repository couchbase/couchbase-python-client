#pragma once

#include "../client.hxx"

#include <couchbase/operations/management/search.hxx>

class SearchIndexManagementOperations
{
  public:
    enum OperationType {
        UNKNOWN,
        UPSERT_INDEX,
        GET_INDEX,
        DROP_INDEX,
        GET_INDEX_DOCUMENT_COUNT,
        GET_ALL_INDEXES,
        GET_INDEX_STATS,
        GET_ALL_STATS,
        FREEZE_PLAN,
        CONTROL_INGEST,
        ANALYZE_DOCUMENT,
        CONTROL_QUERY
    };

    SearchIndexManagementOperations()
      : SearchIndexManagementOperations{ UNKNOWN }
    {
    }
    constexpr SearchIndexManagementOperations(SearchIndexManagementOperations::OperationType op)
      : operation{ op }
    {
    }

    operator OperationType() const
    {
        return operation;
    }
    // lets prevent the implicit promotion of bool to int
    explicit operator bool() = delete;
    constexpr bool operator==(SearchIndexManagementOperations op) const
    {
        return operation == op.operation;
    }
    constexpr bool operator!=(SearchIndexManagementOperations op) const
    {
        return operation != op.operation;
    }

    static const char* ALL_OPERATIONS(void)
    {
        const char* ops = "UPSERT_INDEX "
                          "GET_INDEX "
                          "DROP_INDEX "
                          "GET_INDEX_DOCUMENT_COUNT "
                          "GET_ALL_INDEXES "
                          "GET_INDEX_STATS "
                          "GET_ALL_STATS "
                          "FREEZE_PLAN "
                          "CONTROL_INGEST "
                          "ANALYZE_DOCUMENT "
                          "CONTROL_QUERY";

        return ops;
    }

  private:
    OperationType operation;
};

struct search_index_mgmt_options {
    PyObject* op_args;
    SearchIndexManagementOperations::OperationType op_type = SearchIndexManagementOperations::UNKNOWN;
    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::management_timeout;
};

PyObject*
handle_search_index_mgmt_op(connection* conn, struct search_index_mgmt_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback);

void
add_search_index_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class);
