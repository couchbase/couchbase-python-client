#pragma once

#include "../client.hxx"
#include <couchbase/transactions.hxx>
#include <couchbase/operations/document_query.hxx>

namespace tx = couchbase::transactions;

namespace pycbc_txns
{

class TxOperations
{
  public:
    enum TxOperationType { UNKNOWN, GET, REPLACE, INSERT, REMOVE, QUERY };

    TxOperations()
      : TxOperations{ UNKNOWN }
    {
    }
    constexpr TxOperations(TxOperationType op)
      : operation_{ op }
    {
    }

    operator TxOperationType() const
    {
        return operation_;
    }
    // lets prevent the implicit promotion of bool to int
    explicit operator bool() = delete;
    constexpr bool operator==(TxOperations op) const
    {
        return operation_ == op.operation_;
    }
    constexpr bool operator!=(TxOperations op) const
    {
        return operation_ != op.operation_;
    }

    static const char* ALL_OPERATIONS(void)
    {
        const char* ops = "GET "
                          "REPLACE "
                          "INSERT "
                          "REMOVE "
                          "QUERY";

        return ops;
    }

  private:
    TxOperationType operation_;
};

struct transaction_config {
    PyObject_HEAD tx::transaction_config* cfg;
};

struct per_transaction_config {
    PyObject_HEAD tx::per_transaction_config* cfg;
};

static PyObject*
transaction_config__new__(PyTypeObject*, PyObject*, PyObject*);
static void
transaction_config__dealloc__(pycbc_txns::transaction_config*);
static PyObject*
per_transaction_config__new__(PyTypeObject*, PyObject*, PyObject*);
static void
per_transaction_config__dealloc__(pycbc_txns::per_transaction_config*);
static PyObject*
per_transaction_config__str__(PyObject*);

struct transactions {
    couchbase::transactions::transactions* txns;

    explicit transactions(PyObject* pyObj_conn, tx::transaction_config& cfg)
      : txns(nullptr)
    {
        connection* c = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
        txns = new tx::transactions(*c->cluster_, cfg);
    }
};

struct attempt_context {
    tx::async_attempt_context& ctx;

    explicit attempt_context(tx::async_attempt_context& ctx)
      : ctx(ctx)
    {
    }
};

struct transaction_get_result {
    PyObject_HEAD tx::transaction_get_result* res;
};

struct transaction_query_options {
    PyObject_HEAD tx::transaction_query_options* opts;
};

static PyObject*
transaction_query_options__new__(PyTypeObject*, PyObject*, PyObject*);
static void
transaction_query_options__dealloc__(pycbc_txns::transaction_query_options*);

static PyObject*
transaction_get_result__new__(PyTypeObject*, PyObject*, PyObject*);
static PyObject*
transaction_get_result__str__(pycbc_txns::transaction_get_result* result);
static void
transaction_get_result__dealloc__(pycbc_txns::transaction_get_result* result);
static PyObject*
transaction_get_result__get__(pycbc_txns::transaction_get_result* result, PyObject* args);

PyObject*
add_transaction_objects(PyObject* module);

static void
dealloc_transactions(PyObject* txns);
static void
dealloc_attempt_context(PyObject* ctx);

PyObject*
create_transactions(PyObject*, PyObject*, PyObject*);
PyObject*
run_transactions(PyObject*, PyObject*, PyObject*);
PyObject*
transaction_op(PyObject*, PyObject*, PyObject*);
PyObject*
transaction_query_op(PyObject*, PyObject*, PyObject*);
PyObject*
destroy_transactions(PyObject*, PyObject*, PyObject*);
void
add_transactions(PyObject* mod);
} // namespace pycbc_txns
