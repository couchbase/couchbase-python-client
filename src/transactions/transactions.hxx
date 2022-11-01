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
#include <core/transactions.hxx>
#include <core/operations/document_query.hxx>

namespace tx = couchbase::transactions;
namespace tx_core = couchbase::core::transactions;

namespace pycbc_txns
{

// @TODO: PYCBC-1425, is this the right approach?
using pycbc_txn_complete_callback =
  std::function<void(std::optional<tx_core::transaction_exception>, std::optional<tx::transaction_result>)>;

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
    PyObject_HEAD tx::transactions_config* cfg;
};

struct transaction_options {
    PyObject_HEAD tx::transaction_options* opts;
};

static PyObject*
transaction_config__new__(PyTypeObject*, PyObject*, PyObject*);
static void
transaction_config__dealloc__(pycbc_txns::transaction_config*);
static PyObject*
transaction_config__to_dict__(PyObject*);

static PyObject*
transaction_options__new__(PyTypeObject*, PyObject*, PyObject*);
static void
transaction_options__dealloc__(pycbc_txns::transaction_options*);
static PyObject*
transaction_options__to_dict__(PyObject*);
static PyObject*
transaction_options__str__(PyObject*);

struct transactions {
    tx_core::transactions* txns;

    explicit transactions(PyObject* pyObj_conn, const tx::transactions_config& cfg)
      : txns(nullptr)
    {
        connection* c = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
        txns = new tx_core::transactions(c->cluster_, cfg);
    }
};

struct attempt_context {
    tx_core::async_attempt_context& ctx;

    explicit attempt_context(tx_core::async_attempt_context& ctx)
      : ctx(ctx)
    {
    }
};

struct transaction_get_result {
    PyObject_HEAD tx_core::transaction_get_result* res;
};

struct transaction_query_options {
    PyObject_HEAD tx::transaction_query_options* opts;
};

static PyObject*
transaction_query_options__new__(PyTypeObject*, PyObject*, PyObject*);
static void
transaction_query_options__dealloc__(pycbc_txns::transaction_query_options*);
static PyObject*
transaction_query_options__to_dict__(PyObject*);

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
