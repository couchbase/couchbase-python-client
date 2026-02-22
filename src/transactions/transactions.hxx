/*
 *   Copyright 2016-2026. Couchbase, Inc.
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

#include "../exceptions.hxx"
#include "../pycbc_connection.hxx"
#include <core/operations/document_query.hxx>
#include <core/transactions.hxx>
#include <core/transactions/internal/transaction_context.hxx>

namespace cbtxns = couchbase::transactions;
namespace cbcoretxns = couchbase::core::transactions;

namespace pycbc
{
namespace txns
{

// @TODO: PYCBC-1425, is this the right approach?
using pycbc_txn_complete_callback =
  std::function<void(std::optional<cbcoretxns::transaction_exception>,
                     std::optional<cbtxns::transaction_result>)>;

class TxOperations
{
public:
  enum TxOperationType {
    UNKNOWN,
    GET,
    GET_REPLICA_FROM_PREFERRED_SERVER_GROUP,
    GET_MULTI,
    GET_MULTI_REPLICAS_FROM_PREFERRED_SERVER_GROUP,
    REPLACE,
    INSERT,
    REMOVE,
    QUERY
  };

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
                      "GET_REPLICA_FROM_PREFERRED_SERVER_GROUP "
                      "GET_MULTI "
                      "GET_MULTI_REPLICAS_FROM_PREFERRED_SERVER_GROUP "
                      "REPLACE "
                      "INSERT "
                      "REMOVE "
                      "QUERY";

    return ops;
  }

private:
  TxOperationType operation_;
};

enum TxnExceptionType {
  TRANSACTION_FAILED,
  TRANSACTION_COMMIT_AMBIGUOUS,
  TRANSACTION_EXPIRED,
  TRANSACTION_OPERATION_FAILED,
  FEATURE_NOT_AVAILABLE,
  QUERY_PARSING_FAILURE,
  DOCUMENT_EXISTS,
  DOCUMENT_NOT_FOUND,
  DOCUMENT_UNRETRIEVABLE,
  COUCHBASE_ERROR
};

struct transaction_config {
  PyObject_HEAD cbtxns::transactions_config* cfg;
};

struct transaction_options {
  PyObject_HEAD cbtxns::transaction_options* opts;
};

static PyObject*
transaction_config__new__(PyTypeObject*, PyObject*, PyObject*);
static void
transaction_config__dealloc__(pycbc::txns::transaction_config*);
static PyObject*
transaction_config__to_dict__(PyObject*);

static PyObject*
transaction_options__new__(PyTypeObject*, PyObject*, PyObject*);
static void
transaction_options__dealloc__(pycbc::txns::transaction_options*);
static PyObject*
transaction_options__to_dict__(PyObject*);
static PyObject*
transaction_options__str__(PyObject*);

struct transactions {
  std::shared_ptr<cbcoretxns::transactions> txns;

  explicit transactions(std::shared_ptr<cbcoretxns::transactions> transactions)
    : txns(std::move(transactions))
  {
  }
};

struct transaction_context {
  std::shared_ptr<cbcoretxns::transaction_context> ctx;

  explicit transaction_context(std::shared_ptr<cbcoretxns::transaction_context> ctx)
    : ctx(std::move(ctx))
  {
  }
};

struct transaction_get_result {
  PyObject_HEAD std::unique_ptr<cbcoretxns::transaction_get_result> res;
};

struct transaction_get_multi_result {
  PyObject_HEAD PyObject* content;
};

struct transaction_query_options {
  PyObject_HEAD cbtxns::transaction_query_options* opts;
};

static PyObject*
transaction_query_options__new__(PyTypeObject*, PyObject*, PyObject*);
static void
transaction_query_options__dealloc__(pycbc::txns::transaction_query_options*);
static PyObject*
transaction_query_options__to_dict__(PyObject*);

static PyObject*
transaction_get_result__new__(PyTypeObject*, PyObject*, PyObject*);
static PyObject*
transaction_get_result__str__(pycbc::txns::transaction_get_result* result);
static void
transaction_get_result__dealloc__(pycbc::txns::transaction_get_result* result);
static PyObject*
transaction_get_result__get__(pycbc::txns::transaction_get_result* result, PyObject* args);

static PyObject*
transaction_get_multi_result__new__(PyTypeObject*, PyObject*, PyObject*);
static void
transaction_get_multi_result__dealloc__(pycbc::txns::transaction_get_multi_result* result);

PyObject*
add_transaction_objects(PyObject* module);

static void
dealloc_transactions(PyObject* txns);
static void
dealloc_transaction_context(PyObject* ctx);

PyObject*
create_transactions(PyObject*, PyObject*, PyObject*);
PyObject*
create_transaction_context(PyObject*, PyObject*, PyObject*);
PyObject*
create_new_attempt_context(PyObject*, PyObject*, PyObject*);
PyObject*
transaction_op(PyObject*, PyObject*, PyObject*);
PyObject*
transaction_get_multi_op(PyObject*, PyObject*, PyObject*);
PyObject*
transaction_query_op(PyObject*, PyObject*, PyObject*);
PyObject*
transaction_commit(PyObject*, PyObject*, PyObject*);
PyObject*
transaction_rollback(PyObject*, PyObject*, PyObject*);
PyObject*
destroy_transactions(PyObject*, PyObject*, PyObject*);
void
add_transactions(PyObject* mod);

} // namespace txns
} // namespace pycbc
