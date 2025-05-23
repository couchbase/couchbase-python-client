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

#include "client.hxx"
#include <cstdlib>
#include <structmember.h>

#include <core/meta/version.hxx>

#include "analytics.hxx"
#include "binary_ops.hxx"
#include "connection.hxx"
#include "diagnostics.hxx"
#include "exceptions.hxx"
#include "kv_ops.hxx"
#include "kv_range_scan.hxx"
#include "logger.hxx"
#include "n1ql.hxx"
#include "result.hxx"
#include "search.hxx"
#include "subdoc_ops.hxx"
#include "views.hxx"

#include "management/analytics_management.hxx"
#include "management/bucket_management.hxx"
#include "management/collection_management.hxx"
#include "management/eventing_function_management.hxx"
#include "management/management.hxx"
#include "management/query_index_management.hxx"
#include "management/search_index_management.hxx"
#include "management/user_management.hxx"
#include "management/view_index_management.hxx"

#include "transactions/transactions.hxx"

void
add_ops_enum(PyObject* pyObj_module)
{
  PyObject* pyObj_enum_module = PyImport_ImportModule("enum");
  if (!pyObj_enum_module) {
    return;
  }
  PyObject* pyObj_enum_class = PyObject_GetAttrString(pyObj_enum_module, "Enum");

  PyObject* pyObj_enum_values = PyUnicode_FromString(Operations::ALL_OPERATIONS());
  PyObject* pyObj_enum_name = PyUnicode_FromString("Operations");
  // PyTuple_Pack returns new reference, need to Py_DECREF values provided
  PyObject* pyObj_args = PyTuple_Pack(2, pyObj_enum_name, pyObj_enum_values);
  Py_DECREF(pyObj_enum_name);
  Py_DECREF(pyObj_enum_values);

  PyObject* pyObj_kwargs = PyDict_New();
  PyObject_SetItem(
    pyObj_kwargs, PyUnicode_FromString("module"), PyModule_GetNameObject(pyObj_module));
  PyObject* pyObj_operations = PyObject_Call(pyObj_enum_class, pyObj_args, pyObj_kwargs);
  Py_DECREF(pyObj_args);
  Py_DECREF(pyObj_kwargs);

  if (PyModule_AddObject(pyObj_module, "operations", pyObj_operations) < 0) {
    // only need to Py_DECREF on failure to add when using PyModule_AddObject()
    Py_XDECREF(pyObj_operations);
    return;
  }

  add_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
  add_cluster_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
  add_bucket_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
  add_collection_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
  add_user_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
  add_query_index_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
  add_analytics_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
  add_search_index_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
  add_view_index_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
  add_eventing_function_mgmt_ops_enum(pyObj_module, pyObj_enum_class);
}

void
add_constants(PyObject* module)
{
  if (PyModule_AddIntConstant(module, "FMT_JSON", PYCBC_FMT_JSON) < 0) {
    Py_XDECREF(module);
    return;
  }
  if (PyModule_AddIntConstant(module, "FMT_BYTES", PYCBC_FMT_BYTES) < 0) {
    Py_XDECREF(module);
    return;
  }
  if (PyModule_AddIntConstant(module, "FMT_UTF8", PYCBC_FMT_UTF8) < 0) {
    Py_XDECREF(module);
    return;
  }
  if (PyModule_AddIntConstant(module, "FMT_PICKLE", PYCBC_FMT_PICKLE) < 0) {
    Py_XDECREF(module);
    return;
  }
  if (PyModule_AddIntConstant(module, "FMT_LEGACY_MASK", PYCBC_FMT_LEGACY_MASK) < 0) {
    Py_XDECREF(module);
    return;
  }
  if (PyModule_AddIntConstant(module, "FMT_COMMON_MASK", PYCBC_FMT_COMMON_MASK) < 0) {
    Py_XDECREF(module);
    return;
  }
  auto cxxcbc_metadata = couchbase::core::meta::sdk_build_info_json();
  if (PyModule_AddStringConstant(module, "CXXCBC_METADATA", cxxcbc_metadata.c_str())) {
    Py_XDECREF(module);
    return;
  }
}

static PyObject*
binary_operation(PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* res = handle_binary_op(self, args, kwargs);
  if (res == nullptr && PyErr_Occurred() == nullptr) {
    pycbc_set_python_exception(
      PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Unable to perform binary operation.");
  }
  return res;
}

static PyObject*
binary_multi_operation(PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* res = handle_binary_multi_op(self, args, kwargs);
  if (res == nullptr && PyErr_Occurred() == nullptr) {
    pycbc_set_python_exception(PycbcError::UnsuccessfulOperation,
                               __FILE__,
                               __LINE__,
                               "Unable to perform binary multi operation.");
  }
  return res;
}

static PyObject*
kv_operation(PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* res = handle_kv_op(self, args, kwargs);
  if (res == nullptr && PyErr_Occurred() == nullptr) {
    pycbc_set_python_exception(
      PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Unable to perform KV operation.");
  }
  return res;
}

static PyObject*
kv_multi_operation(PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* res = handle_kv_multi_op(self, args, kwargs);
  if (res == nullptr && PyErr_Occurred() == nullptr) {
    pycbc_set_python_exception(PycbcError::UnsuccessfulOperation,
                               __FILE__,
                               __LINE__,
                               "Unable to perform KV multi operation.");
  }
  return res;
}

static PyObject*
kv_range_scan_operation(PyObject* self, PyObject* args, PyObject* kwargs)
{
  scan_iterator* res = handle_kv_range_scan_op(self, args, kwargs);
  if (res == nullptr && PyErr_Occurred() == nullptr) {
    pycbc_set_python_exception(PycbcError::UnsuccessfulOperation,
                               __FILE__,
                               __LINE__,
                               "Unable to perform KV range scan operation.");
  }
  return reinterpret_cast<PyObject*>(res);
}

static PyObject*
subdoc_operation(PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* res = handle_subdoc_op(self, args, kwargs);
  if (res == nullptr && PyErr_Occurred() == nullptr) {
    pycbc_set_python_exception(PycbcError::UnsuccessfulOperation,
                               __FILE__,
                               __LINE__,
                               "Unable to perform subdocument operation.");
  }
  return res;
}

static PyObject*
diagnostics_operation(PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* res = handle_diagnostics_op(self, args, kwargs);
  if (res == nullptr && PyErr_Occurred() == nullptr) {
    pycbc_set_python_exception(PycbcError::UnsuccessfulOperation,
                               __FILE__,
                               __LINE__,
                               "Unable to perform diagnostics operation.");
  }
  return res;
}

static PyObject*
n1ql_query(PyObject* self, PyObject* args, PyObject* kwargs)
{
  streamed_result* res = handle_n1ql_query(self, args, kwargs);
  if (res == nullptr && PyErr_Occurred() == nullptr) {
    pycbc_set_python_exception(
      PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Unable to perform N1QL query.");
  }
  return reinterpret_cast<PyObject*>(res);
}

static PyObject*
analytics_query(PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyGILState_STATE state = PyGILState_Ensure();
  streamed_result* res = handle_analytics_query(self, args, kwargs);
  if (!res) {
    PyErr_SetString(PyExc_Exception, "Unable to perform analytics query.");
  }
  PyGILState_Release(state);
  return reinterpret_cast<PyObject*>(res);
}

static PyObject*
search_query(PyObject* self, PyObject* args, PyObject* kwargs)
{
  streamed_result* res = handle_search_query(self, args, kwargs);
  if (!res) {
    PyErr_SetString(PyExc_Exception, "Unable to perform search query.");
  }
  return reinterpret_cast<PyObject*>(res);
}

static PyObject*
view_query(PyObject* self, PyObject* args, PyObject* kwargs)
{
  streamed_result* res = handle_view_query(self, args, kwargs);
  if (!res) {
    PyErr_SetString(PyExc_Exception, "Unable to perform view query.");
  }
  return reinterpret_cast<PyObject*>(res);
}

static PyObject*
cluster_info(PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* res = handle_mgmt_op(self, args, kwargs);
  if (res == nullptr && PyErr_Occurred() == nullptr) {
    pycbc_set_python_exception(PycbcError::UnsuccessfulOperation,
                               __FILE__,
                               __LINE__,
                               "Unable to perform cluster info operation.");
  }
  return res;
}

static PyObject*
management_operation(PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* res = handle_mgmt_op(self, args, kwargs);
  if (res == nullptr && PyErr_Occurred() == nullptr) {
    pycbc_set_python_exception(PycbcError::UnsuccessfulOperation,
                               __FILE__,
                               __LINE__,
                               "Unable to perform management operation.");
  }
  return res;
}

static PyObject*
open_or_close_bucket(PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* res = handle_open_or_close_bucket(self, args, kwargs);
  if (res == nullptr && PyErr_Occurred() == nullptr) {
    pycbc_set_python_exception(
      PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Unable to open/close bucket.");
  }
  return res;
}

static PyObject*
create_connection(PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* res = handle_create_connection(self, args, kwargs);
  if (res == nullptr && PyErr_Occurred() == nullptr) {
    pycbc_set_python_exception(
      PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Unable to create connection.");
  }
  return res;
}

static PyObject*
get_connection_information(PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* res = get_connection_info(self, args, kwargs);
  if (res == nullptr && PyErr_Occurred() == nullptr) {
    pycbc_set_python_exception(PycbcError::UnsuccessfulOperation,
                               __FILE__,
                               __LINE__,
                               "Unable to get connection information.");
  }
  return res;
}

static PyObject*
close_connection(PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* res = handle_close_connection(self, args, kwargs);
  if (res == nullptr && PyErr_Occurred() != nullptr) {
    pycbc_set_python_exception(
      PycbcError::UnsuccessfulOperation, __FILE__, __LINE__, "Unable to close connection.");
  }
  return res;
}

static PyObject*
shutdown_logger(PyObject* self, PyObject* Py_UNUSED(ignored))
{
  Py_BEGIN_ALLOW_THREADS couchbase::core::logger::shutdown();
  Py_END_ALLOW_THREADS Py_RETURN_NONE;
}

static struct PyMethodDef methods[] = {
  { "create_connection",
    (PyCFunction)create_connection,
    METH_VARARGS | METH_KEYWORDS,
    "Create connection object" },
  { "get_connection_info",
    (PyCFunction)get_connection_information,
    METH_VARARGS | METH_KEYWORDS,
    "Get connection options" },
  { "open_or_close_bucket",
    (PyCFunction)open_or_close_bucket,
    METH_VARARGS | METH_KEYWORDS,
    "Open or close a bucket" },
  { "close_connection",
    (PyCFunction)close_connection,
    METH_VARARGS | METH_KEYWORDS,
    "Close a connection" },
  { "kv_operation",
    (PyCFunction)kv_operation,
    METH_VARARGS | METH_KEYWORDS,
    "Handle all key/value operations" },
  { "kv_multi_operation",
    (PyCFunction)kv_multi_operation,
    METH_VARARGS | METH_KEYWORDS,
    "Handle all key/value multi operations" },
  { "kv_range_scan_operation",
    (PyCFunction)kv_range_scan_operation,
    METH_VARARGS | METH_KEYWORDS,
    "Handle all key/value range scan operations" },
  { "subdoc_operation",
    (PyCFunction)subdoc_operation,
    METH_VARARGS | METH_KEYWORDS,
    "Handle all subdoc operations" },
  { "binary_operation",
    (PyCFunction)binary_operation,
    METH_VARARGS | METH_KEYWORDS,
    "Handle all binary operations" },
  { "binary_multi_operation",
    (PyCFunction)binary_multi_operation,
    METH_VARARGS | METH_KEYWORDS,
    "Handle all binary multi operations" },
  { "diagnostics_operation",
    (PyCFunction)diagnostics_operation,
    METH_VARARGS | METH_KEYWORDS,
    "Handle all diagnostics operations" },
  { "n1ql_query", (PyCFunction)n1ql_query, METH_VARARGS | METH_KEYWORDS, "Execute N1QL Query" },
  { "analytics_query",
    (PyCFunction)analytics_query,
    METH_VARARGS | METH_KEYWORDS,
    "Execute analytics Query" },
  { "search_query",
    (PyCFunction)search_query,
    METH_VARARGS | METH_KEYWORDS,
    "Execute search Query" },
  { "view_query",
    (PyCFunction)view_query,
    METH_VARARGS | METH_KEYWORDS,
    "Execute map reduce views Query" },
  { "cluster_info",
    (PyCFunction)cluster_info,
    METH_VARARGS | METH_KEYWORDS,
    "Get information on the connected cluster" },
  { "management_operation",
    (PyCFunction)management_operation,
    METH_VARARGS | METH_KEYWORDS,
    "Handle all management operations" },
  { "create_transactions",
    (PyCFunction)pycbc_txns::create_transactions,
    METH_VARARGS | METH_KEYWORDS,
    "Create a transactions object" },
  { "create_transaction_context",
    (PyCFunction)pycbc_txns::create_transaction_context,
    METH_VARARGS | METH_KEYWORDS,
    "Create a transaction context object" },
  { "create_new_attempt_context",
    (PyCFunction)pycbc_txns::create_new_attempt_context,
    METH_VARARGS | METH_KEYWORDS,
    "Create a new attempt context object" },
  { "transaction_op",
    (PyCFunction)pycbc_txns::transaction_op,
    METH_VARARGS | METH_KEYWORDS,
    "perform a transaction kv operation" },
  { "transaction_get_multi_op",
    (PyCFunction)pycbc_txns::transaction_get_multi_op,
    METH_VARARGS | METH_KEYWORDS,
    "perform a transaction kv get_multi operation" },
  { "transaction_query_op",
    (PyCFunction)pycbc_txns::transaction_query_op,
    METH_VARARGS | METH_KEYWORDS,
    "perform a transactional query" },
  { "transaction_commit",
    (PyCFunction)pycbc_txns::transaction_commit,
    METH_VARARGS | METH_KEYWORDS,
    "Commit a transaction" },
  { "transaction_rollback",
    (PyCFunction)pycbc_txns::transaction_rollback,
    METH_VARARGS | METH_KEYWORDS,
    "Rollback a transaction" },
  { "destroy_transactions",
    (PyCFunction)pycbc_txns::destroy_transactions,
    METH_VARARGS | METH_KEYWORDS,
    "shut down transactions object" },
  { "shutdown_logger", (PyCFunction)shutdown_logger, METH_NOARGS, "shut down C++ logger" },
  { nullptr, nullptr, 0, nullptr }
};

static PyModuleDef
init_pycbc_core_module()
{
  PyModuleDef mod = {};
  mod.m_base = PyModuleDef_HEAD_INIT;
  mod.m_name = "pycbc_core";
  mod.m_doc = "Python interface to couchbase-client-cxx";
  mod.m_size = -1;
  mod.m_methods = methods;
  return mod;
}

static PyModuleDef pycbc_core_module = init_pycbc_core_module();

PyMODINIT_FUNC
PyInit_pycbc_core(void)
{
  Py_Initialize();
  PyObject* m = PyModule_Create(&pycbc_core_module);
  if (m == nullptr) {
    return nullptr;
  }

  if (add_result_objects(m) == nullptr) {
    Py_DECREF(m);
    return nullptr;
  }

  if (add_exception_objects(m) == nullptr) {
    Py_DECREF(m);
    return nullptr;
  }

  if (add_logger_objects(m) == nullptr) {
    Py_DECREF(m);
    return nullptr;
  }

  add_ops_enum(m);
  add_constants(m);
  return pycbc_txns::add_transaction_objects(m);
}
