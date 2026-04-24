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

#include "Python.h"
#include "constants.hxx"
#include "exceptions.hxx"
#include "hdr_histogram.hxx"
#include "logger.hxx"
#include "pycbc_connection.hxx"
#include "pycbc_dict_keys.hxx"
#include "pycbc_kv_request.hxx"
#include "result.hxx"
#include "transactions/transactions.hxx"
#include <core/logger/logger.hxx>
#include <core/meta/version.hxx>

namespace pycbc
{

static int
add_constants(PyObject* module)
{
  if (PyModule_AddIntConstant(module, "FMT_JSON", PYCBC_FMT_JSON) < 0) {
    return -1;
  }
  if (PyModule_AddIntConstant(module, "FMT_BYTES", PYCBC_FMT_BYTES) < 0) {
    return -1;
  }
  if (PyModule_AddIntConstant(module, "FMT_UTF8", PYCBC_FMT_UTF8) < 0) {
    return -1;
  }
  if (PyModule_AddIntConstant(module, "FMT_PICKLE", PYCBC_FMT_PICKLE) < 0) {
    return -1;
  }
  if (PyModule_AddIntConstant(module, "FMT_LEGACY_MASK", PYCBC_FMT_LEGACY_MASK) < 0) {
    return -1;
  }
  if (PyModule_AddIntConstant(module, "FMT_COMMON_MASK", PYCBC_FMT_COMMON_MASK) < 0) {
    return -1;
  }

  // Add C++ SDK metadata
  auto cxxcbc_metadata = couchbase::core::meta::sdk_build_info_json();
  if (PyModule_AddStringConstant(module, "CXXCBC_METADATA", cxxcbc_metadata.c_str()) < 0) {
    return -1;
  }

  return 0;
}

// Module-level function to shutdown C++ logger
static PyObject*
shutdown_logger(PyObject* self, PyObject* Py_UNUSED(ignored))
{
  Py_BEGIN_ALLOW_THREADS couchbase::core::logger::shutdown();
  Py_END_ALLOW_THREADS Py_RETURN_NONE;
}

} // namespace pycbc

static PyMethodDef pycbc_core_methods[] = { { "shutdown_logger",
                                              (PyCFunction)pycbc::shutdown_logger,
                                              METH_NOARGS,
                                              PyDoc_STR("Shutdown C++ core logger") },
                                            { "create_transactions",
                                              (PyCFunction)pycbc::txns::create_transactions,
                                              METH_VARARGS | METH_KEYWORDS,
                                              PyDoc_STR("Create transactions") },
                                            { "create_transaction_context",
                                              (PyCFunction)pycbc::txns::create_transaction_context,
                                              METH_VARARGS | METH_KEYWORDS,
                                              PyDoc_STR("Create transaction context") },
                                            { "create_new_attempt_context",
                                              (PyCFunction)pycbc::txns::create_new_attempt_context,
                                              METH_VARARGS | METH_KEYWORDS,
                                              PyDoc_STR("Create new attempt context") },
                                            { "transaction_op",
                                              (PyCFunction)pycbc::txns::transaction_op,
                                              METH_VARARGS | METH_KEYWORDS,
                                              PyDoc_STR("Transaction operation") },
                                            { "transaction_get_multi_op",
                                              (PyCFunction)pycbc::txns::transaction_get_multi_op,
                                              METH_VARARGS | METH_KEYWORDS,
                                              PyDoc_STR("Transaction get multi operation") },
                                            { "transaction_query_op",
                                              (PyCFunction)pycbc::txns::transaction_query_op,
                                              METH_VARARGS | METH_KEYWORDS,
                                              PyDoc_STR("Transaction query operation") },
                                            { "transaction_commit",
                                              (PyCFunction)pycbc::txns::transaction_commit,
                                              METH_VARARGS | METH_KEYWORDS,
                                              PyDoc_STR("Transaction commit") },
                                            { "transaction_rollback",
                                              (PyCFunction)pycbc::txns::transaction_rollback,
                                              METH_VARARGS | METH_KEYWORDS,
                                              PyDoc_STR("Transaction rollback") },
                                            { "destroy_transactions",
                                              (PyCFunction)pycbc::txns::destroy_transactions,
                                              METH_VARARGS | METH_KEYWORDS,
                                              PyDoc_STR("Destroy transactions") },
                                            { nullptr, nullptr, 0, nullptr } };

static PyModuleDef pycbc_core_module = {
  PyModuleDef_HEAD_INIT,
  "_core",                               /* m_name */
  PyDoc_STR("Python SDK C++ extension"), /* m_doc */
  -1,                                    /* m_size */
  pycbc_core_methods,                    /* m_methods */
  nullptr,                               /* m_slots */
  nullptr,                               /* m_traverse */
  nullptr,                               /* m_clear */
  nullptr                                /* m_free */
};

PyMODINIT_FUNC
PyInit__core(void)
{
  Py_Initialize();

  PyObject* module = PyModule_Create(&pycbc_core_module);
  if (module == nullptr) {
    return nullptr;
  }

  if (pycbc::add_connection_type(module) < 0) {
    Py_DECREF(module);
    return nullptr;
  }

  if (pycbc::add_kv_request_type(module) < 0) {
    Py_DECREF(module);
    return nullptr;
  }

  if (pycbc::add_result_objects(module) < 0) {
    Py_DECREF(module);
    return nullptr;
  }

  if (pycbc::add_constants(module) < 0) {
    Py_DECREF(module);
    return nullptr;
  }

  if (pycbc::add_exception_objects(module) == nullptr) {
    Py_DECREF(module);
    return nullptr;
  }

  if (pycbc::add_logger_objects(module) == nullptr) {
    Py_DECREF(module);
    return nullptr;
  }

  if (pycbc::txns::add_transaction_objects(module) == nullptr) {
    Py_DECREF(module);
    return nullptr;
  }

  if (pycbc::add_histogram_objects(module) < 0) {
    Py_DECREF(module);
    return nullptr;
  }

  // Cache exception classes for efficient access
  pycbc::cache_exception_classes();

  init_pycbc_dict_keys();

  return module;
}
