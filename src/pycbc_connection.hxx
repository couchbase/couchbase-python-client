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

#include "Python.h"
#include "connection.hxx"
#include "exceptions.hxx"
#include <memory>
#include <string>

namespace pycbc
{

struct pycbc_connection {
  PyObject_HEAD std::unique_ptr<Connection> conn;
};

extern PyTypeObject pycbc_connection_type;

int
add_connection_type(PyObject* module);

inline bool
validate_connection_and_args(pycbc_connection* self,
                             PyObject* args,
                             PyObject* kwargs,
                             const char* op_name,
                             bool skip_args_validation = false)
{
  if (self->conn == nullptr) {
    PyErr_SetString(PyExc_RuntimeError, "Connection not initialized");
    return false;
  }

  if (skip_args_validation) {
    return true;
  }

  // Validate args is empty (all params should be in kwargs)
  if (args != nullptr && PyTuple_Size(args) > 0) {
    std::string err_msg = std::string(op_name) + " takes no positional arguments";
    raise_invalid_argument(err_msg.c_str(), __FILE__, __LINE__);
    return false;
  }

  if (!kwargs || !PyDict_Check(kwargs)) {
    std::string err_msg = std::string(op_name) + " requires keyword arguments";
    raise_invalid_argument(err_msg.c_str(), __FILE__, __LINE__);
    return false;
  }

  return true;
}

template<typename Request>
static PyObject*
handle_multi_kv_op(pycbc_connection* self, PyObject* args, PyObject* kwargs, const char* op_name)
{
  if (self->conn == nullptr) {
    PyErr_SetString(PyExc_RuntimeError, "Connection not initialized");
    return nullptr;
  }

  PyObject* pyObj_bucket = nullptr;
  PyObject* pyObj_scope = nullptr;
  PyObject* pyObj_collection = nullptr;
  PyObject* pyObj_doc_list = nullptr;
  PyObject* pyObj_op_args = nullptr;
  PyObject* pyObj_per_key_args = nullptr;

  static const char* kw_list[] = { "bucket",  "scope",        "collection", "doc_list",
                                   "op_args", "per_key_args", nullptr };

  if (!PyArg_ParseTupleAndKeywords(args,
                                   kwargs,
                                   "O!O!O!O!O!|O!",
                                   const_cast<char**>(kw_list),
                                   &PyUnicode_Type,
                                   &pyObj_bucket,
                                   &PyUnicode_Type,
                                   &pyObj_scope,
                                   &PyUnicode_Type,
                                   &pyObj_collection,
                                   &PyList_Type,
                                   &pyObj_doc_list,
                                   &PyDict_Type,
                                   &pyObj_op_args,
                                   &PyDict_Type,
                                   &pyObj_per_key_args)) {
    return nullptr;
  }

  return self->conn->execute_multi_op<Request>(
    pyObj_doc_list, pyObj_op_args, pyObj_per_key_args, pyObj_bucket, pyObj_scope, pyObj_collection);
}

} // namespace pycbc
