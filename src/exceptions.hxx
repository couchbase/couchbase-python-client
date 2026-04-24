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
#include "structmember.h"
#include <core/error_context/analytics.hxx>
#include <core/error_context/http.hxx>
#include <core/error_context/key_value_error_context.hxx>
#include <core/error_context/query.hxx>
#include <core/error_context/search.hxx>
#include <core/error_context/subdocument_error_context.hxx>
#include <core/error_context/view.hxx>
#include <couchbase/error_codes.hxx>
#include <string>
#include <system_error>

namespace pycbc
{

/**
 * Exception base type - registered in Python as pycbc_core.exception
 */
struct pycbc_exception {
  PyObject_HEAD std::error_code ec;
  std::string message;
  PyObject* error_context; // Python dict containing error context
  PyObject* exc_info;      // Python dict exception info
  PyObject* inner_exception;
  PyObject* core_span;  // For tracing support
  PyObject* start_time; // For metrics support
  PyObject* end_time;   // For metrics support
};

extern PyTypeObject pycbc_exception_type;

PyObject*
add_exception_objects(PyObject* pyObj_module);

/**
 * Cache frequently-used Python exception classes from couchbase.exceptions module
 * for efficient access. Should be called during module initialization.
 */
void
cache_exception_classes();

/**
 * Get the PyTypeObject for the exception base type.
 */
PyTypeObject*
get_exception_type();

/**
 * Create a new exception base object.
 */
pycbc_exception*
create_pycbc_exception();

/**
 * Build a Python exception from an error_code only (no context).
 *
 * @param ec The error code
 * @param file Source file where error occurred (typically __FILE__)
 * @param line Source line where error occurred (typically __LINE__)
 * @param message Optional custom message (uses ec.message() if nullptr)
 * @return PyObject* pointing to pycbc_exception, or nullptr on failure
 */
PyObject*
build_exception(const std::error_code& ec,
                const char* file = __FILE__,
                int line = __LINE__,
                const char* message = nullptr);

/**
 * Raise InvalidArgumentException with the given message.
 * Sets the Python error indicator and returns nullptr for convenient error propagation.
 *
 * @param message Error message
 * @param file Source file where error occurred (typically __FILE__)
 * @param line Source line where error occurred (typically __LINE__)
 */
PyObject*
raise_invalid_argument(const char* message, const char* file = __FILE__, int line = __LINE__);

/**
 * Raise InvalidArgumentException for missing required field.
 */
PyObject*
raise_required_field_missing(PyObject* interned_key,
                             const char* context,
                             const char* file = __FILE__,
                             int line = __LINE__);

/**
 * Raise InvalidArgumentException for empty required field.
 */
PyObject*
raise_required_field_empty(PyObject* interned_key,
                           const char* context,
                           const char* file = __FILE__,
                           int line = __LINE__);

/**
 * Raise FeatureUnavailableException with the given message.
 * Sets the Python error indicator and returns nullptr for convenient error propagation.
 *
 * @param message Error message
 * @param file Source file where error occurred (typically __FILE__)
 * @param line Source line where error occurred (typically __LINE__)
 */
PyObject*
raise_feature_unavailable(const char* message, const char* file = __FILE__, int line = __LINE__);

/**
 * Raise UnsuccessfulOperationException with the given message.
 * Sets the Python error indicator and returns nullptr for convenient error propagation.
 *
 * @param message Error message
 * @param file Source file where error occurred (typically __FILE__)
 * @param line Source line where error occurred (typically __LINE__)
 */
PyObject*
raise_unsuccessful_operation(const char* message, const char* file = __FILE__, int line = __LINE__);

/**
 * Get the current Python exception as a PyObject* exception.
 * If a Python error is set, fetches and clears it, then returns it as an exception object.
 * If no Python error is set, creates a RuntimeError with the given message.
 *
 * @param default_message Message to use if no Python error is set
 * @param file Source file where error occurred (typically __FILE__)
 * @param line Source line where error occurred (typically __LINE__)
 * @return PyObject* exception object (caller must handle/return this)
 */
PyObject*
get_exception_as_object(const char* default_message = "Unknown error occurred",
                        const char* file = __FILE__,
                        int line = __LINE__);

/**
 * Build a pycbc_exception from the current Python exception.
 * Assumes PyErr_Occurred() is true. Fetches the Python exception and wraps
 * it in a new pycbc_exception, with the original exception accessible
 * via the `inner_exception` attribute.
 *
 * @param default_message Message to use if extracting the message from the Python error fails.
 * @param file Source file where error occurred (typically __FILE__)
 * @param line Source line where error occurred (typically __LINE__)
 * @return PyObject* pointing to a new pycbc_exception, or nullptr on failure
 */
PyObject*
build_pycbc_exception_from_python_exc(const char* default_message = "Unknown error occurred",
                                      const char* file = __FILE__,
                                      int line = __LINE__);

std::string
retry_reason_to_string(couchbase::retry_reason reason);

/**
 * Build exception info dict with file, line, and message information.
 *
 * @param file Source file where error occurred (can be nullptr)
 * @param line Source line where error occurred
 * @param message Error message (can be nullptr)
 * @return PyObject* dict containing exc_info, or nullptr on failure
 */
inline PyObject*
build_exc_info_dict(const char* file, int line, const char* message)
{
  PyObject* exc_info = PyDict_New();
  if (exc_info == nullptr) {
    return nullptr;
  }

  if (file != nullptr) {
    PyObject* pyObj_cinfo = Py_BuildValue("(s,i)", file, line);
    PyDict_SetItemString(exc_info, "cinfo", pyObj_cinfo);
    Py_DECREF(pyObj_cinfo);
  } else {
    PyObject* pyObj_cinfo = Py_BuildValue("(s,i)", "", line);
    PyDict_SetItemString(exc_info, "cinfo", pyObj_cinfo);
    Py_DECREF(pyObj_cinfo);
  }

  if (message != nullptr) {
    PyObject* msg_str = PyUnicode_FromString(message);
    PyDict_SetItemString(exc_info, "message", msg_str);
    Py_DECREF(msg_str);
  }

  return exc_info;
}

template<typename Context>
inline PyObject*
build_base_error_context(const Context& ctx)
{
  PyObject* dict = PyDict_New();
  if (dict == nullptr) {
    return nullptr;
  }

  if (ctx.last_dispatched_to.has_value()) {
    PyObject* pyObj_tmp = PyUnicode_FromString(ctx.last_dispatched_to.value().c_str());
    PyDict_SetItemString(dict, "last_dispatched_to", pyObj_tmp);
    Py_DECREF(pyObj_tmp);
  }

  if (ctx.last_dispatched_from.has_value()) {
    PyObject* pyObj_tmp = PyUnicode_FromString(ctx.last_dispatched_from.value().c_str());
    PyDict_SetItemString(dict, "last_dispatched_from", pyObj_tmp);
    Py_DECREF(pyObj_tmp);
  }

  PyObject* pyObj_tmp = PyLong_FromLong(ctx.retry_attempts);
  PyDict_SetItemString(dict, "retry_attempts", pyObj_tmp);
  Py_DECREF(pyObj_tmp);

  PyObject* retry_reasons = PySet_New(nullptr);
  for (const auto& rr : ctx.retry_reasons) {
    auto reason = retry_reason_to_string(rr);
    PyObject* reason_str = PyUnicode_FromString(reason.c_str());
    PySet_Add(retry_reasons, reason_str);
    Py_DECREF(reason_str);
  }
  Py_ssize_t set_size = PySet_Size(retry_reasons);
  if (set_size > 0) {
    PyDict_SetItemString(dict, "retry_reasons", retry_reasons);
  }
  Py_DECREF(retry_reasons);

  return dict;
}

template<typename Context>
inline PyObject*
build_exception_from_context(const Context& ctx,
                             const char* file = __FILE__,
                             int line = __LINE__,
                             const char* message = nullptr)
{
  if (!ctx.ec()) {
    return nullptr;
  }

  pycbc_exception* base = create_pycbc_exception();
  if (base == nullptr) {
    return nullptr;
  }

  base->ec = ctx.ec();
  base->message = message ? message : ctx.ec().message();
  base->error_context = nullptr; // No context for generic fallback
  base->exc_info = build_exc_info_dict(file, line, message);
  return (PyObject*)base;
}

} // namespace pycbc
