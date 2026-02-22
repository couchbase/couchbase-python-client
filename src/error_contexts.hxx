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
#include "exceptions.hxx"
#include "utils.hxx"
#include <core/error_context/analytics.hxx>
#include <core/error_context/http.hxx>
#include <core/error_context/key_value_error_context.hxx>
#include <core/error_context/query.hxx>
#include <core/error_context/search.hxx>
#include <core/error_context/subdocument_error_context.hxx>
#include <core/error_context/view.hxx>
#include <couchbase/error_codes.hxx>

namespace pycbc
{

// Error context template specializations for converting C++ SDK error contexts
// to Python exception objects.
//
// Separated from exceptions.hxx and utils.hxx to avoid circular dependencies:
//   exceptions.hxx → utils.hxx → error_contexts.hxx

template<typename T>
void
build_kv_error_context(PyObject* error_context, const T& ctx)
{
  add_string_field_if_not_empty(error_context, "key", ctx.id());
  add_string_field_if_not_empty(error_context, "bucket_name", ctx.bucket());
  add_string_field_if_not_empty(error_context, "scope_name", ctx.scope());
  add_string_field_if_not_empty(error_context, "collection_name", ctx.collection());
  add_field(error_context, "opaque", ctx.opaque());
  add_field(error_context, "status_code", ctx.status_code());
}

template<typename T>
void
build_base_http_error_context(PyObject* error_context, const T& ctx)
{
  add_string_field_if_not_empty(error_context, "client_context_id", ctx.client_context_id);
  add_string_field_if_not_empty(error_context, "method", ctx.method);
  add_string_field_if_not_empty(error_context, "path", ctx.path);
  add_field(error_context, "http_status", ctx.http_status);
  add_string_field_if_not_empty(error_context, "http_body", ctx.http_body);
  add_string_field_if_not_empty(error_context, "hostname", ctx.hostname);
  add_field(error_context, "port", ctx.port);
}

// KV contexts use methods with (), HTTP contexts use direct field access
template<typename T>
void
add_base_retry_fields_method(PyObject* error_context, const T& ctx)
{
  add_field(error_context, "last_dispatched_to", ctx.last_dispatched_to());
  add_field(error_context, "last_dispatched_from", ctx.last_dispatched_from());
  add_field(error_context, "retry_attempts", ctx.retry_attempts());
  add_field(error_context, "retry_reasons", ctx.retry_reasons());
}

template<typename T>
void
add_base_retry_fields_direct(PyObject* error_context, const T& ctx)
{
  add_field(error_context, "last_dispatched_to", ctx.last_dispatched_to);
  add_field(error_context, "last_dispatched_from", ctx.last_dispatched_from);
  add_field(error_context, "retry_attempts", ctx.retry_attempts);
  add_field(error_context, "retry_reasons", ctx.retry_reasons);
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::core::key_value_error_context& ctx,
                             const char* file,
                             int line,
                             const char* message)
{
  if (!ctx.ec()) {
    return nullptr;
  }

  PyObject* error_context = PyDict_New();
  if (error_context == nullptr) {
    return nullptr;
  }

  PyObject* pyObj_tmp = PyUnicode_FromString("KeyValueErrorContext");
  PyDict_SetItemString(error_context, "context_type", pyObj_tmp);
  Py_DECREF(pyObj_tmp);

  build_kv_error_context(error_context, ctx);
  add_base_retry_fields_method(error_context, ctx);

  pycbc_exception* exc = create_pycbc_exception();
  if (exc == nullptr) {
    Py_DECREF(error_context);
    return nullptr;
  }

  exc->ec = ctx.ec();
  exc->message = message ? message : ctx.ec().message();
  exc->error_context = error_context;
  exc->exc_info = build_exc_info_dict(file, line, message);

  return (PyObject*)exc;
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::core::subdocument_error_context& ctx,
                             const char* file,
                             int line,
                             const char* message)
{
  if (!ctx.ec()) {
    return nullptr;
  }

  PyObject* error_context = PyDict_New();
  if (error_context == nullptr) {
    return nullptr;
  }

  PyObject* pyObj_tmp = PyUnicode_FromString("SubdocumentErrorContext");
  PyDict_SetItemString(error_context, "context_type", pyObj_tmp);
  Py_DECREF(pyObj_tmp);

  // Subdocument inherits from key_value_error_context
  build_kv_error_context(error_context, ctx);

  add_field(error_context, "first_error_path", ctx.first_error_path());
  add_field(error_context, "first_error_index", ctx.first_error_index());
  add_bool_field(error_context, "deleted", ctx.deleted());

  add_base_retry_fields_method(error_context, ctx);

  pycbc_exception* exc = create_pycbc_exception();
  if (exc == nullptr) {
    Py_DECREF(error_context);
    return nullptr;
  }

  exc->ec = ctx.ec();
  exc->message = message ? message : ctx.ec().message();
  exc->error_context = error_context;
  exc->exc_info = build_exc_info_dict(file, line, message);

  return (PyObject*)exc;
}

// HTTP contexts use .ec field (not .ec() method) and direct field access for retry fields
template<>
inline PyObject*
build_exception_from_context(const couchbase::core::error_context::analytics& ctx,
                             const char* file,
                             int line,
                             const char* message)
{
  if (!ctx.ec) {
    return nullptr;
  }

  PyObject* error_context = PyDict_New();
  if (error_context == nullptr) {
    return nullptr;
  }

  PyObject* pyObj_tmp = PyUnicode_FromString("AnalyticsErrorContext");
  PyDict_SetItemString(error_context, "context_type", pyObj_tmp);
  Py_DECREF(pyObj_tmp);

  add_field(error_context, "first_error_code", ctx.first_error_code);
  add_string_field_if_not_empty(error_context, "first_error_message", ctx.first_error_message);
  add_string_field_if_not_empty(error_context, "statement", ctx.statement);
  add_field(error_context, "parameters", ctx.parameters);

  build_base_http_error_context(error_context, ctx);
  add_base_retry_fields_direct(error_context, ctx);

  pycbc_exception* exc = create_pycbc_exception();
  if (exc == nullptr) {
    Py_DECREF(error_context);
    return nullptr;
  }

  exc->ec = ctx.ec;
  exc->message = message ? message : ctx.ec.message();
  exc->error_context = error_context;
  exc->exc_info = build_exc_info_dict(file, line, message);

  return (PyObject*)exc;
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::core::error_context::search& ctx,
                             const char* file,
                             int line,
                             const char* message)
{
  if (!ctx.ec) {
    return nullptr;
  }

  PyObject* error_context = PyDict_New();
  if (error_context == nullptr) {
    return nullptr;
  }

  PyObject* pyObj_tmp = PyUnicode_FromString("SearchErrorContext");
  PyDict_SetItemString(error_context, "context_type", pyObj_tmp);
  Py_DECREF(pyObj_tmp);

  add_string_field_if_not_empty(error_context, "index_name", ctx.index_name);
  add_string_field_if_not_empty(error_context, "query", ctx.query);
  add_field(error_context, "parameters", ctx.parameters);

  build_base_http_error_context(error_context, ctx);
  add_base_retry_fields_direct(error_context, ctx);

  pycbc_exception* exc = create_pycbc_exception();
  if (exc == nullptr) {
    Py_DECREF(error_context);
    return nullptr;
  }

  exc->ec = ctx.ec;
  exc->message = message ? message : ctx.ec.message();
  exc->error_context = error_context;
  exc->exc_info = build_exc_info_dict(file, line, message);

  return (PyObject*)exc;
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::core::error_context::view& ctx,
                             const char* file,
                             int line,
                             const char* message)
{
  if (!ctx.ec) {
    return nullptr;
  }

  PyObject* error_context = PyDict_New();
  if (error_context == nullptr) {
    return nullptr;
  }

  PyObject* pyObj_tmp = PyUnicode_FromString("ViewErrorContext");
  PyDict_SetItemString(error_context, "context_type", pyObj_tmp);
  Py_DECREF(pyObj_tmp);

  add_string_field_if_not_empty(error_context, "design_document_name", ctx.design_document_name);
  add_string_field_if_not_empty(error_context, "view_name", ctx.view_name);
  add_field(error_context, "query_string", ctx.query_string);

  build_base_http_error_context(error_context, ctx);
  add_base_retry_fields_direct(error_context, ctx);

  pycbc_exception* exc = create_pycbc_exception();
  if (exc == nullptr) {
    Py_DECREF(error_context);
    return nullptr;
  }

  exc->ec = ctx.ec;
  exc->message = message ? message : ctx.ec.message();
  exc->error_context = error_context;
  exc->exc_info = build_exc_info_dict(file, line, message);

  return (PyObject*)exc;
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::core::error_context::query& ctx,
                             const char* file,
                             int line,
                             const char* message)
{
  if (!ctx.ec) {
    return nullptr;
  }

  PyObject* error_context = PyDict_New();
  if (error_context == nullptr) {
    return nullptr;
  }

  PyObject* pyObj_tmp = PyUnicode_FromString("QueryErrorContext");
  PyDict_SetItemString(error_context, "context_type", pyObj_tmp);
  Py_DECREF(pyObj_tmp);

  add_string_field_if_not_empty(error_context, "statement", ctx.statement);
  add_field(error_context, "parameters", ctx.parameters);
  add_string_field_if_not_empty(error_context, "client_context_id", ctx.client_context_id);
  add_field(error_context, "first_error_code", ctx.first_error_code);
  add_string_field_if_not_empty(error_context, "first_error_message", ctx.first_error_message);

  build_base_http_error_context(error_context, ctx);
  add_base_retry_fields_direct(error_context, ctx);

  pycbc_exception* exc = create_pycbc_exception();
  if (exc == nullptr) {
    Py_DECREF(error_context);
    return nullptr;
  }

  exc->ec = ctx.ec;
  exc->message = message ? message : ctx.ec.message();
  exc->error_context = error_context;
  exc->exc_info = build_exc_info_dict(file, line, message);

  return (PyObject*)exc;
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::core::error_context::http& ctx,
                             const char* file,
                             int line,
                             const char* message)
{
  if (!ctx.ec) {
    return nullptr;
  }

  PyObject* error_context = PyDict_New();
  if (error_context == nullptr) {
    return nullptr;
  }

  PyObject* pyObj_tmp = PyUnicode_FromString("HTTPErrorContext");
  PyDict_SetItemString(error_context, "context_type", pyObj_tmp);
  Py_DECREF(pyObj_tmp);

  build_base_http_error_context(error_context, ctx);
  add_base_retry_fields_direct(error_context, ctx);

  pycbc_exception* exc = create_pycbc_exception();
  if (exc == nullptr) {
    Py_DECREF(error_context);
    return nullptr;
  }

  exc->ec = ctx.ec;
  exc->message = message ? message : ctx.ec.message();
  exc->error_context = error_context;
  exc->exc_info = build_exc_info_dict(file, line, message);

  return (PyObject*)exc;
}

template<typename T>
static inline std::size_t
get_cbpp_retries(const T& ctx)
{
  return ctx.retry_attempts;
}

template<>
std::size_t
get_cbpp_retries(const couchbase::core::key_value_error_context& ctx)
{
  return ctx.retry_attempts();
}

template<>
std::size_t
get_cbpp_retries(const couchbase::core::subdocument_error_context& ctx)
{
  return ctx.retry_attempts();
}

} // namespace pycbc
