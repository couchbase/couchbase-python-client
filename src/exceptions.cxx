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

#include "exceptions.hxx"
#include "pytocbpp_defs.hxx"
#include "pytype_utils.hxx"
#include <cstring>

namespace pycbc
{

// Cached Python exception classes
static PyObject* cached_invalid_argument_exc = nullptr;
static PyObject* cached_feature_unavailable_exc = nullptr;
static PyObject* cached_unsuccessful_operation_exc = nullptr;

std::string
retry_reason_to_string(couchbase::retry_reason reason)
{
  switch (reason) {
    case couchbase::retry_reason::socket_not_available:
      return "socket_not_available";
    case couchbase::retry_reason::service_not_available:
      return "service_not_available";
    case couchbase::retry_reason::node_not_available:
      return "node_not_available";
    case couchbase::retry_reason::key_value_not_my_vbucket:
      return "key_value_not_my_vbucket";
    case couchbase::retry_reason::key_value_collection_outdated:
      return "key_value_collection_outdated";
    case couchbase::retry_reason::key_value_error_map_retry_indicated:
      return "key_value_error_map_retry_indicated";
    case couchbase::retry_reason::key_value_locked:
      return "key_value_locked";
    case couchbase::retry_reason::key_value_temporary_failure:
      return "key_value_temporary_failure";
    case couchbase::retry_reason::key_value_sync_write_in_progress:
      return "key_value_sync_write_in_progress";
    case couchbase::retry_reason::key_value_sync_write_re_commit_in_progress:
      return "key_value_sync_write_re_commit_in_progress";
    case couchbase::retry_reason::service_response_code_indicated:
      return "service_response_code_indicated";
    case couchbase::retry_reason::circuit_breaker_open:
      return "circuit_breaker_open";
    case couchbase::retry_reason::query_prepared_statement_failure:
      return "query_prepared_statement_failure";
    case couchbase::retry_reason::query_index_not_found:
      return "query_index_not_found";
    case couchbase::retry_reason::analytics_temporary_failure:
      return "analytics_temporary_failure";
    case couchbase::retry_reason::search_too_many_requests:
      return "search_too_many_requests";
    case couchbase::retry_reason::views_temporary_failure:
      return "views_temporary_failure";
    case couchbase::retry_reason::views_no_active_partition:
      return "views_no_active_partition";
    case couchbase::retry_reason::do_not_retry:
      return "do_not_retry";
    case couchbase::retry_reason::socket_closed_while_in_flight:
      return "socket_closed_while_in_flight";
    case couchbase::retry_reason::unknown:
      return "unknown";
    default:
      return "unknown";
  }
}

static PyObject*
pycbc_exception__err__(pycbc_exception* self, PyObject* Py_UNUSED(ignored))
{
  return PyLong_FromLong(self->ec.value());
}

static PyObject*
pycbc_exception__err_category__(pycbc_exception* self, PyObject* Py_UNUSED(ignored))
{
  return PyUnicode_FromString(self->ec.category().name());
}

static PyObject*
pycbc_exception__strerror__(pycbc_exception* self, PyObject* Py_UNUSED(ignored))
{
  if (!self->message.empty()) {
    return PyUnicode_FromString(self->message.c_str());
  }
  return PyUnicode_FromString(self->ec.message().c_str());
}

static PyObject*
pycbc_exception__error_context__(pycbc_exception* self, PyObject* Py_UNUSED(ignored))
{
  if (self->error_context != nullptr) {
    Py_INCREF(self->error_context);
    return self->error_context;
  }
  Py_RETURN_NONE;
}

static PyObject*
pycbc_exception__info__(pycbc_exception* self, [[maybe_unused]] PyObject* args)
{
  if (self->exc_info) {
    PyObject* pyObj_exc_info = PyDict_Copy(self->exc_info);
    return pyObj_exc_info;
  }
  Py_RETURN_NONE;
}

static void
pycbc_exception__dealloc__(pycbc_exception* self)
{
  Py_XDECREF(self->error_context);
  Py_XDECREF(self->exc_info);
  Py_XDECREF(self->inner_exception);
  Py_XDECREF(self->core_span);
  Py_XDECREF(self->start_time);
  Py_XDECREF(self->end_time);
  self->ec.~error_code();
  self->message.~basic_string();
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject*
pycbc_exception__new__(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
  pycbc_exception* self = (pycbc_exception*)type->tp_alloc(type, 0);
  if (self != nullptr) {
    new (&self->ec) std::error_code();
    new (&self->message) std::string();
    self->error_context = nullptr;
    self->exc_info = nullptr;
    self->inner_exception = nullptr;
    self->core_span = nullptr;
  }
  return (PyObject*)self;
}

static int
pycbc_exception__init__(pycbc_exception* self, PyObject* args, PyObject* kwargs)
{
  if (kwargs != nullptr) {
    PyObject* ec_obj = PyDict_GetItemString(kwargs, "error_code");
    if (ec_obj != nullptr && PyLong_Check(ec_obj)) {
      int ec_value = PyLong_AsLong(ec_obj);
      self->ec = std::error_code(ec_value, std::generic_category());
    }

    PyObject* msg_obj = PyDict_GetItemString(kwargs, "message");
    if (msg_obj != nullptr && PyUnicode_Check(msg_obj)) {
      const char* msg = PyUnicode_AsUTF8(msg_obj);
      if (msg != nullptr) {
        self->message = msg;
      }
    }

    PyObject* ctx_obj = PyDict_GetItemString(kwargs, "error_context");
    if (ctx_obj != nullptr && PyDict_Check(ctx_obj)) {
      Py_INCREF(ctx_obj);
      self->error_context = ctx_obj;
    }
  }
  Py_INCREF(Py_None);
  self->core_span = Py_None;
  Py_INCREF(Py_None);
  self->start_time = Py_None;
  Py_INCREF(Py_None);
  self->end_time = Py_None;

  return 0;
}

static PyMethodDef pycbc_exception_methods[] = {
  { "err", (PyCFunction)pycbc_exception__err__, METH_NOARGS, "Get error code" },
  { "err_category",
    (PyCFunction)pycbc_exception__err_category__,
    METH_NOARGS,
    "Get error category" },
  { "strerror", (PyCFunction)pycbc_exception__strerror__, METH_NOARGS, "Get error message" },
  { "error_context",
    (PyCFunction)pycbc_exception__error_context__,
    METH_NOARGS,
    "Get error context dict" },
  { "error_info", (PyCFunction)pycbc_exception__info__, METH_NOARGS, "Get error info dict" },
  { nullptr, nullptr, 0, nullptr }
};

static PyMemberDef pycbc_exception_members[] = {
  { "core_span",
    T_OBJECT_EX,
    offsetof(pycbc_exception, core_span),
    READONLY,
    PyDoc_STR("Internal dictionary C++ core span information") },
  { "start_time",
    T_OBJECT_EX,
    offsetof(pycbc_exception, start_time),
    READONLY,
    PyDoc_STR("Internal dictionary op start time") },
  { "end_time",
    T_OBJECT_EX,
    offsetof(pycbc_exception, end_time),
    READONLY,
    PyDoc_STR("Internal dictionary op end time") },
  { "inner_exception",
    T_OBJECT_EX,
    offsetof(pycbc_exception, inner_exception),
    READONLY,
    "Inner python exception" },
  { nullptr }
};

PyTypeObject pycbc_exception_type = {
  PyVarObject_HEAD_INIT(nullptr, 0) "pycbc_core.pycbc_exception", // tp_name
  sizeof(pycbc_exception),                                        // tp_basicsize
  0,                                                              // tp_itemsize
  (destructor)pycbc_exception__dealloc__,                         // tp_dealloc
  0,                                                              // tp_vectorcall_offset/tp_print
  nullptr,                                                        // tp_getattr
  nullptr,                                                        // tp_setattr
  nullptr,                                                        // tp_reserved/tp_as_async
  nullptr,                                                        // tp_repr
  nullptr,                                                        // tp_as_number
  nullptr,                                                        // tp_as_sequence
  nullptr,                                                        // tp_as_mapping
  nullptr,                                                        // tp_hash
  nullptr,                                                        // tp_call
  nullptr,                                                        // tp_str
  nullptr,                                                        // tp_getattro
  nullptr,                                                        // tp_setattro
  nullptr,                                                        // tp_as_buffer
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,                       // tp_flags
  PyDoc_STR("pycbc exception object"),                            // tp_doc
  nullptr,                                                        // tp_traverse
  nullptr,                                                        // tp_clear
  nullptr,                                                        // tp_richcompare
  0,                                                              // tp_weaklistoffset
  nullptr,                                                        // tp_iter
  nullptr,                                                        // tp_iternext
  pycbc_exception_methods,                                        // tp_methods
  pycbc_exception_members,                                        // tp_members
  nullptr,                                                        // tp_getset
  nullptr,                                                        // tp_base
  nullptr,                                                        // tp_dict
  nullptr,                                                        // tp_descr_get
  nullptr,                                                        // tp_descr_set
  0,                                                              // tp_dictoffset
  (initproc)pycbc_exception__init__,                              // tp_init
  nullptr,                                                        // tp_alloc
  pycbc_exception__new__,                                         // tp_new
};

PyTypeObject*
get_pycbc_exception_type()
{
  return &pycbc_exception_type;
}

pycbc_exception*
create_pycbc_exception()
{
  PyObject* exc = PyObject_CallObject((PyObject*)get_pycbc_exception_type(), nullptr);
  if (exc == nullptr) {
    // Python error already set by PyObject_CallObject
    return nullptr;
  }
  return reinterpret_cast<pycbc_exception*>(exc);
}

PyObject*
build_exception(const std::error_code& ec, const char* file, int line, const char* message)
{
  pycbc_exception* base = create_pycbc_exception();
  if (base == nullptr) {
    return nullptr;
  }

  base->ec = ec;
  base->message = message ? message : ec.message();
  base->error_context = nullptr; // No context for simple error_code
  base->exc_info = build_exc_info_dict(file, line, message);

  return (PyObject*)base;
}

PyObject*
add_exception_objects(PyObject* pyObj_module)
{
  if (register_pytype(pyObj_module, &pycbc_exception_type, "pycbc_exception") < 0) {
    return nullptr;
  }

  return pyObj_module;
}

void
cache_exception_classes()
{
  // Import couchbase.exceptions module
  PyObject* exceptions_module = PyImport_ImportModule("couchbase.exceptions");
  if (exceptions_module == nullptr) {
    PyErr_Clear();
    return;
  }

  cached_invalid_argument_exc =
    PyObject_GetAttrString(exceptions_module, "InvalidArgumentException");
  if (cached_invalid_argument_exc == nullptr) {
    PyErr_Clear();
  }

  cached_feature_unavailable_exc =
    PyObject_GetAttrString(exceptions_module, "FeatureUnavailableException");
  if (cached_feature_unavailable_exc == nullptr) {
    PyErr_Clear();
  }

  cached_unsuccessful_operation_exc =
    PyObject_GetAttrString(exceptions_module, "UnsuccessfulOperationException");
  if (cached_unsuccessful_operation_exc == nullptr) {
    PyErr_Clear();
  }

  Py_DECREF(exceptions_module);
}

PyObject*
raise_invalid_argument(const char* message, const char* file, int line)
{
  // TODO:  is this possible?
  if (cached_invalid_argument_exc == nullptr) {
    // Fallback to ValueError if exception not cached yet
    PyErr_SetString(PyExc_ValueError, message);
    return nullptr;
  }

  PyObject* args = PyTuple_New(0);
  PyObject* kwargs = PyDict_New();
  PyDict_SetItemString(kwargs, "message", PyUnicode_FromString(message));

  PyObject* exc_info = build_exc_info_dict(file, line, message);
  if (exc_info != nullptr) {
    PyDict_SetItemString(kwargs, "exc_info", exc_info);
    Py_DECREF(exc_info);
  }

  PyObject* exc = PyObject_Call(cached_invalid_argument_exc, args, kwargs);
  Py_DECREF(args);
  Py_DECREF(kwargs);

  if (exc != nullptr) {
    PyErr_SetObject(cached_invalid_argument_exc, exc);
    Py_DECREF(exc);
  }

  return nullptr;
}

PyObject*
raise_feature_unavailable(const char* message, const char* file, int line)
{
  // TODO:  is this possible?
  if (cached_feature_unavailable_exc == nullptr) {
    // Fallback to ValueError if exception not cached yet
    PyErr_SetString(PyExc_ValueError, message);
    return nullptr;
  }

  PyObject* args = PyTuple_New(0);
  PyObject* kwargs = PyDict_New();
  PyDict_SetItemString(kwargs, "message", PyUnicode_FromString(message));

  PyObject* exc_info = build_exc_info_dict(file, line, message);
  if (exc_info != nullptr) {
    PyDict_SetItemString(kwargs, "exc_info", exc_info);
    Py_DECREF(exc_info);
  }

  PyObject* exc = PyObject_Call(cached_feature_unavailable_exc, args, kwargs);
  Py_DECREF(args);
  Py_DECREF(kwargs);

  if (exc != nullptr) {
    PyErr_SetObject(cached_feature_unavailable_exc, exc);
    Py_DECREF(exc);
  }

  return nullptr;
}

PyObject*
raise_unsuccessful_operation(const char* message, const char* file, int line)
{
  // TODO:  is this possible?
  if (cached_unsuccessful_operation_exc == nullptr) {
    // Fallback to ValueError if exception not cached yet
    PyErr_SetString(PyExc_ValueError, message);
    return nullptr;
  }

  PyObject* args = PyTuple_New(0);
  PyObject* kwargs = PyDict_New();
  PyDict_SetItemString(kwargs, "message", PyUnicode_FromString(message));

  PyObject* exc_info = build_exc_info_dict(file, line, message);
  if (exc_info != nullptr) {
    PyDict_SetItemString(kwargs, "exc_info", exc_info);
    Py_DECREF(exc_info);
  }

  PyObject* exc = PyObject_Call(cached_unsuccessful_operation_exc, args, kwargs);
  Py_DECREF(args);
  Py_DECREF(kwargs);

  if (exc != nullptr) {
    PyErr_SetObject(cached_unsuccessful_operation_exc, exc);
    Py_DECREF(exc);
  }

  return nullptr;
}

PyObject*
get_exception_as_object(const char* default_message, const char* file, int line)
{
  if (PyErr_Occurred()) {
    PyObject* exc_type = nullptr;
    PyObject* exc_value = nullptr;
    PyObject* exc_traceback = nullptr;
    PyErr_Fetch(&exc_type, &exc_value, &exc_traceback);

    PyErr_NormalizeException(&exc_type, &exc_value, &exc_traceback);
    if (exc_value != nullptr) {
      Py_XDECREF(exc_type);
      Py_XDECREF(exc_traceback);
      return exc_value;
    }

    if (exc_type != nullptr) {
      PyObject* exc = PyObject_CallObject(exc_type, nullptr);
      Py_DECREF(exc_type);
      Py_XDECREF(exc_traceback);
      if (exc != nullptr) {
        return exc;
      }
    }
    Py_XDECREF(exc_type);
    Py_XDECREF(exc_traceback);
  }

  // No Python error was set, create a RuntimeError with the message
  PyObject* exc = PyObject_CallFunction(PyExc_RuntimeError, "s", default_message);
  if (exc != nullptr) {
    return exc;
  }

  return PyObject_CallFunction(PyExc_RuntimeError, "s", "Unknown error occurred");
}

PyObject*
build_pycbc_exception_from_python_exc(const char* default_message, const char* file, int line)
{
  PyObject* exc_type = nullptr;
  PyObject* exc_value = nullptr;
  PyObject* exc_traceback = nullptr;
  PyErr_Fetch(&exc_type, &exc_value, &exc_traceback);
  PyErr_NormalizeException(&exc_type, &exc_value, &exc_traceback);

  pycbc_exception* pycbc_exc = create_pycbc_exception();
  if (pycbc_exc == nullptr) {
    Py_XDECREF(exc_type);
    Py_XDECREF(exc_value);
    Py_XDECREF(exc_traceback);
    return nullptr;
  }

  // we leave the error_code empty and rely on using the inner_exception

  if (exc_value != nullptr) {
    pycbc_exc->inner_exception = exc_value; // we take ownership of exc_value
    PyObject* exc_str = PyObject_Str(exc_value);
    if (exc_str != nullptr) {
      pycbc_exc->message = PyUnicode_AsUTF8(exc_str);
      Py_DECREF(exc_str);
    } else {
      pycbc_exc->message = default_message;
    }
  } else {
    pycbc_exc->message = default_message;
  }
  Py_XDECREF(exc_type);
  Py_XDECREF(exc_traceback);

  pycbc_exc->exc_info = build_exc_info_dict(file, line, pycbc_exc->message.c_str());

  return reinterpret_cast<PyObject*>(pycbc_exc);
}

} // namespace pycbc
