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

#include "result.hxx"
#include "pytype_utils.hxx"

namespace pycbc
{

// ======================================================================
// pycbc_result type implementation
// ======================================================================

static PyObject*
pycbc_result__new__(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
  pycbc_result* self = (pycbc_result*)type->tp_alloc(type, 0);
  if (self != nullptr) {
    self->raw_result = nullptr;
  }
  return (PyObject*)self;
}

static int
pycbc_result__init__(pycbc_result* self, PyObject* args, PyObject* kwargs)
{
  self->raw_result = PyDict_New();
  if (self->raw_result == nullptr) {
    return -1;
  }
  Py_INCREF(Py_None);
  self->core_span = Py_None;
  Py_INCREF(Py_None);
  self->start_time = Py_None;
  Py_INCREF(Py_None);
  self->end_time = Py_None;
  return 0;
}

static PyObject*
pycbc_result__str__(pycbc_result* self)
{
  const char* format_string = "pycbc_result:{value=%S}";
  return PyUnicode_FromFormat(format_string, self->raw_result);
}

static void
pycbc_result__dealloc__(pycbc_result* self)
{
  Py_XDECREF(self->raw_result);
  Py_XDECREF(self->core_span);
  Py_XDECREF(self->start_time);
  Py_XDECREF(self->end_time);
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyMemberDef pycbc_result_members[] = {
  { "raw_result",
    T_OBJECT_EX,
    offsetof(pycbc_result, raw_result),
    READONLY,
    PyDoc_STR("Internal dictionary containing operation result data") },
  { "core_span",
    T_OBJECT_EX,
    offsetof(pycbc_result, core_span),
    READONLY,
    PyDoc_STR("Internal dictionary C++ core span information") },
  { "start_time",
    T_OBJECT_EX,
    offsetof(pycbc_result, start_time),
    READONLY,
    PyDoc_STR("Internal dictionary op start time") },
  { "end_time",
    T_OBJECT_EX,
    offsetof(pycbc_result, end_time),
    READONLY,
    PyDoc_STR("Internal dictionary op end time") },
  { nullptr }
};

static PyTypeObject pycbc_result_type = {
  PyVarObject_HEAD_INIT(nullptr, 0) "pycbc_core.pycbc_result", // tp_name
  sizeof(pycbc_result),                                        // tp_basicsize
  0,                                                           // tp_itemsize
  (destructor)pycbc_result__dealloc__,                         // tp_dealloc
  0,                                                           // tp_vectorcall_offset
  nullptr,                                                     // tp_getattr
  nullptr,                                                     // tp_setattr
  nullptr,                                                     // tp_as_async
  (reprfunc)pycbc_result__str__,                               // tp_repr
  nullptr,                                                     // tp_as_number
  nullptr,                                                     // tp_as_sequence
  nullptr,                                                     // tp_as_mapping
  nullptr,                                                     // tp_hash
  nullptr,                                                     // tp_call
  nullptr,                                                     // tp_str
  nullptr,                                                     // tp_getattro
  nullptr,                                                     // tp_setattro
  nullptr,                                                     // tp_as_buffer
  Py_TPFLAGS_DEFAULT,                                          // tp_flags
  PyDoc_STR("pycbc result object"),                            // tp_doc
  nullptr,                                                     // tp_traverse
  nullptr,                                                     // tp_clear
  nullptr,                                                     // tp_richcompare
  0,                                                           // tp_weaklistoffset
  nullptr,                                                     // tp_iter
  nullptr,                                                     // tp_iternext
  nullptr,                                                     // tp_methods
  pycbc_result_members,                                        // tp_members
  nullptr,                                                     // tp_getset
  nullptr,                                                     // tp_base
  nullptr,                                                     // tp_dict
  nullptr,                                                     // tp_descr_get
  nullptr,                                                     // tp_descr_set
  0,                                                           // tp_dictoffset
  (initproc)pycbc_result__init__,                              // tp_init
  nullptr,                                                     // tp_alloc
  pycbc_result__new__,                                         // tp_new
};

PyObject*
create_pycbc_result(PyObject* raw_result_dict)
{
  PyObject* obj = PyObject_CallObject((PyObject*)&pycbc_result_type, nullptr);
  if (obj != nullptr && raw_result_dict != nullptr) {
    pycbc_result* res = reinterpret_cast<pycbc_result*>(obj);
    Py_DECREF(res->raw_result); // Release empty dict from init
    Py_INCREF(raw_result_dict); // Take ownership of provided dict
    res->raw_result = raw_result_dict;
  }
  return obj;
}

// ======================================================================
// pycbc_streamed_result type implementation
// ======================================================================

static PyObject*
pycbc_streamed_result__new__(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
  pycbc_streamed_result* self = (pycbc_streamed_result*)type->tp_alloc(type, 0);
  if (self != nullptr) {
    self->ec = std::error_code();
    self->rows = std::make_shared<rows_queue<PyObject*>>();
    self->timeout_ms = std::chrono::milliseconds{ 0 };
    Py_INCREF(Py_None);
    self->core_span = Py_None;
    Py_INCREF(Py_None);
    self->start_time = Py_None;
    Py_INCREF(Py_None);
    self->end_time = Py_None;
  }
  return (PyObject*)self;
}

static void
pycbc_streamed_result__dealloc__(pycbc_streamed_result* self)
{
  Py_XDECREF(self->core_span);
  Py_XDECREF(self->start_time);
  Py_XDECREF(self->end_time);
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject*
pycbc_streamed_result__iter__(PyObject* self)
{
  Py_INCREF(self);
  return self;
}

static PyObject*
pycbc_streamed_result__iternext__(PyObject* self)
{
  pycbc_streamed_result* s_res = reinterpret_cast<pycbc_streamed_result*>(self);
  PyObject* row;
  {
    Py_BEGIN_ALLOW_THREADS row = s_res->rows->get(s_res->timeout_ms);
    Py_END_ALLOW_THREADS
  }
  return row; // Returns NULL (when row is Py_None) to signal StopIteration
}

static PyMemberDef pycbc_streamed_result_members[] = {
  { "core_span",
    T_OBJECT_EX,
    offsetof(pycbc_streamed_result, core_span),
    0,
    PyDoc_STR("Get the streamed results core_span, if it exists.") },
  { "start_time",
    T_OBJECT_EX,
    offsetof(pycbc_streamed_result, start_time),
    READONLY,
    PyDoc_STR("Internal dictionary op start time") },
  { "end_time",
    T_OBJECT_EX,
    offsetof(pycbc_streamed_result, end_time),
    READONLY,
    PyDoc_STR("Internal dictionary op end time") },
  { nullptr } // Sentinel
};

static PyTypeObject pycbc_streamed_result_type = {
  PyVarObject_HEAD_INIT(nullptr, 0) "pycbc_core.pycbc_streamed_result", // tp_name
  sizeof(pycbc_streamed_result),                                        // tp_basicsize
  0,                                                                    // tp_itemsize
  (destructor)pycbc_streamed_result__dealloc__,                         // tp_dealloc
  0,                                                                    // tp_vectorcall_offset
  nullptr,                                                              // tp_getattr
  nullptr,                                                              // tp_setattr
  nullptr,                                                              // tp_as_async
  nullptr,                                                              // tp_repr
  nullptr,                                                              // tp_as_number
  nullptr,                                                              // tp_as_sequence
  nullptr,                                                              // tp_as_mapping
  nullptr,                                                              // tp_hash
  nullptr,                                                              // tp_call
  nullptr,                                                              // tp_str
  nullptr,                                                              // tp_getattro
  nullptr,                                                              // tp_setattro
  nullptr,                                                              // tp_as_buffer
  Py_TPFLAGS_DEFAULT,                                                   // tp_flags
  PyDoc_STR("pycbc streamed result"),                                   // tp_doc
  nullptr,                                                              // tp_traverse
  nullptr,                                                              // tp_clear
  nullptr,                                                              // tp_richcompare
  0,                                                                    // tp_weaklistoffset
  pycbc_streamed_result__iter__,                                        // tp_iter
  pycbc_streamed_result__iternext__,                                    // tp_iternext
  nullptr,                                                              // tp_methods
  pycbc_streamed_result_members,                                        // tp_members
  nullptr,                                                              // tp_getset
  nullptr,                                                              // tp_base
  nullptr,                                                              // tp_dict
  nullptr,                                                              // tp_descr_get
  nullptr,                                                              // tp_descr_set
  0,                                                                    // tp_dictoffset
  nullptr,                      // tp_init (no custom init needed)
  nullptr,                      // tp_alloc
  pycbc_streamed_result__new__, // tp_new
};

pycbc_streamed_result*
create_pycbc_streamed_result(std::chrono::milliseconds timeout_ms)
{
  PyObject* pyObj_res = PyObject_CallObject((PyObject*)&pycbc_streamed_result_type, nullptr);
  pycbc_streamed_result* s_res = reinterpret_cast<pycbc_streamed_result*>(pyObj_res);
  s_res->timeout_ms = timeout_ms;
  return s_res;
}

// ======================================================================
// pycbc_scan_iterator type implementation
// ======================================================================

static PyObject*
pycbc_scan_iterator__new__(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
  pycbc_scan_iterator* self = (pycbc_scan_iterator*)type->tp_alloc(type, 0);
  if (self != nullptr) {
    self->scan_result = nullptr;
  }
  return (PyObject*)self;
}

static void
pycbc_scan_iterator__dealloc__(pycbc_scan_iterator* self)
{
  if (self->scan_result) {
    self->scan_result->cancel();
    self->scan_result.reset();
  }
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject*
pycbc_scan_iterator__iter__(PyObject* self)
{
  Py_INCREF(self);
  return self;
}

static PyObject*
pycbc_scan_iterator__iternext__(PyObject* self)
{
  pycbc_scan_iterator* scan_iter = reinterpret_cast<pycbc_scan_iterator*>(self);

  tl::expected<couchbase::core::range_scan_item, std::error_code> result;
  {
    Py_BEGIN_ALLOW_THREADS result = scan_iter->scan_result->next();
    Py_END_ALLOW_THREADS
  }

  if (!result.has_value()) {
    return build_exception(
      result.error(), __FILE__, __LINE__, "Error retrieving next scan result item.");
  }
  PyObject* pyObj = create_pycbc_result();
  if (pyObj == nullptr) {
    return nullptr;
  }
  pycbc_result* res = reinterpret_cast<pycbc_result*>(pyObj);
  add_field<couchbase::core::range_scan_item>(res->raw_result, "scan_item", result.value());
  return reinterpret_cast<PyObject*>(res);
}

static PyObject*
pycbc_scan_iterator__cancel__(pycbc_scan_iterator* self, PyObject* args)
{
  if (self->scan_result) {
    self->scan_result->cancel();
  }
  Py_RETURN_NONE;
}

static PyObject*
pycbc_scan_iterator__is_cancelled__(pycbc_scan_iterator* self, PyObject* args)
{
  if (self->scan_result && self->scan_result->is_cancelled()) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}

static PyMethodDef pycbc_scan_iterator_methods[] = {
  { "cancel_scan",
    (PyCFunction)pycbc_scan_iterator__cancel__,
    METH_NOARGS,
    PyDoc_STR("Cancel the scan operation") },
  { "is_cancelled",
    (PyCFunction)pycbc_scan_iterator__is_cancelled__,
    METH_NOARGS,
    PyDoc_STR("Check if the scan has been cancelled") },
  { nullptr } // Sentinel
};

static PyTypeObject pycbc_scan_iterator_type = {
  PyVarObject_HEAD_INIT(nullptr, 0) "pycbc_core.pycbc_scan_iterator", // tp_name
  sizeof(pycbc_scan_iterator),                                        // tp_basicsize
  0,                                                                  // tp_itemsize
  (destructor)pycbc_scan_iterator__dealloc__,                         // tp_dealloc
  0,                                                                  // tp_vectorcall_offset
  nullptr,                                                            // tp_getattr
  nullptr,                                                            // tp_setattr
  nullptr,                                                            // tp_as_async
  nullptr,                                                            // tp_repr
  nullptr,                                                            // tp_as_number
  nullptr,                                                            // tp_as_sequence
  nullptr,                                                            // tp_as_mapping
  nullptr,                                                            // tp_hash
  nullptr,                                                            // tp_call
  nullptr,                                                            // tp_str
  nullptr,                                                            // tp_getattro
  nullptr,                                                            // tp_setattro
  nullptr,                                                            // tp_as_buffer
  Py_TPFLAGS_DEFAULT,                                                 // tp_flags
  PyDoc_STR("pycbc range scan iterator"),                             // tp_doc
  nullptr,                                                            // tp_traverse
  nullptr,                                                            // tp_clear
  nullptr,                                                            // tp_richcompare
  0,                                                                  // tp_weaklistoffset
  pycbc_scan_iterator__iter__,                                        // tp_iter
  pycbc_scan_iterator__iternext__,                                    // tp_iternext
  pycbc_scan_iterator_methods,                                        // tp_methods
  nullptr,                                                            // tp_members
  nullptr,                                                            // tp_getset
  nullptr,                                                            // tp_base
  nullptr,                                                            // tp_dict
  nullptr,                                                            // tp_descr_get
  nullptr,                                                            // tp_descr_set
  0,                                                                  // tp_dictoffset
  nullptr,                    // tp_init (no custom init needed)
  nullptr,                    // tp_alloc
  pycbc_scan_iterator__new__, // tp_new
};

pycbc_scan_iterator*
create_pycbc_scan_iterator(couchbase::core::scan_result result)
{
  PyObject* pyObj_iter = PyObject_CallObject((PyObject*)&pycbc_scan_iterator_type, nullptr);
  if (!pyObj_iter) {
    return nullptr;
  }

  pycbc_scan_iterator* iter = reinterpret_cast<pycbc_scan_iterator*>(pyObj_iter);
  iter->scan_result = std::make_shared<couchbase::core::scan_result>(std::move(result));

  return iter;
}

int
add_result_objects(PyObject* module)
{
  if (register_pytype(module, &pycbc_result_type, "pycbc_result") < 0) {
    return -1;
  }

  if (register_pytype(module, &pycbc_streamed_result_type, "pycbc_streamed_result") < 0) {
    return -1;
  }

  if (register_pytype(module, &pycbc_scan_iterator_type, "pycbc_scan_iterator") < 0) {
    return -1;
  }

  return 0;
}

} // namespace pycbc
