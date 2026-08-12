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
  pycbc_result* self = (pycbc_result*)PyType_GenericAlloc(type, 0);
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
  self->core_span = Py_NewRef(Py_None);
  self->start_time = Py_NewRef(Py_None);
  self->end_time = Py_NewRef(Py_None);
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
  free_heap_type_instance(reinterpret_cast<PyObject*>(self));
}

static PyMemberDef pycbc_result_members[] = {
  { "raw_result",
    Py_T_OBJECT_EX,
    offsetof(pycbc_result, raw_result),
    Py_READONLY,
    PyDoc_STR("Internal dictionary containing operation result data") },
  { "core_span",
    Py_T_OBJECT_EX,
    offsetof(pycbc_result, core_span),
    Py_READONLY,
    PyDoc_STR("Internal dictionary C++ core span information") },
  { "start_time",
    Py_T_OBJECT_EX,
    offsetof(pycbc_result, start_time),
    Py_READONLY,
    PyDoc_STR("Internal dictionary op start time") },
  { "end_time",
    Py_T_OBJECT_EX,
    offsetof(pycbc_result, end_time),
    Py_READONLY,
    PyDoc_STR("Internal dictionary op end time") },
  { nullptr }
};

static PyType_Slot pycbc_result_slots[] = { { Py_tp_new, (void*)pycbc_result__new__ },
                                            { Py_tp_init, (void*)pycbc_result__init__ },
                                            { Py_tp_dealloc, (void*)pycbc_result__dealloc__ },
                                            { Py_tp_repr, (void*)pycbc_result__str__ },
                                            { Py_tp_members, (void*)pycbc_result_members },
                                            { Py_tp_doc, (void*)PyDoc_STR("pycbc result object") },
                                            { 0, nullptr } };

static PyType_Spec pycbc_result_spec = { "pycbc_core.pycbc_result",
                                         sizeof(pycbc_result),
                                         0,
                                         Py_TPFLAGS_DEFAULT,
                                         pycbc_result_slots };

static PyObject* pycbc_result_type_obj = nullptr;

PyObject*
create_pycbc_result(PyObject* raw_result_dict)
{
  PyObject* obj = PyObject_CallObject(pycbc_result_type_obj, nullptr);
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
  pycbc_streamed_result* self = (pycbc_streamed_result*)PyType_GenericAlloc(type, 0);
  if (self != nullptr) {
    self->ec = std::error_code();
    new (&self->rows) std::shared_ptr<rows_queue<PyObject*>>();
    self->rows = std::make_shared<rows_queue<PyObject*>>();
    self->timeout_ms = std::chrono::milliseconds{ 0 };
    self->core_span = Py_NewRef(Py_None);
    self->start_time = Py_NewRef(Py_None);
    self->end_time = Py_NewRef(Py_None);
  }
  return (PyObject*)self;
}

static void
pycbc_streamed_result__dealloc__(pycbc_streamed_result* self)
{
  Py_XDECREF(self->core_span);
  Py_XDECREF(self->start_time);
  Py_XDECREF(self->end_time);
  self->rows.reset();
  free_heap_type_instance(reinterpret_cast<PyObject*>(self));
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
    Py_T_OBJECT_EX,
    offsetof(pycbc_streamed_result, core_span),
    0,
    PyDoc_STR("Get the streamed results core_span, if it exists.") },
  { "start_time",
    Py_T_OBJECT_EX,
    offsetof(pycbc_streamed_result, start_time),
    Py_READONLY,
    PyDoc_STR("Internal dictionary op start time") },
  { "end_time",
    Py_T_OBJECT_EX,
    offsetof(pycbc_streamed_result, end_time),
    Py_READONLY,
    PyDoc_STR("Internal dictionary op end time") },
  { nullptr } // Sentinel
};

static PyObject*
pycbc_streamed_result__cancel__(pycbc_streamed_result* self, PyObject* args)
{
  if (self->rows) {
    self->rows->cancel();
  }
  Py_RETURN_NONE;
}

static PyObject*
pycbc_streamed_result__is_cancelled__(pycbc_streamed_result* self, PyObject* args)
{
  if (self->rows && self->rows->is_cancelled()) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}

static PyMethodDef pycbc_streamed_result_methods[] = {
  { "cancel",
    (PyCFunction)pycbc_streamed_result__cancel__,
    METH_NOARGS,
    PyDoc_STR("Cancel the streaming operation so a blocked iterator can unwind") },
  { "is_cancelled",
    (PyCFunction)pycbc_streamed_result__is_cancelled__,
    METH_NOARGS,
    PyDoc_STR("Check if the streaming operation has been cancelled") },
  { nullptr } // Sentinel
};

static PyType_Slot pycbc_streamed_result_slots[] = {
  { Py_tp_new, (void*)pycbc_streamed_result__new__ },
  { Py_tp_dealloc, (void*)pycbc_streamed_result__dealloc__ },
  { Py_tp_iter, (void*)pycbc_streamed_result__iter__ },
  { Py_tp_iternext, (void*)pycbc_streamed_result__iternext__ },
  { Py_tp_methods, (void*)pycbc_streamed_result_methods },
  { Py_tp_members, (void*)pycbc_streamed_result_members },
  { Py_tp_doc, (void*)PyDoc_STR("pycbc streamed result") },
  { 0, nullptr }
};

static PyType_Spec pycbc_streamed_result_spec = { "pycbc_core.pycbc_streamed_result",
                                                  sizeof(pycbc_streamed_result),
                                                  0,
                                                  Py_TPFLAGS_DEFAULT,
                                                  pycbc_streamed_result_slots };

static PyObject* pycbc_streamed_result_type_obj = nullptr;

pycbc_streamed_result*
create_pycbc_streamed_result(std::chrono::milliseconds timeout_ms)
{
  PyObject* pyObj_res = PyObject_CallObject(pycbc_streamed_result_type_obj, nullptr);
  if (pyObj_res == nullptr) {
    return nullptr;
  }
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
  pycbc_scan_iterator* self = (pycbc_scan_iterator*)PyType_GenericAlloc(type, 0);
  if (self != nullptr) {
    new (&self->scan_result) std::shared_ptr<couchbase::core::scan_result>();
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
  free_heap_type_instance(reinterpret_cast<PyObject*>(self));
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
    // Intentional C-API protocol deviation: on error we return an exception
    // object as a normal row instead of calling PyErr_SetString()/returning
    // NULL. The Python wrapper checks each row with
    // isinstance(resp, PycbcCoreException) and raises it itself, so
    // build_exception()'s result must be returned, not raised, here.
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

static PyType_Slot pycbc_scan_iterator_slots[] = {
  { Py_tp_new, (void*)pycbc_scan_iterator__new__ },
  { Py_tp_dealloc, (void*)pycbc_scan_iterator__dealloc__ },
  { Py_tp_iter, (void*)pycbc_scan_iterator__iter__ },
  { Py_tp_iternext, (void*)pycbc_scan_iterator__iternext__ },
  { Py_tp_methods, (void*)pycbc_scan_iterator_methods },
  { Py_tp_doc, (void*)PyDoc_STR("pycbc range scan iterator") },
  { 0, nullptr }
};

static PyType_Spec pycbc_scan_iterator_spec = { "pycbc_core.pycbc_scan_iterator",
                                                sizeof(pycbc_scan_iterator),
                                                0,
                                                Py_TPFLAGS_DEFAULT,
                                                pycbc_scan_iterator_slots };

static PyObject* pycbc_scan_iterator_type_obj = nullptr;

pycbc_scan_iterator*
create_pycbc_scan_iterator(couchbase::core::scan_result result)
{
  PyObject* pyObj_iter = PyObject_CallObject(pycbc_scan_iterator_type_obj, nullptr);
  if (!pyObj_iter) {
    // The scan was already dispatched to the server (orchestrator.scan()) before this
    // wrapper could be allocated. Cancel the scan_result explicitly so it doesn't keep
    // running with no handle able to reach it.
    result.cancel();
    return nullptr;
  }

  pycbc_scan_iterator* iter = reinterpret_cast<pycbc_scan_iterator*>(pyObj_iter);
  iter->scan_result = std::make_shared<couchbase::core::scan_result>(std::move(result));

  return iter;
}

int
add_result_objects(PyObject* module)
{
  pycbc_result_type_obj = PyType_FromSpec(&pycbc_result_spec);
  if (pycbc_result_type_obj == nullptr) {
    return -1;
  }
  if (PyModule_AddType(module, (PyTypeObject*)pycbc_result_type_obj) < 0) {
    return -1;
  }

  pycbc_streamed_result_type_obj = PyType_FromSpec(&pycbc_streamed_result_spec);
  if (pycbc_streamed_result_type_obj == nullptr) {
    return -1;
  }
  if (PyModule_AddType(module, (PyTypeObject*)pycbc_streamed_result_type_obj) < 0) {
    return -1;
  }

  pycbc_scan_iterator_type_obj = PyType_FromSpec(&pycbc_scan_iterator_spec);
  if (pycbc_scan_iterator_type_obj == nullptr) {
    return -1;
  }
  if (PyModule_AddType(module, (PyTypeObject*)pycbc_scan_iterator_type_obj) < 0) {
    return -1;
  }

  return 0;
}

} // namespace pycbc
