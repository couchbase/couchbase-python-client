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
#include <core/logger/logger.hxx>
#include <couchbase/tracing/request_tracer.hxx>
#include <iostream>
#include <stdexcept>

namespace cbtracing = couchbase::tracing;

namespace pycbc
{

class deprecated_request_span : public cbtracing::request_span
{
public:
  explicit deprecated_request_span(PyObject* span,
                                   std::shared_ptr<cbtracing::request_span> parent = nullptr)
    // the name doesn't matter, it is in the underlying python span
    : cbtracing::request_span("", parent)
    , pyObj_span_(span)
  {
    // called by deprecated_request_tracer.start_span & KV/streaming ops (when building a C++ core
    // request), so we are confident we have the GIL
    Py_XINCREF(pyObj_span_);
    if (pyObj_span_ == nullptr) {
      // the python tracer's start_span() failed, so this span degrades to a no-op
      return;
    }
    pyObj_set_attribute_ = PyObject_GetAttrString(pyObj_span_, "set_attribute");
    if (pyObj_set_attribute_ == nullptr) {
      // spans are built where we have no way to report a failure, so no-op add_tag() instead
      PyErr_WriteUnraisable(pyObj_span_);
      CB_LOG_WARNING("PYCBC: Legacy tracing span has no 'set_attribute' method; add_tag() will "
                     "be a no-op for this span.");
    }
  }

  ~deprecated_request_span() override
  {
    // PYCBC-1748 - This can be a race condition when the Python interpreter finalizes before we
    // can decref. The work-around (which has been in tests all along) is to call cluster.close().
    // The FIX is to not use the legacy (deprecated) tracing, which will no longer be an issue w/
    // PYCBC-1746.
    PyGILState_STATE state = PyGILState_Ensure();
    Py_XDECREF(pyObj_set_attribute_);
    Py_XDECREF(pyObj_span_);
    PyGILState_Release(state);
  }

  void add_tag(const std::string& name, std::uint64_t value) override
  {
    PyGILState_STATE state = PyGILState_Ensure();
    call_set_attribute(Py_BuildValue("(sn)", name.c_str(), static_cast<Py_ssize_t>(value)));
    PyGILState_Release(state);
  }

  void add_tag(const std::string& name, const std::string& value) override
  {
    PyGILState_STATE state = PyGILState_Ensure();
    call_set_attribute(Py_BuildValue("(ss)", name.c_str(), value.c_str()));
    PyGILState_Release(state);
  }

  void end() override
  {
    PyGILState_STATE state = PyGILState_Ensure();
    if (pyObj_span_ != nullptr) {
      PyObject* pyObj_finish = PyObject_GetAttrString(pyObj_span_, "finish");
      if (pyObj_finish == nullptr) {
        PyErr_WriteUnraisable(pyObj_span_);
        CB_LOG_WARNING("PYCBC: Legacy tracing span has no 'finish' method; end() is a no-op for "
                       "this span.");
      } else {
        PyObject* pyObj_res = PyObject_CallObject(pyObj_finish, nullptr);
        if (pyObj_res == nullptr) {
          PyErr_WriteUnraisable(pyObj_finish);
          CB_LOG_WARNING("PYCBC: Legacy tracing span's 'finish' method raised an exception.");
        }
        Py_XDECREF(pyObj_res);
        Py_DECREF(pyObj_finish);
      }
    }
    PyGILState_Release(state);
  }

  PyObject* py_span()
  {
    return pyObj_span_;
  }

private:
  // Steals pyObj_args, which is NULL when Py_BuildValue failed. The caller must hold the GIL.
  void call_set_attribute(PyObject* pyObj_args)
  {
    if (pyObj_args == nullptr || pyObj_set_attribute_ == nullptr) {
      // add_tag() returns into C++ core, so an error must never be left pending
      if (PyErr_Occurred() != nullptr) {
        PyErr_WriteUnraisable(pyObj_span_);
        CB_LOG_WARNING("PYCBC: Failed to build args for legacy tracing add_tag().");
      }
      Py_XDECREF(pyObj_args);
      return;
    }
    PyObject* pyObj_res = PyObject_Call(pyObj_set_attribute_, pyObj_args, nullptr);
    if (pyObj_res == nullptr) {
      PyErr_WriteUnraisable(pyObj_set_attribute_);
      CB_LOG_WARNING("PYCBC: Legacy tracing span's 'set_attribute' method raised an exception.");
    }
    Py_XDECREF(pyObj_res);
    Py_DECREF(pyObj_args);
  }

  PyObject* pyObj_span_;
  PyObject* pyObj_set_attribute_{ nullptr };
};

class deprecated_request_tracer : public cbtracing::request_tracer
{
public:
  deprecated_request_tracer(PyObject* tracer)
    : pyObj_tracer_(tracer)
  {
    // Assumption here is we have the GIL when we wrap the python tracer here
    Py_INCREF(tracer);
    pyObj_start_span_ = PyObject_GetAttrString(tracer, "start_span");
    if (pyObj_start_span_ == nullptr) {
      // Leave the pending error set so connect()'s catch reports it, not this message. The
      // destructor does not run when a ctor throws, so undo the INCREF here.
      Py_DECREF(tracer);
      throw std::invalid_argument("Legacy tracer must provide a start_span() method.");
    }
  }

  ~deprecated_request_tracer()
  {
    // PYCBC-1748 - This can be a race condition when the Python interpreter finalizes before we
    // can decref. The work-around (which has been in tests all along) is to call cluster.close().
    // The FIX is to not use the legacy (deprecated) tracing, which will no longer be an issue w/
    // PYCBC-1746.
    PyGILState_STATE state = PyGILState_Ensure();
    Py_DECREF(pyObj_start_span_);
    Py_DECREF(pyObj_tracer_);
    PyGILState_Release(state);
  }

  std::shared_ptr<cbtracing::request_span> start_span(
    std::string name,
    std::shared_ptr<cbtracing::request_span> parent = {}) override
  {
    // defer to the pyObj_tracer_, and wrap the result in a pycbc span. Note: taking the GIL here,
    // and elsewhere (like in the request_span) isn't perhaps the most efficient strategy. We could
    // cache spans and periodically (or just when asked) grab the GIL and create them. However,
    // lets do this first, then think about optimizations
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* pyObj_span = call_start_span(name, parent);
    auto retval = std::make_shared<pycbc::deprecated_request_span>(pyObj_span, parent);
    Py_XDECREF(pyObj_span);
    PyGILState_Release(state);
    return retval;
  }

  void start() override
  {
  }

  void stop() override
  {
  }

private:
  // Adds the python span behind parent, if there is one, to the kwargs. Returns false w/ an error
  // set. The caller must hold the GIL.
  static bool maybe_add_parent(PyObject* pyObj_kwargs,
                               const std::shared_ptr<cbtracing::request_span>& parent)
  {
    if (!parent) {
      return true;
    }
    auto pycbc_parent = std::dynamic_pointer_cast<pycbc::deprecated_request_span>(parent);
    if (pycbc_parent == nullptr || pycbc_parent->py_span() == nullptr) {
      // not a legacy span, or a no-op one, so start a top-level span instead of failing the op
      return true;
    }
    return PyDict_SetItemString(pyObj_kwargs, "parent", pycbc_parent->py_span()) == 0;
  }

  // Calls the python tracer's start_span(). Returns a new reference, or NULL w/ the failure already
  // reported. The caller must hold the GIL.
  PyObject* call_start_span(const std::string& name,
                            const std::shared_ptr<cbtracing::request_span>& parent)
  {
    PyObject* pyObj_span = nullptr;
    PyObject* pyObj_args = PyTuple_New(0);
    PyObject* pyObj_kwargs = PyDict_New();
    PyObject* pyObj_name = PyUnicode_FromString(name.c_str());
    if (pyObj_args != nullptr && pyObj_kwargs != nullptr && pyObj_name != nullptr &&
        PyDict_SetItemString(pyObj_kwargs, "name", pyObj_name) == 0 &&
        maybe_add_parent(pyObj_kwargs, parent)) {
      pyObj_span = PyObject_Call(pyObj_start_span_, pyObj_args, pyObj_kwargs);
    }
    if (pyObj_span == nullptr) {
      // start_span() returns into C++ core, so an error must never be left pending
      PyErr_WriteUnraisable(pyObj_start_span_);
      CB_LOG_WARNING("PYCBC: Legacy tracer's 'start_span' method raised an exception.");
    }
    Py_XDECREF(pyObj_name);
    Py_XDECREF(pyObj_args);
    Py_XDECREF(pyObj_kwargs);
    return pyObj_span;
  }

  PyObject* pyObj_tracer_;
  PyObject* pyObj_start_span_;
};

} // namespace pycbc
