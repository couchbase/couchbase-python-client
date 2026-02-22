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
#include <couchbase/tracing/request_tracer.hxx>
#include <iostream>

namespace cbtracing = couchbase::tracing;

namespace pycbc
{

class deprecated_request_span : public cbtracing::request_span
{
public:
  explicit deprecated_request_span(PyObject* span,
                                   std::shared_ptr<cbtracing::request_span> parent = nullptr)
    : cbtracing::request_span("",
                              parent) // name doesn't matter - it is in the underlying python span
    , pyObj_span_(span)
  {
    // called by deprecated_request_tracer.start_span & KV/streaming ops (when building a C++ core
    // request), so we are confident we have the GIL
    Py_INCREF(span);
    pyObj_set_attribute_ = PyObject_GetAttrString(pyObj_span_, "set_attribute");
  }

  ~deprecated_request_span() override
  {
    // PYCBC-1748 - This can be a race condition when the Python interpreter finalizes before we can
    // decref.
    //              The work-around (which has been in tests all along) is to call cluster.close().
    //              The FIX is to not use the legacy (deprecated) tracing which will no longer be an
    //              issue w/ PYCBC-1746.
    PyGILState_STATE state = PyGILState_Ensure();
    Py_DECREF(pyObj_set_attribute_);
    Py_DECREF(pyObj_span_);
    PyGILState_Release(state);
  }

  void add_tag(const std::string& name, std::uint64_t value) override
  {
    PyGILState_STATE state = PyGILState_Ensure();
    auto pyObj_args = Py_BuildValue("(sn)", name.c_str(), static_cast<Py_ssize_t>(value));
    PyObject_Call(pyObj_set_attribute_, pyObj_args, nullptr);
    Py_DECREF(pyObj_args);
    PyGILState_Release(state);
  }

  void add_tag(const std::string& name, const std::string& value) override
  {
    PyGILState_STATE state = PyGILState_Ensure();
    auto pyObj_args = Py_BuildValue("(ss)", name.c_str(), value.c_str());
    PyObject_Call(pyObj_set_attribute_, pyObj_args, nullptr);
    Py_DECREF(pyObj_args);
    PyGILState_Release(state);
  }

  void end() override
  {
    PyGILState_STATE state = PyGILState_Ensure();
    auto pyObj_end = PyObject_GetAttrString(pyObj_span_, "finish");
    PyObject_CallObject(pyObj_end, nullptr);
    Py_DECREF(pyObj_end);
    PyGILState_Release(state);
  }

  PyObject* py_span()
  {
    return pyObj_span_;
  }

private:
  PyObject* pyObj_span_;
  PyObject* pyObj_set_attribute_;
  PyObject* pyObj_get_context_;
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
    assert(pyObj_start_span_);
  }

  ~deprecated_request_tracer()
  {
    // PYCBC-1748 - This can be a race condition when the Python interpreter finalizes before we can
    // decref.
    //              The work-around (which has been in tests all along) is to call cluster.close().
    //              The FIX is to not use the legacy (deprecated) tracing which will no longer be an
    //              issue w/ PYCBC-1746.
    PyGILState_STATE state = PyGILState_Ensure();
    Py_DECREF(pyObj_start_span_);
    Py_DECREF(pyObj_tracer_);
    PyGILState_Release(state);
  }

  std::shared_ptr<cbtracing::request_span> start_span(
    std::string name,
    std::shared_ptr<cbtracing::request_span> parent = {}) override
  {
    // defer to the pyObj_tracer_, and wrap the result in a pycbc span.  Note: Taking the GIL here,
    // and elsewhere (like in the request_span) isn't perhaps the most efficient strategy.  We could
    // cache spans and periodically (or just when asked) grab the GIL and create them.   However,
    // lets do this first, then think about optimizations
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* pyObj_name = PyUnicode_FromString(name.c_str());
    PyObject* pyObj_args = PyTuple_New(0);
    PyObject* pyObj_kwargs = PyDict_New();
    PyDict_SetItemString(pyObj_kwargs, "name", pyObj_name);
    if (parent) {
      auto pyObj_parent =
        std::dynamic_pointer_cast<pycbc::deprecated_request_span>(parent)->py_span();
      PyDict_SetItemString(pyObj_kwargs, "parent", pyObj_parent);
    }
    auto pyObj_span = PyObject_Call(pyObj_start_span_, pyObj_args, pyObj_kwargs);
    auto retval = std::make_shared<pycbc::deprecated_request_span>(pyObj_span, parent);
    Py_DECREF(pyObj_name);
    Py_DECREF(pyObj_args);
    Py_DECREF(pyObj_kwargs);
    Py_DECREF(pyObj_span);
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
  PyObject* pyObj_tracer_;
  PyObject* pyObj_start_span_;
};

} // namespace pycbc
