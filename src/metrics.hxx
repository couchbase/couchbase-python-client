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

#pragma once

#include <couchbase/metrics/meter.hxx>
// NOLINTNEXTLINE
#include "Python.h" // NOLINT

namespace metrics = couchbase::metrics;

namespace pycbc
{
class value_recorder : public metrics::value_recorder
{
  public:
    explicit value_recorder(PyObject* recorder)
      : metrics::value_recorder()
      , pyObj_recorder_(recorder)
    {
        // A value_recorder is only created from meter::get_value_recorder, which is responsible for obtaining the GIL
        Py_INCREF(pyObj_recorder_);
        pyObj_record_value_ = PyObject_GetAttrString(pyObj_recorder_, "record_value");
        CB_LOG_DEBUG("{}: created value_recorder", "PYCBC");
    }

    ~value_recorder() override
    {
        PyGILState_STATE state = PyGILState_Ensure();
        Py_DECREF(pyObj_recorder_);
        Py_DECREF(pyObj_record_value_);
        PyGILState_Release(state);
        CB_LOG_DEBUG("{}: destroyed value_recorder", "PYCBC");
    }

    void record_value(std::int64_t value) override
    {
        PyGILState_STATE state = PyGILState_Ensure();
        auto pyObj_args = Py_BuildValue("(n)", static_cast<Py_ssize_t>(value));
        PyObject_CallObject(pyObj_record_value_, pyObj_args);
        Py_DECREF(pyObj_args);
        PyGILState_Release(state);
    }

  private:
    PyObject* pyObj_recorder_;
    PyObject* pyObj_record_value_;
};

class meter : public metrics::meter
{
  public:
    meter(PyObject* meter)
      : pyObj_meter_(meter)
    {
        // Assume we have the GIL when creating a CouchbaseMeter
        Py_INCREF(meter);
        pyObj_value_recorder_ = PyObject_GetAttrString(meter, "value_recorder");
        assert(pyObj_value_recorder_);
    }

    ~meter() override
    {
        PyGILState_STATE state = PyGILState_Ensure();
        Py_DECREF(pyObj_value_recorder_);
        Py_DECREF(pyObj_meter_);
        PyGILState_Release(state);
    }

    std::shared_ptr<metrics::value_recorder> get_value_recorder(const std::string& name,
                                                                const std::map<std::string, std::string>& tags) override
    {
        PyGILState_STATE state = PyGILState_Ensure();
        PyObject* pyObj_name = PyUnicode_FromString(name.c_str());
        PyObject* pyObj_tags = PyDict_New();
        for (const auto& [key, value] : tags) {
            PyObject* pyObj_value = PyUnicode_FromString(value.c_str());
            PyDict_SetItemString(pyObj_tags, key.c_str(), pyObj_value);
            Py_DECREF(pyObj_value);
        }
        PyObject* pyObj_args = PyTuple_Pack(2, pyObj_name, pyObj_tags);
        auto pyObj_value_recorder = PyObject_CallObject(pyObj_value_recorder_, pyObj_args);
        auto retval = std::make_shared<value_recorder>(pyObj_value_recorder);
        Py_DECREF(pyObj_name);
        Py_DECREF(pyObj_tags);
        Py_DECREF(pyObj_args);
        Py_DECREF(pyObj_value_recorder);
        PyGILState_Release(state);
        return retval;
    }

  private:
    PyObject* pyObj_meter_;
    PyObject* pyObj_value_recorder_;
};
} // namespace pycbc
