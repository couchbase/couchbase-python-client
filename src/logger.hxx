/*
 *     Copyright 2022 Couchbase, Inc.
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
#include <spdlog/sinks/base_sink.h>
#include <spdlog/details/log_msg.h>
#include <queue>
#include <core/logger/logger.hxx>
#include <core/logger/configuration.hxx>
#include <core/transactions.hxx>

// the spdlog::log_msg uses string_view, since it doesn't want
// copies.   Since we consume the log_msg then asych process it,
// the string_view can be pointing to data that is gone already,
// so lets copy into this struct.

struct log_msg_copy {
    std::string logger_name;
    spdlog::level::level_enum level;
    std::chrono::system_clock::time_point time;
    spdlog::source_loc source;
    std::string payload;

    log_msg_copy(const spdlog::details::log_msg& msg)
    {
        logger_name = std::string(msg.logger_name.data(), msg.logger_name.size());
        payload = std::string(msg.payload.data(), msg.payload.size());
        level = msg.level;
        time = msg.time;
        source = msg.source;
    }
};

size_t
convert_spdlog_level(spdlog::level::level_enum lvl);

couchbase::core::logger::level
convert_python_log_level(PyObject* level);

// Moved to implementing a spdlog::sinks::sink instead of a base_sink.  Allows us to not
// worry about the mutex w/in the base_sink.  The GIL is the locking mechanism that makes
// sure logging is thread safe as we acquire the GIL prior to passing the log message to
// Python's Logging module.
//
// Still probably the better way to do logging: asynchronous logger (see note below).
//
// A third way would be to use asynchronous logger.   However the txns lib only creates synchronous
// loggers now.   This is probably the best solution, which we can do when we merge the txn lib
// into the client lib.
//
class pycbc_logger_sink : public spdlog::sinks::sink
{
  public:
    pycbc_logger_sink(PyObject* pyObj_logger)
      : pyObj_logger_(pyObj_logger)
    {
        Py_INCREF(pyObj_logger_);
    }

    // no copy or move constructor or assignment
    pycbc_logger_sink(const pycbc_logger_sink&) = delete;
    pycbc_logger_sink(pycbc_logger_sink&&) = delete;

    pycbc_logger_sink& operator=(const pycbc_logger_sink&) = delete;
    pycbc_logger_sink& operator=(pycbc_logger_sink&&) = delete;

    ~pycbc_logger_sink()
    {
        if (0 == _Py_IsFinalizing()) {
            auto state = PyGILState_Ensure();
            Py_DECREF(pyObj_logger_);
            PyGILState_Release(state);
        }
    }

    void log(const spdlog::details::log_msg& msg) final
    {
        if (0 == _Py_IsFinalizing()) {
            log_it_(msg);
        }
    }

    void flush() final{};

    void set_pattern(const std::string& pattern) final{};
    void set_formatter(std::unique_ptr<spdlog::formatter> sink_formatter) final{};

  protected:
    void log_it_(const spdlog::details::log_msg& msg)
    {
        PyGILState_STATE state = PyGILState_Ensure();
        try {

            // static initialize the type and method once.   These 'leak' a single
            // object, but that is fine.  Same for an empty tuple we will on each call.
            static PyObject* pyObj_log_record_type = init_log_record_type();
            static PyObject* pyObj_logger_handle_method = init_logger_handle_method();

            // convert the log_msg_copy to a dict first...
            auto pyObj_log_record_details = convert_log_msg(msg);

            // now, create an actual LogRecord from it...
            auto pyObj_log_record = PyObject_CallObject(pyObj_log_record_type, pyObj_log_record_details);
            Py_DECREF(pyObj_log_record_details);
            if (nullptr != pyObj_log_record) {
                // we need to fixup the created time, which cannot be passed in the constructor...
                // The created member is a float containing a float expressed as seconds since the epoch, in UTC.
                PyObject* log_time = convert_time_to_float(msg.time);
                PyObject_SetAttrString(pyObj_log_record, "created", log_time);
                Py_DECREF(log_time);

                // now, we want to hand this record to the logger...
                PyObject* pyObj_args = PyTuple_Pack(1, pyObj_log_record);
                PyObject_CallObject(pyObj_logger_handle_method, pyObj_args);

                // that's it, now cleanup.
                Py_DECREF(pyObj_log_record);
                Py_DECREF(pyObj_args);
            } else {
                PyErr_Print();
            }
            PyGILState_Release(state);
        } catch (...) {
            PyGILState_Release(state);
        }
    }

    PyObject* convert_time_to_float(std::chrono::system_clock::time_point tm)
    {
        auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(tm.time_since_epoch());
        auto time = static_cast<double>(duration_us.count()) / 1000000;
        return PyFloat_FromDouble(time);
    }

    PyObject* convert_log_msg(const log_msg_copy& msg)
    {
        // convert to a python dict, assuming we already have the GIL
        // We need to supply the following keys/values:
        // name: str
        // level: int ( CRITICAL = 50, DEBUG=10, ERROR=40, FATAL=50, INFO=20, WARNING=30, NOTSET=0)
        // TODO: map trace from spdlog - can start with making it debug as well, but really
        //       should add TRACE to python logging levels
        // pathname: str  (path to file that did the logging)
        // lineno: int (line number of line that logged in that file)
        // msg: str (text of the message)
        // args: Dict (extras - probably we will not use that for now)
        // exc_info: str (python exception tuple if there is one)
        PyObject* retval = PyTuple_New(8);
        // name
        PyObject* pyObj_value = PyUnicode_FromStringAndSize(msg.logger_name.data(), msg.logger_name.size());
        PyTuple_SetItem(retval, 0, pyObj_value);
        // level
        pyObj_value = PyLong_FromSize_t(convert_spdlog_level(msg.level));
        PyTuple_SetItem(retval, 1, pyObj_value);
        // pathname
        if (nullptr != msg.source.filename) {
            pyObj_value = PyUnicode_FromString(msg.source.filename);
        } else {
            pyObj_value = PyUnicode_FromString("transactions");
        }
        PyTuple_SetItem(retval, 2, pyObj_value);
        // lineno
        pyObj_value = PyLong_FromSize_t(static_cast<size_t>(msg.source.line));
        PyTuple_SetItem(retval, 3, pyObj_value);
        // msg
        pyObj_value = PyUnicode_FromStringAndSize(msg.payload.data(), msg.payload.size());
        PyTuple_SetItem(retval, 4, pyObj_value);
        // args
        Py_INCREF(Py_None);
        PyTuple_SetItem(retval, 5, Py_None);
        // exc_info
        Py_INCREF(Py_None);
        PyTuple_SetItem(retval, 6, Py_None);
        // func
        if (nullptr != msg.source.funcname) {
            pyObj_value = PyUnicode_FromString(msg.source.funcname);
        } else {
            pyObj_value = Py_None;
            Py_INCREF(pyObj_value);
        }
        PyTuple_SetItem(retval, 7, pyObj_value);

        return retval;
    }

    PyObject* init_log_record_type()
    {
        static PyObject* logging = PyImport_ImportModule("logging");
        assert(nullptr != logging);
        return PyObject_GetAttrString(logging, "LogRecord");
    }

    PyObject* init_logger_handle_method()
    {
        // we want the 'handle' method on the pyObj_logger_, so...
        PyObject* meth = PyObject_GetAttrString(pyObj_logger_, "handle");
        assert(nullptr != meth);
        return meth;
    }

  private:
    PyObject* pyObj_logger_;
};

struct pycbc_logger {
    PyObject_HEAD std::shared_ptr<pycbc_logger_sink> logger_sink_;
};

int
pycbc_logger_type_init(PyObject** ptr);
