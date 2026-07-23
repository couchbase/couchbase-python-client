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
#include <atomic>
#include <core/logger/configuration.hxx>
#include <core/logger/logger.hxx>
#include <core/transactions.hxx>
#include <queue>
#include <spdlog/details/log_msg.h>
#include <spdlog/sinks/base_sink.h>

namespace pycbc
{

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
// MODIFIED: Removed Py_IsFinalizing() usage for Py_LIMITED_API compatibility.
// Uses std::atomic<bool> active_ flag for lifecycle management instead.
// Python-side atexit handler calls deactivate() before interpreter shutdown.
//
class pycbc_logger_sink : public spdlog::sinks::sink
{
public:
  // The logger's handle() method and the LogRecord type are resolved by the caller so a
  // failed lookup can be reported as an exception; both are non-NULL for the sink's lifetime.
  // All three arguments are borrowed: the sink takes its own reference to each, so the caller
  // remains responsible for the references it holds.
  pycbc_logger_sink(PyObject* pyObj_logger,
                    PyObject* pyObj_logger_handle_method,
                    PyObject* pyObj_log_record_type)
    : pyObj_logger_(pyObj_logger)
    , pyObj_logger_handle_method_(pyObj_logger_handle_method)
    , pyObj_log_record_type_(pyObj_log_record_type)
    , active_(true)
  {
    Py_INCREF(pyObj_logger_);
    Py_INCREF(pyObj_logger_handle_method_);
    Py_INCREF(pyObj_log_record_type_);
  }

  // no copy or move constructor or assignment
  pycbc_logger_sink(const pycbc_logger_sink&) = delete;
  pycbc_logger_sink(pycbc_logger_sink&&) = delete;

  pycbc_logger_sink& operator=(const pycbc_logger_sink&) = delete;
  pycbc_logger_sink& operator=(pycbc_logger_sink&&) = delete;

  // Explicitly deactivate the sink before interpreter shutdown
  // Called from Python atexit handler
  void deactivate()
  {
    active_.store(false, std::memory_order_release);
  }

  ~pycbc_logger_sink()
  {
    // Only DECREF if still active (not deactivated via atexit)
    if (active_.load(std::memory_order_acquire)) {
      auto state = PyGILState_Ensure();
      Py_DECREF(pyObj_log_record_type_);
      Py_DECREF(pyObj_logger_handle_method_);
      Py_DECREF(pyObj_logger_);
      PyGILState_Release(state);
    }
  }

  void log(const spdlog::details::log_msg& msg) final
  {
    // Skip logging if deactivated
    if (!active_.load(std::memory_order_acquire)) {
      return;
    }
    log_it_(msg);
  }

  void flush() final {};

  void set_pattern(const std::string& pattern) final {};
  void set_formatter(std::unique_ptr<spdlog::formatter> sink_formatter) final {};

protected:
  void log_it_(const spdlog::details::log_msg& msg)
  {
    PyGILState_STATE state = PyGILState_Ensure();
    try {

      // convert the log_msg_copy to a dict first...
      auto pyObj_log_record_details = convert_log_msg(msg);
      if (nullptr == pyObj_log_record_details) {
        PyErr_WriteUnraisable(pyObj_logger_);
        PyGILState_Release(state);
        return;
      }

      // now, create an actual LogRecord from it...
      auto pyObj_log_record = PyObject_CallObject(pyObj_log_record_type_, pyObj_log_record_details);
      Py_DECREF(pyObj_log_record_details);
      if (nullptr != pyObj_log_record) {
        // we need to fixup the created time, which cannot be passed in the constructor...
        // The created member is a float containing a float expressed as seconds since the epoch, in
        // UTC.
        PyObject* log_time = convert_time_to_float(msg.time);
        if (nullptr == log_time) {
          PyErr_WriteUnraisable(pyObj_log_record);
        } else {
          if (-1 == PyObject_SetAttrString(pyObj_log_record, "created", log_time)) {
            PyErr_WriteUnraisable(pyObj_log_record);
          }
          Py_DECREF(log_time);
        }

        // now, we want to hand this record to the logger...
        PyObject* pyObj_args = PyTuple_Pack(1, pyObj_log_record);
        if (nullptr == pyObj_args) {
          PyErr_WriteUnraisable(pyObj_logger_handle_method_);
        } else {
          PyObject* pyObj_handle_result =
            PyObject_CallObject(pyObj_logger_handle_method_, pyObj_args);
          if (nullptr == pyObj_handle_result) {
            PyErr_WriteUnraisable(pyObj_logger_handle_method_);
          } else {
            Py_DECREF(pyObj_handle_result);
          }
          Py_DECREF(pyObj_args);
        }

        // that's it, now cleanup.
        Py_DECREF(pyObj_log_record);
      } else {
        PyErr_WriteUnraisable(pyObj_log_record_type_);
      }
      PyGILState_Release(state);
    } catch (...) {
      // Only release GIL if still active
      if (active_.load(std::memory_order_acquire)) {
        PyGILState_Release(state);
      }
      throw;
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
    // TODO:  map trace from spdlog - can start with making it debug as well, but really
    //       should add TRACE to python logging levels
    // pathname: str  (path to file that did the logging)
    // lineno: int (line number of line that logged in that file)
    // msg: str (text of the message)
    // args: Dict (extras - probably we will not use that for now)
    // exc_info: str (python exception tuple if there is one)
    PyObject* retval = PyTuple_New(8);
    if (nullptr == retval) {
      return nullptr;
    }

    // stores value into retval at idx, or drops retval and reports failure if value is NULL
    // (e.g. on OOM) so the caller never ends up with a NULL slot in the tuple.
    auto set_item_or_bail = [&](Py_ssize_t idx, PyObject* value) {
      if (nullptr == value) {
        Py_DECREF(retval);
        retval = nullptr;
        return false;
      }
      PyTuple_SetItem(retval, idx, value);
      return true;
    };

    // name
    if (!set_item_or_bail(
          0, PyUnicode_FromStringAndSize(msg.logger_name.data(), msg.logger_name.size()))) {
      return nullptr;
    }
    // level
    if (!set_item_or_bail(1, PyLong_FromSize_t(convert_spdlog_level(msg.level)))) {
      return nullptr;
    }
    // pathname
    PyObject* pyObj_pathname = (nullptr != msg.source.filename)
                                 ? PyUnicode_FromString(msg.source.filename)
                                 : PyUnicode_FromString("transactions");
    if (!set_item_or_bail(2, pyObj_pathname)) {
      return nullptr;
    }
    // lineno
    if (!set_item_or_bail(3, PyLong_FromSize_t(static_cast<size_t>(msg.source.line)))) {
      return nullptr;
    }
    // msg
    // The payload is the only field carrying arbitrary bytes, so decode with "replace" rather
    // than strict UTF-8: a log line with a non-UTF-8 byte degrades instead of being dropped.
    if (!set_item_or_bail(
          4, PyUnicode_DecodeUTF8(msg.payload.data(), msg.payload.size(), "replace"))) {
      return nullptr;
    }
    // args
    Py_INCREF(Py_None);
    PyTuple_SetItem(retval, 5, Py_None);
    // exc_info
    Py_INCREF(Py_None);
    PyTuple_SetItem(retval, 6, Py_None);
    // func
    PyObject* pyObj_funcname;
    if (nullptr != msg.source.funcname) {
      pyObj_funcname = PyUnicode_FromString(msg.source.funcname);
    } else {
      pyObj_funcname = Py_None;
      Py_INCREF(pyObj_funcname);
    }
    if (!set_item_or_bail(7, pyObj_funcname)) {
      return nullptr;
    }

    return retval;
  }

private:
  PyObject* pyObj_logger_;
  PyObject* pyObj_logger_handle_method_;
  PyObject* pyObj_log_record_type_;
  std::atomic<bool> active_;
};

struct pycbc_logger {
  PyObject_HEAD std::shared_ptr<pycbc_logger_sink> logger_sink_;
  bool is_console_logger{ false };
  bool is_file_logger{ false };
};

PyObject*
add_logger_objects(PyObject* pyObj_module);

} // namespace pycbc
