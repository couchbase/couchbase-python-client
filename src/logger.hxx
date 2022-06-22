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
#include <spdlog/sinks/base_sink.h>
#include <spdlog/details/log_msg.h>
#include <queue>
#include <couchbase/logger/logger.hxx>
#include <couchbase/logger/configuration.hxx>
#include <couchbase/transactions.hxx>

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
convert_spdlog_level(spdlog::level::level_enum lvl)
{
    // TODO:  support trace level in the python logger
    switch (lvl) {
        case spdlog::level::level_enum::off:
            return 0;
        case spdlog::level::level_enum::trace:
        case spdlog::level::level_enum::debug:
            return 10;
        case spdlog::level::level_enum::info:
            return 20;
        case spdlog::level::level_enum::warn:
            return 30;
        case spdlog::level::level_enum::err:
            return 40;
        case spdlog::level::level_enum::critical:
            return 50;
        default:
            return 0;
    }
}

couchbase::logger::level
convert_python_log_level(PyObject* level)
{
    auto lvl = PyLong_AsSize_t(level);
    switch (lvl) {
        case 0:
            return couchbase::logger::level::off;
        case 10:
            return couchbase::logger::level::trace;
        case 20:
            return couchbase::logger::level::info;
        case 30:
            return couchbase::logger::level::warn;
        case 40:
            return couchbase::logger::level::err;
        case 50:
            return couchbase::logger::level::critical;
        default:
            return couchbase::logger::level::off;
    }
}

// The thread that calls this sink may have the GIL beforehand.   Or maybe not.
// But, the base_sink has it's own mutex as well, which it locks before calling
// the sink_it_ function.   So... classic deadlock since we cant guarantee the
// ordering of those.   Simplest thing to do is to release the GIL, push the message
// into a queue (locked with different mutex), and have another thread pop the message
// unlock the mutex protecting the queue, _then_ grab the GIL and shove the message
// into the python logger.
//
// Ya "simplest"...   Perhaps another way would be to implement the sink entirely ourselves,
// rather than use the base_sink, and order the mutex there.   This felt safer, but you know,
// worth pondering.
//
// A third way would be to use asynchronous logger.   However the txns lib only creates synchronous
// loggers now.   This is probably the best solution, which we can do when we merge the txn lib
// into the client lib.
//
template<typename Mutex>
class pycbc_logger_sink : public spdlog::sinks::base_sink<Mutex>
{
  public:
    pycbc_logger_sink(PyObject* pyObj_logger)
      : pyObj_logger_(pyObj_logger)
      , running_(true)
    {
        Py_INCREF(pyObj_logger_);
        worker_ = std::thread([&] { this->worker_thread(); });
    }

    virtual ~pycbc_logger_sink()
    {
        std::unique_lock<std::mutex> lock(worker_mutex_);
        running_ = false;
        cv_.notify_all();
        lock.unlock();
        if (1 == PyGILState_Check() && 0 == _Py_IsFinalizing()) {
            Py_BEGIN_ALLOW_THREADS if (worker_.joinable())
            {
                shutdown_worker();
            }
            Py_END_ALLOW_THREADS
        } else {
            shutdown_worker();
        }
        if (0 == _Py_IsFinalizing()) {
            auto state = PyGILState_Ensure();
            Py_DECREF(pyObj_logger_);
            PyGILState_Release(state);
        }
    }

  protected:
    void shutdown_worker()
    {
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    void sink_it_(const spdlog::details::log_msg& msg) override
    {
        if (PyGILState_Check() == 1) {
            Py_BEGIN_ALLOW_THREADS push(msg);
            Py_END_ALLOW_THREADS
        } else {
            push(msg);
        }
    }

    void push(const spdlog::details::log_msg& msg)
    {
        std::unique_lock<std::mutex> lock(worker_mutex_);
        queue_.push(log_msg_copy(msg));
        cv_.notify_all();
    }

    void worker_thread()
    {
        // This is inefficient -- getting the gil for every log message.   However,
        // it also prevents 'jitter'.   Perhaps if too inefficient, we can consider a thread
        // approach.
        PyGILState_STATE state;
        while (true) {
            std::unique_lock<std::mutex> lock(worker_mutex_);
            cv_.wait(lock, [&] { return !running_ || !queue_.empty(); });
            if (!running_) {
                return;
            }
            if (queue_.empty()) {
                continue;
            }
            auto msg = queue_.front();
            queue_.pop();
            lock.unlock();
            if (_Py_IsFinalizing()) {
                return;
            }
            state = PyGILState_Ensure();
            try {

                // static initialize the type and method once.   These 'leak' a single
                // object, but that is fine.  Same for an empty tuple we will on each call.
                static PyObject* pyObj_log_record_type = init_log_record_type();
                static PyObject* pyObj_logger_handle_method = init_logger_handle_method();
                static PyObject* pyObj_empty_tuple = Py_BuildValue("()");

                // convert the log_msg_copy to a dict first...
                auto pyObj_log_dict = convert_log_msg(msg);

                // now, create an actual LogRecord from it...
                auto pyObj_log_record = PyObject_Call(pyObj_log_record_type, pyObj_empty_tuple, pyObj_log_dict);
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
                    Py_DECREF(pyObj_log_dict);
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
    }

    void flush_() override
    {
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
        PyObject* retval = PyDict_New();
        PyObject* pyObj_value = PyUnicode_FromStringAndSize(msg.payload.data(), msg.payload.size());
        PyDict_SetItemString(retval, "msg", pyObj_value);
        Py_DECREF(pyObj_value);
        pyObj_value = PyLong_FromSize_t(convert_spdlog_level(msg.level));
        PyDict_SetItemString(retval, "level", pyObj_value);
        Py_DECREF(pyObj_value);
        pyObj_value = PyUnicode_FromStringAndSize(msg.logger_name.data(), msg.logger_name.size());
        PyDict_SetItemString(retval, "name", pyObj_value);
        Py_DECREF(pyObj_value);
        if (nullptr != msg.source.filename) {
            pyObj_value = PyUnicode_FromString(msg.source.filename);
        } else {
            pyObj_value = PyUnicode_FromString("transactions");
        }
        PyDict_SetItemString(retval, "pathname", pyObj_value);
        Py_DECREF(pyObj_value);
        pyObj_value = PyLong_FromSize_t(static_cast<size_t>(msg.source.line));
        PyDict_SetItemString(retval, "lineno", pyObj_value);
        Py_DECREF(pyObj_value);
        if (nullptr != msg.source.funcname) {
            pyObj_value = PyUnicode_FromString(msg.source.funcname);
        } else {
            pyObj_value = Py_None;
            Py_INCREF(pyObj_value);
        }
        PyDict_SetItemString(retval, "func", pyObj_value);
        Py_DECREF(pyObj_value);
        PyDict_SetItemString(retval, "args", Py_None);
        PyDict_SetItemString(retval, "exc_info", Py_None);
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
    std::mutex worker_mutex_;
    std::queue<log_msg_copy> queue_;
    std::condition_variable cv_;
    std::thread worker_;
    bool running_;
};

PyObject*
configure_logging(PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* pyObj_logger = nullptr;
    PyObject* pyObj_level = nullptr;
    const char* kw_list[] = { "logger", "level", nullptr };
    const char* kw_format = "OO";
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, kw_format, const_cast<char**>(kw_list), &pyObj_logger, &pyObj_level)) {
        return nullptr;
    }
    couchbase::logger::configuration logger_settings;
    logger_settings.console = false;
    logger_settings.sink = std::make_shared<pycbc_logger_sink<std::mutex>>(pyObj_logger);
    auto level = convert_python_log_level(pyObj_level);
    logger_settings.log_level = level;
    couchbase::transactions::create_loggers(logger_settings.log_level, logger_settings.sink);
    couchbase::logger::create_file_logger(logger_settings);
    Py_RETURN_NONE;
}
