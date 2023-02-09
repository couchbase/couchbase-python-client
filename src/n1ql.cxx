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

#include "n1ql.hxx"
#include "exceptions.hxx"
#include "result.hxx"
#include "utils.hxx"
#include <couchbase/query_scan_consistency.hxx>
#include <couchbase/query_profile.hxx>

std::string
scan_consistency_type_to_string(couchbase::query_scan_consistency consistency)
{
    switch (consistency) {
        case couchbase::query_scan_consistency::not_bounded:
            return "not_bounded";
        case couchbase::query_scan_consistency::request_plus:
            return "request_plus";
    }
    // should not be able to reach here, since this is an enum class
    return "unknown";
}

couchbase::query_profile
str_to_profile_mode(std::string profile_mode)
{
    if (profile_mode.compare("off") == 0) {
        return couchbase::query_profile::off;
    }
    if (profile_mode.compare("phases") == 0) {
        return couchbase::query_profile::phases;
    }
    if (profile_mode.compare("timings") == 0) {
        return couchbase::query_profile::timings;
    }
    // TODO: better exception
    PyErr_SetString(PyExc_ValueError, "Invalid Profile Mode.");
    return {};
}

std::string
profile_mode_to_str(couchbase::query_profile profile_mode)
{
    switch (profile_mode) {
        case couchbase::query_profile::off:
            return "off";
        case couchbase::query_profile::phases:
            return "phases";
        case couchbase::query_profile::timings:
            return "timings";
    }
    return "unknown profile_mode";
}

PyObject*
get_result_metrics(couchbase::core::operations::query_response::query_metrics metrics)
{
    PyObject* pyObj_metrics = PyDict_New();
    std::chrono::duration<unsigned long long, std::nano> int_nsec = metrics.elapsed_time;
    PyObject* pyObj_tmp = PyLong_FromUnsignedLongLong(int_nsec.count());
    if (-1 == PyDict_SetItemString(pyObj_metrics, "elapsed_time", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_nsec = metrics.execution_time;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_nsec.count());
    if (-1 == PyDict_SetItemString(pyObj_metrics, "execution_time", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.result_count);
    if (-1 == PyDict_SetItemString(pyObj_metrics, "result_count", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.result_size);
    if (-1 == PyDict_SetItemString(pyObj_metrics, "result_size", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.sort_count);
    if (-1 == PyDict_SetItemString(pyObj_metrics, "sort_count", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.mutation_count);
    if (-1 == PyDict_SetItemString(pyObj_metrics, "mutation_count", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.error_count);
    if (-1 == PyDict_SetItemString(pyObj_metrics, "error_count", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.warning_count);
    if (-1 == PyDict_SetItemString(pyObj_metrics, "warning_count", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    return pyObj_metrics;
}

PyObject*
get_result_metadata(couchbase::core::operations::query_response::query_meta_data metadata, bool include_metrics)
{
    PyObject* pyObj_metadata = PyDict_New();
    PyObject* pyObj_tmp = PyUnicode_FromString(metadata.request_id.c_str());
    if (-1 == PyDict_SetItemString(pyObj_metadata, "request_id", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(metadata.client_context_id.c_str());
    if (-1 == PyDict_SetItemString(pyObj_metadata, "client_context_id", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(metadata.status.c_str());
    if (-1 == PyDict_SetItemString(pyObj_metadata, "status", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    if (metadata.signature.has_value()) {
        pyObj_tmp = json_decode(metadata.signature.value().c_str(), metadata.signature.value().length());
        if (-1 == PyDict_SetItemString(pyObj_metadata, "signature", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_XDECREF(pyObj_tmp);
    }

    if (metadata.profile.has_value()) {
        pyObj_tmp = json_decode(metadata.profile.value().c_str(), metadata.profile.value().length());
        if (-1 == PyDict_SetItemString(pyObj_metadata, "profile", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_XDECREF(pyObj_tmp);
    }

    if (metadata.warnings.has_value()) {
        PyObject* pyObj_warnings = PyList_New(static_cast<Py_ssize_t>(0));
        for (auto const& warning : metadata.warnings.value()) {
            PyObject* pyObj_warning = PyDict_New();

            pyObj_tmp = PyLong_FromLong(warning.code);
            if (-1 == PyDict_SetItemString(pyObj_warning, "code", pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_XDECREF(pyObj_tmp);

            pyObj_tmp = PyUnicode_FromString(warning.message.c_str());
            if (-1 == PyDict_SetItemString(pyObj_warning, "message", pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_XDECREF(pyObj_tmp);

            if (-1 == PyList_Append(pyObj_warnings, pyObj_warning)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_XDECREF(pyObj_warning);
        }

        if (-1 == PyDict_SetItemString(pyObj_metadata, "warnings", pyObj_warnings)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_XDECREF(pyObj_warnings);
    }

    if (metadata.errors.has_value()) {
        PyObject* pyObj_errors = PyList_New(static_cast<Py_ssize_t>(0));
        for (auto const& error : metadata.errors.value()) {
            PyObject* pyObj_error = PyDict_New();

            pyObj_tmp = PyLong_FromLong(error.code);
            if (-1 == PyDict_SetItemString(pyObj_error, "code", pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_XDECREF(pyObj_tmp);

            pyObj_tmp = PyUnicode_FromString(error.message.c_str());
            if (-1 == PyDict_SetItemString(pyObj_error, "message", pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_XDECREF(pyObj_tmp);

            if (-1 == PyList_Append(pyObj_errors, pyObj_error)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_XDECREF(pyObj_error);
        }

        if (-1 == PyDict_SetItemString(pyObj_metadata, "errors", pyObj_errors)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_XDECREF(pyObj_errors);
    }

    if (include_metrics && metadata.metrics.has_value()) {
        PyObject* pyObject_metrics = get_result_metrics(metadata.metrics.value());

        if (-1 == PyDict_SetItemString(pyObj_metadata, "metrics", pyObject_metrics)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_XDECREF(pyObject_metrics);
    }

    return pyObj_metadata;
}

result*
create_result_from_query_response(couchbase::core::operations::query_response resp, bool include_metrics)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);
    res->ec = resp.ctx.ec;

    PyObject* pyObj_payload = PyDict_New();

    PyObject* pyObject_metadata = get_result_metadata(resp.meta, include_metrics);
    if (-1 == PyDict_SetItemString(pyObj_payload, "metadata", pyObject_metadata)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObject_metadata);

    if (-1 == PyDict_SetItemString(res->dict, RESULT_VALUE, pyObj_payload)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_payload);

    return res;
}

void
create_query_result(couchbase::core::operations::query_response resp,
                    bool include_metrics,
                    std::shared_ptr<rows_queue<PyObject*>> rows,
                    PyObject* pyObj_callback,
                    PyObject* pyObj_errback)
{

    auto set_exception = false;
    PyObject* pyObj_exc = nullptr;
    PyObject* pyObj_args = NULL;
    PyObject* pyObj_func = NULL;
    PyObject* pyObj_callback_res = nullptr;

    PyGILState_STATE state = PyGILState_Ensure();
    if (resp.ctx.ec.value()) {
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "Error doing N1QL operation.");
        // lets clear any errors
        PyErr_Clear();
        rows->put(pyObj_exc);
    } else {
        for (auto const& row : resp.rows) {
            PyObject* pyObj_row = PyBytes_FromStringAndSize(row.c_str(), row.length());
            rows->put(pyObj_row);
        }

        auto res = create_result_from_query_response(resp, include_metrics);
        if (res == nullptr || PyErr_Occurred() != nullptr) {
            set_exception = true;
        } else {
            // None indicates done (i.e. raise StopIteration)
            Py_INCREF(Py_None);
            rows->put(Py_None);
            rows->put(reinterpret_cast<PyObject*>(res));
        }
    }

    if (set_exception) {
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "N1QL operation error.");
        rows->put(pyObj_exc);
    }

    // This is for txcouchbase -- let it knows we're done w/ the query request
    if (pyObj_callback != nullptr) {
        pyObj_func = pyObj_callback;
        pyObj_args = PyTuple_New(1);
        PyTuple_SET_ITEM(pyObj_args, 0, PyBool_FromLong(static_cast<long>(1)));
    }

    if (pyObj_func != nullptr) {
        pyObj_callback_res = PyObject_CallObject(pyObj_func, pyObj_args);
        if (pyObj_callback_res) {
            Py_DECREF(pyObj_callback_res);
        } else {
            pycbc_set_python_exception(PycbcError::InternalSDKError, __FILE__, __LINE__, "N1QL complete callback failed.");
        }
        Py_DECREF(pyObj_args);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }

    PyGILState_Release(state);
}

streamed_result*
handle_n1ql_query([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    // need these for all operations
    PyObject* pyObj_conn = nullptr;
    PyObject* pyObj_query_args = nullptr;
    PyObject* pyObj_callback = nullptr;
    PyObject* pyObj_errback = nullptr;
    PyObject* pyObj_row_callback = nullptr;

    static const char* kw_list[] = { "conn", "query_args", "callback", "errback", "row_callback", nullptr };

    const char* kw_format = "O!|OOOO";
    int ret = PyArg_ParseTupleAndKeywords(args,
                                          kwargs,
                                          kw_format,
                                          const_cast<char**>(kw_list),
                                          &PyCapsule_Type,
                                          &pyObj_conn,
                                          &pyObj_query_args,
                                          &pyObj_callback,
                                          &pyObj_errback,
                                          &pyObj_row_callback);
    if (!ret) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse arguments");
        return nullptr;
    }

    connection* conn = nullptr;
    conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    if (nullptr == conn) {
        PyErr_SetString(PyExc_ValueError, "passed null connection");
        return nullptr;
    }
    PyErr_Clear();

    auto req = build_query_request(pyObj_query_args);
    if (PyErr_Occurred()) {
        return nullptr;
    }

    // PyObjects that need to be around for the cxx client lambda
    // have their increment/decrement handled w/in the callback_context struct
    // struct callback_context callback_ctx = { pyObj_callback, pyObj_errback };
    Py_XINCREF(pyObj_errback);
    Py_XINCREF(pyObj_callback);

    // timeout is always set either to default, or timeout provided in options
    streamed_result* streamed_res = create_streamed_result_obj(req.timeout.value());

    // TODO:  let the couchbase++ streaming stabilize a bit more...
    // req.row_callback = [rows = streamed_res->rows](std::string&& row) {
    //     PyGILState_STATE state = PyGILState_Ensure();
    //     PyObject* pyObj_row = PyBytes_FromStringAndSize(row.c_str(), row.length());
    //     rows->put(pyObj_row);
    //     PyGILState_Release(state);
    //     return couchbase::core::utils::json::stream_control::next_row;
    // };

    {
        Py_BEGIN_ALLOW_THREADS conn->cluster_->execute(
          req,
          [rows = streamed_res->rows, include_metrics = req.metrics, pyObj_callback, pyObj_errback](
            couchbase::core::operations::query_response resp) {
              create_query_result(resp, include_metrics, rows, pyObj_callback, pyObj_errback);
          });
        Py_END_ALLOW_THREADS
    }
    return streamed_res;
}
