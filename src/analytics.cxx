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

#include "analytics.hxx"
#include "exceptions.hxx"
#include "result.hxx"
#include "tracing.hxx"
#include <core/analytics_scan_consistency.hxx>

couchbase::core::analytics_scan_consistency
str_to_scan_consistency_type(std::string consistency)
{
    if (consistency.compare("not_bounded") == 0) {
        return couchbase::core::analytics_scan_consistency::not_bounded;
    }
    if (consistency.compare("request_plus") == 0) {
        return couchbase::core::analytics_scan_consistency::request_plus;
    }

    // TODO: better exception
    PyErr_SetString(PyExc_ValueError, "Invalid Analytics Scan Consistency type.");
    return {};
}

PyObject*
analytics_status_to_string(couchbase::core::operations::analytics_response::analytics_status status)
{
    std::string status_str;
    switch (status) {
        case couchbase::core::operations::analytics_response::analytics_status::running: {
            status_str = "running";
            break;
        }
        case couchbase::core::operations::analytics_response::analytics_status::success: {
            status_str = "success";
            break;
        }
        case couchbase::core::operations::analytics_response::analytics_status::errors: {
            status_str = "errors";
            break;
        }
        case couchbase::core::operations::analytics_response::analytics_status::completed: {
            status_str = "completed";
            break;
        }
        case couchbase::core::operations::analytics_response::analytics_status::stopped: {
            status_str = "stopped";
            break;
        }
        case couchbase::core::operations::analytics_response::analytics_status::timedout: {
            status_str = "timeout";
            break;
        }
        case couchbase::core::operations::analytics_response::analytics_status::closed: {
            status_str = "closed";
            break;
        }
        case couchbase::core::operations::analytics_response::analytics_status::fatal: {
            status_str = "fatal";
            break;
        }
        case couchbase::core::operations::analytics_response::analytics_status::aborted: {
            status_str = "aborted";
            break;
        }
        case couchbase::core::operations::analytics_response::analytics_status::unknown:
        default: {
            status_str = "unknown";
            break;
        }
    }
    // should not be able to reach here, since this is an enum class
    return PyUnicode_FromString(status_str.c_str());
}

PyObject*
get_result_metrics(couchbase::core::operations::analytics_response::analytics_metrics metrics)
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

    pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.error_count);
    if (-1 == PyDict_SetItemString(pyObj_metrics, "error_count", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(metrics.processed_objects);
    if (-1 == PyDict_SetItemString(pyObj_metrics, "processed_objects", pyObj_tmp)) {
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
get_result_metadata(couchbase::core::operations::analytics_response::analytics_meta_data metadata, bool include_metrics)
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

    pyObj_tmp = analytics_status_to_string(metadata.status);
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

    PyObject* pyObj_warnings = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& warning : metadata.warnings) {
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

    PyObject* pyObj_errors = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& error : metadata.errors) {
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

    if (include_metrics) {
        PyObject* pyObject_metrics = get_result_metrics(metadata.metrics);

        if (-1 == PyDict_SetItemString(pyObj_metadata, "metrics", pyObject_metrics)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_XDECREF(pyObject_metrics);
    }

    return pyObj_metadata;
}

result*
create_result_from_analytics_response(couchbase::core::operations::analytics_response resp, bool include_metrics)
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
create_analytics_result(couchbase::core::operations::analytics_response resp,
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
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "Error doing analytics operation.");
        // lets clear any errors
        PyErr_Clear();
        rows->put(pyObj_exc);
    } else {
        for (auto const& row : resp.rows) {
            PyObject* pyObj_row = PyBytes_FromStringAndSize(row.c_str(), row.length());
            rows->put(pyObj_row);
        }

        auto res = create_result_from_analytics_response(resp, include_metrics);
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
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "Analytics operation error.");
        rows->put(pyObj_exc);
    }

    // This is for txcouchbase -- let it knows we're done w/ the analytics request
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
            pycbc_set_python_exception(PycbcError::InternalSDKError, __FILE__, __LINE__, "Analytics complete callback failed.");
        }
        Py_DECREF(pyObj_args);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }

    PyGILState_Release(state);
}

streamed_result*
handle_analytics_query([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    // need these for all operations
    PyObject* pyObj_conn = nullptr;
    char* statement = nullptr;

    char* scan_consistency = nullptr;
    char* bucket_name = nullptr;
    char* scope_name = nullptr;
    char* scope_qualifier = nullptr;
    char* client_context_id = nullptr;

    uint64_t timeout = 0;
    // booleans, but use int to read from kwargs
    int metrics = 0;
    int readonly = 0;
    int priority = 0;

    PyObject* pyObj_raw = nullptr;
    PyObject* pyObj_named_parameters = nullptr;
    PyObject* pyObj_positional_parameters = nullptr;
    PyObject* pyObj_serializer = nullptr;
    PyObject* pyObj_callback = nullptr;
    PyObject* pyObj_errback = nullptr;
    PyObject* pyObj_row_callback = nullptr;
    PyObject* pyObj_span = nullptr;

    static const char* kw_list[] = { "conn",
                                     "statement",
                                     "bucket_name",
                                     "scope_name",
                                     "scope_qualifier",
                                     "client_context_id",
                                     "scan_consistency",
                                     "timeout",
                                     "metrics",
                                     "readonly",
                                     "priority",
                                     "named_parameters",
                                     "positional_parameters",
                                     "raw",
                                     "serializer",
                                     "callback",
                                     "errback",
                                     "row_callback",
                                     "span",
                                     nullptr };

    const char* kw_format = "O!s|sssssLiiiOOOOOOOO";
    int ret = PyArg_ParseTupleAndKeywords(args,
                                          kwargs,
                                          kw_format,
                                          const_cast<char**>(kw_list),
                                          &PyCapsule_Type,
                                          &pyObj_conn,
                                          &statement,
                                          &bucket_name,
                                          &scope_name,
                                          &scope_qualifier,
                                          &client_context_id,
                                          &scan_consistency,
                                          &timeout,
                                          &metrics,
                                          &readonly,
                                          &priority,
                                          &pyObj_named_parameters,
                                          &pyObj_positional_parameters,
                                          &pyObj_raw,
                                          &pyObj_serializer,
                                          &pyObj_callback,
                                          &pyObj_errback,
                                          &pyObj_row_callback,
                                          &pyObj_span);
    if (!ret) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse arguments");
        return nullptr;
    }

    connection* conn = nullptr;
    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::analytics_timeout;

    conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    if (nullptr == conn) {
        PyErr_SetString(PyExc_ValueError, "passed null connection");
        return nullptr;
    }
    PyErr_Clear();

    if (0 < timeout) {
        timeout_ms = std::chrono::milliseconds(std::max(0ULL, timeout / 1000ULL));
    }

    couchbase::core::operations::analytics_request req{ statement };
    // positional parameters
    std::vector<couchbase::core::json_string> positional_parameters{};
    if (pyObj_positional_parameters && PyList_Check(pyObj_positional_parameters)) {
        size_t nargs = static_cast<size_t>(PyList_Size(pyObj_positional_parameters));
        size_t ii;
        for (ii = 0; ii < nargs; ++ii) {
            PyObject* pyOb_param = PyList_GetItem(pyObj_positional_parameters, ii);
            if (!pyOb_param) {
                // TODO:  handle this better
                PyErr_SetString(PyExc_ValueError, "Unable to parse positional argument.");
                return nullptr;
            }
            // PyList_GetItem returns borrowed ref, inc while using, decr after done
            Py_INCREF(pyOb_param);
            if (PyUnicode_Check(pyOb_param)) {
                auto res = std::string(PyUnicode_AsUTF8(pyOb_param));
                positional_parameters.push_back(couchbase::core::json_string{ std::move(res) });
            }
            //@TODO: exception if this check fails??
            Py_DECREF(pyOb_param);
            pyOb_param = nullptr;
        }
    }
    if (positional_parameters.size() > 0) {
        req.positional_parameters = positional_parameters;
    }

    // named parameters
    std::map<std::string, couchbase::core::json_string> named_parameters{};
    if (pyObj_named_parameters && PyDict_Check(pyObj_named_parameters)) {
        PyObject *pyObj_key, *pyObj_value;
        Py_ssize_t pos = 0;

        // PyObj_key and pyObj_value are borrowed references
        while (PyDict_Next(pyObj_named_parameters, &pos, &pyObj_key, &pyObj_value)) {
            std::string k;
            if (PyUnicode_Check(pyObj_key)) {
                k = std::string(PyUnicode_AsUTF8(pyObj_key));
            }
            if (PyUnicode_Check(pyObj_value) && !k.empty()) {
                auto res = std::string(PyUnicode_AsUTF8(pyObj_value));
                named_parameters.emplace(k, couchbase::core::json_string{ std::move(res) });
            }
        }
    }
    if (named_parameters.size() > 0) {
        req.named_parameters = named_parameters;
    }

    req.timeout = timeout_ms;
    // req.metrics = metrics == 1;
    req.readonly = readonly == 1;
    req.priority = priority == 1;

    if (scan_consistency) {
        req.scan_consistency = str_to_scan_consistency_type<couchbase::core::analytics_scan_consistency>(scan_consistency);
    }

    if (scope_qualifier != nullptr) {
        req.scope_qualifier = std::string(scope_qualifier);
    }

    // raw options
    std::map<std::string, couchbase::core::json_string> raw_options{};
    if (pyObj_raw && PyDict_Check(pyObj_raw)) {
        PyObject *pyObj_key, *pyObj_value;
        Py_ssize_t pos = 0;

        // PyObj_key and pyObj_value are borrowed references
        while (PyDict_Next(pyObj_raw, &pos, &pyObj_key, &pyObj_value)) {
            std::string k;
            if (PyUnicode_Check(pyObj_key)) {
                k = std::string(PyUnicode_AsUTF8(pyObj_key));
            }
            if (PyUnicode_Check(pyObj_value) && !k.empty()) {
                auto res = std::string(PyUnicode_AsUTF8(pyObj_value));
                raw_options.emplace(k, couchbase::core::json_string{ std::move(res) });
            }
        }
    }
    if (raw_options.size() > 0) {
        req.raw = raw_options;
    }
    if (nullptr != pyObj_span) {
        req.parent_span = std::make_shared<pycbc::request_span>(pyObj_span);
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
          [rows = streamed_res->rows, include_metrics = metrics, pyObj_callback, pyObj_errback](
            couchbase::core::operations::analytics_response resp) {
              create_analytics_result(resp, include_metrics, rows, pyObj_callback, pyObj_errback);
          });
        Py_END_ALLOW_THREADS
    }
    return streamed_res;
}
