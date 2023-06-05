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

#include "connection.hxx"
#include "exceptions.hxx"
#include "tracing.hxx"
#include "metrics.hxx"
#include <core/io/ip_protocol.hxx>

couchbase::core::io::ip_protocol
pyObj_to_ip_protocol(std::string ip_protocol)
{
    if (ip_protocol.compare("force_ipv4") == 0) {
        return couchbase::core::io::ip_protocol::force_ipv4;
    } else if (ip_protocol.compare("force_ipv6") == 0) {
        return couchbase::core::io::ip_protocol::force_ipv6;
    } else {
        return couchbase::core::io::ip_protocol::any;
    }
}

PyObject*
ip_protocol_to_pyObj(couchbase::core::io::ip_protocol ip_protocol)
{
    if (ip_protocol == couchbase::core::io::ip_protocol::force_ipv4) {
        return PyUnicode_FromString("force_ipv4");
    } else if (ip_protocol == couchbase::core::io::ip_protocol::force_ipv6) {
        return PyUnicode_FromString("force_ipv6");
    } else {
        return PyUnicode_FromString("any");
    }
}

couchbase::core::tls_verify_mode
pyObj_to_tls_verify_mode(std::string tls_verify_mode)
{
    if (tls_verify_mode.compare("none") == 0) {
        return couchbase::core::tls_verify_mode::none;
    } else if (tls_verify_mode.compare("peer") == 0) {
        return couchbase::core::tls_verify_mode::peer;
    } else {
        return couchbase::core::tls_verify_mode::none;
    }
}

PyObject*
tls_verify_mode_to_pyObj(couchbase::core::tls_verify_mode tls_verify_mode)
{
    if (tls_verify_mode == couchbase::core::tls_verify_mode::none) {
        return PyUnicode_FromString("none");
    } else if (tls_verify_mode == couchbase::core::tls_verify_mode::peer) {
        return PyUnicode_FromString("peer");
    } else {
        return PyUnicode_FromString("none");
    }
}

static void
dealloc_conn(PyObject* obj)
{
    auto conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(obj, "conn_"));
    if (conn) {
        auto barrier = std::make_shared<std::promise<void>>();
        auto f = barrier->get_future();
        conn->cluster_->close([barrier]() { barrier->set_value(); });
        f.get();
        conn->io_.stop();
        for (auto& t : conn->io_threads_) {
            if (t.joinable()) {
                t.join();
            }
        }
    }
    CB_LOG_DEBUG("{}: dealloc_conn completed", "PYCBC");
    delete conn;
}

void
bucket_op_callback(std::error_code ec,
                   bool open,
                   PyObject* pyObj_callback,
                   PyObject* pyObj_errback,
                   std::shared_ptr<std::promise<PyObject*>> barrier)
{
    PyObject* pyObj_args = nullptr;
    PyObject* pyObj_func = nullptr;
    PyObject* pyObj_exc = nullptr;
    PyObject* pyObj_callback_res = nullptr;

    PyGILState_STATE state = PyGILState_Ensure();

    if (ec.value()) {
        std::string msg = "Error trying to ";
        msg.append((open ? "open" : "close") + std::string(" bucket."));
        pyObj_exc = pycbc_build_exception(ec, __FILE__, __LINE__, msg);
        if (pyObj_errback == nullptr) {
            barrier->set_value(pyObj_exc);
        } else {
            pyObj_func = pyObj_errback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, pyObj_exc);
        }
    } else {
        if (pyObj_callback == nullptr) {
            barrier->set_value(PyBool_FromLong(static_cast<long>(1)));
        } else {
            pyObj_func = pyObj_callback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, PyBool_FromLong(static_cast<long>(1)));
        }
    }

    if (pyObj_func != nullptr) {
        pyObj_callback_res = PyObject_CallObject(pyObj_func, pyObj_args);
        if (pyObj_callback_res) {
            Py_DECREF(pyObj_callback_res);
        } else {
            std::string msg;
            msg.append((open ? "Open" : "Close") + std::string(" bucket callback failed"));
            pycbc_set_python_exception(PycbcError::InternalSDKError, __FILE__, __LINE__, msg.c_str());
        }
        Py_DECREF(pyObj_args);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }
    CB_LOG_DEBUG("{}: open/close bucket callback completed", "PYCBC");
    PyGILState_Release(state);
}

void
close_connection_callback(PyObject* pyObj_conn,
                          PyObject* pyObj_callback,
                          PyObject* pyObj_errback,
                          std::shared_ptr<std::promise<PyObject*>> barrier)
{
    PyObject* pyObj_args = NULL;
    PyObject* pyObj_func = NULL;
    PyObject* pyObj_callback_res = nullptr;

    PyGILState_STATE state = PyGILState_Ensure();

    if (pyObj_callback == nullptr) {
        barrier->set_value(PyBool_FromLong(static_cast<long>(1)));
    } else {
        pyObj_func = pyObj_callback;
        pyObj_args = PyTuple_New(1);
        PyTuple_SET_ITEM(pyObj_args, 0, PyBool_FromLong(static_cast<long>(1)));
    }

    if (pyObj_func != nullptr) {
        pyObj_callback_res = PyObject_CallObject(pyObj_func, pyObj_args);
        CB_LOG_DEBUG("{}: return from close conn callback.", "PYCBC");
        if (pyObj_callback_res) {
            Py_DECREF(pyObj_callback_res);
        } else {
            pycbc_set_python_exception(PycbcError::InternalSDKError, __FILE__, __LINE__, "Close connection callback failed.");
        }
        Py_DECREF(pyObj_args);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }
    CB_LOG_DEBUG("{}: close conn callback completed", "PYCBC");
    auto conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    conn->io_.stop();
    // the pyObj_conn was incref'd before being passed into this callback, decref it here
    Py_DECREF(pyObj_conn);
    PyGILState_Release(state);
}

void
create_connection_callback(PyObject* pyObj_conn,
                           std::error_code ec,
                           PyObject* pyObj_callback,
                           PyObject* pyObj_errback,
                           std::shared_ptr<std::promise<PyObject*>> barrier)
{
    PyObject* pyObj_args = NULL;
    PyObject* pyObj_func = NULL;
    PyObject* pyObj_exc = nullptr;
    PyObject* pyObj_callback_res = nullptr;

    PyGILState_STATE state = PyGILState_Ensure();

    if (ec.value()) {
        pyObj_exc = pycbc_build_exception(ec, __FILE__, __LINE__, "Error creating a connection.");
        if (pyObj_errback == nullptr) {
            barrier->set_value(pyObj_exc);
        } else {
            pyObj_func = pyObj_errback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, pyObj_exc);
        }
    } else {
        if (pyObj_callback == nullptr) {
            barrier->set_value(pyObj_conn);
        } else {
            pyObj_func = pyObj_callback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, pyObj_conn);
        }
    }

    if (pyObj_func != nullptr) {
        pyObj_callback_res = PyObject_CallObject(pyObj_func, pyObj_args);
        if (pyObj_callback_res) {
            Py_DECREF(pyObj_callback_res);
        } else {
            pycbc_set_python_exception(PycbcError::InternalSDKError, __FILE__, __LINE__, "Create connection callback failed.");
        }
        Py_DECREF(pyObj_args);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }
    Py_DECREF(pyObj_conn);
    CB_LOG_DEBUG("{}: create conn callback completed", "PYCBC");
    PyGILState_Release(state);
}

couchbase::core::cluster_credentials
get_cluster_credentials(PyObject* pyObj_auth)
{
    couchbase::core::cluster_credentials auth{};
    PyObject* pyObj_username = PyDict_GetItemString(pyObj_auth, "username");
    if (pyObj_username != nullptr) {
        auto username = std::string(PyUnicode_AsUTF8(pyObj_username));
        auth.username = username;
    }

    PyObject* pyObj_password = PyDict_GetItemString(pyObj_auth, "password");
    if (pyObj_password != nullptr) {
        auto pw = std::string(PyUnicode_AsUTF8(pyObj_password));
        auth.password = pw;
    }

    PyObject* pyObj_cert_path = PyDict_GetItemString(pyObj_auth, "cert_path");
    if (pyObj_cert_path != nullptr) {
        auto cert_path = std::string(PyUnicode_AsUTF8(pyObj_cert_path));
        auth.certificate_path = cert_path;
    }

    PyObject* pyObj_key_path = PyDict_GetItemString(pyObj_auth, "key_path");
    if (pyObj_key_path != nullptr) {
        auto key_path = std::string(PyUnicode_AsUTF8(pyObj_key_path));
        auth.key_path = key_path;
    }

    PyObject* pyObj_allowed_sasl_mechanisms = PyDict_GetItemString(pyObj_auth, "allowed_sasl_mechanisms");
    if (pyObj_allowed_sasl_mechanisms != nullptr && PyList_Check(pyObj_allowed_sasl_mechanisms)) {
        if (auth.allowed_sasl_mechanisms.has_value()) {
            auth.allowed_sasl_mechanisms.value().clear();
        }
        size_t nargs = static_cast<size_t>(PyList_Size(pyObj_allowed_sasl_mechanisms));
        size_t ii;
        std::vector<std::string> allowed_sasl_mechanisms{};
        for (ii = 0; ii < nargs; ++ii) {
            PyObject* pyOb_mechanism = PyList_GetItem(pyObj_allowed_sasl_mechanisms, ii);
            auto mechanism = std::string(PyUnicode_AsUTF8(pyOb_mechanism));
            allowed_sasl_mechanisms.emplace_back(mechanism);
        }
        auth.allowed_sasl_mechanisms = allowed_sasl_mechanisms;
    }

    return auth;
}

PyObject*
get_metrics_options(const couchbase::core::metrics::logging_meter_options& logging_options)
{
    PyObject* pyObj_opts = PyDict_New();
    std::chrono::duration<unsigned long long, std::milli> int_msec = logging_options.emit_interval;
    PyObject* pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "emit_interval", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    return pyObj_opts;
}

void
update_cluster_logging_meter_options(couchbase::core::cluster_options& options, PyObject* pyObj_emit_interval)
{
    couchbase::core::metrics::logging_meter_options logging_options{};
    bool has_logging_meter_options = false;

    if (pyObj_emit_interval != nullptr) {
        auto emit_interval = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_emit_interval));
        auto emit_interval_ms = std::chrono::milliseconds(std::max(0ULL, emit_interval / 1000ULL));
        logging_options.emit_interval = emit_interval_ms;
        has_logging_meter_options = true;
    }

    if (has_logging_meter_options) {
        options.metrics_options = logging_options;
    }
}

PyObject*
get_tracing_options(const couchbase::core::tracing::threshold_logging_options& tracing_options)
{
    PyObject* pyObj_opts = PyDict_New();
    std::chrono::duration<unsigned long long, std::milli> int_msec = tracing_options.orphaned_emit_interval;
    PyObject* pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "orphaned_emit_interval", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromSize_t(tracing_options.orphaned_sample_size);
    if (-1 == PyDict_SetItemString(pyObj_opts, "orphaned_sample_size", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = tracing_options.threshold_emit_interval;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "threshold_emit_interval", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromSize_t(tracing_options.threshold_sample_size);
    if (-1 == PyDict_SetItemString(pyObj_opts, "threshold_sample_size", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = tracing_options.key_value_threshold;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "key_value_threshold", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = tracing_options.query_threshold;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "query_threshold", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = tracing_options.view_threshold;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "view_threshold", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = tracing_options.search_threshold;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "search_threshold", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = tracing_options.analytics_threshold;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "analytics_threshold", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = tracing_options.management_threshold;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "management_threshold", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = tracing_options.eventing_threshold;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "eventing_threshold", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    return pyObj_opts;
}

void
update_cluster_tracing_options(couchbase::core::cluster_options& options, PyObject* pyObj_tracing_opts)
{
    couchbase::core::tracing::threshold_logging_options tracing_options{};
    bool has_tracing_options = false;

    PyObject* pyObj_kv_threshold = PyDict_GetItemString(pyObj_tracing_opts, "key_value_threshold");
    if (pyObj_kv_threshold != nullptr) {
        auto kv_threshold = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_kv_threshold));
        auto kv_threshold_ms = std::chrono::milliseconds(std::max(0ULL, kv_threshold / 1000ULL));
        tracing_options.key_value_threshold = kv_threshold_ms;
        has_tracing_options = true;
    }

    PyObject* pyObj_view_threshold = PyDict_GetItemString(pyObj_tracing_opts, "view_threshold");
    if (pyObj_view_threshold != nullptr) {
        auto view_threshold = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_view_threshold));
        auto view_threshold_ms = std::chrono::milliseconds(std::max(0ULL, view_threshold / 1000ULL));
        tracing_options.view_threshold = view_threshold_ms;
        has_tracing_options = true;
    }

    PyObject* pyObj_query_threshold = PyDict_GetItemString(pyObj_tracing_opts, "query_threshold");
    if (pyObj_query_threshold != nullptr) {
        auto query_threshold = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_query_threshold));
        auto query_threshold_ms = std::chrono::milliseconds(std::max(0ULL, query_threshold / 1000ULL));
        tracing_options.query_threshold = query_threshold_ms;
        has_tracing_options = true;
    }

    PyObject* pyObj_search_threshold = PyDict_GetItemString(pyObj_tracing_opts, "search_threshold");
    if (pyObj_search_threshold != nullptr) {
        auto search_threshold = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_search_threshold));
        auto search_threshold_ms = std::chrono::milliseconds(std::max(0ULL, search_threshold / 1000ULL));
        tracing_options.search_threshold = search_threshold_ms;
        has_tracing_options = true;
    }

    PyObject* pyObj_analytics_threshold = PyDict_GetItemString(pyObj_tracing_opts, "analytics_threshold");
    if (pyObj_analytics_threshold != nullptr) {
        auto analytics_threshold = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_analytics_threshold));
        auto analytics_threshold_ms = std::chrono::milliseconds(std::max(0ULL, analytics_threshold / 1000ULL));
        tracing_options.analytics_threshold = analytics_threshold_ms;
        has_tracing_options = true;
    }

    PyObject* pyObj_eventing_threshold = PyDict_GetItemString(pyObj_tracing_opts, "eventing_threshold");
    if (pyObj_eventing_threshold != nullptr) {
        auto eventing_threshold = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_eventing_threshold));
        auto eventing_threshold_ms = std::chrono::milliseconds(std::max(0ULL, eventing_threshold / 1000ULL));
        tracing_options.eventing_threshold = eventing_threshold_ms;
        has_tracing_options = true;
    }

    PyObject* pyObj_management_threshold = PyDict_GetItemString(pyObj_tracing_opts, "management_threshold");
    if (pyObj_management_threshold != nullptr) {
        auto management_threshold = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_management_threshold));
        auto management_threshold_ms = std::chrono::milliseconds(std::max(0ULL, management_threshold / 1000ULL));
        tracing_options.management_threshold = management_threshold_ms;
        has_tracing_options = true;
    }

    PyObject* pyObj_threshold_sample_size = PyDict_GetItemString(pyObj_tracing_opts, "threshold_sample_size");
    if (pyObj_threshold_sample_size != nullptr) {
        auto threshold_sample_size = static_cast<size_t>(PyLong_AsUnsignedLong(pyObj_threshold_sample_size));
        tracing_options.threshold_sample_size = threshold_sample_size;
        has_tracing_options = true;
    }

    PyObject* pyObj_threshold_emit_interval = PyDict_GetItemString(pyObj_tracing_opts, "threshold_emit_interval");
    if (pyObj_threshold_emit_interval != nullptr) {
        auto threshold_emit_interval = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_threshold_emit_interval));
        auto threshold_emit_interval_ms = std::chrono::milliseconds(std::max(0ULL, threshold_emit_interval / 1000ULL));
        tracing_options.threshold_emit_interval = threshold_emit_interval_ms;
        has_tracing_options = true;
    }

    PyObject* pyObj_orphaned_emit_interval = PyDict_GetItemString(pyObj_tracing_opts, "orphaned_emit_interval");
    if (pyObj_orphaned_emit_interval != nullptr) {
        auto orphaned_emit_interval = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_orphaned_emit_interval));
        auto orphaned_emit_interval_ms = std::chrono::milliseconds(std::max(0ULL, orphaned_emit_interval / 1000ULL));
        tracing_options.orphaned_emit_interval = orphaned_emit_interval_ms;
        has_tracing_options = true;
    }

    PyObject* pyObj_orphaned_sample_size = PyDict_GetItemString(pyObj_tracing_opts, "orphaned_sample_size");
    if (pyObj_orphaned_sample_size != nullptr) {
        auto orphaned_sample_size = static_cast<size_t>(PyLong_AsUnsignedLong(pyObj_orphaned_sample_size));
        tracing_options.orphaned_sample_size = orphaned_sample_size;
        has_tracing_options = true;
    }

    if (has_tracing_options) {
        options.tracing_options = tracing_options;
    }
}

void
update_cluster_timeout_options(couchbase::core::cluster_options& options, PyObject* pyObj_timeout_opts)
{
    PyObject* pyObj_bootstrap_timeout = PyDict_GetItemString(pyObj_timeout_opts, "bootstrap_timeout");
    if (pyObj_bootstrap_timeout != nullptr) {
        auto bootstrap_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_bootstrap_timeout));
        auto bootstrap_timeout_ms = std::chrono::milliseconds(std::max(0ULL, bootstrap_timeout / 1000ULL));
        options.bootstrap_timeout = bootstrap_timeout_ms;
    }

    PyObject* pyObj_resolve_timeout = PyDict_GetItemString(pyObj_timeout_opts, "resolve_timeout");
    if (pyObj_resolve_timeout != nullptr) {
        auto resolve_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_resolve_timeout));
        auto resolve_timeout_ms = std::chrono::milliseconds(std::max(0ULL, resolve_timeout / 1000ULL));
        options.resolve_timeout = resolve_timeout_ms;
    }

    PyObject* pyObj_connect_timeout = PyDict_GetItemString(pyObj_timeout_opts, "connect_timeout");
    if (pyObj_connect_timeout != nullptr) {
        auto connect_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_connect_timeout));
        auto connect_timeout_ms = std::chrono::milliseconds(std::max(0ULL, connect_timeout / 1000ULL));
        options.connect_timeout = connect_timeout_ms;
    }

    PyObject* pyObj_key_value_timeout = PyDict_GetItemString(pyObj_timeout_opts, "key_value_timeout");
    if (pyObj_key_value_timeout != nullptr) {
        auto key_value_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_key_value_timeout));
        auto key_value_timeout_ms = std::chrono::milliseconds(std::max(0ULL, key_value_timeout / 1000ULL));
        options.key_value_timeout = key_value_timeout_ms;
    }

    PyObject* pyObj_key_value_durable_timeout = PyDict_GetItemString(pyObj_timeout_opts, "key_value_durable_timeout");
    if (pyObj_key_value_durable_timeout != nullptr) {
        auto key_value_durable_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_key_value_durable_timeout));
        auto key_value_durable_timeout_ms = std::chrono::milliseconds(std::max(0ULL, key_value_durable_timeout / 1000ULL));
        options.key_value_durable_timeout = key_value_durable_timeout_ms;
    }

    PyObject* pyObj_view_timeout = PyDict_GetItemString(pyObj_timeout_opts, "view_timeout");
    if (pyObj_view_timeout != nullptr) {
        auto view_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_view_timeout));
        auto view_timeout_ms = std::chrono::milliseconds(std::max(0ULL, view_timeout / 1000ULL));
        options.view_timeout = view_timeout_ms;
    }

    PyObject* pyObj_query_timeout = PyDict_GetItemString(pyObj_timeout_opts, "query_timeout");
    if (pyObj_query_timeout != nullptr) {
        auto query_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_query_timeout));
        auto query_timeout_ms = std::chrono::milliseconds(std::max(0ULL, query_timeout / 1000ULL));
        options.query_timeout = query_timeout_ms;
    }

    PyObject* pyObj_analytics_timeout = PyDict_GetItemString(pyObj_timeout_opts, "analytics_timeout");
    if (pyObj_analytics_timeout != nullptr) {
        auto analytics_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_analytics_timeout));
        auto analytics_timeout_ms = std::chrono::milliseconds(std::max(0ULL, analytics_timeout / 1000ULL));
        options.analytics_timeout = analytics_timeout_ms;
    }

    PyObject* pyObj_search_timeout = PyDict_GetItemString(pyObj_timeout_opts, "search_timeout");
    if (pyObj_search_timeout != nullptr) {
        auto search_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_search_timeout));
        auto search_timeout_ms = std::chrono::milliseconds(std::max(0ULL, search_timeout / 1000ULL));
        options.search_timeout = search_timeout_ms;
    }

    PyObject* pyObj_management_timeout = PyDict_GetItemString(pyObj_timeout_opts, "management_timeout");
    if (pyObj_management_timeout != nullptr) {
        auto management_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_management_timeout));
        auto management_timeout_ms = std::chrono::milliseconds(std::max(0ULL, management_timeout / 1000ULL));
        options.management_timeout = management_timeout_ms;
    }

    PyObject* pyObj_idle_http_connection_timeout = PyDict_GetItemString(pyObj_timeout_opts, "idle_http_connection_timeout");
    if (pyObj_idle_http_connection_timeout != nullptr) {
        auto idle_http_connection_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_idle_http_connection_timeout));
        auto idle_http_connection_timeout_ms = std::chrono::milliseconds(std::max(0ULL, idle_http_connection_timeout / 1000ULL));
        options.idle_http_connection_timeout = idle_http_connection_timeout_ms;
    }

    PyObject* pyObj_config_idle_redial_timeout = PyDict_GetItemString(pyObj_timeout_opts, "config_idle_redial_timeout");
    if (pyObj_config_idle_redial_timeout != nullptr) {
        auto config_idle_redial_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_config_idle_redial_timeout));
        auto config_idle_redial_timeout_ms = std::chrono::milliseconds(std::max(0ULL, config_idle_redial_timeout / 1000ULL));
        options.config_idle_redial_timeout = config_idle_redial_timeout_ms;
    }
}

void
update_cluster_options(couchbase::core::cluster_options& options, PyObject* pyObj_options, PyObject* pyObj_auth)
{
    PyObject* pyObj_timeout_opts = PyDict_GetItemString(pyObj_options, "timeout_options");
    if (pyObj_timeout_opts != nullptr) {
        update_cluster_timeout_options(options, pyObj_timeout_opts);
    }

    PyObject* pyObj_tracing_opts = PyDict_GetItemString(pyObj_options, "tracing_options");
    if (pyObj_tracing_opts != nullptr) {
        update_cluster_tracing_options(options, pyObj_tracing_opts);
    }

    PyObject* pyObj_emit_interval = PyDict_GetItemString(pyObj_options, "emit_interval");
    if (pyObj_emit_interval != nullptr) {
        update_cluster_logging_meter_options(options, pyObj_emit_interval);
    }

    PyObject* pyObj_enable_tls = PyDict_GetItemString(pyObj_options, "enable_tls");
    if (pyObj_enable_tls != nullptr && pyObj_enable_tls == Py_True) {
        options.enable_tls = true;
    }

    PyObject* pyObj_trust_store_path = PyDict_GetItemString(pyObj_auth, "trust_store_path");
    if (pyObj_trust_store_path != nullptr) {
        auto trust_store_path = std::string(PyUnicode_AsUTF8(pyObj_trust_store_path));
        options.trust_certificate = trust_store_path;
    } else {
        pyObj_trust_store_path = PyDict_GetItemString(pyObj_options, "trust_store_path");
        if (pyObj_trust_store_path != nullptr) {
            auto trust_store_path = std::string(PyUnicode_AsUTF8(pyObj_trust_store_path));
            options.trust_certificate = trust_store_path;
        }
    }

    PyObject* pyObj_disable_mozilla_ca_certificates = PyDict_GetItemString(pyObj_options, "disable_mozilla_ca_certificates");
    if (pyObj_disable_mozilla_ca_certificates != nullptr && pyObj_disable_mozilla_ca_certificates == Py_True) {
        options.disable_mozilla_ca_certificates = true;
    }

    PyObject* pyObj_enable_mut_tokens = PyDict_GetItemString(pyObj_options, "enable_mutation_tokens");
    if (pyObj_enable_mut_tokens != nullptr && pyObj_enable_mut_tokens == Py_False) {
        options.enable_mutation_tokens = false;
    }

    PyObject* pyObj_enable_tcp_keep_alive = PyDict_GetItemString(pyObj_options, "enable_tcp_keep_alive");
    if (pyObj_enable_tcp_keep_alive != nullptr && pyObj_enable_tcp_keep_alive == Py_False) {
        options.enable_tcp_keep_alive = false;
    }

    PyObject* pyObj_use_ip_protocol = PyDict_GetItemString(pyObj_options, "use_ip_protocol");
    if (pyObj_use_ip_protocol != nullptr) {
        options.use_ip_protocol = pyObj_to_ip_protocol(std::string(PyUnicode_AsUTF8(pyObj_use_ip_protocol)));
    }

    PyObject* pyObj_enable_dns_srv = PyDict_GetItemString(pyObj_options, "enable_dns_srv");
    if (pyObj_enable_dns_srv != nullptr && pyObj_enable_dns_srv == Py_False) {
        options.enable_dns_srv = false;
    }

    PyObject* pyObj_show_queries = PyDict_GetItemString(pyObj_options, "show_queries");
    if (pyObj_show_queries != nullptr && pyObj_show_queries == Py_True) {
        options.show_queries = true;
    }

    PyObject* pyObj_enable_unordered_execution = PyDict_GetItemString(pyObj_options, "enable_unordered_execution");
    if (pyObj_enable_unordered_execution != nullptr && pyObj_enable_unordered_execution == Py_False) {
        options.enable_unordered_execution = false;
    }

    PyObject* pyObj_enable_clustermap_notification = PyDict_GetItemString(pyObj_options, "enable_clustermap_notification");
    if (pyObj_enable_clustermap_notification != nullptr && pyObj_enable_clustermap_notification == Py_False) {
        options.enable_clustermap_notification = false;
    }

    PyObject* pyObj_enable_compression = PyDict_GetItemString(pyObj_options, "enable_compression");
    if (pyObj_enable_compression != nullptr && pyObj_enable_compression == Py_False) {
        options.enable_compression = false;
    }

    PyObject* pyObj_enable_tracing = PyDict_GetItemString(pyObj_options, "enable_tracing");
    if (pyObj_enable_tracing != nullptr && pyObj_enable_tracing == Py_False) {
        options.enable_tracing = false;
    }

    PyObject* pyObj_enable_metrics = PyDict_GetItemString(pyObj_options, "enable_metrics");
    if (pyObj_enable_metrics != nullptr && pyObj_enable_metrics == Py_False) {
        options.enable_metrics = false;
    }

    PyObject* pyObj_network = PyDict_GetItemString(pyObj_options, "network");
    if (pyObj_network != nullptr) {
        auto network = std::string(PyUnicode_AsUTF8(pyObj_network));
        options.network = network;
    }

    PyObject* pyObj_tls_verify = PyDict_GetItemString(pyObj_options, "tls_verify");
    if (pyObj_tls_verify != nullptr) {
        options.tls_verify = pyObj_to_tls_verify_mode(std::string(PyUnicode_AsUTF8(pyObj_tls_verify)));
    }

    PyObject* pyObj_tcp_keep_alive_interval = PyDict_GetItemString(pyObj_options, "tcp_keep_alive_interval");
    if (pyObj_tcp_keep_alive_interval != nullptr) {
        auto tcp_keep_alive_interval = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_tcp_keep_alive_interval));
        auto tcp_keep_alive_interval_ms = std::chrono::milliseconds(std::max(0ULL, tcp_keep_alive_interval / 1000ULL));
        options.tcp_keep_alive_interval = tcp_keep_alive_interval_ms;
    }

    PyObject* pyObj_config_poll_interval = PyDict_GetItemString(pyObj_options, "config_poll_interval");
    if (pyObj_config_poll_interval != nullptr) {
        auto config_poll_interval = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_config_poll_interval));
        auto config_poll_interval_ms = std::chrono::milliseconds(std::max(0ULL, config_poll_interval / 1000ULL));
        options.config_poll_interval = config_poll_interval_ms;
    }

    PyObject* pyObj_config_poll_floor = PyDict_GetItemString(pyObj_options, "config_poll_floor");
    if (pyObj_config_poll_floor != nullptr) {
        auto config_poll_floor = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_config_poll_floor));
        auto config_poll_floor_ms = std::chrono::milliseconds(std::max(0ULL, config_poll_floor / 1000ULL));
        options.config_poll_floor = config_poll_floor_ms;
    }

    PyObject* pyObj_user_agent_extra = PyDict_GetItemString(pyObj_options, "user_agent_extra");
    if (pyObj_user_agent_extra != nullptr) {
        auto user_agent_extra = std::string(PyUnicode_AsUTF8(pyObj_user_agent_extra));
        options.user_agent_extra = user_agent_extra;
    }

    PyObject* pyObj_max_http_connections = PyDict_GetItemString(pyObj_options, "max_http_connections");
    if (pyObj_max_http_connections != nullptr) {
        auto max_http_connections = static_cast<size_t>(PyLong_AsUnsignedLong(pyObj_max_http_connections));
        options.max_http_connections = max_http_connections;
    }

    PyObject* pyObj_tracer = PyDict_GetItemString(pyObj_options, "tracer");
    if (pyObj_tracer != nullptr) {
        options.tracer = std::make_shared<pycbc::request_tracer>(pyObj_tracer);
    }

    PyObject* pyObj_meter = PyDict_GetItemString(pyObj_options, "meter");
    if (pyObj_meter != nullptr) {
        options.meter = std::make_shared<pycbc::meter>(pyObj_meter);
    }

    PyObject* pyObj_dns_nameserver = PyDict_GetItemString(pyObj_options, "dns_nameserver");
    PyObject* pyObj_dns_port = PyDict_GetItemString(pyObj_options, "dns_port");
    PyObject* pyObj_dns_srv_timeout = nullptr;
    if (pyObj_timeout_opts != nullptr) {
        pyObj_dns_srv_timeout = PyDict_GetItemString(pyObj_timeout_opts, "dns_srv_timeout");
    }
    if (pyObj_dns_srv_timeout != nullptr || pyObj_dns_nameserver != nullptr || pyObj_dns_port != nullptr) {
        auto nameserver =
          pyObj_dns_nameserver != nullptr ? std::string(PyUnicode_AsUTF8(pyObj_dns_nameserver)) : options.dns_config.nameserver();
        auto port = pyObj_dns_port != nullptr ? static_cast<uint16_t>(PyLong_AsUnsignedLong(pyObj_dns_port)) : options.dns_config.port();
        auto dns_srv_timeout_ms = couchbase::core::timeout_defaults::dns_srv_timeout;
        if (pyObj_dns_srv_timeout != nullptr) {
            auto dns_srv_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_dns_srv_timeout));
            dns_srv_timeout_ms = std::chrono::milliseconds(std::max(0ULL, dns_srv_timeout / 1000ULL));
        }
        options.dns_config = couchbase::core::io::dns::dns_config(nameserver, port, dns_srv_timeout_ms);
    }

    PyObject* pyObj_dump_configuration = PyDict_GetItemString(pyObj_options, "dump_configuration");
    if (pyObj_dump_configuration != nullptr && pyObj_dump_configuration == Py_True) {
        options.dump_configuration = true;
    }
}

PyObject*
handle_create_connection([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    char* conn_str = nullptr;
    PyObject* pyObj_auth = nullptr;
    PyObject* pyObj_options = nullptr;
    PyObject* pyObj_callback = nullptr;
    PyObject* pyObj_errback = nullptr;
    PyObject* pyObj_result = nullptr;

    static const char* kw_list[] = { "", "auth", "options", "callback", "errback", nullptr };

    const char* kw_format = "s|OOOO";
    int ret = PyArg_ParseTupleAndKeywords(
      args, kwargs, kw_format, const_cast<char**>(kw_list), &conn_str, &pyObj_auth, &pyObj_options, &pyObj_callback, &pyObj_errback);

    if (!ret) {
        std::string msg = "Cannot create connection. Unable to parse args/kwargs.";
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, msg.c_str());
        return nullptr;
    }

    couchbase::core::utils::connection_string connection_str = couchbase::core::utils::parse_connection_string(conn_str);
    couchbase::core::cluster_credentials auth = get_cluster_credentials(pyObj_auth);
    try {
        update_cluster_options(connection_str.options, pyObj_options, pyObj_auth);
    } catch (const std::invalid_argument& e) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, e.what());
        return nullptr;
    } catch (const std::exception& e) {
        PyErr_SetString(PyExc_Exception, e.what());
        return nullptr;
    }

    PyObject* pyObj_num_io_threads = PyDict_GetItemString(pyObj_options, "num_io_threads");
    int num_io_threads = 1;
    if (pyObj_num_io_threads != nullptr) {
        num_io_threads = static_cast<uint32_t>(PyLong_AsUnsignedLong(pyObj_num_io_threads));
    }

    connection* const conn = new connection(num_io_threads);
    PyObject* pyObj_conn = PyCapsule_New(conn, "conn_", dealloc_conn);

    if (pyObj_conn == nullptr) {
        pycbc_set_python_exception(
          PycbcError::InternalSDKError, __FILE__, __LINE__, "Cannot create connection. Unable to create PyCapsule.");
        return nullptr;
    }

    // PyObjects that need to be around for the cxx client lambda
    // have their increment/decrement handled w/in the callback_context struct
    // struct callback_context callback_ctx = { pyObj_callback, pyObj_errback };
    Py_XINCREF(pyObj_callback);
    Py_XINCREF(pyObj_errback);

    Py_XINCREF(pyObj_conn);
    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();
    {
        int callback_count = 0;
        Py_BEGIN_ALLOW_THREADS conn->cluster_->open(
          couchbase::core::origin(auth, connection_str),
          [pyObj_conn, pyObj_callback, pyObj_errback, callback_count, barrier](std::error_code ec) mutable {
              if (callback_count == 0) {
                  create_connection_callback(pyObj_conn, ec, pyObj_callback, pyObj_errback, barrier);
              }
              callback_count++;
          });
        Py_END_ALLOW_THREADS
    }
    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        PyObject* ret = nullptr;
        Py_BEGIN_ALLOW_THREADS ret = f.get();
        Py_END_ALLOW_THREADS return ret;
    }
    Py_RETURN_NONE;
}

PyObject*
get_connection_info([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* pyObj_conn = nullptr;
    static const char* kw_list[] = { "", nullptr };

    const char* kw_format = "O!";
    int ret = PyArg_ParseTupleAndKeywords(args, kwargs, kw_format, const_cast<char**>(kw_list), &PyCapsule_Type, &pyObj_conn);

    if (!ret) {
        std::string msg = "Cannot get connection options. Unable to parse args/kwargs.";
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, msg.c_str());
        return nullptr;
    }

    connection* conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    if (nullptr == conn) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, NULL_CONN_OBJECT);
        return nullptr;
    }

    auto cluster_info = conn->cluster_->origin();
    if (cluster_info.first) {
        Py_RETURN_NONE;
    }
    auto opts = cluster_info.second.options();
    PyObject* pyObj_opts = PyDict_New();
    std::chrono::duration<unsigned long long, std::milli> int_msec = opts.bootstrap_timeout;
    PyObject* pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "bootstrap_timeout", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = opts.resolve_timeout;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "resolve_timeout", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = opts.connect_timeout;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "connect_timeout", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = opts.key_value_timeout;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "key_value_timeout", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = opts.key_value_durable_timeout;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "key_value_durable_timeout", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = opts.view_timeout;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "view_timeout", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = opts.query_timeout;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "query_timeout", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = opts.analytics_timeout;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "analytics_timeout", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = opts.search_timeout;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "search_timeout", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = opts.management_timeout;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "management_timeout", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = opts.dns_config.timeout();
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "dns_srv_timeout", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    if (-1 == PyDict_SetItemString(pyObj_opts, "enable_tls", opts.enable_tls ? Py_True : Py_False)) {
        PyErr_Print();
        PyErr_Clear();
    }

    pyObj_tmp = PyUnicode_FromString(opts.trust_certificate.c_str());
    if (-1 == PyDict_SetItemString(pyObj_opts, "trust_certificate", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    if (-1 ==
        PyDict_SetItemString(pyObj_opts, "disable_mozilla_ca_certificates", opts.disable_mozilla_ca_certificates ? Py_True : Py_False)) {
        PyErr_Print();
        PyErr_Clear();
    }

    if (-1 == PyDict_SetItemString(pyObj_opts, "enable_mutation_tokens", opts.enable_mutation_tokens ? Py_True : Py_False)) {
        PyErr_Print();
        PyErr_Clear();
    }

    if (-1 == PyDict_SetItemString(pyObj_opts, "enable_tcp_keep_alive", opts.enable_tcp_keep_alive ? Py_True : Py_False)) {
        PyErr_Print();
        PyErr_Clear();
    }

    pyObj_tmp = ip_protocol_to_pyObj(opts.use_ip_protocol);
    if (-1 == PyDict_SetItemString(pyObj_opts, "ip_protocol", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    if (-1 == PyDict_SetItemString(pyObj_opts, "enable_dns_srv", opts.enable_dns_srv ? Py_True : Py_False)) {
        PyErr_Print();
        PyErr_Clear();
    }

    if (-1 == PyDict_SetItemString(pyObj_opts, "show_queries", opts.show_queries ? Py_True : Py_False)) {
        PyErr_Print();
        PyErr_Clear();
    }

    if (-1 == PyDict_SetItemString(pyObj_opts, "enable_unordered_execution", opts.enable_unordered_execution ? Py_True : Py_False)) {
        PyErr_Print();
        PyErr_Clear();
    }

    if (-1 ==
        PyDict_SetItemString(pyObj_opts, "enable_clustermap_notification", opts.enable_clustermap_notification ? Py_True : Py_False)) {
        PyErr_Print();
        PyErr_Clear();
    }

    if (-1 == PyDict_SetItemString(pyObj_opts, "enable_compression", opts.enable_compression ? Py_True : Py_False)) {
        PyErr_Print();
        PyErr_Clear();
    }

    if (-1 == PyDict_SetItemString(pyObj_opts, "enable_tracing", opts.enable_tracing ? Py_True : Py_False)) {
        PyErr_Print();
        PyErr_Clear();
    }

    if (-1 == PyDict_SetItemString(pyObj_opts, "enable_metrics", opts.enable_metrics ? Py_True : Py_False)) {
        PyErr_Print();
        PyErr_Clear();
    }

    pyObj_tmp = PyUnicode_FromString(opts.network.c_str());
    if (-1 == PyDict_SetItemString(pyObj_opts, "network", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = get_tracing_options(opts.tracing_options);
    if (-1 == PyDict_SetItemString(pyObj_opts, "tracing_options", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = get_metrics_options(opts.metrics_options);
    if (-1 == PyDict_SetItemString(pyObj_opts, "metrics_options", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = tls_verify_mode_to_pyObj(opts.tls_verify);
    if (-1 == PyDict_SetItemString(pyObj_opts, "tls_verify", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    if (-1 == PyDict_SetItemString(pyObj_opts, "has_tracer", opts.tracer != nullptr ? Py_True : Py_False)) {
        PyErr_Print();
        PyErr_Clear();
    }

    if (-1 == PyDict_SetItemString(pyObj_opts, "has_meter", opts.meter != nullptr ? Py_True : Py_False)) {
        PyErr_Print();
        PyErr_Clear();
    }

    int_msec = opts.tcp_keep_alive_interval;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "tcp_keep_alive_interval", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = opts.config_poll_interval;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "config_poll_interval", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = opts.config_poll_floor;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "config_poll_floor", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = opts.config_idle_redial_timeout;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "config_idle_redial_timeout", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromSize_t(opts.max_http_connections);
    if (-1 == PyDict_SetItemString(pyObj_opts, "max_http_connections", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    int_msec = opts.idle_http_connection_timeout;
    pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
    if (-1 == PyDict_SetItemString(pyObj_opts, "idle_http_connection_timeout", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(opts.user_agent_extra.c_str());
    if (-1 == PyDict_SetItemString(pyObj_opts, "user_agent_extra", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    auto credentials = cluster_info.second.credentials();
    PyObject* pyObj_creds = PyDict_New();

    pyObj_tmp = PyUnicode_FromString(credentials.username.c_str());
    if (-1 == PyDict_SetItemString(pyObj_creds, "username", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(credentials.password.c_str());
    if (-1 == PyDict_SetItemString(pyObj_creds, "password", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(credentials.certificate_path.c_str());
    if (-1 == PyDict_SetItemString(pyObj_creds, "certificate_path", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(credentials.key_path.c_str());
    if (-1 == PyDict_SetItemString(pyObj_creds, "key_path", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    PyObject* pyObj_allowed_sasl_mechanisms = PyList_New(static_cast<Py_ssize_t>(0));
    if (credentials.allowed_sasl_mechanisms.has_value()) {
        for (auto const& mech : credentials.allowed_sasl_mechanisms.value()) {
            pyObj_tmp = PyUnicode_FromString(mech.c_str());
            if (-1 == PyList_Append(pyObj_allowed_sasl_mechanisms, pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_XDECREF(pyObj_tmp);
        }
    }

    if (-1 == PyDict_SetItemString(pyObj_creds, "allowed_sasl_mechanisms", pyObj_allowed_sasl_mechanisms)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_allowed_sasl_mechanisms);

    if (-1 == PyDict_SetItemString(pyObj_opts, "credentials", pyObj_creds)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_creds);

    if (-1 == PyDict_SetItemString(pyObj_opts, "dump_configuration", opts.dump_configuration ? Py_True : Py_False)) {
        PyErr_Print();
        PyErr_Clear();
    }

    return pyObj_opts;
}

PyObject*
handle_close_connection([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* pyObj_conn = nullptr;
    PyObject* pyObj_callback = nullptr;
    PyObject* pyObj_errback = nullptr;
    PyObject* pyObj_result = nullptr;

    static const char* kw_list[] = { "", "callback", "errback", nullptr };

    const char* kw_format = "O!|OO";
    int ret = PyArg_ParseTupleAndKeywords(
      args, kwargs, kw_format, const_cast<char**>(kw_list), &PyCapsule_Type, &pyObj_conn, &pyObj_callback, &pyObj_errback);

    if (!ret) {
        std::string msg = "Cannot close connection. Unable to parse args/kwargs.";
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, msg.c_str());
        return nullptr;
    }

    connection* conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    if (nullptr == conn) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, NULL_CONN_OBJECT);
        return nullptr;
    }

    // PyObjects that need to be around for the cxx client lambda
    // have their increment/decrement handled w/in the callback_context struct
    // struct callback_context callback_ctx = { pyObj_callback, pyObj_errback };
    Py_XINCREF(pyObj_callback);
    Py_XINCREF(pyObj_errback);
    Py_XINCREF(pyObj_conn);
    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();
    {
        int callback_count = 0;
        Py_BEGIN_ALLOW_THREADS conn->cluster_->close([pyObj_conn, pyObj_callback, pyObj_errback, callback_count, barrier]() mutable {
            if (callback_count == 0) {
                close_connection_callback(pyObj_conn, pyObj_callback, pyObj_errback, barrier);
            } else {
                CB_LOG_DEBUG("close callback called {} times already!", callback_count);
                callback_count++;
            }
        });
        Py_END_ALLOW_THREADS
    }
    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        PyObject* ret = nullptr;
        Py_BEGIN_ALLOW_THREADS ret = f.get();
        Py_END_ALLOW_THREADS return ret;
    }
    Py_RETURN_NONE;
}

PyObject*
handle_open_or_close_bucket([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    char* bucket_name = nullptr;
    PyObject* pyObj_conn = nullptr;
    PyObject* pyObj_callback = nullptr;
    PyObject* pyObj_errback = nullptr;
    PyObject* pyObj_result = nullptr;

    int open = 1;

    static const char* kw_list[] = { "", "", "callback", "errback", "open_bucket", nullptr };

    // TODO:  something about passing in a boolean corrupts the param before it
    //      "O!s|OOp" would cause the errback PyObject to be corrupted -- no idea why???
    //      don't seem to have this issue in the KV ops...
    const char* kw_format = "O!s|OOi";

    int ret = PyArg_ParseTupleAndKeywords(args,
                                          kwargs,
                                          kw_format,
                                          const_cast<char**>(kw_list),
                                          &PyCapsule_Type,
                                          &pyObj_conn,
                                          &bucket_name,
                                          &pyObj_callback,
                                          &pyObj_errback,
                                          &open);

    if (!ret) {
        std::string msg = "Cannot ";
        msg.append(open == 1 ? "open" : "close");
        msg.append(" bucket.  Unable to parse args/kwargs.");
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, msg.c_str());
        return nullptr;
    }

    connection* conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    if (nullptr == conn) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, NULL_CONN_OBJECT);
        return nullptr;
    }

    // PyObjects that need to be around for the cxx client lambda
    // have their increment/decrement handled w/in the callback_context struct
    // struct callback_context callback_ctx = { pyObj_callback, pyObj_errback };
    Py_XINCREF(pyObj_callback);
    Py_XINCREF(pyObj_errback);

    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();
    {
        int callback_count = 0;
        Py_BEGIN_ALLOW_THREADS if (open)
        {
            conn->cluster_->open_bucket(bucket_name,
                                        [pyObj_callback, pyObj_errback, callback_count, open, barrier](std::error_code ec) mutable {
                                            // @TODO: should the c++ client execute this lambda more than once?
                                            if (callback_count == 0) {
                                                bucket_op_callback(ec, open, pyObj_callback, pyObj_errback, barrier);
                                            }
                                            callback_count++;
                                        });
        }
        else
        {
            conn->cluster_->close_bucket(bucket_name,
                                         [pyObj_callback, pyObj_errback, callback_count, open, barrier](std::error_code ec) mutable {
                                             if (callback_count == 0) {
                                                 bucket_op_callback(ec, open, pyObj_callback, pyObj_errback, barrier);
                                             }
                                             callback_count++;
                                         });
        }
        Py_END_ALLOW_THREADS
    }
    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        PyObject* ret = nullptr;
        Py_BEGIN_ALLOW_THREADS ret = f.get();
        Py_END_ALLOW_THREADS return ret;
    }
    Py_RETURN_NONE;
}
