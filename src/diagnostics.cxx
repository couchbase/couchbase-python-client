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

#include "diagnostics.hxx"

template<typename T>
void
add_extras_to_service_endpoint([[maybe_unused]] const T& t, [[maybe_unused]] PyObject* dict)
{
}

template<>
void
add_extras_to_service_endpoint<couchbase::core::diag::endpoint_ping_info>(const couchbase::core::diag::endpoint_ping_info& e,
                                                                          PyObject* pyObj_dict)
{

    long duration = e.latency.count();
    PyObject* pyObj_tmp = PyLong_FromLong(duration);
    if (-1 == PyDict_SetItemString(pyObj_dict, "latency_us", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    std::string ping_state = std::string();
    switch (e.state) {
        case couchbase::core::diag::ping_state::ok: {
            ping_state = "ok";
            break;
        }
        case couchbase::core::diag::ping_state::timeout: {
            ping_state = "timeout";
            break;
        }
        case couchbase::core::diag::ping_state::error: {
            ping_state = "error";
            break;
        }
        default: {
            break;
        }
    };

    if (ping_state.length() > 0) {
        pyObj_tmp = PyUnicode_FromString(ping_state.c_str());
        if (-1 == PyDict_SetItemString(pyObj_dict, "state", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_XDECREF(pyObj_tmp);
    }

    if (e.error.has_value()) {
        pyObj_tmp = PyUnicode_FromString(e.error.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_dict, "error", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_XDECREF(pyObj_tmp);
    }
}

template<>
void
add_extras_to_service_endpoint<couchbase::core::diag::endpoint_diag_info>(const couchbase::core::diag::endpoint_diag_info& e,
                                                                          PyObject* pyObj_dict)
{
    PyObject* pyObj_tmp = nullptr;

    if (e.last_activity.has_value()) {
        long duration = e.last_activity.value().count();
        pyObj_tmp = PyLong_FromLong(duration);
        if (-1 == PyDict_SetItemString(pyObj_dict, "last_activity_us", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_XDECREF(pyObj_tmp);
    }

    std::string endpoint_state = std::string();
    switch (e.state) {
        case couchbase::core::diag::endpoint_state::disconnected: {
            endpoint_state = "disconnected";
            break;
        }
        case couchbase::core::diag::endpoint_state::connecting: {
            endpoint_state = "connecting";
            break;
        }
        case couchbase::core::diag::endpoint_state::connected: {
            endpoint_state = "connected";
            break;
        }
        case couchbase::core::diag::endpoint_state::disconnecting: {
            endpoint_state = "disconnecting";
            break;
        }
        default: {
            break;
        }
    };

    if (endpoint_state.length() > 0) {
        pyObj_tmp = PyUnicode_FromString(endpoint_state.c_str());
        if (-1 == PyDict_SetItemString(pyObj_dict, "state", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_XDECREF(pyObj_tmp);
    }
}

template<typename T>
PyObject*
get_service_endpoints(const T& resp)
{
    PyObject* pyObj_services_dict = PyDict_New();
    for (auto const& service : resp.services) {
        PyObject* pyObj_endpoints = PyList_New(static_cast<Py_ssize_t>(0));
        std::string service_type = service_type_to_str(service.first);
        for (auto e : service.second) {
            PyObject* pyObj_service_dict = PyDict_New();

            PyObject* pyObj_tmp = PyUnicode_FromString(e.id.c_str());
            if (-1 == PyDict_SetItemString(pyObj_service_dict, "id", pyObj_tmp)) {
                Py_XDECREF(pyObj_tmp);
                Py_XDECREF(pyObj_endpoints);
                Py_XDECREF(pyObj_services_dict);
                Py_XDECREF(pyObj_service_dict);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);

            pyObj_tmp = PyUnicode_FromString(e.local.c_str());
            if (-1 == PyDict_SetItemString(pyObj_service_dict, "local", pyObj_tmp)) {
                Py_XDECREF(pyObj_tmp);
                Py_XDECREF(pyObj_endpoints);
                Py_XDECREF(pyObj_services_dict);
                Py_DECREF(pyObj_service_dict);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);

            pyObj_tmp = PyUnicode_FromString(e.remote.c_str());
            if (-1 == PyDict_SetItemString(pyObj_service_dict, "remote", pyObj_tmp)) {
                Py_XDECREF(pyObj_tmp);
                Py_XDECREF(pyObj_endpoints);
                Py_XDECREF(pyObj_services_dict);
                Py_DECREF(pyObj_service_dict);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);

            if (e.bucket.has_value()) {
                pyObj_tmp = PyUnicode_FromString(e.bucket.value().c_str());
                if (-1 == PyDict_SetItemString(pyObj_service_dict, "namespace", pyObj_tmp)) {
                    Py_XDECREF(pyObj_tmp);
                    Py_XDECREF(pyObj_endpoints);
                    Py_XDECREF(pyObj_services_dict);
                    Py_DECREF(pyObj_service_dict);
                    return nullptr;
                }
                Py_DECREF(pyObj_tmp);
            }

            add_extras_to_service_endpoint(e, pyObj_service_dict);
            PyList_Append(pyObj_endpoints, pyObj_service_dict);
            Py_DECREF(pyObj_service_dict);
        }

        if (-1 == PyDict_SetItemString(pyObj_services_dict, service_type.c_str(), pyObj_endpoints)) {
            Py_XDECREF(pyObj_endpoints);
            Py_DECREF(pyObj_services_dict);
            return nullptr;
        }
        Py_DECREF(pyObj_endpoints);
    }

    return pyObj_services_dict;
}

template<typename T>
result*
create_diagnostics_op_result(const T& resp)
{
    PyObject* result_obj = create_result_obj();
    result* res = reinterpret_cast<result*>(result_obj);

    PyObject* pyObj_tmp = PyUnicode_FromString(resp.id.c_str());
    if (-1 == PyDict_SetItemString(res->dict, "id", pyObj_tmp)) {
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(resp.sdk.c_str());
    if (-1 == PyDict_SetItemString(res->dict, "sdk", pyObj_tmp)) {
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLong(resp.version);
    if (-1 == PyDict_SetItemString(res->dict, "version", pyObj_tmp)) {
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (resp.services.size() > 0) {
        PyObject* pyObj_services_dict = get_service_endpoints(resp);
        if (pyObj_services_dict == nullptr) {
            return nullptr;
        }

        if (-1 == PyDict_SetItemString(res->dict, "endpoints", pyObj_services_dict)) {
            Py_XDECREF(pyObj_services_dict);
            return nullptr;
        }
        Py_DECREF(pyObj_services_dict);
    }
    return res;
}

template<typename T>
void
create_diagnostics_op_response(const T& resp,
                               PyObject* pyObj_callback,
                               PyObject* pyObj_errback,
                               std::shared_ptr<std::promise<PyObject*>> barrier)
{
    PyObject* pyObj_args = nullptr;
    PyObject* pyObj_kwargs = nullptr;
    PyObject* pyObj_func = nullptr;
    PyObject* pyObj_exc = nullptr;
    PyObject* pyObj_callback_res = nullptr;
    auto set_exception = false;

    PyGILState_STATE state = PyGILState_Ensure();

    auto res = create_diagnostics_op_result(resp);
    if (res == nullptr || PyErr_Occurred() != nullptr) {
        set_exception = true;
    } else {
        if (pyObj_callback == nullptr) {
            barrier->set_value(reinterpret_cast<PyObject*>(res));
        } else {
            pyObj_func = pyObj_callback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, reinterpret_cast<PyObject*>(res));
        }
    }

    if (set_exception) {
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "Diagnostic operation error.");
        if (pyObj_errback == nullptr) {
            barrier->set_value(pyObj_exc);
        } else {
            pyObj_func = pyObj_errback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, pyObj_exc);
        }
    }

    if (!set_exception && pyObj_func != nullptr) {
        pyObj_callback_res = PyObject_Call(pyObj_func, pyObj_args, pyObj_kwargs);
        if (pyObj_callback_res) {
            Py_DECREF(pyObj_callback_res);
        } else {
            PyErr_Print();
            // @TODO: how to catch exception here?
        }
        Py_DECREF(pyObj_args);
        Py_XDECREF(pyObj_kwargs);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }

    PyGILState_Release(state);
}

PyObject*
handle_diagnostics_op([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    // need these for all operations
    PyObject* pyObj_conn = nullptr;
    Operations::OperationType op_type = Operations::UNKNOWN;
    char* bucket = nullptr;
    uint64_t timeout = 0;
    char* report_id = nullptr;
    PyObject* pyObj_service_types = nullptr;
    PyObject* pyObj_callback = nullptr;
    PyObject* pyObj_errback = nullptr;

    static const char* kw_list[] = { "conn", "op_type", "bucket", "timeout", "report_id", "service_types", "callback", "errback", nullptr };

    const char* kw_format = "O!I|sLsOOO";
    int ret = PyArg_ParseTupleAndKeywords(args,
                                          kwargs,
                                          kw_format,
                                          const_cast<char**>(kw_list),
                                          &PyCapsule_Type,
                                          &pyObj_conn,
                                          &op_type,
                                          &bucket,
                                          &timeout,
                                          &report_id,
                                          &pyObj_service_types,
                                          &pyObj_callback,
                                          &pyObj_errback);
    if (!ret) {
        pycbc_set_python_exception(
          PycbcError::InvalidArgument, __FILE__, __LINE__, "Cannot perform diagnostics operation.  Unable to parse args/kwargs.");
        return nullptr;
    }

    connection* conn = nullptr;
    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::key_value_timeout;

    conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    if (nullptr == conn) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, NULL_CONN_OBJECT);
        return nullptr;
    }

    if (0 < timeout) {
        timeout_ms = std::chrono::milliseconds(std::max(0ULL, timeout / 1000ULL));
    }

    auto reportId = report_id ? std::optional<std::string>{ report_id } : std::nullopt;
    auto bucketName = bucket ? std::optional<std::string>{ bucket } : std::nullopt;
    std::set<couchbase::core::service_type> services;
    if (pyObj_service_types && PyList_Check(pyObj_service_types)) {
        for (Py_ssize_t i = 0; i < PyList_Size(pyObj_service_types); i++) {
            PyObject* pyObj_svc = PyList_GetItem(pyObj_service_types, i);
            // PyList_GetItem returns borrowed ref, inc while using, decr after done
            Py_INCREF(pyObj_svc);
            if (PyUnicode_Check(pyObj_svc)) {
                auto res = std::string(PyUnicode_AsUTF8(pyObj_svc));
                auto svc_type = str_to_service_type(res);
                services.insert(svc_type);
            }
            Py_DECREF(pyObj_svc);
            pyObj_svc = nullptr;
        }
    }

    // PyObjects that need to be around for the cxx client lambda
    // have their increment/decrement handled w/in the callback_context struct
    // struct callback_context callback_ctx = { pyObj_callback, pyObj_errback };
    Py_XINCREF(pyObj_callback);
    Py_XINCREF(pyObj_errback);

    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();

    if (op_type == Operations::DIAGNOSTICS) {
        Py_BEGIN_ALLOW_THREADS conn->cluster_->diagnostics(
          reportId, [pyObj_callback, pyObj_errback, barrier](couchbase::core::diag::diagnostics_result r) {
              create_diagnostics_op_response(r, pyObj_callback, pyObj_errback, barrier);
          });
        Py_END_ALLOW_THREADS
    } else {
        couchbase::core::diag::ping_result resp;
        Py_BEGIN_ALLOW_THREADS conn->cluster_->ping(
          reportId, bucketName, services, [pyObj_callback, pyObj_errback, barrier](couchbase::core::diag::ping_result r) {
              create_diagnostics_op_response(r, pyObj_callback, pyObj_errback, barrier);
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
