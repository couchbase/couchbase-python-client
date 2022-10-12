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

#include "eventing_function_management.hxx"
#include <core/management/eventing_function.hxx>
#include <core/management/eventing_status.hxx>
#include <core/operations/management/eventing_problem.hxx>
#include <couchbase/query_scan_consistency.hxx>

PyObject*
build_eventing_function_status_functions(std::vector<couchbase::core::management::eventing::function_state> functions)
{
    PyObject* pyObj_functions = PyList_New(static_cast<Py_ssize_t>(0));
    PyObject* pyObj_tmp = nullptr;

    for (auto const& function : functions) {
        PyObject* pyObj_function = PyDict_New();

        pyObj_tmp = PyUnicode_FromString(function.name.c_str());
        if (-1 == PyDict_SetItemString(pyObj_function, "name", pyObj_tmp)) {
            Py_XDECREF(pyObj_function);
            Py_XDECREF(pyObj_functions);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        switch (function.status) {
            case couchbase::core::management::eventing::function_status::undeployed: {
                pyObj_tmp = PyUnicode_FromString("undeployed");
                break;
            }
            case couchbase::core::management::eventing::function_status::undeploying: {
                pyObj_tmp = PyUnicode_FromString("undeploying");
                break;
            }
            case couchbase::core::management::eventing::function_status::deploying: {
                pyObj_tmp = PyUnicode_FromString("deploying");
                break;
            }
            case couchbase::core::management::eventing::function_status::deployed: {
                pyObj_tmp = PyUnicode_FromString("deployed");
                break;
            }
            case couchbase::core::management::eventing::function_status::pausing: {
                pyObj_tmp = PyUnicode_FromString("pausing");
                break;
            }
            case couchbase::core::management::eventing::function_status::paused: {
                pyObj_tmp = PyUnicode_FromString("paused");
                break;
            }
            default: {
                pyObj_tmp = PyUnicode_FromString("undeployed");
                break;
            }
        }
        if (-1 == PyDict_SetItemString(pyObj_function, "status", pyObj_tmp)) {
            Py_DECREF(pyObj_function);
            Py_XDECREF(pyObj_functions);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyLong_FromUnsignedLongLong(function.num_bootstrapping_nodes);
        if (-1 == PyDict_SetItemString(pyObj_function, "num_bootstrapping_nodes", pyObj_tmp)) {
            Py_DECREF(pyObj_function);
            Py_XDECREF(pyObj_functions);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyLong_FromUnsignedLongLong(function.num_deployed_nodes);
        if (-1 == PyDict_SetItemString(pyObj_function, "num_deployed_nodes", pyObj_tmp)) {
            Py_DECREF(pyObj_function);
            Py_XDECREF(pyObj_functions);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        switch (function.deployment_status) {
            case couchbase::core::management::eventing::function_deployment_status::deployed: {
                pyObj_tmp = PyUnicode_FromString("deployed");
                break;
            }
            case couchbase::core::management::eventing::function_deployment_status::undeployed: {
                pyObj_tmp = PyUnicode_FromString("undeployed");
                break;
            }
            default: {
                pyObj_tmp = PyUnicode_FromString("undeployed");
                break;
            }
        }
        if (-1 == PyDict_SetItemString(pyObj_function, "deployment_status", pyObj_tmp)) {
            Py_DECREF(pyObj_function);
            Py_XDECREF(pyObj_functions);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        switch (function.processing_status) {
            case couchbase::core::management::eventing::function_processing_status::paused: {
                pyObj_tmp = PyUnicode_FromString("paused");
                break;
            }
            case couchbase::core::management::eventing::function_processing_status::running: {
                pyObj_tmp = PyUnicode_FromString("running");
                break;
            }
            default: {
                pyObj_tmp = PyUnicode_FromString("paused");
                break;
            }
        }
        if (-1 == PyDict_SetItemString(pyObj_function, "processing_status", pyObj_tmp)) {
            Py_DECREF(pyObj_function);
            Py_XDECREF(pyObj_functions);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        if (function.redeploy_required.has_value()) {
            if (function.redeploy_required.value()) {
                if (-1 == PyDict_SetItemString(pyObj_function, "redeploy_required", Py_True)) {
                    Py_DECREF(pyObj_function);
                    Py_XDECREF(pyObj_functions);
                    Py_XDECREF(pyObj_tmp);
                    return nullptr;
                }
            } else {
                if (-1 == PyDict_SetItemString(pyObj_function, "redeploy_required", Py_False)) {
                    Py_DECREF(pyObj_function);
                    Py_XDECREF(pyObj_functions);
                    Py_XDECREF(pyObj_tmp);
                    return nullptr;
                }
            }
        }

        if (-1 == PyList_Append(pyObj_functions, pyObj_function)) {
            Py_XDECREF(pyObj_function);
            Py_XDECREF(pyObj_functions);
            return nullptr;
        }
        Py_DECREF(pyObj_function);
    }

    return pyObj_functions;
}

PyObject*
build_eventing_function_status(const couchbase::core::management::eventing::status& status)
{
    PyObject* pyObj_status = PyDict_New();
    PyObject* pyObj_tmp = nullptr;

    pyObj_tmp = PyLong_FromLongLong(status.num_eventing_nodes);
    if (-1 == PyDict_SetItemString(pyObj_status, "num_eventing_nodes", pyObj_tmp)) {
        Py_XDECREF(pyObj_status);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = build_eventing_function_status_functions(status.functions);
    if (pyObj_tmp == nullptr) {
        Py_DECREF(pyObj_status);
        return nullptr;
    }
    if (-1 == PyDict_SetItemString(pyObj_status, "functions", pyObj_tmp)) {
        Py_DECREF(pyObj_status);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    return pyObj_status;
}

PyObject*
build_eventing_function_settings(const couchbase::core::management::eventing::function_settings& settings)
{
    PyObject* pyObj_settings = PyDict_New();
    PyObject* pyObj_tmp = nullptr;

    if (settings.cpp_worker_count.has_value()) {
        pyObj_tmp = PyLong_FromLongLong(settings.cpp_worker_count.value());
        if (-1 == PyDict_SetItemString(pyObj_settings, "cpp_worker_count", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.dcp_stream_boundary.has_value()) {
        switch (settings.dcp_stream_boundary.value()) {
            case couchbase::core::management::eventing::function_dcp_boundary::everything: {
                pyObj_tmp = PyUnicode_FromString("everything");
                break;
            }
            case couchbase::core::management::eventing::function_dcp_boundary::from_now: {
                pyObj_tmp = PyUnicode_FromString("from_now");
                break;
            }
            default: {
                pyObj_tmp = PyUnicode_FromString("everything");
                break;
            }
        }
        if (-1 == PyDict_SetItemString(pyObj_settings, "dcp_stream_boundary", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.description.has_value()) {
        pyObj_tmp = PyUnicode_FromString(settings.description.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_settings, "description", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.deployment_status.has_value()) {
        switch (settings.deployment_status.value()) {
            case couchbase::core::management::eventing::function_deployment_status::deployed: {
                pyObj_tmp = PyUnicode_FromString("deployed");
                break;
            }
            case couchbase::core::management::eventing::function_deployment_status::undeployed: {
                pyObj_tmp = PyUnicode_FromString("undeployed");
                break;
            }
            default: {
                pyObj_tmp = PyUnicode_FromString("undeployed");
                break;
            }
        }
        if (-1 == PyDict_SetItemString(pyObj_settings, "deployment_status", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.processing_status.has_value()) {
        switch (settings.processing_status.value()) {
            case couchbase::core::management::eventing::function_processing_status::running: {
                pyObj_tmp = PyUnicode_FromString("running");
                break;
            }
            case couchbase::core::management::eventing::function_processing_status::paused: {
                pyObj_tmp = PyUnicode_FromString("paused");
                break;
            }
            default: {
                pyObj_tmp = PyUnicode_FromString("running");
                break;
            }
        }
        if (-1 == PyDict_SetItemString(pyObj_settings, "processing_status", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.log_level.has_value()) {
        switch (settings.log_level.value()) {
            case couchbase::core::management::eventing::function_log_level::info: {
                pyObj_tmp = PyUnicode_FromString("info");
                break;
            }
            case couchbase::core::management::eventing::function_log_level::error: {
                pyObj_tmp = PyUnicode_FromString("error");
                break;
            }
            case couchbase::core::management::eventing::function_log_level::warning: {
                pyObj_tmp = PyUnicode_FromString("warning");
                break;
            }
            case couchbase::core::management::eventing::function_log_level::debug: {
                pyObj_tmp = PyUnicode_FromString("debug");
                break;
            }
            case couchbase::core::management::eventing::function_log_level::trace: {
                pyObj_tmp = PyUnicode_FromString("trace");
                break;
            }
            default: {
                pyObj_tmp = PyUnicode_FromString("info");
                break;
            }
        }
        if (-1 == PyDict_SetItemString(pyObj_settings, "log_level", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.language_compatibility.has_value()) {
        switch (settings.language_compatibility.value()) {
            case couchbase::core::management::eventing::function_language_compatibility::version_6_0_0: {
                pyObj_tmp = PyUnicode_FromString("version_6_0_0");
                break;
            }
            case couchbase::core::management::eventing::function_language_compatibility::version_6_5_0: {
                pyObj_tmp = PyUnicode_FromString("version_6_5_0");
                break;
            }
            case couchbase::core::management::eventing::function_language_compatibility::version_6_6_2: {
                pyObj_tmp = PyUnicode_FromString("version_6_6_2");
                break;
            }
            default: {
                pyObj_tmp = PyUnicode_FromString("version_6_6_2");
                break;
            }
        }
        if (-1 == PyDict_SetItemString(pyObj_settings, "language_compatibility", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.execution_timeout.has_value()) {
        std::chrono::duration<unsigned long long> int_sec = settings.execution_timeout.value();
        pyObj_tmp = PyLong_FromUnsignedLongLong(int_sec.count());
        if (-1 == PyDict_SetItemString(pyObj_settings, "execution_timeout", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.lcb_inst_capacity.has_value()) {
        pyObj_tmp = PyLong_FromLongLong(settings.lcb_inst_capacity.value());
        if (-1 == PyDict_SetItemString(pyObj_settings, "lcb_inst_capacity", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.lcb_retry_count.has_value()) {
        pyObj_tmp = PyLong_FromLongLong(settings.lcb_retry_count.value());
        if (-1 == PyDict_SetItemString(pyObj_settings, "lcb_retry_count", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.lcb_timeout.has_value()) {
        std::chrono::duration<unsigned long long> int_sec = settings.lcb_timeout.value();
        pyObj_tmp = PyLong_FromUnsignedLongLong(int_sec.count());
        if (-1 == PyDict_SetItemString(pyObj_settings, "lcb_timeout", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.query_consistency.has_value()) {
        switch (settings.query_consistency.value()) {
            case couchbase::query_scan_consistency::not_bounded: {
                pyObj_tmp = PyUnicode_FromString("not_bounded");
                break;
            }
            case couchbase::query_scan_consistency::request_plus: {
                pyObj_tmp = PyUnicode_FromString("request_plus");
                break;
            }
            default: {
                pyObj_tmp = PyUnicode_FromString("not_bounded");
                break;
            }
        }
        if (-1 == PyDict_SetItemString(pyObj_settings, "query_consistency", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.num_timer_partitions.has_value()) {
        pyObj_tmp = PyLong_FromLongLong(settings.num_timer_partitions.value());
        if (-1 == PyDict_SetItemString(pyObj_settings, "num_timer_partitions", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.sock_batch_size.has_value()) {
        pyObj_tmp = PyLong_FromLongLong(settings.sock_batch_size.value());
        if (-1 == PyDict_SetItemString(pyObj_settings, "sock_batch_size", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.tick_duration.has_value()) {
        std::chrono::duration<unsigned long long, std::milli> int_msec = settings.tick_duration.value();
        pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
        if (-1 == PyDict_SetItemString(pyObj_settings, "tick_duration", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.timer_context_size.has_value()) {
        pyObj_tmp = PyLong_FromLongLong(settings.timer_context_size.value());
        if (-1 == PyDict_SetItemString(pyObj_settings, "timer_context_size", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.user_prefix.has_value()) {
        pyObj_tmp = PyUnicode_FromString(settings.user_prefix.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_settings, "user_prefix", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.bucket_cache_size.has_value()) {
        pyObj_tmp = PyLong_FromLongLong(settings.bucket_cache_size.value());
        if (-1 == PyDict_SetItemString(pyObj_settings, "bucket_cache_size", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.bucket_cache_age.has_value()) {
        std::chrono::duration<unsigned long long, std::milli> int_msec = settings.bucket_cache_age.value();
        pyObj_tmp = PyLong_FromUnsignedLongLong(int_msec.count());
        if (-1 == PyDict_SetItemString(pyObj_settings, "bucket_cache_age", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.curl_max_allowed_resp_size.has_value()) {
        pyObj_tmp = PyLong_FromLongLong(settings.curl_max_allowed_resp_size.value());
        if (-1 == PyDict_SetItemString(pyObj_settings, "curl_max_allowed_resp_size", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.query_prepare_all.has_value()) {
        if (settings.query_prepare_all.value()) {
            if (-1 == PyDict_SetItemString(pyObj_settings, "query_prepare_all", Py_True)) {
                Py_DECREF(pyObj_settings);
                return nullptr;
            }
        } else {
            if (-1 == PyDict_SetItemString(pyObj_settings, "query_prepare_all", Py_False)) {
                Py_DECREF(pyObj_settings);
                return nullptr;
            }
        }
    }

    if (settings.worker_count.has_value()) {
        pyObj_tmp = PyLong_FromLongLong(settings.worker_count.value());
        if (-1 == PyDict_SetItemString(pyObj_settings, "worker_count", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.handler_headers.size() > 0) {
        PyObject* pyObj_handler_headers = PyList_New(static_cast<Py_ssize_t>(0));
        for (auto const& header : settings.handler_headers) {
            pyObj_tmp = PyUnicode_FromString(header.c_str());
            if (-1 == PyList_Append(pyObj_handler_headers, pyObj_tmp)) {
                Py_DECREF(pyObj_settings);
                Py_XDECREF(pyObj_handler_headers);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);
        }
        if (-1 == PyDict_SetItemString(pyObj_settings, "handler_headers", pyObj_handler_headers)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_handler_headers);
            return nullptr;
        }
        Py_DECREF(pyObj_handler_headers);
    }

    if (settings.handler_footers.size() > 0) {
        PyObject* pyObj_handler_footers = PyList_New(static_cast<Py_ssize_t>(0));
        for (auto const& footer : settings.handler_footers) {
            pyObj_tmp = PyUnicode_FromString(footer.c_str());
            if (-1 == PyList_Append(pyObj_handler_footers, pyObj_tmp)) {
                Py_DECREF(pyObj_settings);
                Py_XDECREF(pyObj_handler_footers);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);
        }
        if (-1 == PyDict_SetItemString(pyObj_settings, "handler_footers", pyObj_handler_footers)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_handler_footers);
            return nullptr;
        }
        Py_DECREF(pyObj_handler_footers);
    }

    if (settings.enable_app_log_rotation.has_value()) {
        if (settings.enable_app_log_rotation.value()) {
            if (-1 == PyDict_SetItemString(pyObj_settings, "enable_app_log_rotation", Py_True)) {
                Py_DECREF(pyObj_settings);
                return nullptr;
            }
        } else {
            if (-1 == PyDict_SetItemString(pyObj_settings, "enable_app_log_rotation", Py_False)) {
                Py_DECREF(pyObj_settings);
                return nullptr;
            }
        }
    }

    if (settings.app_log_dir.has_value()) {
        pyObj_tmp = PyUnicode_FromString(settings.app_log_dir.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_settings, "app_log_dir", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.app_log_max_size.has_value()) {
        pyObj_tmp = PyLong_FromLongLong(settings.app_log_max_size.value());
        if (-1 == PyDict_SetItemString(pyObj_settings, "app_log_max_size", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.app_log_max_files.has_value()) {
        pyObj_tmp = PyLong_FromLongLong(settings.app_log_max_files.value());
        if (-1 == PyDict_SetItemString(pyObj_settings, "app_log_max_files", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (settings.checkpoint_interval.has_value()) {
        std::chrono::duration<unsigned long long> int_sec = settings.checkpoint_interval.value();
        pyObj_tmp = PyLong_FromUnsignedLongLong(int_sec.count());
        if (-1 == PyDict_SetItemString(pyObj_settings, "checkpoint_interval", pyObj_tmp)) {
            Py_DECREF(pyObj_settings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    return pyObj_settings;
}

PyObject*
build_eventing_function_keyspace(const couchbase::core::management::eventing::function_keyspace& keyspace)
{
    PyObject* pyObj_keyspace = PyDict_New();

    PyObject* pyObj_tmp = PyUnicode_FromString(keyspace.bucket.c_str());
    if (-1 == PyDict_SetItemString(pyObj_keyspace, "bucket", pyObj_tmp)) {
        Py_XDECREF(pyObj_keyspace);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (keyspace.scope.has_value()) {
        pyObj_tmp = PyUnicode_FromString(keyspace.scope.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_keyspace, "scope", pyObj_tmp)) {
            Py_DECREF(pyObj_keyspace);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (keyspace.collection.has_value()) {
        pyObj_tmp = PyUnicode_FromString(keyspace.collection.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_keyspace, "collection", pyObj_tmp)) {
            Py_DECREF(pyObj_keyspace);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }
    return pyObj_keyspace;
}

PyObject*
build_function_bucket_bindings(std::vector<couchbase::core::management::eventing::function_bucket_binding> bucket_bindings)
{
    PyObject* pyObj_bucket_bindings = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& binding : bucket_bindings) {
        PyObject* pyObj_binding = PyDict_New();

        PyObject* pyObj_tmp = PyUnicode_FromString(binding.alias.c_str());
        if (-1 == PyDict_SetItemString(pyObj_binding, "alias", pyObj_tmp)) {
            Py_XDECREF(pyObj_binding);
            Py_XDECREF(pyObj_bucket_bindings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = build_eventing_function_keyspace(binding.name);
        if (pyObj_tmp == nullptr) {
            Py_DECREF(pyObj_binding);
            Py_XDECREF(pyObj_bucket_bindings);
            return nullptr;
        }
        if (-1 == PyDict_SetItemString(pyObj_binding, "name", pyObj_tmp)) {
            Py_XDECREF(pyObj_binding);
            Py_XDECREF(pyObj_bucket_bindings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        switch (binding.access) {
            case couchbase::core::management::eventing::function_bucket_access::read_only: {
                pyObj_tmp = PyUnicode_FromString("read_only");
                break;
            }
            case couchbase::core::management::eventing::function_bucket_access::read_write: {
                pyObj_tmp = PyUnicode_FromString("read_write");
                break;
            }
            default: {
                pyObj_tmp = PyUnicode_FromString("read_write");
                break;
            }
        }
        if (-1 == PyDict_SetItemString(pyObj_binding, "access", pyObj_tmp)) {
            Py_XDECREF(pyObj_binding);
            Py_XDECREF(pyObj_bucket_bindings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        PyList_Append(pyObj_bucket_bindings, pyObj_binding);
        Py_DECREF(pyObj_binding);
    }

    return pyObj_bucket_bindings;
}

PyObject*
build_function_url_bindings(std::vector<couchbase::core::management::eventing::function_url_binding> url_bindings)
{
    PyObject* pyObj_url_bindings = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& binding : url_bindings) {
        PyObject* pyObj_binding = PyDict_New();

        PyObject* pyObj_tmp = PyUnicode_FromString(binding.alias.c_str());
        if (-1 == PyDict_SetItemString(pyObj_binding, "alias", pyObj_tmp)) {
            Py_XDECREF(pyObj_binding);
            Py_XDECREF(pyObj_url_bindings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(binding.hostname.c_str());
        if (-1 == PyDict_SetItemString(pyObj_binding, "hostname", pyObj_tmp)) {
            Py_DECREF(pyObj_binding);
            Py_XDECREF(pyObj_url_bindings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        if (binding.allow_cookies) {
            if (-1 == PyDict_SetItemString(pyObj_binding, "allow_cookies", Py_True)) {
                Py_DECREF(pyObj_binding);
                Py_XDECREF(pyObj_url_bindings);
                return nullptr;
            }
        } else {
            if (-1 == PyDict_SetItemString(pyObj_binding, "allow_cookies", Py_False)) {
                Py_DECREF(pyObj_binding);
                Py_XDECREF(pyObj_url_bindings);
                return nullptr;
            }
        }

        if (binding.validate_ssl_certificate) {
            if (-1 == PyDict_SetItemString(pyObj_binding, "validate_ssl_certificate", Py_True)) {
                Py_DECREF(pyObj_binding);
                Py_XDECREF(pyObj_url_bindings);
                return nullptr;
            }
        } else {
            if (-1 == PyDict_SetItemString(pyObj_binding, "validate_ssl_certificate", Py_False)) {
                Py_DECREF(pyObj_binding);
                Py_XDECREF(pyObj_url_bindings);
                return nullptr;
            }
        }

        if (std::holds_alternative<couchbase::core::management::eventing::function_url_no_auth>(binding.auth)) {
            pyObj_tmp = PyUnicode_FromString("no-auth");
            if (-1 == PyDict_SetItemString(pyObj_binding, "auth_type", pyObj_tmp)) {
                Py_DECREF(pyObj_binding);
                Py_XDECREF(pyObj_url_bindings);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);
        } else if (std::holds_alternative<couchbase::core::management::eventing::function_url_auth_basic>(binding.auth)) {
            pyObj_tmp = PyUnicode_FromString("basic");
            if (-1 == PyDict_SetItemString(pyObj_binding, "auth_type", pyObj_tmp)) {
                Py_DECREF(pyObj_binding);
                Py_XDECREF(pyObj_url_bindings);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);

            pyObj_tmp =
              PyUnicode_FromString(std::get<couchbase::core::management::eventing::function_url_auth_basic>(binding.auth).username.c_str());
            if (-1 == PyDict_SetItemString(pyObj_binding, "username", pyObj_tmp)) {
                Py_DECREF(pyObj_binding);
                Py_XDECREF(pyObj_url_bindings);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);
        } else if (std::holds_alternative<couchbase::core::management::eventing::function_url_auth_digest>(binding.auth)) {
            pyObj_tmp = PyUnicode_FromString("digest");
            if (-1 == PyDict_SetItemString(pyObj_binding, "auth_type", pyObj_tmp)) {
                Py_DECREF(pyObj_binding);
                Py_XDECREF(pyObj_url_bindings);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);

            pyObj_tmp = PyUnicode_FromString(
              std::get<couchbase::core::management::eventing::function_url_auth_digest>(binding.auth).username.c_str());
            if (-1 == PyDict_SetItemString(pyObj_binding, "username", pyObj_tmp)) {
                Py_DECREF(pyObj_binding);
                Py_XDECREF(pyObj_url_bindings);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);
        } else if (std::holds_alternative<couchbase::core::management::eventing::function_url_auth_bearer>(binding.auth)) {
            pyObj_tmp = PyUnicode_FromString("bearer");
            if (-1 == PyDict_SetItemString(pyObj_binding, "auth_type", pyObj_tmp)) {
                Py_DECREF(pyObj_binding);
                Py_XDECREF(pyObj_url_bindings);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);
        }

        PyList_Append(pyObj_url_bindings, pyObj_binding);
        Py_DECREF(pyObj_binding);
    }

    return pyObj_url_bindings;
}

PyObject*
build_function_constant_bindings(std::vector<couchbase::core::management::eventing::function_constant_binding> constant_bindings)
{
    PyObject* pyObj_constant_bindings = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& binding : constant_bindings) {
        PyObject* pyObj_binding = PyDict_New();

        PyObject* pyObj_tmp = PyUnicode_FromString(binding.alias.c_str());
        if (-1 == PyDict_SetItemString(pyObj_binding, "alias", pyObj_tmp)) {
            Py_XDECREF(pyObj_binding);
            Py_XDECREF(pyObj_constant_bindings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(binding.literal.c_str());
        if (-1 == PyDict_SetItemString(pyObj_binding, "literal", pyObj_tmp)) {
            Py_DECREF(pyObj_binding);
            Py_XDECREF(pyObj_constant_bindings);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        PyList_Append(pyObj_constant_bindings, pyObj_binding);
        Py_DECREF(pyObj_binding);
    }
    return pyObj_constant_bindings;
}

PyObject*
build_eventing_function(const couchbase::core::management::eventing::function& function)
{
    PyObject* pyObj_eventing_function = PyDict_New();
    PyObject* pyObj_tmp = PyUnicode_FromString(function.name.c_str());
    if (-1 == PyDict_SetItemString(pyObj_eventing_function, "name", pyObj_tmp)) {
        Py_XDECREF(pyObj_eventing_function);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(function.code.c_str());
    if (-1 == PyDict_SetItemString(pyObj_eventing_function, "code", pyObj_tmp)) {
        Py_DECREF(pyObj_eventing_function);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = build_eventing_function_keyspace(function.metadata_keyspace);
    if (pyObj_tmp == nullptr) {
        Py_DECREF(pyObj_eventing_function);
        return nullptr;
    }
    if (-1 == PyDict_SetItemString(pyObj_eventing_function, "metadata_keyspace", pyObj_tmp)) {
        Py_DECREF(pyObj_eventing_function);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = build_eventing_function_keyspace(function.source_keyspace);
    if (pyObj_tmp == nullptr) {
        Py_DECREF(pyObj_eventing_function);
        return nullptr;
    }
    if (-1 == PyDict_SetItemString(pyObj_eventing_function, "source_keyspace", pyObj_tmp)) {
        Py_DECREF(pyObj_eventing_function);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (function.version.has_value()) {
        pyObj_tmp = PyUnicode_FromString(function.version.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_eventing_function, "version", pyObj_tmp)) {
            Py_DECREF(pyObj_eventing_function);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (function.enforce_schema.has_value()) {
        if (function.enforce_schema.value()) {
            if (-1 == PyDict_SetItemString(pyObj_eventing_function, "enforce_schema", Py_True)) {
                Py_DECREF(pyObj_eventing_function);
                return nullptr;
            }
        } else {
            if (-1 == PyDict_SetItemString(pyObj_eventing_function, "enforce_schema", Py_False)) {
                Py_DECREF(pyObj_eventing_function);
                return nullptr;
            }
        }
    }

    if (function.handler_uuid.has_value()) {
        pyObj_tmp = PyLong_FromLongLong(function.handler_uuid.value());
        if (-1 == PyDict_SetItemString(pyObj_eventing_function, "handler_uuid", pyObj_tmp)) {
            Py_DECREF(pyObj_eventing_function);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (function.function_instance_id.has_value()) {
        pyObj_tmp = PyUnicode_FromString(function.function_instance_id.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_eventing_function, "function_instance_id", pyObj_tmp)) {
            Py_DECREF(pyObj_eventing_function);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    pyObj_tmp = build_function_bucket_bindings(function.bucket_bindings);
    if (pyObj_tmp == nullptr) {
        Py_DECREF(pyObj_eventing_function);
        return nullptr;
    }
    if (-1 == PyDict_SetItemString(pyObj_eventing_function, "bucket_bindings", pyObj_tmp)) {
        Py_DECREF(pyObj_eventing_function);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = build_function_url_bindings(function.url_bindings);
    if (pyObj_tmp == nullptr) {
        Py_DECREF(pyObj_eventing_function);
        return nullptr;
    }
    if (-1 == PyDict_SetItemString(pyObj_eventing_function, "url_bindings", pyObj_tmp)) {
        Py_DECREF(pyObj_eventing_function);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = build_function_constant_bindings(function.constant_bindings);
    if (pyObj_tmp == nullptr) {
        Py_DECREF(pyObj_eventing_function);
        return nullptr;
    }
    if (-1 == PyDict_SetItemString(pyObj_eventing_function, "constant_bindings", pyObj_tmp)) {
        Py_DECREF(pyObj_eventing_function);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = build_eventing_function_settings(function.settings);
    if (pyObj_tmp == nullptr) {
        Py_DECREF(pyObj_eventing_function);
        return nullptr;
    }
    if (-1 == PyDict_SetItemString(pyObj_eventing_function, "settings", pyObj_tmp)) {
        Py_DECREF(pyObj_eventing_function);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    return pyObj_eventing_function;
}

template<typename Response>
result*
create_result_from_eventing_function_mgmt_response([[maybe_unused]] const Response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);
    return res;
}

template<>
result*
create_result_from_eventing_function_mgmt_response<couchbase::core::operations::management::eventing_get_function_response>(
  const couchbase::core::operations::management::eventing_get_function_response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);

    PyObject* pyObj_eventing_function = build_eventing_function(resp.function);
    if (-1 == PyDict_SetItemString(res->dict, "function", pyObj_eventing_function)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_eventing_function);
        return nullptr;
    }
    Py_DECREF(pyObj_eventing_function);

    return res;
}

template<>
result*
create_result_from_eventing_function_mgmt_response<couchbase::core::operations::management::eventing_get_all_functions_response>(
  const couchbase::core::operations::management::eventing_get_all_functions_response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);

    PyObject* pyObj_functions = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& function : resp.functions) {
        PyObject* pyObj_eventing_function = build_eventing_function(function);
        if (pyObj_eventing_function == nullptr) {
            Py_XDECREF(pyObj_functions);
            Py_XDECREF(pyObj_result);
            return nullptr;
        }
        if (-1 == PyList_Append(pyObj_functions, pyObj_eventing_function)) {
            Py_XDECREF(pyObj_functions);
            Py_XDECREF(pyObj_eventing_function);
            Py_XDECREF(pyObj_result);
            return nullptr;
        }
        Py_DECREF(pyObj_eventing_function);
    }
    if (-1 == PyDict_SetItemString(res->dict, "function", pyObj_functions)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_functions);
        return nullptr;
    }
    Py_DECREF(pyObj_functions);

    return res;
}

template<>
result*
create_result_from_eventing_function_mgmt_response<couchbase::core::operations::management::eventing_get_status_response>(
  const couchbase::core::operations::management::eventing_get_status_response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);

    PyObject* pyObj_eventing_function_status = build_eventing_function_status(resp.status);
    if (-1 == PyDict_SetItemString(res->dict, "status", pyObj_eventing_function_status)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_eventing_function_status);
        return nullptr;
    }
    Py_DECREF(pyObj_eventing_function_status);

    return res;
}

PyObject*
build_eventing_function_mgmt_problem(const couchbase::core::operations::management::eventing_problem& problem)
{
    PyObject* pyObj_problem = PyDict_New();
    PyObject* pyObj_tmp = PyUnicode_FromString(problem.name.c_str());
    if (-1 == PyDict_SetItemString(pyObj_problem, "name", pyObj_tmp)) {
        Py_XDECREF(pyObj_problem);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(problem.description.c_str());
    if (-1 == PyDict_SetItemString(pyObj_problem, "description", pyObj_tmp)) {
        Py_XDECREF(pyObj_problem);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(problem.code);
    if (-1 == PyDict_SetItemString(pyObj_problem, "code", pyObj_tmp)) {
        Py_XDECREF(pyObj_problem);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    return pyObj_problem;
}

template<typename Response>
void
create_result_from_eventing_function_mgmt_op_response(Response& resp,
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
    if (resp.ctx.ec.value()) {
        PyObject* pyObj_problem = nullptr;
        if (resp.error.has_value()) {
            pyObj_problem = build_eventing_function_mgmt_problem(resp.error.value());
        }
        pyObj_exc = build_exception_from_context(
          resp.ctx, __FILE__, __LINE__, "Error doing eventing function mgmt operation.", "EventingFunctionMgmt");
        if (pyObj_problem != nullptr) {
            pycbc_add_exception_info(pyObj_exc, "eventing_problem", pyObj_problem);
        }
        if (pyObj_errback == nullptr) {
            barrier->set_value(pyObj_exc);
        } else {
            pyObj_func = pyObj_errback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, pyObj_exc);
        }
        // lets clear any errors
        PyErr_Clear();
    } else {
        auto res = create_result_from_eventing_function_mgmt_response(resp);

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
    }

    if (set_exception) {
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "Eventing function mgmt operation error.");
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
            // @TODO:  how to handle this situation?
        }
        Py_DECREF(pyObj_args);
        Py_XDECREF(pyObj_kwargs);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }

    PyGILState_Release(state);
}

couchbase::core::management::eventing::function_settings
get_event_function_settings(PyObject* pyObj_settings)
{
    couchbase::core::management::eventing::function_settings settings{};

    PyObject* pyObj_cpp_worker_count = PyDict_GetItemString(pyObj_settings, "cpp_worker_count");
    if (pyObj_cpp_worker_count != nullptr) {
        settings.cpp_worker_count = static_cast<std::int64_t>(PyLong_AsLongLong(pyObj_cpp_worker_count));
    }

    PyObject* pyObj_dcp_stream_boundary = PyDict_GetItemString(pyObj_settings, "dcp_stream_boundary");
    if (pyObj_dcp_stream_boundary != nullptr) {
        auto dcp_boundary = std::string(PyUnicode_AsUTF8(pyObj_dcp_stream_boundary));
        if (dcp_boundary.compare("everything") == 0) {
            settings.dcp_stream_boundary = couchbase::core::management::eventing::function_dcp_boundary::everything;
        } else if (dcp_boundary.compare("from_now") == 0) {
            settings.dcp_stream_boundary = couchbase::core::management::eventing::function_dcp_boundary::from_now;
        }
    }

    PyObject* pyObj_description = PyDict_GetItemString(pyObj_settings, "description");
    if (pyObj_description != nullptr) {
        auto description = std::string(PyUnicode_AsUTF8(pyObj_description));
        settings.description = description;
    }

    PyObject* pyObj_deployment_status = PyDict_GetItemString(pyObj_settings, "deployment_status");
    if (pyObj_deployment_status != nullptr) {
        auto deployment_status = std::string(PyUnicode_AsUTF8(pyObj_deployment_status));
        if (deployment_status.compare("deployed") == 0) {
            settings.deployment_status = couchbase::core::management::eventing::function_deployment_status::deployed;
        } else if (deployment_status.compare("undeployed") == 0) {
            settings.deployment_status = couchbase::core::management::eventing::function_deployment_status::undeployed;
        }
    }

    PyObject* pyObj_processing_status = PyDict_GetItemString(pyObj_settings, "processing_status");
    if (pyObj_processing_status != nullptr) {
        auto processing_status = std::string(PyUnicode_AsUTF8(pyObj_processing_status));
        if (processing_status.compare("running") == 0) {
            settings.processing_status = couchbase::core::management::eventing::function_processing_status::running;
        } else if (processing_status.compare("paused") == 0) {
            settings.processing_status = couchbase::core::management::eventing::function_processing_status::paused;
        }
    }

    PyObject* pyObj_log_level = PyDict_GetItemString(pyObj_settings, "log_level");
    if (pyObj_log_level != nullptr) {
        auto log_level = std::string(PyUnicode_AsUTF8(pyObj_log_level));
        if (log_level.compare("info") == 0) {
            settings.log_level = couchbase::core::management::eventing::function_log_level::info;
        } else if (log_level.compare("error") == 0) {
            settings.log_level = couchbase::core::management::eventing::function_log_level::error;
        } else if (log_level.compare("warning") == 0) {
            settings.log_level = couchbase::core::management::eventing::function_log_level::warning;
        } else if (log_level.compare("debug") == 0) {
            settings.log_level = couchbase::core::management::eventing::function_log_level::debug;
        } else if (log_level.compare("trace") == 0) {
            settings.log_level = couchbase::core::management::eventing::function_log_level::trace;
        }
    }

    PyObject* pyObj_language_compatibility = PyDict_GetItemString(pyObj_settings, "language_compatibility");
    if (pyObj_language_compatibility != nullptr) {
        auto language_compatibility = std::string(PyUnicode_AsUTF8(pyObj_language_compatibility));
        if (language_compatibility.compare("version_6_0_0") == 0) {
            settings.language_compatibility = couchbase::core::management::eventing::function_language_compatibility::version_6_0_0;
        } else if (language_compatibility.compare("version_6_5_0") == 0) {
            settings.language_compatibility = couchbase::core::management::eventing::function_language_compatibility::version_6_5_0;
        } else if (language_compatibility.compare("version_6_6_2") == 0) {
            settings.language_compatibility = couchbase::core::management::eventing::function_language_compatibility::version_6_6_2;
        }
    }

    PyObject* pyObj_execution_timeout = PyDict_GetItemString(pyObj_settings, "execution_timeout");
    if (pyObj_execution_timeout != nullptr) {
        auto execution_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_execution_timeout));
        settings.execution_timeout = std::chrono::seconds(execution_timeout);
    }

    PyObject* pyObj_lcb_inst_capacity = PyDict_GetItemString(pyObj_settings, "lcb_inst_capacity");
    if (pyObj_lcb_inst_capacity != nullptr) {
        settings.lcb_inst_capacity = static_cast<std::int64_t>(PyLong_AsLongLong(pyObj_lcb_inst_capacity));
    }

    PyObject* pyObj_lcb_retry_count = PyDict_GetItemString(pyObj_settings, "lcb_retry_count");
    if (pyObj_lcb_retry_count != nullptr) {
        settings.lcb_retry_count = static_cast<std::int64_t>(PyLong_AsLongLong(pyObj_lcb_retry_count));
    }

    PyObject* pyObj_lcb_timeout = PyDict_GetItemString(pyObj_settings, "lcb_timeout");
    if (pyObj_execution_timeout != nullptr) {
        auto lcb_timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_lcb_timeout));
        settings.lcb_timeout = std::chrono::seconds(lcb_timeout);
    }

    PyObject* pyObj_query_consistency = PyDict_GetItemString(pyObj_settings, "query_consistency");
    if (pyObj_query_consistency != nullptr) {
        auto query_consistency = std::string(PyUnicode_AsUTF8(pyObj_query_consistency));
        if (query_consistency.compare("not_bounded") == 0) {
            settings.query_consistency = couchbase::query_scan_consistency::not_bounded;
        } else if (query_consistency.compare("request_plus") == 0) {
            settings.query_consistency = couchbase::query_scan_consistency::request_plus;
        }
    }

    PyObject* pyObj_num_timer_partitions = PyDict_GetItemString(pyObj_settings, "num_timer_partitions");
    if (pyObj_num_timer_partitions != nullptr) {
        settings.num_timer_partitions = static_cast<std::int64_t>(PyLong_AsLongLong(pyObj_num_timer_partitions));
    }

    PyObject* pyObj_sock_batch_size = PyDict_GetItemString(pyObj_settings, "sock_batch_size");
    if (pyObj_sock_batch_size != nullptr) {
        settings.sock_batch_size = static_cast<std::int64_t>(PyLong_AsLongLong(pyObj_sock_batch_size));
    }

    PyObject* pyObj_tick_duration = PyDict_GetItemString(pyObj_settings, "tick_duration");
    if (pyObj_tick_duration != nullptr) {
        auto tick_duration = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_tick_duration));
        settings.tick_duration = std::chrono::milliseconds(std::max(0ULL, tick_duration / 1000ULL));
    }

    PyObject* pyObj_timer_context_size = PyDict_GetItemString(pyObj_settings, "timer_context_size");
    if (pyObj_timer_context_size != nullptr) {
        settings.timer_context_size = static_cast<std::int64_t>(PyLong_AsLongLong(pyObj_timer_context_size));
    }

    PyObject* pyObj_user_prefix = PyDict_GetItemString(pyObj_settings, "user_prefix");
    if (pyObj_user_prefix != nullptr) {
        settings.user_prefix = std::string(PyUnicode_AsUTF8(pyObj_user_prefix));
    }

    PyObject* pyObj_bucket_cache_size = PyDict_GetItemString(pyObj_settings, "bucket_cache_size");
    if (pyObj_bucket_cache_size != nullptr) {
        settings.bucket_cache_size = static_cast<std::int64_t>(PyLong_AsLongLong(pyObj_bucket_cache_size));
    }

    PyObject* pyObj_bucket_cache_age = PyDict_GetItemString(pyObj_settings, "bucket_cache_age");
    if (pyObj_bucket_cache_age != nullptr) {
        auto bucket_cache_age = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_bucket_cache_age));
        settings.bucket_cache_age = std::chrono::milliseconds(std::max(0ULL, bucket_cache_age / 1000ULL));
    }

    PyObject* pyObj_curl_max_allowed_resp_size = PyDict_GetItemString(pyObj_settings, "curl_max_allowed_resp_size");
    if (pyObj_curl_max_allowed_resp_size != nullptr) {
        settings.curl_max_allowed_resp_size = static_cast<std::int64_t>(PyLong_AsLongLong(pyObj_curl_max_allowed_resp_size));
    }

    PyObject* pyObj_query_prepare_all = PyDict_GetItemString(pyObj_settings, "query_prepare_all");
    if (pyObj_query_prepare_all != nullptr) {
        if (pyObj_query_prepare_all == Py_True) {
            settings.query_prepare_all = true;
        } else {
            settings.query_prepare_all = false;
        }
    }

    PyObject* pyObj_worker_count = PyDict_GetItemString(pyObj_settings, "worker_count");
    if (pyObj_worker_count != nullptr) {
        settings.worker_count = static_cast<std::int64_t>(PyLong_AsLongLong(pyObj_worker_count));
    }

    PyObject* pyObj_handler_headers = PyDict_GetItemString(pyObj_settings, "handler_headers");
    if (pyObj_handler_headers != nullptr && PyList_Check(pyObj_handler_headers)) {
        size_t nheaders = static_cast<size_t>(PyList_Size(pyObj_handler_headers));
        size_t ii;
        std::vector<std::string> headers{};
        for (ii = 0; ii < nheaders; ++ii) {
            PyObject* pyObj_header = PyList_GetItem(pyObj_handler_headers, ii);
            headers.emplace_back(std::string(PyUnicode_AsUTF8(pyObj_header)));
        }
        if (headers.size() > 0) {
            settings.handler_headers = headers;
        }
    }

    PyObject* pyObj_handler_footers = PyDict_GetItemString(pyObj_settings, "handler_footers");
    if (pyObj_handler_footers != nullptr) {
        size_t nfooters = static_cast<size_t>(PyList_Size(pyObj_handler_footers));
        size_t ii;
        std::vector<std::string> footers{};
        for (ii = 0; ii < nfooters; ++ii) {
            PyObject* pyObj_footer = PyList_GetItem(pyObj_handler_footers, ii);
            footers.emplace_back(std::string(PyUnicode_AsUTF8(pyObj_footer)));
        }
        if (footers.size() > 0) {
            settings.handler_footers = footers;
        }
    }

    PyObject* pyObj_enable_app_log_rotation = PyDict_GetItemString(pyObj_settings, "enable_app_log_rotation");
    if (pyObj_enable_app_log_rotation != nullptr) {
        if (pyObj_enable_app_log_rotation == Py_True) {
            settings.enable_app_log_rotation = true;
        } else {
            settings.enable_app_log_rotation = false;
        }
    }

    PyObject* pyObj_app_log_dir = PyDict_GetItemString(pyObj_settings, "app_log_dir");
    if (pyObj_app_log_dir != nullptr) {
        settings.app_log_dir = std::string(PyUnicode_AsUTF8(pyObj_app_log_dir));
    }

    PyObject* pyObj_app_log_max_size = PyDict_GetItemString(pyObj_settings, "app_log_max_size");
    if (pyObj_app_log_max_size != nullptr) {
        settings.app_log_max_size = static_cast<std::int64_t>(PyLong_AsLongLong(pyObj_app_log_max_size));
    }

    PyObject* pyObj_app_log_max_files = PyDict_GetItemString(pyObj_settings, "app_log_max_files");
    if (pyObj_app_log_max_files != nullptr) {
        settings.app_log_max_files = static_cast<std::int64_t>(PyLong_AsLongLong(pyObj_app_log_max_files));
    }

    PyObject* pyObj_checkpoint_interval = PyDict_GetItemString(pyObj_settings, "checkpoint_interval");
    if (pyObj_checkpoint_interval != nullptr) {
        auto checkpoint_interval = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_checkpoint_interval));
        settings.checkpoint_interval = std::chrono::seconds(checkpoint_interval);
    }

    return settings;
}

couchbase::core::management::eventing::function_keyspace
get_eventing_function_keyspace(PyObject* pyObj_keyspace)
{
    couchbase::core::management::eventing::function_keyspace keyspace{};

    PyObject* pyObj_bucket = PyDict_GetItemString(pyObj_keyspace, "bucket");
    if (pyObj_bucket == nullptr) {
        pycbc_set_python_exception(
          PycbcError::InvalidArgument, __FILE__, __LINE__, "Expected eventing function keyspace bucket to be provided.");
        throw std::invalid_argument("bucket name");
    }
    auto bucket_name = std::string(PyUnicode_AsUTF8(pyObj_bucket));
    keyspace.bucket = bucket_name;

    PyObject* pyObj_scope = PyDict_GetItemString(pyObj_keyspace, "scope");
    if (pyObj_scope != nullptr) {
        keyspace.scope = std::string(PyUnicode_AsUTF8(pyObj_scope));
    }

    PyObject* pyObj_collection = PyDict_GetItemString(pyObj_keyspace, "collection");
    if (pyObj_collection != nullptr) {
        keyspace.collection = std::string(PyUnicode_AsUTF8(pyObj_collection));
    }

    return keyspace;
}

std::vector<couchbase::core::management::eventing::function_constant_binding>
get_function_constant_bindings(PyObject* pyObj_function_constant_bindings)
{
    std::vector<couchbase::core::management::eventing::function_constant_binding> bindings{};
    if (pyObj_function_constant_bindings && PyList_Check(pyObj_function_constant_bindings)) {
        size_t nbindings = static_cast<size_t>(PyList_Size(pyObj_function_constant_bindings));
        size_t ii;
        for (ii = 0; ii < nbindings; ++ii) {
            PyObject* pyObj_binding = PyList_GetItem(pyObj_function_constant_bindings, ii);
            if (!pyObj_binding) {
                pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Could not determine constant binding.");
                throw std::invalid_argument("constant binding");
            }
            // PyList_GetItem returns borrowed ref, inc while using, decr after done
            Py_INCREF(pyObj_binding);
            couchbase::core::management::eventing::function_constant_binding constant_binding{};
            PyObject* pyObj_alias = PyDict_GetItemString(pyObj_binding, "alias");
            if (pyObj_alias != nullptr) {
                constant_binding.alias = std::string(PyUnicode_AsUTF8(pyObj_alias));
            }

            PyObject* pyObj_literal = PyDict_GetItemString(pyObj_binding, "literal");
            if (pyObj_literal != nullptr) {
                constant_binding.literal = std::string(PyUnicode_AsUTF8(pyObj_literal));
            }

            bindings.emplace_back(constant_binding);
            Py_DECREF(pyObj_binding);
            pyObj_binding = nullptr;
        }
    }
    return bindings;
}

std::vector<couchbase::core::management::eventing::function_url_binding>
get_function_url_bindings(PyObject* pyObj_function_url_bindings)
{
    std::vector<couchbase::core::management::eventing::function_url_binding> bindings{};
    if (pyObj_function_url_bindings && PyList_Check(pyObj_function_url_bindings)) {
        size_t nbindings = static_cast<size_t>(PyList_Size(pyObj_function_url_bindings));
        size_t ii;
        for (ii = 0; ii < nbindings; ++ii) {
            PyObject* pyObj_binding = PyList_GetItem(pyObj_function_url_bindings, ii);
            if (!pyObj_binding) {
                pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Could not determine url binding.");
                throw std::invalid_argument("url binding");
            }
            // PyList_GetItem returns borrowed ref, inc while using, decr after done
            Py_INCREF(pyObj_binding);
            couchbase::core::management::eventing::function_url_binding url_binding{};
            PyObject* pyObj_alias = PyDict_GetItemString(pyObj_binding, "alias");
            if (pyObj_alias != nullptr) {
                url_binding.alias = std::string(PyUnicode_AsUTF8(pyObj_alias));
            }

            PyObject* pyObj_hostname = PyDict_GetItemString(pyObj_binding, "hostname");
            if (pyObj_hostname != nullptr) {
                url_binding.hostname = std::string(PyUnicode_AsUTF8(pyObj_hostname));
            }

            PyObject* pyObj_allow_cookies = PyDict_GetItemString(pyObj_binding, "allow_cookies");
            if (pyObj_allow_cookies != nullptr) {
                if (pyObj_allow_cookies == Py_True) {
                    url_binding.allow_cookies = true;
                }
            }

            PyObject* pyObj_validate_ssl_certificate = PyDict_GetItemString(pyObj_binding, "validate_ssl_certificate");
            if (pyObj_validate_ssl_certificate != nullptr) {
                if (pyObj_validate_ssl_certificate == Py_False) {
                    url_binding.validate_ssl_certificate = false;
                }
            }

            PyObject* pyObj_auth_type = PyDict_GetItemString(pyObj_binding, "auth_type");
            auto auth_type = std::string(PyUnicode_AsUTF8(pyObj_auth_type));
            if (auth_type.compare("basic") == 0) {
                couchbase::core::management::eventing::function_url_auth_basic auth{};
                PyObject* pyObj_username = PyDict_GetItemString(pyObj_binding, "username");
                auth.username = std::string(PyUnicode_AsUTF8(pyObj_username));
                PyObject* pyObj_password = PyDict_GetItemString(pyObj_binding, "password");
                auth.password = std::string(PyUnicode_AsUTF8(pyObj_password));
                url_binding.auth = auth;
            } else if (auth_type.compare("digest") == 0) {
                couchbase::core::management::eventing::function_url_auth_digest auth{};
                PyObject* pyObj_username = PyDict_GetItemString(pyObj_binding, "username");
                auth.username = std::string(PyUnicode_AsUTF8(pyObj_username));
                PyObject* pyObj_password = PyDict_GetItemString(pyObj_binding, "password");
                auth.password = std::string(PyUnicode_AsUTF8(pyObj_password));
                url_binding.auth = auth;
            } else if (auth_type.compare("bearer") == 0) {
                couchbase::core::management::eventing::function_url_auth_bearer auth{};
                PyObject* pyObj_key = PyDict_GetItemString(pyObj_binding, "bearer_key");
                auth.key = std::string(PyUnicode_AsUTF8(pyObj_key));
                url_binding.auth = auth;
            }

            bindings.emplace_back(url_binding);
            Py_DECREF(pyObj_binding);
            pyObj_binding = nullptr;
        }
    }
    return bindings;
}

std::vector<couchbase::core::management::eventing::function_bucket_binding>
get_function_bucket_bindings(PyObject* pyObj_bucket_bindings)
{
    std::vector<couchbase::core::management::eventing::function_bucket_binding> bindings{};
    if (pyObj_bucket_bindings && PyList_Check(pyObj_bucket_bindings)) {
        size_t nbindings = static_cast<size_t>(PyList_Size(pyObj_bucket_bindings));
        size_t ii;
        for (ii = 0; ii < nbindings; ++ii) {
            PyObject* pyObj_binding = PyList_GetItem(pyObj_bucket_bindings, ii);
            if (!pyObj_binding) {
                pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Could not determine bucket binding.");
                throw std::invalid_argument("bucket binding");
            }
            // PyList_GetItem returns borrowed ref, inc while using, decr after done
            Py_INCREF(pyObj_binding);
            couchbase::core::management::eventing::function_bucket_binding bucket_binding{};
            PyObject* pyObj_alias = PyDict_GetItemString(pyObj_binding, "alias");
            if (pyObj_alias != nullptr) {
                bucket_binding.alias = std::string(PyUnicode_AsUTF8(pyObj_alias));
            }

            PyObject* pyObj_keyspace = PyDict_GetItemString(pyObj_binding, "name");
            bucket_binding.name = get_eventing_function_keyspace(pyObj_keyspace);

            PyObject* pyObj_access = PyDict_GetItemString(pyObj_binding, "access");
            if (pyObj_access != nullptr) {
                auto access = std::string(PyUnicode_AsUTF8(pyObj_access));
                if (access.compare("read_only") == 0) {
                    bucket_binding.access = couchbase::core::management::eventing::function_bucket_access::read_only;
                }
            }

            bindings.emplace_back(bucket_binding);
            Py_DECREF(pyObj_binding);
            pyObj_binding = nullptr;
        }
    }
    return bindings;
}

couchbase::core::management::eventing::function
get_eventing_function(PyObject* pyObj_eventing_function)
{
    couchbase::core::management::eventing::function eventing_function{};

    PyObject* pyObj_name = PyDict_GetItemString(pyObj_eventing_function, "name");
    if (pyObj_name == nullptr) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Expected eventing function name to be provided.");
        throw std::invalid_argument("name");
    }
    auto name = std::string(PyUnicode_AsUTF8(pyObj_name));
    eventing_function.name = name;

    PyObject* pyObj_code = PyDict_GetItemString(pyObj_eventing_function, "code");
    if (pyObj_code == nullptr) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Expected eventing function code to be provided.");
        throw std::invalid_argument("code");
    }
    auto code = std::string(PyUnicode_AsUTF8(pyObj_code));
    eventing_function.code = code;

    PyObject* pyObj_metadata_keyspace = PyDict_GetItemString(pyObj_eventing_function, "metadata_keyspace");
    eventing_function.metadata_keyspace = get_eventing_function_keyspace(pyObj_metadata_keyspace);

    PyObject* pyObj_source_keyspace = PyDict_GetItemString(pyObj_eventing_function, "source_keyspace");
    eventing_function.source_keyspace = get_eventing_function_keyspace(pyObj_source_keyspace);

    PyObject* pyObj_version = PyDict_GetItemString(pyObj_eventing_function, "version");
    if (pyObj_version != nullptr) {
        eventing_function.version = std::string(PyUnicode_AsUTF8(pyObj_version));
    }

    PyObject* pyObj_enforce_schema = PyDict_GetItemString(pyObj_eventing_function, "enforce_schema");
    if (pyObj_enforce_schema != nullptr) {
        if (pyObj_enforce_schema == Py_True) {
            eventing_function.enforce_schema = true;
        } else {
            eventing_function.enforce_schema = false;
        }
    }

    PyObject* pyObj_handler_uuid = PyDict_GetItemString(pyObj_eventing_function, "handler_uuid");
    if (pyObj_handler_uuid != nullptr) {
        eventing_function.handler_uuid = static_cast<std::int64_t>(PyLong_AsLongLong(pyObj_handler_uuid));
    }

    PyObject* pyObj_function_instance_id = PyDict_GetItemString(pyObj_eventing_function, "function_instance_id");
    if (pyObj_function_instance_id != nullptr) {
        eventing_function.function_instance_id = std::string(PyUnicode_AsUTF8(pyObj_function_instance_id));
    }

    PyObject* pyObj_bucket_bindings = PyDict_GetItemString(pyObj_eventing_function, "bucket_bindings");
    auto bucket_bindings = get_function_bucket_bindings(pyObj_bucket_bindings);
    if (bucket_bindings.size() > 0) {
        eventing_function.bucket_bindings = bucket_bindings;
    }

    PyObject* pyObj_url_bindings = PyDict_GetItemString(pyObj_eventing_function, "url_bindings");
    auto url_bindings = get_function_url_bindings(pyObj_url_bindings);
    if (url_bindings.size() > 0) {
        eventing_function.url_bindings = url_bindings;
    }

    PyObject* pyObj_constant_bindings = PyDict_GetItemString(pyObj_eventing_function, "constant_bindings");
    auto constant_bindings = get_function_constant_bindings(pyObj_constant_bindings);
    if (constant_bindings.size() > 0) {
        eventing_function.constant_bindings = constant_bindings;
    }

    PyObject* pyObj_settings = PyDict_GetItemString(pyObj_eventing_function, "settings");
    auto function_settings = get_event_function_settings(pyObj_settings);
    eventing_function.settings = function_settings;

    return eventing_function;
}

template<typename Request>
Request
get_eventing_function_mgmt_req(PyObject* op_args)
{
    Request req{};

    PyObject* pyObj_name = PyDict_GetItemString(op_args, "name");
    auto name = std::string(PyUnicode_AsUTF8(pyObj_name));
    req.name = name;

    PyObject* pyObj_client_context_id = PyDict_GetItemString(op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    return req;
}

template<typename Request>
PyObject*
do_eventing_function_mgmt_op(connection& conn,
                             Request& req,
                             PyObject* pyObj_callback,
                             PyObject* pyObj_errback,
                             std::shared_ptr<std::promise<PyObject*>> barrier)
{
    using response_type = typename Request::response_type;
    Py_BEGIN_ALLOW_THREADS conn.cluster_->execute(req, [pyObj_callback, pyObj_errback, barrier](response_type resp) {
        create_result_from_eventing_function_mgmt_op_response(resp, pyObj_callback, pyObj_errback, barrier);
    });
    Py_END_ALLOW_THREADS Py_RETURN_NONE;
}

PyObject*
handle_eventing_function_mgmt_op(connection* conn,
                                 struct eventing_function_mgmt_options* options,
                                 PyObject* pyObj_callback,
                                 PyObject* pyObj_errback)
{
    PyObject* res = nullptr;
    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();
    try {
        switch (options->op_type) {
            case EventingFunctionManagementOperations::UPSERT_FUNCTION: {
                couchbase::core::operations::management::eventing_upsert_function_request req{};
                PyObject* pyObj_client_context_id = PyDict_GetItemString(options->op_args, "client_context_id");
                if (pyObj_client_context_id != nullptr) {
                    auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
                    req.client_context_id = client_context_id;
                }
                PyObject* pyObj_eventing_function = PyDict_GetItemString(options->op_args, "eventing_function");
                req.function = get_eventing_function(pyObj_eventing_function);
                req.timeout = options->timeout_ms;

                res = do_eventing_function_mgmt_op<couchbase::core::operations::management::eventing_upsert_function_request>(
                  *conn, req, pyObj_callback, pyObj_errback, barrier);
                break;
            }
            case EventingFunctionManagementOperations::DEPLOY_FUNCTION: {
                auto req = get_eventing_function_mgmt_req<couchbase::core::operations::management::eventing_deploy_function_request>(
                  options->op_args);
                req.timeout = options->timeout_ms;

                res = do_eventing_function_mgmt_op<couchbase::core::operations::management::eventing_deploy_function_request>(
                  *conn, req, pyObj_callback, pyObj_errback, barrier);
                break;
            }
            case EventingFunctionManagementOperations::GET_FUNCTION: {
                auto req =
                  get_eventing_function_mgmt_req<couchbase::core::operations::management::eventing_get_function_request>(options->op_args);
                req.timeout = options->timeout_ms;

                res = do_eventing_function_mgmt_op<couchbase::core::operations::management::eventing_get_function_request>(
                  *conn, req, pyObj_callback, pyObj_errback, barrier);
                break;
            }
            case EventingFunctionManagementOperations::PAUSE_FUNCTION: {
                auto req = get_eventing_function_mgmt_req<couchbase::core::operations::management::eventing_pause_function_request>(
                  options->op_args);
                req.timeout = options->timeout_ms;

                res = do_eventing_function_mgmt_op<couchbase::core::operations::management::eventing_pause_function_request>(
                  *conn, req, pyObj_callback, pyObj_errback, barrier);
                break;
            }
            case EventingFunctionManagementOperations::RESUME_FUNCTION: {
                auto req = get_eventing_function_mgmt_req<couchbase::core::operations::management::eventing_resume_function_request>(
                  options->op_args);
                req.timeout = options->timeout_ms;

                res = do_eventing_function_mgmt_op<couchbase::core::operations::management::eventing_resume_function_request>(
                  *conn, req, pyObj_callback, pyObj_errback, barrier);
                break;
            }
            case EventingFunctionManagementOperations::UNDEPLOY_FUNCTION: {
                auto req = get_eventing_function_mgmt_req<couchbase::core::operations::management::eventing_undeploy_function_request>(
                  options->op_args);
                req.timeout = options->timeout_ms;

                res = do_eventing_function_mgmt_op<couchbase::core::operations::management::eventing_undeploy_function_request>(
                  *conn, req, pyObj_callback, pyObj_errback, barrier);
                break;
            }
            case EventingFunctionManagementOperations::DROP_FUNCTION: {
                auto req =
                  get_eventing_function_mgmt_req<couchbase::core::operations::management::eventing_drop_function_request>(options->op_args);
                req.timeout = options->timeout_ms;

                res = do_eventing_function_mgmt_op<couchbase::core::operations::management::eventing_drop_function_request>(
                  *conn, req, pyObj_callback, pyObj_errback, barrier);
                break;
            }
            case EventingFunctionManagementOperations::GET_ALL_FUNCTIONS: {
                couchbase::core::operations::management::eventing_get_all_functions_request req{};
                PyObject* pyObj_client_context_id = PyDict_GetItemString(options->op_args, "client_context_id");
                if (pyObj_client_context_id != nullptr) {
                    auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
                    req.client_context_id = client_context_id;
                }
                req.timeout = options->timeout_ms;

                res = do_eventing_function_mgmt_op<couchbase::core::operations::management::eventing_get_all_functions_request>(
                  *conn, req, pyObj_callback, pyObj_errback, barrier);
                break;
            }
            case EventingFunctionManagementOperations::GET_STATUS: {
                couchbase::core::operations::management::eventing_get_status_request req{};
                PyObject* pyObj_client_context_id = PyDict_GetItemString(options->op_args, "client_context_id");
                if (pyObj_client_context_id != nullptr) {
                    auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
                    req.client_context_id = client_context_id;
                }
                req.timeout = options->timeout_ms;

                res = do_eventing_function_mgmt_op<couchbase::core::operations::management::eventing_get_status_request>(
                  *conn, req, pyObj_callback, pyObj_errback, barrier);
                break;
            }
            default: {
                pycbc_set_python_exception(
                  PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized eventing function mgmt operation passed in.");
                barrier->set_value(nullptr);
                break;
            }
        };
    } catch (const std::invalid_argument&) {
    }

    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        PyObject* ret = nullptr;
        Py_BEGIN_ALLOW_THREADS ret = f.get();
        Py_END_ALLOW_THREADS return ret;
    }

    if (res == nullptr) {
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
        return nullptr;
    }

    return res;
}

void
add_eventing_function_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class)
{
    PyObject* pyObj_enum_values = PyUnicode_FromString(EventingFunctionManagementOperations::ALL_OPERATIONS());
    PyObject* pyObj_enum_name = PyUnicode_FromString("EventingFunctionManagementOperations");
    // PyTuple_Pack returns new reference, need to Py_DECREF values provided
    PyObject* pyObj_args = PyTuple_Pack(2, pyObj_enum_name, pyObj_enum_values);
    Py_DECREF(pyObj_enum_name);
    Py_DECREF(pyObj_enum_values);

    PyObject* pyObj_kwargs = PyDict_New();
    PyObject_SetItem(pyObj_kwargs, PyUnicode_FromString("module"), PyModule_GetNameObject(pyObj_module));
    PyObject* pyObj_mgmt_operations = PyObject_Call(pyObj_enum_class, pyObj_args, pyObj_kwargs);
    Py_DECREF(pyObj_args);
    Py_DECREF(pyObj_kwargs);

    if (PyModule_AddObject(pyObj_module, "eventing_function_mgmt_operations", pyObj_mgmt_operations) < 0) {
        // only need to Py_DECREF on failure to add when using PyModule_AddObject()
        Py_XDECREF(pyObj_mgmt_operations);
        return;
    }
}
