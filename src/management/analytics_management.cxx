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

#include "analytics_management.hxx"
#include "analytics_link.hxx"
#include "../exceptions.hxx"
#include <core/analytics_scan_consistency.hxx>
#include <core/management/analytics_link.hxx>
#include <core/management/analytics_index.hxx>
#include <core/management/analytics_dataset.hxx>

/* couchbase::core::operations::management::analytics_* request building methods */

template<typename T>
T
get_index_request_base(struct analytics_mgmt_options* options)
{
    T req{};

    PyObject* pyObj_dataverse_name = PyDict_GetItemString(options->op_args, "dataverse_name");
    if (pyObj_dataverse_name != nullptr) {
        auto dataverse_name = std::string(PyUnicode_AsUTF8(pyObj_dataverse_name));
        req.dataverse_name = dataverse_name;
    }

    PyObject* pyObj_dataset_name = PyDict_GetItemString(options->op_args, "dataset_name");
    auto dataset_name = std::string(PyUnicode_AsUTF8(pyObj_dataset_name));
    req.dataset_name = dataset_name;

    PyObject* pyObj_index_name = PyDict_GetItemString(options->op_args, "index_name");
    auto index_name = std::string(PyUnicode_AsUTF8(pyObj_index_name));
    req.index_name = index_name;

    PyObject* pyObj_client_context_id = PyDict_GetItemString(options->op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    req.timeout = options->timeout_ms;

    return req;
}

couchbase::core::operations::management::analytics_index_drop_request
get_index_drop_request(struct analytics_mgmt_options* options)
{
    auto req = get_index_request_base<couchbase::core::operations::management::analytics_index_drop_request>(options);

    PyObject* pyObj_ignore_if_does_not_exist = PyDict_GetItemString(options->op_args, "ignore_if_does_not_exist");
    if (pyObj_ignore_if_does_not_exist) {
        if (pyObj_ignore_if_does_not_exist == Py_True) {
            req.ignore_if_does_not_exist = true;
        }
    }

    return req;
}

couchbase::core::operations::management::analytics_index_create_request
get_index_create_request(struct analytics_mgmt_options* options)
{
    auto req = get_index_request_base<couchbase::core::operations::management::analytics_index_create_request>(options);

    PyObject* pyObj_fields = PyDict_GetItemString(options->op_args, "fields");

    if (pyObj_fields != nullptr && PyDict_Check(pyObj_fields)) {
        std::map<std::string, std::string> fields{};
        PyObject *pyObj_key, *pyObj_value;
        Py_ssize_t pos = 0;

        while (PyDict_Next(pyObj_fields, &pos, &pyObj_key, &pyObj_value)) {
            std::string k;
            if (PyUnicode_Check(pyObj_key)) {
                k = std::string(PyUnicode_AsUTF8(pyObj_key));
            }
            if (PyUnicode_Check(pyObj_value) && !k.empty()) {
                auto value = std::string(PyUnicode_AsUTF8(pyObj_value));
                fields.emplace(k, value);
            }
        }
        if (fields.size() > 0) {
            req.fields = fields;
        }
    }

    PyObject* pyObj_ignore_if_exists = PyDict_GetItemString(options->op_args, "ignore_if_exists");
    if (pyObj_ignore_if_exists) {
        if (pyObj_ignore_if_exists == Py_True) {
            req.ignore_if_exists = true;
        }
    }

    return req;
}

template<typename T>
T
get_dataset_request_base(struct analytics_mgmt_options* options)
{
    T req{};

    PyObject* pyObj_dataverse_name = PyDict_GetItemString(options->op_args, "dataverse_name");
    if (pyObj_dataverse_name != nullptr) {
        auto dataverse_name = std::string(PyUnicode_AsUTF8(pyObj_dataverse_name));
        req.dataverse_name = dataverse_name;
    }

    PyObject* pyObj_dataset_name = PyDict_GetItemString(options->op_args, "dataset_name");
    auto dataset_name = std::string(PyUnicode_AsUTF8(pyObj_dataset_name));
    req.dataset_name = dataset_name;

    PyObject* pyObj_client_context_id = PyDict_GetItemString(options->op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    req.timeout = options->timeout_ms;

    return req;
}

couchbase::core::operations::management::analytics_dataset_drop_request
get_dataset_drop_request(struct analytics_mgmt_options* options)
{
    auto req = get_dataset_request_base<couchbase::core::operations::management::analytics_dataset_drop_request>(options);

    PyObject* pyObj_ignore_if_does_not_exist = PyDict_GetItemString(options->op_args, "ignore_if_does_not_exist");
    if (pyObj_ignore_if_does_not_exist) {
        if (pyObj_ignore_if_does_not_exist == Py_True) {
            req.ignore_if_does_not_exist = true;
        }
    }

    return req;
}

couchbase::core::operations::management::analytics_dataset_create_request
get_dataset_create_request(struct analytics_mgmt_options* options)
{
    auto req = get_dataset_request_base<couchbase::core::operations::management::analytics_dataset_create_request>(options);

    PyObject* pyObj_bucket_name = PyDict_GetItemString(options->op_args, "bucket_name");
    auto bucket_name = std::string(PyUnicode_AsUTF8(pyObj_bucket_name));
    req.bucket_name = bucket_name;

    PyObject* pyObj_condition = PyDict_GetItemString(options->op_args, "condition");
    if (pyObj_condition != nullptr) {
        auto condition = std::string(PyUnicode_AsUTF8(pyObj_condition));
        req.condition = condition;
    }

    PyObject* pyObj_ignore_if_exists = PyDict_GetItemString(options->op_args, "ignore_if_exists");
    if (pyObj_ignore_if_exists) {
        if (pyObj_ignore_if_exists == Py_True) {
            req.ignore_if_exists = true;
        }
    }

    return req;
}

template<typename T>
T
get_dataverse_request_base(struct analytics_mgmt_options* options)
{
    T req{};

    PyObject* pyObj_dataverse_name = PyDict_GetItemString(options->op_args, "dataverse_name");
    auto dataverse_name = std::string(PyUnicode_AsUTF8(pyObj_dataverse_name));

    req.dataverse_name = dataverse_name;
    req.timeout = options->timeout_ms;

    return req;
}

couchbase::core::operations::management::analytics_dataverse_drop_request
get_dataverse_drop_request(struct analytics_mgmt_options* options)
{
    auto req = get_dataverse_request_base<couchbase::core::operations::management::analytics_dataverse_drop_request>(options);

    PyObject* pyObj_ignore_if_does_not_exist = PyDict_GetItemString(options->op_args, "ignore_if_does_not_exist");
    if (pyObj_ignore_if_does_not_exist) {
        if (pyObj_ignore_if_does_not_exist == Py_True) {
            req.ignore_if_does_not_exist = true;
        }
    }

    return req;
}

couchbase::core::operations::management::analytics_dataverse_create_request
get_dataverse_create_request(struct analytics_mgmt_options* options)
{
    auto req = get_dataverse_request_base<couchbase::core::operations::management::analytics_dataverse_create_request>(options);

    PyObject* pyObj_ignore_if_exists = PyDict_GetItemString(options->op_args, "ignore_if_exists");
    if (pyObj_ignore_if_exists) {
        if (pyObj_ignore_if_exists == Py_True) {
            req.ignore_if_exists = true;
        }
    }

    return req;
}

/* couchbase::core::operations::management::analytics_* response building methods */

template<typename T>
result*
create_base_result_from_analytics_mgmt_response(const T& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);

    PyObject* pyObj_tmp = PyUnicode_FromString(resp.status.c_str());
    if (-1 == PyDict_SetItemString(res->dict, "status", pyObj_tmp)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    PyObject* pyObj_query_problems = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& error : resp.errors) {
        PyObject* pyObj_query_problem = PyDict_New();
        pyObj_tmp = PyLong_FromUnsignedLongLong(error.code);
        if (-1 == PyDict_SetItemString(pyObj_query_problem, "code", pyObj_tmp)) {
            Py_XDECREF(pyObj_result);
            Py_XDECREF(pyObj_query_problems);
            Py_XDECREF(pyObj_query_problem);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(error.message.c_str());
        if (-1 == PyDict_SetItemString(pyObj_query_problem, "message", pyObj_tmp)) {
            Py_XDECREF(pyObj_result);
            Py_XDECREF(pyObj_query_problems);
            Py_DECREF(pyObj_query_problem);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }
    Py_ssize_t set_size = PyList_Size(pyObj_query_problems);
    if (set_size > 0) {
        if (-1 == PyDict_SetItemString(res->dict, "errors", pyObj_query_problems)) {
            Py_XDECREF(pyObj_result);
            Py_XDECREF(pyObj_query_problems);
            return nullptr;
        }
    }
    Py_DECREF(pyObj_query_problems);

    return res;
}

template<typename T>
result*
create_result_from_analytics_mgmt_response(const T& resp)
{
    return create_base_result_from_analytics_mgmt_response(resp);
}

template<>
result*
create_result_from_analytics_mgmt_response(const couchbase::core::operations::management::analytics_dataset_get_all_response& resp)
{
    auto res = create_base_result_from_analytics_mgmt_response(resp);
    if (res == nullptr) {
        return nullptr;
    }
    PyObject* pyObj_datasets = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& dataset : resp.datasets) {
        PyObject* pyObj_dataset = PyDict_New();
        PyObject* pyObj_tmp = PyUnicode_FromString(dataset.name.c_str());
        if (-1 == PyDict_SetItemString(pyObj_dataset, "dataset_name", pyObj_tmp)) {
            Py_XDECREF(pyObj_datasets);
            Py_XDECREF(pyObj_dataset);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(dataset.dataverse_name.c_str());
        if (-1 == PyDict_SetItemString(pyObj_dataset, "dataverse_name", pyObj_tmp)) {
            Py_XDECREF(pyObj_datasets);
            Py_DECREF(pyObj_dataset);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(dataset.link_name.c_str());
        if (-1 == PyDict_SetItemString(pyObj_dataset, "link_name", pyObj_tmp)) {
            Py_XDECREF(pyObj_datasets);
            Py_DECREF(pyObj_dataset);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(dataset.bucket_name.c_str());
        if (-1 == PyDict_SetItemString(pyObj_dataset, "bucket_name", pyObj_tmp)) {
            Py_XDECREF(pyObj_datasets);
            Py_DECREF(pyObj_dataset);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        PyList_Append(pyObj_datasets, pyObj_dataset);
        Py_DECREF(pyObj_dataset);
    }

    if (-1 == PyDict_SetItemString(res->dict, "datasets", pyObj_datasets)) {
        Py_XDECREF(pyObj_datasets);
        return nullptr;
    }
    Py_DECREF(pyObj_datasets);
    return res;
}

template<>
result*
create_result_from_analytics_mgmt_response(const couchbase::core::operations::management::analytics_index_get_all_response& resp)
{
    auto res = create_base_result_from_analytics_mgmt_response(resp);
    if (res == nullptr) {
        return nullptr;
    }
    PyObject* pyObj_indexes = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& index : resp.indexes) {
        PyObject* pyObj_index = PyDict_New();
        PyObject* pyObj_tmp = PyUnicode_FromString(index.name.c_str());
        if (-1 == PyDict_SetItemString(pyObj_index, "name", pyObj_tmp)) {
            Py_XDECREF(pyObj_indexes);
            Py_XDECREF(pyObj_index);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(index.dataverse_name.c_str());
        if (-1 == PyDict_SetItemString(pyObj_index, "dataverse_name", pyObj_tmp)) {
            Py_XDECREF(pyObj_indexes);
            Py_DECREF(pyObj_index);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(index.dataset_name.c_str());
        if (-1 == PyDict_SetItemString(pyObj_index, "dataset_name", pyObj_tmp)) {
            Py_XDECREF(pyObj_indexes);
            Py_DECREF(pyObj_index);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        if (index.is_primary) {
            if (-1 == PyDict_SetItemString(pyObj_index, "is_primary", Py_True)) {
                Py_XDECREF(pyObj_indexes);
                Py_DECREF(pyObj_index);
                return nullptr;
            }
        } else {
            if (-1 == PyDict_SetItemString(pyObj_index, "is_primary", Py_False)) {
                Py_XDECREF(pyObj_indexes);
                Py_DECREF(pyObj_index);
                return nullptr;
            }
        }

        PyList_Append(pyObj_indexes, pyObj_index);
        Py_DECREF(pyObj_index);
    }

    if (-1 == PyDict_SetItemString(res->dict, "indexes", pyObj_indexes)) {
        Py_XDECREF(pyObj_indexes);
        return nullptr;
    }
    Py_DECREF(pyObj_indexes);
    return res;
}

template<>
result*
create_result_from_analytics_mgmt_response(const couchbase::core::operations::management::analytics_get_pending_mutations_response& resp)
{
    auto res = create_base_result_from_analytics_mgmt_response(resp);
    if (res == nullptr) {
        return nullptr;
    }
    PyObject* pyObj_stats = PyDict_New();
    for (auto const& stat : resp.stats) {
        PyObject* pyObj_key = PyUnicode_FromString(stat.first.c_str());
        PyObject* pyObj_value = PyLong_FromUnsignedLongLong(stat.second);
        if (-1 == PyDict_SetItem(pyObj_stats, pyObj_key, pyObj_value)) {
            Py_XDECREF(pyObj_stats);
            Py_XDECREF(pyObj_key);
            Py_XDECREF(pyObj_value);
            return nullptr;
        }
        Py_DECREF(pyObj_key);
        Py_DECREF(pyObj_value);
    }

    if (-1 == PyDict_SetItemString(res->dict, "stats", pyObj_stats)) {
        Py_XDECREF(pyObj_stats);
        return nullptr;
    }
    Py_DECREF(pyObj_stats);
    return res;
}

template<>
result*
create_result_from_analytics_mgmt_response(const couchbase::core::operations::management::analytics_link_get_all_response& resp)
{
    auto res = create_base_result_from_analytics_mgmt_response(resp);
    if (res == nullptr) {
        return nullptr;
    }
    PyObject* pyObj_couchbase_links = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& link : resp.couchbase) {
        PyObject* pyObj_link = build_couchbase_remote_link(link);
        if (pyObj_link == nullptr) {
            Py_XDECREF(pyObj_couchbase_links);
            return nullptr;
        }
        PyList_Append(pyObj_couchbase_links, pyObj_link);
        Py_DECREF(pyObj_link);
    }
    if (-1 == PyDict_SetItemString(res->dict, "couchbase_links", pyObj_couchbase_links)) {
        Py_XDECREF(pyObj_couchbase_links);
        return nullptr;
    }
    Py_DECREF(pyObj_couchbase_links);

    PyObject* pyObj_s3_links = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& link : resp.s3) {
        PyObject* pyObj_link = build_s3_link(link);
        if (pyObj_link == nullptr) {
            Py_XDECREF(pyObj_couchbase_links);
            Py_XDECREF(pyObj_s3_links);
            return nullptr;
        }
        PyList_Append(pyObj_s3_links, pyObj_link);
        Py_DECREF(pyObj_link);
    }
    if (-1 == PyDict_SetItemString(res->dict, "s3_links", pyObj_s3_links)) {
        Py_XDECREF(pyObj_couchbase_links);
        Py_XDECREF(pyObj_s3_links);
        return nullptr;
    }
    Py_DECREF(pyObj_s3_links);

    PyObject* pyObj_azure_blob_links = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& link : resp.azure_blob) {
        PyObject* pyObj_link = build_azure_blob_link(link);
        if (pyObj_link == nullptr) {
            Py_XDECREF(pyObj_couchbase_links);
            Py_XDECREF(pyObj_s3_links);
            Py_XDECREF(pyObj_azure_blob_links);
            return nullptr;
        }
        PyList_Append(pyObj_azure_blob_links, pyObj_link);
        Py_DECREF(pyObj_link);
    }
    if (-1 == PyDict_SetItemString(res->dict, "azure_blob_links", pyObj_azure_blob_links)) {
        Py_XDECREF(pyObj_couchbase_links);
        Py_XDECREF(pyObj_s3_links);
        Py_XDECREF(pyObj_azure_blob_links);
        return nullptr;
    }
    Py_DECREF(pyObj_azure_blob_links);

    return res;
}

template<typename Response>
void
create_result_from_analytics_mgmt_op_response(const Response& resp,
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
        pyObj_exc =
          build_exception_from_context(resp.ctx, __FILE__, __LINE__, "Error doing analytics index mgmt operation.", "AnalyticsIndexMgmt");
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
        auto res = create_result_from_analytics_mgmt_response(resp);
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
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "Analytics index mgmt operation error.");
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

template<typename Request>
PyObject*
do_analytics_mgmt_op(connection& conn,
                     Request& req,
                     PyObject* pyObj_callback,
                     PyObject* pyObj_errback,
                     std::shared_ptr<std::promise<PyObject*>> barrier)
{
    using response_type = typename Request::response_type;
    Py_BEGIN_ALLOW_THREADS conn.cluster_->execute(req, [pyObj_callback, pyObj_errback, barrier](response_type resp) {
        create_result_from_analytics_mgmt_op_response(resp, pyObj_callback, pyObj_errback, barrier);
    });
    Py_END_ALLOW_THREADS Py_RETURN_NONE;
}

PyObject*
handle_analytics_mgmt_op(connection* conn, struct analytics_mgmt_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback)
{
    PyObject* res = nullptr;
    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();
    switch (options->op_type) {
        case AnalyticsManagementOperations::CREATE_DATAVERSE: {
            auto req = get_dataverse_create_request(options);
            res = do_analytics_mgmt_op<couchbase::core::operations::management::analytics_dataverse_create_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case AnalyticsManagementOperations::CREATE_DATASET: {
            auto req = get_dataset_create_request(options);
            res = do_analytics_mgmt_op<couchbase::core::operations::management::analytics_dataset_create_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case AnalyticsManagementOperations::CREATE_INDEX: {
            auto req = get_index_create_request(options);
            res = do_analytics_mgmt_op<couchbase::core::operations::management::analytics_index_create_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case AnalyticsManagementOperations::GET_ALL_DATASETS: {
            couchbase::core::operations::management::analytics_dataset_get_all_request req{};
            PyObject* pyObj_client_context_id = PyDict_GetItemString(options->op_args, "client_context_id");
            if (pyObj_client_context_id != nullptr) {
                auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
                req.client_context_id = client_context_id;
            }

            req.timeout = options->timeout_ms;
            res = do_analytics_mgmt_op<couchbase::core::operations::management::analytics_dataset_get_all_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case AnalyticsManagementOperations::GET_ALL_INDEXES: {
            couchbase::core::operations::management::analytics_index_get_all_request req{};
            PyObject* pyObj_client_context_id = PyDict_GetItemString(options->op_args, "client_context_id");
            if (pyObj_client_context_id != nullptr) {
                auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
                req.client_context_id = client_context_id;
            }

            req.timeout = options->timeout_ms;
            res = do_analytics_mgmt_op<couchbase::core::operations::management::analytics_index_get_all_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case AnalyticsManagementOperations::DROP_DATAVERSE: {
            auto req = get_dataverse_drop_request(options);
            res = do_analytics_mgmt_op<couchbase::core::operations::management::analytics_dataverse_drop_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case AnalyticsManagementOperations::DROP_DATASET: {
            auto req = get_dataset_drop_request(options);
            res = do_analytics_mgmt_op<couchbase::core::operations::management::analytics_dataset_drop_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case AnalyticsManagementOperations::DROP_INDEX: {
            auto req = get_index_drop_request(options);
            res = do_analytics_mgmt_op<couchbase::core::operations::management::analytics_index_drop_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case AnalyticsManagementOperations::GET_PENDING_MUTATIONS: {
            couchbase::core::operations::management::analytics_get_pending_mutations_request req{};
            PyObject* pyObj_client_context_id = PyDict_GetItemString(options->op_args, "client_context_id");
            if (pyObj_client_context_id != nullptr) {
                auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
                req.client_context_id = client_context_id;
            }

            req.timeout = options->timeout_ms;
            res = do_analytics_mgmt_op<couchbase::core::operations::management::analytics_get_pending_mutations_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case AnalyticsManagementOperations::LINK_CREATE: {
            PyObject* pyObj_link_type = PyDict_GetItemString(options->op_args, "link_type");
            if (pyObj_link_type == nullptr) {
                pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Invalid analytics link type.");
                Py_XDECREF(pyObj_callback);
                Py_XDECREF(pyObj_errback);
            }
            auto link_type = std::string(PyUnicode_AsUTF8(pyObj_link_type));
            if (link_type.compare("couchbase") == 0) {
                auto req = get_analytics_link_create_request<couchbase::core::management::analytics::couchbase_remote_link>(options);
                res = do_analytics_mgmt_op<couchbase::core::operations::management::analytics_link_create_request<
                  couchbase::core::management::analytics::couchbase_remote_link>>(*conn, req, pyObj_callback, pyObj_errback, barrier);
            } else if (link_type.compare("s3") == 0) {
                auto req = get_analytics_link_create_request<couchbase::core::management::analytics::s3_external_link>(options);
                res = do_analytics_mgmt_op<couchbase::core::operations::management::analytics_link_create_request<
                  couchbase::core::management::analytics::s3_external_link>>(*conn, req, pyObj_callback, pyObj_errback, barrier);
            } else if (link_type.compare("azureblob") == 0) {
                auto req = get_analytics_link_create_request<couchbase::core::management::analytics::azure_blob_external_link>(options);
                res = do_analytics_mgmt_op<couchbase::core::operations::management::analytics_link_create_request<
                  couchbase::core::management::analytics::azure_blob_external_link>>(*conn, req, pyObj_callback, pyObj_errback, barrier);
            }
            break;
        }
        case AnalyticsManagementOperations::LINK_CONNECT: {
            auto req = get_link_connect_request(options);
            res = do_analytics_mgmt_op<couchbase::core::operations::management::analytics_link_connect_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case AnalyticsManagementOperations::GET_ALL_LINKS: {
            auto req = get_link_get_all_request(options);
            res = do_analytics_mgmt_op<couchbase::core::operations::management::analytics_link_get_all_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case AnalyticsManagementOperations::LINK_DISCONNECT: {
            auto req = get_link_disconnect_request(options);
            res = do_analytics_mgmt_op<couchbase::core::operations::management::analytics_link_disconnect_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case AnalyticsManagementOperations::LINK_REPLACE: {
            PyObject* pyObj_link_type = PyDict_GetItemString(options->op_args, "link_type");
            if (pyObj_link_type == nullptr) {
                pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Invalid analytics link type.");
                Py_XDECREF(pyObj_callback);
                Py_XDECREF(pyObj_errback);
                return nullptr;
            }
            auto link_type = std::string(PyUnicode_AsUTF8(pyObj_link_type));
            if (link_type.compare("couchbase") == 0) {
                auto req = get_analytics_link_replace_request<couchbase::core::management::analytics::couchbase_remote_link>(options);
                res = do_analytics_mgmt_op<couchbase::core::operations::management::analytics_link_replace_request<
                  couchbase::core::management::analytics::couchbase_remote_link>>(*conn, req, pyObj_callback, pyObj_errback, barrier);
            } else if (link_type.compare("s3") == 0) {
                auto req = get_analytics_link_replace_request<couchbase::core::management::analytics::s3_external_link>(options);
                res = do_analytics_mgmt_op<couchbase::core::operations::management::analytics_link_replace_request<
                  couchbase::core::management::analytics::s3_external_link>>(*conn, req, pyObj_callback, pyObj_errback, barrier);
            } else if (link_type.compare("azureblob") == 0) {
                auto req = get_analytics_link_replace_request<couchbase::core::management::analytics::azure_blob_external_link>(options);
                res = do_analytics_mgmt_op<couchbase::core::operations::management::analytics_link_replace_request<
                  couchbase::core::management::analytics::azure_blob_external_link>>(*conn, req, pyObj_callback, pyObj_errback, barrier);
            }
            break;
        }
        case AnalyticsManagementOperations::DROP_LINK: {
            auto req = get_link_drop_request(options);
            res = do_analytics_mgmt_op<couchbase::core::operations::management::analytics_link_drop_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        default: {
            pycbc_set_python_exception(
              PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized analytics index mgmt operation passed in.");
            barrier->set_value(nullptr);
            Py_XDECREF(pyObj_callback);
            Py_XDECREF(pyObj_errback);
            break;
        }
    };
    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        PyObject* ret = nullptr;
        Py_BEGIN_ALLOW_THREADS ret = f.get();
        Py_END_ALLOW_THREADS return ret;
    }
    return res;
}

void
add_analytics_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class)
{
    PyObject* pyObj_enum_values = PyUnicode_FromString(AnalyticsManagementOperations::ALL_OPERATIONS());
    PyObject* pyObj_enum_name = PyUnicode_FromString("AnalyticsManagementOperations");
    // PyTuple_Pack returns new reference, need to Py_DECREF values provided
    PyObject* pyObj_args = PyTuple_Pack(2, pyObj_enum_name, pyObj_enum_values);
    Py_DECREF(pyObj_enum_name);
    Py_DECREF(pyObj_enum_values);

    PyObject* pyObj_kwargs = PyDict_New();
    PyObject_SetItem(pyObj_kwargs, PyUnicode_FromString("module"), PyModule_GetNameObject(pyObj_module));
    PyObject* pyObj_mgmt_operations = PyObject_Call(pyObj_enum_class, pyObj_args, pyObj_kwargs);
    Py_DECREF(pyObj_args);
    Py_DECREF(pyObj_kwargs);

    if (PyModule_AddObject(pyObj_module, "analytics_mgmt_operations", pyObj_mgmt_operations) < 0) {
        // only need to Py_DECREF on failure to add when using PyModule_AddObject()
        Py_XDECREF(pyObj_mgmt_operations);
        return;
    }
}
