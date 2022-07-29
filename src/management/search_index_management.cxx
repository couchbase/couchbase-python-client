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

#include "search_index_management.hxx"
#include "../exceptions.hxx"
#include <core/management/search_index.hxx>

PyObject*
build_search_index(couchbase::core::management::search::index index)
{
    PyObject* pyObj_index = PyDict_New();

    PyObject* pyObj_tmp = PyUnicode_FromString(index.uuid.c_str());
    if (-1 == PyDict_SetItemString(pyObj_index, "uuid", pyObj_tmp)) {
        Py_XDECREF(pyObj_index);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(index.name.c_str());
    if (-1 == PyDict_SetItemString(pyObj_index, "name", pyObj_tmp)) {
        Py_DECREF(pyObj_index);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(index.type.c_str());
    if (-1 == PyDict_SetItemString(pyObj_index, "type", pyObj_tmp)) {
        Py_DECREF(pyObj_index);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (!index.params_json.empty()) {
        pyObj_tmp = PyUnicode_FromString(index.params_json.c_str());
        if (-1 == PyDict_SetItemString(pyObj_index, "params_json", pyObj_tmp)) {
            Py_DECREF(pyObj_index);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    pyObj_tmp = PyUnicode_FromString(index.source_uuid.c_str());
    if (-1 == PyDict_SetItemString(pyObj_index, "source_uuid", pyObj_tmp)) {
        Py_DECREF(pyObj_index);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(index.source_name.c_str());
    if (-1 == PyDict_SetItemString(pyObj_index, "source_name", pyObj_tmp)) {
        Py_DECREF(pyObj_index);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (!index.source_params_json.empty()) {
        pyObj_tmp = PyUnicode_FromString(index.source_params_json.c_str());
        if (-1 == PyDict_SetItemString(pyObj_index, "source_params_json", pyObj_tmp)) {
            Py_DECREF(pyObj_index);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (!index.plan_params_json.empty()) {
        pyObj_tmp = PyUnicode_FromString(index.plan_params_json.c_str());
        if (-1 == PyDict_SetItemString(pyObj_index, "plan_params_json", pyObj_tmp)) {
            Py_DECREF(pyObj_index);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    return pyObj_index;
}

template<typename T>
result*
create_result_from_search_index_mgmt_response(const T& resp)
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

    pyObj_tmp = PyUnicode_FromString(resp.error.c_str());
    if (-1 == PyDict_SetItemString(res->dict, "error", pyObj_tmp)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    return res;
}

template<>
result*
create_result_from_search_index_mgmt_response(const couchbase::core::operations::management::search_index_get_response& resp)
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

    pyObj_tmp = PyUnicode_FromString(resp.error.c_str());
    if (-1 == PyDict_SetItemString(res->dict, "error", pyObj_tmp)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = build_search_index(resp.index);
    if (pyObj_tmp == nullptr) {
        Py_XDECREF(pyObj_result);
        return nullptr;
    }
    if (-1 == PyDict_SetItemString(res->dict, "index", pyObj_tmp)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    return res;
}

template<>
result*
create_result_from_search_index_mgmt_response(const couchbase::core::operations::management::search_index_get_all_response& resp)
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

    pyObj_tmp = PyUnicode_FromString(resp.impl_version.c_str());
    if (-1 == PyDict_SetItemString(res->dict, "impl_version", pyObj_tmp)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    PyObject* pyObj_indexes = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& index : resp.indexes) {
        PyObject* pyObj_index = build_search_index(index);
        if (pyObj_index == nullptr) {
            Py_XDECREF(pyObj_result);
            Py_XDECREF(pyObj_indexes);
            return nullptr;
        }
        PyList_Append(pyObj_indexes, pyObj_index);
        Py_DECREF(pyObj_index);
    }

    if (-1 == PyDict_SetItemString(res->dict, "indexes", pyObj_indexes)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_indexes);
        return nullptr;
    }
    Py_DECREF(pyObj_indexes);

    return res;
}

template<>
result*
create_result_from_search_index_mgmt_response(
  const couchbase::core::operations::management::search_index_get_documents_count_response& resp)
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

    pyObj_tmp = PyUnicode_FromString(resp.error.c_str());
    if (-1 == PyDict_SetItemString(res->dict, "error", pyObj_tmp)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromLongLong(resp.count);
    if (-1 == PyDict_SetItemString(res->dict, "count", pyObj_tmp)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    return res;
}

template<>
result*
create_result_from_search_index_mgmt_response(const couchbase::core::operations::management::search_index_get_stats_response& resp)
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

    pyObj_tmp = PyUnicode_FromString(resp.error.c_str());
    if (-1 == PyDict_SetItemString(res->dict, "error", pyObj_tmp)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(resp.stats.c_str());
    if (-1 == PyDict_SetItemString(res->dict, "stats", pyObj_tmp)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    return res;
}

template<>
result*
create_result_from_search_index_mgmt_response(const couchbase::core::operations::management::search_index_analyze_document_response& resp)
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

    pyObj_tmp = PyUnicode_FromString(resp.error.c_str());
    if (-1 == PyDict_SetItemString(res->dict, "error", pyObj_tmp)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(resp.analysis.c_str());
    if (-1 == PyDict_SetItemString(res->dict, "analysis", pyObj_tmp)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    return res;
}

template<>
result*
create_result_from_search_index_mgmt_response(const couchbase::core::operations::management::search_index_stats_response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);

    PyObject* pyObj_stats = PyUnicode_FromString(resp.stats.c_str());
    if (-1 == PyDict_SetItemString(res->dict, "stats", pyObj_stats)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_stats);
        return nullptr;
    }
    Py_DECREF(pyObj_stats);

    return res;
}

template<typename Response>
void
create_result_from_search_index_mgmt_op_response(const Response& resp,
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
          build_exception_from_context(resp.ctx, __FILE__, __LINE__, "Error doing search index mgmt operation.", "SearchIndexMgmt");
        if (pyObj_errback == nullptr) {
            barrier->set_value(pyObj_exc);
        } else {
            // pyObj_exc = build_exception_from_context(resp.ctx);
            pyObj_func = pyObj_errback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, pyObj_exc);
        }
        // lets clear any errors
        PyErr_Clear();
    } else {
        auto res = create_result_from_search_index_mgmt_response(resp);
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
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "Search index mgmt operation error.");
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

couchbase::core::management::search::index
get_search_index(PyObject* pyObj_index)
{
    couchbase::core::management::search::index index{};

    PyObject* pyObj_uuid = PyDict_GetItemString(pyObj_index, "uuid");
    if (pyObj_uuid != nullptr) {
        auto uuid = std::string(PyUnicode_AsUTF8(pyObj_uuid));
        index.uuid = uuid;
    }

    PyObject* pyObj_name = PyDict_GetItemString(pyObj_index, "name");
    if (pyObj_name != nullptr) {
        auto name = std::string(PyUnicode_AsUTF8(pyObj_name));
        index.name = name;
    }

    PyObject* pyObj_type = PyDict_GetItemString(pyObj_index, "type");
    if (pyObj_type != nullptr) {
        auto type = std::string(PyUnicode_AsUTF8(pyObj_type));
        index.type = type;
    }

    PyObject* pyObj_params_json = PyDict_GetItemString(pyObj_index, "params_json");
    if (pyObj_params_json != nullptr) {
        auto params_json = std::string(PyUnicode_AsUTF8(pyObj_params_json));
        index.params_json = params_json;
    }

    PyObject* pyObj_source_uuid = PyDict_GetItemString(pyObj_index, "source_uuid");
    if (pyObj_source_uuid != nullptr) {
        auto source_uuid = std::string(PyUnicode_AsUTF8(pyObj_source_uuid));
        index.source_uuid = source_uuid;
    }

    PyObject* pyObj_source_name = PyDict_GetItemString(pyObj_index, "source_name");
    if (pyObj_source_name != nullptr) {
        auto source_name = std::string(PyUnicode_AsUTF8(pyObj_source_name));
        index.source_name = source_name;
    }

    PyObject* pyObj_source_type = PyDict_GetItemString(pyObj_index, "source_type");
    if (pyObj_source_type != nullptr) {
        auto source_type = std::string(PyUnicode_AsUTF8(pyObj_source_type));
        index.source_type = source_type;
    }

    PyObject* pyObj_source_params_json = PyDict_GetItemString(pyObj_index, "source_params_json");
    if (pyObj_source_params_json != nullptr) {
        auto source_params_json = std::string(PyUnicode_AsUTF8(pyObj_source_params_json));
        index.source_params_json = source_params_json;
    }

    PyObject* pyObj_plan_params_json = PyDict_GetItemString(pyObj_index, "plan_params_json");
    if (pyObj_plan_params_json != nullptr) {
        auto plan_params_json = std::string(PyUnicode_AsUTF8(pyObj_plan_params_json));
        index.plan_params_json = plan_params_json;
    }

    return index;
}

couchbase::core::operations::management::search_index_control_ingest_request
get_search_index_control_ingest_req(PyObject* op_args)
{
    couchbase::core::operations::management::search_index_control_ingest_request req{};

    PyObject* pyObj_index_name = PyDict_GetItemString(op_args, "index_name");
    auto index_name = std::string(PyUnicode_AsUTF8(pyObj_index_name));
    req.index_name = index_name;

    PyObject* pyObj_pause = PyDict_GetItemString(op_args, "pause");
    if (pyObj_pause != nullptr) {
        if (pyObj_pause == Py_True) {
            req.pause = true;
        } else {
            req.pause = false;
        }
    }

    PyObject* pyObj_client_context_id = PyDict_GetItemString(op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    return req;
}

couchbase::core::operations::management::search_index_control_plan_freeze_request
get_search_index_control_freeze_req(PyObject* op_args)
{
    couchbase::core::operations::management::search_index_control_plan_freeze_request req{};

    PyObject* pyObj_index_name = PyDict_GetItemString(op_args, "index_name");
    auto index_name = std::string(PyUnicode_AsUTF8(pyObj_index_name));
    req.index_name = index_name;

    PyObject* pyObj_freeze = PyDict_GetItemString(op_args, "freeze");
    if (pyObj_freeze != nullptr) {
        if (pyObj_freeze == Py_True) {
            req.freeze = true;
        } else {
            req.freeze = false;
        }
    }

    PyObject* pyObj_client_context_id = PyDict_GetItemString(op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    return req;
}

couchbase::core::operations::management::search_index_control_query_request
get_search_index_control_query_req(PyObject* op_args)
{
    couchbase::core::operations::management::search_index_control_query_request req{};

    PyObject* pyObj_index_name = PyDict_GetItemString(op_args, "index_name");
    auto index_name = std::string(PyUnicode_AsUTF8(pyObj_index_name));
    req.index_name = index_name;

    PyObject* pyObj_allow = PyDict_GetItemString(op_args, "allow");
    if (pyObj_allow != nullptr) {
        if (pyObj_allow == Py_True) {
            req.allow = true;
        } else {
            req.allow = false;
        }
    }

    PyObject* pyObj_client_context_id = PyDict_GetItemString(op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    return req;
}

couchbase::core::operations::management::search_index_analyze_document_request
get_search_index_analyze_doc_req(PyObject* op_args)
{
    couchbase::core::operations::management::search_index_analyze_document_request req{};

    PyObject* pyObj_index_name = PyDict_GetItemString(op_args, "index_name");
    auto index_name = std::string(PyUnicode_AsUTF8(pyObj_index_name));
    req.index_name = index_name;

    PyObject* pyObj_encoded_document = PyDict_GetItemString(op_args, "encoded_document");
    auto encoded_document = std::string(PyUnicode_AsUTF8(pyObj_encoded_document));
    req.encoded_document = encoded_document;

    PyObject* pyObj_client_context_id = PyDict_GetItemString(op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    return req;
}

template<typename T>
T
get_search_index_with_name_req(PyObject* op_args)
{
    T req{};

    PyObject* pyObj_index_name = PyDict_GetItemString(op_args, "index_name");
    auto index_name = std::string(PyUnicode_AsUTF8(pyObj_index_name));
    req.index_name = index_name;

    PyObject* pyObj_client_context_id = PyDict_GetItemString(op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    return req;
}

template<typename T>
T
get_search_index_req(PyObject* op_args)
{
    T req{};

    PyObject* pyObj_client_context_id = PyDict_GetItemString(op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    return req;
}

couchbase::core::operations::management::search_index_upsert_request
get_search_index_upsert_req(PyObject* op_args)
{
    couchbase::core::operations::management::search_index_upsert_request req{};

    PyObject* pyObj_index = PyDict_GetItemString(op_args, "index");
    Py_INCREF(pyObj_index);
    req.index = get_search_index(pyObj_index);
    Py_DECREF(pyObj_index);

    PyObject* pyObj_client_context_id = PyDict_GetItemString(op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    return req;
}

template<typename Request>
PyObject*
do_search_index_mgmt_op(connection& conn,
                        Request& req,
                        PyObject* pyObj_callback,
                        PyObject* pyObj_errback,
                        std::shared_ptr<std::promise<PyObject*>> barrier)
{
    using response_type = typename Request::response_type;
    Py_BEGIN_ALLOW_THREADS conn.cluster_->execute(req, [pyObj_callback, pyObj_errback, barrier](response_type resp) {
        create_result_from_search_index_mgmt_op_response(resp, pyObj_callback, pyObj_errback, barrier);
    });
    Py_END_ALLOW_THREADS Py_RETURN_NONE;
}

PyObject*
handle_search_index_mgmt_op(connection* conn, struct search_index_mgmt_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback)
{
    PyObject* res = nullptr;
    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();
    switch (options->op_type) {
        case SearchIndexManagementOperations::UPSERT_INDEX: {
            auto req = get_search_index_upsert_req(options->op_args);
            req.timeout = options->timeout_ms;

            res = do_search_index_mgmt_op<couchbase::core::operations::management::search_index_upsert_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case SearchIndexManagementOperations::GET_INDEX: {
            auto req = get_search_index_with_name_req<couchbase::core::operations::management::search_index_get_request>(options->op_args);
            req.timeout = options->timeout_ms;

            res = do_search_index_mgmt_op<couchbase::core::operations::management::search_index_get_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case SearchIndexManagementOperations::DROP_INDEX: {
            auto req = get_search_index_with_name_req<couchbase::core::operations::management::search_index_drop_request>(options->op_args);
            req.timeout = options->timeout_ms;

            res = do_search_index_mgmt_op<couchbase::core::operations::management::search_index_drop_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case SearchIndexManagementOperations::GET_INDEX_DOCUMENT_COUNT: {
            auto req = get_search_index_with_name_req<couchbase::core::operations::management::search_index_get_documents_count_request>(
              options->op_args);
            req.timeout = options->timeout_ms;

            res = do_search_index_mgmt_op<couchbase::core::operations::management::search_index_get_documents_count_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case SearchIndexManagementOperations::GET_ALL_INDEXES: {
            auto req = get_search_index_req<couchbase::core::operations::management::search_index_get_all_request>(options->op_args);
            req.timeout = options->timeout_ms;

            res = do_search_index_mgmt_op<couchbase::core::operations::management::search_index_get_all_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case SearchIndexManagementOperations::GET_INDEX_STATS: {
            auto req =
              get_search_index_with_name_req<couchbase::core::operations::management::search_index_get_stats_request>(options->op_args);
            req.timeout = options->timeout_ms;

            res = do_search_index_mgmt_op<couchbase::core::operations::management::search_index_get_stats_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case SearchIndexManagementOperations::GET_ALL_STATS: {
            auto req = get_search_index_req<couchbase::core::operations::management::search_index_stats_request>(options->op_args);
            req.timeout = options->timeout_ms;

            res = do_search_index_mgmt_op<couchbase::core::operations::management::search_index_stats_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case SearchIndexManagementOperations::FREEZE_PLAN: {
            auto req = get_search_index_control_freeze_req(options->op_args);
            req.timeout = options->timeout_ms;

            res = do_search_index_mgmt_op<couchbase::core::operations::management::search_index_control_plan_freeze_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case SearchIndexManagementOperations::CONTROL_INGEST: {
            auto req = get_search_index_control_ingest_req(options->op_args);
            req.timeout = options->timeout_ms;

            res = do_search_index_mgmt_op<couchbase::core::operations::management::search_index_control_ingest_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case SearchIndexManagementOperations::ANALYZE_DOCUMENT: {
            auto req = get_search_index_analyze_doc_req(options->op_args);
            req.timeout = options->timeout_ms;

            res = do_search_index_mgmt_op<couchbase::core::operations::management::search_index_analyze_document_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case SearchIndexManagementOperations::CONTROL_QUERY: {
            auto req = get_search_index_control_query_req(options->op_args);
            req.timeout = options->timeout_ms;

            res = do_search_index_mgmt_op<couchbase::core::operations::management::search_index_control_query_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        default: {
            pycbc_set_python_exception(
              PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized search index mgmt operation passed in.");
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
add_search_index_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class)
{
    PyObject* pyObj_enum_values = PyUnicode_FromString(SearchIndexManagementOperations::ALL_OPERATIONS());
    PyObject* pyObj_enum_name = PyUnicode_FromString("SearchIndexManagementOperations");
    // PyTuple_Pack returns new reference, need to Py_DECREF values provided
    PyObject* pyObj_args = PyTuple_Pack(2, pyObj_enum_name, pyObj_enum_values);
    Py_DECREF(pyObj_enum_name);
    Py_DECREF(pyObj_enum_values);

    PyObject* pyObj_kwargs = PyDict_New();
    PyObject_SetItem(pyObj_kwargs, PyUnicode_FromString("module"), PyModule_GetNameObject(pyObj_module));
    PyObject* pyObj_mgmt_operations = PyObject_Call(pyObj_enum_class, pyObj_args, pyObj_kwargs);
    Py_DECREF(pyObj_args);
    Py_DECREF(pyObj_kwargs);

    if (PyModule_AddObject(pyObj_module, "search_index_mgmt_operations", pyObj_mgmt_operations) < 0) {
        // only need to Py_DECREF on failure to add when using PyModule_AddObject()
        Py_XDECREF(pyObj_mgmt_operations);
        return;
    }
}
