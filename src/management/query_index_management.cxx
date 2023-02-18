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

#include "query_index_management.hxx"
#include "../exceptions.hxx"
#include <couchbase/management/query_index.hxx>

PyObject*
build_query_index(const couchbase::management::query::index& index)
{
    PyObject* pyObj_index = PyDict_New();
    if (index.is_primary) {
        //@TODO:  I do not think an increment is necessary since adding to the
        //  dict will increment the ref
        // Py_INCREF(Py_True);
        if (-1 == PyDict_SetItemString(pyObj_index, "is_primary", Py_True)) {
            Py_DECREF(pyObj_index);
            return nullptr;
        }
    } else {
        // Py_INCREF(Py_False);
        if (-1 == PyDict_SetItemString(pyObj_index, "is_primary", Py_False)) {
            Py_DECREF(pyObj_index);
            return nullptr;
        }
    }

    PyObject* pyObj_tmp = PyUnicode_FromString(index.name.c_str());
    if (-1 == PyDict_SetItemString(pyObj_index, "name", pyObj_tmp)) {
        Py_DECREF(pyObj_index);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(index.state.c_str());
    if (-1 == PyDict_SetItemString(pyObj_index, "state", pyObj_tmp)) {
        Py_DECREF(pyObj_index);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (index.collection_name.has_value()) {
        pyObj_tmp = PyUnicode_FromString(index.collection_name->c_str());
        if (-1 == PyDict_SetItemString(pyObj_index, "collection_name", pyObj_tmp)) {
            Py_DECREF(pyObj_index);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    pyObj_tmp = PyUnicode_FromString(index.type.c_str());
    if (-1 == PyDict_SetItemString(pyObj_index, "type", pyObj_tmp)) {
        Py_DECREF(pyObj_index);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (index.index_key.size() > 0) {
        PyObject* pyObj_index_keys = PyList_New(static_cast<Py_ssize_t>(0));
        for (auto const& index_key : index.index_key) {
            PyObject* pyObj_index_key = PyUnicode_FromString(index_key.c_str());
            PyList_Append(pyObj_index_keys, pyObj_index_key);
            Py_DECREF(pyObj_index_key);
        }

        if (-1 == PyDict_SetItemString(pyObj_index, "index_key", pyObj_index_keys)) {
            Py_DECREF(pyObj_index);
            Py_XDECREF(pyObj_index_keys);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_index_keys);
    }

    if (index.partition.has_value()) {
        pyObj_tmp = PyUnicode_FromString(index.partition.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_index, "partition", pyObj_tmp)) {
            Py_DECREF(pyObj_index);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (index.condition.has_value()) {
        pyObj_tmp = PyUnicode_FromString(index.condition.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_index, "condition", pyObj_tmp)) {
            Py_DECREF(pyObj_index);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    pyObj_tmp = PyUnicode_FromString(index.bucket_name.c_str());
    if (-1 == PyDict_SetItemString(pyObj_index, "bucket_name", pyObj_tmp)) {
        Py_DECREF(pyObj_index);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (index.scope_name.has_value()) {
        pyObj_tmp = PyUnicode_FromString(index.scope_name.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_index, "scope_name", pyObj_tmp)) {
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
create_result_from_query_index_mgmt_response(const T& resp)
{
    PyObject* result_obj = create_result_obj();
    result* res = reinterpret_cast<result*>(result_obj);

    PyObject* pyObj_tmp = PyUnicode_FromString(resp.status.c_str());
    if (-1 == PyDict_SetItemString(res->dict, "status", pyObj_tmp)) {
        Py_XDECREF(result_obj);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    PyObject* pyObj_query_problems = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& error : resp.errors) {
        PyObject* pyObj_query_problem = PyDict_New();
        pyObj_tmp = PyLong_FromUnsignedLongLong(error.code);
        if (-1 == PyDict_SetItemString(pyObj_query_problem, "code", pyObj_tmp)) {
            Py_XDECREF(result_obj);
            Py_XDECREF(pyObj_query_problems);
            Py_XDECREF(pyObj_query_problem);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(error.message.c_str());
        if (-1 == PyDict_SetItemString(pyObj_query_problem, "message", pyObj_tmp)) {
            Py_XDECREF(result_obj);
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
            Py_XDECREF(result_obj);
            Py_XDECREF(pyObj_query_problems);
            return nullptr;
        }
    }
    Py_DECREF(pyObj_query_problems);

    return res;
}

template<>
result*
create_result_from_query_index_mgmt_response(const couchbase::core::operations::management::query_index_get_all_response& resp)
{
    PyObject* result_obj = create_result_obj();
    result* res = reinterpret_cast<result*>(result_obj);

    PyObject* pyObj_tmp = PyUnicode_FromString(resp.status.c_str());
    if (-1 == PyDict_SetItemString(res->dict, "status", pyObj_tmp)) {
        Py_XDECREF(result_obj);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    PyObject* pyObj_indexes = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& index : resp.indexes) {
        PyObject* pyObj_index = build_query_index(index);
        if (pyObj_index == nullptr) {
            Py_XDECREF(result_obj);
            Py_XDECREF(pyObj_indexes);
            return nullptr;
        }
        PyList_Append(pyObj_indexes, pyObj_index);
        Py_DECREF(pyObj_index);
    }

    if (-1 == PyDict_SetItemString(res->dict, "indexes", pyObj_indexes)) {
        Py_XDECREF(result_obj);
        Py_XDECREF(pyObj_indexes);
        return nullptr;
    }
    Py_DECREF(pyObj_indexes);

    return res;
}

template<typename Response>
void
create_result_from_query_index_mgmt_op_response(const Response& resp,
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
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "Error doing query index mgmt operation.", "QueryIndexMgmt");
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
        auto res = create_result_from_query_index_mgmt_response(resp);
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
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "Collection mgmt operation error.");
        if (pyObj_errback == nullptr) {
            barrier->set_value(pyObj_exc);
        } else {
            pyObj_func = pyObj_errback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, pyObj_exc);
        }
    } else if (pyObj_func != nullptr) {
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

template<>
void
create_result_from_query_index_mgmt_op_response(const couchbase::manager_error_context& ctx,
                                                PyObject* pyObj_callback,
                                                PyObject* pyObj_errback,
                                                std::shared_ptr<std::promise<PyObject*>> barrier)
{
    PyObject* pyObj_args = nullptr;
    PyObject* pyObj_kwargs = nullptr;
    PyObject* pyObj_func = nullptr;
    PyObject* pyObj_exc = nullptr;
    PyObject* pyObj_callback_res = nullptr;

    PyGILState_STATE state = PyGILState_Ensure();
    if (ctx.ec()) {
        pyObj_exc = build_exception_from_context(ctx, __FILE__, __LINE__, "Error doing query index mgmt operation.", "QueryIndexMgmt");
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
        Py_INCREF(Py_None);
        if (pyObj_callback == nullptr) {
            barrier->set_value(Py_None);
        } else {
            pyObj_func = pyObj_callback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, Py_None);
        }
    }

    if (pyObj_func != nullptr) {
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

couchbase::core::operations::management::query_index_create_request
get_create_query_index_req(PyObject* op_args)
{
    couchbase::core::operations::management::query_index_create_request req{};

    PyObject* pyObj_scope_name = PyDict_GetItemString(op_args, "scope_name");
    if (pyObj_scope_name != nullptr) {
        auto scope_name = std::string(PyUnicode_AsUTF8(pyObj_scope_name));
        req.scope_name = scope_name;
    }

    PyObject* pyObj_collection_name = PyDict_GetItemString(op_args, "collection_name");
    if (pyObj_collection_name != nullptr) {
        auto collection_name = std::string(PyUnicode_AsUTF8(pyObj_collection_name));
        req.collection_name = collection_name;
    }

    PyObject* pyObj_index_name = PyDict_GetItemString(op_args, "index_name");
    if (pyObj_index_name != nullptr) {
        auto index_name = std::string(PyUnicode_AsUTF8(pyObj_index_name));
        req.index_name = index_name;
    }

    PyObject* pyObj_is_primary = PyDict_GetItemString(op_args, "is_primary");
    if (pyObj_is_primary != nullptr) {
        if (pyObj_is_primary == Py_True) {
            req.is_primary = true;
        } else {
            req.is_primary = false;
        }
    }

    PyObject* pyObj_ignore_if_exists = PyDict_GetItemString(op_args, "ignore_if_exists");
    if (pyObj_ignore_if_exists != nullptr) {
        if (pyObj_ignore_if_exists == Py_True) {
            req.ignore_if_exists = true;
        } else {
            req.ignore_if_exists = false;
        }
    }

    PyObject* pyObj_deferred = PyDict_GetItemString(op_args, "deferred");
    if (pyObj_deferred != nullptr) {
        if (pyObj_deferred == Py_True) {
            req.deferred = true;
        } else {
            req.deferred = false;
        }
    }

    PyObject* pyObj_condition = PyDict_GetItemString(op_args, "condition");
    if (pyObj_condition != nullptr) {
        auto condition = std::string(PyUnicode_AsUTF8(pyObj_condition));
        req.condition = condition;
    }

    PyObject* pyObj_num_replicas = PyDict_GetItemString(op_args, "num_replicas");
    if (pyObj_num_replicas != nullptr) {
        auto num_replicas = static_cast<int>(PyLong_AsLong(pyObj_num_replicas));
        req.num_replicas = num_replicas;
    }

    PyObject* pyObj_fields = PyDict_GetItemString(op_args, "fields");
    if (pyObj_fields != nullptr) {
        size_t nfields = static_cast<size_t>(PyList_GET_SIZE(pyObj_fields));
        std::vector<std::string> fields{};
        size_t ii;
        for (ii = 0; ii < nfields; ++ii) {
            PyObject* pyObj_field = PyList_GetItem(pyObj_fields, ii);
            auto field = std::string(PyUnicode_AsUTF8(pyObj_field));
            fields.push_back(field);
        }

        req.fields = fields;
    }

    return req;
}

couchbase::core::operations::management::query_index_drop_request
get_drop_query_index_req(PyObject* op_args)
{
    couchbase::core::operations::management::query_index_drop_request req{};

    PyObject* pyObj_scope_name = PyDict_GetItemString(op_args, "scope_name");
    if (pyObj_scope_name != nullptr) {
        auto scope_name = std::string(PyUnicode_AsUTF8(pyObj_scope_name));
        req.scope_name = scope_name;
    }

    PyObject* pyObj_collection_name = PyDict_GetItemString(op_args, "collection_name");
    if (pyObj_collection_name != nullptr) {
        auto collection_name = std::string(PyUnicode_AsUTF8(pyObj_collection_name));
        req.collection_name = collection_name;
    }

    PyObject* pyObj_index_name = PyDict_GetItemString(op_args, "index_name");
    if (pyObj_index_name != nullptr) {
        auto index_name = std::string(PyUnicode_AsUTF8(pyObj_index_name));
        req.index_name = index_name;
    }

    PyObject* pyObj_is_primary = PyDict_GetItemString(op_args, "is_primary");
    if (pyObj_is_primary != nullptr) {
        if (pyObj_is_primary == Py_True) {
            req.is_primary = true;
        } else {
            req.is_primary = false;
        }
    }

    PyObject* pyObj_ignore_if_does_not_exist = PyDict_GetItemString(op_args, "ignore_if_does_not_exist");
    if (pyObj_ignore_if_does_not_exist != nullptr) {
        if (pyObj_ignore_if_does_not_exist == Py_True) {
            req.ignore_if_does_not_exist = true;
        } else {
            req.ignore_if_does_not_exist = false;
        }
    }

    return req;
}

template<typename Request>
PyObject*
do_query_index_mgmt_op(connection& conn,
                       Request& req,
                       PyObject* pyObj_callback,
                       PyObject* pyObj_errback,
                       std::shared_ptr<std::promise<PyObject*>> barrier)
{
    using response_type = typename Request::response_type;
    Py_BEGIN_ALLOW_THREADS conn.cluster_->execute(req, [pyObj_callback, pyObj_errback, barrier](response_type resp) {
        create_result_from_query_index_mgmt_op_response(resp, pyObj_callback, pyObj_errback, barrier);
    });
    Py_END_ALLOW_THREADS Py_RETURN_NONE;
}

PyObject*
handle_query_index_mgmt_op(connection* conn, struct query_index_mgmt_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback)
{
    PyObject* res = nullptr;
    std::shared_ptr<std::promise<PyObject*>> barrier = nullptr;
    std::future<PyObject*> fut;
    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        barrier = std::make_shared<std::promise<PyObject*>>();
        fut = barrier->get_future();
    }
    PyObject* pyObj_bucket_name = PyDict_GetItemString(options->op_args, "bucket_name");
    auto bucket_name = std::string(PyUnicode_AsUTF8(pyObj_bucket_name));
    std::string scope_name{};
    PyObject* pyObj_scope_name = PyDict_GetItemString(options->op_args, "scope_name");
    if (pyObj_scope_name != nullptr) {
        scope_name = std::string(PyUnicode_AsUTF8(pyObj_scope_name));
    }
    std::string collection_name{};
    PyObject* pyObj_collection_name = PyDict_GetItemString(options->op_args, "collection_name");
    if (pyObj_collection_name != nullptr) {
        collection_name = std::string(PyUnicode_AsUTF8(pyObj_collection_name));
    }

    switch (options->op_type) {
        case QueryIndexManagementOperations::CREATE_INDEX: {
            auto req = get_create_query_index_req(options->op_args);
            req.bucket_name = bucket_name;
            req.timeout = options->timeout_ms;
            if (!scope_name.empty()) {
                req.scope_name = scope_name;
            }
            if (!collection_name.empty()) {
                req.collection_name = collection_name;
            }

            res = do_query_index_mgmt_op<couchbase::core::operations::management::query_index_create_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case QueryIndexManagementOperations::DROP_INDEX: {
            auto req = get_drop_query_index_req(options->op_args);
            req.bucket_name = bucket_name;
            req.timeout = options->timeout_ms;
            if (!scope_name.empty()) {
                req.scope_name = scope_name;
            }
            if (!collection_name.empty()) {
                req.collection_name = collection_name;
            }

            res = do_query_index_mgmt_op<couchbase::core::operations::management::query_index_drop_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case QueryIndexManagementOperations::GET_ALL_INDEXES: {
            couchbase::core::operations::management::query_index_get_all_request req{};
            req.bucket_name = bucket_name;
            req.timeout = options->timeout_ms;
            if (!scope_name.empty()) {
                req.scope_name = scope_name;
            }
            if (!collection_name.empty()) {
                req.collection_name = collection_name;
            }

            res = do_query_index_mgmt_op<couchbase::core::operations::management::query_index_get_all_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case QueryIndexManagementOperations::BUILD_DEFERRED_INDEXES: {
            couchbase::core::operations::management::query_index_build_deferred_request req{};
            req.bucket_name = bucket_name;
            req.timeout = options->timeout_ms;
            if (!scope_name.empty()) {
                req.scope_name = scope_name;
            }
            if (!collection_name.empty()) {
                req.collection_name = collection_name;
            }
            res = do_query_index_mgmt_op<couchbase::core::operations::management::query_index_build_deferred_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        default: {
            pycbc_set_python_exception(
              PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized query index mgmt operation passed in.");
            barrier->set_value(nullptr);
            Py_XDECREF(pyObj_callback);
            Py_XDECREF(pyObj_errback);
            break;
        }
    };
    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        PyObject* ret = nullptr;
        Py_BEGIN_ALLOW_THREADS ret = fut.get();
        Py_END_ALLOW_THREADS return ret;
    }
    return res;
}

void
add_query_index_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class)
{
    PyObject* pyObj_enum_values = PyUnicode_FromString(QueryIndexManagementOperations::ALL_OPERATIONS());
    PyObject* pyObj_enum_name = PyUnicode_FromString("QueryIndexManagementOperations");
    // PyTuple_Pack returns new reference, need to Py_DECREF values provided
    PyObject* pyObj_args = PyTuple_Pack(2, pyObj_enum_name, pyObj_enum_values);
    Py_DECREF(pyObj_enum_name);
    Py_DECREF(pyObj_enum_values);

    PyObject* pyObj_kwargs = PyDict_New();
    PyObject_SetItem(pyObj_kwargs, PyUnicode_FromString("module"), PyModule_GetNameObject(pyObj_module));
    PyObject* pyObj_mgmt_operations = PyObject_Call(pyObj_enum_class, pyObj_args, pyObj_kwargs);
    Py_DECREF(pyObj_args);
    Py_DECREF(pyObj_kwargs);

    if (PyModule_AddObject(pyObj_module, "query_index_mgmt_operations", pyObj_mgmt_operations) < 0) {
        // only need to Py_DECREF on failure to add when using PyModule_AddObject()
        Py_XDECREF(pyObj_mgmt_operations);
        return;
    }
}
