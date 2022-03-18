#include "query_index_management.hxx"
#include "../exceptions.hxx"

PyObject*
build_query_index(const couchbase::operations::management::query_index_get_all_response::query_index& index)
{
    PyObject* pyObj_index = PyDict_New();

    PyObject* pyObj_tmp = PyUnicode_FromString(index.id.c_str());
    if (-1 == PyDict_SetItemString(pyObj_index, "id", pyObj_tmp)) {
        Py_XDECREF(pyObj_index);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

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

    pyObj_tmp = PyUnicode_FromString(index.name.c_str());
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

    pyObj_tmp = PyUnicode_FromString(index.datastore_id.c_str());
    if (-1 == PyDict_SetItemString(pyObj_index, "datastore_id", pyObj_tmp)) {
        Py_DECREF(pyObj_index);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(index.keyspace_id.c_str());
    if (-1 == PyDict_SetItemString(pyObj_index, "keyspace_id", pyObj_tmp)) {
        Py_DECREF(pyObj_index);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(index.namespace_id.c_str());
    if (-1 == PyDict_SetItemString(pyObj_index, "namespace_id", pyObj_tmp)) {
        Py_DECREF(pyObj_index);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(index.collection_name.c_str());
    if (-1 == PyDict_SetItemString(pyObj_index, "collection_name", pyObj_tmp)) {
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

    if (index.bucket_id.has_value()) {
        pyObj_tmp = PyUnicode_FromString(index.bucket_id.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_index, "bucket_id", pyObj_tmp)) {
            Py_DECREF(pyObj_index);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (index.scope_id.has_value()) {
        pyObj_tmp = PyUnicode_FromString(index.scope_id.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_index, "scope_id", pyObj_tmp)) {
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
create_result_from_query_index_mgmt_response<couchbase::operations::management::query_index_get_all_response>(
  const couchbase::operations::management::query_index_get_all_response& resp)
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

// template<typename T>
// void
// create_result_from_query_index_mgmt_op_response(const T& resp, PyObject* pyObj_callback, PyObject* pyObj_errback,
// std::shared_ptr<std::promise<PyObject*>> barrier)
// {
//     PyGILState_STATE state = PyGILState_Ensure();
//     PyObject* pyObj_args = NULL;
//     PyObject* pyObj_func = NULL;

//     if (resp.ctx.ec.value()) {
//         PyObject* pyObj_exc = build_exception(resp.ctx);
//         pyObj_func = ctx.get_errback();
//         pyObj_args = PyTuple_New(1);
//         PyTuple_SET_ITEM(pyObj_args, 0, pyObj_exc);

//     } else {
//         auto res = create_result_from_query_index_mgmt_response(resp);
//         // TODO:  check if PyErr_Occurred() != nullptr and raise error accordingly
//         pyObj_func = ctx.get_callback();
//         pyObj_args = PyTuple_New(1);
//         PyTuple_SET_ITEM(pyObj_args, 0, reinterpret_cast<PyObject*>(res));
//     }

//     PyObject* r = PyObject_CallObject(const_cast<PyObject*>(pyObj_func), pyObj_args);
//     if (r) {
//         Py_XDECREF(r);
//     } else {
//         PyErr_Print();
//     }

//     Py_XDECREF(pyObj_args);
//     // Py_XDECREF(pyObj_func);
//     ctx.decrement_PyObjects();
//     PyGILState_Release(state);
// }

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
        if (pyObj_errback == nullptr) {
            // make sure this is an HTTPException
            auto pycbc_ex =
              PycbcHttpException("Error doing query index mgmt operation.", __FILE__, __LINE__, resp.ctx, PycbcError::HTTPError);
            auto exc = std::make_exception_ptr(pycbc_ex);
            barrier->set_exception(exc);
        } else {
            pyObj_exc = build_exception_from_context(resp.ctx);
            pyObj_func = pyObj_errback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, pyObj_exc);
            pyObj_kwargs = pycbc_get_exception_kwargs("Error doing query index mgmt operation.", __FILE__, __LINE__);
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
        if (pyObj_errback == nullptr) {
            auto pycbc_ex = PycbcException("Query index mgmt operation error.", __FILE__, __LINE__, PycbcError::UnableToBuildResult);
            auto exc = std::make_exception_ptr(pycbc_ex);
            barrier->set_exception(exc);
        } else {
            pyObj_func = pyObj_errback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, Py_None);
            pyObj_kwargs =
              pycbc_core_get_exception_kwargs("Query index mgmt operation error.", PycbcError::UnableToBuildResult, __FILE__, __LINE__);
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
        Py_XDECREF(pyObj_exc);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }
    PyGILState_Release(state);
}

couchbase::operations::management::query_index_create_request
get_create_query_index_req(PyObject* op_args)
{
    couchbase::operations::management::query_index_create_request req{};

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

couchbase::operations::management::query_index_drop_request
get_drop_query_index_req(PyObject* op_args)
{
    couchbase::operations::management::query_index_drop_request req{};

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
handle_query_mgmt_blocking_result(std::future<PyObject*>&& fut)
{
    PyObject* ret = nullptr;
    bool http_ex = false;
    std::string file;
    int line;
    couchbase::error_context::http ctx{};
    std::error_code ec;
    std::string msg;

    Py_BEGIN_ALLOW_THREADS
    try {
        ret = fut.get();
    } catch (PycbcHttpException e) {
        http_ex = true;
        msg = e.what();
        file = e.get_file();
        line = e.get_line();
        ec = e.get_error_code();
        ctx = e.get_context();
    } catch (PycbcException e) {
        msg = e.what();
        file = e.get_file();
        line = e.get_line();
        ec = e.get_error_code();
    } catch (const std::exception& e) {
        ec = PycbcError::InternalSDKError;
        msg = e.what();
    }
    Py_END_ALLOW_THREADS

      std::string ec_category = std::string(ec.category().name());
    if (http_ex) {
        PyObject* pyObj_base_exc = build_exception_from_context(ctx);
        pycbc_set_python_exception(msg.c_str(), ec, file.c_str(), line, pyObj_base_exc);
        Py_DECREF(pyObj_base_exc);
    } else if (!file.empty()) {
        pycbc_set_python_exception(msg.c_str(), ec, file.c_str(), line);
    } else if (ec_category.compare("pycbc") == 0) {
        pycbc_set_python_exception(msg.c_str(), ec, __FILE__, __LINE__);
    }
    return ret;
}

PyObject*
handle_query_index_mgmt_op(connection* conn, struct query_index_mgmt_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback)
{
    PyObject* res = nullptr;
    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();
    PyObject* pyObj_bucket_name = PyDict_GetItemString(options->op_args, "bucket_name");
    auto bucket_name = std::string(PyUnicode_AsUTF8(pyObj_bucket_name));
    switch (options->op_type) {
        case QueryIndexManagementOperations::CREATE_INDEX: {
            auto req = get_create_query_index_req(options->op_args);
            req.bucket_name = bucket_name;
            req.timeout = options->timeout_ms;

            res = do_query_index_mgmt_op<couchbase::operations::management::query_index_create_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case QueryIndexManagementOperations::DROP_INDEX: {
            auto req = get_drop_query_index_req(options->op_args);
            req.bucket_name = bucket_name;
            req.timeout = options->timeout_ms;

            res = do_query_index_mgmt_op<couchbase::operations::management::query_index_drop_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case QueryIndexManagementOperations::GET_ALL_INDEXES: {
            couchbase::operations::management::query_index_get_all_request req{};
            req.bucket_name = bucket_name;
            req.timeout = options->timeout_ms;

            res = do_query_index_mgmt_op<couchbase::operations::management::query_index_get_all_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case QueryIndexManagementOperations::BUILD_DEFERRED_INDEXES: {
            couchbase::operations::management::query_index_build_deferred_request req{};
            req.bucket_name = bucket_name;
            req.timeout = options->timeout_ms;

            res = do_query_index_mgmt_op<couchbase::operations::management::query_index_build_deferred_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        default: {
            pycbc_set_python_exception(
              "Unrecognized query index mgmt operation passed in.", PycbcError::InvalidArgument, __FILE__, __LINE__);
            Py_XDECREF(pyObj_callback);
            Py_XDECREF(pyObj_errback);
            return nullptr;
        }
    };
    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        // can only be a single future (if not doing std::shared),
        // so use move semantics
        return handle_query_mgmt_blocking_result(std::move(f));
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
