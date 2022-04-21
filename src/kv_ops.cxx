#include "kv_ops.hxx"
#include "exceptions.hxx"
#include "result.hxx"

template<typename T>
result*
add_extras_to_result([[maybe_unused]] const T& t, result* res)
{
    return res;
}

template<>
result*
add_extras_to_result<couchbase::operations::get_projected_response>(const couchbase::operations::get_projected_response& resp, result* res)
{
    if (resp.expiry) {
        PyObject* pyObj_tmp = PyLong_FromUnsignedLong(resp.expiry.value());
        if (-1 == PyDict_SetItemString(res->dict, RESULT_EXPIRY, pyObj_tmp)) {
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }
    return res;
}

template<>
result*
add_extras_to_result<couchbase::operations::exists_response>(const couchbase::operations::exists_response& resp, result* res)
{
    PyObject* pyObj_tmp = PyBool_FromLong(static_cast<long>(resp.exists()));
    if (-1 == PyDict_SetItemString(res->dict, RESULT_EXISTS, pyObj_tmp)) {
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);
    return res;
}

template<typename T>
result*
create_base_result_from_get_operation_response(const char* key, const T& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);
    res->ec = resp.ctx.ec;

    PyObject* pyObj_tmp = PyLong_FromUnsignedLongLong(resp.cas.value);
    if (-1 == PyDict_SetItemString(res->dict, RESULT_CAS, pyObj_tmp)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLong(resp.flags);
    if (-1 == PyDict_SetItemString(res->dict, RESULT_FLAGS, pyObj_tmp)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_XDECREF(pyObj_tmp);

    if (nullptr != key) {
        pyObj_tmp = PyUnicode_FromString(key);
        if (-1 == PyDict_SetItemString(res->dict, RESULT_KEY, pyObj_tmp)) {
            Py_XDECREF(pyObj_result);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (!res->ec) {
        pyObj_tmp = PyBytes_FromStringAndSize(resp.value.c_str(), resp.value.length());
        if (-1 == PyDict_SetItemString(res->dict, RESULT_VALUE, pyObj_tmp)) {
            Py_XDECREF(pyObj_result);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }
    return res;
}

template<>
result*
create_base_result_from_get_operation_response<couchbase::operations::exists_response>(const char* key,
                                                                                       const couchbase::operations::exists_response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);
    res->ec = resp.ctx.ec;

    PyObject* pyObj_tmp = PyLong_FromUnsignedLongLong(resp.cas.value);
    if (-1 == PyDict_SetItemString(res->dict, RESULT_CAS, pyObj_tmp)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (nullptr != key) {
        pyObj_tmp = PyUnicode_FromString(key);
        if (-1 == PyDict_SetItemString(res->dict, RESULT_KEY, pyObj_tmp)) {
            Py_XDECREF(pyObj_result);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }
    return res;
}

template<>
result*
create_base_result_from_get_operation_response<couchbase::operations::touch_response>(const char* key,
                                                                                      const couchbase::operations::touch_response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);
    res->ec = resp.ctx.ec;

    PyObject* pyObj_tmp = PyLong_FromUnsignedLongLong(resp.cas.value);
    if (-1 == PyDict_SetItemString(res->dict, RESULT_CAS, pyObj_tmp)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (nullptr != key) {
        pyObj_tmp = PyUnicode_FromString(key);
        if (-1 == PyDict_SetItemString(res->dict, RESULT_KEY, pyObj_tmp)) {
            Py_XDECREF(pyObj_result);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }
    return res;
}

template<>
result*
create_base_result_from_get_operation_response<couchbase::operations::unlock_response>(const char* key,
                                                                                       const couchbase::operations::unlock_response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);
    res->ec = resp.ctx.ec;

    PyObject* pyObj_tmp = PyLong_FromUnsignedLongLong(resp.cas.value);
    if (-1 == PyDict_SetItemString(res->dict, RESULT_CAS, pyObj_tmp)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (nullptr != key) {
        pyObj_tmp = PyUnicode_FromString(key);
        if (-1 == PyDict_SetItemString(res->dict, RESULT_KEY, pyObj_tmp)) {
            Py_XDECREF(pyObj_result);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    return res;
}

template<typename T>
void
create_result_from_get_operation_response(const char* key,
                                          const T& resp,
                                          PyObject* pyObj_callback,
                                          PyObject* pyObj_errback,
                                          std::shared_ptr<std::promise<PyObject*>> barrier)
{
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* pyObj_args = NULL;
    PyObject* pyObj_kwargs = nullptr;
    PyObject* pyObj_exc = nullptr;
    PyObject* pyObj_func = nullptr;
    PyObject* pyObj_callback_res = nullptr;
    auto set_exception = false;

    if (resp.ctx.ec.value()) {
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "KV read operation error.");
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
        auto res = create_base_result_from_get_operation_response(key, resp);
        if (res != nullptr) {
            res = add_extras_to_result(resp, res);
        }

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
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "KV read operation error.");
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
        Py_XDECREF(pyObj_exc);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }
    PyGILState_Release(state);
}

// Until CXXCBC-82 is resolved
template<>
void
create_result_from_get_operation_response<couchbase::operations::exists_response>(const char* key,
                                                                                  const couchbase::operations::exists_response& resp,
                                                                                  PyObject* pyObj_callback,
                                                                                  PyObject* pyObj_errback,
                                                                                  std::shared_ptr<std::promise<PyObject*>> barrier)
{
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* pyObj_args = NULL;
    PyObject* pyObj_kwargs = nullptr;
    PyObject* pyObj_exc = nullptr;
    PyObject* pyObj_func = nullptr;
    PyObject* pyObj_callback_res = nullptr;
    auto set_exception = false;

    if (resp.ctx.ec.value() && resp.ctx.ec.value() != 101) {
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "KV read operation error.");
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
        auto res = create_base_result_from_get_operation_response(key, resp);
        if (res != nullptr) {
            res = add_extras_to_result(resp, res);
        }

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
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "KV read operation error.");
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
            pycbc_set_python_exception(PycbcError::InternalSDKError, __FILE__, __LINE__, "KV read operation callback failed.");
        }
        Py_DECREF(pyObj_args);
        Py_XDECREF(pyObj_exc);
        Py_XDECREF(pyObj_kwargs);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }
    PyGILState_Release(state);
}

template<typename Request>
void
do_get(connection& conn, Request& req, PyObject* pyObj_callback, PyObject* pyObj_errback, std::shared_ptr<std::promise<PyObject*>> barrier)
{
    using response_type = typename Request::response_type;
    Py_BEGIN_ALLOW_THREADS conn.cluster_->execute(req, [key = req.id.key(), pyObj_callback, pyObj_errback, barrier](response_type resp) {
        create_result_from_get_operation_response(key.c_str(), resp, pyObj_callback, pyObj_errback, barrier);
    });
    Py_END_ALLOW_THREADS
}

PyObject*
prepare_and_execute_read_op(struct read_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback)
{
    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();

    switch (options->op_type) {
        case Operations::GET: {
            couchbase::operations::get_request req{ options->id };
            req.timeout = options->timeout_ms;
            do_get<couchbase::operations::get_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case Operations::GET_PROJECTED: {
            std::vector<std::string> projections;
            if (nullptr != options->project) {
                // this needs to be a list...
                bool ok = !!PyList_Check(options->project);
                if (ok) {
                    // and the list needs to all be strings
                    for (Py_ssize_t i = 0; ok && i < PyList_Size(options->project); i++) {
                        PyObject* pyObj_projection = PyList_GetItem(options->project, i);
                        // PyList_GetItem returns borrowed ref, inc while using, decr after done
                        Py_INCREF(pyObj_projection);
                        ok = !!PyUnicode_Check(pyObj_projection);
                        // lets build the projections vector we will use in the request, since
                        // we are iterating...
                        projections.emplace_back(PyUnicode_AsUTF8(pyObj_projection));
                        Py_DECREF(pyObj_projection);
                        pyObj_projection = nullptr;
                    }
                }
                if (!ok) {
                    pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Project must be a list of strings.");
                    barrier->set_value(nullptr);
                    Py_XDECREF(pyObj_callback);
                    Py_XDECREF(pyObj_errback);
                    return nullptr;
                }
            }

            couchbase::operations::get_projected_request req{ options->id };
            req.timeout = options->timeout_ms;
            req.with_expiry = !!options->with_expiry;
            req.projections = projections;
            do_get<couchbase::operations::get_projected_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case Operations::GET_AND_TOUCH: {
            couchbase::operations::get_and_touch_request req{ options->id };
            req.expiry = options->expiry;
            req.timeout = options->timeout_ms;
            do_get<couchbase::operations::get_and_touch_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case Operations::GET_AND_LOCK: {
            couchbase::operations::get_and_lock_request req{ options->id };
            req.lock_time = options->lock_time;
            req.timeout = options->timeout_ms;
            do_get<couchbase::operations::get_and_lock_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case Operations::EXISTS: {
            couchbase::operations::exists_request req{ options->id };
            req.timeout = options->timeout_ms;
            do_get<couchbase::operations::exists_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case Operations::TOUCH: {
            couchbase::operations::touch_request req{ options->id };
            req.expiry = options->expiry;
            req.timeout = options->timeout_ms;
            do_get<couchbase::operations::touch_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case Operations::UNLOCK: {
            couchbase::operations::unlock_request req{ options->id };
            req.cas = options->cas;
            req.timeout = options->timeout_ms;
            do_get<couchbase::operations::unlock_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        default: {
            pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized get operation passed in.");
            barrier->set_value(nullptr);
            Py_XDECREF(pyObj_callback);
            Py_XDECREF(pyObj_errback);
            return nullptr;
        }
    };
    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        PyObject* ret = nullptr;
        Py_BEGIN_ALLOW_THREADS ret = f.get();
        Py_END_ALLOW_THREADS return ret;
    }
    Py_RETURN_NONE;
}

template<typename T>
result*
create_base_result_from_mutation_operation_response(const char* key, const T& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);
    res->ec = resp.ctx.ec;
    PyObject* pyObj_tmp = PyLong_FromUnsignedLongLong(resp.cas.value);
    if (-1 == PyDict_SetItemString(res->dict, RESULT_CAS, pyObj_tmp)) {
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (nullptr != key) {
        pyObj_tmp = PyUnicode_FromString(key);
        if (-1 == PyDict_SetItemString(res->dict, RESULT_KEY, pyObj_tmp)) {
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    PyObject* pyObj_mutation_token = create_mutation_token_obj(resp.token);
    if (-1 == PyDict_SetItemString(res->dict, RESULT_MUTATION_TOKEN, pyObj_mutation_token)) {
        Py_XDECREF(pyObj_mutation_token);
        return nullptr;
    }
    Py_DECREF(pyObj_mutation_token);

    return res;
}

template<typename T>
void
create_result_from_mutation_operation_response(const char* key,
                                               const T& resp,
                                               PyObject* pyObj_callback,
                                               PyObject* pyObj_errback,
                                               std::shared_ptr<std::promise<PyObject*>> barrier)
{
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* pyObj_args = nullptr;
    PyObject* pyObj_kwargs = nullptr;
    PyObject* pyObj_exc = nullptr;
    PyObject* pyObj_func = nullptr;
    PyObject* pyObj_callback_res = nullptr;
    auto set_exception = false;

    if (resp.ctx.ec.value()) {
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "KV mutation operation error.");
        if (pyObj_errback == nullptr) {
            barrier->set_value(pyObj_exc);
        } else {
            pyObj_func = pyObj_errback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, pyObj_exc);
        }
    } else {
        auto res = create_base_result_from_mutation_operation_response(key, resp);
        if (res != nullptr) {
            res = add_extras_to_result(resp, res);
        }

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
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "KV mutation operation error.");
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
            pycbc_set_python_exception(PycbcError::InternalSDKError, __FILE__, __LINE__, "Mutation operation callback failed.");
        }
        Py_DECREF(pyObj_args);
        Py_XDECREF(pyObj_exc);
        Py_XDECREF(pyObj_kwargs);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }
    // LOG_INFO("{}: create mutation callback completed", "PYCBC");
    PyGILState_Release(state);
}

template<typename Request>
void
do_mutation(connection& conn,
            Request& req,
            PyObject* pyObj_callback,
            PyObject* pyObj_errback,
            std::shared_ptr<std::promise<PyObject*>> barrier)
{
    using response_type = typename Request::response_type;
    Py_BEGIN_ALLOW_THREADS conn.cluster_->execute(req, [key = req.id.key(), pyObj_callback, pyObj_errback, barrier](response_type resp) {
        create_result_from_mutation_operation_response(key.c_str(), resp, pyObj_callback, pyObj_errback, barrier);
    });
    Py_END_ALLOW_THREADS
}

PyObject*
prepare_and_execute_mutation_op(struct mutation_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback)
{
    // auto value = !options->value ? std::string() : json_encode(options->value);
    // auto encoded =
    //   !options->value ? std::tuple<std::string, uint32_t>{ std::string(), 0 } : encode_value(pyObj_transcoder, options->value);

    // **DO NOT DECREF** these -- things from tuples are borrowed references!!
    PyObject* pyObj_value = nullptr;
    PyObject* pyObj_flags = nullptr;
    std::string value = std::string();

    if (options->value) {
        pyObj_value = PyTuple_GET_ITEM(options->value, 0);
        pyObj_flags = PyTuple_GET_ITEM(options->value, 1);

        if (PyUnicode_Check(pyObj_value)) {
            value = std::string(PyUnicode_AsUTF8(pyObj_value));
        } else {
            PyObject* pyObj_unicode = PyUnicode_FromEncodedObject(pyObj_value, "utf-8", "strict");
            value = std::string(PyUnicode_AsUTF8(pyObj_unicode));
            Py_DECREF(pyObj_unicode);
        }
    }

    couchbase::protocol::durability_level durability_level = couchbase::protocol::durability_level::none;
    if (options->durability != 0) {
        durability_level = static_cast<couchbase::protocol::durability_level>(options->durability);
    }

    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();

    switch (options->op_type) {
        case Operations::INSERT: {
            couchbase::operations::insert_request req{ options->id, value };
            req.flags = static_cast<uint32_t>(PyLong_AsLong(pyObj_flags));
            req.timeout = options->timeout_ms;
            req.expiry = options->expiry;
            req.durability_level = durability_level;
            do_mutation<couchbase::operations::insert_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case Operations::UPSERT: {
            couchbase::operations::upsert_request req{ options->id, value };
            req.flags = static_cast<uint32_t>(PyLong_AsLong(pyObj_flags));
            req.timeout = options->timeout_ms;
            if (0 < options->expiry) {
                req.expiry = options->expiry;
            }
            req.durability_level = durability_level;
            if (options->preserve_expiry) {
                req.preserve_expiry = options->preserve_expiry;
            }
            do_mutation<couchbase::operations::upsert_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case Operations::REPLACE: {
            couchbase::operations::replace_request req{ options->id, value };
            req.flags = static_cast<uint32_t>(PyLong_AsLong(pyObj_flags));
            req.timeout = options->timeout_ms;
            if (0 < options->expiry) {
                req.expiry = options->expiry;
            }
            req.cas = options->cas;
            req.durability_level = durability_level;
            if (options->preserve_expiry) {
                req.preserve_expiry = options->preserve_expiry;
            }
            do_mutation<couchbase::operations::replace_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case Operations::REMOVE: {
            couchbase::operations::remove_request req{ options->id };
            req.timeout = options->timeout_ms;
            req.cas = options->cas;
            req.durability_level = durability_level;
            do_mutation<couchbase::operations::remove_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        default: {
            pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized mutation operation passed in.");
            barrier->set_value(nullptr);
            Py_XDECREF(pyObj_callback);
            Py_XDECREF(pyObj_errback);
            return nullptr;
        }
    }
    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        PyObject* ret = nullptr;
        Py_BEGIN_ALLOW_THREADS ret = f.get();
        Py_END_ALLOW_THREADS return ret;
    }
    Py_RETURN_NONE;
}

PyObject*
handle_kv_op([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    // need these for all operations
    PyObject* pyObj_conn = nullptr;
    char* bucket = nullptr;
    char* scope = nullptr;
    char* collection = nullptr;
    char* key = nullptr;
    Operations::OperationType op_type = Operations::UNKNOWN;
    PyObject* pyObj_callback = nullptr;
    PyObject* pyObj_errback = nullptr;

    // sometimes req, sometimes optional
    PyObject* pyObj_value = nullptr;
    PyObject* pyObj_span = nullptr;
    PyObject* pyObj_project = nullptr;
    PyObject* pyObj_durability = nullptr;

    // optional
    // uint8_t durability = 0;
    uint32_t expiry = 0;
    uint32_t lock_time;
    uint64_t timeout = 0;
    uint64_t cas_int = 0;
    // booleans, but use int to read from kwargs
    int with_expiry = 0;
    int preserve_expiry = 0;

    static const char* kw_list[] = { "conn",    "bucket",  "scope",       "collection_name", "key",        "op_type", "value",
                                     "span",    "project", "callback",    "errback",         "durability", "expiry",  "lock_time",
                                     "timeout", "cas",     "with_expiry", "preserve_expiry", nullptr };

    const char* kw_format = "O!ssssI|OOOOOOIILLii";
    int ret = PyArg_ParseTupleAndKeywords(args,
                                          kwargs,
                                          kw_format,
                                          const_cast<char**>(kw_list),
                                          &PyCapsule_Type,
                                          &pyObj_conn,
                                          &bucket,
                                          &scope,
                                          &collection,
                                          &key,
                                          &op_type,
                                          &pyObj_value,
                                          &pyObj_span,
                                          &pyObj_project,
                                          &pyObj_callback,
                                          &pyObj_errback,
                                          &pyObj_durability,
                                          &expiry,
                                          &lock_time,
                                          &timeout,
                                          &cas_int,
                                          &with_expiry,
                                          &preserve_expiry);
    if (!ret) {
        pycbc_set_python_exception(
          PycbcError::InvalidArgument, __FILE__, __LINE__, "Cannot perform kv operation.  Unable to parse args/kwargs.");
        return nullptr;
    }

    PyObject* pyObj_result = nullptr;
    connection* conn = nullptr;
    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::key_value_timeout;

    conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    if (nullptr == conn) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, NULL_CONN_OBJECT);
        return nullptr;
    }
    // PyErr_Clear();
    couchbase::document_id id{ bucket, scope, collection, key };
    if (0 < timeout) {
        timeout_ms = std::chrono::milliseconds(std::max(0ULL, timeout / 1000ULL));
    }

    couchbase::cas cas = couchbase::cas{ 0 };
    if (cas_int != 0) {
        cas = couchbase::cas{ cas_int };
    }

    // PyObjects that need to be around for the cxx client lambda
    // have their increment/decrement handled w/in the callback_context struct
    // struct callback_context callback_ctx = { pyObj_callback, pyObj_errback, pyObj_transcoder };
    Py_XINCREF(pyObj_callback);
    Py_XINCREF(pyObj_errback);

    switch (op_type) {
        case Operations::INSERT:
        case Operations::UPSERT:
        case Operations::REPLACE:
        case Operations::REMOVE: {

            uint8_t durability = 0;
            uint8_t replicate_to = 0;
            uint8_t persist_to = 0;
            if (pyObj_durability) {
                if (PyDict_Check(pyObj_durability)) {
                    PyObject* pyObj_replicate_to = PyDict_GetItemString(pyObj_durability, "replicate_to");
                    if (pyObj_replicate_to) {
                        replicate_to = static_cast<uint8_t>(PyLong_AsLong(pyObj_replicate_to));
                    }

                    PyObject* pyObj_persist_to = PyDict_GetItemString(pyObj_durability, "persist_to");
                    if (pyObj_persist_to) {
                        persist_to = static_cast<uint8_t>(PyLong_AsLong(pyObj_persist_to));
                    }
                } else if (PyLong_Check(pyObj_durability)) {
                    durability = static_cast<uint8_t>(PyLong_AsLong(pyObj_durability));
                }
            }
            struct mutation_options opts = { conn,       id,     op_type,    pyObj_value, durability, replicate_to,
                                             persist_to, expiry, timeout_ms, pyObj_span,  cas,        preserve_expiry == 1 };
            pyObj_result = prepare_and_execute_mutation_op(&opts, pyObj_callback, pyObj_errback);
            break;
        }
        case Operations::GET:
        case Operations::GET_PROJECTED:
        case Operations::GET_AND_LOCK:
        case Operations::GET_AND_TOUCH:
        case Operations::TOUCH:
        case Operations::EXISTS:
        case Operations::UNLOCK: {
            if (pyObj_project != nullptr || with_expiry) {
                op_type = Operations::GET_PROJECTED;
            }
            struct read_options opts = {
                conn, id, op_type, timeout_ms, with_expiry == 1, expiry, lock_time, cas, pyObj_span, pyObj_project
            };
            pyObj_result = prepare_and_execute_read_op(&opts, pyObj_callback, pyObj_errback);
            break;
        }
        default: {
            pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized KV operation passed in.");
            Py_XDECREF(pyObj_callback);
            Py_XDECREF(pyObj_errback);
            return nullptr;
        }
    };

    return pyObj_result;
}
