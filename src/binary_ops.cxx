#include "binary_ops.hxx"
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
add_extras_to_result<couchbase::operations::increment_response>(const couchbase::operations::increment_response& resp, result* res)
{
    PyObject* pyObj_tmp = PyLong_FromUnsignedLongLong(resp.content);
    if (-1 == PyDict_SetItemString(res->dict, "content", pyObj_tmp)) {
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);
    return res;
}

template<>
result*
add_extras_to_result<couchbase::operations::decrement_response>(const couchbase::operations::decrement_response& resp, result* res)
{
    PyObject* pyObj_tmp = PyLong_FromUnsignedLongLong(resp.content);
    if (-1 == PyDict_SetItemString(res->dict, "content", pyObj_tmp)) {
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);
    return res;
}

template<typename T>
result*
create_base_result_from_binary_op_response(const char* key, const T& resp)
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

    PyObject* pyObj_mutation_token = create_mutation_token_obj(resp.token);
    if (-1 == PyDict_SetItemString(res->dict, RESULT_MUTATION_TOKEN, pyObj_mutation_token)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_mutation_token);
        return nullptr;
    }
    Py_DECREF(pyObj_mutation_token);
    return res;
}

template<typename T>
void
create_result_from_binary_op_response(const char* key,
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
        if (pyObj_errback == nullptr) {
            auto pycbc_ex = PycbcKeyValueException("Binary operation error.", __FILE__, __LINE__, resp.ctx);
            auto exc = std::make_exception_ptr(pycbc_ex);
            barrier->set_exception(exc);
        } else {
            pyObj_exc = build_exception_from_context(resp.ctx);
            pyObj_func = pyObj_errback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, pyObj_exc);
        }
        // lets clear any errors
        PyErr_Clear();
    } else {
        auto res = create_base_result_from_binary_op_response(key, resp);
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
        if (pyObj_errback == nullptr) {
            auto pycbc_ex = PycbcException("Binary operation error.", __FILE__, __LINE__, PycbcError::UnableToBuildResult);
            auto exc = std::make_exception_ptr(pycbc_ex);
            barrier->set_exception(exc);
        } else {
            pyObj_func = pyObj_errback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, Py_None);
            pyObj_kwargs = pycbc_core_get_exception_kwargs("Binary operation error.", PycbcError::UnableToBuildResult, __FILE__, __LINE__);
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

template<typename Request>
void
do_binary_op(connection& conn,
             Request& req,
             PyObject* pyObj_callback,
             PyObject* pyObj_errback,
             std::shared_ptr<std::promise<PyObject*>> barrier)
{
    using response_type = typename Request::response_type;
    Py_BEGIN_ALLOW_THREADS conn.cluster_->execute(req, [key = req.id.key(), pyObj_callback, pyObj_errback, barrier](response_type resp) {
        create_result_from_binary_op_response(key.c_str(), resp, pyObj_callback, pyObj_errback, barrier);
    });
    Py_END_ALLOW_THREADS
}

PyObject*
prepare_and_execute_counter_op(struct counter_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback)
{
    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();

    if (options->op_type == Operations::INCREMENT) {
        couchbase::operations::increment_request req{ options->id };
        req.delta = options->delta;
        req.initial_value = options->initial_value;
        req.timeout = options->timeout_ms;
        req.durability_level = options->durability;
        if (0 < options->expiry) {
            req.expiry = options->expiry;
        }
        do_binary_op<couchbase::operations::increment_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier);
    } else {
        couchbase::operations::decrement_request req{ options->id };
        req.delta = options->delta;
        req.initial_value = options->initial_value;
        req.timeout = options->timeout_ms;
        req.durability_level = options->durability;
        if (0 < options->expiry) {
            req.expiry = options->expiry;
        }
        do_binary_op<couchbase::operations::decrement_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier);
    }
    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        return handle_binary_blocking_result(std::move(f));
    }
    Py_RETURN_NONE;
}

PyObject*
prepare_and_execute_binary_mutation_op(struct binary_mutation_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback)
{
    couchbase::protocol::cas cas = couchbase::protocol::cas{ 0 };
    if (options->cas != 0) {
        cas = couchbase::protocol::cas{ options->cas };
    }

    if (!PyBytes_Check(options->pyObj_value)) {
        pycbc_set_python_exception("Value should be bytes object.", PycbcError::InvalidArgument, __FILE__, __LINE__);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
        return nullptr;
    }

    PyObject* pyObj_unicode = PyUnicode_FromEncodedObject(options->pyObj_value, "utf-8", "strict");
    if (!pyObj_unicode) {
        pycbc_set_python_exception("Unable to encode value.", PycbcError::InvalidArgument, __FILE__, __LINE__);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
        return nullptr;
    }
    std::string value = std::string(PyUnicode_AsUTF8(pyObj_unicode));
    Py_XDECREF(pyObj_unicode);

    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();

    if (options->op_type == Operations::APPEND) {
        couchbase::operations::append_request req{ options->id };
        // @TODO(): C++ client doesn't handle cas
        // req.cas = cas;
        req.timeout = options->timeout_ms;
        req.durability_level = options->durability;
        req.value = value;
        do_binary_op<couchbase::operations::append_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier);

    } else {

        couchbase::operations::prepend_request req{ options->id };
        // @TODO(): C++ client doesn't handle cas
        // req.cas = cas;
        req.timeout = options->timeout_ms;
        req.durability_level = options->durability;
        req.value = value;
        do_binary_op<couchbase::operations::prepend_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier);
    }
    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        return handle_binary_blocking_result(std::move(f));
    }
    Py_RETURN_NONE;
}

PyObject*
handle_binary_op([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
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
    PyObject* pyObj_durability = nullptr;

    // optional
    uint64_t delta = 0;
    uint64_t initial_value = 0;
    uint32_t expiry = 0;
    uint64_t timeout = 0;
    uint64_t cas = 0;

    static const char* kw_list[] = { "conn", "bucket",     "scope",  "collection_name", "key", "op_type", "callback", "errback", "value",
                                     "span", "durability", "expiry", "timeout",         "cas", "delta",   "initial",  nullptr };

    const char* kw_format = "O!ssssI|OOOOOILLLL";
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
                                          &pyObj_callback,
                                          &pyObj_errback,
                                          &pyObj_value,
                                          &pyObj_span,
                                          &pyObj_durability,
                                          &expiry,
                                          &timeout,
                                          &cas,
                                          &delta,
                                          &initial_value);

    if (!ret) {
        pycbc_set_python_exception(
          "Cannot perform binary operation.  Unable to parse args/kwargs.", PycbcError::InvalidArgument, __FILE__, __LINE__);
        return nullptr;
    }

    connection* conn = nullptr;
    conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    if (nullptr == conn) {
        pycbc_set_python_exception(NULL_CONN_OBJECT, PycbcError::InvalidArgument, __FILE__, __LINE__);
        return nullptr;
    }
    // PyErr_Clear();

    couchbase::document_id id{ bucket, scope, collection, key };

    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::key_value_timeout;
    if (0 < timeout) {
        timeout_ms = std::chrono::milliseconds(std::max(0ULL, timeout / 1000ULL));
    }

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

    // @TODO(): C++ client doesn't handle observable durability
    couchbase::protocol::durability_level durability_level = couchbase::protocol::durability_level::none;
    if (durability != 0) {
        durability_level = static_cast<couchbase::protocol::durability_level>(durability);
    }

    // PyObjects that need to be around for the cxx client lambda
    // have their increment/decrement handled w/in the callback_context struct
    // struct callback_context callback_ctx = { pyObj_callback, pyObj_errback };
    Py_XINCREF(pyObj_callback);
    Py_XINCREF(pyObj_errback);

    PyObject* pyObj_result = nullptr;
    switch (op_type) {
        case Operations::APPEND:
        case Operations::PREPEND: {

            struct binary_mutation_options opts = { conn,         id,         op_type,          pyObj_value, timeout_ms,
                                                    replicate_to, persist_to, durability_level, cas,         pyObj_span };
            pyObj_result = prepare_and_execute_binary_mutation_op(&opts, pyObj_callback, pyObj_errback);
            break;
        }
        case Operations::INCREMENT:
        case Operations::DECREMENT: {
            struct counter_options opts = { conn,         id,         op_type,          delta,         timeout_ms, expiry,
                                            replicate_to, persist_to, durability_level, initial_value, pyObj_span };
            pyObj_result = prepare_and_execute_counter_op(&opts, pyObj_callback, pyObj_errback);
            break;
        }
        default: {
            pycbc_set_python_exception("Unrecognized binary operation passed in.", PycbcError::InvalidArgument, __FILE__, __LINE__);
            Py_XDECREF(pyObj_callback);
            Py_XDECREF(pyObj_errback);
            return nullptr;
        }
    };

    return pyObj_result;
}

PyObject*
handle_binary_blocking_result(std::future<PyObject*>&& fut)
{
    PyObject* ret = nullptr;
    bool kv_ex = false;
    std::string file;
    int line;
    couchbase::error_context::key_value ctx{};
    std::error_code ec;
    std::string msg;

    Py_BEGIN_ALLOW_THREADS
    try {
        ret = fut.get();
    } catch (PycbcKeyValueException e) {
        kv_ex = true;
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
    if (kv_ex) {
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
