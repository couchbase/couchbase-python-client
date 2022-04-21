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
                                      std::shared_ptr<std::promise<PyObject*>> barrier,
                                      result* multi_result = nullptr)
{
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* pyObj_args = NULL;
    PyObject* pyObj_kwargs = nullptr;
    PyObject* pyObj_exc = nullptr;
    PyObject* pyObj_func = nullptr;
    PyObject* pyObj_callback_res = nullptr;
    auto set_exception = false;

    if (resp.ctx.ec.value()) {
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "Binary operation error.");
        if (pyObj_errback == nullptr) {
            if (multi_result != nullptr) {
                Py_INCREF(Py_False);
                barrier->set_value(Py_False);
                if (-1 == PyDict_SetItemString(multi_result->dict, key, pyObj_exc)) {
                    // TODO:  not much we can do here...maybe?
                    PyErr_Print();
                    PyErr_Clear();
                }
                // won't fall into logic path where pyObj_exc is decremented later
                Py_DECREF(pyObj_exc);
            } else {
                barrier->set_value(pyObj_exc);
            }
        } else {
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
                if (multi_result != nullptr) {
                    Py_INCREF(Py_True);
                    barrier->set_value(Py_True);
                    if (-1 == PyDict_SetItemString(multi_result->dict, key, reinterpret_cast<PyObject*>(res))) {
                        // TODO:  not much we can do here...maybe?
                        PyErr_Print();
                        PyErr_Clear();
                    }
                    Py_DECREF(reinterpret_cast<PyObject*>(res));
                } else {
                    barrier->set_value(reinterpret_cast<PyObject*>(res));
                }
            } else {
                pyObj_func = pyObj_callback;
                pyObj_args = PyTuple_New(1);
                PyTuple_SET_ITEM(pyObj_args, 0, reinterpret_cast<PyObject*>(res));
            }
        }
    }

    if (set_exception) {
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "Binary operation error.");
        if (pyObj_errback == nullptr) {
            if (multi_result != nullptr) {
                Py_INCREF(Py_False);
                barrier->set_value(Py_False);
                if (-1 == PyDict_SetItemString(multi_result->dict, key, pyObj_exc)) {
                    // TODO:  not much we can do here...maybe?
                    PyErr_Print();
                    PyErr_Clear();
                }
                // won't fall into logic path where pyObj_exc is decremented later
                Py_DECREF(pyObj_exc);
            } else {
                barrier->set_value(pyObj_exc);
            }
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
void
do_binary_op(connection& conn,
             Request& req,
             PyObject* pyObj_callback,
             PyObject* pyObj_errback,
             std::shared_ptr<std::promise<PyObject*>> barrier,
             result* multi_result = nullptr)
{
    using response_type = typename Request::response_type;
    Py_BEGIN_ALLOW_THREADS conn.cluster_->execute(
      req, [key = req.id.key(), pyObj_callback, pyObj_errback, barrier, multi_result](response_type resp) {
          create_result_from_binary_op_response(key.c_str(), resp, pyObj_callback, pyObj_errback, barrier, multi_result);
      });
    Py_END_ALLOW_THREADS
}

PyObject*
prepare_and_execute_counter_op(struct counter_options* options,
                               PyObject* pyObj_callback,
                               PyObject* pyObj_errback,
                               std::shared_ptr<std::promise<PyObject*>> barrier,
                               result* multi_result = nullptr)
{
    if (options->op_type == Operations::INCREMENT) {
        couchbase::operations::increment_request req{ options->id };
        req.delta = options->delta;
        req.initial_value = options->initial_value;
        req.timeout = options->timeout_ms;
        req.durability_level = options->durability;
        if (0 < options->expiry) {
            req.expiry = options->expiry;
        }
        do_binary_op<couchbase::operations::increment_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
    } else {
        couchbase::operations::decrement_request req{ options->id };
        req.delta = options->delta;
        req.initial_value = options->initial_value;
        req.timeout = options->timeout_ms;
        req.durability_level = options->durability;
        if (0 < options->expiry) {
            req.expiry = options->expiry;
        }
        do_binary_op<couchbase::operations::decrement_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
    }
    Py_RETURN_NONE;
}

PyObject*
prepare_and_execute_binary_mutation_op(struct binary_mutation_options* options,
                                       PyObject* pyObj_callback,
                                       PyObject* pyObj_errback,
                                       std::shared_ptr<std::promise<PyObject*>> barrier,
                                       result* multi_result = nullptr)
{
    if (!PyBytes_Check(options->pyObj_value)) {
        if (multi_result != nullptr) {
            PyObject* pyObj_exc = pycbc_build_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Value should be bytes object.");
            if (-1 == PyDict_SetItemString(multi_result->dict, options->id.key().c_str(), pyObj_exc)) {
                // TODO:  not much we can do here...maybe?
                PyErr_Print();
                PyErr_Clear();
            }
            Py_DECREF(pyObj_exc);
            Py_INCREF(Py_False);
            barrier->set_value(Py_False);
            Py_RETURN_NONE;
        }
        barrier->set_value(nullptr);
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Value should be bytes object.");
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
        return nullptr;
    }

    PyObject* pyObj_unicode = PyUnicode_FromEncodedObject(options->pyObj_value, "utf-8", "strict");
    if (!pyObj_unicode) {
        if (multi_result != nullptr) {
            PyObject* pyObj_exc = pycbc_build_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unable to encode value.");
            if (-1 == PyDict_SetItemString(multi_result->dict, options->id.key().c_str(), pyObj_exc)) {
                // TODO:  not much we can do here...maybe?
                PyErr_Print();
                PyErr_Clear();
            }
            Py_DECREF(pyObj_exc);
            Py_INCREF(Py_False);
            barrier->set_value(Py_False);
            Py_RETURN_NONE;
        }
        barrier->set_value(nullptr);
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unable to encode value.");
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
        return nullptr;
    }
    std::string value = std::string(PyUnicode_AsUTF8(pyObj_unicode));
    Py_XDECREF(pyObj_unicode);

    if (options->op_type == Operations::APPEND) {
        couchbase::operations::append_request req{ options->id };
        // @TODO(): C++ client doesn't handle cas
        // req.cas = cas;
        req.timeout = options->timeout_ms;
        req.durability_level = options->durability;
        req.value = value;
        do_binary_op<couchbase::operations::append_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);

    } else {

        couchbase::operations::prepend_request req{ options->id };
        // @TODO(): C++ client doesn't handle cas
        // req.cas = cas;
        req.timeout = options->timeout_ms;
        req.durability_level = options->durability;
        req.value = value;
        do_binary_op<couchbase::operations::prepend_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
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
    uint64_t cas_int = 0;

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
                                          &cas_int,
                                          &delta,
                                          &initial_value);

    if (!ret) {
        pycbc_set_python_exception(
          PycbcError::InvalidArgument, __FILE__, __LINE__, "Cannot perform binary operation.  Unable to parse args/kwargs.");
        return nullptr;
    }

    connection* conn = nullptr;
    conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    if (nullptr == conn) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, NULL_CONN_OBJECT);
        return nullptr;
    }

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

    couchbase::cas cas = couchbase::cas{ 0 };
    if (cas_int != 0) {
        cas = couchbase::cas{ cas_int };
    }

    // PyObjects that need to be around for the cxx client lambda
    // have their increment/decrement handled w/in the callback_context struct
    // struct callback_context callback_ctx = { pyObj_callback, pyObj_errback };
    Py_XINCREF(pyObj_callback);
    Py_XINCREF(pyObj_errback);

    PyObject* pyObj_op_response = nullptr;
    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();

    switch (op_type) {
        case Operations::APPEND:
        case Operations::PREPEND: {

            struct binary_mutation_options opts = { conn,         id,         op_type,          pyObj_value, timeout_ms,
                                                    replicate_to, persist_to, durability_level, cas,         pyObj_span };
            pyObj_op_response = prepare_and_execute_binary_mutation_op(&opts, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case Operations::INCREMENT:
        case Operations::DECREMENT: {
            struct counter_options opts = { conn,         id,         op_type,          delta,         timeout_ms, expiry,
                                            replicate_to, persist_to, durability_level, initial_value, pyObj_span };
            pyObj_op_response = prepare_and_execute_counter_op(&opts, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        default: {
            pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized binary operation passed in.");
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

    return pyObj_op_response;
}

struct counter_options
get_counter_options(PyObject* op_args)
{

    struct counter_options opts {
    };

    PyObject* pyObj_delta = PyDict_GetItemString(op_args, "delta");
    if (pyObj_delta != nullptr) {
        auto delta = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_delta));
        opts.delta = delta;
    }

    PyObject* pyObj_initial = PyDict_GetItemString(op_args, "initial");
    if (pyObj_initial != nullptr) {
        auto initial = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_initial));
        opts.initial_value = initial;
    }

    PyObject* pyObj_span = PyDict_GetItemString(op_args, "span");
    if (pyObj_span != nullptr) {
        opts.pyObj_span = pyObj_span;
    }

    PyObject* pyObj_expiry = PyDict_GetItemString(op_args, "expiry");
    if (pyObj_expiry != nullptr) {
        auto expiry = static_cast<uint32_t>(PyLong_AsUnsignedLong(pyObj_expiry));
        opts.expiry = expiry;
    }

    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::key_value_timeout;
    PyObject* pyObj_timeout = PyDict_GetItemString(op_args, "timeout");
    if (pyObj_timeout != nullptr) {
        auto timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_timeout));
        auto timeout_ms = std::chrono::milliseconds(std::max(0ULL, timeout / 1000ULL));
        if (0 < timeout) {
            opts.timeout_ms = timeout_ms;
        }
    }

    uint8_t durability = 0;
    PyObject* pyObj_durability = PyDict_GetItemString(op_args, "durability");
    couchbase::protocol::durability_level durability_level = couchbase::protocol::durability_level::none;
    if (pyObj_durability) {
        if (PyDict_Check(pyObj_durability)) {
            PyObject* pyObj_replicate_to = PyDict_GetItemString(pyObj_durability, "replicate_to");
            if (pyObj_replicate_to) {
                auto replicate_to = static_cast<uint8_t>(PyLong_AsLong(pyObj_replicate_to));
                opts.replicate_to = replicate_to;
            }
            PyObject* pyObj_persist_to = PyDict_GetItemString(pyObj_durability, "persist_to");
            if (pyObj_persist_to) {
                auto persist_to = static_cast<uint8_t>(PyLong_AsLong(pyObj_persist_to));
                opts.persist_to = persist_to;
            }
        } else if (PyLong_Check(pyObj_durability)) {
            durability = static_cast<uint8_t>(PyLong_AsLong(pyObj_durability));
        }

        if (durability != 0) {
            durability_level = static_cast<couchbase::protocol::durability_level>(durability);
        }
        opts.durability = durability_level;
    }

    return opts;
}

struct binary_mutation_options
get_binary_mutation_options(PyObject* op_args)
{

    struct binary_mutation_options opts {
    };

    PyObject* pyObj_span = PyDict_GetItemString(op_args, "span");
    if (pyObj_span != nullptr) {
        opts.pyObj_span = pyObj_span;
    }

    PyObject* pyObj_cas = PyDict_GetItemString(op_args, "cas");
    couchbase::cas cas = couchbase::cas{ 0 };
    if (pyObj_cas != nullptr) {
        auto cas_int = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_cas));
        if (cas_int != 0) {
            cas = couchbase::cas{ cas_int };
        }
    }
    opts.cas = cas;

    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::key_value_timeout;
    PyObject* pyObj_timeout = PyDict_GetItemString(op_args, "timeout");
    if (pyObj_timeout != nullptr) {
        auto timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_timeout));
        auto timeout_ms = std::chrono::milliseconds(std::max(0ULL, timeout / 1000ULL));
        if (0 < timeout) {
            opts.timeout_ms = timeout_ms;
        }
    }

    uint8_t durability = 0;
    PyObject* pyObj_durability = PyDict_GetItemString(op_args, "durability");
    couchbase::protocol::durability_level durability_level = couchbase::protocol::durability_level::none;
    if (pyObj_durability) {
        if (PyDict_Check(pyObj_durability)) {
            PyObject* pyObj_replicate_to = PyDict_GetItemString(pyObj_durability, "replicate_to");
            if (pyObj_replicate_to) {
                auto replicate_to = static_cast<uint8_t>(PyLong_AsLong(pyObj_replicate_to));
                opts.replicate_to = replicate_to;
            }
            PyObject* pyObj_persist_to = PyDict_GetItemString(pyObj_durability, "persist_to");
            if (pyObj_persist_to) {
                auto persist_to = static_cast<uint8_t>(PyLong_AsLong(pyObj_persist_to));
                opts.persist_to = persist_to;
            }
        } else if (PyLong_Check(pyObj_durability)) {
            durability = static_cast<uint8_t>(PyLong_AsLong(pyObj_durability));
        }

        if (durability != 0) {
            durability_level = static_cast<couchbase::protocol::durability_level>(durability);
        }
        opts.durability = durability_level;
    }

    return opts;
}

PyObject*
handle_binary_multi_op([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* pyObj_conn = nullptr;
    char* bucket = nullptr;
    char* scope = nullptr;
    char* collection = nullptr;
    Operations::OperationType op_type = Operations::UNKNOWN;
    PyObject* pyObj_op_args = nullptr;

    static const char* kw_list[] = { "conn", "bucket", "scope", "collection_name", "op_type", "op_args", nullptr };

    const char* kw_format = "O!sssIO";
    int ret = PyArg_ParseTupleAndKeywords(args,
                                          kwargs,
                                          kw_format,
                                          const_cast<char**>(kw_list),
                                          &PyCapsule_Type,
                                          &pyObj_conn,
                                          &bucket,
                                          &scope,
                                          &collection,
                                          &op_type,
                                          &pyObj_op_args);
    if (!ret) {
        pycbc_set_python_exception(
          PycbcError::InvalidArgument, __FILE__, __LINE__, "Cannot perform binary operation.  Unable to parse args/kwargs.");
        return nullptr;
    }

    connection* conn = nullptr;

    conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    if (nullptr == conn) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, NULL_CONN_OBJECT);
        return nullptr;
    }

    std::vector<std::future<PyObject*>> op_results{};

    PyObject* pyObj_multi_result = create_result_obj();
    result* multi_result = reinterpret_cast<result*>(pyObj_multi_result);

    if (pyObj_op_args && PyDict_Check(pyObj_op_args)) {
        PyObject *pyObj_doc_key, *pyObj_op_dict;
        Py_ssize_t pos = 0;

        // PyObj_key and pyObj_value are borrowed references
        while (PyDict_Next(pyObj_op_args, &pos, &pyObj_doc_key, &pyObj_op_dict)) {
            std::string k;
            bool do_op = false;
            PyObject* pyObj_op_response = nullptr;
            if (PyUnicode_Check(pyObj_doc_key)) {
                k = std::string(PyUnicode_AsUTF8(pyObj_doc_key));
            }
            auto barrier = std::make_shared<std::promise<PyObject*>>();
            auto f = barrier->get_future();
            if (PyDict_Check(pyObj_op_dict) && !k.empty()) {
                PyObject* pyObj_value = PyDict_GetItemString(pyObj_op_dict, "value");
                switch (op_type) {
                    case Operations::APPEND:
                    case Operations::PREPEND: {
                        auto opts = get_binary_mutation_options(pyObj_op_dict);
                        opts.op_type = op_type;
                        if (pyObj_value != nullptr) {
                            opts.pyObj_value = pyObj_value;
                        }

                        couchbase::document_id id{ bucket, scope, collection, k };
                        opts.conn = conn;
                        opts.id = id;

                        pyObj_op_response = prepare_and_execute_binary_mutation_op(&opts, nullptr, nullptr, barrier, multi_result);
                        break;
                    }
                    case Operations::INCREMENT:
                    case Operations::DECREMENT: {
                        auto opts = get_counter_options(pyObj_op_dict);
                        opts.op_type = op_type;

                        couchbase::document_id id{ bucket, scope, collection, k };
                        opts.conn = conn;
                        opts.id = id;

                        pyObj_op_response = prepare_and_execute_counter_op(&opts, nullptr, nullptr, barrier, multi_result);
                        break;
                    }
                    default: {
                        PyObject* pyObj_exc = pycbc_build_exception(
                          PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized binary operation passed in.");
                        barrier->set_value(pyObj_exc);
                        break;
                    }
                };
            }

            Py_XDECREF(pyObj_op_response);
            op_results.emplace_back(std::move(f));
        }
    }

    auto all_okay = true;
    for (auto i = 0; i < op_results.size(); i++) {
        PyObject* res = nullptr;
        Py_BEGIN_ALLOW_THREADS res = op_results[i].get();
        Py_END_ALLOW_THREADS if (res == Py_False)
        {
            all_okay = false;
        }
        Py_XDECREF(res);
    }

    if (all_okay) {
        PyDict_SetItemString(multi_result->dict, "all_okay", Py_True);
    } else {
        PyDict_SetItemString(multi_result->dict, "all_okay", Py_False);
    }

    return reinterpret_cast<PyObject*>(multi_result);
}
