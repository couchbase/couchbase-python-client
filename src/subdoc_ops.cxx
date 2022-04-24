#include "subdoc_ops.hxx"
#include "exceptions.hxx"
#include "result.hxx"
#include <couchbase/cas.hxx>

template<typename T>
result*
add_extras_to_result([[maybe_unused]] const T& t, result* res)
{
    return res;
}

template<>
result*
add_extras_to_result<couchbase::operations::lookup_in_response>(const couchbase::operations::lookup_in_response& resp, result* res)
{
    if (!res->ec) {
        PyObject* pyObj_fields = PyList_New(static_cast<Py_ssize_t>(0));
        for (auto f : resp.fields) {
            PyObject* pyObj_field = PyDict_New();

            PyObject* pyObj_tmp = PyLong_FromUnsignedLong(static_cast<unsigned long>(f.opcode));
            if (-1 == PyDict_SetItemString(pyObj_field, "opcode", pyObj_tmp)) {
                Py_XDECREF(pyObj_fields);
                Py_XDECREF(pyObj_field);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);

            pyObj_tmp = PyBool_FromLong(static_cast<long>(f.exists));
            if (-1 == PyDict_SetItemString(pyObj_field, "exists", pyObj_tmp)) {
                Py_XDECREF(pyObj_fields);
                Py_XDECREF(pyObj_field);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);

            pyObj_tmp = PyLong_FromUnsignedLong(static_cast<unsigned long>(f.status));
            if (-1 == PyDict_SetItemString(pyObj_field, "status", pyObj_tmp)) {
                Py_XDECREF(pyObj_fields);
                Py_XDECREF(pyObj_field);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);

            pyObj_tmp = PyUnicode_DecodeUTF8(f.path.c_str(), f.path.length(), "strict");
            if (-1 == PyDict_SetItemString(pyObj_field, "path", pyObj_tmp)) {
                Py_XDECREF(pyObj_fields);
                Py_XDECREF(pyObj_field);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);

            pyObj_tmp = PyLong_FromUnsignedLong(static_cast<unsigned long>(f.original_index));
            if (-1 == PyDict_SetItemString(pyObj_field, "original_index", pyObj_tmp)) {
                Py_XDECREF(pyObj_fields);
                Py_XDECREF(pyObj_field);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);

            if (f.value.length()) {
                pyObj_tmp = PyBytes_FromStringAndSize(f.value.c_str(), f.value.length());
                if (-1 == PyDict_SetItemString(pyObj_field, RESULT_VALUE, pyObj_tmp)) {
                    Py_XDECREF(pyObj_fields);
                    Py_XDECREF(pyObj_field);
                    Py_XDECREF(pyObj_tmp);
                    return nullptr;
                }
                Py_DECREF(pyObj_tmp);
            }
            PyList_Append(pyObj_fields, pyObj_field);
            Py_DECREF(pyObj_field);
        }

        if (-1 == PyDict_SetItemString(res->dict, RESULT_VALUE, pyObj_fields)) {
            Py_XDECREF(pyObj_fields);
            return nullptr;
        }
        Py_DECREF(pyObj_fields);
    }
    return res;
}

template<>
result*
add_extras_to_result<couchbase::operations::mutate_in_response>(const couchbase::operations::mutate_in_response& resp, result* res)
{
    PyObject* pyObj_mutation_token = create_mutation_token_obj(resp.token);
    if (-1 == PyDict_SetItemString(res->dict, RESULT_MUTATION_TOKEN, pyObj_mutation_token)) {
        Py_XDECREF(pyObj_mutation_token);
        return nullptr;
    }
    Py_DECREF(pyObj_mutation_token);

    if (!res->ec) {
        PyObject* pyObj_fields = PyList_New(static_cast<Py_ssize_t>(0));
        for (auto f : resp.fields) {
            PyObject* pyObj_field = PyDict_New();
            PyObject* pyObj_tmp = PyLong_FromUnsignedLong(static_cast<unsigned long>(f.opcode));
            if (-1 == PyDict_SetItemString(pyObj_field, "opcode", pyObj_tmp)) {
                Py_XDECREF(pyObj_fields);
                Py_XDECREF(pyObj_field);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);

            pyObj_tmp = PyLong_FromUnsignedLong(static_cast<unsigned long>(f.status));
            if (-1 == PyDict_SetItemString(pyObj_field, "status", pyObj_tmp)) {
                Py_XDECREF(pyObj_fields);
                Py_XDECREF(pyObj_field);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);

            pyObj_tmp = PyUnicode_DecodeUTF8(f.path.c_str(), f.path.length(), "strict");
            if (-1 == PyDict_SetItemString(pyObj_field, "path", pyObj_tmp)) {
                Py_XDECREF(pyObj_fields);
                Py_XDECREF(pyObj_field);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);

            pyObj_tmp = PyLong_FromUnsignedLong(static_cast<unsigned long>(f.original_index));
            if (-1 == PyDict_SetItemString(pyObj_field, "original_index", pyObj_tmp)) {
                Py_XDECREF(pyObj_fields);
                Py_XDECREF(pyObj_field);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);

            if (f.value.length()) {
                pyObj_tmp = PyBytes_FromStringAndSize(f.value.c_str(), f.value.length());
                if (-1 == PyDict_SetItemString(pyObj_field, RESULT_VALUE, pyObj_tmp)) {
                    Py_XDECREF(pyObj_fields);
                    Py_XDECREF(pyObj_field);
                    Py_XDECREF(pyObj_tmp);
                    return nullptr;
                }
                Py_DECREF(pyObj_tmp);
            }
            PyList_Append(pyObj_fields, pyObj_field);
            Py_DECREF(pyObj_field);
        }

        if (-1 == PyDict_SetItemString(res->dict, RESULT_VALUE, pyObj_fields)) {
            Py_XDECREF(pyObj_fields);
            return nullptr;
        }
        Py_DECREF(pyObj_fields);
    }
    return res;
}

template<typename T>
result*
create_base_result_from_subdoc_op_response(const char* key, const T& resp)
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

    if (-1 == PyDict_SetItemString(res->dict, RESULT_FLAGS, Py_None)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }

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
create_result_from_subdoc_op_response(const char* key,
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
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "Subdoc operation error.");
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
        auto res = create_base_result_from_subdoc_op_response(key, resp);
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
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "Subdoc operation error.");
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
void
do_subdoc_op(connection& conn,
             Request& req,
             PyObject* pyObj_callback,
             PyObject* pyObj_errback,
             std::shared_ptr<std::promise<PyObject*>> barrier)
{
    using response_type = typename Request::response_type;
    Py_BEGIN_ALLOW_THREADS conn.cluster_->execute(req, [key = req.id.key(), pyObj_callback, pyObj_errback, barrier](response_type resp) {
        create_result_from_subdoc_op_response(key.c_str(), resp, pyObj_callback, pyObj_errback, barrier);
    });
    Py_END_ALLOW_THREADS
}

PyObject*
prepare_and_execute_lookup_in_op(struct lookup_in_options* options,
                                 size_t nspecs,
                                 PyObject* pyObj_callback,
                                 PyObject* pyObj_errback,
                                 std::shared_ptr<std::promise<PyObject*>> barrier)
{
    size_t ii;
    couchbase::protocol::lookup_in_request_body::lookup_in_specs specs = couchbase::protocol::lookup_in_request_body::lookup_in_specs{};
    for (ii = 0; ii < nspecs; ++ii) {

        struct lookup_in_spec new_spec = {};
        PyObject* pyObj_spec = nullptr;
        if (PyTuple_Check(options->specs)) {
            pyObj_spec = PyTuple_GetItem(options->specs, ii);
        } else {
            pyObj_spec = PyList_GetItem(options->specs, ii);
        }

        if (!pyObj_spec) {
            pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unable to parse spec.");
            Py_XDECREF(pyObj_callback);
            Py_XDECREF(pyObj_errback);
            return nullptr;
        }

        if (!PyArg_ParseTuple(pyObj_spec, "bsp", &new_spec.op, &new_spec.path, &new_spec.xattr)) {
            pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unable to parse spec.");
            Py_XDECREF(pyObj_callback);
            Py_XDECREF(pyObj_errback);
            return nullptr;
        }
        specs.add_spec(new_spec.op, new_spec.xattr ? specs.path_flag_xattr : 0, new_spec.path);
    }

    couchbase::operations::lookup_in_request req{ options->id };
    req.timeout = options->timeout_ms;
    req.specs = specs;
    do_subdoc_op<couchbase::operations::lookup_in_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier);
    Py_RETURN_NONE;
}

PyObject*
prepare_and_execute_mutate_in_op(struct mutate_in_options* options,
                                 size_t nspecs,
                                 PyObject* pyObj_callback,
                                 PyObject* pyObj_errback,
                                 std::shared_ptr<std::promise<PyObject*>> barrier)
{
    size_t ii;
    couchbase::protocol::mutate_in_request_body::mutate_in_specs specs = couchbase::protocol::mutate_in_request_body::mutate_in_specs{};
    for (ii = 0; ii < nspecs; ++ii) {

        struct mutate_in_spec new_spec = {};
        PyObject* pyObj_spec = nullptr;
        if (PyTuple_Check(options->specs)) {
            pyObj_spec = PyTuple_GetItem(options->specs, ii);
        } else {
            pyObj_spec = PyList_GetItem(options->specs, ii);
        }

        if (!pyObj_spec) {
            pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unable to parse spec.");
            Py_XDECREF(pyObj_callback);
            Py_XDECREF(pyObj_errback);
            return nullptr;
        }

        if (!PyArg_ParseTuple(pyObj_spec,
                              "bsppp|O",
                              &new_spec.op,
                              &new_spec.path,
                              &new_spec.create_parents,
                              &new_spec.xattr,
                              &new_spec.expand_macros,
                              &new_spec.pyObj_value)) {
            pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unable to parse spec.");
            Py_XDECREF(pyObj_callback);
            Py_XDECREF(pyObj_errback);
            return nullptr;
        }
        new_spec.flags = specs.build_path_flags(new_spec.xattr, new_spec.create_parents, new_spec.expand_macros);

        // **DO NOT DECREF** these -- things from tuples are borrowed references!!
        PyObject* pyObj_value = nullptr;
        std::string value = std::string();

        if (new_spec.pyObj_value) {
            if (PyUnicode_Check(new_spec.pyObj_value)) {
                value = std::string(PyUnicode_AsUTF8(new_spec.pyObj_value));
            } else {
                PyObject* pyObj_unicode = PyUnicode_FromEncodedObject(new_spec.pyObj_value, "utf-8", "strict");
                value = std::string(PyUnicode_AsUTF8(pyObj_unicode));
                Py_DECREF(pyObj_unicode);
            }
        }

        switch (couchbase::protocol::subdoc_opcode(new_spec.op)) {
            case couchbase::protocol::subdoc_opcode::array_push_last:
            case couchbase::protocol::subdoc_opcode::array_push_first:
            case couchbase::protocol::subdoc_opcode::array_insert:
            case couchbase::protocol::subdoc_opcode::array_add_unique: {
                if (!value.empty()) {
                    value = value.substr(1, value.length() - 2);
                }
                new_spec.value = value;
                break;
            }
            default: {
                new_spec.value = value;
            }
        };

        specs.add_spec(new_spec.op, new_spec.flags, new_spec.path, new_spec.value);
    }

    couchbase::protocol::durability_level durability_level = couchbase::protocol::durability_level::none;
    if (options->durability != 0) {
        durability_level = static_cast<couchbase::protocol::durability_level>(options->durability);
    }

    couchbase::cas cas = couchbase::cas{ 0 };
    if (options->cas != 0) {
        cas = couchbase::cas{ options->cas };
    }

    couchbase::protocol::mutate_in_request_body::store_semantics_type store_semantics;
    switch (options->semantics) {
        case 1: {
            store_semantics = couchbase::protocol::mutate_in_request_body::store_semantics_type::upsert;
            break;
        }
        case 2: {
            store_semantics = couchbase::protocol::mutate_in_request_body::store_semantics_type::insert;
            break;
        }
        default: {
            store_semantics = couchbase::protocol::mutate_in_request_body::store_semantics_type::replace;
            break;
        }
    };

    couchbase::operations::mutate_in_request req{ options->id };
    req.cas = cas;
    req.timeout = options->timeout_ms;
    if (0 < options->expiry) {
        req.expiry = options->expiry;
    }
    req.store_semantics = store_semantics;
    req.specs = specs;
    req.durability_level = durability_level;
    if (options->preserve_expiry) {
        req.preserve_expiry = options->preserve_expiry;
    }

    do_subdoc_op<couchbase::operations::mutate_in_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier);
    Py_RETURN_NONE;
}

PyObject*
handle_subdoc_op([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
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
    PyObject* pyObj_spec = nullptr;
    PyObject* pyObj_span = nullptr;
    PyObject* pyObj_durability = nullptr;

    // optional
    uint8_t semantics = 0;
    uint32_t expiry = 0;
    uint64_t timeout = 0;
    uint64_t cas = 0;
    // booleans, but use int to read from kwargs
    int access_deleted = 0;
    int preserve_expiry = 0;

    static const char* kw_list[] = {
        "conn", "bucket",     "scope",           "collection_name", "key",     "op_type", "callback",       "errback",         "spec",
        "span", "durability", "store_semantics", "expiry",          "timeout", "cas",     "access_deleted", "preserve_expiry", nullptr
    };

    const char* kw_format = "O!ssssI|OOOOObILLii";
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
                                          &pyObj_spec,
                                          &pyObj_span,
                                          &pyObj_durability,
                                          &semantics,
                                          &expiry,
                                          &timeout,
                                          &cas,
                                          &access_deleted,
                                          &preserve_expiry);

    if (!ret) {
        pycbc_set_python_exception(
          PycbcError::InvalidArgument, __FILE__, __LINE__, "Cannot perform subdoc operation.  Unable to parse args/kwargs.");
        return nullptr;
    }

    if (!PyTuple_Check(pyObj_spec) && !PyList_Check(pyObj_spec)) {
        pycbc_set_python_exception(
          PycbcError::InvalidArgument, __FILE__, __LINE__, "Cannot perform subdoc operation.  Value must be a tuple or list.");
        return nullptr;
    }

    size_t nspecs;
    if (PyTuple_Check(pyObj_spec)) {
        nspecs = static_cast<size_t>(PyTuple_GET_SIZE(pyObj_spec));
    } else {
        nspecs = static_cast<size_t>(PyList_GET_SIZE(pyObj_spec));
    }

    if (nspecs == 0) {
        pycbc_set_python_exception(
          PycbcError::InvalidArgument, __FILE__, __LINE__, "Cannot perform subdoc operation.  Need at least one command.");
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

    // PyObjects that need to be around for the cxx client lambda
    // have their increment/decrement handled w/in the callback_context struct
    // struct callback_context callback_ctx = { pyObj_callback, pyObj_errback };
    Py_XINCREF(pyObj_callback);
    Py_XINCREF(pyObj_errback);

    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();
    switch (op_type) {
        case Operations::LOOKUP_IN: {
            struct lookup_in_options opts = { conn, id, Operations::LOOKUP_IN, timeout_ms, access_deleted == 1, pyObj_span, pyObj_spec };
            prepare_and_execute_lookup_in_op(&opts, nspecs, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case Operations::MUTATE_IN: {
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

            struct mutate_in_options opts = {
                conn, id,         Operations::MUTATE_IN, durability,          replicate_to, persist_to, semantics, expiry,
                cas,  timeout_ms, preserve_expiry == 1,  access_deleted == 1, pyObj_span,   pyObj_spec
            };
            prepare_and_execute_mutate_in_op(&opts, nspecs, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        default: {
            pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized subdoc operation passed in.");
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
    Py_RETURN_NONE;
}
