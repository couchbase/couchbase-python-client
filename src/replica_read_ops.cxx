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

#include "replica_read_ops.hxx"
#include "exceptions.hxx"
#include "result.hxx"
#include "utils.hxx"

// @TODO:  use this for other operations?  possibly have access to flags in future?

struct kv_value {
    PyObject* bytes_value;
    std::uint32_t flags;
};

struct pass_thru_decoder {
    using value_type = std::pair<PyObject*, std::uint32_t>;

    static auto decode(const std::vector<std::byte> data, std::uint32_t flags) -> value_type
    {
        PyObject* pyObj_value = nullptr;
        try {
            pyObj_value = binary_to_PyObject(data);
        } catch (const std::exception& e) {
            PyErr_SetString(PyExc_TypeError, e.what());
        }
        return { pyObj_value, flags };
    }
};

template<typename Result>
PyObject*
get_replica_result(const char* key, const Result& replica_result)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);

    PyObject* pyObj_tmp = nullptr;
    if (nullptr != key) {
        PyObject* pyObj_tmp = PyUnicode_FromString(key);
        if (-1 == PyDict_SetItemString(res->dict, RESULT_KEY, pyObj_tmp)) {
            Py_DECREF(pyObj_result);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (-1 == PyDict_SetItemString(res->dict, "is_replica", replica_result.is_replica() ? Py_True : Py_False)) {
        Py_DECREF(pyObj_result);
        return nullptr;
    }

    pyObj_tmp = PyLong_FromUnsignedLongLong(replica_result.cas().value());
    if (-1 == PyDict_SetItemString(res->dict, RESULT_CAS, pyObj_tmp)) {
        Py_DECREF(pyObj_result);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    auto [pyObj_value, flags] = replica_result.template content_as<pass_thru_decoder>();

    pyObj_tmp = PyLong_FromUnsignedLong(flags);
    if (-1 == PyDict_SetItemString(res->dict, RESULT_FLAGS, pyObj_tmp)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    if (-1 == PyDict_SetItemString(res->dict, RESULT_VALUE, pyObj_value)) {
        Py_DECREF(pyObj_result);
        Py_DECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_value);

    return reinterpret_cast<PyObject*>(res);
}

template<>
PyObject*
get_replica_result<couchbase::get_all_replicas_result>(const char* key, const couchbase::get_all_replicas_result& replica_results)
{
    streamed_result* streamed_res = create_streamed_result_obj(couchbase::core::timeout_defaults::key_value_durable_timeout);
    for (auto const& replica : replica_results) {
        auto res = get_replica_result(key, replica);
        if (res == nullptr) {
            return nullptr;
        }
        streamed_res->rows->put(res);
    }
    Py_INCREF(Py_None);
    streamed_res->rows->put(Py_None);
    return reinterpret_cast<PyObject*>(streamed_res);
}

template<typename Context, typename Response>
void
handle_replica_result(const char* key,
                      const Context& ctx,
                      const Response& resp,
                      PyObject* pyObj_callback,
                      PyObject* pyObj_errback,
                      std::shared_ptr<std::promise<PyObject*>> barrier = nullptr,
                      result* multi_result = nullptr)
{
    PyObject* pyObj_args = NULL;
    PyObject* pyObj_kwargs = nullptr;
    PyObject* pyObj_exc = nullptr;
    PyObject* pyObj_func = nullptr;
    PyObject* pyObj_callback_res = nullptr;
    auto set_exception = false;

    PyGILState_STATE state = PyGILState_Ensure();
    if (ctx.ec()) {
        pyObj_exc = build_exception_from_context(ctx, __FILE__, __LINE__, "KV read replica operation error.");
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
        auto res = get_replica_result(key, resp);
        if (res == nullptr || PyErr_Occurred() != nullptr) {
            set_exception = true;
        } else {
            if (pyObj_callback == nullptr) {
                if (multi_result != nullptr) {
                    Py_INCREF(Py_True);
                    barrier->set_value(Py_True);
                    if (-1 == PyDict_SetItemString(multi_result->dict, key, res)) {
                        // TODO:  not much we can do here...maybe?
                        PyErr_Print();
                        PyErr_Clear();
                    }
                    Py_DECREF(res);
                } else {
                    barrier->set_value(res);
                }
            } else {
                pyObj_func = pyObj_callback;
                pyObj_args = PyTuple_New(1);
                PyTuple_SET_ITEM(pyObj_args, 0, res);
            }
        }
    }

    if (set_exception) {
        pyObj_exc = pycbc_build_exception(
          PycbcError::UnableToBuildResult, __FILE__, __LINE__, "Unable to build result object for KV read replica operation.");
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
    } else if (pyObj_func != nullptr) {
        pyObj_callback_res = PyObject_Call(pyObj_func, pyObj_args, pyObj_kwargs);
        if (pyObj_callback_res) {
            Py_DECREF(pyObj_callback_res);
        } else {
            pycbc_set_python_exception(PycbcError::InternalSDKError, __FILE__, __LINE__, "KV read replica operation callback failed.");
        }
        Py_DECREF(pyObj_args);
        Py_XDECREF(pyObj_kwargs);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }
    PyGILState_Release(state);
}

PyObject*
handle_replica_op([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
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
    PyObject* pyObj_span = nullptr;

    // optional
    uint64_t timeout = 0;

    static const char* kw_list[] = { "conn", "bucket",   "scope",   "collection_name", "key",  "op_type",
                                     "span", "callback", "errback", "timeout",         nullptr };

    const char* kw_format = "O!ssssI|OOOL";
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
                                          &pyObj_span,
                                          &pyObj_callback,
                                          &pyObj_errback,
                                          &timeout);
    if (!ret) {
        pycbc_set_python_exception(
          PycbcError::InvalidArgument, __FILE__, __LINE__, "Cannot perform replica read operation.  Unable to parse args/kwargs.");
        return nullptr;
    }

    connection* conn = nullptr;
    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::key_value_timeout;

    conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    if (nullptr == conn) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, NULL_CONN_OBJECT);
        return nullptr;
    }

    couchbase::core::document_id id{ bucket, scope, collection, key };
    if (0 < timeout) {
        timeout_ms = std::chrono::milliseconds(std::max(0ULL, timeout / 1000ULL));
    }

    // PyObjects that need to be around for the cxx client lambda
    // have their increment/decrement handled w/in the callback_context struct
    // struct callback_context callback_ctx = { pyObj_callback, pyObj_errback, pyObj_transcoder };
    Py_XINCREF(pyObj_callback);
    Py_XINCREF(pyObj_errback);

    std::shared_ptr<std::promise<PyObject*>> barrier = nullptr;
    std::future<PyObject*> fut;
    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        barrier = std::make_shared<std::promise<PyObject*>>();
        fut = barrier->get_future();
    }

    if (op_type == Operations::GET_ANY_REPLICA) {
        auto opts = couchbase::get_any_replica_options{}.timeout(timeout_ms).build();
        Py_BEGIN_ALLOW_THREADS couchbase::core::impl::initiate_get_any_replica_operation(
          conn->cluster_,
          id.bucket(),
          id.scope(),
          id.collection(),
          id.key(),
          opts,
          [key = id.key(), pyObj_callback, pyObj_errback, barrier](auto ctx, auto resp) {
              handle_replica_result(key.c_str(), std::move(ctx), std::move(resp), pyObj_callback, pyObj_errback, barrier);
          });
        Py_END_ALLOW_THREADS
    } else if (op_type == Operations::GET_ALL_REPLICAS) {
        auto opts = couchbase::get_all_replicas_options{}.timeout(timeout_ms).build();
        Py_BEGIN_ALLOW_THREADS couchbase::core::impl::initiate_get_all_replicas_operation(
          conn->cluster_,
          id.bucket(),
          id.scope(),
          id.collection(),
          id.key(),
          opts,
          [key = id.key(), pyObj_callback, pyObj_errback, barrier](auto ctx, auto resp) {
              handle_replica_result(key.c_str(), std::move(ctx), std::move(resp), pyObj_callback, pyObj_errback, barrier);
          });
        Py_END_ALLOW_THREADS
    } else {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized replica operation passed in.");
        barrier->set_value(nullptr);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);

        return nullptr;
    }

    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        PyObject* ret = nullptr;
        Py_BEGIN_ALLOW_THREADS ret = fut.get();
        Py_END_ALLOW_THREADS return ret;
    }

    Py_RETURN_NONE;
}

PyObject*
handle_replica_multi_op([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
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
          PycbcError::InvalidArgument, __FILE__, __LINE__, "Cannot perform multi replica read operation.  Unable to parse args/kwargs.");
        return nullptr;
    }

    connection* conn = nullptr;
    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::key_value_timeout;

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
            if (PyUnicode_Check(pyObj_doc_key)) {
                k = std::string(PyUnicode_AsUTF8(pyObj_doc_key));
            }
            auto barrier = std::make_shared<std::promise<PyObject*>>();
            auto f = barrier->get_future();

            if (PyDict_Check(pyObj_op_dict) && !k.empty()) {

                couchbase::core::document_id id{ bucket, scope, collection, k };
                std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::key_value_timeout;
                PyObject* pyObj_timeout = PyDict_GetItemString(pyObj_op_dict, "timeout");
                if (pyObj_timeout != nullptr) {
                    auto timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_timeout));
                    timeout_ms = std::chrono::milliseconds(std::max(0ULL, timeout / 1000ULL));
                }

                if (op_type == Operations::GET_ANY_REPLICA) {
                    auto opts = couchbase::get_any_replica_options{}.timeout(timeout_ms).build();
                    Py_BEGIN_ALLOW_THREADS couchbase::core::impl::initiate_get_any_replica_operation(
                      conn->cluster_,
                      id.bucket(),
                      id.scope(),
                      id.collection(),
                      id.key(),
                      opts,
                      [key = id.key(), barrier, multi_result](auto ctx, auto resp) {
                          handle_replica_result(key.c_str(), std::move(ctx), std::move(resp), nullptr, nullptr, barrier, multi_result);
                      });
                    Py_END_ALLOW_THREADS

                } else if (op_type == Operations::GET_ALL_REPLICAS) {
                    auto opts = couchbase::get_all_replicas_options{}.timeout(timeout_ms).build();
                    Py_BEGIN_ALLOW_THREADS couchbase::core::impl::initiate_get_all_replicas_operation(
                      conn->cluster_,
                      id.bucket(),
                      id.scope(),
                      id.collection(),
                      id.key(),
                      opts,
                      [key = id.key(), barrier, multi_result](auto ctx, auto resp) {
                          handle_replica_result(key.c_str(), std::move(ctx), std::move(resp), nullptr, nullptr, barrier, multi_result);
                      });
                    Py_END_ALLOW_THREADS
                } else {
                    PyObject* pyObj_exc = pycbc_build_exception(
                      PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized replica read operation passed in.");
                    barrier->set_value(pyObj_exc);
                    break;
                }
            }
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
