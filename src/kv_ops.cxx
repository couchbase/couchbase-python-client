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

#include "kv_ops.hxx"
#include "exceptions.hxx"
#include "result.hxx"
#include "tracing.hxx"
#include "utils.hxx"

template<typename T>
result*
add_extras_to_result([[maybe_unused]] const T& resp, result* res)
{
    return res;
}

template<typename T>
result*
add_flags_and_value_to_result(const T& resp, result* res)
{
    PyObject* pyObj_tmp = PyLong_FromUnsignedLong(resp.flags);
    if (-1 == PyDict_SetItemString(res->dict, RESULT_FLAGS, pyObj_tmp)) {
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_XDECREF(pyObj_tmp);

    if (!res->ec) {
        try {
            pyObj_tmp = binary_to_PyObject(resp.value);
        } catch (const std::exception& e) {
            PyErr_SetString(PyExc_TypeError, e.what());
            return nullptr;
        }
        if (-1 == PyDict_SetItemString(res->dict, RESULT_VALUE, pyObj_tmp)) {
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }
    return res;
}

template<>
result*
add_extras_to_result<couchbase::core::operations::exists_response>(const couchbase::core::operations::exists_response& resp, result* res)
{
    PyObject* pyObj_tmp = PyBool_FromLong(static_cast<long>(resp.exists()));
    if (-1 == PyDict_SetItemString(res->dict, RESULT_EXISTS, pyObj_tmp)) {
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);
    return res;
}

template<>
result*
add_extras_to_result<couchbase::core::operations::get_projected_response>(const couchbase::core::operations::get_projected_response& resp,
                                                                          result* res)
{
    if (resp.expiry) {
        PyObject* pyObj_tmp = PyLong_FromUnsignedLong(resp.expiry.value());
        if (-1 == PyDict_SetItemString(res->dict, RESULT_EXPIRY, pyObj_tmp)) {
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    return add_flags_and_value_to_result(resp, res);
}

template<>
result*
add_extras_to_result<couchbase::core::operations::get_response>(const couchbase::core::operations::get_response& resp, result* res)
{
    return add_flags_and_value_to_result(resp, res);
}
template<>
result*
add_extras_to_result<couchbase::core::operations::get_and_lock_response>(const couchbase::core::operations::get_and_lock_response& resp,
                                                                         result* res)
{
    return add_flags_and_value_to_result(resp, res);
}

template<>
result*
add_extras_to_result<couchbase::core::operations::get_and_touch_response>(const couchbase::core::operations::get_and_touch_response& resp,
                                                                          result* res)
{
    return add_flags_and_value_to_result(resp, res);
}

template<>
result*
add_extras_to_result<couchbase::core::operations::get_any_replica_response>(
  const couchbase::core::operations::get_any_replica_response& resp,
  result* res)
{
    if (-1 == PyDict_SetItemString(res->dict, "is_replica", resp.replica ? Py_True : Py_False)) {
        return nullptr;
    }

    return add_flags_and_value_to_result(resp, res);
}

template<>
result*
add_extras_to_result<couchbase::core::operations::get_all_replicas_response::entry>(
  const couchbase::core::operations::get_all_replicas_response::entry& resp,
  result* res)
{
    if (-1 == PyDict_SetItemString(res->dict, "is_replica", resp.replica ? Py_True : Py_False)) {
        return nullptr;
    }

    return add_flags_and_value_to_result(resp, res);
}

template<typename T>
result*
create_base_result_from_get_operation_response(const char* key, const T& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);
    res->ec = resp.ctx.ec();

    PyObject* pyObj_tmp = PyLong_FromUnsignedLongLong(resp.cas.value());
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
create_base_result_from_get_operation_response<couchbase::core::operations::get_all_replicas_response::entry>(
  const char* key,
  const couchbase::core::operations::get_all_replicas_response::entry& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);

    PyObject* pyObj_tmp = PyLong_FromUnsignedLongLong(resp.cas.value());
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

    if (resp.ctx.ec()) {
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "KV read operation error.");
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
        auto res = create_base_result_from_get_operation_response(key, resp);
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
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "KV read operation error.");
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

template<>
void
create_result_from_get_operation_response<couchbase::core::operations::get_all_replicas_response>(
  const char* key,
  const couchbase::core::operations::get_all_replicas_response& resp,
  PyObject* pyObj_callback,
  PyObject* pyObj_errback,
  std::shared_ptr<std::promise<PyObject*>> barrier,
  result* multi_result)
{
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* pyObj_args = NULL;
    PyObject* pyObj_kwargs = nullptr;
    PyObject* pyObj_exc = nullptr;
    PyObject* pyObj_func = nullptr;
    PyObject* pyObj_callback_res = nullptr;
    auto set_exception = false;
    streamed_result* streamed_res = nullptr;

    if (resp.ctx.ec()) {
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "KV read operation error.");
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
        auto streamed_res = create_streamed_result_obj(couchbase::core::timeout_defaults::key_value_durable_timeout);
        for (auto const& entry : resp.entries) {
            auto res = create_base_result_from_get_operation_response(key, entry);
            if (res == nullptr) {
                set_exception = true;
                break;
            }
            res = add_extras_to_result(entry, res);
            streamed_res->rows->put(reinterpret_cast<PyObject*>(res));
        }

        if (PyErr_Occurred() != nullptr) {
            set_exception = true;
        } else if (!set_exception) {
            Py_INCREF(Py_None);
            streamed_res->rows->put(Py_None);
            if (pyObj_callback == nullptr) {
                if (multi_result != nullptr) {
                    Py_INCREF(Py_True);
                    barrier->set_value(Py_True);
                    if (-1 == PyDict_SetItemString(multi_result->dict, key, reinterpret_cast<PyObject*>(streamed_res))) {
                        // TODO:  not much we can do here...maybe?
                        PyErr_Print();
                        PyErr_Clear();
                    }
                    Py_DECREF(reinterpret_cast<PyObject*>(streamed_res));
                } else {
                    barrier->set_value(reinterpret_cast<PyObject*>(streamed_res));
                }
            } else {
                pyObj_func = pyObj_callback;
                pyObj_args = PyTuple_New(1);
                PyTuple_SET_ITEM(pyObj_args, 0, reinterpret_cast<PyObject*>(streamed_res));
            }
        }
    }

    if (set_exception) {
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "KV read operation error.");
        streamed_res->rows->put(pyObj_exc);
        if (pyObj_errback == nullptr) {
            if (multi_result != nullptr) {
                Py_INCREF(Py_False);
                barrier->set_value(Py_False);
                if (-1 == PyDict_SetItemString(multi_result->dict, key, reinterpret_cast<PyObject*>(streamed_res))) {
                    // TODO:  not much we can do here...maybe?
                    PyErr_Print();
                    PyErr_Clear();
                }
                // won't fall into logic path where pyObj_exc is decremented later
                Py_DECREF(pyObj_exc);
            } else {
                barrier->set_value(reinterpret_cast<PyObject*>(streamed_res));
            }
        } else {
            pyObj_func = pyObj_errback;
            pyObj_args = PyTuple_New(1);
            PyTuple_SET_ITEM(pyObj_args, 0, reinterpret_cast<PyObject*>(streamed_res));
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

template<typename Request>
void
do_get(connection& conn,
       Request& req,
       PyObject* pyObj_callback,
       PyObject* pyObj_errback,
       std::shared_ptr<std::promise<PyObject*>> barrier,
       result* multi_result = nullptr)
{
    using response_type = typename Request::response_type;
    Py_BEGIN_ALLOW_THREADS conn.cluster_->execute(
      req, [key = req.id.key(), pyObj_callback, pyObj_errback, barrier, multi_result](response_type resp) {
          create_result_from_get_operation_response(key.c_str(), resp, pyObj_callback, pyObj_errback, barrier, multi_result);
      });
    Py_END_ALLOW_THREADS
}

PyObject*
prepare_and_execute_read_op(struct read_options* options,
                            PyObject* pyObj_callback,
                            PyObject* pyObj_errback,
                            std::shared_ptr<std::promise<PyObject*>> barrier,
                            result* multi_result = nullptr)
{
    switch (options->op_type) {
        case Operations::GET: {
            couchbase::core::operations::get_request req{ options->id };
            req.timeout = options->timeout_ms;
            if (nullptr != options->span) {
                req.parent_span = std::make_shared<pycbc::request_span>(options->span);
            }
            do_get<couchbase::core::operations::get_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
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
                    if (multi_result != nullptr) {
                        PyObject* pyObj_exc =
                          pycbc_build_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Project must be a list of strings.");
                        if (-1 == PyDict_SetItemString(multi_result->dict, options->id.key().c_str(), pyObj_exc)) {
                            // TODO:  not much we can do here...maybe?
                            PyErr_Print();
                            PyErr_Clear();
                        }
                        Py_DECREF(pyObj_exc);
                        Py_INCREF(Py_False);
                        barrier->set_value(Py_False);
                    } else {
                        if (barrier) {
                            barrier->set_value(nullptr);
                        }
                        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Project must be a list of strings.");
                        Py_XDECREF(pyObj_callback);
                        Py_XDECREF(pyObj_errback);
                        return nullptr;
                    }
                }
            }

            couchbase::core::operations::get_projected_request req{ options->id };
            req.timeout = options->timeout_ms;
            req.with_expiry = !!options->with_expiry;
            req.projections = projections;
            if (nullptr != options->span) {
                req.parent_span = std::make_shared<pycbc::request_span>(options->span);
            }
            do_get<couchbase::core::operations::get_projected_request>(
              *(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
            break;
        }
        case Operations::GET_ANY_REPLICA: {
            couchbase::core::operations::get_any_replica_request req{ options->id, options->timeout_ms };
            do_get<couchbase::core::operations::get_any_replica_request>(
              *(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
            break;
        }
        case Operations::GET_ALL_REPLICAS: {
            couchbase::core::operations::get_all_replicas_request req{ options->id, options->timeout_ms };
            do_get<couchbase::core::operations::get_all_replicas_request>(
              *(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
            break;
        }
        case Operations::GET_AND_TOUCH: {
            couchbase::core::operations::get_and_touch_request req{ options->id };
            req.expiry = options->expiry;
            req.timeout = options->timeout_ms;
            if (nullptr != options->span) {
                req.parent_span = std::make_shared<pycbc::request_span>(options->span);
            }
            do_get<couchbase::core::operations::get_and_touch_request>(
              *(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
            break;
        }
        case Operations::GET_AND_LOCK: {
            couchbase::core::operations::get_and_lock_request req{ options->id };
            req.lock_time = options->lock_time;
            req.timeout = options->timeout_ms;
            if (nullptr != options->span) {
                req.parent_span = std::make_shared<pycbc::request_span>(options->span);
            }
            do_get<couchbase::core::operations::get_and_lock_request>(
              *(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
            break;
        }
        case Operations::EXISTS: {
            couchbase::core::operations::exists_request req{ options->id };
            req.timeout = options->timeout_ms;
            if (nullptr != options->span) {
                req.parent_span = std::make_shared<pycbc::request_span>(options->span);
            }
            do_get<couchbase::core::operations::exists_request>(
              *(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
            break;
        }
        case Operations::TOUCH: {
            couchbase::core::operations::touch_request req{ options->id };
            req.expiry = options->expiry;
            req.timeout = options->timeout_ms;
            if (nullptr != options->span) {
                req.parent_span = std::make_shared<pycbc::request_span>(options->span);
            }
            do_get<couchbase::core::operations::touch_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
            break;
        }
        case Operations::UNLOCK: {
            couchbase::core::operations::unlock_request req{ options->id };
            req.cas = options->cas;
            req.timeout = options->timeout_ms;
            if (nullptr != options->span) {
                req.parent_span = std::make_shared<pycbc::request_span>(options->span);
            }
            do_get<couchbase::core::operations::unlock_request>(
              *(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
            break;
        }
        default: {
            if (multi_result != nullptr) {
                PyObject* pyObj_exc =
                  pycbc_build_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized get operation passed in.");
                if (-1 == PyDict_SetItemString(multi_result->dict, options->id.key().c_str(), pyObj_exc)) {
                    // TODO:  not much we can do here...maybe?
                    PyErr_Print();
                    PyErr_Clear();
                }
                Py_DECREF(pyObj_exc);
                Py_INCREF(Py_False);
                barrier->set_value(Py_False);
                break;
            } else {
                if (barrier) {
                    barrier->set_value(nullptr);
                }
                pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized get operation passed in.");
                Py_XDECREF(pyObj_callback);
                Py_XDECREF(pyObj_errback);
                return nullptr;
            }
        }
    };
    Py_RETURN_NONE;
}

template<typename Response>
result*
create_base_result_from_mutation_operation_response(const char* key, const Response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);
    PyObject* pyObj_tmp = PyLong_FromUnsignedLongLong(resp.cas.value());
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

template<typename Response>
void
create_result_from_mutation_operation_response(const char* key,
                                               const Response& resp,
                                               PyObject* pyObj_callback,
                                               PyObject* pyObj_errback,
                                               std::shared_ptr<std::promise<PyObject*>> barrier,
                                               result* multi_result = nullptr)
{
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* pyObj_args = nullptr;
    PyObject* pyObj_kwargs = nullptr;
    PyObject* pyObj_exc = nullptr;
    PyObject* pyObj_func = nullptr;
    PyObject* pyObj_callback_res = nullptr;
    auto set_exception = false;

    if (resp.ctx.ec()) {
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "KV mutation operation error.");
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
        auto res = create_base_result_from_mutation_operation_response(key, resp);
        if (res != nullptr) {
            res->ec = resp.ctx.ec();
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
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "KV mutation operation error.");
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
            pycbc_set_python_exception(PycbcError::InternalSDKError, __FILE__, __LINE__, "Mutation operation callback failed.");
        }
        Py_DECREF(pyObj_args);
        Py_XDECREF(pyObj_kwargs);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }
    // CB_CB_CB_LOG_DEBUG("{}: create mutation callback completed", "PYCBC");
    PyGILState_Release(state);
}

template<typename Request>
void
do_mutation(connection& conn,
            Request& req,
            PyObject* pyObj_callback,
            PyObject* pyObj_errback,
            std::shared_ptr<std::promise<PyObject*>> barrier,
            result* multi_result = nullptr)
{
    using response_type = typename Request::response_type;
    Py_BEGIN_ALLOW_THREADS conn.cluster_->execute(
      req, [key = req.id.key(), pyObj_callback, pyObj_errback, barrier, multi_result](response_type resp) {
          create_result_from_mutation_operation_response(key.c_str(), resp, pyObj_callback, pyObj_errback, barrier, multi_result);
      });
    Py_END_ALLOW_THREADS
}

PyObject*
prepare_and_execute_mutation_op(struct mutation_options* options,
                                PyObject* pyObj_callback,
                                PyObject* pyObj_errback,
                                std::shared_ptr<std::promise<PyObject*>> barrier = nullptr,
                                result* multi_result = nullptr)
{
    // **DO NOT DECREF** these -- content from tuples are borrowed references!!
    PyObject* pyObj_flags = nullptr;
    PyObject* pyObj_value = nullptr;
    couchbase::core::utils::binary value;

    if (options->value) {
        pyObj_value = PyTuple_GET_ITEM(options->value, 0);
        pyObj_flags = PyTuple_GET_ITEM(options->value, 1);
        try {
            value = PyObject_to_binary(pyObj_value);
        } catch (const std::exception& e) {
            if (multi_result != nullptr) {
                PyObject* pyObj_exc = pycbc_build_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, e.what());
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
            if (barrier) {
                barrier->set_value(nullptr);
            }
            pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, e.what());
            Py_XDECREF(pyObj_callback);
            Py_XDECREF(pyObj_errback);
            return nullptr;
        }
    }

    switch (options->op_type) {
        case Operations::INSERT: {
            auto req = couchbase::core::operations::insert_request{ options->id };
            req.timeout = options->timeout_ms;
            req.value = value;
            req.flags = static_cast<uint32_t>(PyLong_AsLong(pyObj_flags));
            if (options->expiry > 0) {
                req.expiry = options->expiry;
            }
            if (nullptr != options->span) {
                req.parent_span = std::make_shared<pycbc::request_span>(options->span);
            }
            if (options->use_legacy_durability) {
                auto req_legacy_durability =
                  couchbase::core::operations::insert_request_with_legacy_durability{ req, options->persist_to, options->replicate_to };
                do_mutation(*(options->conn), req_legacy_durability, pyObj_callback, pyObj_errback, barrier, multi_result);
                break;
            }
            req.durability_level = options->durability_level;
            do_mutation(*(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
            break;
        }
        case Operations::UPSERT: {
            auto req = couchbase::core::operations::upsert_request{ options->id };
            req.timeout = options->timeout_ms;
            req.value = value;
            req.flags = static_cast<uint32_t>(PyLong_AsLong(pyObj_flags));
            if (options->expiry > 0) {
                req.expiry = options->expiry;
            }
            if (options->preserve_expiry) {
                req.preserve_expiry = options->preserve_expiry;
            }
            if (nullptr != options->span) {
                req.parent_span = std::make_shared<pycbc::request_span>(options->span);
            }
            if (options->use_legacy_durability) {
                auto req_legacy_durability =
                  couchbase::core::operations::upsert_request_with_legacy_durability{ req, options->persist_to, options->replicate_to };
                do_mutation(*(options->conn), req_legacy_durability, pyObj_callback, pyObj_errback, barrier, multi_result);
                break;
            }
            req.durability_level = options->durability_level;
            do_mutation(*(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
            break;
        }
        case Operations::REPLACE: {
            auto req = couchbase::core::operations::replace_request{ options->id };
            req.timeout = options->timeout_ms;
            req.cas = options->cas;
            req.value = value;
            req.flags = static_cast<uint32_t>(PyLong_AsLong(pyObj_flags));
            if (options->expiry > 0) {
                req.expiry = options->expiry;
            }
            if (options->preserve_expiry) {
                req.preserve_expiry = options->preserve_expiry;
            }
            if (nullptr != options->span) {
                req.parent_span = std::make_shared<pycbc::request_span>(options->span);
            }
            if (options->use_legacy_durability) {
                auto req_legacy_durability =
                  couchbase::core::operations::replace_request_with_legacy_durability{ req, options->persist_to, options->replicate_to };
                do_mutation(*(options->conn), req_legacy_durability, pyObj_callback, pyObj_errback, barrier, multi_result);
                break;
            }
            req.durability_level = options->durability_level;
            do_mutation(*(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
            break;
        }
        case Operations::REMOVE: {
            auto req = couchbase::core::operations::remove_request{ options->id };
            req.timeout = options->timeout_ms;
            req.cas = options->cas;
            if (nullptr != options->span) {
                req.parent_span = std::make_shared<pycbc::request_span>(options->span);
            }
            if (options->use_legacy_durability) {
                auto req_legacy_durability =
                  couchbase::core::operations::remove_request_with_legacy_durability{ req, options->persist_to, options->replicate_to };
                do_mutation(*(options->conn), req_legacy_durability, pyObj_callback, pyObj_errback, barrier, multi_result);
                break;
            }
            req.durability_level = options->durability_level;
            do_mutation(*(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
            break;
        }
        default: {
            if (multi_result != nullptr) {
                PyObject* pyObj_exc =
                  pycbc_build_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized mutation operation passed in.");
                if (-1 == PyDict_SetItemString(multi_result->dict, options->id.key().c_str(), pyObj_exc)) {
                    // TODO:  not much we can do here...maybe?
                    PyErr_Print();
                    PyErr_Clear();
                }
                Py_DECREF(pyObj_exc);
                Py_INCREF(Py_False);
                barrier->set_value(Py_False);
                break;
            }

            if (barrier) {
                barrier->set_value(nullptr);
            }
            pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized mutation operation passed in.");
            Py_XDECREF(pyObj_callback);
            Py_XDECREF(pyObj_errback);
            return nullptr;
        }
    }
    Py_RETURN_NONE;
}

struct read_options
get_read_options(PyObject* op_args)
{
    struct read_options opts {
    };

    PyObject* pyObj_span = PyDict_GetItemString(op_args, "span");
    if (pyObj_span != nullptr) {
        opts.span = pyObj_span;
    }

    PyObject* pyObj_expiry = PyDict_GetItemString(op_args, "expiry");
    if (pyObj_expiry != nullptr) {
        auto expiry = static_cast<uint32_t>(PyLong_AsUnsignedLong(pyObj_expiry));
        opts.expiry = expiry;
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

    PyObject* pyObj_lock_time = PyDict_GetItemString(op_args, "lock_time");
    if (pyObj_lock_time != nullptr) {
        auto lock_time = static_cast<uint32_t>(PyLong_AsUnsignedLong(pyObj_lock_time));
        opts.lock_time = lock_time;
    }

    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::key_value_timeout;
    PyObject* pyObj_timeout = PyDict_GetItemString(op_args, "timeout");
    if (pyObj_timeout != nullptr) {
        auto timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_timeout));
        timeout_ms = std::chrono::milliseconds(std::max(0ULL, timeout / 1000ULL));
        if (0 < timeout) {
            opts.timeout_ms = timeout_ms;
        }
    }

    PyObject* pyObj_with_expiry = PyDict_GetItemString(op_args, "with_expiry");
    opts.with_expiry = pyObj_with_expiry != nullptr && pyObj_with_expiry == Py_True ? true : false;

    return opts;
}

mutation_options
get_mutation_options(PyObject* op_args)
{
    struct mutation_options opts;

    PyObject* pyObj_span = PyDict_GetItemString(op_args, "span");
    if (pyObj_span != nullptr) {
        opts.span = pyObj_span;
    }

    PyObject* pyObj_expiry = PyDict_GetItemString(op_args, "expiry");
    if (pyObj_expiry != nullptr) {
        opts.expiry = static_cast<uint32_t>(PyLong_AsUnsignedLong(pyObj_expiry));
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

    PyObject* pyObj_preserve_expiry = PyDict_GetItemString(op_args, "preserve_expiry");
    opts.preserve_expiry = pyObj_preserve_expiry != nullptr && pyObj_preserve_expiry == Py_True ? true : false;

    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::key_value_timeout;
    PyObject* pyObj_timeout = PyDict_GetItemString(op_args, "timeout");
    if (pyObj_timeout != nullptr) {
        auto timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_timeout));
        timeout_ms = std::chrono::milliseconds(std::max(0ULL, timeout / 1000ULL));
        if (0 < timeout) {
            opts.timeout_ms = timeout_ms;
        }
    }

    PyObject* pyObj_durability = PyDict_GetItemString(op_args, "durability");
    if (pyObj_durability) {
        if (PyDict_Check(pyObj_durability)) {
            auto durability = PyObject_to_durability(pyObj_durability);
            opts.use_legacy_durability = true;
            opts.persist_to = durability.first;
            opts.replicate_to = durability.second;
        } else if (PyLong_Check(pyObj_durability)) {
            opts.durability_level = PyObject_to_durability_level(pyObj_durability);
        }
    }

    return opts;
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
    PyObject* pyObj_value = nullptr;
    PyObject* pyObj_op_args = nullptr;
    PyObject* pyObj_callback = nullptr;
    PyObject* pyObj_errback = nullptr;

    static const char* kw_list[] = { "conn", "bucket", "scope", "collection_name", "key", "op_type", "value", "op_args", nullptr };

    const char* kw_format = "O!ssssI|OO";
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
                                          &pyObj_op_args);
    if (!ret) {
        pycbc_set_python_exception(
          PycbcError::InvalidArgument, __FILE__, __LINE__, "Cannot perform kv operation.  Unable to parse args/kwargs.");
        return nullptr;
    }

    connection* conn = nullptr;

    conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    if (nullptr == conn) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, NULL_CONN_OBJECT);
        return nullptr;
    }

    // PyObjects that need to be around for the cxx client lambda
    // have their increment/decrement handled w/in the callback_context struct
    // struct callback_context callback_ctx = { pyObj_callback, pyObj_errback, pyObj_transcoder };
    pyObj_callback = PyDict_GetItemString(pyObj_op_args, "callback");
    pyObj_errback = PyDict_GetItemString(pyObj_op_args, "errback");
    Py_XINCREF(pyObj_callback);
    Py_XINCREF(pyObj_errback);

    PyObject* pyObj_op_response = nullptr;
    std::shared_ptr<std::promise<PyObject*>> barrier = nullptr;
    std::future<PyObject*> fut;
    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        barrier = std::make_shared<std::promise<PyObject*>>();
        fut = barrier->get_future();
    }

    switch (op_type) {
        case Operations::INSERT:
        case Operations::UPSERT:
        case Operations::REPLACE:
        case Operations::REMOVE: {
            auto opts = get_mutation_options(pyObj_op_args);
            opts.conn = conn;
            opts.id = couchbase::core::document_id{ bucket, scope, collection, key };
            opts.op_type = op_type;
            if (pyObj_value != nullptr) {
                opts.value = pyObj_value;
            }
            try {
                pyObj_op_response = prepare_and_execute_mutation_op(&opts, pyObj_callback, pyObj_errback, barrier);
            } catch (const std::system_error& e) {
                if (barrier) {
                    barrier->set_value(nullptr);
                }
                pycbc_set_python_exception(e.code(), __FILE__, __LINE__, e.what());
                Py_XDECREF(pyObj_callback);
                Py_XDECREF(pyObj_errback);
                pyObj_op_response = nullptr;
            }
            break;
        }
        case Operations::GET:
        case Operations::GET_PROJECTED:
        case Operations::GET_ANY_REPLICA:
        case Operations::GET_ALL_REPLICAS:
        case Operations::GET_AND_LOCK:
        case Operations::GET_AND_TOUCH:
        case Operations::TOUCH:
        case Operations::EXISTS:
        case Operations::UNLOCK: {
            auto opts = get_read_options(pyObj_op_args);
            opts.conn = conn;
            opts.id = couchbase::core::document_id{ bucket, scope, collection, key };
            PyObject* pyObj_project = PyDict_GetItemString(pyObj_op_args, "project");
            if (pyObj_project != nullptr || opts.with_expiry) {
                op_type = Operations::GET_PROJECTED;
                opts.project = pyObj_project;
            }
            opts.op_type = op_type;
            try {
                pyObj_op_response = prepare_and_execute_read_op(&opts, pyObj_callback, pyObj_errback, barrier);
            } catch (const std::system_error& e) {
                if (barrier) {
                    barrier->set_value(nullptr);
                }
                pycbc_set_python_exception(e.code(), __FILE__, __LINE__, e.what());
                Py_XDECREF(pyObj_callback);
                Py_XDECREF(pyObj_errback);
                pyObj_op_response = nullptr;
            }
            break;
        }
        default: {
            pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized KV operation passed in.");
            if (barrier) {
                barrier->set_value(nullptr);
            }
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
    return pyObj_op_response;
}

PyObject*
handle_kv_multi_op([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
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
          PycbcError::InvalidArgument, __FILE__, __LINE__, "Cannot perform kv multi operation.  Unable to parse args/kwargs.");
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
            PyObject* pyObj_op_response = nullptr;
            if (PyUnicode_Check(pyObj_doc_key)) {
                k = std::string(PyUnicode_AsUTF8(pyObj_doc_key));
            }
            auto barrier = std::make_shared<std::promise<PyObject*>>();
            auto f = barrier->get_future();
            if (PyDict_Check(pyObj_op_dict) && !k.empty()) {
                PyObject* pyObj_value = PyDict_GetItemString(pyObj_op_dict, "value");

                switch (op_type) {
                    case Operations::INSERT:
                    case Operations::UPSERT:
                    case Operations::REPLACE:
                    case Operations::REMOVE: {
                        auto opts = get_mutation_options(pyObj_op_dict);
                        opts.conn = conn;
                        opts.id = couchbase::core::document_id{ bucket, scope, collection, k };
                        opts.op_type = op_type;
                        if (pyObj_value != nullptr) {
                            opts.value = pyObj_value;
                        }

                        try {
                            pyObj_op_response = prepare_and_execute_mutation_op(&opts, nullptr, nullptr, barrier, multi_result);
                        } catch (const std::system_error& e) {
                            PyObject* pyObj_exc = pycbc_build_exception(e.code(), __FILE__, __LINE__, e.what());
                            barrier->set_value(pyObj_exc);
                        }
                        break;
                    }
                    case Operations::GET:
                    case Operations::GET_PROJECTED:
                    case Operations::GET_ANY_REPLICA:
                    case Operations::GET_ALL_REPLICAS:
                    case Operations::GET_AND_LOCK:
                    case Operations::GET_AND_TOUCH:
                    case Operations::TOUCH:
                    case Operations::EXISTS:
                    case Operations::UNLOCK: {
                        auto opts = get_read_options(pyObj_op_dict);
                        opts.conn = conn;
                        opts.id = couchbase::core::document_id{ bucket, scope, collection, k };
                        PyObject* pyObj_project = PyDict_GetItemString(pyObj_op_dict, "project");
                        if (pyObj_project != nullptr || opts.with_expiry) {
                            op_type = Operations::GET_PROJECTED;
                            opts.project = pyObj_project;
                        }
                        opts.op_type = op_type;
                        try {
                            pyObj_op_response = prepare_and_execute_read_op(&opts, nullptr, nullptr, barrier, multi_result);
                        } catch (const std::system_error& e) {
                            PyObject* pyObj_exc = pycbc_build_exception(e.code(), __FILE__, __LINE__, e.what());
                            barrier->set_value(pyObj_exc);
                        }
                        break;
                    }
                    default: {
                        PyObject* pyObj_exc =
                          pycbc_build_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized KV operation passed in.");
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
