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

// Until CXXCBC-82 is resolved
template<>
void
create_result_from_get_operation_response<couchbase::operations::exists_response>(const char* key,
                                                                                  const couchbase::operations::exists_response& resp,
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

    if (resp.ctx.ec.value() && resp.ctx.ec.value() != 101) {
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
            pycbc_set_python_exception(PycbcError::InternalSDKError, __FILE__, __LINE__, "KV read operation callback failed.");
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
            couchbase::operations::get_request req{ options->id };
            req.timeout = options->timeout_ms;
            do_get<couchbase::operations::get_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
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
                        barrier->set_value(nullptr);
                        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Project must be a list of strings.");
                        Py_XDECREF(pyObj_callback);
                        Py_XDECREF(pyObj_errback);
                        return nullptr;
                    }
                }
            }

            couchbase::operations::get_projected_request req{ options->id };
            req.timeout = options->timeout_ms;
            req.with_expiry = !!options->with_expiry;
            req.projections = projections;
            do_get<couchbase::operations::get_projected_request>(
              *(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
            break;
        }
        case Operations::GET_AND_TOUCH: {
            couchbase::operations::get_and_touch_request req{ options->id };
            req.expiry = options->expiry;
            req.timeout = options->timeout_ms;
            do_get<couchbase::operations::get_and_touch_request>(
              *(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
            break;
        }
        case Operations::GET_AND_LOCK: {
            couchbase::operations::get_and_lock_request req{ options->id };
            req.lock_time = options->lock_time;
            req.timeout = options->timeout_ms;
            do_get<couchbase::operations::get_and_lock_request>(
              *(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
            break;
        }
        case Operations::EXISTS: {
            couchbase::operations::exists_request req{ options->id };
            req.timeout = options->timeout_ms;
            do_get<couchbase::operations::exists_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
            break;
        }
        case Operations::TOUCH: {
            couchbase::operations::touch_request req{ options->id };
            req.expiry = options->expiry;
            req.timeout = options->timeout_ms;
            do_get<couchbase::operations::touch_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
            break;
        }
        case Operations::UNLOCK: {
            couchbase::operations::unlock_request req{ options->id };
            req.cas = options->cas;
            req.timeout = options->timeout_ms;
            do_get<couchbase::operations::unlock_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
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
                barrier->set_value(nullptr);
                pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized get operation passed in.");
                Py_XDECREF(pyObj_callback);
                Py_XDECREF(pyObj_errback);
                return nullptr;
            }
        }
    };
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

    if (resp.ctx.ec.value()) {
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
    } else {
        auto res = create_base_result_from_mutation_operation_response(key, resp);
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
    }

    if (!set_exception && pyObj_func != nullptr) {
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
    // LOG_INFO("{}: create mutation callback completed", "PYCBC");
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
                                std::shared_ptr<std::promise<PyObject*>> barrier,
                                result* multi_result = nullptr)
{
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

    switch (options->op_type) {
        case Operations::INSERT: {
            couchbase::operations::insert_request req{ options->id, value };
            req.flags = static_cast<uint32_t>(PyLong_AsLong(pyObj_flags));
            req.timeout = options->timeout_ms;
            req.expiry = options->expiry;
            req.durability_level = durability_level;
            do_mutation<couchbase::operations::insert_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
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
            do_mutation<couchbase::operations::upsert_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
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
            do_mutation<couchbase::operations::replace_request>(
              *(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
            break;
        }
        case Operations::REMOVE: {
            couchbase::operations::remove_request req{ options->id };
            req.timeout = options->timeout_ms;
            req.cas = options->cas;
            req.durability_level = durability_level;
            do_mutation<couchbase::operations::remove_request>(*(options->conn), req, pyObj_callback, pyObj_errback, barrier, multi_result);
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

            barrier->set_value(nullptr);
            pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized mutation operation passed in.");
            Py_XDECREF(pyObj_callback);
            Py_XDECREF(pyObj_errback);
            return nullptr;
        }
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

    PyObject* pyObj_op_response = nullptr;
    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();

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
            pyObj_op_response = prepare_and_execute_mutation_op(&opts, pyObj_callback, pyObj_errback, barrier);
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
            pyObj_op_response = prepare_and_execute_read_op(&opts, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        default: {
            pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized KV operation passed in.");
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

    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::key_value_timeout;
    PyObject* pyObj_timeout = PyDict_GetItemString(op_args, "timeout");
    if (pyObj_timeout != nullptr) {
        auto timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_timeout));
        auto timeout_ms = std::chrono::milliseconds(std::max(0ULL, timeout / 1000ULL));
        if (0 < timeout) {
            opts.timeout_ms = timeout_ms;
        }
    }

    return opts;
}

struct mutation_options
get_mutation_options(PyObject* op_args)
{

    struct mutation_options opts {
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

    PyObject* pyObj_preserve_expiry = PyDict_GetItemString(op_args, "preserve_expiry");
    if (pyObj_preserve_expiry != nullptr) {
        if (pyObj_preserve_expiry == Py_True) {
            opts.preserve_expiry = true;
        } else {
            opts.preserve_expiry = false;
        }
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
            opts.durability = durability;
        }
    }

    return opts;
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
                        opts.op_type = op_type;
                        if (pyObj_value != nullptr) {
                            opts.value = pyObj_value;
                        }

                        couchbase::document_id id{ bucket, scope, collection, k };
                        opts.conn = conn;
                        opts.id = id;

                        pyObj_op_response = prepare_and_execute_mutation_op(&opts, nullptr, nullptr, barrier, multi_result);
                        break;
                    }
                    case Operations::GET:
                    case Operations::GET_PROJECTED:
                    case Operations::GET_AND_LOCK:
                    case Operations::GET_AND_TOUCH:
                    case Operations::TOUCH:
                    case Operations::EXISTS:
                    case Operations::UNLOCK: {
                        auto opts = get_read_options(pyObj_op_dict);
                        opts.op_type = op_type;

                        auto with_expiry = false;
                        PyObject* pyObj_with_expiry = PyDict_GetItemString(pyObj_op_dict, "with_expiry");
                        if (pyObj_with_expiry != nullptr) {
                            if (pyObj_with_expiry == Py_True) {
                                with_expiry = true;
                            } else {
                                with_expiry = false;
                            }
                        }
                        opts.with_expiry = with_expiry;

                        PyObject* pyObj_project = PyDict_GetItemString(pyObj_op_dict, "project");
                        if (pyObj_project != nullptr || with_expiry) {
                            op_type = Operations::GET_PROJECTED;
                            opts.project = pyObj_project;
                        }

                        couchbase::document_id id{ bucket, scope, collection, k };
                        opts.conn = conn;
                        opts.id = id;

                        pyObj_op_response = prepare_and_execute_read_op(&opts, nullptr, nullptr, barrier, multi_result);
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
