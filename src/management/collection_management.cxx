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

#include "collection_management.hxx"
#include "../exceptions.hxx"

template<typename T>
result*
create_result_from_collection_mgmt_response([[maybe_unused]] const T& resp)
{
    PyObject* result_obj = create_result_obj();
    result* res = reinterpret_cast<result*>(result_obj);
    return res;
}

template<>
result*
create_result_from_collection_mgmt_response<couchbase::core::operations::management::scope_get_all_response>(
  const couchbase::core::operations::management::scope_get_all_response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);
    PyObject* pyObj_scopes = PyList_New(static_cast<Py_ssize_t>(0));
    PyObject* pyObj_tmp = nullptr;

    for (auto const& scope : resp.manifest.scopes) {
        PyObject* pyObj_scope_spec = PyDict_New();
        pyObj_tmp = PyUnicode_FromString(scope.name.c_str());
        if (-1 == PyDict_SetItemString(pyObj_scope_spec, "name", pyObj_tmp)) {
            Py_XDECREF(pyObj_scopes);
            Py_XDECREF(pyObj_scope_spec);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        PyObject* pyObj_collections = PyList_New(static_cast<Py_ssize_t>(0));
        for (auto const& collection : scope.collections) {
            PyObject* pyObj_collection_spec = PyDict_New();
            pyObj_tmp = PyUnicode_FromString(collection.name.c_str());
            if (-1 == PyDict_SetItemString(pyObj_collection_spec, "name", pyObj_tmp)) {
                Py_XDECREF(pyObj_scopes);
                Py_XDECREF(pyObj_collections);
                Py_DECREF(pyObj_scope_spec);
                Py_DECREF(pyObj_collection_spec);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);

            pyObj_tmp = PyUnicode_FromString(scope.name.c_str());
            if (-1 == PyDict_SetItemString(pyObj_collection_spec, "scope_name", pyObj_tmp)) {
                Py_XDECREF(pyObj_scopes);
                Py_XDECREF(pyObj_collections);
                Py_DECREF(pyObj_scope_spec);
                Py_DECREF(pyObj_collection_spec);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);

            pyObj_tmp = PyLong_FromUnsignedLong(collection.max_expiry);
            if (-1 == PyDict_SetItemString(pyObj_collection_spec, "max_expiry", pyObj_tmp)) {
                Py_XDECREF(pyObj_scopes);
                Py_XDECREF(pyObj_collections);
                Py_DECREF(pyObj_scope_spec);
                Py_DECREF(pyObj_collection_spec);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);

            PyList_Append(pyObj_collections, pyObj_collection_spec);
            Py_DECREF(pyObj_collection_spec);
        }

        if (-1 == PyDict_SetItemString(pyObj_scope_spec, "collections", pyObj_collections)) {
            Py_XDECREF(pyObj_scopes);
            Py_XDECREF(pyObj_collections);
            Py_DECREF(pyObj_scope_spec);
            return nullptr;
        }
        Py_DECREF(pyObj_collections);

        PyList_Append(pyObj_scopes, pyObj_scope_spec);
        Py_DECREF(pyObj_scope_spec);
    }
    if (-1 == PyDict_SetItemString(res->dict, "scopes", pyObj_scopes)) {
        Py_XDECREF(pyObj_scopes);
        return nullptr;
    }
    Py_DECREF(pyObj_scopes);

    return res;
}

template<typename T>
void
create_result_from_collection_mgmt_op_response(const T& resp,
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
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "Error doing collection mgmt operation.", "CollectionMgmt");
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
        auto res = create_result_from_collection_mgmt_response(resp);
        if (res == nullptr) {
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
    }

    if (!set_exception && pyObj_func != nullptr) {
        pyObj_callback_res = PyObject_Call(pyObj_func, pyObj_args, pyObj_kwargs);
        if (pyObj_callback_res) {
            Py_DECREF(pyObj_callback_res);
        } else {
            PyErr_Print();
            // @TODO: how to catch exception here?
        }
        Py_DECREF(pyObj_args);
        Py_XDECREF(pyObj_kwargs);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }

    PyGILState_Release(state);
}

template<typename Request>
PyObject*
do_collection_mgmt_op(connection& conn,
                      Request& req,
                      PyObject* pyObj_callback,
                      PyObject* pyObj_errback,
                      std::shared_ptr<std::promise<PyObject*>> barrier)
{
    using response_type = typename Request::response_type;
    Py_BEGIN_ALLOW_THREADS conn.cluster_->execute(req, [pyObj_callback, pyObj_errback, barrier](response_type resp) {
        create_result_from_collection_mgmt_op_response(resp, pyObj_callback, pyObj_errback, barrier);
    });
    Py_END_ALLOW_THREADS Py_RETURN_NONE;
}

PyObject*
handle_collection_mgmt_op(connection* conn, struct collection_mgmt_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback)
{
    PyObject* res = nullptr;
    PyObject* pyObj_bucket_name = PyDict_GetItemString(options->op_args, "bucket_name");
    auto bucket_name = std::string(PyUnicode_AsUTF8(pyObj_bucket_name));
    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();

    switch (options->op_type) {
        case CollectionManagementOperations::CREATE_SCOPE: {
            PyObject* pyObj_scope_name = PyDict_GetItemString(options->op_args, "scope_name");
            auto scope_name = std::string(PyUnicode_AsUTF8(pyObj_scope_name));
            couchbase::core::operations::management::scope_create_request req{};
            req.bucket_name = bucket_name;
            req.scope_name = scope_name;
            req.timeout = options->timeout_ms;

            res = do_collection_mgmt_op<couchbase::core::operations::management::scope_create_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case CollectionManagementOperations::DROP_SCOPE: {
            PyObject* pyObj_scope_name = PyDict_GetItemString(options->op_args, "scope_name");
            auto scope_name = std::string(PyUnicode_AsUTF8(pyObj_scope_name));
            couchbase::core::operations::management::scope_drop_request req{};
            req.bucket_name = bucket_name;
            req.scope_name = scope_name;
            req.timeout = options->timeout_ms;

            res = do_collection_mgmt_op<couchbase::core::operations::management::scope_drop_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case CollectionManagementOperations::GET_ALL_SCOPES: {
            couchbase::core::operations::management::scope_get_all_request req{};
            req.bucket_name = bucket_name;
            req.timeout = options->timeout_ms;

            res = do_collection_mgmt_op<couchbase::core::operations::management::scope_get_all_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case CollectionManagementOperations::CREATE_COLLECTION: {
            PyObject* pyObj_scope_name = PyDict_GetItemString(options->op_args, "scope_name");
            auto scope_name = std::string(PyUnicode_AsUTF8(pyObj_scope_name));
            PyObject* pyObj_collection_name = PyDict_GetItemString(options->op_args, "collection_name");
            auto collection_name = std::string(PyUnicode_AsUTF8(pyObj_collection_name));
            // optional
            PyObject* pyObj_max_expiry = PyDict_GetItemString(options->op_args, "max_expiry");

            couchbase::core::operations::management::collection_create_request req{};
            req.bucket_name = bucket_name;
            req.scope_name = scope_name;
            req.collection_name = collection_name;
            if (pyObj_max_expiry != nullptr) {
                req.max_expiry = static_cast<uint32_t>(PyLong_AsUnsignedLong(pyObj_max_expiry));
            }
            req.timeout = options->timeout_ms;

            res = do_collection_mgmt_op<couchbase::core::operations::management::collection_create_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case CollectionManagementOperations::DROP_COLLECTION: {
            PyObject* pyObj_scope_name = PyDict_GetItemString(options->op_args, "scope_name");
            auto scope_name = std::string(PyUnicode_AsUTF8(pyObj_scope_name));
            PyObject* pyObj_collection_name = PyDict_GetItemString(options->op_args, "collection_name");
            auto collection_name = std::string(PyUnicode_AsUTF8(pyObj_collection_name));

            couchbase::core::operations::management::collection_drop_request req{};
            req.bucket_name = bucket_name;
            req.scope_name = scope_name;
            req.collection_name = collection_name;
            req.timeout = options->timeout_ms;

            res = do_collection_mgmt_op<couchbase::core::operations::management::collection_drop_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        default: {
            pycbc_set_python_exception(
              PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized collection mgmt operation passed in.");
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
add_collection_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class)
{
    PyObject* pyObj_enum_values = PyUnicode_FromString(CollectionManagementOperations::ALL_OPERATIONS());
    PyObject* pyObj_enum_name = PyUnicode_FromString("CollectionManagementOperations");
    // PyTuple_Pack returns new reference, need to Py_DECREF values provided
    PyObject* pyObj_args = PyTuple_Pack(2, pyObj_enum_name, pyObj_enum_values);
    Py_DECREF(pyObj_enum_name);
    Py_DECREF(pyObj_enum_values);

    PyObject* pyObj_kwargs = PyDict_New();
    PyObject_SetItem(pyObj_kwargs, PyUnicode_FromString("module"), PyModule_GetNameObject(pyObj_module));
    PyObject* pyObj_mgmt_operations = PyObject_Call(pyObj_enum_class, pyObj_args, pyObj_kwargs);
    Py_DECREF(pyObj_args);
    Py_DECREF(pyObj_kwargs);

    if (PyModule_AddObject(pyObj_module, "collection_mgmt_operations", pyObj_mgmt_operations) < 0) {
        // only need to Py_DECREF on failure to add when using PyModule_AddObject()
        Py_XDECREF(pyObj_mgmt_operations);
        return;
    }
}
