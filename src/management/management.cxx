
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

#include "management.hxx"
#include "../exceptions.hxx"

template<typename T>
result*
create_result_from_mgmt_response(const T& resp)
{
    PyObject* result_obj = create_result_obj();
    result* res = reinterpret_cast<result*>(result_obj);
    return res;
}

template<>
result*
create_result_from_mgmt_response<couchbase::core::operations::management::cluster_describe_response>(
  const couchbase::core::operations::management::cluster_describe_response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);
    PyObject* pyObj_tmp = nullptr;

    PyObject* pyObj_nodes = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& node : resp.info.nodes) {
        PyObject* pyObj_node = PyDict_New();

        pyObj_tmp = PyUnicode_FromString(node.uuid.c_str());
        if (-1 == PyDict_SetItemString(pyObj_node, "uuid", pyObj_tmp)) {
            Py_XDECREF(pyObj_nodes);
            Py_XDECREF(pyObj_node);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(node.otp_node.c_str());
        if (-1 == PyDict_SetItemString(pyObj_node, "otp_node", pyObj_tmp)) {
            Py_XDECREF(pyObj_nodes);
            Py_DECREF(pyObj_node);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(node.status.c_str());
        if (-1 == PyDict_SetItemString(pyObj_node, "status", pyObj_tmp)) {
            Py_XDECREF(pyObj_nodes);
            Py_DECREF(pyObj_node);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(node.hostname.c_str());
        if (-1 == PyDict_SetItemString(pyObj_node, "hostname", pyObj_tmp)) {
            Py_XDECREF(pyObj_nodes);
            Py_DECREF(pyObj_node);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(node.os.c_str());
        if (-1 == PyDict_SetItemString(pyObj_node, "os", pyObj_tmp)) {
            Py_XDECREF(pyObj_nodes);
            Py_DECREF(pyObj_node);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(node.version.c_str());
        if (-1 == PyDict_SetItemString(pyObj_node, "version", pyObj_tmp)) {
            Py_XDECREF(pyObj_nodes);
            Py_DECREF(pyObj_node);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        PyObject* pyObj_node_svcs = PyList_New(static_cast<Py_ssize_t>(0));
        for (auto const& svc : node.services) {
            PyObject* pyObj_node_svc = nullptr;
            pyObj_node_svc = PyUnicode_FromString(node.version.c_str());
            if (pyObj_node_svc) {
                PyList_Append(pyObj_node_svcs, pyObj_node_svc);
                Py_DECREF(pyObj_node_svc);
            }
        }

        if (-1 == PyDict_SetItemString(pyObj_node, "services", pyObj_node_svcs)) {
            Py_XDECREF(pyObj_nodes);
            Py_DECREF(pyObj_node);
            Py_XDECREF(pyObj_node_svcs);
            return nullptr;
        }
        Py_DECREF(pyObj_node_svcs);
        PyList_Append(pyObj_nodes, pyObj_node);
        Py_DECREF(pyObj_node);
    }

    if (-1 == PyDict_SetItemString(res->dict, "nodes", pyObj_nodes)) {
        Py_XDECREF(pyObj_nodes);
        return nullptr;
    }
    Py_DECREF(pyObj_nodes);

    PyObject* pyObj_buckets = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& bucket : resp.info.buckets) {
        PyObject* pyObj_bucket = PyDict_New();

        pyObj_tmp = PyUnicode_FromString(bucket.uuid.c_str());
        if (-1 == PyDict_SetItemString(pyObj_bucket, "uuid", pyObj_tmp)) {
            Py_XDECREF(pyObj_buckets);
            Py_XDECREF(pyObj_bucket);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(bucket.name.c_str());
        if (-1 == PyDict_SetItemString(pyObj_bucket, "name", pyObj_tmp)) {
            Py_XDECREF(pyObj_buckets);
            Py_DECREF(pyObj_bucket);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);

        PyList_Append(pyObj_buckets, pyObj_bucket);
        Py_DECREF(pyObj_bucket);
    }

    if (-1 == PyDict_SetItemString(res->dict, "buckets", pyObj_buckets)) {
        Py_DECREF(pyObj_buckets);
        return nullptr;
    }
    Py_DECREF(pyObj_buckets);

    PyObject* pyObj_svc_type_set = PySet_New(nullptr);
    for (auto const& svc_type : resp.info.services) {
        std::string service_type = service_type_to_str(svc_type);
        pyObj_tmp = PyUnicode_FromString(service_type.c_str());
        if (-1 == PySet_Add(pyObj_svc_type_set, pyObj_tmp)) {
            Py_XDECREF(pyObj_svc_type_set);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_tmp);
    }

    if (-1 == PyDict_SetItemString(res->dict, "service_types", pyObj_svc_type_set)) {
        Py_DECREF(pyObj_svc_type_set);
        return nullptr;
    }
    Py_DECREF(pyObj_svc_type_set);

    return res;
}

template<typename T>
void
create_result_from_mgmt_op_response(const T& resp,
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
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "Error doing collection mgmt operation.", "ClusterMgmt");
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
        auto res = create_result_from_mgmt_response(resp);
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
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "Cluster mgmt operation error.");
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
do_mgmt_op(connection& conn,
           Request& req,
           PyObject* pyObj_callback,
           PyObject* pyObj_errback,
           std::shared_ptr<std::promise<PyObject*>> barrier)
{
    using response_type = typename Request::response_type;
    Py_BEGIN_ALLOW_THREADS conn.cluster_->execute(req, [pyObj_callback, pyObj_errback, barrier](response_type resp) {
        create_result_from_mgmt_op_response(resp, pyObj_callback, pyObj_errback, barrier);
    });
    Py_END_ALLOW_THREADS Py_RETURN_NONE;
}

PyObject*
handle_mgmt_op([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    // need these for all operations
    PyObject* pyObj_conn = nullptr;
    ManagementOperations::OperationType mgmt_op = ManagementOperations::UNKNOWN;
    int op_type = 0;
    PyObject* pyObj_callback = nullptr;
    PyObject* pyObj_errback = nullptr;
    PyObject* op_args = nullptr;

    uint64_t timeout = 0;

    static const char* kw_list[] = { "conn", "mgmt_op", "op_type", "callback", "errback", "timeout", "op_args", nullptr };

    const char* kw_format = "O!II|OOLO";
    int ret = PyArg_ParseTupleAndKeywords(args,
                                          kwargs,
                                          kw_format,
                                          const_cast<char**>(kw_list),
                                          &PyCapsule_Type,
                                          &pyObj_conn,
                                          &mgmt_op,
                                          &op_type,
                                          &pyObj_callback,
                                          &pyObj_errback,
                                          &timeout,
                                          &op_args);
    if (!ret) {
        pycbc_set_python_exception(
          PycbcError::InvalidArgument, __FILE__, __LINE__, "Cannot perform management operation.  Unable to parse args/kwargs.");
        return nullptr;
    }

    connection* conn = nullptr;
    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::management_timeout;

    conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    if (nullptr == conn) {
        pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, NULL_CONN_OBJECT);
        return nullptr;
    }
    PyErr_Clear();
    if (0 < timeout) {
        timeout_ms = std::chrono::milliseconds(std::max(0ULL, timeout / 1000ULL));
    }

    // PyObjects that need to be around for the cxx client lambda
    // have their increment/decrement handled w/in the callback_context struct
    // struct callback_context callback_ctx = { callback, errback };
    Py_XINCREF(pyObj_callback);
    Py_XINCREF(pyObj_errback);
    PyObject* res = nullptr;

    switch (mgmt_op) {
        case ManagementOperations::CLUSTER: {
            auto barrier = std::make_shared<std::promise<PyObject*>>();
            auto f = barrier->get_future();

            auto op = static_cast<ClusterManagementOperations::OperationType>(op_type);
            if (op == ClusterManagementOperations::GET_CLUSTER_INFO) {
                couchbase::core::operations::management::cluster_describe_request req{};
                req.timeout = timeout_ms;
                res = do_mgmt_op<couchbase::core::operations::management::cluster_describe_request>(
                  *conn, req, pyObj_callback, pyObj_errback, barrier);
            } else if (op == ClusterManagementOperations::ENABLE_DP) {
                couchbase::core::operations::management::cluster_developer_preview_enable_request req{};
                req.timeout = timeout_ms;
                res = do_mgmt_op<couchbase::core::operations::management::cluster_developer_preview_enable_request>(
                  *conn, req, pyObj_callback, pyObj_errback, barrier);
            }

            if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
                PyObject* ret = nullptr;
                Py_BEGIN_ALLOW_THREADS ret = f.get();
                Py_END_ALLOW_THREADS return ret;
            }
            break;
        }
        case ManagementOperations::BUCKET: {
            struct bucket_mgmt_options opts = { op_args, static_cast<BucketManagementOperations::OperationType>(op_type), timeout_ms };
            res = handle_bucket_mgmt_op(conn, &opts, pyObj_callback, pyObj_errback);
            break;
        }
        case ManagementOperations::COLLECTION: {
            // struct callback_context callback_ctx = { pyObj_callback, pyObj_errback };
            struct collection_mgmt_options opts = { op_args,
                                                    static_cast<CollectionManagementOperations::OperationType>(op_type),
                                                    timeout_ms };
            res = handle_collection_mgmt_op(conn, &opts, pyObj_callback, pyObj_errback);
            break;
        }
        case ManagementOperations::USER: {
            struct user_mgmt_options opts = { op_args, static_cast<UserManagementOperations::OperationType>(op_type), timeout_ms };
            res = handle_user_mgmt_op(conn, &opts, pyObj_callback, pyObj_errback);
            break;
        }
        case ManagementOperations::QUERY_INDEX: {
            struct query_index_mgmt_options opts = { op_args,
                                                     static_cast<QueryIndexManagementOperations::OperationType>(op_type),
                                                     timeout_ms };
            res = handle_query_index_mgmt_op(conn, &opts, pyObj_callback, pyObj_errback);
            break;
        }
        case ManagementOperations::ANALYTICS: {
            struct analytics_mgmt_options opts = { op_args,
                                                   static_cast<AnalyticsManagementOperations::OperationType>(op_type),
                                                   timeout_ms };
            res = handle_analytics_mgmt_op(conn, &opts, pyObj_callback, pyObj_errback);
            break;
        }
        case ManagementOperations::SEARCH_INDEX: {
            struct search_index_mgmt_options opts = { op_args,
                                                      static_cast<SearchIndexManagementOperations::OperationType>(op_type),
                                                      timeout_ms };
            res = handle_search_index_mgmt_op(conn, &opts, pyObj_callback, pyObj_errback);
            break;
        }
        case ManagementOperations::VIEW_INDEX: {
            struct view_index_mgmt_options opts = { op_args,
                                                    static_cast<ViewIndexManagementOperations::OperationType>(op_type),
                                                    timeout_ms };
            res = handle_view_index_mgmt_op(conn, &opts, pyObj_callback, pyObj_errback);
            break;
        }
        case ManagementOperations::EVENTING_FUNCTION: {
            struct eventing_function_mgmt_options opts = { op_args,
                                                           static_cast<EventingFunctionManagementOperations::OperationType>(op_type),
                                                           timeout_ms };
            res = handle_eventing_function_mgmt_op(conn, &opts, pyObj_callback, pyObj_errback);
            break;
        }
        default: {
            pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized management operation passed in.");
            Py_XDECREF(pyObj_callback);
            Py_XDECREF(pyObj_errback);
            break;
        }
    };
    return res;
}

void
add_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class)
{
    PyObject* pyObj_enum_values = PyUnicode_FromString(ManagementOperations::ALL_OPERATIONS());
    PyObject* pyObj_enum_name = PyUnicode_FromString("ManagementOperations");
    // PyTuple_Pack returns new reference, need to Py_DECREF values provided
    PyObject* pyObj_args = PyTuple_Pack(2, pyObj_enum_name, pyObj_enum_values);
    Py_DECREF(pyObj_enum_name);
    Py_DECREF(pyObj_enum_values);

    PyObject* pyObj_kwargs = PyDict_New();
    PyObject_SetItem(pyObj_kwargs, PyUnicode_FromString("module"), PyModule_GetNameObject(pyObj_module));
    PyObject* pyObj_mgmt_operations = PyObject_Call(pyObj_enum_class, pyObj_args, pyObj_kwargs);
    Py_DECREF(pyObj_args);
    Py_DECREF(pyObj_kwargs);

    if (PyModule_AddObject(pyObj_module, "mgmt_operations", pyObj_mgmt_operations) < 0) {
        // only need to Py_DECREF on failure to add when using PyModule_AddObject()
        Py_XDECREF(pyObj_mgmt_operations);
        return;
    }
}

void
add_cluster_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class)
{
    PyObject* pyObj_enum_values = PyUnicode_FromString(ClusterManagementOperations::ALL_OPERATIONS());
    PyObject* pyObj_enum_name = PyUnicode_FromString("ClusterManagementOperations");
    // PyTuple_Pack returns new reference, need to Py_DECREF values provided
    PyObject* pyObj_args = PyTuple_Pack(2, pyObj_enum_name, pyObj_enum_values);
    Py_DECREF(pyObj_enum_name);
    Py_DECREF(pyObj_enum_values);

    PyObject* pyObj_kwargs = PyDict_New();
    PyObject_SetItem(pyObj_kwargs, PyUnicode_FromString("module"), PyModule_GetNameObject(pyObj_module));
    PyObject* pyObj_mgmt_operations = PyObject_Call(pyObj_enum_class, pyObj_args, pyObj_kwargs);
    Py_DECREF(pyObj_args);
    Py_DECREF(pyObj_kwargs);

    if (PyModule_AddObject(pyObj_module, "cluster_mgmt_operations", pyObj_mgmt_operations) < 0) {
        // only need to Py_DECREF on failure to add when using PyModule_AddObject()
        Py_XDECREF(pyObj_mgmt_operations);
        return;
    }
}
