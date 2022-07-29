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

#include "view_index_management.hxx"
#include "../exceptions.hxx"
#include <core/management/design_document.hxx>
#include <core/design_document_namespace.hxx>

PyObject*
build_design_doc(couchbase::core::management::views::design_document dd)
{
    PyObject* pyObj_dd = PyDict_New();

    PyObject* pyObj_tmp = PyUnicode_FromString(dd.rev.c_str());
    if (-1 == PyDict_SetItemString(pyObj_dd, "rev", pyObj_tmp)) {
        Py_XDECREF(pyObj_dd);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(dd.name.c_str());
    if (-1 == PyDict_SetItemString(pyObj_dd, "name", pyObj_tmp)) {
        Py_DECREF(pyObj_dd);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    std::string ns = "development";
    if (dd.ns == couchbase::core::design_document_namespace::production) {
        ns = "production";
    }

    pyObj_tmp = PyUnicode_FromString(ns.c_str());
    if (-1 == PyDict_SetItemString(pyObj_dd, "namespace", pyObj_tmp)) {
        Py_DECREF(pyObj_dd);
        Py_XDECREF(pyObj_tmp);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    PyObject* pyObj_views = PyDict_New();
    for (const auto [name, view] : dd.views) {
        PyObject* pyObj_view = PyDict_New();

        if (view.map.has_value()) {
            pyObj_tmp = PyUnicode_FromString(view.map.value().c_str());
            if (-1 == PyDict_SetItemString(pyObj_view, "map", pyObj_tmp)) {
                Py_DECREF(pyObj_dd);
                Py_XDECREF(pyObj_view);
                Py_XDECREF(pyObj_views);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);
        }

        if (view.reduce.has_value()) {
            pyObj_tmp = PyUnicode_FromString(view.reduce.value().c_str());
            if (-1 == PyDict_SetItemString(pyObj_view, "reduce", pyObj_tmp)) {
                Py_DECREF(pyObj_dd);
                Py_DECREF(pyObj_view);
                Py_XDECREF(pyObj_views);
                Py_XDECREF(pyObj_tmp);
                return nullptr;
            }
            Py_DECREF(pyObj_tmp);
        }

        if (-1 == PyDict_SetItemString(pyObj_views, name.c_str(), pyObj_view)) {
            Py_DECREF(pyObj_dd);
            Py_DECREF(pyObj_view);
            Py_XDECREF(pyObj_views);
            Py_XDECREF(pyObj_tmp);
            return nullptr;
        }
        Py_DECREF(pyObj_view);
    }

    if (-1 == PyDict_SetItemString(pyObj_dd, "views", pyObj_views)) {
        Py_DECREF(pyObj_dd);
        Py_XDECREF(pyObj_views);
        return nullptr;
    }
    Py_DECREF(pyObj_views);

    return pyObj_dd;
}

template<typename T>
result*
create_result_from_view_index_mgmt_response([[maybe_unused]] const T& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);

    return res;
}

template<>
result*
create_result_from_view_index_mgmt_response<couchbase::core::operations::management::view_index_get_all_response>(
  const couchbase::core::operations::management::view_index_get_all_response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);

    PyObject* pyObj_design_documents = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& design_document : resp.design_documents) {
        PyObject* pyObj_design_doc = build_design_doc(design_document);
        if (pyObj_design_doc == nullptr) {
            Py_XDECREF(pyObj_result);
            Py_XDECREF(pyObj_design_documents);
            return nullptr;
        }
        PyList_Append(pyObj_design_documents, pyObj_design_doc);
        Py_DECREF(pyObj_design_doc);
    }

    if (-1 == PyDict_SetItemString(res->dict, "design_documents", pyObj_design_documents)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_design_documents);
        return nullptr;
    }
    Py_DECREF(pyObj_design_documents);
    return res;
}

template<>
result*
create_result_from_view_index_mgmt_response<couchbase::core::operations::management::view_index_get_response>(
  const couchbase::core::operations::management::view_index_get_response& resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);

    PyObject* pyObj_design_doc = build_design_doc(resp.document);
    if (-1 == PyDict_SetItemString(res->dict, "design_document", pyObj_design_doc)) {
        Py_XDECREF(pyObj_result);
        Py_XDECREF(pyObj_design_doc);
        return nullptr;
    }
    Py_DECREF(pyObj_design_doc);

    return res;
}

template<typename Response>
void
create_result_from_view_index_mgmt_op_response(const Response& resp,
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
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "Error doing view index mgmt operation.", "ViewIndexMgmt");
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
        auto res = create_result_from_view_index_mgmt_response(resp);
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
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "View index mgmt operation error.");
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

couchbase::core::management::views::design_document
get_design_doc(PyObject* pyObj_dd)
{
    PyObject* pyObj_name = PyDict_GetItemString(pyObj_dd, "name");
    auto name = std::string(PyUnicode_AsUTF8(pyObj_name));

    PyObject* pyObj_namespace = PyDict_GetItemString(pyObj_dd, "namespace");
    auto namespace_ = std::string(PyUnicode_AsUTF8(pyObj_namespace));

    auto ns = couchbase::core::design_document_namespace::development;
    if (namespace_.compare("production") == 0) {
        ns = couchbase::core::design_document_namespace::production;
    }

    std::map<std::string, couchbase::core::management::views::design_document::view> views{};
    PyObject* pyObj_views = PyDict_GetItemString(pyObj_dd, "views");
    if (pyObj_views && PyDict_Check(pyObj_views)) {
        PyObject *pyObj_key, *pyObj_value;
        Py_ssize_t pos = 0;

        // PyObj_key and pyObj_value are borrowed references
        while (PyDict_Next(pyObj_views, &pos, &pyObj_key, &pyObj_value)) {
            std::string k;
            if (PyUnicode_Check(pyObj_key)) {
                k = std::string(PyUnicode_AsUTF8(pyObj_key));
            }
            if (PyDict_Check(pyObj_value) && !k.empty()) {
                couchbase::core::management::views::design_document::view view{ k };

                PyObject* pyObj_tmp = PyDict_GetItemString(pyObj_value, "map");
                if (pyObj_tmp != nullptr) {
                    auto map = std::string(PyUnicode_AsUTF8(pyObj_tmp));
                    view.map = map;
                }

                pyObj_tmp = PyDict_GetItemString(pyObj_value, "reduce");
                if (pyObj_tmp != nullptr) {
                    auto reduce = std::string(PyUnicode_AsUTF8(pyObj_tmp));
                    view.reduce = reduce;
                }
                views.emplace(k, view);
            }
        }
    }
    couchbase::core::management::views::design_document dd{};
    dd.name = name;
    dd.ns = ns;

    if (views.size() > 0) {
        dd.views = views;
    }

    PyObject* pyObj_rev = PyDict_GetItemString(pyObj_dd, "rev");
    if (pyObj_rev != nullptr) {
        auto rev = std::string(PyUnicode_AsUTF8(pyObj_rev));
        dd.rev = rev;
    }

    return dd;
}

template<typename T>
T
get_view_mgmt_req_base(PyObject* op_args)
{
    T req{};

    PyObject* pyObj_bucket_name = PyDict_GetItemString(op_args, "bucket_name");
    auto bucket_name = std::string(PyUnicode_AsUTF8(pyObj_bucket_name));
    req.bucket_name = bucket_name;

    PyObject* pyObj_client_context_id = PyDict_GetItemString(op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    return req;
}

couchbase::core::operations::management::view_index_get_all_request
get_view_index_get_all_req(PyObject* op_args)
{
    auto req = get_view_mgmt_req_base<couchbase::core::operations::management::view_index_get_all_request>(op_args);

    PyObject* pyObj_namespace = PyDict_GetItemString(op_args, "namespace");
    auto namespace_ = std::string(PyUnicode_AsUTF8(pyObj_namespace));

    auto ns = couchbase::core::design_document_namespace::development;
    if (namespace_.compare("production") == 0) {
        ns = couchbase::core::design_document_namespace::production;
    }
    req.ns = ns;

    return req;
}

couchbase::core::operations::management::view_index_get_request
get_view_index_get_req(PyObject* op_args)
{
    auto req = get_view_mgmt_req_base<couchbase::core::operations::management::view_index_get_request>(op_args);
    PyObject* pyObj_document_name = PyDict_GetItemString(op_args, "document_name");
    auto document_name = std::string(PyUnicode_AsUTF8(pyObj_document_name));
    req.document_name = document_name;

    PyObject* pyObj_namespace = PyDict_GetItemString(op_args, "namespace");
    auto namespace_ = std::string(PyUnicode_AsUTF8(pyObj_namespace));

    auto ns = couchbase::core::design_document_namespace::development;
    if (namespace_.compare("production") == 0) {
        ns = couchbase::core::design_document_namespace::production;
    }
    req.ns = ns;

    return req;
}

couchbase::core::operations::management::view_index_drop_request
get_view_index_drop_req(PyObject* op_args)
{
    auto req = get_view_mgmt_req_base<couchbase::core::operations::management::view_index_drop_request>(op_args);
    PyObject* pyObj_document_name = PyDict_GetItemString(op_args, "document_name");
    auto document_name = std::string(PyUnicode_AsUTF8(pyObj_document_name));
    req.document_name = document_name;

    PyObject* pyObj_namespace = PyDict_GetItemString(op_args, "namespace");
    auto namespace_ = std::string(PyUnicode_AsUTF8(pyObj_namespace));

    auto ns = couchbase::core::design_document_namespace::development;
    if (namespace_.compare("production") == 0) {
        ns = couchbase::core::design_document_namespace::production;
    }
    req.ns = ns;

    return req;
}

couchbase::core::operations::management::view_index_upsert_request
get_view_index_upsert_req(PyObject* op_args)
{
    auto req = get_view_mgmt_req_base<couchbase::core::operations::management::view_index_upsert_request>(op_args);
    PyObject* pyObj_design_doc = PyDict_GetItemString(op_args, "design_docucment");
    if (pyObj_design_doc != nullptr) {
        auto design_doc = get_design_doc(pyObj_design_doc);
        req.document = design_doc;
    }
    return req;
}

template<typename Request>
PyObject*
do_view_index_mgmt_op(connection& conn,
                      Request& req,
                      PyObject* pyObj_callback,
                      PyObject* pyObj_errback,
                      std::shared_ptr<std::promise<PyObject*>> barrier)
{
    using response_type = typename Request::response_type;
    Py_BEGIN_ALLOW_THREADS conn.cluster_->execute(req, [pyObj_callback, pyObj_errback, barrier](response_type resp) {
        create_result_from_view_index_mgmt_op_response(resp, pyObj_callback, pyObj_errback, barrier);
    });
    Py_END_ALLOW_THREADS Py_RETURN_NONE;
}

PyObject*
handle_view_index_mgmt_op(connection* conn, struct view_index_mgmt_options* options, PyObject* pyObj_callback, PyObject* pyObj_errback)
{
    PyObject* res = nullptr;
    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();
    switch (options->op_type) {
        case ViewIndexManagementOperations::UPSERT_INDEX: {
            auto req = get_view_index_upsert_req(options->op_args);
            req.timeout = options->timeout_ms;

            res = do_view_index_mgmt_op<couchbase::core::operations::management::view_index_upsert_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case ViewIndexManagementOperations::GET_INDEX: {
            auto req = get_view_index_get_req(options->op_args);
            req.timeout = options->timeout_ms;

            res = do_view_index_mgmt_op<couchbase::core::operations::management::view_index_get_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case ViewIndexManagementOperations::DROP_INDEX: {
            auto req = get_view_index_drop_req(options->op_args);
            req.timeout = options->timeout_ms;

            res = do_view_index_mgmt_op<couchbase::core::operations::management::view_index_drop_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        case ViewIndexManagementOperations::GET_ALL_INDEXES: {
            auto req = get_view_index_get_all_req(options->op_args);
            req.timeout = options->timeout_ms;

            res = do_view_index_mgmt_op<couchbase::core::operations::management::view_index_get_all_request>(
              *conn, req, pyObj_callback, pyObj_errback, barrier);
            break;
        }
        default: {
            pycbc_set_python_exception(
              PycbcError::InvalidArgument, __FILE__, __LINE__, "Unrecognized view index mgmt operation passed in.");
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
add_view_index_mgmt_ops_enum(PyObject* pyObj_module, PyObject* pyObj_enum_class)
{
    PyObject* pyObj_enum_values = PyUnicode_FromString(ViewIndexManagementOperations::ALL_OPERATIONS());
    PyObject* pyObj_enum_name = PyUnicode_FromString("ViewIndexManagementOperations");
    // PyTuple_Pack returns new reference, need to Py_DECREF values provided
    PyObject* pyObj_args = PyTuple_Pack(2, pyObj_enum_name, pyObj_enum_values);
    Py_DECREF(pyObj_enum_name);
    Py_DECREF(pyObj_enum_values);

    PyObject* pyObj_kwargs = PyDict_New();
    PyObject_SetItem(pyObj_kwargs, PyUnicode_FromString("module"), PyModule_GetNameObject(pyObj_module));
    PyObject* pyObj_mgmt_operations = PyObject_Call(pyObj_enum_class, pyObj_args, pyObj_kwargs);
    Py_DECREF(pyObj_args);
    Py_DECREF(pyObj_kwargs);

    if (PyModule_AddObject(pyObj_module, "view_index_mgmt_operations", pyObj_mgmt_operations) < 0) {
        // only need to Py_DECREF on failure to add when using PyModule_AddObject()
        Py_XDECREF(pyObj_mgmt_operations);
        return;
    }
}
