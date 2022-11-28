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

#include "views.hxx"
#include "exceptions.hxx"
#include "result.hxx"
#include "tracing.hxx"
#include <core/view_scan_consistency.hxx>
#include <core/view_sort_order.hxx>
#include <core/management/design_document.hxx>
#include <core/design_document_namespace.hxx>

result*
create_result_from_view_response(couchbase::core::operations::document_view_response resp)
{
    PyObject* pyObj_result = create_result_obj();
    result* res = reinterpret_cast<result*>(pyObj_result);
    res->ec = resp.ctx.ec;

    PyObject* pyObj_tmp = nullptr;
    PyObject* pyObj_payload = PyDict_New();

    if (resp.error.has_value()) {
        PyObject* pyObj_error = PyDict_New();
        pyObj_tmp = PyUnicode_FromString(resp.error.value().code.c_str());
        if (-1 == PyDict_SetItemString(pyObj_error, "code", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(resp.error.value().message.c_str());
        if (-1 == PyDict_SetItemString(pyObj_error, "message", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        if (-1 == PyDict_SetItemString(pyObj_payload, "error", pyObj_error)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_error);
    }

    PyObject* pyObj_meta = PyDict_New();
    if (resp.meta.total_rows.has_value()) {
        pyObj_tmp = PyLong_FromUnsignedLongLong(resp.meta.total_rows.value());
        if (-1 == PyDict_SetItemString(pyObj_meta, "total_rows", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }
    if (resp.meta.debug_info.has_value()) {
        pyObj_tmp = PyUnicode_FromString(resp.meta.debug_info.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_meta, "debug_info", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }

    if (-1 == PyDict_SetItemString(pyObj_payload, "metadata", pyObj_meta)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_meta);

    // if (resp.rows.size() > 0) {
    //     PyObject* pyObj_rows = PyList_New(static_cast<Py_ssize_t>(0));
    //     for (auto const& row : resp.rows) {
    //         PyObject* pyObj_row = PyDict_New();

    //         if (row.id.has_value()) {
    //             pyObj_tmp = PyUnicode_FromString(row.id.value().c_str());
    //             if (-1 == PyDict_SetItemString(pyObj_row, "id", pyObj_tmp)) {
    //                 PyErr_Print();
    //                 PyErr_Clear();
    //             }
    //             Py_DECREF(pyObj_tmp);
    //         }

    //         pyObj_tmp = PyUnicode_FromString(row.key.c_str());
    //         if (-1 == PyDict_SetItemString(pyObj_row, "key", pyObj_tmp)) {
    //             PyErr_Print();
    //             PyErr_Clear();
    //         }
    //         Py_DECREF(pyObj_tmp);

    //         pyObj_tmp = PyUnicode_FromString(row.value.c_str());
    //         if (-1 == PyDict_SetItemString(pyObj_row, "value", pyObj_tmp)) {
    //             PyErr_Print();
    //             PyErr_Clear();
    //         }
    //         Py_DECREF(pyObj_tmp);

    //         if (-1 == PyList_Append(pyObj_rows, pyObj_row)) {
    //             PyErr_Print();
    //             PyErr_Clear();
    //         }
    //     }

    //     if (-1 == PyDict_SetItemString(pyObj_payload, "rows", pyObj_rows)) {
    //         PyErr_Print();
    //         PyErr_Clear();
    //     }
    //     Py_DECREF(pyObj_rows);
    // }

    if (-1 == PyDict_SetItemString(res->dict, RESULT_VALUE, pyObj_payload)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_payload);

    return res;
}

void
create_view_result(couchbase::core::operations::document_view_response resp,
                   std::shared_ptr<rows_queue<PyObject*>> rows,
                   PyObject* pyObj_callback,
                   PyObject* pyObj_errback)
{

    auto set_exception = false;
    PyObject* pyObj_exc = nullptr;
    PyObject* pyObj_args = NULL;
    PyObject* pyObj_func = NULL;
    PyObject* pyObj_callback_res = nullptr;

    PyGILState_STATE state = PyGILState_Ensure();

    if (resp.ctx.ec.value()) {
        pyObj_exc = build_exception_from_context(resp.ctx, __FILE__, __LINE__, "Error doing views operation.");
        // lets clear any errors
        PyErr_Clear();
        rows->put(pyObj_exc);
    } else {
        for (auto const& row : resp.rows) {
            PyObject* pyObj_row = PyDict_New();
            PyObject* pyObj_tmp = nullptr;

            if (row.id.has_value()) {
                pyObj_tmp = PyUnicode_FromString(row.id.value().c_str());
                if (-1 == PyDict_SetItemString(pyObj_row, "id", pyObj_tmp)) {
                    PyErr_Print();
                    PyErr_Clear();
                }
                Py_DECREF(pyObj_tmp);
            }

            pyObj_tmp = PyUnicode_FromString(row.key.c_str());
            if (-1 == PyDict_SetItemString(pyObj_row, "key", pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_DECREF(pyObj_tmp);

            pyObj_tmp = PyUnicode_FromString(row.value.c_str());
            if (-1 == PyDict_SetItemString(pyObj_row, "value", pyObj_tmp)) {
                PyErr_Print();
                PyErr_Clear();
            }
            Py_DECREF(pyObj_tmp);

            rows->put(pyObj_row);
        }

        auto res = create_result_from_view_response(resp);
        if (res == nullptr || PyErr_Occurred() != nullptr) {
            set_exception = true;
        } else {
            // None indicates done (i.e. raise StopIteration)
            Py_INCREF(Py_None);
            rows->put(Py_None);
            rows->put(reinterpret_cast<PyObject*>(res));
        }
    }

    if (set_exception) {
        pyObj_exc = pycbc_build_exception(PycbcError::UnableToBuildResult, __FILE__, __LINE__, "Views operation error.");
        rows->put(pyObj_exc);
    }

    // This is for txcouchbase -- let it knows we're done w/ the query request
    if (pyObj_callback != nullptr) {
        pyObj_func = pyObj_callback;
        pyObj_args = PyTuple_New(1);
        PyTuple_SET_ITEM(pyObj_args, 0, PyBool_FromLong(static_cast<long>(1)));
    }

    if (pyObj_func != nullptr) {
        pyObj_callback_res = PyObject_CallObject(pyObj_func, pyObj_args);
        if (pyObj_callback_res) {
            Py_DECREF(pyObj_callback_res);
        } else {
            pycbc_set_python_exception(PycbcError::InternalSDKError, __FILE__, __LINE__, "Views complete callback failed.");
        }
        Py_DECREF(pyObj_args);
        Py_XDECREF(pyObj_callback);
        Py_XDECREF(pyObj_errback);
    }

    PyGILState_Release(state);
}

couchbase::core::operations::document_view_request
get_view_request(PyObject* op_args)
{
    PyObject* pyObj_bucket_name = PyDict_GetItemString(op_args, "bucket_name");
    auto bucket_name = std::string(PyUnicode_AsUTF8(pyObj_bucket_name));

    PyObject* pyObj_document_name = PyDict_GetItemString(op_args, "document_name");
    auto document_name = std::string(PyUnicode_AsUTF8(pyObj_document_name));

    PyObject* pyObj_view_name = PyDict_GetItemString(op_args, "view_name");
    auto view_name = std::string(PyUnicode_AsUTF8(pyObj_view_name));

    couchbase::core::operations::document_view_request req{ bucket_name, document_name, view_name };

    PyObject* pyObj_namespace = PyDict_GetItemString(op_args, "namespace");
    if (pyObj_namespace != nullptr) {
        auto ns = couchbase::core::design_document_namespace::development;
        if (pyObj_namespace == Py_False) {
            ns = couchbase::core::design_document_namespace::production;
        }
        req.ns = ns;
    }

    PyObject* pyObj_limit = PyDict_GetItemString(op_args, "limit");
    if (pyObj_limit != nullptr) {
        auto limit = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_limit));
        req.limit = limit;
    }

    PyObject* pyObj_skip = PyDict_GetItemString(op_args, "skip");
    if (pyObj_skip != nullptr) {
        auto skip = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_skip));
        req.skip = skip;
    }

    PyObject* pyObj_scan_consistency = PyDict_GetItemString(op_args, "scan_consistency");
    if (pyObj_scan_consistency != nullptr) {
        auto scan_consistency = std::string(PyUnicode_AsUTF8(pyObj_scan_consistency));
        if (scan_consistency.compare("ok") == 0) {
            req.consistency = couchbase::core::view_scan_consistency::not_bounded;
        } else if (scan_consistency.compare("update_after") == 0) {
            req.consistency = couchbase::core::view_scan_consistency::update_after;
        } else if (scan_consistency.compare("false") == 0) {
            req.consistency = couchbase::core::view_scan_consistency::request_plus;
        }
    }

    PyObject* pyObj_keys = PyDict_GetItemString(op_args, "keys");
    if (pyObj_keys != nullptr && PyList_Check(pyObj_keys)) {
        size_t nkeys = static_cast<size_t>(PyList_GET_SIZE(pyObj_keys));
        std::vector<std::string> keys{};
        size_t ii;
        for (ii = 0; ii < nkeys; ++ii) {
            PyObject* pyObj_key = PyList_GetItem(pyObj_keys, ii);
            auto key = std::string(PyUnicode_AsUTF8(pyObj_key));
            keys.push_back(key);
        }

        if (keys.size() > 0) {
            req.keys = keys;
        }
    }

    PyObject* pyObj_key = PyDict_GetItemString(op_args, "key");
    if (pyObj_key != nullptr) {
        auto key = std::string(PyUnicode_AsUTF8(pyObj_key));
        req.key = key;
    }

    PyObject* pyObj_start_key = PyDict_GetItemString(op_args, "start_key");
    if (pyObj_start_key != nullptr) {
        auto start_key = std::string(PyUnicode_AsUTF8(pyObj_start_key));
        req.start_key = start_key;
    }

    PyObject* pyObj_end_key = PyDict_GetItemString(op_args, "end_key");
    if (pyObj_end_key != nullptr) {
        auto end_key = std::string(PyUnicode_AsUTF8(pyObj_end_key));
        req.end_key = end_key;
    }

    PyObject* pyObj_start_key_doc_id = PyDict_GetItemString(op_args, "start_key_doc_id");
    if (pyObj_start_key_doc_id != nullptr) {
        auto start_key_doc_id = std::string(PyUnicode_AsUTF8(pyObj_start_key_doc_id));
        req.start_key_doc_id = start_key_doc_id;
    }

    PyObject* pyObj_end_key_doc_id = PyDict_GetItemString(op_args, "end_key_doc_id");
    if (pyObj_end_key_doc_id != nullptr) {
        auto end_key_doc_id = std::string(PyUnicode_AsUTF8(pyObj_end_key_doc_id));
        req.end_key_doc_id = end_key_doc_id;
    }

    PyObject* pyObj_inclusive_end = PyDict_GetItemString(op_args, "inclusive_end");
    if (pyObj_inclusive_end != nullptr) {
        if (pyObj_inclusive_end == Py_True) {
            req.inclusive_end = true;
        } else {
            req.inclusive_end = false;
        }
    }

    PyObject* pyObj_reduce = PyDict_GetItemString(op_args, "reduce");
    if (pyObj_reduce != nullptr) {
        if (pyObj_reduce == Py_True) {
            req.reduce = true;
        } else {
            req.reduce = false;
        }
    }

    PyObject* pyObj_group = PyDict_GetItemString(op_args, "group");
    if (pyObj_group != nullptr) {
        if (pyObj_group == Py_True) {
            req.group = true;
        } else {
            req.group = false;
        }
    }

    PyObject* pyObj_group_level = PyDict_GetItemString(op_args, "group_level");
    if (pyObj_group_level != nullptr) {
        auto group_level = static_cast<uint32_t>(PyLong_AsUnsignedLong(pyObj_group_level));
        req.group_level = group_level;
    }

    PyObject* pyObj_debug = PyDict_GetItemString(op_args, "debug");
    if (pyObj_debug != nullptr && pyObj_debug == Py_True) {
        req.debug = true;
    }

    PyObject* pyObj_order = PyDict_GetItemString(op_args, "order");
    if (pyObj_order != nullptr) {
        auto order = std::string(PyUnicode_AsUTF8(pyObj_order));
        if (order.compare("false") == 0) {
            req.order = couchbase::core::view_sort_order::ascending;
        } else if (order.compare("true") == 0) {
            req.order = couchbase::core::view_sort_order::descending;
        }
    }

    PyObject* pyObj_query_string = PyDict_GetItemString(op_args, "query_string");
    if (pyObj_query_string != nullptr && PyList_Check(pyObj_query_string)) {
        size_t nqstrings = static_cast<size_t>(PyList_GET_SIZE(pyObj_query_string));
        std::vector<std::string> query_string{};
        size_t ii;
        for (ii = 0; ii < nqstrings; ++ii) {
            PyObject* pyObj_q_string = PyList_GetItem(pyObj_query_string, ii);
            auto q_string = std::string(PyUnicode_AsUTF8(pyObj_q_string));
            query_string.push_back(q_string);
        }

        if (query_string.size() > 0) {
            req.query_string = query_string;
        }
    }

    PyObject* pyObj_client_context_id = PyDict_GetItemString(op_args, "client_context_id");
    if (pyObj_client_context_id != nullptr) {
        auto client_context_id = std::string(PyUnicode_AsUTF8(pyObj_client_context_id));
        req.client_context_id = client_context_id;
    }

    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::view_timeout;
    PyObject* pyObj_timeout = PyDict_GetItemString(op_args, "timeout");
    if (pyObj_timeout != nullptr) {
        auto timeout = static_cast<uint64_t>(PyLong_AsUnsignedLongLong(pyObj_timeout));
        if (0 < timeout) {
            timeout_ms = std::chrono::milliseconds(std::max(0ULL, timeout / 1000ULL));
        }
    }
    req.timeout = timeout_ms;

    return req;
}

streamed_result*
handle_view_query([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    // need these for all operations
    PyObject* pyObj_conn = nullptr;
    // optional
    PyObject* pyObj_op_args = nullptr;
    PyObject* pyObj_serializer = nullptr;
    PyObject* pyObj_callback = nullptr;
    PyObject* pyObj_errback = nullptr;
    PyObject* pyObj_row_callback = nullptr;
    PyObject* pyObj_span = nullptr;

    static const char* kw_list[] = { "conn", "op_args", "serializer", "callback", "errback", "row_callback", "span", nullptr };

    const char* kw_format = "O!|OOOOOO";
    int ret = PyArg_ParseTupleAndKeywords(args,
                                          kwargs,
                                          kw_format,
                                          const_cast<char**>(kw_list),
                                          &PyCapsule_Type,
                                          &pyObj_conn,
                                          &pyObj_op_args,
                                          &pyObj_serializer,
                                          &pyObj_callback,
                                          &pyObj_errback,
                                          &pyObj_row_callback,
                                          &pyObj_span);
    if (!ret) {
        PyErr_Print();
        PyErr_SetString(PyExc_ValueError, "Unable to parse arguments");
        return nullptr;
    }

    connection* conn = nullptr;
    conn = reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
    if (nullptr == conn) {
        PyErr_SetString(PyExc_ValueError, "passed null connection");
        return nullptr;
    }
    PyErr_Clear();

    auto req = get_view_request(pyObj_op_args);

    // timeout is always set either to default, or timeout provided in options
    streamed_result* streamed_res = create_streamed_result_obj(req.timeout.value());

    if (nullptr != pyObj_span) {
        req.parent_span = std::make_shared<pycbc::request_span>(pyObj_span);
    }
    // TODO:  let the couchbase++ streaming stabilize a bit more...
    // req.row_callback = [rows = streamed_res->rows](std::string&& row) {
    //     PyGILState_STATE state = PyGILState_Ensure();
    //     PyObject* pyObj_row = PyBytes_FromStringAndSize(row.c_str(), row.length());
    //     rows->put(pyObj_row);
    //     PyGILState_Release(state);
    //     return couchbase::core::utils::json::stream_control::next_row;
    // };

    // we need the callback, errback, and logic to all stick around, so...
    // use XINCREF b/c they _could_ be NULL
    Py_XINCREF(pyObj_errback);
    Py_XINCREF(pyObj_callback);

    {
        Py_BEGIN_ALLOW_THREADS conn->cluster_->execute(
          req, [rows = streamed_res->rows, pyObj_callback, pyObj_errback](couchbase::core::operations::document_view_response resp) {
              create_view_result(resp, rows, pyObj_callback, pyObj_errback);
          });
        Py_END_ALLOW_THREADS
    }
    return streamed_res;
}
