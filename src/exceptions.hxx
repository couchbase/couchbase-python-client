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

#pragma once

#include <exception>

#include "client.hxx"
#include <couchbase/manager_error_context.hxx>

#define DISPATCHED_TO "last_dispatched_to"
#define DISPATCHED_FROM "last_dispatched_from"
#define RETRY_ATTEMPTS "retry_attempts"
#define RETRY_REASONS "retry_reasons"

#define CONTEXT_TYPE "context_type"
#define CONTEXT_DETAIL_TYPE "context_detail_type"
#define CLIENT_CONTEXT_ID "client_context_id"

#define KV_DOCUMENT_ID "key"
#define KV_DOCUMENT_BUCKET "bucket_name"
#define KV_DOCUMENT_SCOPE "scope_name"
#define KV_DOCUMENT_COLLECTION "collection_name"
#define KV_OPAQUE "opaque"
#define KV_STATUS_CODE "status_code"
#define KV_ERROR_MAP_INFO "error_map_info"
#define KV_EXTENDED_ERROR_INFO "extended_error_info"

#define MGMT_CONTENT "content"
#define MGMT_PATH "path"
#define MGMT_STATUS "http_status"

#define HTTP_STATUS "http_status"
#define HTTP_METHOD "method"
#define HTTP_PATH "path"
#define HTTP_BODY "http_body"

#define QUERY_FIRST_ERROR_CODE "first_error_code"
#define QUERY_FIRST_ERROR_MSG "first_error_message"
#define QUERY_STATEMENT "statement"
#define QUERY_PARAMETERS "parameters"

#define SEARCH_INDEX_NAME "index_name"
#define SEARCH_QUERY "query"
#define SEARCH_PARAMETERS "parameters"

#define SUBDOC_PATH "first_error_path"
#define SUBDOC_INDEX "first_error_index"
#define SUBDOC_DELETED "deleted"

#define VIEW_DDOC_NAME "design_document_name"
#define VIEW_NAME "view_name"
#define VIEW_QUERY "query_string"

#define NULL_CONN_OBJECT "Received a null connection."

struct exception_base {
    PyObject_HEAD std::error_code ec;
    PyObject* error_context = nullptr;
    PyObject* exc_info = nullptr;
};

int
pycbc_exception_base_type_init(PyObject** ptr);

exception_base*
create_exception_base_obj();

std::string
retry_reason_to_string(couchbase::retry_reason reason);

// start - needed for Pycbc error code
enum class PycbcError {
    InvalidArgument = 3,
    InternalSDKError = 5000,
    HTTPError = 5001,
    UnsuccessfulOperation,
    UnableToBuildResult,
    CallbackUnsuccessful
};

namespace std
{
template<>
struct is_error_code_enum<PycbcError> : true_type {
};
} // namespace std

std::error_code
make_error_code(PycbcError ec);
// end - needed for Pycbc error code

PyObject*
build_kv_error_map_info(couchbase::key_value_error_map_info error_info);

void
build_kv_error_context(const couchbase::key_value_error_context& ctx, PyObject* pyObj_ctx);

/*

Build exceptions via error context

*/
template<class T>
PyObject*
build_base_error_context(const T& ctx)
{
    PyObject* pyObj_error_context = PyDict_New();

    PyObject* pyObj_tmp = nullptr;
    if (ctx.last_dispatched_to.has_value()) {
        pyObj_tmp = PyUnicode_FromString(ctx.last_dispatched_to.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_error_context, DISPATCHED_TO, pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }
    if (ctx.last_dispatched_from.has_value()) {
        pyObj_tmp = PyUnicode_FromString(ctx.last_dispatched_from.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_error_context, DISPATCHED_FROM, pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }

    pyObj_tmp = PyLong_FromLong(ctx.retry_attempts);
    if (-1 == PyDict_SetItemString(pyObj_error_context, RETRY_ATTEMPTS, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    PyObject* rr_set = PySet_New(nullptr);
    for (auto rr : ctx.retry_reasons) {
        std::string reason = retry_reason_to_string(rr);
        pyObj_tmp = PyUnicode_FromString(reason.c_str());
        if (-1 == PySet_Add(rr_set, pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }

    Py_ssize_t set_size = PySet_Size(rr_set);
    if (set_size > 0) {
        if (-1 == PyDict_SetItemString(pyObj_error_context, RETRY_REASONS, rr_set)) {
            PyErr_Print();
            PyErr_Clear();
        }
    }
    Py_DECREF(rr_set);

    return pyObj_error_context;
}

template<typename Context>
inline PyObject*
build_base_error_context_new(const Context& ctx)
{
    PyObject* pyObj_error_context = PyDict_New();

    PyObject* pyObj_tmp = nullptr;
    if (ctx.last_dispatched_to().has_value()) {
        pyObj_tmp = PyUnicode_FromString(ctx.last_dispatched_to().value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_error_context, DISPATCHED_TO, pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }
    if (ctx.last_dispatched_from().has_value()) {
        pyObj_tmp = PyUnicode_FromString(ctx.last_dispatched_from().value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_error_context, DISPATCHED_FROM, pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }

    pyObj_tmp = PyLong_FromLong(ctx.retry_attempts());
    if (-1 == PyDict_SetItemString(pyObj_error_context, RETRY_ATTEMPTS, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    PyObject* rr_set = PySet_New(nullptr);
    for (auto rr : ctx.retry_reasons()) {
        std::string reason = retry_reason_to_string(rr);
        pyObj_tmp = PyUnicode_FromString(reason.c_str());
        if (-1 == PySet_Add(rr_set, pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }

    Py_ssize_t set_size = PySet_Size(rr_set);
    if (set_size > 0) {
        if (-1 == PyDict_SetItemString(pyObj_error_context, RETRY_REASONS, rr_set)) {
            PyErr_Print();
            PyErr_Clear();
        }
    }
    Py_DECREF(rr_set);

    return pyObj_error_context;
}

template<typename T>
void
build_base_http_error_context(const T& ctx, PyObject* pyObj_error_context)
{
    PyObject* pyObj_tmp = nullptr;
    pyObj_tmp = PyUnicode_FromString(ctx.client_context_id.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, CLIENT_CONTEXT_ID, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(ctx.method.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, HTTP_METHOD, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(ctx.path.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, HTTP_PATH, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromLong(static_cast<uint32_t>(ctx.http_status));
    if (-1 == PyDict_SetItemString(pyObj_error_context, HTTP_STATUS, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(ctx.http_body.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, HTTP_BODY, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);
}

template<typename T>
PyObject*
build_exception_from_context(const T& ctx,
                             const char* file = __FILE__,
                             int line = __LINE__,
                             std::string error_msg = std::string(),
                             std::string context_detail_type = std::string())
{
    exception_base* exc = create_exception_base_obj();
    exc->ec = ctx.ec();
    exc->error_context = build_base_error_context(ctx);

    return reinterpret_cast<PyObject*>(exc);
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::key_value_error_context& ctx,
                             const char* file,
                             int line,
                             std::string error_msg,
                             std::string context_detail_type)
{
    exception_base* exc = create_exception_base_obj();
    exc->ec = ctx.ec();
    PyObject* pyObj_error_context = build_base_error_context_new(ctx);

    build_kv_error_context(ctx, pyObj_error_context);

    std::string context_type = "KeyValueErrorContext";
    PyObject* pyObj_tmp = PyUnicode_FromString(context_type.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, CONTEXT_TYPE, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    exc->error_context = pyObj_error_context;

    PyObject* pyObj_exc_info = PyDict_New();

    PyObject* pyObj_cinfo = Py_BuildValue("(s,i)", file, line);
    if (-1 == PyDict_SetItemString(pyObj_exc_info, "cinfo", pyObj_cinfo)) {
        PyErr_Print();
        Py_XDECREF(pyObj_cinfo);
    }
    Py_DECREF(pyObj_cinfo);

    if (!error_msg.empty()) {
        PyObject* pyObj_error_msg = PyUnicode_FromString(error_msg.c_str());
        if (-1 == PyDict_SetItemString(pyObj_exc_info, "error_message", pyObj_error_msg)) {
            PyErr_Print();
            Py_XDECREF(pyObj_error_msg);
        }
        Py_DECREF(pyObj_error_msg);
    }

    exc->exc_info = pyObj_exc_info;

    return reinterpret_cast<PyObject*>(exc);
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::manager_error_context& ctx,
                             const char* file,
                             int line,
                             std::string error_msg,
                             std::string context_detail_type)
{
    exception_base* exc = create_exception_base_obj();
    exc->ec = ctx.ec();
    PyObject* pyObj_error_context = build_base_error_context_new(ctx);

    PyObject* pyObj_tmp = nullptr;
    pyObj_tmp = PyUnicode_FromString(ctx.client_context_id().c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, CLIENT_CONTEXT_ID, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(ctx.content().c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, MGMT_CONTENT, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(ctx.path().c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, MGMT_PATH, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromLong(static_cast<uint32_t>(ctx.http_status()));
    if (-1 == PyDict_SetItemString(pyObj_error_context, MGMT_STATUS, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    std::string context_type = "ManagementErrorContext";
    pyObj_tmp = PyUnicode_FromString(context_type.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, CONTEXT_TYPE, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    if (!context_detail_type.empty()) {
        pyObj_tmp = PyUnicode_FromString(context_detail_type.c_str());
        if (-1 == PyDict_SetItemString(pyObj_error_context, CONTEXT_DETAIL_TYPE, pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }

    exc->error_context = pyObj_error_context;

    PyObject* pyObj_exc_info = PyDict_New();

    PyObject* pyObj_cinfo = Py_BuildValue("(s,i)", file, line);
    if (-1 == PyDict_SetItemString(pyObj_exc_info, "cinfo", pyObj_cinfo)) {
        PyErr_Print();
        Py_XDECREF(pyObj_cinfo);
    }
    Py_DECREF(pyObj_cinfo);

    if (!error_msg.empty()) {
        PyObject* pyObj_error_msg = PyUnicode_FromString(error_msg.c_str());
        if (-1 == PyDict_SetItemString(pyObj_exc_info, "error_message", pyObj_error_msg)) {
            PyErr_Print();
            Py_XDECREF(pyObj_error_msg);
        }
        Py_DECREF(pyObj_error_msg);
    }

    exc->exc_info = pyObj_exc_info;

    return reinterpret_cast<PyObject*>(exc);
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::subdocument_error_context& ctx,
                             const char* file,
                             int line,
                             std::string error_msg,
                             std::string context_detail_type)
{
    exception_base* exc = create_exception_base_obj();
    exc->ec = ctx.ec();
    PyObject* pyObj_error_context = build_base_error_context_new(ctx);

    build_kv_error_context(ctx, pyObj_error_context);

    std::string context_type = "SubdocumentErrorContext";
    PyObject* pyObj_tmp = PyUnicode_FromString(context_type.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, CONTEXT_TYPE, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    exc->error_context = pyObj_error_context;

    PyObject* pyObj_exc_info = PyDict_New();

    PyObject* pyObj_cinfo = Py_BuildValue("(s,i)", file, line);
    if (-1 == PyDict_SetItemString(pyObj_exc_info, "cinfo", pyObj_cinfo)) {
        PyErr_Print();
        Py_XDECREF(pyObj_cinfo);
    }
    Py_DECREF(pyObj_cinfo);

    if (!error_msg.empty()) {
        PyObject* pyObj_error_msg = PyUnicode_FromString(error_msg.c_str());
        if (-1 == PyDict_SetItemString(pyObj_exc_info, "error_message", pyObj_error_msg)) {
            PyErr_Print();
            Py_XDECREF(pyObj_error_msg);
        }
        Py_DECREF(pyObj_error_msg);
    }

    exc->exc_info = pyObj_exc_info;

    return reinterpret_cast<PyObject*>(exc);
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::core::error_context::http& ctx,
                             const char* file,
                             int line,
                             std::string error_msg,
                             std::string context_detail_type)
{
    exception_base* exc = create_exception_base_obj();
    exc->ec = ctx.ec;

    PyObject* pyObj_error_context = build_base_error_context(ctx);
    build_base_http_error_context(ctx, pyObj_error_context);

    std::string context_type = "HTTPErrorContext";
    PyObject* pyObj_tmp = nullptr;
    pyObj_tmp = PyUnicode_FromString(context_type.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, CONTEXT_TYPE, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    if (!context_detail_type.empty()) {
        pyObj_tmp = PyUnicode_FromString(context_detail_type.c_str());
        if (-1 == PyDict_SetItemString(pyObj_error_context, CONTEXT_DETAIL_TYPE, pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }

    exc->error_context = pyObj_error_context;

    PyObject* pyObj_exc_info = PyDict_New();

    PyObject* pyObj_cinfo = Py_BuildValue("(s,i)", file, line);
    if (-1 == PyDict_SetItemString(pyObj_exc_info, "cinfo", pyObj_cinfo)) {
        PyErr_Print();
        Py_XDECREF(pyObj_cinfo);
    }
    Py_DECREF(pyObj_cinfo);

    if (!error_msg.empty()) {
        PyObject* pyObj_error_msg = PyUnicode_FromString(error_msg.c_str());
        if (-1 == PyDict_SetItemString(pyObj_exc_info, "error_message", pyObj_error_msg)) {
            PyErr_Print();
            Py_XDECREF(pyObj_error_msg);
        }
        Py_DECREF(pyObj_error_msg);
    }

    exc->exc_info = pyObj_exc_info;

    return reinterpret_cast<PyObject*>(exc);
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::core::error_context::query& ctx,
                             const char* file,
                             int line,
                             std::string error_msg,
                             std::string context_detail_type)
{
    exception_base* exc = create_exception_base_obj();
    exc->ec = ctx.ec;
    PyObject* pyObj_error_context = build_base_error_context(ctx);
    build_base_http_error_context(ctx, pyObj_error_context);

    PyObject* pyObj_tmp = nullptr;
    pyObj_tmp = PyLong_FromLongLong(ctx.first_error_code);
    if (-1 == PyDict_SetItemString(pyObj_error_context, QUERY_FIRST_ERROR_CODE, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(ctx.first_error_message.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, QUERY_FIRST_ERROR_MSG, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(ctx.statement.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, QUERY_STATEMENT, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    if (ctx.parameters.has_value()) {
        pyObj_tmp = PyUnicode_FromString(ctx.parameters.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_error_context, QUERY_PARAMETERS, pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }

    std::string context_type = "QueryErrorContext";
    pyObj_tmp = PyUnicode_FromString(context_type.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, CONTEXT_TYPE, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    exc->error_context = pyObj_error_context;

    PyObject* pyObj_exc_info = PyDict_New();

    PyObject* pyObj_cinfo = Py_BuildValue("(s,i)", file, line);
    if (-1 == PyDict_SetItemString(pyObj_exc_info, "cinfo", pyObj_cinfo)) {
        PyErr_Print();
        Py_XDECREF(pyObj_cinfo);
    }
    Py_DECREF(pyObj_cinfo);

    if (!error_msg.empty()) {
        PyObject* pyObj_error_msg = PyUnicode_FromString(error_msg.c_str());
        if (-1 == PyDict_SetItemString(pyObj_exc_info, "error_message", pyObj_error_msg)) {
            PyErr_Print();
            Py_XDECREF(pyObj_error_msg);
        }
        Py_DECREF(pyObj_error_msg);
    }

    exc->exc_info = pyObj_exc_info;

    return reinterpret_cast<PyObject*>(exc);
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::core::error_context::analytics& ctx,
                             const char* file,
                             int line,
                             std::string error_msg,
                             std::string context_detail_type)
{
    exception_base* exc = create_exception_base_obj();
    exc->ec = ctx.ec;
    PyObject* pyObj_error_context = build_base_error_context(ctx);
    build_base_http_error_context(ctx, pyObj_error_context);

    PyObject* pyObj_tmp = nullptr;
    pyObj_tmp = PyLong_FromLongLong(ctx.first_error_code);
    if (-1 == PyDict_SetItemString(pyObj_error_context, QUERY_FIRST_ERROR_CODE, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(ctx.first_error_message.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, QUERY_FIRST_ERROR_MSG, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(ctx.statement.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, QUERY_STATEMENT, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    if (ctx.parameters.has_value()) {
        pyObj_tmp = PyUnicode_FromString(ctx.parameters.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_error_context, QUERY_PARAMETERS, pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }

    std::string context_type = "AnalyticsErrorContext";
    pyObj_tmp = PyUnicode_FromString(context_type.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, CONTEXT_TYPE, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    exc->error_context = pyObj_error_context;

    PyObject* pyObj_exc_info = PyDict_New();

    PyObject* pyObj_cinfo = Py_BuildValue("(s,i)", file, line);
    if (-1 == PyDict_SetItemString(pyObj_exc_info, "cinfo", pyObj_cinfo)) {
        PyErr_Print();
        Py_XDECREF(pyObj_cinfo);
    }
    Py_DECREF(pyObj_cinfo);

    if (!error_msg.empty()) {
        PyObject* pyObj_error_msg = PyUnicode_FromString(error_msg.c_str());
        if (-1 == PyDict_SetItemString(pyObj_exc_info, "error_message", pyObj_error_msg)) {
            PyErr_Print();
            Py_XDECREF(pyObj_error_msg);
        }
        Py_DECREF(pyObj_error_msg);
    }

    exc->exc_info = pyObj_exc_info;

    return reinterpret_cast<PyObject*>(exc);
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::core::error_context::search& ctx,
                             const char* file,
                             int line,
                             std::string error_msg,
                             std::string context_detail_type)
{
    exception_base* exc = create_exception_base_obj();
    exc->ec = ctx.ec;
    PyObject* pyObj_error_context = build_base_error_context(ctx);
    build_base_http_error_context(ctx, pyObj_error_context);

    PyObject* pyObj_tmp = nullptr;
    pyObj_tmp = PyUnicode_FromString(ctx.index_name.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, SEARCH_INDEX_NAME, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(ctx.query.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, SEARCH_QUERY, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    if (ctx.parameters.has_value()) {
        pyObj_tmp = PyUnicode_FromString(ctx.parameters.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_error_context, SEARCH_PARAMETERS, pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }

    std::string context_type = "SearchErrorContext";
    pyObj_tmp = PyUnicode_FromString(context_type.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, CONTEXT_TYPE, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    exc->error_context = pyObj_error_context;

    PyObject* pyObj_exc_info = PyDict_New();

    PyObject* pyObj_cinfo = Py_BuildValue("(s,i)", file, line);
    if (-1 == PyDict_SetItemString(pyObj_exc_info, "cinfo", pyObj_cinfo)) {
        PyErr_Print();
        Py_XDECREF(pyObj_cinfo);
    }
    Py_DECREF(pyObj_cinfo);

    if (!error_msg.empty()) {
        PyObject* pyObj_error_msg = PyUnicode_FromString(error_msg.c_str());
        if (-1 == PyDict_SetItemString(pyObj_exc_info, "error_message", pyObj_error_msg)) {
            PyErr_Print();
            Py_XDECREF(pyObj_error_msg);
        }
        Py_DECREF(pyObj_error_msg);
    }

    exc->exc_info = pyObj_exc_info;

    return reinterpret_cast<PyObject*>(exc);
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::core::error_context::view& ctx,
                             const char* file,
                             int line,
                             std::string error_msg,
                             std::string context_detail_type)
{
    exception_base* exc = create_exception_base_obj();
    exc->ec = ctx.ec;
    PyObject* pyObj_error_context = build_base_error_context(ctx);
    build_base_http_error_context(ctx, pyObj_error_context);

    PyObject* pyObj_tmp = nullptr;
    pyObj_tmp = PyUnicode_FromString(ctx.design_document_name.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, VIEW_DDOC_NAME, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(ctx.view_name.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, VIEW_NAME, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    PyObject* pyObj_query_string = PyList_New(static_cast<Py_ssize_t>(0));
    for (auto const& query : ctx.query_string) {
        pyObj_tmp = PyUnicode_FromString(query.c_str());
        if (-1 == PyList_Append(pyObj_query_string, pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }

    if (-1 == PyDict_SetItemString(pyObj_error_context, VIEW_QUERY, pyObj_query_string)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_query_string);

    std::string context_type = "ViewErrorContext";
    pyObj_tmp = PyUnicode_FromString(context_type.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, CONTEXT_TYPE, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    exc->error_context = pyObj_error_context;

    PyObject* pyObj_exc_info = PyDict_New();

    PyObject* pyObj_cinfo = Py_BuildValue("(s,i)", file, line);
    if (-1 == PyDict_SetItemString(pyObj_exc_info, "cinfo", pyObj_cinfo)) {
        PyErr_Print();
        Py_XDECREF(pyObj_cinfo);
    }
    Py_DECREF(pyObj_cinfo);

    if (!error_msg.empty()) {
        PyObject* pyObj_error_msg = PyUnicode_FromString(error_msg.c_str());
        if (-1 == PyDict_SetItemString(pyObj_exc_info, "error_message", pyObj_error_msg)) {
            PyErr_Print();
            Py_XDECREF(pyObj_error_msg);
        }
        Py_DECREF(pyObj_error_msg);
    }

    exc->exc_info = pyObj_exc_info;

    return reinterpret_cast<PyObject*>(exc);
}

void
pycbc_set_python_exception(std::error_code ec, const char* file, int line, const char* msg);

PyObject*
pycbc_build_exception(std::error_code ec, const char* file, int line, std::string msg);

void
pycbc_add_exception_info(PyObject* pyObj_exc_base, const char* key, PyObject* pyObj_value);
