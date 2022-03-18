#pragma once

#include <exception>

#include "client.hxx"

#define DISPATCHED_TO "last_dispatched_to"
#define DISPATCHED_FROM "last_dispatched_from"
#define RETRY_ATTEMPTS "retry_attempts"
#define RETRY_REASONS "retry_reasons"

#define CONTEXT_TYPE "context_type"
#define CLIENT_CONTEXT_ID "client_context_id"

#define KV_DOCUMENT_ID "key"
#define KV_DOCUMENT_BUCKET "bucket_name"
#define KV_DOCUMENT_SCOPE "scope_name"
#define KV_DOCUMENT_COLLECTION "collection_name"
#define KV_OPAQUE "opaque"
#define KV_STATUS_CODE "status_code"
#define KV_ERROR_MAP_INFO "error_map_info"
#define KV_EXTENDED_ERROR_INFO "extended_error_info"

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

#define VIEW_DDOC_NAME "design_document_name"
#define VIEW_NAME "view_name"
#define VIEW_QUERY "query_string"

#define NULL_CONN_OBJECT "Received a null connection."

struct exception_base {
    PyObject_HEAD std::error_code ec;
    PyObject* error_context = nullptr;
};

int
pycbc_exception_base_type_init(PyObject** ptr);

exception_base*
create_exception_base_obj();

std::string
retry_reason_to_string(couchbase::io::retry_reason reason);

PyObject*
build_kv_error_map_info(couchbase::error_map::error_info error_info);

// @TODO:  these are going way, replace w/ build_exception_from_* methods
template<typename T>
PyObject*
build_exception(const T& ec)
{
    exception_base* exc = create_exception_base_obj();
    return reinterpret_cast<PyObject*>(exc);
}

template<>
inline PyObject*
build_exception(const std::error_code& ec)
{
    exception_base* exc = create_exception_base_obj();
    exc->ec = ec;
    return reinterpret_cast<PyObject*>(exc);
}

/*

Build exceptions via error context

*/

template<typename T>
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
build_exception_from_context(const T& ctx)
{
    exception_base* exc = create_exception_base_obj();
    exc->ec = ctx.ec;
    exc->error_context = build_base_error_context(ctx);
    Py_INCREF(exc->error_context);

    return reinterpret_cast<PyObject*>(exc);
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::error_context::key_value& ctx)
{
    exception_base* exc = create_exception_base_obj();
    exc->ec = ctx.ec;
    PyObject* pyObj_error_context = build_base_error_context(ctx);

    PyObject* pyObj_tmp = nullptr;
    pyObj_tmp = PyUnicode_FromString(ctx.id.key().c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, KV_DOCUMENT_ID, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(ctx.id.bucket().c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, KV_DOCUMENT_BUCKET, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(ctx.id.scope().c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, KV_DOCUMENT_SCOPE, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(ctx.id.collection().c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, KV_DOCUMENT_COLLECTION, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromLong(ctx.opaque);
    if (-1 == PyDict_SetItemString(pyObj_error_context, KV_OPAQUE, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    if (ctx.status_code.has_value()) {
        pyObj_tmp = PyLong_FromLong(static_cast<uint16_t>(ctx.status_code.value()));
        if (-1 == PyDict_SetItemString(pyObj_error_context, KV_STATUS_CODE, pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }

    if (ctx.error_map_info.has_value()) {
        PyObject* err_info = build_kv_error_map_info(ctx.error_map_info.value());
        if (-1 == PyDict_SetItemString(pyObj_error_context, KV_ERROR_MAP_INFO, err_info)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(err_info);
    }

    if (ctx.enhanced_error_info.has_value()) {
        PyObject* enhanced_err_info = PyDict_New();
        pyObj_tmp = PyUnicode_FromString(ctx.enhanced_error_info.value().reference.c_str());
        if (-1 == PyDict_SetItemString(enhanced_err_info, "reference", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(ctx.enhanced_error_info.value().context.c_str());
        if (-1 == PyDict_SetItemString(enhanced_err_info, "context", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        if (-1 == PyDict_SetItemString(pyObj_error_context, KV_EXTENDED_ERROR_INFO, enhanced_err_info)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(enhanced_err_info);
    }

    std::string context_type = "KeyValueErrorContext";
    pyObj_tmp = PyUnicode_FromString(context_type.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, CONTEXT_TYPE, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    exc->error_context = pyObj_error_context;
    Py_INCREF(exc->error_context);

    return reinterpret_cast<PyObject*>(exc);
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::error_context::http& ctx)
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

    exc->error_context = pyObj_error_context;
    Py_INCREF(exc->error_context);

    return reinterpret_cast<PyObject*>(exc);
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::error_context::query& ctx)
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
    Py_INCREF(exc->error_context);

    return reinterpret_cast<PyObject*>(exc);
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::error_context::analytics& ctx)
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
    Py_INCREF(exc->error_context);

    return reinterpret_cast<PyObject*>(exc);
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::error_context::search& ctx)
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

    if (ctx.query.has_value()) {
        pyObj_tmp = PyUnicode_FromString(ctx.query.value().c_str());
        if (-1 == PyDict_SetItemString(pyObj_error_context, SEARCH_QUERY, pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }

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
    Py_INCREF(exc->error_context);

    return reinterpret_cast<PyObject*>(exc);
}

template<>
inline PyObject*
build_exception_from_context(const couchbase::error_context::view& ctx)
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
    Py_DECREF(pyObj_tmp);

    std::string context_type = "ViewErrorContext";
    pyObj_tmp = PyUnicode_FromString(context_type.c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_context, CONTEXT_TYPE, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    exc->error_context = pyObj_error_context;
    Py_INCREF(exc->error_context);

    return reinterpret_cast<PyObject*>(exc);
}

// start - needed for Pycbc error code
enum class PycbcError {
    InvalidArgument = 10,
    HTTPError,
    UnsuccessfulOperation,
    UnableToBuildResult,
    CallbackUnsuccessful,
    InternalSDKError
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

class PycbcException : public std::exception
{
  public:
    PycbcException(std::string msg_, const char* file_, int line_, std::error_code ec_)
      : msg{ msg_ }
      , file{ file_ }
      , line{ line_ }
      , ec{ ec_ }
    {
    }

    ~PycbcException() throw()
    {
    }

    const char* get_file() const
    {
        return file;
    }
    int get_line() const
    {
        return line;
    }
    std::error_code get_error_code() const
    {
        return ec;
    }
    std::string get_error_code_category() const
    {
        return std::string(ec.category().name());
    }

    const char* what() const throw()
    {
        return msg.c_str();
    }

  protected:
    std::string msg;
    const char* file;
    int line;
    std::error_code ec;
};

class PycbcKeyValueException : public PycbcException
{
  public:
    PycbcKeyValueException(std::string msg_, const char* file_, int line_, couchbase::error_context::key_value ctx_)
      : PycbcException(msg_, file_, line_, ctx_.ec)
      , ctx{ ctx_ }
    {
    }

    couchbase::error_context::key_value get_context() const
    {
        return ctx;
    }

  protected:
    couchbase::error_context::key_value ctx;
};

class PycbcHttpException : public PycbcException
{
  public:
    PycbcHttpException(std::string msg_, const char* file_, int line_, couchbase::error_context::http ctx_)
      : PycbcException(msg_, file_, line_, ctx_.ec)
      , ctx{ ctx_ }
    {
    }
    PycbcHttpException(std::string msg_, const char* file_, int line_, couchbase::error_context::http ctx_, std::error_code ec_)
      : PycbcException(msg_, file_, line_, ec_)
      , ctx{ ctx_ }
    {
    }

    couchbase::error_context::http get_context() const
    {
        return ctx;
    }

  protected:
    couchbase::error_context::http ctx;
};

void
pycbc_set_python_exception(const char* msg, std::error_code ec, const char* file, int line, PyObject* pyObj_base = nullptr);

PyObject*
pycbc_core_get_exception_kwargs(std::string msg, std::error_code ec, const char* file, int line);

PyObject*
pycbc_get_exception_kwargs(std::string msg, const char* file, int line);

template<typename T>
void
pycbc_set_exception(const T& ex)
{
    pycbc_set_python_exception(ex.what(), ex.get_error_code(), ex.get_file(), ex.get_line());
}

template<>
inline void
pycbc_set_exception<PycbcKeyValueException>(const PycbcKeyValueException& ex)
{
    auto ctx = ex.get_context();
    PyObject* pyObj_base_exc = build_exception_from_context(ctx);
    pycbc_set_python_exception(ex.what(), ex.get_error_code(), ex.get_file(), ex.get_line(), pyObj_base_exc);
    // Don't need the pyObj_base_exc any longer
    Py_DECREF(pyObj_base_exc);
}

template<>
inline void
pycbc_set_exception<PycbcHttpException>(const PycbcHttpException& ex)
{
    auto ctx = ex.get_context();
    PyObject* pyObj_base_exc = build_exception_from_context(ctx);
    pycbc_set_python_exception(ex.what(), ex.get_error_code(), ex.get_file(), ex.get_line(), pyObj_base_exc);
    // Don't need the pyObj_base_exc any longer
    Py_DECREF(pyObj_base_exc);
}
