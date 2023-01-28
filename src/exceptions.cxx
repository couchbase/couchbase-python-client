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

#include "exceptions.hxx"

PyTypeObject exception_base_type = { PyObject_HEAD_INIT(NULL) 0 };

static PyObject*
exception_base__category__(exception_base* self, [[maybe_unused]] PyObject* args)
{
    return PyUnicode_FromString(self->ec.category().name());
}

static PyObject*
exception_base__err__(exception_base* self, [[maybe_unused]] PyObject* args)
{
    return PyLong_FromLong(self->ec.value());
}
static PyObject*
exception_base__strerror__(exception_base* self, [[maybe_unused]] PyObject* args)
{
    if (self->ec) {
        return PyUnicode_FromString(self->ec.message().c_str());
    }
    Py_RETURN_NONE;
}

static PyObject*
exception_base__context__(exception_base* self, [[maybe_unused]] PyObject* args)
{
    if (self->error_context) {
        PyObject* pyObj_error_context = PyDict_Copy(self->error_context);
        return pyObj_error_context;
    }
    Py_RETURN_NONE;
}

static PyObject*
exception_base__info__(exception_base* self, [[maybe_unused]] PyObject* args)
{
    if (self->exc_info) {
        PyObject* pyObj_exc_info = PyDict_Copy(self->exc_info);
        return pyObj_exc_info;
    }
    Py_RETURN_NONE;
}

static void
exception_base_dealloc(exception_base* self)
{
    if (self->error_context) {
        if (PyDict_Check(self->error_context)) {
            PyDict_Clear(self->error_context);
        }
        Py_DECREF(self->error_context);
    }
    if (self->exc_info) {
        if (PyDict_Check(self->exc_info)) {
            PyDict_Clear(self->exc_info);
        }
        Py_DECREF(self->exc_info);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
    CB_LOG_DEBUG("{}: exception_base_dealloc completed", "PYCBC");
}

static PyObject*
exception_base__new__(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
    // lets just initialize from a result object.
    const char* kw[] = { "result", nullptr };
    PyObject* result_obj = nullptr;
    auto self = reinterpret_cast<exception_base*>(type->tp_alloc(type, 0));
    PyArg_ParseTupleAndKeywords(args, kwargs, "|iO", const_cast<char**>(kw), &result_obj);
    if (nullptr != result_obj) {
        if (PyObject_IsInstance(result_obj, reinterpret_cast<PyObject*>(&result_type))) {
            self->ec = reinterpret_cast<result*>(result_obj)->ec;
        }
        Py_DECREF(result_obj);
    } else {
        self->ec = std::error_code();
    }
    return reinterpret_cast<PyObject*>(self);
}

static PyMethodDef exception_base_methods[] = {
    { "strerror", (PyCFunction)exception_base__strerror__, METH_NOARGS, PyDoc_STR("String description of error") },
    { "err", (PyCFunction)exception_base__err__, METH_NOARGS, PyDoc_STR("Integer error code") },
    { "err_category", (PyCFunction)exception_base__category__, METH_NOARGS, PyDoc_STR("error category, expressed as a string") },
    { "error_context", (PyCFunction)exception_base__context__, METH_NOARGS, PyDoc_STR("error context dict") },
    { "error_info", (PyCFunction)exception_base__info__, METH_NOARGS, PyDoc_STR("error info dict") },
    { nullptr, nullptr, 0, nullptr }
};

int
pycbc_exception_base_type_init(PyObject** ptr)
{
    PyTypeObject* p = &exception_base_type;

    *ptr = (PyObject*)p;
    if (p->tp_name) {
        return 0;
    }

    p->tp_name = "pycbc_core.exception";
    p->tp_doc = "Base class for exceptions coming from pycbc_core";
    p->tp_basicsize = sizeof(exception_base);
    p->tp_itemsize = 0;
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_new = exception_base__new__;
    p->tp_dealloc = (destructor)exception_base_dealloc;
    p->tp_methods = exception_base_methods;

    return PyType_Ready(p);
}

exception_base*
create_exception_base_obj()
{
    PyObject* exc = PyObject_CallObject(reinterpret_cast<PyObject*>(&exception_base_type), nullptr);
    return reinterpret_cast<exception_base*>(exc);
}

std::string
retry_reason_to_string(couchbase::retry_reason reason)
{
    switch (reason) {
        case couchbase::retry_reason::socket_not_available:
            return "socket_not_available";
        case couchbase::retry_reason::service_not_available:
            return "service_not_available";
        case couchbase::retry_reason::node_not_available:
            return "node_not_available";
        case couchbase::retry_reason::key_value_not_my_vbucket:
            return "key_value_not_my_vbucket";
        case couchbase::retry_reason::key_value_collection_outdated:
            return "key_value_collection_outdated";
        case couchbase::retry_reason::key_value_error_map_retry_indicated:
            return "key_value_error_map_retry_indicated";
        case couchbase::retry_reason::key_value_locked:
            return "key_value_locked";
        case couchbase::retry_reason::key_value_temporary_failure:
            return "key_value_temporary_failure";
        case couchbase::retry_reason::key_value_sync_write_in_progress:
            return "key_value_sync_write_in_progress";
        case couchbase::retry_reason::key_value_sync_write_re_commit_in_progress:
            return "key_value_sync_write_re_commit_in_progress";
        case couchbase::retry_reason::service_response_code_indicated:
            return "service_response_code_indicated";
        case couchbase::retry_reason::circuit_breaker_open:
            return "circuit_breaker_open";
        case couchbase::retry_reason::query_prepared_statement_failure:
            return "query_prepared_statement_failure";
        case couchbase::retry_reason::query_index_not_found:
            return "query_index_not_found";
        case couchbase::retry_reason::analytics_temporary_failure:
            return "analytics_temporary_failure";
        case couchbase::retry_reason::search_too_many_requests:
            return "search_too_many_requests";
        case couchbase::retry_reason::views_temporary_failure:
            return "views_temporary_failure";
        case couchbase::retry_reason::views_no_active_partition:
            return "views_no_active_partition";
        case couchbase::retry_reason::do_not_retry:
            return "do_not_retry";
        case couchbase::retry_reason::socket_closed_while_in_flight:
            return "socket_closed_while_in_flight";
        case couchbase::retry_reason::unknown:
            return "unknown";
        default:
            return "unknown";
    }
}

PyObject*
build_kv_error_map_info(couchbase::key_value_error_map_info error_info)
{
    PyObject* err_info = PyDict_New();
    PyObject* pyObj_tmp = PyLong_FromLong(static_cast<uint16_t>(error_info.code()));
    if (-1 == PyDict_SetItemString(err_info, "code", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(error_info.name().c_str());
    if (-1 == PyDict_SetItemString(err_info, "name", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(error_info.description().c_str());
    if (-1 == PyDict_SetItemString(err_info, "description", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    PyObject* attr_set = PySet_New(nullptr);
    for (auto attr : error_info.attributes()) {
        pyObj_tmp = PyLong_FromLong(static_cast<uint16_t>(attr));
        if (-1 == PySet_Add(attr_set, pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }
    Py_ssize_t set_size = PySet_Size(attr_set);
    if (set_size > 0) {
        if (-1 == PyDict_SetItemString(err_info, "attributes", attr_set)) {
            PyErr_Print();
            PyErr_Clear();
        }
    }
    Py_XDECREF(attr_set);

    return err_info;
}

void
build_kv_error_context(const couchbase::key_value_error_context& ctx, PyObject* pyObj_error_ctx)
{
    PyObject* pyObj_tmp = nullptr;
    pyObj_tmp = PyUnicode_FromString(ctx.id().c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_ctx, KV_DOCUMENT_ID, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(ctx.bucket().c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_ctx, KV_DOCUMENT_BUCKET, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(ctx.scope().c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_ctx, KV_DOCUMENT_SCOPE, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyUnicode_FromString(ctx.collection().c_str());
    if (-1 == PyDict_SetItemString(pyObj_error_ctx, KV_DOCUMENT_COLLECTION, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromLong(ctx.opaque());
    if (-1 == PyDict_SetItemString(pyObj_error_ctx, KV_OPAQUE, pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(pyObj_tmp);

    if (ctx.status_code().has_value()) {
        pyObj_tmp = PyLong_FromLong(static_cast<uint16_t>(ctx.status_code().value()));
        if (-1 == PyDict_SetItemString(pyObj_error_ctx, KV_STATUS_CODE, pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);
    }

    if (ctx.error_map_info().has_value()) {
        PyObject* err_info = build_kv_error_map_info(ctx.error_map_info().value());
        if (-1 == PyDict_SetItemString(pyObj_error_ctx, KV_ERROR_MAP_INFO, err_info)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(err_info);
    }

    if (ctx.extended_error_info().has_value()) {
        PyObject* enhanced_err_info = PyDict_New();
        pyObj_tmp = PyUnicode_FromString(ctx.extended_error_info().value().reference().c_str());
        if (-1 == PyDict_SetItemString(enhanced_err_info, "reference", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        pyObj_tmp = PyUnicode_FromString(ctx.extended_error_info().value().context().c_str());
        if (-1 == PyDict_SetItemString(enhanced_err_info, "context", pyObj_tmp)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(pyObj_tmp);

        if (-1 == PyDict_SetItemString(pyObj_error_ctx, KV_EXTENDED_ERROR_INFO, enhanced_err_info)) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_DECREF(enhanced_err_info);
    }
}

struct PycbcErrorCategory : std::error_category {
    const char* name() const noexcept override;
    std::string message(int ec) const override;
};

const char*
PycbcErrorCategory::name() const noexcept
{
    return "pycbc";
}

std::string
PycbcErrorCategory::message(int ec) const
{
    switch (static_cast<PycbcError>(ec)) {
        case PycbcError::InvalidArgument:
            return "Invalid argument";
        case PycbcError::HTTPError:
            return "HTTP Error";
        case PycbcError::UnsuccessfulOperation:
            return "Unsuccessful operation";
        case PycbcError::UnableToBuildResult:
            return "Unable to build operation's result";
        case PycbcError::CallbackUnsuccessful:
            return "Async callback failed";
        case PycbcError::InternalSDKError:
            return "Internal SDK error occurred";
        default:
            return "(Unrecognized error)";
    }
}

const PycbcErrorCategory defaultPycbcErrorCategory{};

std::error_code
make_error_code(PycbcError ec)
{
    return { static_cast<int>(ec), defaultPycbcErrorCategory };
}

PyObject*
get_pycbc_exception_class(PyObject* pyObj_exc_module, std::error_code ec)
{
    switch (static_cast<PycbcError>(ec.value())) {
        case PycbcError::InvalidArgument:
            return PyObject_GetAttrString(pyObj_exc_module, "InvalidArgumentException");
        case PycbcError::HTTPError:
            return PyObject_GetAttrString(pyObj_exc_module, "HTTPException");
        case PycbcError::UnsuccessfulOperation:
            return PyObject_GetAttrString(pyObj_exc_module, "UnsuccessfulOperationException");
        case PycbcError::UnableToBuildResult:
        case PycbcError::CallbackUnsuccessful:
        case PycbcError::InternalSDKError:
        default:
            return PyObject_GetAttrString(pyObj_exc_module, "InternalSDKException");
    }

    return PyObject_GetAttrString(pyObj_exc_module, "InternalSDKException");
}

void
pycbc_set_python_exception(std::error_code ec, const char* file, int line, const char* msg)
{
    PyObject *pyObj_type = nullptr, *pyObj_value = nullptr, *pyObj_traceback = nullptr;
    PyObject* pyObj_exc_class = nullptr;
    PyObject* pyObj_exc_instance = nullptr;

    PyErr_Fetch(&pyObj_type, &pyObj_value, &pyObj_traceback);
    PyErr_Clear();

    PyObject* pyObj_exc_params = PyDict_New();

    if (pyObj_type != nullptr) {
        PyErr_NormalizeException(&pyObj_type, &pyObj_value, &pyObj_traceback);
        if (-1 == PyDict_SetItemString(pyObj_exc_params, "inner_cause", pyObj_value)) {
            PyErr_Print();
            Py_DECREF(pyObj_type);
            Py_XDECREF(pyObj_value);
            Py_XDECREF(pyObj_traceback);
            Py_DECREF(pyObj_exc_params);
            return;
        }
        Py_XDECREF(pyObj_type);
        Py_XDECREF(pyObj_value);
    }

    PyObject* pyObj_cinfo = Py_BuildValue("(s,i)", file, line);
    if (-1 == PyDict_SetItemString(pyObj_exc_params, "cinfo", pyObj_cinfo)) {
        PyErr_Print();
        Py_XDECREF(pyObj_cinfo);
        Py_DECREF(pyObj_exc_params);
        return;
    }
    Py_DECREF(pyObj_cinfo);

    PyObject* pyObj_exc_module = PyImport_ImportModule("couchbase.exceptions");
    if (pyObj_exc_module == nullptr) {
        PyErr_Print();
        Py_DECREF(pyObj_exc_params);
        return;
    }

    pyObj_exc_class = get_pycbc_exception_class(pyObj_exc_module, ec);
    if (pyObj_exc_class == nullptr) {
        PyErr_Print();
        Py_XDECREF(pyObj_exc_params);
        Py_DECREF(pyObj_exc_module);
        return;
    }
    Py_DECREF(pyObj_exc_module);

    PyObject* pyObj_args = PyTuple_New(0);
    PyObject* pyObj_kwargs = PyDict_New();
    PyObject* pyObj_tmp = PyUnicode_FromString(msg);
    if (-1 == PyDict_SetItemString(pyObj_kwargs, "message", pyObj_tmp)) {
        PyErr_Print();
        Py_XDECREF(pyObj_args);
        Py_XDECREF(pyObj_kwargs);
        Py_XDECREF(pyObj_tmp);
        Py_DECREF(pyObj_exc_params);
        Py_DECREF(pyObj_exc_class);
        return;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromLong(ec.value());
    if (-1 == PyDict_SetItemString(pyObj_kwargs, "error_code", pyObj_tmp)) {
        PyErr_Print();
        Py_XDECREF(pyObj_args);
        Py_XDECREF(pyObj_kwargs);
        Py_XDECREF(pyObj_tmp);
        Py_DECREF(pyObj_exc_params);
        Py_DECREF(pyObj_exc_class);
        return;
    }
    Py_DECREF(pyObj_tmp);

    if (-1 == PyDict_SetItemString(pyObj_kwargs, "exc_info", pyObj_exc_params)) {
        PyErr_Print();
        Py_DECREF(pyObj_args);
        Py_DECREF(pyObj_kwargs);
        Py_DECREF(pyObj_exc_params);
        Py_DECREF(pyObj_exc_class);
        return;
    }
    Py_DECREF(pyObj_exc_params);

    pyObj_exc_instance = PyObject_Call(pyObj_exc_class, pyObj_args, pyObj_kwargs);
    Py_DECREF(pyObj_args);
    Py_DECREF(pyObj_kwargs);
    Py_DECREF(pyObj_exc_class);

    if (pyObj_exc_instance == nullptr) {
        Py_XDECREF(pyObj_traceback);
        return;
    }

    Py_INCREF(Py_TYPE(pyObj_exc_instance));
    PyErr_Restore((PyObject*)Py_TYPE(pyObj_exc_instance), pyObj_exc_instance, pyObj_traceback);
}

PyObject*
pycbc_build_exception(std::error_code ec, const char* file, int line, std::string msg)
{
    PyObject *pyObj_type = nullptr, *pyObj_value = nullptr, *pyObj_traceback = nullptr;

    PyErr_Fetch(&pyObj_type, &pyObj_value, &pyObj_traceback);
    PyErr_Clear();

    PyObject* pyObj_exc_info = PyDict_New();

    if (pyObj_type != nullptr) {
        PyErr_NormalizeException(&pyObj_type, &pyObj_value, &pyObj_traceback);
        if (-1 == PyDict_SetItemString(pyObj_exc_info, "inner_cause", pyObj_value)) {
            PyErr_Print();
            Py_DECREF(pyObj_type);
            Py_XDECREF(pyObj_value);

            Py_XDECREF(pyObj_exc_info);
            return nullptr;
        }
        Py_DECREF(pyObj_type);
        Py_XDECREF(pyObj_value);
        // Py_XDECREF(pyObj_traceback);
    }

    PyObject* pyObj_cinfo = Py_BuildValue("(s,i)", file, line);
    if (-1 == PyDict_SetItemString(pyObj_exc_info, "cinfo", pyObj_cinfo)) {
        PyErr_Print();
        Py_XDECREF(pyObj_cinfo);
        Py_XDECREF(pyObj_exc_info);
        return nullptr;
    }
    Py_DECREF(pyObj_cinfo);

    if (!msg.empty()) {
        PyObject* pyObj_msg = PyUnicode_FromString(msg.c_str());
        if (-1 == PyDict_SetItemString(pyObj_exc_info, "error_msg", pyObj_msg)) {
            PyErr_Print();
            Py_DECREF(pyObj_exc_info);
            Py_XDECREF(pyObj_msg);
            return nullptr;
        }
        Py_DECREF(pyObj_msg);
    }

    exception_base* exc = create_exception_base_obj();
    exc->ec = ec;

    exc->exc_info = pyObj_exc_info;
    Py_INCREF(exc->exc_info);

    return reinterpret_cast<PyObject*>(exc);
}

void
pycbc_add_exception_info(PyObject* pyObj_exc_base, const char* key, PyObject* pyObj_value)
{
    exception_base* exc = reinterpret_cast<exception_base*>(pyObj_exc_base);

    if (exc->exc_info) {
        if (-1 == PyDict_SetItemString(exc->exc_info, key, pyObj_value)) {
            PyErr_Print();
            return;
        }
        Py_DECREF(pyObj_value);
    } else {
        PyObject* pyObj_exc_info = PyDict_New();
        if (-1 == PyDict_SetItemString(pyObj_exc_info, key, pyObj_value)) {
            PyErr_Print();
            Py_XDECREF(pyObj_exc_info);
            return;
        }
        Py_DECREF(pyObj_value);
        exc->exc_info = pyObj_exc_info;
        Py_INCREF(exc->exc_info);
    }
}
