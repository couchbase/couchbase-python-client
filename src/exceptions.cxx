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
        // Py_INCREF(self->error_context);
        return self->error_context;
    }
    Py_RETURN_NONE;
}

static void
exception_base_dealloc(exception_base* self)
{
    if (self->error_context) {
        Py_XDECREF(self->error_context);
    }
    LOG_INFO("{}: exception_base_dealloc completed", "PYCBC");
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
retry_reason_to_string(couchbase::io::retry_reason reason)
{
    switch (reason) {
        case couchbase::io::retry_reason::socket_not_available:
            return "socket_not_available";
        case couchbase::io::retry_reason::service_not_available:
            return "service_not_available";
        case couchbase::io::retry_reason::node_not_available:
            return "node_not_available";
        case couchbase::io::retry_reason::kv_not_my_vbucket:
            return "kv_not_my_vbucket";
        case couchbase::io::retry_reason::kv_collection_outdated:
            return "kv_collection_outdated";
        case couchbase::io::retry_reason::kv_error_map_retry_indicated:
            return "kv_error_map_retry_indicated";
        case couchbase::io::retry_reason::kv_locked:
            return "kv_locked";
        case couchbase::io::retry_reason::kv_temporary_failure:
            return "kv_temporary_failure";
        case couchbase::io::retry_reason::kv_sync_write_in_progress:
            return "kv_sync_write_in_progress";
        case couchbase::io::retry_reason::kv_sync_write_re_commit_in_progress:
            return "kv_sync_write_re_commit_in_progress";
        case couchbase::io::retry_reason::service_response_code_indicated:
            return "service_response_code_indicated";
        case couchbase::io::retry_reason::circuit_breaker_open:
            return "circuit_breaker_open";
        case couchbase::io::retry_reason::query_prepared_statement_failure:
            return "query_prepared_statement_failure";
        case couchbase::io::retry_reason::query_index_not_found:
            return "query_index_not_found";
        case couchbase::io::retry_reason::analytics_temporary_failure:
            return "analytics_temporary_failure";
        case couchbase::io::retry_reason::search_too_many_requests:
            return "search_too_many_requests";
        case couchbase::io::retry_reason::views_temporary_failure:
            return "views_temporary_failure";
        case couchbase::io::retry_reason::views_no_active_partition:
            return "views_no_active_partition";
        case couchbase::io::retry_reason::do_not_retry:
            return "do_not_retry";
        case couchbase::io::retry_reason::socket_closed_while_in_flight:
            return "socket_closed_while_in_flight";
        case couchbase::io::retry_reason::unknown:
            return "unknown";
        default:
            return "unknown";
    }
}

PyObject*
build_kv_error_map_info(couchbase::error_map::error_info error_info)
{
    PyObject* err_info = PyDict_New();
    if (-1 == PyDict_SetItemString(err_info, "code", PyLong_FromLong(static_cast<uint16_t>(error_info.code)))) {
        PyErr_Print();
        PyErr_Clear();
    }
    if (-1 == PyDict_SetItemString(err_info, "name", PyUnicode_FromString(error_info.name.c_str()))) {
        PyErr_Print();
        PyErr_Clear();
    }
    if (-1 == PyDict_SetItemString(err_info, "description", PyUnicode_FromString(error_info.description.c_str()))) {
        PyErr_Print();
        PyErr_Clear();
    }

    PyObject* attr_set = PySet_New(nullptr);
    for (auto attr : error_info.attributes) {
        if (-1 == PySet_Add(attr_set, PyLong_FromLong(static_cast<uint16_t>(attr)))) {
            PyErr_Print();
            PyErr_Clear();
        }
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

PyObject*
build_exception_from_err_code(const std::error_code& ec)
{
    exception_base* exc = create_exception_base_obj();
    exc->ec = ec;
    return reinterpret_cast<PyObject*>(exc);
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
get_pycbc_exception_class(PyObject* pyObj_exc_module, std::error_code ec, PyObject* pyObj_base_exc = nullptr)
{
    auto err_cat = std::string(ec.category().name());
    if (pyObj_base_exc != nullptr || err_cat.compare("pycbc") != 0) {
        return PyObject_GetAttrString(pyObj_exc_module, "PycbcException");
    }

    switch (static_cast<PycbcError>(ec.value())) {
        case PycbcError::InvalidArgument:
            return PyObject_GetAttrString(pyObj_exc_module, "InvalidArgumentException");
        case PycbcError::HTTPError:
            return PyObject_GetAttrString(pyObj_exc_module, "HTTPException");
        case PycbcError::UnsuccessfulOperation:
        case PycbcError::UnableToBuildResult:
        case PycbcError::CallbackUnsuccessful:
        case PycbcError::InternalSDKError:
        default:
            return PyObject_GetAttrString(pyObj_exc_module, "InternalSDKException");
    }

    return PyObject_GetAttrString(pyObj_exc_module, "InternalSDKException");
}

PyObject*
get_pycbc_exception_class_enum_value(std::error_code ec)
{
    switch (static_cast<PycbcError>(ec.value())) {
        case PycbcError::InvalidArgument:
            return PyLong_FromLong(static_cast<int>(2));
        case PycbcError::HTTPError:
            return PyLong_FromLong(static_cast<int>(5001));
        case PycbcError::UnsuccessfulOperation:
        case PycbcError::UnableToBuildResult:
        case PycbcError::CallbackUnsuccessful:
        case PycbcError::InternalSDKError:
        default:
            return PyLong_FromLong(static_cast<int>(5000));
    }
}

void
pycbc_set_python_exception(const char* msg, std::error_code ec, const char* file, int line, PyObject* pyObj_base_exc)
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

    if (PyGILState_Check()) {
        LOG_INFO("{}: we have the GIL!", "PYCBC");
    } else {
        LOG_INFO("{}: we DONT have the GIL!", "PYCBC");
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

    pyObj_exc_class = get_pycbc_exception_class(pyObj_exc_module, ec, pyObj_base_exc);
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

    exception_base* exc = nullptr;
    if (pyObj_base_exc != nullptr) {
        exc = reinterpret_cast<exception_base*>(pyObj_base_exc);
    }

    if (exc != nullptr && exc->error_context != nullptr) {
        if (-1 == PyDict_SetItemString(pyObj_kwargs, "context", exc->error_context)) {
            PyErr_Print();
            Py_DECREF(pyObj_args);
            Py_DECREF(pyObj_kwargs);
            Py_DECREF(pyObj_exc_params);
            Py_DECREF(pyObj_exc_class);
            return;
        }
        Py_DECREF(exc->error_context);
    }

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
pycbc_core_get_exception_kwargs(std::string msg, std::error_code ec, const char* file, int line)
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
            return nullptr;
        }
        Py_XDECREF(pyObj_type);
        Py_XDECREF(pyObj_value);
    }

    if (PyGILState_Check()) {
        LOG_INFO("{}: we have the GIL!", "PYCBC");
    } else {
        LOG_INFO("{}: we DONT have the GIL!", "PYCBC");
    }

    PyObject* pyObj_cinfo = Py_BuildValue("(s,i)", file, line);
    if (-1 == PyDict_SetItemString(pyObj_exc_params, "cinfo", pyObj_cinfo)) {
        PyErr_Print();
        Py_XDECREF(pyObj_cinfo);
        Py_DECREF(pyObj_exc_params);
        return nullptr;
    }
    Py_DECREF(pyObj_cinfo);

    PyObject* pyObj_tmp = PyUnicode_FromString(msg.c_str());
    if (-1 == PyDict_SetItemString(pyObj_exc_params, "message", pyObj_tmp)) {
        PyErr_Print();
        Py_XDECREF(pyObj_tmp);
        Py_DECREF(pyObj_exc_params);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    pyObj_tmp = get_pycbc_exception_class_enum_value(ec);
    if (-1 == PyDict_SetItemString(pyObj_exc_params, "error_code", pyObj_tmp)) {
        PyErr_Print();
        Py_XDECREF(pyObj_tmp);
        Py_DECREF(pyObj_exc_params);
        return nullptr;
    }
    Py_DECREF(pyObj_tmp);

    PyObject* pyObj_kwargs = PyDict_New();
    if (-1 == PyDict_SetItemString(pyObj_kwargs, "exc_info", pyObj_exc_params)) {
        PyErr_Print();
        Py_XDECREF(pyObj_kwargs);
        Py_XDECREF(pyObj_exc_params);
        return nullptr;
    }
    Py_DECREF(pyObj_exc_params);

    return pyObj_kwargs;
}

PyObject*
pycbc_get_exception_kwargs(std::string msg, const char* file, int line)
{
    PyObject* pyObj_kwargs = PyDict_New();
    PyObject* pyObj_exc_params = PyDict_New();

    PyObject* pyObj_cinfo = Py_BuildValue("(s,i)", file, line);
    if (-1 == PyDict_SetItemString(pyObj_exc_params, "cinfo", pyObj_cinfo)) {
        PyErr_Print();
        Py_XDECREF(pyObj_cinfo);
        Py_XDECREF(pyObj_kwargs);
        Py_XDECREF(pyObj_exc_params);
        return nullptr;
    }
    Py_DECREF(pyObj_cinfo);

    if (-1 == PyDict_SetItemString(pyObj_kwargs, "exc_info", pyObj_exc_params)) {
        PyErr_Print();
        Py_XDECREF(pyObj_kwargs);
        Py_XDECREF(pyObj_exc_params);
        return nullptr;
    }
    Py_DECREF(pyObj_exc_params);

    if (!msg.empty()) {
        PyObject* pyObj_msg = PyUnicode_FromString(msg.c_str());
        if (-1 == PyDict_SetItemString(pyObj_kwargs, "error_msg", pyObj_msg)) {
            PyErr_Print();
            Py_DECREF(pyObj_kwargs);
            Py_XDECREF(pyObj_msg);
            return nullptr;
        }
        Py_DECREF(pyObj_msg);
    }

    return pyObj_kwargs;
}
