
#include "transactions.hxx"
#include "../n1ql.hxx"
#include <couchbase/cluster.hxx>
#include <couchbase/transactions/transaction_get_result.hxx>
#include <couchbase/transactions/internal/exceptions_internal.hxx>
#include <couchbase/operations.hxx>
#include <couchbase/query_scan_consistency.hxx>
#include <sstream>

void
pycbc_txns::dealloc_transactions(PyObject* obj)
{
    auto txns = reinterpret_cast<pycbc_txns::transactions*>(PyCapsule_GetPointer(obj, "txns_"));
    Py_XDECREF(txns->conn);
    txns->txns->close();
    delete txns->txns;
    delete txns;
    LOG_INFO("dealloc transactions");
}

void
pycbc_txns::dealloc_attempt_context(PyObject* obj)
{
    auto ctx = reinterpret_cast<pycbc_txns::attempt_context*>(PyCapsule_GetPointer(obj, "ctx_"));
    delete ctx;
    LOG_INFO("dealloc attempt_context");
}

void
pycbc_txns::transaction_config__dealloc__(pycbc_txns::transaction_config* cfg)
{
    delete cfg->cfg;
    LOG_INFO("dealloc transaction_config");
}

void
pycbc_txns::per_transaction_config__dealloc__(pycbc_txns::per_transaction_config* cfg)
{
    delete cfg->cfg;
    LOG_INFO("dealloc per_transaction_config");
}

PyObject*
pycbc_txns::transaction_config__new__(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
    PyObject* durability_level = nullptr;
    PyObject* cleanup_window = nullptr;
    PyObject* kv_timeout = nullptr;
    PyObject* expiration_time = nullptr;
    char* scan_consistency = nullptr;
    PyObject* cleanup_lost_attempts = nullptr;
    PyObject* cleanup_client_attempts = nullptr;
    char* metadata_bucket = nullptr;
    char* metadata_scope = nullptr;
    char* metadata_collection = nullptr;

    const char* kw_list[] = { "durability_level",
                              "cleanup_window",
                              "kv_timeout",
                              "expiration_time",
                              "cleanup_lost_attempts",
                              "cleanup_client_attempts",
                              "metadata_bucket",
                              "metadata_scope",
                              "metadata_collection",
                              "scan_consistency",
                              nullptr };
    const char* kw_format = "|OOOOOOsssss";
    auto self = reinterpret_cast<pycbc_txns::transaction_config*>(type->tp_alloc(type, 0));

    self->cfg = new tx::transaction_config();

    if (!PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     kw_format,
                                     const_cast<char**>(kw_list),
                                     &durability_level,
                                     &cleanup_window,
                                     &kv_timeout,
                                     &expiration_time,
                                     &cleanup_lost_attempts,
                                     &cleanup_client_attempts,
                                     &metadata_bucket,
                                     &metadata_scope,
                                     &metadata_collection,
                                     &scan_consistency)) {
        PyErr_SetString(PyExc_ValueError, "couldn't parse args");
        Py_RETURN_NONE;
    }
    if (nullptr != durability_level) {
        self->cfg->durability_level(static_cast<tx::durability_level>(PyLong_AsUnsignedLong(durability_level)));
    }
    if (nullptr != cleanup_window) {
        self->cfg->cleanup_window(std::chrono::microseconds(PyLong_AsUnsignedLongLong(cleanup_window)));
    }
    if (nullptr != kv_timeout) {
        self->cfg->kv_timeout(std::chrono::milliseconds(PyLong_AsUnsignedLongLong(kv_timeout) / 1000));
    }
    if (nullptr != expiration_time) {
        self->cfg->expiration_time(std::chrono::microseconds(PyLong_AsUnsignedLongLong(expiration_time)));
    }
    if (nullptr != cleanup_lost_attempts) {
        self->cfg->cleanup_lost_attempts(!!PyObject_IsTrue(cleanup_lost_attempts));
    }
    if (nullptr != cleanup_client_attempts) {
        self->cfg->cleanup_client_attempts(!!PyObject_IsTrue(cleanup_client_attempts));
    }
    if (nullptr != metadata_bucket && nullptr != metadata_scope && nullptr != metadata_collection) {
        self->cfg->custom_metadata_collection(metadata_bucket, metadata_scope, metadata_collection);
    }
    if (nullptr != scan_consistency) {
        self->cfg->scan_consistency(str_to_scan_consistency_type<couchbase::query_scan_consistency>(scan_consistency));
    }
    return reinterpret_cast<PyObject*>(self);
}

PyObject*
pycbc_txns::per_transaction_config__new__(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
    PyObject* durability_level = nullptr;
    PyObject* kv_timeout = nullptr;
    PyObject* expiration_time = nullptr;
    char* scan_consistency = nullptr;

    const char* kw_list[] = { "durability_level", "kv_timeout", "expiration_time", "scan_consistency", nullptr };
    const char* kw_format = "|OOOs";
    auto self = reinterpret_cast<pycbc_txns::per_transaction_config*>(type->tp_alloc(type, 0));

    self->cfg = new tx::per_transaction_config();
    LOG_INFO("per_transaction_config__new__ called");
    if (!PyArg_ParseTupleAndKeywords(
          args, kwargs, kw_format, const_cast<char**>(kw_list), &durability_level, &kv_timeout, &expiration_time, &scan_consistency)) {
        PyErr_SetString(PyExc_ValueError, "couldn't parse args");
        Py_RETURN_NONE;
    }
    if (nullptr != durability_level) {
        self->cfg->durability_level(static_cast<tx::durability_level>(PyLong_AsUnsignedLong(durability_level)));
    }
    if (nullptr != kv_timeout) {
        self->cfg->kv_timeout(std::chrono::milliseconds(PyLong_AsUnsignedLongLong(kv_timeout) / 1000));
    }
    if (nullptr != expiration_time) {
        self->cfg->expiration_time(std::chrono::microseconds(PyLong_AsUnsignedLongLong(expiration_time)));
    }
    if (nullptr != scan_consistency) {
        self->cfg->scan_consistency(str_to_scan_consistency_type<couchbase::query_scan_consistency>(scan_consistency));
    }
    return reinterpret_cast<PyObject*>(self);
}

PyObject*
pycbc_txns::per_transaction_config__str__(PyObject* self)
{
    auto cfg = reinterpret_cast<pycbc_txns::per_transaction_config*>(self)->cfg;
    std::stringstream stream;
    stream << "per_transaction_config{";
    if (nullptr != cfg) {
        if (cfg->durability_level()) {
            stream << "durability: " << durability_level_to_string(*cfg->durability_level()) << ", ";
        }
        if (cfg->kv_timeout()) {
            stream << "kv_timeout: " << cfg->kv_timeout()->count() << "ms, ";
        }
        if (cfg->expiration_time()) {
            stream << "expiration_time: " << cfg->expiration_time()->count() << "ns, ";
        }
        if (cfg->scan_consistency()) {
            stream << "scan_consistency: " << scan_consistency_type_to_string(*cfg->scan_consistency());
        }
    }
    stream << "}";
    return PyUnicode_FromString(stream.str().c_str());
}

static PyTypeObject
init_transaction_config_type()
{
    PyTypeObject r = {};
    r.tp_name = "pycbc_core.transaction_config";
    r.tp_doc = "Transaction configuration";
    r.tp_basicsize = sizeof(pycbc_txns::transaction_config);
    r.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    r.tp_new = pycbc_txns::transaction_config__new__;
    r.tp_dealloc = (destructor)pycbc_txns::transaction_config__dealloc__;
    return r;
}

static PyTypeObject transaction_config_type = init_transaction_config_type();

static PyTypeObject
init_per_transaction_config_type()
{
    PyTypeObject r = {};
    r.tp_name = "pycbc_core.per_transaction_config";
    r.tp_doc = "Per-Transaction configuration";
    r.tp_basicsize = sizeof(pycbc_txns::per_transaction_config);
    r.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    r.tp_new = pycbc_txns::per_transaction_config__new__;
    r.tp_str = (reprfunc)pycbc_txns::per_transaction_config__str__;
    r.tp_dealloc = (destructor)pycbc_txns::per_transaction_config__dealloc__;
    return r;
}

static PyTypeObject per_transaction_config_type = init_per_transaction_config_type();

PyObject*
pycbc_txns::transaction_query_options__new__(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
    PyObject* pyObj_raw = nullptr;
    PyObject* pyObj_ad_hoc = nullptr;
    char* scan_consistency = nullptr;
    PyObject* pyObj_profile_mode = nullptr;
    char* client_context_id = nullptr;
    PyObject* pyObj_scan_wait = nullptr;
    PyObject* pyObj_read_only = nullptr;
    PyObject* pyObj_scan_cap = nullptr;
    PyObject* pyObj_pipeline_batch = nullptr;
    PyObject* pyObj_pipeline_cap = nullptr;
    char* scope = nullptr;
    char* bucket = nullptr;
    PyObject* pyObj_metrics = nullptr;
    PyObject* pyObj_max_parallelism = nullptr;

    const char* kw_list[] = { "raw",       "ad_hoc",    "scan_consistency", "profile_mode",    "client_context_id",
                              "scan_wait", "read_only", "scan_cap",         "pipeline_batch",  "pipeline_cap",
                              "scope",     "bucket",    "metrics",          "max_parallelism", nullptr };
    const char* kw_format = "|OOsOsOOOOOssOO";

    auto self = reinterpret_cast<pycbc_txns::transaction_query_options*>(type->tp_alloc(type, 0));
    self->opts = new tx::transaction_query_options();
    if (!PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     kw_format,
                                     const_cast<char**>(kw_list),
                                     &pyObj_raw,
                                     &pyObj_ad_hoc,
                                     &scan_consistency,
                                     &pyObj_profile_mode,
                                     &client_context_id,
                                     &pyObj_scan_wait,
                                     &pyObj_read_only,
                                     &pyObj_scan_cap,
                                     &pyObj_pipeline_batch,
                                     &pyObj_pipeline_cap,
                                     &scope,
                                     &bucket,
                                     &pyObj_metrics,
                                     &pyObj_max_parallelism)) {
        PyErr_SetString(PyExc_ValueError, "couldn't parse args");
        Py_RETURN_NONE;
    }
    auto opts = reinterpret_cast<pycbc_txns::transaction_query_options*>(self)->opts;
    // TODO: actually set them
    if (nullptr != pyObj_max_parallelism) {
        self->opts->max_parallelism(PyLong_AsUnsignedLongLong(pyObj_max_parallelism));
    }
    if (nullptr != scope && nullptr != bucket) {
        self->opts->bucket_name(bucket);
        self->opts->scope_name(scope);
    }
    if (nullptr != pyObj_pipeline_cap) {
        self->opts->pipeline_cap(PyLong_AsUnsignedLongLong(pyObj_pipeline_cap));
    }
    if (nullptr != pyObj_pipeline_batch) {
        self->opts->pipeline_batch(PyLong_AsUnsignedLongLong(pyObj_pipeline_batch));
    }
    if (nullptr != pyObj_scan_cap) {
        self->opts->scan_cap(PyLong_AsUnsignedLongLong(pyObj_scan_cap));
    }
    if (nullptr != pyObj_read_only) {
        self->opts->readonly(!!PyObject_IsTrue(pyObj_read_only));
    }
    if (nullptr != pyObj_scan_wait) {
        self->opts->scan_wait(std::chrono::milliseconds(PyLong_AsUnsignedLongLong(pyObj_scan_wait) / 1000));
    }
    if (nullptr != client_context_id) {
        self->opts->client_context_id(client_context_id);
    }
    if (nullptr != pyObj_profile_mode) {
        // TODO: look at profile mode enum, and then put it in here.
    }
    if (nullptr != scan_consistency) {
        self->opts->scan_consistency(str_to_scan_consistency_type<couchbase::query_scan_consistency>(scan_consistency));
    }
    if (nullptr != pyObj_ad_hoc) {
        self->opts->ad_hoc(!!PyObject_IsTrue(pyObj_ad_hoc));
    }
    if (nullptr != pyObj_raw) {
        if (!PyDict_Check(pyObj_raw)) {
            PyErr_SetString(PyExc_ValueError, "raw options should be a dict[str, str], where the values are json strings");
            Py_RETURN_NONE;
        }
        PyObject *pyObj_key, *pyObj_value;
        Py_ssize_t pos = 0;

        while (PyDict_Next(pyObj_raw, &pos, &pyObj_key, &pyObj_value)) {
            std::string k;
            if (PyUnicode_Check(pyObj_key)) {
                k = std::string(PyUnicode_AsUTF8(pyObj_key));
            } else {
                PyErr_SetString(PyExc_ValueError, "raw options should be a dict[str, str], where the values are json strings");
                Py_RETURN_NONE;
            }
            if (PyUnicode_Check(pyObj_value) && !k.empty()) {
                couchbase::json_string val{ std::string(PyUnicode_AsUTF8(pyObj_value)) };
                self->opts->raw(k, val);
            } else {
                PyErr_SetString(PyExc_ValueError, "raw options should be a dict[str, str], where the values are json strings");
                Py_RETURN_NONE;
            }
        }
        pyObj_key = nullptr;
        pyObj_value = nullptr;
    }
    return reinterpret_cast<PyObject*>(self);
}

void
pycbc_txns::transaction_query_options__dealloc__(pycbc_txns::transaction_query_options* opts)
{
    delete opts->opts;
    LOG_INFO("dealloc transaction_query_options");
}

static PyTypeObject
init_transaction_query_options_type()
{
    PyTypeObject r = {};
    r.tp_name = "pycbc_core.transaction_query_options";
    r.tp_doc = "Transaction query options";
    r.tp_basicsize = sizeof(pycbc_txns::transaction_query_options);
    r.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    r.tp_new = pycbc_txns::transaction_query_options__new__;
    r.tp_dealloc = (destructor)pycbc_txns::transaction_query_options__dealloc__;
    return r;
}

static PyTypeObject transaction_query_options_type = init_transaction_query_options_type();

PyObject*
pycbc_txns::transaction_get_result__new__(PyTypeObject* type, PyObject*, PyObject*)
{
    auto self = reinterpret_cast<pycbc_txns::transaction_get_result*>(type->tp_alloc(type, 0));
    self->res = tx::transaction_get_result();
    return reinterpret_cast<PyObject*>(self);
}

PyObject*
pycbc_txns::transaction_get_result__str__(pycbc_txns::transaction_get_result* result)
{
    const char* format_string = "transaction_get_result:{key=%s, cas=%llu, value=%s}";
    return PyUnicode_FromFormat(
      format_string, result->res.id().key().c_str(), result->res.cas(), result->res.content<std::string>().c_str());
}

void
pycbc_txns::transaction_get_result__dealloc__(pycbc_txns::transaction_get_result* result)
{
    LOG_INFO("dealloc transaction_get_result");
}

// TODO: a better way later, perhaps an exposed enum like operations
const std::string ID{ "id" };
const std::string CAS{ "cas" };
const std::string VALUE{ "value" };

PyObject*
pycbc_txns::transaction_get_result__get__(pycbc_txns::transaction_get_result* result, PyObject* args)
{
    const char* field_name = nullptr;
    PyObject* default_value = nullptr;
    if (!PyArg_ParseTuple(args, "s|O", &field_name, &default_value)) {
        PyErr_SetString(PyExc_ValueError, "couldn't parse args");
        Py_RETURN_NONE;
    }
    if (ID == field_name) {
        return PyUnicode_FromString(result->res.id().key().c_str());
    }
    if (CAS == field_name) {
        return PyLong_FromUnsignedLongLong(result->res.cas());
    }
    if (VALUE == field_name) {
        return PyBytes_FromString(result->res.content<std::string>().c_str());
    }
    PyErr_SetString(PyExc_ValueError, fmt::format("unknown field_name {}", field_name).c_str());
    Py_RETURN_NONE;
}

static PyMethodDef transaction_get_result_methods[] = {
    { "get", (PyCFunction)pycbc_txns::transaction_get_result__get__, METH_VARARGS, PyDoc_STR("get field in result object") },
    { NULL, NULL, 0, NULL }
};

static PyTypeObject
init_transaction_get_result_type()
{
    PyTypeObject r = {};
    r.tp_name = "pycbc_core.transaction_get_result";
    r.tp_doc = "Result of transaction operation on client";
    r.tp_basicsize = sizeof(pycbc_txns::transaction_get_result);
    r.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    r.tp_new = pycbc_txns::transaction_get_result__new__;
    r.tp_dealloc = (destructor)pycbc_txns::transaction_get_result__dealloc__;
    r.tp_methods = transaction_get_result_methods;
    r.tp_repr = (reprfunc)pycbc_txns::transaction_get_result__str__;
    return r;
}

static PyTypeObject transaction_get_result_type = init_transaction_get_result_type();

PyObject*
pycbc_txns::add_transaction_objects(PyObject* pyObj_module)
{
    PyObject* pyObj_enum_module = PyImport_ImportModule("enum");
    if (!pyObj_enum_module) {
        return nullptr;
    }
    PyObject* pyObj_enum_class = PyObject_GetAttrString(pyObj_enum_module, "Enum");
    PyObject* pyObj_enum_values = PyUnicode_FromString(pycbc_txns::TxOperations::ALL_OPERATIONS());
    PyObject* pyObj_enum_name = PyUnicode_FromString("TransactionOperations");
    PyObject* pyObj_args = PyTuple_Pack(2, pyObj_enum_name, pyObj_enum_values);
    Py_DECREF(pyObj_enum_name);
    Py_DECREF(pyObj_enum_values);

    PyObject* pyObj_kwargs = PyDict_New();
    PyObject_SetItem(pyObj_kwargs, PyUnicode_FromString("module"), PyModule_GetNameObject(pyObj_module));
    PyObject* transaction_operations = PyObject_Call(pyObj_enum_class, pyObj_args, pyObj_kwargs);
    Py_DECREF(pyObj_args);
    Py_DECREF(pyObj_kwargs);

    if (PyModule_AddObject(pyObj_module, "transaction_operations", transaction_operations)) {
        Py_XDECREF(transaction_operations);
        return nullptr;
    }
    Py_DECREF(pyObj_enum_class);
    Py_DECREF(pyObj_enum_module);
    if (PyType_Ready(&transaction_get_result_type) == 0) {
        Py_INCREF(&transaction_get_result_type);
        if (PyModule_AddObject(pyObj_module, "transaction_get_result", reinterpret_cast<PyObject*>(&transaction_get_result_type)) == 0) {
            if (PyType_Ready(&transaction_config_type) == 0) {
                Py_INCREF(&transaction_config_type);
                if (PyModule_AddObject(pyObj_module, "transaction_config", reinterpret_cast<PyObject*>(&transaction_config_type)) == 0) {
                    if (PyType_Ready(&transaction_query_options_type) == 0) {
                        Py_INCREF(&transaction_query_options_type);
                        if (PyModule_AddObject(pyObj_module,
                                               "transaction_query_options",
                                               reinterpret_cast<PyObject*>(&transaction_query_options_type)) == 0) {
                            if (PyType_Ready(&per_transaction_config_type) == 0) {
                                Py_INCREF(&per_transaction_config_type);
                                if (PyModule_AddObject(pyObj_module,
                                                       "per_transaction_config",
                                                       reinterpret_cast<PyObject*>(&per_transaction_config_type)) == 0) {
                                    return pyObj_module;
                                }
                                Py_DECREF(&per_transaction_config_type);
                            }
                        }
                        Py_DECREF(&transaction_query_options_type);
                    }
                }
                Py_DECREF(&transaction_config_type);
            }
        }
        Py_DECREF(&transaction_get_result_type);
    }
    Py_DECREF(pyObj_module);
    return nullptr;
}

PyObject*
pycbc_txns::create_transactions([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    // we expect it to be called like:
    // create_transactions(conn, config)
    PyObject* pyObj_conn = nullptr;
    PyObject* pyObj_config = nullptr;
    const char* kw_list[] = { "conn", "config", nullptr };
    const char* kw_format = "O!O";
    int ret =
      PyArg_ParseTupleAndKeywords(args, kwargs, kw_format, const_cast<char**>(kw_list), &PyCapsule_Type, &pyObj_conn, &pyObj_config);

    if (!ret) {
        PyErr_SetString(PyExc_ValueError, "couldn't parse args");
        Py_RETURN_NONE;
    }
    if (nullptr == pyObj_conn) {
        PyErr_SetString(PyExc_ValueError, "expected a connection object");
        Py_RETURN_NONE;
    }
    if (nullptr == pyObj_config) {
        PyErr_SetString(PyExc_ValueError, "expected a TransactionConfig object");
        Py_RETURN_NONE;
    }

    auto txns = new pycbc_txns::transactions(pyObj_conn, *reinterpret_cast<pycbc_txns::transaction_config*>(pyObj_config)->cfg);
    PyObject* pyObj_txns = PyCapsule_New(txns, "txns_", dealloc_transactions);
    return pyObj_txns;
}

PyObject*
pycbc_txns::destroy_transactions([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* pyObj_txns = nullptr;
    const char* kw_list[] = { "txns", nullptr };
    const char* kw_format = "O!";
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, kw_format, const_cast<char**>(kw_list), &PyCapsule_Type, &pyObj_txns)) {
        PyErr_SetString(PyExc_ValueError, "couldn't parse args");
        Py_RETURN_NONE;
    }
    if (nullptr == pyObj_txns) {
        PyErr_SetString(PyExc_ValueError, "expected a transactions object");
        Py_RETURN_NONE;
    }
    auto txns = reinterpret_cast<pycbc_txns::transactions*>(PyCapsule_GetPointer(pyObj_txns, "txns_"));
    if (nullptr == txns) {
        PyErr_SetString(PyExc_ValueError, "passed null transactions");
        Py_RETURN_NONE;
    }
    Py_BEGIN_ALLOW_THREADS txns->txns->close();
    Py_END_ALLOW_THREADS Py_RETURN_NONE;
}

void
handle_returning_void(PyObject* pyObj_callback,
                      PyObject* pyObj_errback,
                      std::shared_ptr<std::promise<PyObject*>> barrier,
                      std::exception_ptr err)
{
    auto state = PyGILState_Ensure();
    PyObject* args = nullptr;
    PyObject* func = nullptr;
    if (err) {
        // TODO: flesh out exception handling!
        if (nullptr == pyObj_errback) {
            Py_INCREF(Py_None);
            barrier->set_value(Py_None);
            PyErr_SetString(PyExc_ValueError, "Placeholder for coming transactions exceptions");
        } else {
            args = PyTuple_Pack(1, Py_None);
            func = pyObj_errback;
        }
    } else {
        if (nullptr == pyObj_callback) {
            Py_INCREF(Py_None);
            barrier->set_value(Py_None);
        } else {
            args = PyTuple_Pack(1, Py_None);
            func = pyObj_callback;
        }
    }
    if (nullptr != func) {
        PyObject_CallObject(func, args);
        Py_DECREF(pyObj_errback);
        Py_DECREF(pyObj_callback);
    }
    PyGILState_Release(state);
}

PyObject*
err_to_error_context(const tx::transaction_operation_failed& err)
{
    PyObject* pyObj_error_ctx = PyDict_New();
    // TODO: fill in some context.  Probably the details are not important to our
    //       users, but worth having in there.  For now, just some basic stuff.
    //       Eventually the exception will have more info, which we can put here.
    PyObject* pyObj_value = PyUnicode_FromString(err.what());
    PyDict_SetItemString(pyObj_error_ctx, "message", pyObj_value);
    Py_DECREF(pyObj_value);
    pyObj_value = PyUnicode_FromString("transaction_op_failed");
    PyDict_SetItemString(pyObj_error_ctx, "type", pyObj_value);
    return pyObj_error_ctx;
}

PyObject*
err_to_error_context(const tx::transaction_exception& err)
{
    PyObject* pyObj_error_ctx = PyDict_New();
    const char* failure_type = "Unknown";
    switch (err.type()) {
        case tx::failure_type::FAIL:
            failure_type = "Fail";
            break;
        case tx::failure_type::COMMIT_AMBIGUOUS:
            failure_type = "Commit Ambiguous";
            break;
        case tx::failure_type::EXPIRY:
            failure_type = "Expiry";
            break;
    }
    PyObject* tmp = PyUnicode_FromString(failure_type);
    PyDict_SetItemString(pyObj_error_ctx, "failure_type", tmp);
    Py_DECREF(tmp);
    tmp = PyUnicode_FromString(err.what());
    PyDict_SetItemString(pyObj_error_ctx, "message", tmp);
    Py_DECREF(tmp);
    return pyObj_error_ctx;
}

PyObject*
extract_error_context(std::exception_ptr err)
{
    PyObject* retval = PyDict_New();
    // TODO: populate a dict with info on the exception,
    //   however we don't have much in there now.
    assert(!!err);
    try {
        std::rethrow_exception(err);
        return retval;
    } catch (const tx::transaction_exception e) {
        return err_to_error_context(e);
    } catch (const tx::transaction_operation_failed ex) {
        return err_to_error_context(ex);
    } catch (const std::exception& exc) {
        PyObject* pyObj_value = PyUnicode_FromString(exc.what());
        PyDict_SetItemString(retval, "message", pyObj_value);
        Py_DECREF(pyObj_value);
        pyObj_value = PyUnicode_FromString("transaction_op_failed");
        PyDict_SetItemString(retval, "type", pyObj_value);
        return retval;
    }
}

void
handle_returning_transaction_get_result(PyObject* pyObj_callback,
                                        PyObject* pyObj_errback,
                                        std::shared_ptr<std::promise<PyObject*>> barrier,
                                        std::exception_ptr err,
                                        std::optional<couchbase::transactions::transaction_get_result> res)
{
    // TODO: flesh out transaction_get_result and exceptions...
    auto state = PyGILState_Ensure();
    PyObject* args = nullptr;
    PyObject* func = nullptr;
    if (err) {
        if (nullptr == pyObj_errback) {
            barrier->set_exception(err);
        } else {
            args = PyTuple_Pack(1, extract_error_context(err));
            func = pyObj_errback;
        }
    } else {
        PyObject* transaction_get_result_obj = PyObject_CallObject(reinterpret_cast<PyObject*>(&transaction_get_result_type), nullptr);
        auto result = reinterpret_cast<pycbc_txns::transaction_get_result*>(transaction_get_result_obj);
        // now lets copy it in
        // TODO: ideally we'd have a move constructor for transaction_get_result, but for now...
        result->res = res.value();
        if (nullptr == pyObj_callback) {
            barrier->set_value(transaction_get_result_obj);
        } else {
            args = PyTuple_Pack(1, transaction_get_result_obj);
            func = pyObj_callback;
        }
    }
    if (nullptr != func) {
        PyObject_CallObject(func, args);
        Py_DECREF(pyObj_errback);
        Py_DECREF(pyObj_callback);
    }
    PyGILState_Release(state);
}
PyObject*
pycbc_txns::transaction_query_op([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* pyObj_ctx = nullptr;
    PyObject* pyObj_options = nullptr;
    PyObject* pyObj_callback = nullptr;
    PyObject* pyObj_errback = nullptr;
    const char* statement = nullptr;

    const char* kw_list[] = { "ctx", "statement", "options", "callback", "errback", nullptr };
    const char* kw_format = "O!sO|OO";
    if (!PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     kw_format,
                                     const_cast<char**>(kw_list),
                                     &PyCapsule_Type,
                                     &pyObj_ctx,
                                     &statement,
                                     &pyObj_options,
                                     &pyObj_callback,
                                     &pyObj_errback)) {
        PyErr_SetString(PyExc_ValueError, "couldn't parse args");
        Py_RETURN_NONE;
    }
    if (nullptr == pyObj_ctx) {
        PyErr_SetString(PyExc_ValueError, "expected attempt_context");
        Py_RETURN_NONE;
    }
    auto ctx = reinterpret_cast<pycbc_txns::attempt_context*>(PyCapsule_GetPointer(pyObj_ctx, "ctx_"));
    if (nullptr == ctx) {
        PyErr_SetString(PyExc_ValueError, "passed null attempt_context");
        Py_RETURN_NONE;
    }
    if (nullptr == statement) {
        PyErr_SetString(PyExc_ValueError, "expected query statement");
        Py_RETURN_NONE;
    }
    if (nullptr == pyObj_options) {
        PyErr_SetString(PyExc_ValueError, "expected options");
        Py_RETURN_NONE;
    }
    auto opt = reinterpret_cast<pycbc_txns::transaction_query_options*>(pyObj_options);
    Py_XINCREF(pyObj_callback);
    Py_XINCREF(pyObj_errback);
    Py_XINCREF(pyObj_options);
    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();
    Py_BEGIN_ALLOW_THREADS ctx->ctx.query(statement,
                                          *opt->opts,
                                          [pyObj_options, pyObj_callback, pyObj_errback, barrier](
                                            std::exception_ptr err, std::optional<couchbase::operations::query_response> resp) {
                                              auto state = PyGILState_Ensure();
                                              PyObject* args = nullptr;
                                              PyObject* func = nullptr;
                                              if (err) {
                                                  // TODO: flesh out exception handling!
                                                  if (nullptr == pyObj_errback) {
                                                      barrier->set_exception(err);
                                                  } else {
                                                      args = PyTuple_Pack(1, extract_error_context(err));
                                                      func = pyObj_errback;
                                                  }
                                              } else {
                                                  PyObject* json = PyBytes_FromString(resp->ctx.http_body.c_str());
                                                  if (nullptr == pyObj_callback) {
                                                      barrier->set_value(json);
                                                  } else {
                                                      args = PyTuple_Pack(1, json);
                                                      func = pyObj_callback;
                                                      Py_DECREF(json);
                                                  }
                                              }
                                              if (nullptr != func) {
                                                  PyObject_CallObject(func, args);
                                                  Py_DECREF(pyObj_errback);
                                                  Py_DECREF(pyObj_callback);
                                              }
                                              Py_DECREF(pyObj_options);
                                              PyGILState_Release(state);
                                          });
    Py_END_ALLOW_THREADS if (nullptr == pyObj_callback || nullptr == pyObj_errback)
    {
        PyObject* ret = nullptr;
        std::string msg;
        Py_BEGIN_ALLOW_THREADS
        try {
            ret = f.get();
        } catch (const std::exception& e) {
            // ideally we form a python exception that uses the info in
            // extract_error_context(std::current_exception())
            msg = e.what();
        }
        Py_END_ALLOW_THREADS if (!msg.empty())
        {
            PyErr_SetString(PyExc_ValueError, msg.c_str());
        }
        return ret;
    }
    Py_RETURN_NONE;
}

PyObject*
pycbc_txns::transaction_op([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* pyObj_ctx = nullptr;
    PyObject* pyObj_callback = nullptr;
    PyObject* pyObj_errback = nullptr;
    PyObject* pyObj_txn_get_result = nullptr;
    PyObject* pyObj_value = nullptr;
    const char* bucket = nullptr;
    const char* scope = nullptr;
    const char* collection = nullptr;
    const char* key = nullptr;
    std::string value;
    TxOperations::TxOperationType op_type = TxOperations::UNKNOWN;
    const char* kw_list[] = { "ctx",      "bucket",  "scope", "collection_name", "key",  "op",
                              "callback", "errback", "value", "txn_get_result",  nullptr };
    const char* kw_format = "O!|ssssIOOSO";

    int ret = PyArg_ParseTupleAndKeywords(args,
                                          kwargs,
                                          kw_format,
                                          const_cast<char**>(kw_list),
                                          &PyCapsule_Type,
                                          &pyObj_ctx,
                                          &bucket,
                                          &scope,
                                          &collection,
                                          &key,
                                          &op_type,
                                          &pyObj_callback,
                                          &pyObj_errback,
                                          &pyObj_value,
                                          &pyObj_txn_get_result);
    if (!ret) {
        PyErr_SetString(PyExc_ValueError, "couldn't parse args");
        Py_RETURN_NONE;
    }
    if (nullptr != pyObj_value) {
        value = PyBytes_AsString(pyObj_value);
        LOG_INFO("value is {}", value);
    }
    if (nullptr == pyObj_ctx) {
        PyErr_SetString(PyExc_ValueError, "no attempt_context passed in");
        Py_RETURN_NONE;
    }
    auto ctx = reinterpret_cast<pycbc_txns::attempt_context*>(PyCapsule_GetPointer(pyObj_ctx, "ctx_"));
    if (nullptr == ctx) {
        PyErr_SetString(PyExc_ValueError, "passed null attempt_context");
        Py_RETURN_NONE;
    }

    Py_XINCREF(pyObj_callback);
    Py_XINCREF(pyObj_errback);

    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();
    switch (op_type) {
        case TxOperations::GET: {
            if (nullptr == bucket || nullptr == scope || nullptr == collection || nullptr == key) {
                PyErr_SetString(PyExc_ValueError, "couldn't create document id for get");
                Py_RETURN_NONE;
            }
            couchbase::document_id id{ bucket, scope, collection, key };
            Py_BEGIN_ALLOW_THREADS ctx->ctx.get_optional(
              id, [barrier, pyObj_callback, pyObj_errback](std::exception_ptr err, std::optional<tx::transaction_get_result> res) {
                  handle_returning_transaction_get_result(pyObj_callback, pyObj_errback, barrier, err, res);
              });
            Py_END_ALLOW_THREADS break;
        }
        case TxOperations::INSERT: {
            if (nullptr == bucket || nullptr == scope || nullptr == collection || nullptr == key) {
                PyErr_SetString(PyExc_ValueError, "couldn't create document id for insert");
                Py_RETURN_NONE;
            }
            couchbase::document_id id{ bucket, scope, collection, key };
            if (nullptr == pyObj_value) {
                PyErr_SetString(PyExc_ValueError, fmt::format("no value given for an insert of key {}", id.key()).c_str());
                Py_RETURN_NONE;
            }
            Py_BEGIN_ALLOW_THREADS ctx->ctx.insert(
              id, value, [barrier, pyObj_callback, pyObj_errback](std::exception_ptr err, std::optional<tx::transaction_get_result> res) {
                  handle_returning_transaction_get_result(pyObj_callback, pyObj_errback, barrier, err, res);
              });
            Py_END_ALLOW_THREADS break;
        }
        case TxOperations::REPLACE: {
            if (nullptr == pyObj_value) {
                PyErr_SetString(PyExc_ValueError, "replace expects a value");
                Py_RETURN_NONE;
            }
            if (nullptr == pyObj_txn_get_result || 0 == PyObject_TypeCheck(pyObj_txn_get_result, &transaction_get_result_type)) {
                PyErr_SetString(PyExc_ValueError, "replace expects to be passed a transaction_get_result");
                Py_RETURN_NONE;
            }
            auto tx_get_result = reinterpret_cast<pycbc_txns::transaction_get_result*>(pyObj_txn_get_result);
            Py_BEGIN_ALLOW_THREADS ctx->ctx.replace(
              tx_get_result->res,
              value,
              [pyObj_callback, pyObj_errback, barrier](std::exception_ptr err, std::optional<tx::transaction_get_result> res) {
                  handle_returning_transaction_get_result(pyObj_callback, pyObj_errback, barrier, err, res);
              });
            Py_END_ALLOW_THREADS break;
        }
        case TxOperations::REMOVE: {
            if (nullptr == pyObj_txn_get_result || 0 == PyObject_TypeCheck(pyObj_txn_get_result, &transaction_get_result_type)) {
                PyErr_SetString(PyExc_ValueError, "remove expects to be passed a transaction_get_result");
                Py_RETURN_NONE;
            }
            auto tx_get_result = reinterpret_cast<pycbc_txns::transaction_get_result*>(pyObj_txn_get_result);
            Py_BEGIN_ALLOW_THREADS ctx->ctx.remove(tx_get_result->res, [pyObj_callback, pyObj_errback, barrier](std::exception_ptr err) {
                handle_returning_void(pyObj_callback, pyObj_errback, barrier, err);
            });
            Py_END_ALLOW_THREADS break;
        }
        default:
            // return error!
            LOG_INFO("unknown op {}", op_type);
            PyErr_SetString(PyExc_ValueError, "unknown txn operation");
    }
    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        PyObject* ret = nullptr;
        std::string msg;
        Py_BEGIN_ALLOW_THREADS
        try {
            ret = f.get();
        } catch (const std::exception& e) {
            // ideally we form a python exception that uses the info in
            // extract_error_context(std::current_exception())
            msg = e.what();
        }
        Py_END_ALLOW_THREADS if (!msg.empty())
        {
            PyErr_SetString(PyExc_ValueError, msg.c_str());
        }
        return ret;
    }
    Py_RETURN_NONE;
}

PyObject*
transaction_result_to_dict(std::optional<tx::transaction_result> res)
{
    PyObject* dict = PyDict_New();
    if (res) {
        PyObject* tmp = PyUnicode_FromString(res->transaction_id.c_str());
        PyDict_SetItemString(dict, "transaction_id", tmp);
        Py_DECREF(tmp);
        PyDict_SetItemString(dict, "unstaging_complete", res->unstaging_complete ? Py_True : Py_False);
    }
    return dict;
}

PyObject*
pycbc_txns::run_transactions([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
    // we expect to be called like:
    // run_transactions(txns, logic, callback, errback) // maybe a per_txn_config as well.
    PyObject* pyObj_txns = nullptr;
    PyObject* pyObj_logic = nullptr;
    PyObject* pyObj_callback = nullptr;
    PyObject* pyObj_errback = nullptr;
    PyObject* pyObj_per_txn_config = nullptr;
    const char* kw_list[] = { "txns", "logic", "callback", "errback", "per_txn_config", nullptr };
    const char* kw_format = "O!O|OOO";
    int ret = PyArg_ParseTupleAndKeywords(args,
                                          kwargs,
                                          kw_format,
                                          const_cast<char**>(kw_list),
                                          &PyCapsule_Type,
                                          &pyObj_txns,
                                          &pyObj_logic,
                                          &pyObj_callback,
                                          &pyObj_errback,
                                          &pyObj_per_txn_config);
    if (!ret) {
        PyErr_SetString(PyExc_ValueError, "couldn't parse args");
        return nullptr;
    }
    auto txns = reinterpret_cast<pycbc_txns::transactions*>(PyCapsule_GetPointer(pyObj_txns, "txns_"));
    if (nullptr == txns) {
        PyErr_SetString(PyExc_ValueError, "passed null transactions");
        return nullptr;
    }
    if (nullptr != pyObj_per_txn_config) {
        if (!PyObject_IsInstance(pyObj_per_txn_config, reinterpret_cast<PyObject*>(&per_transaction_config_type))) {
            PyErr_SetString(PyExc_ValueError, "expected a valid per_transaction_config object");
            return nullptr;
        }
    }
    // we need the callback, errback, and logic to all stick around, so...
    Py_XINCREF(pyObj_errback);
    Py_XINCREF(pyObj_callback);
    Py_XINCREF(pyObj_logic);
    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();
    auto logic = [pyObj_logic](tx::async_attempt_context& ctx) {
        auto state = PyGILState_Ensure();
        auto py_ctx = new pycbc_txns::attempt_context(ctx);
        PyObject* pyObj_ctx = PyCapsule_New(py_ctx, "ctx_", dealloc_attempt_context);
        PyObject* args = PyTuple_Pack(1, pyObj_ctx);
        PyErr_Clear();
        PyObject_CallObject(pyObj_logic, args);
        // ideally we get some info from the exception and pass it into the exception we throw below.  For
        // now, lets just check if one did occur, and make sure we throw a c++ exception so we rollback.
        bool py_error = (nullptr != PyErr_Occurred());
        PyGILState_Release(state);
        // now we raise an exception so we will rollback
        if (py_error) {
            throw std::runtime_error("Python error caught - rolling back");
        }
    };
    auto cb = [pyObj_callback, pyObj_errback, barrier, pyObj_logic](std::optional<tx::transaction_exception> err,
                                                                    std::optional<tx::transaction_result> res) {
        auto state = PyGILState_Ensure();
        PyObject* args = nullptr;
        PyObject* func = nullptr;
        if (err) {
            if (nullptr == pyObj_errback) {
                barrier->set_exception(std::make_exception_ptr(*err));
            } else {
                args = PyTuple_Pack(1, err_to_error_context(*err));
                func = pyObj_errback;
            }
        } else {
            PyObject* ret = transaction_result_to_dict(res);
            if (nullptr == pyObj_callback) {
                barrier->set_value(ret);
            } else {
                args = PyTuple_Pack(1, ret);
                func = pyObj_callback;
            }
        }
        if (nullptr != func) {
            PyObject_CallObject(func, args);
            Py_DECREF(pyObj_errback);
            Py_DECREF(pyObj_callback);
        }
        Py_XDECREF(pyObj_logic);
        PyGILState_Release(state);
    };
    tx::per_transaction_config* cfg = nullptr;
    if (nullptr != pyObj_per_txn_config && Py_None != pyObj_per_txn_config) {
        cfg = reinterpret_cast<pycbc_txns::per_transaction_config*>(pyObj_per_txn_config)->cfg;
    }
    Py_BEGIN_ALLOW_THREADS if (nullptr == cfg)
    {
        txns->txns->run(logic, cb);
    }
    else
    {
        auto expiry = cfg->expiration_time();
        LOG_INFO("calling transactions.run with expiry {}ms", expiry.has_value() ? expiry->count() : 0);
        txns->txns->run(*cfg, logic, cb);
    }
    Py_END_ALLOW_THREADS if (nullptr == pyObj_callback || nullptr == pyObj_errback)
    {
        std::string msg;
        PyObject* retval = nullptr;
        Py_BEGIN_ALLOW_THREADS
        try {
            retval = f.get();
        } catch (const std::exception& e) {
            // ideally we form a python exception that uses the info in
            // extract_error_context(std::current_exception())
            msg = e.what();
        }
        Py_END_ALLOW_THREADS if (!msg.empty())
        {
            PyErr_SetString(PyExc_ValueError, msg.c_str());
            return nullptr;
        }
        else
        {
            return retval;
        }
    }
    Py_RETURN_NONE;
}
