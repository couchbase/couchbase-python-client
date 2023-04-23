
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

#include "transactions.hxx"
#include "../n1ql.hxx"
#include "../utils.hxx"
#include <core/cluster.hxx>
#include <core/transactions/transaction_get_result.hxx>
#include <core/transactions/durability_level.hxx>
#include <core/transactions/internal/exceptions_internal.hxx>
#include <core/operations.hxx>
#include <couchbase/query_scan_consistency.hxx>
#include <sstream>

void
add_to_dict(PyObject* dict, std::string key, std::string value)
{
    PyObject* pyObj_value = PyUnicode_FromString(value.c_str());
    PyDict_SetItemString(dict, key.c_str(), pyObj_value);
    Py_DECREF(pyObj_value);
}

void
add_to_dict(PyObject* dict, std::string key, int64_t value)
{
    PyObject* pyObj_val = PyLong_FromLongLong(value);
    PyDict_SetItemString(dict, key.c_str(), pyObj_val);
    Py_DECREF(pyObj_val);
}

void
add_to_dict(PyObject* dict, std::string key, bool value)
{
    PyDict_SetItemString(dict, key.c_str(), value ? Py_True : Py_False);
}

void
pycbc_txns::dealloc_transactions(PyObject* obj)
{
    auto txns = reinterpret_cast<pycbc_txns::transactions*>(PyCapsule_GetPointer(obj, "txns_"));
    txns->txns->close();
    delete txns->txns;
    CB_LOG_DEBUG("dealloc transactions");
}

void
pycbc_txns::dealloc_attempt_context(PyObject* obj)
{
    auto ctx = reinterpret_cast<pycbc_txns::attempt_context*>(PyCapsule_GetPointer(obj, "ctx_"));
    delete ctx;
    CB_LOG_DEBUG("dealloc attempt_context");
}

/* pycbc_txns::transaction_config type methods */

void
pycbc_txns::transaction_config__dealloc__(pycbc_txns::transaction_config* cfg)
{
    delete cfg->cfg;
    Py_TYPE(cfg)->tp_free((PyObject*)cfg);
    CB_LOG_DEBUG("dealloc transaction_config");
}

PyObject*
pycbc_txns::transaction_config__to_dict__(PyObject* self)
{
    auto conf = reinterpret_cast<pycbc_txns::transaction_config*>(self);
    PyObject* retval = PyDict_New();
    add_to_dict(retval, "durability_level", static_cast<int64_t>(conf->cfg->durability_level()));
    add_to_dict(retval, "cleanup_window", static_cast<int64_t>(conf->cfg->cleanup_config().cleanup_window().count()));
    if (conf->cfg->kv_timeout()) {
        add_to_dict(retval, "kv_timeout", static_cast<int64_t>(conf->cfg->kv_timeout()->count()));
    }
    add_to_dict(retval, "expiration_time", static_cast<int64_t>(conf->cfg->expiration_time().count()));
    add_to_dict(retval, "cleanup_lost_attempts", conf->cfg->cleanup_config().cleanup_lost_attempts());
    add_to_dict(retval, "cleanup_client_attempts", conf->cfg->cleanup_config().cleanup_client_attempts());
    add_to_dict(retval, "scan_consistency", scan_consistency_type_to_string(conf->cfg->query_config().scan_consistency()));
    if (conf->cfg->metadata_collection()) {
        std::string meta = fmt::format("{}.{}.{}",
                                       conf->cfg->metadata_collection()->bucket,
                                       conf->cfg->metadata_collection()->scope,
                                       conf->cfg->metadata_collection()->collection);
        add_to_dict(retval, "metadata_collection", meta);
    }
    return retval;
}

static PyMethodDef transaction_config_methods[] = {
    { "to_dict", (PyCFunction)pycbc_txns::transaction_config__to_dict__, METH_NOARGS, PyDoc_STR("transaction_config as a dict") },
    { NULL, NULL, 0, NULL }
};

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
    const char* kw_format = "|OOOOOOssss";
    auto self = reinterpret_cast<pycbc_txns::transaction_config*>(type->tp_alloc(type, 0));

    self->cfg = new tx::transactions_config();

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
        self->cfg->durability_level(static_cast<couchbase::durability_level>(PyLong_AsUnsignedLong(durability_level)));
    }
    if (nullptr != cleanup_window) {
        self->cfg->cleanup_config().cleanup_window(std::chrono::microseconds(PyLong_AsUnsignedLongLong(cleanup_window)));
    }
    if (nullptr != kv_timeout) {
        self->cfg->kv_timeout(std::chrono::milliseconds(PyLong_AsUnsignedLongLong(kv_timeout) / 1000));
    }
    if (nullptr != expiration_time) {
        self->cfg->expiration_time(std::chrono::microseconds(PyLong_AsUnsignedLongLong(expiration_time)));
    }
    if (nullptr != cleanup_lost_attempts) {
        self->cfg->cleanup_config().cleanup_lost_attempts(!!PyObject_IsTrue(cleanup_lost_attempts));
    }
    if (nullptr != cleanup_client_attempts) {
        self->cfg->cleanup_config().cleanup_client_attempts(!!PyObject_IsTrue(cleanup_client_attempts));
    }
    if (nullptr != metadata_bucket && nullptr != metadata_scope && nullptr != metadata_collection) {
        auto keyspace = tx::transaction_keyspace{ metadata_bucket, metadata_scope, metadata_collection };
        self->cfg->metadata_collection(keyspace);
    }
    if (nullptr != scan_consistency) {
        self->cfg->query_config().scan_consistency(str_to_scan_consistency_type<couchbase::query_scan_consistency>(scan_consistency));
    }
    return reinterpret_cast<PyObject*>(self);
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
    r.tp_methods = transaction_config_methods;
    return r;
}

static PyTypeObject transaction_config_type = init_transaction_config_type();

/* pycbc_txns::transaction_options type methods */

void
pycbc_txns::transaction_options__dealloc__(pycbc_txns::transaction_options* opts)
{
    delete opts->opts;
    Py_TYPE(opts)->tp_free((PyObject*)opts);
    CB_LOG_DEBUG("dealloc transaction_options");
}

PyObject*
pycbc_txns::transaction_options__to_dict__(PyObject* self)
{
    auto opts = reinterpret_cast<pycbc_txns::transaction_options*>(self);
    PyObject* retval = PyDict_New();
    if (opts->opts->kv_timeout()) {
        add_to_dict(retval, "kv_timeout", static_cast<int64_t>(opts->opts->kv_timeout()->count()));
    }
    if (opts->opts->expiration_time()) {
        add_to_dict(retval, "expiration_time", static_cast<int64_t>(opts->opts->expiration_time()->count()));
    }
    if (opts->opts->durability_level()) {
        add_to_dict(retval, "durability_level", static_cast<int64_t>(opts->opts->durability_level().value()));
    }
    if (opts->opts->scan_consistency()) {
        add_to_dict(retval, "scan_consistency", scan_consistency_type_to_string(*opts->opts->scan_consistency()));
    }
    if (opts->opts->metadata_collection()) {
        std::string meta = fmt::format("{}.{}.{}",
                                       opts->opts->metadata_collection()->bucket,
                                       opts->opts->metadata_collection()->scope,
                                       opts->opts->metadata_collection()->collection);
        add_to_dict(retval, "metadata_collection", meta);
    }
    return retval;
}

PyObject*
pycbc_txns::transaction_options__str__(PyObject* self)
{
    auto opts = reinterpret_cast<pycbc_txns::transaction_options*>(self)->opts;
    std::stringstream stream;
    stream << "transaction_options{";
    if (nullptr != opts) {
        if (opts->durability_level()) {
            stream << "durability: " << tx_core::durability_level_to_string(*opts->durability_level()) << ", ";
        }
        if (opts->kv_timeout()) {
            stream << "kv_timeout: " << opts->kv_timeout()->count() << "ms, ";
        }
        if (opts->expiration_time()) {
            stream << "expiration_time: " << opts->expiration_time()->count() << "ns, ";
        }
        if (opts->scan_consistency()) {
            stream << "scan_consistency: " << scan_consistency_type_to_string(*opts->scan_consistency());
        }
    }
    stream << "}";
    return PyUnicode_FromString(stream.str().c_str());
}

static PyMethodDef transaction_options_methods[] = {
    { "to_dict", (PyCFunction)pycbc_txns::transaction_options__to_dict__, METH_NOARGS, PyDoc_STR("transaction_options as a dict") },
    { NULL, NULL, 0, NULL }
};

PyObject*
pycbc_txns::transaction_options__new__(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
    PyObject* durability_level = nullptr;
    PyObject* kv_timeout = nullptr;
    PyObject* expiration_time = nullptr;
    char* scan_consistency = nullptr;
    char* metadata_bucket = nullptr;
    char* metadata_scope = nullptr;
    char* metadata_collection = nullptr;

    const char* kw_list[] = { "durability_level", "kv_timeout",     "expiration_time",     "scan_consistency",
                              "metadata_bucket",  "metadata_scope", "metadata_collection", nullptr };
    const char* kw_format = "|OOOssss";
    auto self = reinterpret_cast<pycbc_txns::transaction_options*>(type->tp_alloc(type, 0));

    self->opts = new tx::transaction_options();
    CB_LOG_DEBUG("transaction_options__new__ called");
    if (!PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     kw_format,
                                     const_cast<char**>(kw_list),
                                     &durability_level,
                                     &kv_timeout,
                                     &expiration_time,
                                     &scan_consistency,
                                     &metadata_bucket,
                                     &metadata_scope,
                                     &metadata_collection)) {
        PyErr_SetString(PyExc_ValueError, "couldn't parse args");
        Py_RETURN_NONE;
    }
    if (nullptr != durability_level) {
        self->opts->durability_level(static_cast<couchbase::durability_level>(PyLong_AsUnsignedLong(durability_level)));
    }
    if (nullptr != kv_timeout) {
        self->opts->kv_timeout(std::chrono::milliseconds(PyLong_AsUnsignedLongLong(kv_timeout) / 1000));
    }
    if (nullptr != expiration_time) {
        self->opts->expiration_time(std::chrono::microseconds(PyLong_AsUnsignedLongLong(expiration_time)));
    }
    if (nullptr != scan_consistency) {
        self->opts->scan_consistency(str_to_scan_consistency_type<couchbase::query_scan_consistency>(scan_consistency));
    }
    if (nullptr != metadata_bucket && nullptr != metadata_scope && nullptr != metadata_collection) {
        auto keyspace = tx::transaction_keyspace{ metadata_bucket, metadata_scope, metadata_collection };
        self->opts->metadata_collection(keyspace);
    }

    return reinterpret_cast<PyObject*>(self);
}

static PyTypeObject
init_transaction_options_type()
{
    PyTypeObject r = {};
    r.tp_name = "pycbc_core.transaction_options";
    r.tp_doc = "Transaction options";
    r.tp_basicsize = sizeof(pycbc_txns::transaction_options);
    r.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    r.tp_new = pycbc_txns::transaction_options__new__;
    r.tp_str = (reprfunc)pycbc_txns::transaction_options__str__;
    r.tp_dealloc = (destructor)pycbc_txns::transaction_options__dealloc__;
    r.tp_methods = transaction_options_methods;
    return r;
}

static PyTypeObject transaction_options_type = init_transaction_options_type();

/* pycbc_txns::transaction_query_options type methods */

void
pycbc_txns::transaction_query_options__dealloc__(pycbc_txns::transaction_query_options* opts)
{
    delete opts->opts;
    Py_TYPE(opts)->tp_free((PyObject*)opts);
    CB_LOG_DEBUG("dealloc transaction_query_options");
}

PyObject*
pycbc_txns::transaction_query_options__to_dict__(PyObject* self)
{
    auto opts = reinterpret_cast<pycbc_txns::transaction_query_options*>(self);
    PyObject* retval = PyDict_New();
    auto query_opts = opts->opts->get_query_options().build();
    add_to_dict(retval, "adhoc", query_opts.adhoc);
    add_to_dict(retval, "metrics", query_opts.metrics);
    add_to_dict(retval, "read_only", query_opts.readonly);
    add_to_dict(retval, "flex_index", query_opts.flex_index);
    add_to_dict(retval, "preserve_expiry", query_opts.preserve_expiry);
    if (query_opts.max_parallelism.has_value()) {
        add_to_dict(retval, "max_parallelism", static_cast<int64_t>(query_opts.max_parallelism.value()));
    }
    if (query_opts.scan_cap.has_value()) {
        add_to_dict(retval, "scan_cap", static_cast<int64_t>(query_opts.scan_cap.value()));
    }
    if (query_opts.scan_wait) {
        add_to_dict(retval, "scan_wait", static_cast<int64_t>(query_opts.scan_wait->count()));
    }
    if (query_opts.pipeline_batch.has_value()) {
        add_to_dict(retval, "pipeline_batch", static_cast<int64_t>(query_opts.pipeline_batch.value()));
    }
    if (query_opts.pipeline_cap.has_value()) {
        add_to_dict(retval, "pipeline_cap", static_cast<int64_t>(query_opts.pipeline_cap.value()));
    }
    if (query_opts.client_context_id.has_value()) {
        add_to_dict(retval, "client_context_id", query_opts.client_context_id.value());
    }
    if (query_opts.scan_consistency.has_value()) {
        add_to_dict(retval, "scan_consistency", scan_consistency_type_to_string(query_opts.scan_consistency.value()));
    }
    add_to_dict(retval, "profile", profile_mode_to_str(query_opts.profile));

    if (!query_opts.raw.empty()) {
        PyObject* raw = PyDict_New();
        for (auto const& [key, val] : query_opts.raw) {
            auto val_str = binary_to_string(val);
            add_to_dict(raw, key, val_str);
        }
        PyDict_SetItemString(retval, "raw", raw);
        Py_DECREF(raw);
    }

    if (!query_opts.positional_parameters.empty()) {
        PyObject* pyObj_pos = PyList_New(0);
        for (auto& val : query_opts.positional_parameters) {
            auto val_str = binary_to_string(val);
            PyObject* pyObj_val = PyUnicode_FromString(val_str.c_str());
            PyList_Append(pyObj_pos, pyObj_val);
            Py_DECREF(pyObj_val);
        }
        PyDict_SetItemString(retval, "positional_parameters", pyObj_pos);
        Py_DECREF(pyObj_pos);
    }
    if (!query_opts.named_parameters.empty()) {
        PyObject* pyObj_named = PyDict_New();
        for (auto& [key, value] : query_opts.named_parameters) {
            auto val_str = binary_to_string(value);
            add_to_dict(pyObj_named, key, val_str);
        }
        PyDict_SetItemString(retval, "named_parameters", pyObj_named);
        Py_DECREF(pyObj_named);
    }
    return retval;
}

static PyMethodDef transaction_query_options_methods[] = { { "to_dict",
                                                             (PyCFunction)pycbc_txns::transaction_query_options__to_dict__,
                                                             METH_NOARGS,
                                                             PyDoc_STR("transaction_query_options as a dict") },
                                                           { NULL, NULL, 0, NULL } };

PyObject*
pycbc_txns::transaction_query_options__new__(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
    PyObject* pyObj_query_args = nullptr;
    const char* kw_list[] = { "query_args", nullptr };
    const char* kw_format = "|O";
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, kw_format, const_cast<char**>(kw_list), &pyObj_query_args)) {
        PyErr_SetString(PyExc_ValueError, "couldn't parse args");
        Py_RETURN_NONE;
    }
    auto self = reinterpret_cast<pycbc_txns::transaction_query_options*>(type->tp_alloc(type, 0));
    auto req = build_query_request(pyObj_query_args);
    if (PyErr_Occurred()) {
        return nullptr;
    }
    self->opts = new tx::transaction_query_options();
    self->opts->ad_hoc(req.adhoc);
    self->opts->metrics(req.metrics);
    self->opts->readonly(req.readonly);
    // @TODO:  add flex index to txn QueryOptions eventually?
    // self->opts->flex_index(req.flex_index);
    if (req.max_parallelism.has_value()) {
        self->opts->max_parallelism(req.max_parallelism.value());
    }
    if (req.scan_cap.has_value()) {
        self->opts->scan_cap(req.scan_cap.value());
    }
    if (req.scan_wait.has_value()) {
        self->opts->scan_wait(req.scan_wait.value());
    }
    if (req.scan_cap.has_value()) {
        self->opts->scan_cap(req.scan_cap.value());
    }
    if (req.pipeline_batch.has_value()) {
        self->opts->pipeline_batch(req.pipeline_batch.value());
    }
    if (req.pipeline_cap.has_value()) {
        self->opts->pipeline_cap(req.pipeline_cap.value());
    }
    if (req.client_context_id.has_value()) {
        self->opts->client_context_id(req.client_context_id.value());
    }
    if (req.scan_consistency.has_value()) {
        self->opts->scan_consistency(req.scan_consistency.value());
    }
    self->opts->profile(req.profile);
    if (req.raw.size() > 0) {
        std::map<std::string, std::vector<std::byte>, std::less<>> raw_options{};
        for (auto& [name, option] : req.raw) {
            raw_options[name] = std::move(option.bytes());
        }
        self->opts->encoded_raw_options(raw_options);
    }
    if (req.positional_parameters.size() > 0) {
        std::vector<std::vector<std::byte>> positional_params{};
        for (auto& param : req.positional_parameters) {
            positional_params.emplace_back(std::move(param.bytes()));
        }
        self->opts->encoded_positional_parameters(positional_params);
    }
    if (req.named_parameters.size() > 0) {
        std::map<std::string, std::vector<std::byte>, std::less<>> named_params{};
        for (auto& [name, param] : req.named_parameters) {
            named_params[name] = std::move(param.bytes());
        }
        self->opts->encoded_named_parameters(named_params);
    }
    return reinterpret_cast<PyObject*>(self);
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
    r.tp_methods = transaction_query_options_methods;
    return r;
}

static PyTypeObject transaction_query_options_type = init_transaction_query_options_type();

/* pycbc_txns::transaction_query_options type methods */

void
pycbc_txns::transaction_get_result__dealloc__(pycbc_txns::transaction_get_result* result)
{
    delete result->res;
    Py_TYPE(result)->tp_free((PyObject*)result);
    CB_LOG_DEBUG("dealloc transaction_get_result");
}

PyObject*
pycbc_txns::transaction_get_result__str__(pycbc_txns::transaction_get_result* result)
{
    const char* format_string = "transaction_get_result:{key=%s, cas=%llu, value=%s}";
    auto value = couchbase::core::utils::json::generate(result->res->content<tao::json::value>());
    return PyUnicode_FromFormat(format_string, result->res->id().key().c_str(), result->res->cas(), value.c_str());
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
        return PyUnicode_FromString(result->res->id().key().c_str());
    }
    if (CAS == field_name) {
        return PyLong_FromUnsignedLongLong(result->res->cas().value());
    }
    if (VALUE == field_name) {
        try {
            return binary_to_PyObject(result->res->content());
        } catch (const std::exception& e) {
            PyErr_SetString(PyExc_TypeError, e.what());
            Py_RETURN_NONE;
        }
    }
    PyErr_SetString(PyExc_ValueError, fmt::format("unknown field_name {}", field_name).c_str());
    Py_RETURN_NONE;
}

static PyMethodDef transaction_get_result_methods[] = {
    { "get", (PyCFunction)pycbc_txns::transaction_get_result__get__, METH_VARARGS, PyDoc_STR("get field in result object") },
    { NULL, NULL, 0, NULL }
};

PyObject*
pycbc_txns::transaction_get_result__new__(PyTypeObject* type, PyObject*, PyObject*)
{
    auto self = reinterpret_cast<pycbc_txns::transaction_get_result*>(type->tp_alloc(type, 0));
    self->res = new tx_core::transaction_get_result();
    return reinterpret_cast<PyObject*>(self);
}

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
                            if (PyType_Ready(&transaction_options_type) == 0) {
                                Py_INCREF(&transaction_options_type);
                                if (PyModule_AddObject(
                                      pyObj_module, "transaction_options", reinterpret_cast<PyObject*>(&transaction_options_type)) == 0) {
                                    return pyObj_module;
                                }
                                Py_DECREF(&transaction_options_type);
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

    pycbc_txns::transactions* txns;
    Py_BEGIN_ALLOW_THREADS txns =
      new pycbc_txns::transactions(pyObj_conn, *reinterpret_cast<pycbc_txns::transaction_config*>(pyObj_config)->cfg);
    Py_END_ALLOW_THREADS PyObject* pyObj_txns = PyCapsule_New(txns, "txns_", dealloc_transactions);
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

PyObject*
init_transaction_exception_type(const char* klass)
{
    static PyObject* couchbase_exceptions = PyImport_ImportModule("couchbase.exceptions");
    assert(nullptr != couchbase_exceptions);
    return PyObject_GetAttrString(couchbase_exceptions, klass);
}

void
build_inner_exception(PyObject* pyObj_inner_exc,
                      PyObject* pyObj_type,
                      PyObject* pyObj_value,
                      PyObject* pyObj_traceback,
                      const char* file,
                      int line)
{
    if (pyObj_type != nullptr) {
        PyErr_NormalizeException(&pyObj_type, &pyObj_value, &pyObj_traceback);
        if (-1 == PyDict_SetItemString(pyObj_inner_exc, "inner_cause", pyObj_value)) {
            PyErr_Print();
            // the important key is the "inner_cause", if this fails just don't populate the dict
            // we will check for the "inner_cause" key later in convert_to_python_exc_type()
            return;
        }
        PyObject* pyObj_cinfo = Py_BuildValue("(s,i)", file, line);
        if (-1 == PyDict_SetItemString(pyObj_inner_exc, "cinfo", pyObj_cinfo)) {
            PyErr_Print();
        }
        Py_XDECREF(pyObj_cinfo);
    }
}

PyObject*
convert_to_python_exc_type(std::exception_ptr err, bool set_exception = false, PyObject* pyObj_inner_exc = nullptr)
{
    static PyObject* pyObj_txn_failed = init_transaction_exception_type("TransactionFailed");
    static PyObject* pyObj_txn_expired = init_transaction_exception_type("TransactionExpired");
    static PyObject* pyObj_txn_ambig = init_transaction_exception_type("TransactionCommitAmbiguous");
    static PyObject* pyObj_txn_op_failed = init_transaction_exception_type("TransactionOperationFailed");
    static PyObject* pyObj_document_exists_ex = init_transaction_exception_type("DocumentExistsException");
    static PyObject* pyObj_document_not_found_ex = init_transaction_exception_type("DocumentNotFoundException");
    static PyObject* pyObj_query_parsing_failure = init_transaction_exception_type("ParsingFailedException");
    static PyObject* pyObj_couchbase_error = init_transaction_exception_type("CouchbaseException");
    PyObject* pyObj_error_ctx = PyDict_New();
    PyObject* pyObj_exc_type = nullptr;
    PyObject* pyObj_final_error = nullptr;
    const char* message = nullptr;

    // Must be an error
    assert(!!err);

    try {
        std::rethrow_exception(err);
    } catch (const tx_core::transaction_exception& e) {
        pyObj_final_error = pyObj_txn_failed;
        switch (e.type()) {
            case tx_core::failure_type::FAIL:
                pyObj_exc_type = pyObj_txn_failed;
                break;
            case tx_core::failure_type::COMMIT_AMBIGUOUS:
                pyObj_exc_type = pyObj_txn_ambig;
                break;
            case tx_core::failure_type::EXPIRY:
                pyObj_exc_type = pyObj_txn_expired;
                break;
        }
        message = e.what();
    } catch (const tx_core::transaction_operation_failed& e) {
        pyObj_exc_type = pyObj_txn_op_failed;
        message = e.what();
    } catch (const tx_core::query_parsing_failure& e) {
        pyObj_exc_type = pyObj_query_parsing_failure;
        message = e.what();
    } catch (const tx_core::document_exists& e) {
        pyObj_exc_type = pyObj_document_exists_ex;
        message = e.what();
    } catch (const tx_core::document_not_found& e) {
        pyObj_exc_type = pyObj_document_exists_ex;
        message = e.what();
    } catch (const tx_core::op_exception& e) {
        pyObj_exc_type = pyObj_couchbase_error;
        message = e.what();
    } catch (const std::exception& e) {
        pyObj_exc_type = pyObj_couchbase_error;
        message = e.what();
    } catch (...) {
        pyObj_exc_type = pyObj_couchbase_error;
        message = "Unknown error";
    }
    PyObject* tmp = PyUnicode_FromString(message);
    PyDict_SetItemString(pyObj_error_ctx, "message", tmp);
    Py_DECREF(tmp);
    if (pyObj_inner_exc != nullptr) {
        tmp = PyDict_GetItemString(pyObj_inner_exc, "inner_cause");
        if (tmp != nullptr) {
            PyDict_SetItemString(pyObj_error_ctx, "exc_info", pyObj_inner_exc);
            Py_DECREF(pyObj_inner_exc);
        }
    }
    PyObject* pyObj_args = PyTuple_New(0);
    pyObj_final_error = PyObject_Call(pyObj_exc_type, pyObj_args, pyObj_error_ctx);
    Py_DECREF(pyObj_args);
    if (set_exception) {
        PyErr_SetObject(pyObj_exc_type, pyObj_final_error);
        return nullptr;
    }
    return pyObj_final_error;
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
        if (nullptr == pyObj_errback) {
            Py_INCREF(Py_None);
            barrier->set_exception(err);
        } else {
            args = PyTuple_Pack(1, convert_to_python_exc_type(err));
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

void
handle_returning_transaction_get_result(PyObject* pyObj_callback,
                                        PyObject* pyObj_errback,
                                        std::shared_ptr<std::promise<PyObject*>> barrier,
                                        std::exception_ptr err,
                                        std::optional<couchbase::core::transactions::transaction_get_result> res)
{
    // TODO: flesh out transaction_get_result and exceptions...
    auto state = PyGILState_Ensure();
    PyObject* args = nullptr;
    PyObject* func = nullptr;
    if (err) {
        if (nullptr == pyObj_errback) {
            barrier->set_exception(err);
        } else {
            args = PyTuple_Pack(1, convert_to_python_exc_type(err));
            func = pyObj_errback;
        }
    } else {
        PyObject* pyObj_get_result = nullptr;

        // BUG(PYCBC-1476): We should revert to using direct get
        // operations once the underlying issue has been resolved.
        if (!res.has_value()) {
            pyObj_get_result = pycbc_build_exception(couchbase::errc::make_error_code(couchbase::errc::key_value::document_not_found),
                                                     __FILE__,
                                                     __LINE__,
                                                     "Txn get op: document not found.");
        } else {
            pyObj_get_result = PyObject_CallObject(reinterpret_cast<PyObject*>(&transaction_get_result_type), nullptr);
            auto result = reinterpret_cast<pycbc_txns::transaction_get_result*>(pyObj_get_result);
            // now lets copy it in
            // TODO: ideally we'd have a move constructor for transaction_get_result, but for now...
            result->res = new tx_core::transaction_get_result(res.value());
        }

        if (nullptr == pyObj_callback) {
            barrier->set_value(pyObj_get_result);
        } else {
            args = PyTuple_Pack(1, pyObj_get_result);
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
                                            std::exception_ptr err, std::optional<couchbase::core::operations::query_response> resp) {
                                              auto state = PyGILState_Ensure();
                                              PyObject* args = nullptr;
                                              PyObject* func = nullptr;
                                              if (err) {
                                                  // TODO: flesh out exception handling!
                                                  if (nullptr == pyObj_errback) {
                                                      barrier->set_exception(err);
                                                  } else {
                                                      args = PyTuple_Pack(1, convert_to_python_exc_type(err));
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
        std::exception_ptr err;
        Py_BEGIN_ALLOW_THREADS
        try {
            ret = f.get();
        } catch (...) {
            err = std::current_exception();
        }
        Py_END_ALLOW_THREADS if (err)
        {
            return convert_to_python_exc_type(err, true);
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
    tao::json::value value;
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
        char* buf;
        Py_ssize_t nbuf;
        if (PyBytes_AsStringAndSize(pyObj_value, &buf, &nbuf) == -1) {
            pycbc_set_python_exception(
              PycbcError::InvalidArgument, __FILE__, __LINE__, "Unable to determine bytes object from provided value.");
            Py_RETURN_NONE;
        }
        auto size = py_ssize_t_to_size_t(nbuf);
        value = couchbase::core::utils::json::parse(reinterpret_cast<const char*>(buf), size);
        CB_LOG_DEBUG("value is {}", buf);
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
            couchbase::core::document_id id{ bucket, scope, collection, key };
            Py_BEGIN_ALLOW_THREADS ctx->ctx.get_optional(
              id, [barrier, pyObj_callback, pyObj_errback](std::exception_ptr err, std::optional<tx_core::transaction_get_result> res) {
                  handle_returning_transaction_get_result(pyObj_callback, pyObj_errback, barrier, err, res);
              });
            Py_END_ALLOW_THREADS break;
        }
        case TxOperations::INSERT: {
            if (nullptr == bucket || nullptr == scope || nullptr == collection || nullptr == key) {
                PyErr_SetString(PyExc_ValueError, "couldn't create document id for insert");
                Py_RETURN_NONE;
            }
            couchbase::core::document_id id{ bucket, scope, collection, key };
            if (nullptr == pyObj_value) {
                PyErr_SetString(PyExc_ValueError, fmt::format("no value given for an insert of key {}", id.key()).c_str());
                Py_RETURN_NONE;
            }
            Py_BEGIN_ALLOW_THREADS ctx->ctx.insert(
              id,
              value,
              [barrier, pyObj_callback, pyObj_errback](std::exception_ptr err, std::optional<tx_core::transaction_get_result> res) {
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
              *tx_get_result->res,
              value,
              [pyObj_callback, pyObj_errback, barrier](std::exception_ptr err, std::optional<tx_core::transaction_get_result> res) {
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
            Py_BEGIN_ALLOW_THREADS ctx->ctx.remove(*tx_get_result->res, [pyObj_callback, pyObj_errback, barrier](std::exception_ptr err) {
                handle_returning_void(pyObj_callback, pyObj_errback, barrier, err);
            });
            Py_END_ALLOW_THREADS break;
        }
        default:
            // return error!
            CB_LOG_DEBUG("unknown op {}", op_type);
            PyErr_SetString(PyExc_ValueError, "unknown txn operation");
    }
    if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
        PyObject* ret = nullptr;
        std::string msg;
        std::exception_ptr err;
        Py_BEGIN_ALLOW_THREADS
        try {
            ret = f.get();
        } catch (...) {
            err = std::current_exception();
        }
        Py_END_ALLOW_THREADS if (err)
        {
            return convert_to_python_exc_type(err, true);
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
    PyObject* pyObj_transaction_options = nullptr;
    const char* kw_list[] = { "txns", "logic", "callback", "errback", "transaction_options", nullptr };
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
                                          &pyObj_transaction_options);
    if (!ret) {
        PyErr_SetString(PyExc_ValueError, "couldn't parse args");
        return nullptr;
    }
    auto txns = reinterpret_cast<pycbc_txns::transactions*>(PyCapsule_GetPointer(pyObj_txns, "txns_"));
    if (nullptr == txns) {
        PyErr_SetString(PyExc_ValueError, "passed null transactions");
        return nullptr;
    }
    if (nullptr != pyObj_transaction_options) {
        if (!PyObject_IsInstance(pyObj_transaction_options, reinterpret_cast<PyObject*>(&transaction_options_type))) {
            PyErr_SetString(PyExc_ValueError, "expected a valid transaction_options object");
            return nullptr;
        }
    }
    // we need the callback, errback, and logic to all stick around, so...
    Py_XINCREF(pyObj_errback);
    Py_XINCREF(pyObj_callback);
    Py_XINCREF(pyObj_logic);
    // create a placeholder for potentially storing inner exception for TransactionFailed
    // make sure the container sticks around
    PyObject* pyObj_inner_exc = PyDict_New();
    Py_INCREF(pyObj_inner_exc);
    auto barrier = std::make_shared<std::promise<PyObject*>>();
    auto f = barrier->get_future();
    auto logic = [pyObj_logic, pyObj_inner_exc](tx_core::async_attempt_context& ctx) {
        auto state = PyGILState_Ensure();
        auto py_ctx = new pycbc_txns::attempt_context(ctx);
        PyObject* pyObj_ctx = PyCapsule_New(py_ctx, "ctx_", dealloc_attempt_context);
        PyObject* args = PyTuple_Pack(1, pyObj_ctx);
        PyErr_Clear();
        PyObject_CallObject(pyObj_logic, args);
        // ideally we get some info from the exception and pass it into the exception we throw below.  For
        // now, lets just check if one did occur, and make sure we throw a c++ exception so we rollback.
        PyObject* pyObj_exc_type = nullptr;
        PyObject* pyObj_exc_value = nullptr;
        PyObject* pyObj_exc_trace = nullptr;
        bool py_error = false;
        std::string py_error_message("Unknown Python Error");
        PyErr_Fetch(&pyObj_exc_type, &pyObj_exc_value, &pyObj_exc_trace);
        if (nullptr != pyObj_exc_type) {
            PyObject* pyObj_txn_exc = init_transaction_exception_type("TransactionException");
            if (nullptr == pyObj_exc_value || !PyErr_GivenExceptionMatches(pyObj_exc_value, pyObj_txn_exc)) {
                // raise a c++ exception to insure rollback.
                py_error = true;
                if (nullptr != pyObj_exc_value) {
                    PyObject* pyObj_exc_message = PyObject_GetAttrString(pyObj_exc_value, "message");
                    if (pyObj_exc_message == nullptr || pyObj_exc_message == Py_None) {
                        pyObj_exc_message = PyObject_Repr(pyObj_exc_value);
                    }
                    py_error_message = PyUnicode_AsUTF8(pyObj_exc_message);
                    Py_XDECREF(pyObj_exc_message);
                    build_inner_exception(pyObj_inner_exc, pyObj_exc_type, pyObj_exc_value, pyObj_exc_trace, __FILE__, __LINE__);
                }
            }
        }
        // Decrement references and eat the exception since we captured the inner exception
        // for instances in which we want to keep the inner exception
        Py_XDECREF(pyObj_exc_type);
        Py_XDECREF(pyObj_exc_value);
        Py_XDECREF(pyObj_exc_trace);
        PyErr_Restore(nullptr, nullptr, nullptr);
        PyGILState_Release(state);
        // now we raise an exception so we will rollback
        if (py_error) {
            throw std::runtime_error(py_error_message);
        }
    };
    auto cb = [pyObj_callback, pyObj_errback, barrier, pyObj_logic, pyObj_inner_exc](std::optional<tx_core::transaction_exception> err,
                                                                                     std::optional<tx::transaction_result> res) {
        auto state = PyGILState_Ensure();
        PyObject* args = nullptr;
        PyObject* func = nullptr;
        if (err) {
            if (nullptr == pyObj_errback) {
                barrier->set_exception(std::make_exception_ptr(*err));
            } else {
                args = PyTuple_Pack(1, convert_to_python_exc_type(std::make_exception_ptr(*err), false, pyObj_inner_exc));
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
            Py_DECREF(pyObj_inner_exc);
        }
        Py_XDECREF(pyObj_logic);
        PyGILState_Release(state);
    };
    tx::transaction_options* opts = nullptr;
    if (nullptr != pyObj_transaction_options && Py_None != pyObj_transaction_options) {
        opts = reinterpret_cast<pycbc_txns::transaction_options*>(pyObj_transaction_options)->opts;
    }
    Py_BEGIN_ALLOW_THREADS if (nullptr == opts)
    {
        // @TODO: PYCBC-1425, is this the right approach?
        txns->txns->run(logic, std::forward<pycbc_txns::pycbc_txn_complete_callback>(cb));
    }
    else
    {
        auto expiry = opts->expiration_time();
        CB_LOG_DEBUG("calling transactions.run with expiry {}ns", expiry.has_value() ? expiry->count() : 0);
        // @TODO: PYCBC-1425, is this the right approach?
        txns->txns->run(*opts, logic, std::forward<pycbc_txns::pycbc_txn_complete_callback>(cb));
    }
    Py_END_ALLOW_THREADS if (nullptr == pyObj_callback || nullptr == pyObj_errback)
    {
        std::exception_ptr err;
        PyObject* retval = nullptr;
        Py_BEGIN_ALLOW_THREADS
        try {
            retval = f.get();
        } catch (...) {
            err = std::current_exception();
        }
        Py_END_ALLOW_THREADS if (err)
        {
            retval = convert_to_python_exc_type(err, true, pyObj_inner_exc);
            Py_DECREF(pyObj_inner_exc);
        }
        return retval;
    }
    Py_RETURN_NONE;
}
