
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
    txns->txns->close();
    delete txns->txns;
    LOG_DEBUG("dealloc transactions");
}

void
pycbc_txns::dealloc_attempt_context(PyObject* obj)
{
    auto ctx = reinterpret_cast<pycbc_txns::attempt_context*>(PyCapsule_GetPointer(obj, "ctx_"));
    delete ctx;
    LOG_DEBUG("dealloc attempt_context");
}

void
pycbc_txns::transaction_config__dealloc__(pycbc_txns::transaction_config* cfg)
{
    delete cfg->cfg;
    LOG_DEBUG("dealloc transaction_config");
}

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

PyObject*
pycbc_txns::transaction_config__to_dict__(PyObject* self)
{
    auto conf = reinterpret_cast<pycbc_txns::transaction_config*>(self);
    PyObject* retval = PyDict_New();
    add_to_dict(retval, "durability_level", static_cast<int64_t>(conf->cfg->durability_level()));
    add_to_dict(retval, "cleanup_window", static_cast<int64_t>(conf->cfg->cleanup_window().count()));
    if (conf->cfg->kv_timeout()) {
        add_to_dict(retval, "kv_timeout", static_cast<int64_t>(conf->cfg->kv_timeout()->count()));
    }
    add_to_dict(retval, "expiration_time", static_cast<int64_t>(conf->cfg->expiration_time().count()));
    add_to_dict(retval, "cleanup_lost_attempts", conf->cfg->cleanup_lost_attempts());
    add_to_dict(retval, "cleanup_client_attempts", conf->cfg->cleanup_client_attempts());
    add_to_dict(retval, "scan_consistency", scan_consistency_type_to_string(conf->cfg->scan_consistency()));
    if (conf->cfg->custom_metadata_collection()) {
        std::string meta = fmt::format("{}.{}.{}",
                                       conf->cfg->custom_metadata_collection()->bucket,
                                       conf->cfg->custom_metadata_collection()->scope,
                                       conf->cfg->custom_metadata_collection()->collection);
        add_to_dict(retval, "metadata_collection", meta);
    }
    return retval;
}

void
pycbc_txns::per_transaction_config__dealloc__(pycbc_txns::per_transaction_config* cfg)
{
    delete cfg->cfg;
    LOG_DEBUG("dealloc per_transaction_config");
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
    const char* kw_format = "|OOOOOOssss";
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
    char* metadata_bucket = nullptr;
    char* metadata_scope = nullptr;
    char* metadata_collection = nullptr;

    const char* kw_list[] = { "durability_level", "kv_timeout",     "expiration_time",     "scan_consistency",
                              "metadata_bucket",  "metadata_scope", "metadata_collection", nullptr };
    const char* kw_format = "|OOOssss";
    auto self = reinterpret_cast<pycbc_txns::per_transaction_config*>(type->tp_alloc(type, 0));

    self->cfg = new tx::per_transaction_config();
    LOG_DEBUG("per_transaction_config__new__ called");
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
    if (nullptr != metadata_bucket && nullptr != metadata_scope && nullptr != metadata_collection) {
        self->cfg->custom_metadata_collection({ metadata_bucket, metadata_scope, metadata_collection });
    }

    return reinterpret_cast<PyObject*>(self);
}

PyObject*
pycbc_txns::per_transaction_config__to_dict__(PyObject* self)
{
    auto conf = reinterpret_cast<pycbc_txns::per_transaction_config*>(self);
    PyObject* retval = PyDict_New();
    if (conf->cfg->kv_timeout()) {
        add_to_dict(retval, "kv_timeout", static_cast<int64_t>(conf->cfg->kv_timeout()->count()));
    }
    if (conf->cfg->expiration_time()) {
        add_to_dict(retval, "expiration_time", static_cast<int64_t>(conf->cfg->expiration_time()->count()));
    }
    if (conf->cfg->durability_level()) {
        add_to_dict(retval, "durability_level", static_cast<int64_t>(conf->cfg->durability_level().value()));
    }
    if (conf->cfg->scan_consistency()) {
        add_to_dict(retval, "scan_consistency", scan_consistency_type_to_string(*conf->cfg->scan_consistency()));
    }
    if (conf->cfg->custom_metadata_collection()) {
        std::string meta = fmt::format("{}.{}.{}",
                                       conf->cfg->custom_metadata_collection()->bucket,
                                       conf->cfg->custom_metadata_collection()->scope,
                                       conf->cfg->custom_metadata_collection()->collection);
        add_to_dict(retval, "metadata_collection", meta);
    }
    return retval;
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

static PyMethodDef transaction_config_methods[] = {
    { "to_dict", (PyCFunction)pycbc_txns::transaction_config__to_dict__, METH_NOARGS, PyDoc_STR("transaction_config as a dict") },
    { NULL, NULL, 0, NULL }
};

static PyMethodDef per_transaction_config_methods[] = {
    { "to_dict", (PyCFunction)pycbc_txns::per_transaction_config__to_dict__, METH_NOARGS, PyDoc_STR("per_transaction_config as a dict") },
    { NULL, NULL, 0, NULL }
};

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
    r.tp_methods = per_transaction_config_methods;
    return r;
}

static PyTypeObject per_transaction_config_type = init_per_transaction_config_type();

PyObject*
pycbc_txns::transaction_query_options__new__(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
    PyObject* pyObj_raw = nullptr;
    PyObject* pyObj_ad_hoc = nullptr;
    char* scan_consistency = nullptr;
    char* profile_mode = nullptr;
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
    PyObject* pyObj_positional_params = nullptr;
    PyObject* pyObj_named_params = nullptr;

    const char* kw_list[] = { "raw",
                              "ad_hoc",
                              "scan_consistency",
                              "profile_mode",
                              "client_context_id",
                              "scan_wait",
                              "read_only",
                              "scan_cap",
                              "pipeline_batch",
                              "pipeline_cap",
                              "scope",
                              "bucket",
                              "metrics",
                              "max_parallelism",
                              "positional_parameters",
                              "named_parameters",
                              nullptr };
    const char* kw_format = "|OOsssOOOOOssOOOO";

    auto self = reinterpret_cast<pycbc_txns::transaction_query_options*>(type->tp_alloc(type, 0));
    self->opts = new tx::transaction_query_options();
    if (!PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     kw_format,
                                     const_cast<char**>(kw_list),
                                     &pyObj_raw,
                                     &pyObj_ad_hoc,
                                     &scan_consistency,
                                     &profile_mode,
                                     &client_context_id,
                                     &pyObj_scan_wait,
                                     &pyObj_read_only,
                                     &pyObj_scan_cap,
                                     &pyObj_pipeline_batch,
                                     &pyObj_pipeline_cap,
                                     &scope,
                                     &bucket,
                                     &pyObj_metrics,
                                     &pyObj_max_parallelism,
                                     &pyObj_positional_params,
                                     &pyObj_named_params)) {
        PyErr_SetString(PyExc_ValueError, "couldn't parse args");
        Py_RETURN_NONE;
    }
    auto opts = reinterpret_cast<pycbc_txns::transaction_query_options*>(self)->opts;
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
    if (nullptr != profile_mode) {
        self->opts->profile(str_to_profile_mode(profile_mode));
    }
    if (nullptr != scan_consistency) {
        self->opts->scan_consistency(str_to_scan_consistency_type<couchbase::query_scan_consistency>(scan_consistency));
        if (PyErr_Occurred()) {
            return nullptr;
        }
    }
    if (nullptr != pyObj_ad_hoc) {
        self->opts->ad_hoc(!!PyObject_IsTrue(pyObj_ad_hoc));
    }
    if (nullptr != pyObj_metrics) {
        self->opts->metrics(!!PyObject_IsTrue(pyObj_metrics));
    }
    if (nullptr != pyObj_raw) {
        if (!PyDict_Check(pyObj_raw)) {
            PyErr_SetString(PyExc_ValueError, "raw option isn't a dict!  The raw option should be a dict[str, JSONString].");
            return nullptr;
        }
        PyObject *pyObj_key, *pyObj_value;
        Py_ssize_t pos = 0;

        while (PyDict_Next(pyObj_raw, &pos, &pyObj_key, &pyObj_value)) {
            std::string k;
            if (PyUnicode_Check(pyObj_key)) {
                k = std::string(PyUnicode_AsUTF8(pyObj_key));
            } else {
                PyErr_SetString(PyExc_ValueError, "Raw option key not a string!  The raw option should be a dict[str, JSONString].");
                return nullptr;
            }
            if (k.empty()) {
                PyErr_SetString(PyExc_ValueError, "Key is empty!  The raw option should be a dict[str, JSONString].");
                return nullptr;
            }
            if (PyBytes_Check(pyObj_value)) {
                couchbase::json_string val(std::string(PyBytes_AsString(pyObj_value)));
                self->opts->raw(k, val);
            } else {
                PyErr_SetString(PyExc_ValueError, "Raw option value not a string!  The raw option should be a dict[str, JSONString].");
                return nullptr;
            }
        }
        pyObj_key = nullptr;
        pyObj_value = nullptr;
    }

    if (nullptr != pyObj_positional_params) {
        if (!PyList_Check(pyObj_positional_params)) {
            PyErr_SetString(PyExc_ValueError, "Positional parameters options must be a list.");
            return nullptr;
        }
        std::vector<couchbase::json_string> pos_opts{};
        for (size_t i = 0; i < PyList_Size(pyObj_positional_params); i++) {
            PyObject* pyObj_value = PyList_GetItem(pyObj_positional_params, i);
            if (PyBytes_Check(pyObj_value)) {
                pos_opts.emplace_back(PyBytes_AsString(pyObj_value));
            } else {
                PyErr_SetString(PyExc_ValueError, "Positional parameter options must all be json strings");
                return nullptr;
            }
        }
        self->opts->positional_parameters(pos_opts);
    }
    if (nullptr != pyObj_named_params) {
        if (!PyDict_Check(pyObj_named_params)) {
            PyErr_SetString(PyExc_ValueError, "Named parameter options must be a dict[str, JSONType]");
            return nullptr;
        }
        PyObject* pyObj_key = nullptr;
        PyObject* pyObj_value = nullptr;
        Py_ssize_t pos = 0;
        std::map<std::string, couchbase::json_string> params{};
        while (PyDict_Next(pyObj_named_params, &pos, &pyObj_key, &pyObj_value)) {
            params[PyUnicode_AsUTF8(pyObj_key)] = couchbase::json_string(PyBytes_AsString(pyObj_value));
        }
        self->opts->named_parameters(params);
    }
    return reinterpret_cast<PyObject*>(self);
}

PyObject*
pycbc_txns::transaction_query_options__to_dict__(PyObject* self)
{
    auto opts = reinterpret_cast<pycbc_txns::transaction_query_options*>(self);
    PyObject* retval = PyDict_New();
    auto req = opts->opts->query_request();
    PyObject* raw = PyDict_New();
    for (auto const& [key, val] : req.raw) {
        add_to_dict(raw, key, val.str());
    }
    PyDict_SetItemString(retval, "raw", raw);
    Py_DECREF(raw);
    add_to_dict(retval, "adhoc", req.adhoc);
    if (req.scan_consistency) {
        add_to_dict(retval, "scan_consistency", scan_consistency_type_to_string(req.scan_consistency.value()));
    }
    add_to_dict(retval, "profile", profile_mode_to_str(req.profile));
    if (req.client_context_id) {
        add_to_dict(retval, "client_context_id", req.client_context_id.value());
    }
    if (req.scan_wait) {
        add_to_dict(retval, "scan_wait", static_cast<int64_t>(req.scan_wait->count()));
    }
    add_to_dict(retval, "read_only", req.readonly);
    if (req.scan_cap) {
        add_to_dict(retval, "scan_cap", static_cast<int64_t>(req.scan_cap.value()));
    }
    if (req.pipeline_batch) {
        add_to_dict(retval, "pipeline_batch", static_cast<int64_t>(req.pipeline_batch.value()));
    }
    if (req.pipeline_cap) {
        add_to_dict(retval, "pipeline_cap", static_cast<int64_t>(req.pipeline_cap.value()));
    }
    if (req.scope_name) {
        add_to_dict(retval, "scope", req.scope_name.value());
    }
    if (req.bucket_name) {
        add_to_dict(retval, "bucket", req.bucket_name.value());
    }
    add_to_dict(retval, "metrics", req.metrics);
    if (req.max_parallelism) {
        add_to_dict(retval, "max_parallelism", static_cast<int64_t>(req.max_parallelism.value()));
    }
    if (!req.positional_parameters.empty()) {
        PyObject* pyObj_pos = PyList_New(0);
        for (auto& val : req.positional_parameters) {
            PyObject* pyObj_val = PyUnicode_FromString(val.str().c_str());
            PyList_Append(pyObj_pos, pyObj_val);
            Py_DECREF(pyObj_val);
        }
        PyDict_SetItemString(retval, "positional_parameters", pyObj_pos);
        Py_DECREF(pyObj_pos);
    }
    if (!req.named_parameters.empty()) {
        PyObject* pyObj_named = PyDict_New();
        for (auto& [key, value] : req.named_parameters) {
            add_to_dict(pyObj_named, key, value.str());
        }
        PyDict_SetItemString(retval, "named_parameters", pyObj_named);
        Py_DECREF(pyObj_named);
    }
    return retval;
}

void
pycbc_txns::transaction_query_options__dealloc__(pycbc_txns::transaction_query_options* opts)
{
    delete opts->opts;
    LOG_DEBUG("dealloc transaction_query_options");
}

static PyMethodDef transaction_query_options_methods[] = { { "to_dict",
                                                             (PyCFunction)pycbc_txns::transaction_query_options__to_dict__,
                                                             METH_NOARGS,
                                                             PyDoc_STR("transaction_query_options as a dict") },
                                                           { NULL, NULL, 0, NULL } };

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

PyObject*
pycbc_txns::transaction_get_result__new__(PyTypeObject* type, PyObject*, PyObject*)
{
    auto self = reinterpret_cast<pycbc_txns::transaction_get_result*>(type->tp_alloc(type, 0));
    self->res = new tx::transaction_get_result();
    return reinterpret_cast<PyObject*>(self);
}

PyObject*
pycbc_txns::transaction_get_result__str__(pycbc_txns::transaction_get_result* result)
{
    const char* format_string = "transaction_get_result:{key=%s, cas=%llu, value=%s}";
    return PyUnicode_FromFormat(
      format_string, result->res->id().key().c_str(), result->res->cas(), result->res->content<std::string>().c_str());
}

void
pycbc_txns::transaction_get_result__dealloc__(pycbc_txns::transaction_get_result* result)
{
    delete result->res;
    LOG_DEBUG("dealloc transaction_get_result");
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
        return PyLong_FromUnsignedLongLong(result->res->cas());
    }
    if (VALUE == field_name) {
        return PyBytes_FromString(result->res->content<std::string>().c_str());
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

PyObject*
convert_to_python_exc_type(std::exception_ptr err, bool set_exception = false)
{
    static PyObject* pyObj_txn_failed = init_transaction_exception_type("TransactionFailed");
    static PyObject* pyObj_txn_expired = init_transaction_exception_type("TransactionExpired");
    static PyObject* pyObj_txn_ambig = init_transaction_exception_type("TransactionCommitAmbiguous");
    static PyObject* pyObj_txn_op_failed = init_transaction_exception_type("TransactionOperationFailed");
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
    } catch (const tx::transaction_exception& e) {
        pyObj_final_error = pyObj_txn_failed;
        switch (e.type()) {
            case tx::failure_type::FAIL:
                pyObj_exc_type = pyObj_txn_failed;
                break;
            case tx::failure_type::COMMIT_AMBIGUOUS:
                pyObj_exc_type = pyObj_txn_ambig;
                break;
            case tx::failure_type::EXPIRY:
                pyObj_exc_type = pyObj_txn_expired;
                break;
        }
        message = e.what();
    } catch (const tx::transaction_operation_failed& e) {
        pyObj_exc_type = pyObj_txn_op_failed;
        message = e.what();
    } catch (const tx::query_parsing_failure& e) {
        pyObj_exc_type = pyObj_query_parsing_failure;
        message = e.what();
    } catch (const tx::query_exception& e) {
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
            args = PyTuple_Pack(1, convert_to_python_exc_type(err));
            func = pyObj_errback;
        }
    } else {
        PyObject* transaction_get_result_obj = PyObject_CallObject(reinterpret_cast<PyObject*>(&transaction_get_result_type), nullptr);
        auto result = reinterpret_cast<pycbc_txns::transaction_get_result*>(transaction_get_result_obj);
        // now lets copy it in
        // TODO: ideally we'd have a move constructor for transaction_get_result, but for now...
        result->res = new tx::transaction_get_result(res.value());
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
        LOG_DEBUG("value is {}", value);
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
              *tx_get_result->res,
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
            Py_BEGIN_ALLOW_THREADS ctx->ctx.remove(*tx_get_result->res, [pyObj_callback, pyObj_errback, barrier](std::exception_ptr err) {
                handle_returning_void(pyObj_callback, pyObj_errback, barrier, err);
            });
            Py_END_ALLOW_THREADS break;
        }
        default:
            // return error!
            LOG_DEBUG("unknown op {}", op_type);
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
                    py_error_message = PyUnicode_AsUTF8(PyObject_Repr(pyObj_exc_value));
                }
            }
        }
        // eat the exception in any case
        PyErr_Restore(nullptr, nullptr, nullptr);

        PyGILState_Release(state);
        // now we raise an exception so we will rollback
        if (py_error) {
            throw std::runtime_error(py_error_message);
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
                args = PyTuple_Pack(1, convert_to_python_exc_type(std::make_exception_ptr(*err)));
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
        LOG_DEBUG("calling transactions.run with expiry {}ms", expiry.has_value() ? expiry->count() : 0);
        txns->txns->run(*cfg, logic, cb);
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
            return convert_to_python_exc_type(err, true);
        }
        else
        {
            return retval;
        }
    }
    Py_RETURN_NONE;
}
