
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
#include "../exceptions.hxx"
#include "../n1ql.hxx"
#include "../utils.hxx"
#include <core/cluster.hxx>
#include <core/operations.hxx>
#include <core/transactions/durability_level.hxx>
#include <core/transactions/internal/exceptions_internal.hxx>
#include <core/transactions/transaction_get_result.hxx>
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
  txns->txns.reset();
  CB_LOG_DEBUG("dealloc transactions");
}

void
pycbc_txns::dealloc_transaction_context(PyObject* obj)
{
  auto ctx = reinterpret_cast<pycbc_txns::transaction_context*>(PyCapsule_GetPointer(obj, "ctx_"));
  delete ctx;
  CB_LOG_DEBUG("dealloc transaction_context");
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
  add_to_dict(retval,
              "cleanup_window",
              static_cast<int64_t>(conf->cfg->cleanup_config().cleanup_window().count()));
  add_to_dict(retval, "timeout", static_cast<int64_t>(conf->cfg->timeout().count()));
  add_to_dict(retval, "cleanup_lost_attempts", conf->cfg->cleanup_config().cleanup_lost_attempts());
  add_to_dict(
    retval, "cleanup_client_attempts", conf->cfg->cleanup_config().cleanup_client_attempts());
  add_to_dict(retval,
              "scan_consistency",
              scan_consistency_type_to_string(conf->cfg->query_config().scan_consistency()));
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
  { "to_dict",
    (PyCFunction)pycbc_txns::transaction_config__to_dict__,
    METH_NOARGS,
    PyDoc_STR("transaction_config as a dict") },
  { NULL, NULL, 0, NULL }
};

PyObject*
pycbc_txns::transaction_config__new__(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
  PyObject* durability_level = nullptr;
  PyObject* cleanup_window = nullptr;
  PyObject* timeout = nullptr;
  char* scan_consistency = nullptr;
  PyObject* cleanup_lost_attempts = nullptr;
  PyObject* cleanup_client_attempts = nullptr;
  char* metadata_bucket = nullptr;
  char* metadata_scope = nullptr;
  char* metadata_collection = nullptr;

  const char* kw_list[] = { "durability_level",
                            "cleanup_window",
                            "timeout",
                            "cleanup_lost_attempts",
                            "cleanup_client_attempts",
                            "metadata_bucket",
                            "metadata_scope",
                            "metadata_collection",
                            "scan_consistency",
                            nullptr };
  const char* kw_format = "|OOOOOssss";
  auto self = reinterpret_cast<pycbc_txns::transaction_config*>(type->tp_alloc(type, 0));

  self->cfg = new tx::transactions_config();

  if (!PyArg_ParseTupleAndKeywords(args,
                                   kwargs,
                                   kw_format,
                                   const_cast<char**>(kw_list),
                                   &durability_level,
                                   &cleanup_window,
                                   &timeout,
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
    self->cfg->durability_level(
      static_cast<couchbase::durability_level>(PyLong_AsUnsignedLong(durability_level)));
  }
  if (nullptr != cleanup_window) {
    self->cfg->cleanup_config().cleanup_window(
      std::chrono::microseconds(PyLong_AsUnsignedLongLong(cleanup_window)));
  }
  if (nullptr != timeout) {
    self->cfg->timeout(std::chrono::microseconds(PyLong_AsUnsignedLongLong(timeout)));
  }
  if (nullptr != cleanup_lost_attempts) {
    self->cfg->cleanup_config().cleanup_lost_attempts(!!PyObject_IsTrue(cleanup_lost_attempts));
  }
  if (nullptr != cleanup_client_attempts) {
    self->cfg->cleanup_config().cleanup_client_attempts(!!PyObject_IsTrue(cleanup_client_attempts));
  }
  if (nullptr != metadata_bucket && nullptr != metadata_scope && nullptr != metadata_collection) {
    auto keyspace =
      tx::transaction_keyspace{ metadata_bucket, metadata_scope, metadata_collection };
    self->cfg->metadata_collection(keyspace);
  }
  if (nullptr != scan_consistency) {
    self->cfg->query_config().scan_consistency(
      str_to_scan_consistency_type<couchbase::query_scan_consistency>(scan_consistency));
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
  if (opts->opts->timeout()) {
    add_to_dict(retval, "timeout", static_cast<int64_t>(opts->opts->timeout()->count()));
  }
  if (opts->opts->durability_level()) {
    add_to_dict(
      retval, "durability_level", static_cast<int64_t>(opts->opts->durability_level().value()));
  }
  if (opts->opts->scan_consistency()) {
    add_to_dict(
      retval, "scan_consistency", scan_consistency_type_to_string(*opts->opts->scan_consistency()));
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
      stream << "durability: " << tx_core::durability_level_to_string(*opts->durability_level())
             << ", ";
    }
    if (opts->timeout()) {
      stream << "timeout: " << opts->timeout()->count() << "ns, ";
    }
    if (opts->scan_consistency()) {
      stream << "scan_consistency: " << scan_consistency_type_to_string(*opts->scan_consistency());
    }
  }
  stream << "}";
  return PyUnicode_FromString(stream.str().c_str());
}

static PyMethodDef transaction_options_methods[] = {
  { "to_dict",
    (PyCFunction)pycbc_txns::transaction_options__to_dict__,
    METH_NOARGS,
    PyDoc_STR("transaction_options as a dict") },
  { NULL, NULL, 0, NULL }
};

PyObject*
pycbc_txns::transaction_options__new__(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
  PyObject* durability_level = nullptr;
  PyObject* timeout = nullptr;
  char* scan_consistency = nullptr;
  char* metadata_bucket = nullptr;
  char* metadata_scope = nullptr;
  char* metadata_collection = nullptr;

  const char* kw_list[] = {
    "durability_level",    "timeout", "scan_consistency", "metadata_bucket", "metadata_scope",
    "metadata_collection", nullptr
  };
  const char* kw_format = "|OOssss";
  auto self = reinterpret_cast<pycbc_txns::transaction_options*>(type->tp_alloc(type, 0));

  self->opts = new tx::transaction_options();
  CB_LOG_DEBUG("transaction_options__new__ called");
  if (!PyArg_ParseTupleAndKeywords(args,
                                   kwargs,
                                   kw_format,
                                   const_cast<char**>(kw_list),
                                   &durability_level,
                                   &timeout,
                                   &scan_consistency,
                                   &metadata_bucket,
                                   &metadata_scope,
                                   &metadata_collection)) {
    PyErr_SetString(PyExc_ValueError, "couldn't parse args");
    Py_RETURN_NONE;
  }
  if (nullptr != durability_level) {
    self->opts->durability_level(
      static_cast<couchbase::durability_level>(PyLong_AsUnsignedLong(durability_level)));
  }
  if (nullptr != timeout) {
    self->opts->timeout(std::chrono::microseconds(PyLong_AsUnsignedLongLong(timeout)));
  }
  if (nullptr != scan_consistency) {
    self->opts->scan_consistency(
      str_to_scan_consistency_type<couchbase::query_scan_consistency>(scan_consistency));
  }
  if (nullptr != metadata_bucket && nullptr != metadata_scope && nullptr != metadata_collection) {
    auto keyspace =
      tx::transaction_keyspace{ metadata_bucket, metadata_scope, metadata_collection };
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
    add_to_dict(
      retval, "max_parallelism", static_cast<int64_t>(query_opts.max_parallelism.value()));
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
    add_to_dict(retval,
                "scan_consistency",
                scan_consistency_type_to_string(query_opts.scan_consistency.value()));
  }
  if (query_opts.profile.has_value()) {
    add_to_dict(retval, "profile", profile_mode_to_str(query_opts.profile.value()));
  }

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

static PyMethodDef transaction_query_options_methods[] = {
  { "to_dict",
    (PyCFunction)pycbc_txns::transaction_query_options__to_dict__,
    METH_NOARGS,
    PyDoc_STR("transaction_query_options as a dict") },
  { NULL, NULL, 0, NULL }
};

PyObject*
pycbc_txns::transaction_query_options__new__(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
  PyObject* pyObj_query_args = nullptr;
  const char* kw_list[] = { "query_args", nullptr };
  const char* kw_format = "|O";
  if (!PyArg_ParseTupleAndKeywords(
        args, kwargs, kw_format, const_cast<char**>(kw_list), &pyObj_query_args)) {
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
  if (req.profile.has_value()) {
    self->opts->profile(req.profile.value());
  }
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
  result->res.reset();
  Py_TYPE(result)->tp_free((PyObject*)result);
  CB_LOG_DEBUG("dealloc transaction_get_result");
}

PyObject*
pycbc_txns::transaction_get_result__str__(pycbc_txns::transaction_get_result* result)
{
  if (result->res->content().data.size() != 0) {
    auto value = reinterpret_cast<const char*>(result->res->content().data.data());
    auto flags = result->res->content().flags;
    return PyUnicode_FromFormat("transaction_get_result:{key=%s, cas=%llu, value=%s, flags=%lu}",
                                result->res->id().key().c_str(),
                                result->res->cas(),
                                value,
                                flags);
  } else {
    return PyUnicode_FromFormat("transaction_get_result:{key=%s, cas=%llu}",
                                result->res->id().key().c_str(),
                                result->res->cas());
  }
}

// TODO: a better way later, perhaps an exposed enum like operations
const std::string ID{ "id" };
const std::string CAS{ "cas" };
const std::string VALUE{ "value" };

PyObject*
pycbc_txns::transaction_get_result__get__(pycbc_txns::transaction_get_result* result,
                                          PyObject* args)
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
    PyObject* pyObj_value = nullptr;
    PyObject* pyObj_flags = PyLong_FromUnsignedLong(result->res->content().flags);
    try {
      pyObj_value = binary_to_PyObject(result->res->content().data);
    } catch (const std::exception& e) {
      PyErr_SetString(PyExc_TypeError, e.what());
      Py_RETURN_NONE;
    }
    PyObject* pyObj_result = PyTuple_Pack(2, pyObj_value, pyObj_flags);
    Py_DECREF(pyObj_value);
    Py_DECREF(pyObj_flags);
    return pyObj_result;
  }
  PyErr_SetString(PyExc_ValueError, fmt::format("unknown field_name {}", field_name).c_str());
  Py_RETURN_NONE;
}

static PyMethodDef transaction_get_result_methods[] = {
  { "get",
    (PyCFunction)pycbc_txns::transaction_get_result__get__,
    METH_VARARGS,
    PyDoc_STR("get field in result object") },
  { NULL, NULL, 0, NULL }
};

PyObject*
pycbc_txns::transaction_get_result__new__(PyTypeObject* type, PyObject*, PyObject*)
{
  auto self = reinterpret_cast<pycbc_txns::transaction_get_result*>(type->tp_alloc(type, 0));
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
  PyObject_SetItem(
    pyObj_kwargs, PyUnicode_FromString("module"), PyModule_GetNameObject(pyObj_module));
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
    if (PyModule_AddObject(pyObj_module,
                           "transaction_get_result",
                           reinterpret_cast<PyObject*>(&transaction_get_result_type)) == 0) {
      if (PyType_Ready(&transaction_config_type) == 0) {
        Py_INCREF(&transaction_config_type);
        if (PyModule_AddObject(pyObj_module,
                               "transaction_config",
                               reinterpret_cast<PyObject*>(&transaction_config_type)) == 0) {
          if (PyType_Ready(&transaction_query_options_type) == 0) {
            Py_INCREF(&transaction_query_options_type);
            if (PyModule_AddObject(pyObj_module,
                                   "transaction_query_options",
                                   reinterpret_cast<PyObject*>(&transaction_query_options_type)) ==
                0) {
              if (PyType_Ready(&transaction_options_type) == 0) {
                Py_INCREF(&transaction_options_type);
                if (PyModule_AddObject(pyObj_module,
                                       "transaction_options",
                                       reinterpret_cast<PyObject*>(&transaction_options_type)) ==
                    0) {
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
  int ret = PyArg_ParseTupleAndKeywords(args,
                                        kwargs,
                                        kw_format,
                                        const_cast<char**>(kw_list),
                                        &PyCapsule_Type,
                                        &pyObj_conn,
                                        &pyObj_config);

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

  std::pair<std::error_code, std::shared_ptr<tx_core::transactions>> res;
  Py_BEGIN_ALLOW_THREADS auto conn =
    reinterpret_cast<connection*>(PyCapsule_GetPointer(pyObj_conn, "conn_"));
  auto txn_config = reinterpret_cast<pycbc_txns::transaction_config*>(pyObj_config)->cfg;
  std::future<std::pair<std::error_code, std::shared_ptr<tx_core::transactions>>> fut =
    tx_core::transactions::create(conn->cluster_, *txn_config);
  res = fut.get();
  Py_END_ALLOW_THREADS if (res.first.value())
  {
    pycbc_set_python_exception(res.first, __FILE__, __LINE__, res.first.message().c_str());
    return nullptr;
  }
  pycbc_txns::transactions* txns = new pycbc_txns::transactions(res.second);
  PyObject* pyObj_txns = PyCapsule_New(txns, "txns_", dealloc_transactions);
  return pyObj_txns;
}

PyObject*
pycbc_txns::destroy_transactions([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* pyObj_txns = nullptr;
  const char* kw_list[] = { "txns", nullptr };
  const char* kw_format = "O!";
  if (!PyArg_ParseTupleAndKeywords(
        args, kwargs, kw_format, const_cast<char**>(kw_list), &PyCapsule_Type, &pyObj_txns)) {
    PyErr_SetString(PyExc_ValueError, "couldn't parse args");
    Py_RETURN_NONE;
  }
  if (nullptr == pyObj_txns) {
    PyErr_SetString(PyExc_ValueError, "expected a transactions object");
    Py_RETURN_NONE;
  }
  auto txns =
    reinterpret_cast<pycbc_txns::transactions*>(PyCapsule_GetPointer(pyObj_txns, "txns_"));
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

std::string
txn_external_exception_to_string(tx_core::external_exception ext_exception)
{
  switch (ext_exception) {
    case tx_core::external_exception::UNKNOWN:
      return "unknown";
    case tx_core::external_exception::COUCHBASE_EXCEPTION:
      return "couchbase_exception";
    case tx_core::external_exception::NOT_SET:
      return "not_set";
    case tx_core::external_exception::ACTIVE_TRANSACTION_RECORD_ENTRY_NOT_FOUND:
      return "active_transaction_record_entry_not_found";
    case tx_core::external_exception::ACTIVE_TRANSACTION_RECORD_FULL:
      return "active_transaction_record_full";
    case tx_core::external_exception::COMMIT_NOT_PERMITTED:
      return "commit_not_permitted";
    case tx_core::external_exception::ACTIVE_TRANSACTION_RECORD_NOT_FOUND:
      return "active_transaction_record_not_found";
    case tx_core::external_exception::CONCURRENT_OPERATIONS_DETECTED_ON_SAME_DOCUMENT:
      return "concurrent_operations_detected_on_same_document";
    case tx_core::external_exception::DOCUMENT_ALREADY_IN_TRANSACTION:
      return "document_already_in_transaction";
    case tx_core::external_exception::DOCUMENT_EXISTS_EXCEPTION:
      return "document_exists_exception";
    case tx_core::external_exception::DOCUMENT_NOT_FOUND_EXCEPTION:
      return "document_not_found_exception";
    case tx_core::external_exception::FEATURE_NOT_AVAILABLE_EXCEPTION:
      return "feature_not_available_exception";
    case tx_core::external_exception::FORWARD_COMPATIBILITY_FAILURE:
      return "forward_compatibility_failure";
    case tx_core::external_exception::ILLEGAL_STATE_EXCEPTION:
      return "illegal_state_exception";
    case tx_core::external_exception::PARSING_FAILURE:
      return "parsing_failure";
    case tx_core::external_exception::PREVIOUS_OPERATION_FAILED:
      return "previous_operation_failed";
    case tx_core::external_exception::REQUEST_CANCELED_EXCEPTION:
      return "request_canceled_exception";
    case tx_core::external_exception::ROLLBACK_NOT_PERMITTED:
      return "rollback_not_permitted";
    case tx_core::external_exception::SERVICE_NOT_AVAILABLE_EXCEPTION:
      return "service_not_available_exception";
    case tx_core::external_exception::TRANSACTION_ABORTED_EXTERNALLY:
      return "transaction_aborted_externally";
    case tx_core::external_exception::TRANSACTION_ALREADY_ABORTED:
      return "transaction_already_aborted";
    case tx_core::external_exception::TRANSACTION_ALREADY_COMMITTED:
      return "transaction_already_committed";
  }
  return "unknown";
}

PyObject*
create_python_exception(pycbc_txns::TxnExceptionType exc_type,
                        const char* message,
                        bool set_exception = false,
                        PyObject* pyObj_inner_exc = nullptr)
{
  static PyObject* pyObj_txn_failed = init_transaction_exception_type("TransactionFailed");
  static PyObject* pyObj_txn_expired = init_transaction_exception_type("TransactionExpired");
  static PyObject* pyObj_txn_ambig = init_transaction_exception_type("TransactionCommitAmbiguous");
  static PyObject* pyObj_txn_op_failed =
    init_transaction_exception_type("TransactionOperationFailed");
  static PyObject* pyObj_document_exists_ex =
    init_transaction_exception_type("DocumentExistsException");
  static PyObject* pyObj_document_not_found_ex =
    init_transaction_exception_type("DocumentNotFoundException");
  static PyObject* pyObj_query_parsing_failure =
    init_transaction_exception_type("ParsingFailedException");
  static PyObject* pyObj_couchbase_error = init_transaction_exception_type("CouchbaseException");
  static PyObject* pyObj_feature_not_available_error =
    init_transaction_exception_type("FeatureUnavailableException");

  PyObject* pyObj_final_error = nullptr;
  PyObject* pyObj_exc_type = nullptr;
  PyObject* pyObj_error_ctx = PyDict_New();

  switch (exc_type) {
    case pycbc_txns::TxnExceptionType::TRANSACTION_FAILED: {
      pyObj_exc_type = pyObj_txn_failed;
      break;
    }
    case pycbc_txns::TxnExceptionType::TRANSACTION_COMMIT_AMBIGUOUS: {
      pyObj_exc_type = pyObj_txn_ambig;
      break;
    }
    case pycbc_txns::TxnExceptionType::TRANSACTION_EXPIRED: {
      pyObj_exc_type = pyObj_txn_expired;
      break;
    }
    case pycbc_txns::TxnExceptionType::TRANSACTION_OPERATION_FAILED: {
      pyObj_exc_type = pyObj_txn_op_failed;
      break;
    }
    case pycbc_txns::TxnExceptionType::FEATURE_NOT_AVAILABLE: {
      pyObj_exc_type = pyObj_feature_not_available_error;
      break;
    }
    case pycbc_txns::TxnExceptionType::QUERY_PARSING_FAILURE: {
      pyObj_exc_type = pyObj_query_parsing_failure;
      break;
    }
    case pycbc_txns::TxnExceptionType::DOCUMENT_EXISTS: {
      pyObj_exc_type = pyObj_document_exists_ex;
      break;
    }
    case pycbc_txns::TxnExceptionType::DOCUMENT_NOT_FOUND: {
      pyObj_exc_type = pyObj_document_not_found_ex;
      break;
    }
    case pycbc_txns::TxnExceptionType::COUCHBASE_ERROR:
    default:
      pyObj_exc_type = pyObj_couchbase_error;
  }
  PyObject* pyObj_tmp = PyUnicode_FromString(message);
  PyDict_SetItemString(pyObj_error_ctx, "message", pyObj_tmp);
  Py_DECREF(pyObj_tmp);
  if (pyObj_inner_exc != nullptr) {
    pyObj_tmp = PyDict_GetItemString(pyObj_inner_exc, "inner_cause");
    if (pyObj_tmp != nullptr) {
      PyDict_SetItemString(pyObj_error_ctx, "exc_info", pyObj_inner_exc);
      Py_DECREF(pyObj_inner_exc);
    }
    Py_DECREF(pyObj_tmp);
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

PyObject*
convert_to_python_exc_type(std::exception_ptr err,
                           bool set_exception = false,
                           PyObject* pyObj_inner_exc = nullptr)
{
  auto exc_type = pycbc_txns::TxnExceptionType::COUCHBASE_ERROR;
  const char* message = nullptr;

  // Must be an error
  assert(!!err);

  try {
    std::rethrow_exception(err);
  } catch (const tx_core::transaction_exception& e) {
    switch (e.type()) {
      case tx_core::failure_type::FAIL:
        exc_type = pycbc_txns::TxnExceptionType::TRANSACTION_FAILED;
        break;
      case tx_core::failure_type::COMMIT_AMBIGUOUS:
        exc_type = pycbc_txns::TxnExceptionType::TRANSACTION_COMMIT_AMBIGUOUS;
        break;
      case tx_core::failure_type::EXPIRY:
        exc_type = pycbc_txns::TxnExceptionType::TRANSACTION_EXPIRED;
        break;
    }
    message = e.what();
  } catch (const tx_core::transaction_operation_failed& e) {
    if (e.cause() == tx_core::external_exception::FEATURE_NOT_AVAILABLE_EXCEPTION) {
      exc_type = pycbc_txns::TxnExceptionType::FEATURE_NOT_AVAILABLE;
      message = "Possibly attempting a binary transaction operation with a server version < 7.6.2";
    } else {
      // follow logic that was used in C++ core transactions::wrap_run() to call
      // transaction_context::handle_error() which boils down to (not exactly, should suffice
      // for our purposes) calling transaction_operation_failed::get_final_exception()
      if (e.to_raise() == tx_core::final_error::EXPIRED) {
        exc_type = pycbc_txns::TxnExceptionType::TRANSACTION_EXPIRED;
      } else if (e.to_raise() == tx_core::final_error::AMBIGUOUS) {
        exc_type = pycbc_txns::TxnExceptionType::TRANSACTION_COMMIT_AMBIGUOUS;
      } else {
        exc_type = pycbc_txns::TxnExceptionType::TRANSACTION_OPERATION_FAILED;
      }
      message = e.what();
    }
  } catch (const tx_core::query_parsing_failure& e) {
    exc_type = pycbc_txns::TxnExceptionType::QUERY_PARSING_FAILURE;
    message = e.what();
  } catch (const tx_core::document_exists& e) {
    exc_type = pycbc_txns::TxnExceptionType::DOCUMENT_EXISTS;
    message = e.what();
  } catch (const tx_core::document_not_found& e) {
    exc_type = pycbc_txns::TxnExceptionType::DOCUMENT_NOT_FOUND;
    message = e.what();
  } catch (const tx_core::op_exception& e) {
    message = e.what();
  } catch (const std::exception& e) {
    message = e.what();
  } catch (...) {
    message = "Unknown error";
  }
  return create_python_exception(exc_type, message, set_exception, pyObj_inner_exc);
}

void
handle_returning_void(PyObject* pyObj_callback,
                      PyObject* pyObj_errback,
                      std::shared_ptr<std::promise<PyObject*>> barrier,
                      std::exception_ptr err)
{
  auto state = PyGILState_Ensure();
  PyObject* pyObj_args = nullptr;
  PyObject* pyObj_func = nullptr;
  PyObject* pyObj_err = nullptr;
  if (err) {
    pyObj_err = convert_to_python_exc_type(err);
    if (nullptr == pyObj_errback) {
      barrier->set_value(pyObj_err);
    } else {
      pyObj_args = PyTuple_New(1);
      PyTuple_SetItem(pyObj_args, 0, pyObj_err);
      pyObj_func = pyObj_errback;
    }
  } else {
    Py_INCREF(Py_None);
    if (nullptr == pyObj_callback) {
      barrier->set_value(Py_None);
    } else {
      pyObj_args = PyTuple_New(1);
      PyTuple_SetItem(pyObj_args, 0, Py_None);
      pyObj_func = pyObj_callback;
    }
  }
  if (nullptr != pyObj_func) {
    PyObject_CallObject(pyObj_func, pyObj_args);
    Py_DECREF(pyObj_errback);
    Py_DECREF(pyObj_callback);
    Py_DECREF(pyObj_args);
  }
  PyGILState_Release(state);
}

void
handle_returning_transaction_get_result(PyObject* pyObj_callback,
                                        PyObject* pyObj_errback,
                                        std::shared_ptr<std::promise<PyObject*>> barrier,
                                        std::exception_ptr err,
                                        std::optional<tx_core::transaction_get_result> res)
{
  // TODO: flesh out transaction_get_result and exceptions...
  auto state = PyGILState_Ensure();
  PyObject* pyObj_args = nullptr;
  PyObject* pyObj_func = nullptr;
  PyObject* pyObj_err = nullptr;
  if (err) {
    pyObj_err = convert_to_python_exc_type(err);
    if (nullptr == pyObj_errback) {
      barrier->set_value(pyObj_err);
    } else {
      pyObj_args = PyTuple_New(1);
      PyTuple_SetItem(pyObj_args, 0, pyObj_err);
      pyObj_func = pyObj_errback;
    }
  } else {
    PyObject* pyObj_get_result = nullptr;

    // BUG(PYCBC-1476): We should revert to using direct get
    // operations once the underlying issue has been resolved.
    if (!res.has_value()) {
      pyObj_get_result = pycbc_build_exception(
        couchbase::errc::make_error_code(couchbase::errc::key_value::document_not_found),
        __FILE__,
        __LINE__,
        "Txn get op: document not found.");
    } else {
      pyObj_get_result =
        PyObject_CallObject(reinterpret_cast<PyObject*>(&transaction_get_result_type), nullptr);
      auto result = reinterpret_cast<pycbc_txns::transaction_get_result*>(pyObj_get_result);
      result->res = std::make_unique<tx_core::transaction_get_result>(std::move(res.value()));
    }

    if (nullptr == pyObj_callback) {
      barrier->set_value(pyObj_get_result);
    } else {
      pyObj_args = PyTuple_New(1);
      PyTuple_SetItem(pyObj_args, 0, pyObj_get_result);
      pyObj_func = pyObj_callback;
    }
  }
  if (nullptr != pyObj_func) {
    PyObject_CallObject(pyObj_func, pyObj_args);
    Py_DECREF(pyObj_errback);
    Py_DECREF(pyObj_callback);
    Py_DECREF(pyObj_args);
  }
  PyGILState_Release(state);
}

void
handle_returning_query_result(PyObject* pyObj_callback,
                              PyObject* pyObj_errback,
                              std::shared_ptr<std::promise<PyObject*>> barrier,
                              std::exception_ptr err,
                              std::optional<couchbase::core::operations::query_response> res)
{
  auto state = PyGILState_Ensure();
  PyObject* pyObj_args = nullptr;
  PyObject* pyObj_func = nullptr;
  PyObject* pyObj_err = nullptr;
  if (err) {
    pyObj_err = convert_to_python_exc_type(err);
    if (nullptr == pyObj_errback) {
      barrier->set_value(pyObj_err);
    } else {
      pyObj_args = PyTuple_New(1);
      PyTuple_SetItem(pyObj_args, 0, pyObj_err);
      pyObj_func = pyObj_errback;
    }
  } else {
    PyObject* pyObj_json = PyBytes_FromString(res->ctx.http_body.c_str());
    if (nullptr == pyObj_callback) {
      barrier->set_value(pyObj_json);
    } else {
      pyObj_args = PyTuple_New(1);
      PyTuple_SetItem(pyObj_args, 0, pyObj_json);
      pyObj_func = pyObj_callback;
    }
  }
  if (nullptr != pyObj_func) {
    PyObject_CallObject(pyObj_func, pyObj_args);
    Py_DECREF(pyObj_errback);
    Py_DECREF(pyObj_callback);
    Py_DECREF(pyObj_args);
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
    PyErr_SetString(PyExc_ValueError, "expected transaction_context");
    Py_RETURN_NONE;
  }
  auto ctx =
    reinterpret_cast<pycbc_txns::transaction_context*>(PyCapsule_GetPointer(pyObj_ctx, "ctx_"));
  if (nullptr == ctx) {
    PyErr_SetString(PyExc_ValueError, "passed null transaction_context");
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
  auto barrier = std::make_shared<std::promise<PyObject*>>();
  auto fut = barrier->get_future();
  Py_BEGIN_ALLOW_THREADS ctx->ctx->query(
    statement,
    *opt->opts,
    [pyObj_callback, pyObj_errback, barrier](
      std::exception_ptr err, std::optional<couchbase::core::operations::query_response> resp) {
      handle_returning_query_result(pyObj_callback, pyObj_errback, barrier, err, resp);
    });
  Py_END_ALLOW_THREADS if (nullptr == pyObj_callback || nullptr == pyObj_errback)
  {
    PyObject* ret = nullptr;
    Py_BEGIN_ALLOW_THREADS ret = fut.get();
    Py_END_ALLOW_THREADS return ret;
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
  couchbase::codec::encoded_value value{};
  TxOperations::TxOperationType op_type = TxOperations::UNKNOWN;
  const char* kw_list[] = { "ctx",      "bucket",  "scope", "collection_name", "key",  "op",
                            "callback", "errback", "value", "txn_get_result",  nullptr };
  const char* kw_format = "O!|ssssIOOOO";

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
    PyObject* pyObj_data = PyTuple_GET_ITEM(pyObj_value, 0);
    PyObject* pyObj_flags = PyTuple_GET_ITEM(pyObj_value, 1);
    value.flags = static_cast<uint32_t>(PyLong_AsLong(pyObj_flags));
    try {
      value.data = PyObject_to_binary(pyObj_data);
    } catch (const std::exception& e) {
      pycbc_set_python_exception(PycbcError::InvalidArgument, __FILE__, __LINE__, e.what());
      Py_RETURN_NONE;
    }
  }
  if (nullptr == pyObj_ctx) {
    PyErr_SetString(PyExc_ValueError, "no transaction_context passed in");
    Py_RETURN_NONE;
  }
  auto ctx =
    reinterpret_cast<pycbc_txns::transaction_context*>(PyCapsule_GetPointer(pyObj_ctx, "ctx_"));
  if (nullptr == ctx) {
    PyErr_SetString(PyExc_ValueError, "passed null transaction_context");
    Py_RETURN_NONE;
  }

  Py_XINCREF(pyObj_callback);
  Py_XINCREF(pyObj_errback);

  auto barrier = std::make_shared<std::promise<PyObject*>>();
  auto fut = barrier->get_future();
  switch (op_type) {
    case TxOperations::GET: {
      if (nullptr == bucket || nullptr == scope || nullptr == collection || nullptr == key) {
        PyErr_SetString(PyExc_ValueError, "couldn't create document id for get");
        Py_RETURN_NONE;
      }
      couchbase::core::document_id id{ bucket, scope, collection, key };
      Py_BEGIN_ALLOW_THREADS ctx->ctx->get_optional(
        id,
        [barrier, pyObj_callback, pyObj_errback](
          std::exception_ptr err, std::optional<tx_core::transaction_get_result> res) {
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
        PyErr_SetString(PyExc_ValueError,
                        fmt::format("no value given for an insert of key {}", id.key()).c_str());
        Py_RETURN_NONE;
      }
      Py_BEGIN_ALLOW_THREADS ctx->ctx->insert(
        id,
        value,
        [barrier, pyObj_callback, pyObj_errback](
          std::exception_ptr err, std::optional<tx_core::transaction_get_result> res) {
          handle_returning_transaction_get_result(pyObj_callback, pyObj_errback, barrier, err, res);
        });
      Py_END_ALLOW_THREADS break;
    }
    case TxOperations::REPLACE: {
      if (nullptr == pyObj_value) {
        PyErr_SetString(PyExc_ValueError, "replace expects a value");
        Py_RETURN_NONE;
      }
      if (nullptr == pyObj_txn_get_result ||
          0 == PyObject_TypeCheck(pyObj_txn_get_result, &transaction_get_result_type)) {
        PyErr_SetString(PyExc_ValueError, "replace expects to be passed a transaction_get_result");
        Py_RETURN_NONE;
      }
      auto tx_get_result =
        reinterpret_cast<pycbc_txns::transaction_get_result*>(pyObj_txn_get_result);
      Py_BEGIN_ALLOW_THREADS ctx->ctx->replace(
        *tx_get_result->res,
        value,
        [pyObj_callback, pyObj_errback, barrier](
          std::exception_ptr err, std::optional<tx_core::transaction_get_result> res) {
          handle_returning_transaction_get_result(pyObj_callback, pyObj_errback, barrier, err, res);
        });
      Py_END_ALLOW_THREADS break;
    }
    case TxOperations::REMOVE: {
      if (nullptr == pyObj_txn_get_result ||
          0 == PyObject_TypeCheck(pyObj_txn_get_result, &transaction_get_result_type)) {
        PyErr_SetString(PyExc_ValueError, "remove expects to be passed a transaction_get_result");
        Py_RETURN_NONE;
      }
      auto tx_get_result =
        reinterpret_cast<pycbc_txns::transaction_get_result*>(pyObj_txn_get_result);
      Py_BEGIN_ALLOW_THREADS ctx->ctx->remove(
        *tx_get_result->res, [pyObj_callback, pyObj_errback, barrier](std::exception_ptr err) {
          handle_returning_void(pyObj_callback, pyObj_errback, barrier, err);
        });
      Py_END_ALLOW_THREADS break;
    }
    default:
      // return error!
      PyErr_SetString(PyExc_ValueError, "unknown txn operation");
  }
  if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
    PyObject* ret = nullptr;
    Py_BEGIN_ALLOW_THREADS ret = fut.get();
    Py_END_ALLOW_THREADS return ret;
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
pycbc_txns::create_new_attempt_context([[maybe_unused]] PyObject* self,
                                       PyObject* args,
                                       PyObject* kwargs)
{
  PyObject* pyObj_ctx = nullptr;
  PyObject* pyObj_callback = nullptr;
  PyObject* pyObj_errback = nullptr;
  const char* kw_list[] = { "ctx", "callback", "errback", nullptr };
  const char* kw_format = "O!|OO";
  int ret = PyArg_ParseTupleAndKeywords(args,
                                        kwargs,
                                        kw_format,
                                        const_cast<char**>(kw_list),
                                        &PyCapsule_Type,
                                        &pyObj_ctx,
                                        &pyObj_callback,
                                        &pyObj_errback);
  if (!ret) {
    PyErr_SetString(PyExc_ValueError, "couldn't parse args");
    return nullptr;
  }
  auto ctx =
    reinterpret_cast<pycbc_txns::transaction_context*>(PyCapsule_GetPointer(pyObj_ctx, "ctx_"));

  if (nullptr == ctx) {
    PyErr_SetString(PyExc_ValueError, "passed null transaction context");
    return nullptr;
  }

  std::shared_ptr<std::promise<PyObject*>> barrier = nullptr;
  std::future<PyObject*> fut;
  Py_XINCREF(pyObj_callback);
  Py_XINCREF(pyObj_errback);
  if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
    barrier = std::make_shared<std::promise<PyObject*>>();
    fut = barrier->get_future();
  }
  Py_BEGIN_ALLOW_THREADS ctx->ctx->new_attempt_context(
    [barrier, pyObj_callback, pyObj_errback](std::exception_ptr err) {
      handle_returning_void(pyObj_callback, pyObj_errback, barrier, err);
    });
  Py_END_ALLOW_THREADS if (nullptr == pyObj_callback || nullptr == pyObj_errback)
  {
    PyObject* ret = nullptr;
    Py_BEGIN_ALLOW_THREADS ret = fut.get();
    Py_END_ALLOW_THREADS return ret;
  }
  Py_RETURN_NONE;
}

PyObject*
pycbc_txns::create_transaction_context([[maybe_unused]] PyObject* self,
                                       PyObject* args,
                                       PyObject* kwargs)
{
  PyObject* pyObj_txns = nullptr;
  PyObject* pyObj_transaction_options = nullptr;
  const char* kw_list[] = { "txns", "transaction_options", nullptr };
  const char* kw_format = "O!|O";
  int ret = PyArg_ParseTupleAndKeywords(args,
                                        kwargs,
                                        kw_format,
                                        const_cast<char**>(kw_list),
                                        &PyCapsule_Type,
                                        &pyObj_txns,
                                        &pyObj_transaction_options);
  if (!ret) {
    PyErr_SetString(PyExc_ValueError, "couldn't parse args");
    return nullptr;
  }
  auto txns =
    reinterpret_cast<pycbc_txns::transactions*>(PyCapsule_GetPointer(pyObj_txns, "txns_"));

  if (nullptr == txns) {
    PyErr_SetString(PyExc_ValueError, "passed null transactions");
    return nullptr;
  }
  if (nullptr != pyObj_transaction_options) {
    if (!PyObject_IsInstance(pyObj_transaction_options,
                             reinterpret_cast<PyObject*>(&transaction_options_type))) {
      PyErr_SetString(PyExc_ValueError, "expected a valid transaction_options object");
      return nullptr;
    }
  }

  auto tx_options =
    nullptr != pyObj_transaction_options && Py_None != pyObj_transaction_options
      ? *(reinterpret_cast<pycbc_txns::transaction_options*>(pyObj_transaction_options)->opts)
      : tx::transaction_options();
  auto py_ctx = new pycbc_txns::transaction_context(
    tx_core::transaction_context::create(*txns->txns, tx_options));
  PyObject* pyObj_ctx = PyCapsule_New(py_ctx, "ctx_", dealloc_transaction_context);
  return pyObj_ctx;
}

PyObject*
pycbc_txns::transaction_commit([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* pyObj_ctx = nullptr;
  PyObject* pyObj_callback = nullptr;
  PyObject* pyObj_errback = nullptr;
  const char* kw_list[] = { "ctx", "callback", "errback", nullptr };
  const char* kw_format = "O!|OO";
  int ret = PyArg_ParseTupleAndKeywords(args,
                                        kwargs,
                                        kw_format,
                                        const_cast<char**>(kw_list),
                                        &PyCapsule_Type,
                                        &pyObj_ctx,
                                        &pyObj_callback,
                                        &pyObj_errback);
  if (!ret) {
    PyErr_SetString(PyExc_ValueError, "couldn't parse args");
    return nullptr;
  }
  auto ctx =
    reinterpret_cast<pycbc_txns::transaction_context*>(PyCapsule_GetPointer(pyObj_ctx, "ctx_"));

  if (nullptr == ctx) {
    PyErr_SetString(PyExc_ValueError, "passed null transaction context");
    return nullptr;
  }

  std::shared_ptr<std::promise<PyObject*>> barrier = nullptr;
  std::future<PyObject*> fut;
  Py_XINCREF(pyObj_callback);
  Py_XINCREF(pyObj_errback);
  if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
    barrier = std::make_shared<std::promise<PyObject*>>();
    fut = barrier->get_future();
  }
  Py_BEGIN_ALLOW_THREADS ctx->ctx->finalize(
    [pyObj_callback, pyObj_errback, barrier](std::optional<tx_core::transaction_exception> err,
                                             std::optional<tx::transaction_result> res) {
      auto state = PyGILState_Ensure();
      PyObject* pyObj_args = nullptr;
      PyObject* pyObj_func = nullptr;
      PyObject* pyObj_err = nullptr;
      auto exc_type = pycbc_txns::TxnExceptionType::COUCHBASE_ERROR;
      if (err) {
        switch (err->type()) {
          case tx_core::failure_type::FAIL:
            exc_type = pycbc_txns::TxnExceptionType::TRANSACTION_FAILED;
            break;
          case tx_core::failure_type::COMMIT_AMBIGUOUS:
            exc_type = pycbc_txns::TxnExceptionType::TRANSACTION_COMMIT_AMBIGUOUS;
            break;
          case tx_core::failure_type::EXPIRY:
            exc_type = pycbc_txns::TxnExceptionType::TRANSACTION_EXPIRED;
            break;
        }
        auto message = txn_external_exception_to_string(err->cause());
        pyObj_err = create_python_exception(exc_type, message.c_str());
        if (nullptr == pyObj_errback) {
          barrier->set_value(pyObj_err);
        } else {
          pyObj_args = PyTuple_New(1);
          PyTuple_SET_ITEM(pyObj_args, 0, pyObj_err);
          pyObj_func = pyObj_errback;
        }
      } else {
        PyObject* ret = transaction_result_to_dict(res);
        if (nullptr == pyObj_callback) {
          barrier->set_value(ret);
        } else {
          pyObj_args = PyTuple_New(1);
          PyTuple_SET_ITEM(pyObj_args, 0, ret);
          pyObj_func = pyObj_callback;
        }
      }
      if (nullptr != pyObj_func) {
        PyObject_CallObject(pyObj_func, pyObj_args);
        Py_DECREF(pyObj_errback);
        Py_DECREF(pyObj_callback);
        Py_DECREF(pyObj_args);
      }
      PyGILState_Release(state);
    });
  Py_END_ALLOW_THREADS if (nullptr == pyObj_callback || nullptr == pyObj_errback)
  {
    PyObject* ret = nullptr;
    Py_BEGIN_ALLOW_THREADS ret = fut.get();
    Py_END_ALLOW_THREADS return ret;
  }
  Py_RETURN_NONE;
}

PyObject*
pycbc_txns::transaction_rollback([[maybe_unused]] PyObject* self, PyObject* args, PyObject* kwargs)
{
  PyObject* pyObj_ctx = nullptr;
  PyObject* pyObj_callback = nullptr;
  PyObject* pyObj_errback = nullptr;
  const char* kw_list[] = { "ctx", "callback", "errback", nullptr };
  const char* kw_format = "O!|OO";
  int ret = PyArg_ParseTupleAndKeywords(args,
                                        kwargs,
                                        kw_format,
                                        const_cast<char**>(kw_list),
                                        &PyCapsule_Type,
                                        &pyObj_ctx,
                                        &pyObj_callback,
                                        &pyObj_errback);
  if (!ret) {
    PyErr_SetString(PyExc_ValueError, "couldn't parse args");
    return nullptr;
  }
  auto ctx =
    reinterpret_cast<pycbc_txns::transaction_context*>(PyCapsule_GetPointer(pyObj_ctx, "ctx_"));

  if (nullptr == ctx) {
    PyErr_SetString(PyExc_ValueError, "passed null transaction context");
    return nullptr;
  }
  std::shared_ptr<std::promise<PyObject*>> barrier = nullptr;
  std::future<PyObject*> fut;
  Py_XINCREF(pyObj_callback);
  Py_XINCREF(pyObj_errback);
  if (nullptr == pyObj_callback || nullptr == pyObj_errback) {
    barrier = std::make_shared<std::promise<PyObject*>>();
    fut = barrier->get_future();
  }
  Py_BEGIN_ALLOW_THREADS
  {
    ctx->ctx->rollback([pyObj_callback, pyObj_errback, barrier](std::exception_ptr err) {
      handle_returning_void(pyObj_callback, pyObj_errback, barrier, err);
    });
  }
  Py_END_ALLOW_THREADS if (nullptr == pyObj_callback || nullptr == pyObj_errback)
  {
    PyObject* ret = nullptr;
    Py_BEGIN_ALLOW_THREADS ret = fut.get();
    Py_END_ALLOW_THREADS return ret;
  }
  Py_RETURN_NONE;
}
