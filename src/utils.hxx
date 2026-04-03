/*
 *   Copyright 2016-2026. Couchbase, Inc.
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

#include "Python.h"
#include "cpp_core_enums_autogen.hxx"
#include "cpp_types.hxx"
#include "deprecated_tracing.hxx"
#include "exceptions.hxx"
#include "pytocbpp_defs.hxx"
#include <chrono>
#include <core/cluster_credentials.hxx>
#include <core/cluster_options.hxx>
#include <core/io/dns_config.hxx>
#include <core/operations/document_analytics.hxx>
#include <core/operations/document_query.hxx>
#include <core/operations/document_search.hxx>
#include <core/operations/document_view.hxx>
#include <core/tracing/wrapper_sdk_tracer.hxx>

// Note: Error context template specializations have been moved to error_contexts.hxx
// to avoid circular dependencies between exceptions.hxx and utils.hxx.

namespace pycbc
{

inline bool
validate_and_incref_callbacks(PyObject*& pyObj_callback, PyObject*& pyObj_errback)
{
  if (pyObj_callback != nullptr && !PyCallable_Check(pyObj_callback)) {
    raise_invalid_argument("callback must be callable", __FILE__, __LINE__);
    return false;
  }

  if (pyObj_errback != nullptr && !PyCallable_Check(pyObj_errback)) {
    raise_invalid_argument("errback must be callable", __FILE__, __LINE__);
    return false;
  }

  Py_XINCREF(pyObj_callback);
  Py_XINCREF(pyObj_errback);

  return true;
}

template<typename T>
inline void
extract_field(PyObject* kwargs, const char* key, T& dest)
{
  PyObject* pyObj = PyDict_GetItemString(kwargs, key);
  if (pyObj != nullptr) {
    dest = py_to_cbpp<T>(pyObj);
  }
}

template<typename T>
inline void
extract_field_if_not_empty(PyObject* dict, const char* key, T& dest)
{
  PyObject* pyObj = PyDict_GetItemString(dict, key);
  if (pyObj != nullptr && pyObj != Py_None) {
    dest = py_to_cbpp<T>(pyObj);
  }
}

// Extract boolean field from dict (specialized - checks Py_True/Py_False directly)
// Preserves defaults when key is missing or None (doesn't treat None as false)
inline void
extract_bool_field(PyObject* dict, const char* key, bool& dest)
{
  PyObject* pyObj = PyDict_GetItemString(dict, key);
  if (pyObj == Py_True) {
    dest = true;
  } else if (pyObj == Py_False) {
    dest = false;
  }
}

template<typename T>
inline bool
extract_required_field(PyObject* kwargs,
                       const char* key,
                       T& dest,
                       const char* context,
                       const char* file,
                       int line)
{
  PyObject* pyObj = PyDict_GetItemString(kwargs, key);
  if (pyObj == nullptr || pyObj == Py_None) {
    std::string msg = "Missing required '";
    msg += key;
    msg += "' field in ";
    msg += context;
    raise_invalid_argument(msg.c_str(), __FILE__, __LINE__);
    return false;
  }

  dest = py_to_cbpp<T>(pyObj);

  if constexpr (std::is_same_v<T, std::string>) {
    if (dest.empty()) {
      std::string msg = "Required '";
      msg += key;
      msg += "' field in ";
      msg += context;
      msg += " cannot be empty";
      raise_invalid_argument(msg.c_str(), __FILE__, __LINE__);
      return false;
    }
  }

  return true;
}

// TODO(PYCBC-1746): Delete w/ removal of legacy tracing logic
inline void
extract_legacy_span_field(PyObject* dict,
                          const char* key,
                          std::shared_ptr<couchbase::tracing::request_span>& dest)
{
  PyObject* pyObj = PyDict_GetItemString(dict, key);
  if (pyObj != nullptr) {
    // We should have the legacy tracer make the request, but this follows what we have done
    // historically. A moot point once we remove access to the (faulty) legacy tracing.
    dest = std::make_shared<pycbc::deprecated_request_span>(pyObj);
  }
}

// Add a field to result dict (auto-converts C++ value to Python)
template<typename T>
inline void
add_field(PyObject* dict, const char* key, const T& value)
{
  PyObject* pyObj = cbpp_to_py(value);
  PyDict_SetItemString(dict, key, pyObj);
  Py_DECREF(pyObj);
}

inline void
add_field(PyObject* dict, const char* key, PyObject* value)
{
  PyDict_SetItemString(dict, key, value);
  Py_DECREF(value);
}

inline void
add_bool_field(PyObject* dict, const char* key, bool value)
{
  PyDict_SetItemString(dict, key, value ? Py_True : Py_False);
}

inline void
add_string_field_if_not_empty(PyObject* dict, const char* key, const std::string& value)
{
  if (!value.empty()) {
    PyObject* pyObj = PyUnicode_FromString(value.c_str());
    PyDict_SetItemString(dict, key, pyObj);
    Py_DECREF(pyObj);
  }
}

// Add duration field to dict (specialized - as milliseconds)
inline void
add_duration_field(PyObject* dict, const char* key, const std::chrono::milliseconds& value)
{
  PyDict_SetItemString(dict, key, PyLong_FromUnsignedLongLong(value.count()));
}

inline void
add_cpp_core_span_field(
  PyObject* dict,
  const char* key,
  const std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span>& wrapperSpan)
{
  PyObject* pyObj = cbpp_wrapper_span_to_py(wrapperSpan);
  if (pyObj != nullptr) {
    PyDict_SetItemString(dict, key, pyObj);
    Py_DECREF(pyObj);
  }
}

// ======================================================================
// Helper function to update cluster_options from Python dict
// ======================================================================
inline void
update_cluster_options_from_py(couchbase::core::cluster_options& options,
                               PyObject* pyObj_options,
                               PyObject* pyObj_auth)
{
  if (!pyObj_options || !PyDict_Check(pyObj_options)) {
    return;
  }

  // Trust store path (check auth first, then options)
  PyObject* pyObj_trust_store = nullptr;
  if (pyObj_auth && PyDict_Check(pyObj_auth)) {
    pyObj_trust_store = PyDict_GetItemString(pyObj_auth, "trust_store_path");
  }
  if (!pyObj_trust_store) {
    pyObj_trust_store = PyDict_GetItemString(pyObj_options, "trust_store_path");
  }
  if (pyObj_trust_store && PyUnicode_Check(pyObj_trust_store)) {
    options.trust_certificate = PyUnicode_AsUTF8(pyObj_trust_store);
  }

  // TLS options
  extract_field(pyObj_options, "tls_verify", options.tls_verify);
  extract_bool_field(pyObj_options, "enable_tls", options.enable_tls);

  // Network/protocol options
  extract_field(pyObj_options, "use_ip_protocol", options.use_ip_protocol);
  extract_field(pyObj_options, "network", options.network);
  // CXXCBC-133 ; these options are not used
  extract_bool_field(pyObj_options, "enable_tcp_keep_alive", options.enable_tcp_keep_alive);
  extract_field<std::chrono::milliseconds>(
    pyObj_options, "tcp_keep_alive_interval", options.tcp_keep_alive_interval);

  // Feature flags (boolean options)
  extract_bool_field(pyObj_options, "enable_mutation_tokens", options.enable_mutation_tokens);
  extract_bool_field(pyObj_options, "enable_dns_srv", options.enable_dns_srv);
  extract_bool_field(pyObj_options, "enable_compression", options.enable_compression);
  extract_bool_field(
    pyObj_options, "disable_mozilla_ca_certificates", options.disable_mozilla_ca_certificates);
  extract_bool_field(pyObj_options, "enable_lazy_connections", options.enable_lazy_connections);
  extract_bool_field(pyObj_options, "show_queries", options.show_queries);
  extract_bool_field(
    pyObj_options, "enable_unordered_execution", options.enable_unordered_execution);
  extract_bool_field(
    pyObj_options, "enable_clustermap_notification", options.enable_clustermap_notification);
  extract_bool_field(
    pyObj_options, "allow_enterprise_analytics", options.allow_enterprise_analytics);

  // Always disable metrics for now
  options.enable_metrics = false;
  // Always disable tracing, this will be ignored if we set the tracer
  options.enable_tracing = false;
  PyObject* pyObj_setup_sdk_tracing = PyDict_GetItemString(pyObj_options, "setup_sdk_tracing");
  if (pyObj_setup_sdk_tracing != nullptr && pyObj_setup_sdk_tracing == Py_True) {
    PyObject* pyObj_legacy_tracer = PyDict_GetItemString(pyObj_options, "legacy_tracer");
    if (pyObj_legacy_tracer != nullptr) {
      options.tracer = std::make_shared<pycbc::deprecated_request_tracer>(pyObj_legacy_tracer);
    } else {
      options.tracer = std::make_shared<couchbase::core::tracing::wrapper_sdk_tracer>();
    }
  }

  // Other
  extract_field<std::chrono::milliseconds>(
    pyObj_options, "config_poll_interval", options.config_poll_interval);
  extract_field<std::chrono::milliseconds>(
    pyObj_options, "config_poll_floor", options.config_poll_floor);
  extract_field_if_not_empty<std::string>(
    pyObj_options, "user_agent_extra", options.user_agent_extra);
  extract_field<std::size_t>(pyObj_options, "max_http_connections", options.max_http_connections);
  extract_bool_field(pyObj_options, "dump_configuration", options.dump_configuration);

  // Preferred server group (Python key "preferred_server_group" -> C++ field "server_group")
  {
    PyObject* pyObj_server_group = PyDict_GetItemString(pyObj_options, "preferred_server_group");
    if (pyObj_server_group && PyUnicode_Check(pyObj_server_group)) {
      options.server_group = std::string(PyUnicode_AsUTF8(pyObj_server_group));
    }
  }

  // App telemetry options (Python key "app_telemetry_backoff" -> C++ field
  // "app_telemetry_backoff_interval")
  extract_bool_field(pyObj_options, "enable_app_telemetry", options.enable_app_telemetry);
  extract_field_if_not_empty<std::string>(
    pyObj_options, "app_telemetry_endpoint", options.app_telemetry_endpoint);
  extract_field<std::chrono::milliseconds>(
    pyObj_options, "app_telemetry_backoff", options.app_telemetry_backoff_interval);
  extract_field<std::chrono::milliseconds>(
    pyObj_options, "app_telemetry_ping_interval", options.app_telemetry_ping_interval);
  extract_field<std::chrono::milliseconds>(
    pyObj_options, "app_telemetry_ping_timeout", options.app_telemetry_ping_timeout);

  // Timeout options (nested dict with durations in microseconds)
  PyObject* pyObj_timeout_opts = PyDict_GetItemString(pyObj_options, "timeout_options");
  // we parse srv timeout later, but go ahead and pull from timeout options (if available)
  PyObject* pyObj_dns_srv_timeout = nullptr;
  if (pyObj_timeout_opts && PyDict_Check(pyObj_timeout_opts)) {
    extract_field<std::chrono::milliseconds>(
      pyObj_timeout_opts, "bootstrap_timeout", options.bootstrap_timeout);
    extract_field<std::chrono::milliseconds>(
      pyObj_timeout_opts, "resolve_timeout", options.resolve_timeout);
    extract_field<std::chrono::milliseconds>(
      pyObj_timeout_opts, "connect_timeout", options.connect_timeout);
    extract_field<std::chrono::milliseconds>(
      pyObj_timeout_opts, "key_value_timeout", options.key_value_timeout);
    extract_field<std::chrono::milliseconds>(
      pyObj_timeout_opts, "key_value_durable_timeout", options.key_value_durable_timeout);
    extract_field<std::chrono::milliseconds>(
      pyObj_timeout_opts, "view_timeout", options.view_timeout);
    extract_field<std::chrono::milliseconds>(
      pyObj_timeout_opts, "query_timeout", options.query_timeout);
    extract_field<std::chrono::milliseconds>(
      pyObj_timeout_opts, "analytics_timeout", options.analytics_timeout);
    extract_field<std::chrono::milliseconds>(
      pyObj_timeout_opts, "search_timeout", options.search_timeout);
    extract_field<std::chrono::milliseconds>(
      pyObj_timeout_opts, "management_timeout", options.management_timeout);
    extract_field<std::chrono::milliseconds>(
      pyObj_timeout_opts, "idle_http_connection_timeout", options.idle_http_connection_timeout);
    extract_field<std::chrono::milliseconds>(
      pyObj_timeout_opts, "config_idle_redial_timeout", options.config_idle_redial_timeout);
    pyObj_dns_srv_timeout = PyDict_GetItemString(pyObj_timeout_opts, "dns_srv_timeout");
  }

  // DNS configuration (requires special handling as a group)
  // dns_srv_timeout is in timeout_options dict, but dns_nameserver and dns_port are in options dict
  PyObject* pyObj_dns_nameserver = PyDict_GetItemString(pyObj_options, "dns_nameserver");
  PyObject* pyObj_dns_port = PyDict_GetItemString(pyObj_options, "dns_port");
  if (pyObj_dns_srv_timeout || pyObj_dns_nameserver || pyObj_dns_port) {
    auto nameserver = pyObj_dns_nameserver && PyUnicode_Check(pyObj_dns_nameserver)
                        ? std::string(PyUnicode_AsUTF8(pyObj_dns_nameserver))
                        : options.dns_config.nameserver();
    auto port = pyObj_dns_port && PyLong_Check(pyObj_dns_port)
                  ? static_cast<uint16_t>(PyLong_AsUnsignedLong(pyObj_dns_port))
                  : options.dns_config.port();
    auto dns_srv_timeout_ms = options.dns_config.timeout(); // default
    if (pyObj_dns_srv_timeout && PyLong_Check(pyObj_dns_srv_timeout)) {
      // auto us = PyLong_AsUnsignedLongLong(pyObj_dns_srv_timeout);
      // dns_srv_timeout_ms = std::chrono::milliseconds(std::max(0ULL, us / 1000ULL));
      dns_srv_timeout_ms = py_to_cbpp<std::chrono::milliseconds>(pyObj_dns_srv_timeout);
    }
    options.dns_config = couchbase::core::io::dns::dns_config(nameserver, port, dns_srv_timeout_ms);
  }
}

inline PyObject*
cluster_options_to_py(const couchbase::core::cluster_options& opts,
                      const couchbase::core::cluster_credentials& creds)
{
  PyObject* dict = PyDict_New();
  if (dict == nullptr) {
    return nullptr;
  }

  // timeouts
  PyObject* timeout_dict = PyDict_New();
  add_field<std::chrono::milliseconds>(timeout_dict, "bootstrap_timeout", opts.bootstrap_timeout);
  add_field<std::chrono::milliseconds>(timeout_dict, "resolve_timeout", opts.resolve_timeout);
  add_field<std::chrono::milliseconds>(timeout_dict, "connect_timeout", opts.connect_timeout);
  add_field<std::chrono::milliseconds>(timeout_dict, "key_value_timeout", opts.key_value_timeout);
  add_field<std::chrono::milliseconds>(
    timeout_dict, "key_value_durable_timeout", opts.key_value_durable_timeout);
  add_field<std::chrono::milliseconds>(timeout_dict, "view_timeout", opts.view_timeout);
  add_field<std::chrono::milliseconds>(timeout_dict, "query_timeout", opts.query_timeout);
  add_field<std::chrono::milliseconds>(timeout_dict, "analytics_timeout", opts.analytics_timeout);
  add_field<std::chrono::milliseconds>(timeout_dict, "search_timeout", opts.search_timeout);
  add_field<std::chrono::milliseconds>(timeout_dict, "management_timeout", opts.management_timeout);
  add_field<std::chrono::milliseconds>(timeout_dict, "dns_srv_timeout", opts.dns_config.timeout());
  add_field<std::chrono::milliseconds>(
    timeout_dict, "idle_http_connection_timeout", opts.idle_http_connection_timeout);
  add_field<std::chrono::milliseconds>(
    timeout_dict, "config_idle_redial_timeout", opts.config_idle_redial_timeout);
  PyDict_SetItemString(dict, "timeout_options", timeout_dict);
  Py_DECREF(timeout_dict);

  // DNS configuration
  add_bool_field(dict, "enable_dns_srv", opts.enable_dns_srv);
  add_field(dict, "dns_nameserver", opts.dns_config.nameserver());
  add_field(dict, "use_ip_protocol", opts.dns_config.port());

  // TLS options
  add_bool_field(dict, "enable_tls", opts.enable_tls);
  add_string_field_if_not_empty(dict, "trust_store_path", opts.trust_certificate);
  add_field(dict, "tls_verify", opts.tls_verify);

  // Network/protocol options
  add_field(dict, "use_ip_protocol", opts.use_ip_protocol);
  add_string_field_if_not_empty(dict, "network", opts.network);
  add_field<std::chrono::milliseconds>(
    dict, "tcp_keep_alive_interval", opts.tcp_keep_alive_interval);
  add_bool_field(dict, "enable_tcp_keep_alive", opts.enable_tcp_keep_alive);

  // Feature flags
  add_bool_field(dict, "enable_mutation_tokens", opts.enable_mutation_tokens);
  add_bool_field(dict, "enable_compression", opts.enable_compression);
  add_bool_field(dict, "disable_mozilla_ca_certificates", opts.disable_mozilla_ca_certificates);
  add_bool_field(dict, "enable_metrics", opts.enable_metrics);
  add_bool_field(dict, "enable_tracing", opts.enable_tracing);
  add_bool_field(dict, "enable_lazy_connections", opts.enable_lazy_connections);
  add_bool_field(dict, "show_queries", opts.show_queries);
  add_bool_field(dict, "enable_unordered_execution", opts.enable_unordered_execution);
  add_bool_field(dict, "enable_clustermap_notification", opts.enable_clustermap_notification);
  add_bool_field(dict, "allow_enterprise_analytics", opts.allow_enterprise_analytics);
  add_bool_field(dict, "dump_configuration", opts.dump_configuration);
  add_bool_field(dict, "enable_app_telemetry", opts.enable_app_telemetry);

  // Other
  add_field<std::chrono::milliseconds>(dict, "config_poll_interval", opts.config_poll_interval);
  add_field<std::chrono::milliseconds>(dict, "config_poll_floor", opts.config_poll_floor);
  add_string_field_if_not_empty(dict, "user_agent_extra", opts.user_agent_extra);
  add_field<std::size_t>(dict, "max_http_connections", opts.max_http_connections);
  add_string_field_if_not_empty(dict, "preferred_server_group", opts.server_group);
  add_string_field_if_not_empty(dict, "app_telemetry_endpoint", opts.app_telemetry_endpoint);
  add_field<std::chrono::milliseconds>(
    dict, "app_telemetry_backoff", opts.app_telemetry_backoff_interval);
  add_field<std::chrono::milliseconds>(
    dict, "app_telemetry_ping_interval", opts.app_telemetry_ping_interval);
  add_field<std::chrono::milliseconds>(
    dict, "app_telemetry_ping_timeout", opts.app_telemetry_ping_timeout);

  // Credentials
  PyObject* creds_dict = cbpp_to_py(creds);
  PyDict_SetItemString(dict, "credentials", creds_dict);
  Py_DECREF(creds_dict);

  return dict;
}

inline PyObject*
get_default_timeouts()
{
  PyObject* result = PyDict_New();
  if (!result) {
    return nullptr;
  }
  add_field<std::chrono::milliseconds>(
    result, "bootstrap_timeout", couchbase::core::timeout_defaults::bootstrap_timeout);
  add_field<std::chrono::milliseconds>(
    result, "dispatch_timeout", couchbase::core::timeout_defaults::dispatch_timeout);
  add_field<std::chrono::milliseconds>(
    result, "resolve_timeout", couchbase::core::timeout_defaults::resolve_timeout);
  add_field<std::chrono::milliseconds>(
    result, "connect_timeout", couchbase::core::timeout_defaults::connect_timeout);
  add_field<std::chrono::milliseconds>(
    result, "key_value_timeout", couchbase::core::timeout_defaults::key_value_timeout);
  add_field<std::chrono::milliseconds>(
    result,
    "key_value_durable_timeout",
    couchbase::core::timeout_defaults::key_value_durable_timeout);
  add_field<std::chrono::milliseconds>(
    result, "key_value_scan_timeout", couchbase::core::timeout_defaults::key_value_scan_timeout);
  add_field<std::chrono::milliseconds>(
    result, "view_timeout", couchbase::core::timeout_defaults::view_timeout);
  add_field<std::chrono::milliseconds>(
    result, "query_timeout", couchbase::core::timeout_defaults::query_timeout);
  add_field<std::chrono::milliseconds>(
    result, "analytics_timeout", couchbase::core::timeout_defaults::analytics_timeout);
  add_field<std::chrono::milliseconds>(
    result, "search_timeout", couchbase::core::timeout_defaults::search_timeout);
  add_field<std::chrono::milliseconds>(
    result, "management_timeout", couchbase::core::timeout_defaults::management_timeout);
  add_field<std::chrono::milliseconds>(
    result, "eventing_timeout", couchbase::core::timeout_defaults::eventing_timeout);
  add_field<std::chrono::milliseconds>(
    result, "dns_srv_timeout", couchbase::core::timeout_defaults::dns_srv_timeout);
  add_field<std::chrono::milliseconds>(
    result, "tcp_keep_alive_interval", couchbase::core::timeout_defaults::tcp_keep_alive_interval);
  add_field<std::chrono::milliseconds>(
    result, "config_poll_interval", couchbase::core::timeout_defaults::config_poll_interval);
  add_field<std::chrono::milliseconds>(
    result, "config_poll_floor", couchbase::core::timeout_defaults::config_poll_floor);
  add_field<std::chrono::milliseconds>(
    result,
    "config_idle_redial_timeout",
    couchbase::core::timeout_defaults::config_idle_redial_timeout);
  add_field<std::chrono::milliseconds>(
    result,
    "idle_http_connection_timeout",
    couchbase::core::timeout_defaults::idle_http_connection_timeout);
  add_field<std::chrono::milliseconds>(
    result,
    "app_telemetry_ping_interval",
    couchbase::core::timeout_defaults::app_telemetry_ping_interval);
  add_field<std::chrono::milliseconds>(
    result,
    "app_telemetry_ping_timeout",
    couchbase::core::timeout_defaults::app_telemetry_ping_timeout);
  add_field<std::chrono::milliseconds>(
    result,
    "app_telemetry_backoff_interval",
    couchbase::core::timeout_defaults::app_telemetry_backoff_interval);

  return result;
}

template<typename Request>
inline std::chrono::milliseconds
get_default_timeout(const Request& req)
{
  if constexpr (std::is_same_v<Request, couchbase::core::operations::analytics_request>) {
    return couchbase::core::timeout_defaults::analytics_timeout;
  }
  if constexpr (std::is_same_v<Request, couchbase::core::operations::query_request>) {
    return couchbase::core::timeout_defaults::query_timeout;
  }
  if constexpr (std::is_same_v<Request, couchbase::core::operations::search_request>) {
    return couchbase::core::timeout_defaults::search_timeout;
  }
  if constexpr (std::is_same_v<Request, couchbase::core::operations::document_view_request>) {
    return couchbase::core::timeout_defaults::view_timeout;
  }
}

} // namespace pycbc
