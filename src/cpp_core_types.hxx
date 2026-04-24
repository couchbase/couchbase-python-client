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
#include "cpp_types.hxx"
#include "pycbc_kv_request.hxx"
#include "pytocbpp_defs.hxx"
#include "result.hxx"
#include "utils.hxx"
#include <core/cluster_credentials.hxx>
#include <core/cluster_options.hxx>
#include <core/document_id.hxx>
#include <core/json_string.hxx>
#include <core/query_context.hxx>
#include <core/tracing/wrapper_sdk_tracer.hxx>
#include <couchbase/cas.hxx>
#include <couchbase/mutation_token.hxx>

namespace pycbc
{

// ==========================================================================================
// couchbase::core::document_id
// ==========================================================================================
template<>
struct py_to_cbpp_t<couchbase::core::document_id> {
  static inline couchbase::core::document_id from_py(pycbc_kv_request* request)
  {
    return { PyUnicode_AsUTF8(request->bucket),
             PyUnicode_AsUTF8(request->scope),
             PyUnicode_AsUTF8(request->collection),
             PyUnicode_AsUTF8(request->key) };
  }

  static inline couchbase::core::document_id from_py(PyObject* pyObj)
  {
    std::string bucket;
    std::string scope;
    std::string collection;
    std::string key;

    extract_field(pyObj, "bucket", bucket);
    extract_field(pyObj, "scope", scope);
    extract_field(pyObj, "collection", collection);
    extract_field(pyObj, "key", key);

    return couchbase::core::document_id{ bucket, scope, collection, key };
  }

  static inline PyObject* to_py(const couchbase::core::document_id& id)
  {
    PyObject* dict = PyDict_New();
    if (dict == nullptr) {
      return nullptr;
    }

    add_field(dict, "bucket", id.bucket());
    add_field(dict, "scope", id.scope());
    add_field(dict, "collection", id.collection());
    add_field(dict, "key", id.key());

    return dict;
  }
};

// ==========================================================================================
// couchbase::cas
// ==========================================================================================
template<>
struct py_to_cbpp_t<couchbase::cas> {

  static inline PyObject* to_py(const couchbase::cas& cppObj)
  {
    return PyLong_FromUnsignedLongLong(cppObj.value());
  }

  static inline couchbase::cas from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return couchbase::cas{ 0 };
    }

    if (!PyLong_Check(pyObj)) {
      return couchbase::cas{ 0 };
    }

    return couchbase::cas{ PyLong_AsUnsignedLongLong(pyObj) };
  }
};

// ==========================================================================================
// couchbase::mutation_token
// ==========================================================================================
template<>
struct py_to_cbpp_t<couchbase::mutation_token> {

  static inline PyObject* to_py(const couchbase::mutation_token& token)
  {
    PyObject* dict = PyDict_New();
    if (dict == nullptr) {
      return nullptr;
    }

    add_field(dict, "partition_id", static_cast<unsigned long>(token.partition_id()));
    add_field(dict, "partition_uuid", token.partition_uuid());
    add_field(dict, "sequence_number", token.sequence_number());
    add_field(dict, "bucket_name", token.bucket_name());

    return dict;
  }

  static inline couchbase::mutation_token from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return {};
    }

    if (!PyDict_Check(pyObj)) {
      return {};
    }

    std::uint64_t partition_uuid;
    std::uint64_t sequence_number;
    std::uint16_t partition_id;
    std::string bucket_name;

    extract_field(pyObj, "partition_uuid", partition_uuid);
    extract_field(pyObj, "sequence_number", sequence_number);
    extract_field(pyObj, "partition_id", partition_id);
    extract_field(pyObj, "bucket_name", bucket_name);

    return couchbase::mutation_token{ partition_uuid, sequence_number, partition_id, bucket_name };
  }
};

// ==========================================================================================
// couchbase::core::cluster_credentials
// ==========================================================================================
template<>
struct py_to_cbpp_t<couchbase::core::cluster_credentials> {

  static inline couchbase::core::cluster_credentials from_py(PyObject* pyObj)
  {
    couchbase::core::cluster_credentials creds;

    if (pyObj == nullptr || !PyDict_Check(pyObj)) {
      return creds;
    }

    extract_field(pyObj, "username", creds.username);
    extract_field(pyObj, "password", creds.password);
    extract_field(pyObj, "cert_path", creds.certificate_path);
    extract_field(pyObj, "key_path", creds.key_path);
    extract_field(pyObj, "allowed_sasl_mechanisms", creds.allowed_sasl_mechanisms);
    extract_field(pyObj, "jwt_token", creds.jwt_token);

    return creds;
  }

  static inline PyObject* to_py(const couchbase::core::cluster_credentials& creds)
  {
    PyObject* dict = PyDict_New();
    if (dict == nullptr) {
      return nullptr;
    }

    add_string_field_if_not_empty(dict, "username", creds.username);
    add_string_field_if_not_empty(dict, "password", creds.password);
    add_string_field_if_not_empty(dict, "cert_path", creds.certificate_path);
    add_string_field_if_not_empty(dict, "key_path", creds.key_path);
    add_string_field_if_not_empty(dict, "jwt_token", creds.jwt_token);

    if (creds.allowed_sasl_mechanisms.has_value() &&
        !creds.allowed_sasl_mechanisms.value().empty()) {
      add_field(dict, "allowed_sasl_mechanisms", creds.allowed_sasl_mechanisms.value());
    }

    return dict;
  }
};

// ==========================================================================================
// couchbase::core::json_string
// ==========================================================================================
template<>
struct py_to_cbpp_t<couchbase::core::json_string> {
  static inline PyObject* to_py(const couchbase::core::json_string& cppObj)
  {
    // json_string can hold either string or binary data
    if (cppObj.is_string()) {
      const auto& str = cppObj.str();
      return PyBytes_FromStringAndSize(str.data(), static_cast<Py_ssize_t>(str.size()));
    } else if (cppObj.is_binary()) {
      const auto& bytes = cppObj.bytes();
      return PyBytes_FromStringAndSize(reinterpret_cast<const char*>(bytes.data()),
                                       static_cast<Py_ssize_t>(bytes.size()));
    }
    // Empty case
    return PyBytes_FromStringAndSize("", 0);
  }

  static inline couchbase::core::json_string from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return couchbase::core::json_string{};
    }

    if (!PyBytes_Check(pyObj)) {
      return couchbase::core::json_string{};
    }

    char* buffer = nullptr;
    Py_ssize_t length = 0;
    if (PyBytes_AsStringAndSize(pyObj, &buffer, &length) != 0) {
      return couchbase::core::json_string{};
    }

    if (buffer == nullptr || length == 0) {
      return couchbase::core::json_string{};
    }

    auto* begin = reinterpret_cast<const std::byte*>(buffer);
    auto* end = begin + length;
    return couchbase::core::json_string{ std::vector<std::byte>(begin, end) };
  }
};

// ==========================================================================================
// couchbase::core::query_context
// ==========================================================================================
template<>
struct py_to_cbpp_t<couchbase::core::query_context> {
  static inline PyObject* to_py(const couchbase::core::query_context& cppObj)
  {
    PyObject* dict = PyDict_New();
    if (dict == nullptr) {
      return nullptr;
    }

    add_field(dict, "bucket_name", cppObj.bucket_name());
    add_field(dict, "bucket_name", cppObj.scope_name());
    return dict;
  }

  static inline couchbase::core::query_context from_py(PyObject* pyObj)
  {
    std::string bucket_name;
    std::string scope_name;
    extract_field(pyObj, "bucket_name", bucket_name);
    extract_field(pyObj, "scope_name", scope_name);
    if (!bucket_name.empty() || !scope_name.empty()) {
      return couchbase::core::query_context(bucket_name, scope_name);
    }
    return couchbase::core::query_context();
  }
};

// ==========================================================================================
// couchbase::core::tracing::wrapper_sdk_span
// ==========================================================================================
template<>
struct py_to_cbpp_t<std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span>> {
  static inline PyObject* cbpp_wrapper_span_to_py(
    std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span> wrapperSpan)
  {
    if (nullptr == wrapperSpan || wrapperSpan->children().empty()) {
      Py_RETURN_NONE;
    }

    PyObject* spanObj = PyDict_New();
    if (spanObj == nullptr) {
      return nullptr;
    }

    PyObject* attributesObj = PyDict_New();
    if (attributesObj == nullptr) {
      Py_DECREF(attributesObj);
      return nullptr;
    }

    for (const auto& [key, value] : wrapperSpan->uint_tags()) {
      add_field<std::uint64_t>(attributesObj, key.c_str(), value);
    }
    for (const auto& [key, value] : wrapperSpan->string_tags()) {
      add_field<std::string>(attributesObj, key.c_str(), value);
    }
    add_field(spanObj, "attributes", attributesObj);
    add_field<std::vector<std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span>>>(
      spanObj, "children", wrapperSpan->children());
    return spanObj;
  }

  static inline PyObject* to_py(
    std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span> wrapperSpan)
  {
    if (nullptr == wrapperSpan) {
      Py_RETURN_NONE;
    }
    PyObject* pyObj = PyDict_New();
    if (pyObj == nullptr) {
      return nullptr;
    }
    add_field<std::string>(pyObj, "name", wrapperSpan->name());
    add_field<std::chrono::system_clock::time_point>(pyObj, "start", wrapperSpan->start_time());
    add_field<std::chrono::system_clock::time_point>(pyObj, "end", wrapperSpan->end_time());

    PyObject* attributesObj = PyDict_New();
    if (attributesObj == nullptr) {
      Py_DECREF(pyObj);
      return nullptr;
    }
    for (const auto& [key, value] : wrapperSpan->uint_tags()) {
      add_field<std::uint64_t>(attributesObj, key.c_str(), value);
    }
    for (const auto& [key, value] : wrapperSpan->string_tags()) {
      add_field<std::string>(attributesObj, key.c_str(), value);
    }
    add_field(pyObj, "attributes", attributesObj);

    // some operations in the C++ core handle sub-operations (e.g. replicas, etc.)
    if (!wrapperSpan->children().empty()) {
      add_field<std::vector<std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span>>>(
        pyObj, "children", wrapperSpan->children());
    }
    return pyObj;
  }
};

} // namespace pycbc
