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

#include "../cpp_core_enums_autogen.hxx"
#include "../cpp_core_types.hxx"
#include "../cpp_types.hxx"
#include "../pytocbpp_defs.hxx"
#include "../utils.hxx"
#include "Python.h"
#include <couchbase/query_profile.hxx>
#include <couchbase/query_scan_consistency.hxx>
#include <couchbase/transactions/transaction_query_options.hxx>

namespace cbtxns = couchbase::transactions;

namespace pycbc
{

template<>
struct py_to_cbpp_t<cbtxns::transaction_query_options> {
  static inline cbtxns::transaction_query_options from_py(PyObject* pyObj)
  {
    if (pyObj == nullptr || pyObj == Py_None) {
      return cbtxns::transaction_query_options();
    }

    cbtxns::transaction_query_options cppObj;

    std::optional<bool> adhoc;
    extract_field(pyObj, "adhoc", adhoc);
    if (adhoc.has_value()) {
      cppObj.ad_hoc(adhoc.value());
    }

    std::optional<bool> metrics;
    extract_field(pyObj, "metrics", metrics);
    if (metrics.has_value()) {
      cppObj.metrics(metrics.value());
    }

    std::optional<bool> readonly;
    extract_field(pyObj, "readonly", readonly);
    if (readonly.has_value()) {
      cppObj.readonly(readonly.value());
    }

    std::optional<std::uint64_t> max_parallelism;
    extract_field(pyObj, "max_parallelism", max_parallelism);
    if (max_parallelism.has_value()) {
      cppObj.max_parallelism(max_parallelism.value());
    }

    std::optional<std::uint64_t> scan_cap;
    extract_field(pyObj, "scan_cap", scan_cap);
    if (scan_cap.has_value()) {
      cppObj.scan_cap(scan_cap.value());
    }

    std::optional<std::chrono::milliseconds> scan_wait;
    extract_field(pyObj, "scan_wait", scan_wait);
    if (scan_wait.has_value()) {
      cppObj.scan_wait(scan_wait.value());
    }

    std::optional<std::uint64_t> pipeline_batch;
    extract_field(pyObj, "pipeline_batch", pipeline_batch);
    if (pipeline_batch.has_value()) {
      cppObj.pipeline_batch(pipeline_batch.value());
    }

    std::optional<std::uint64_t> pipeline_cap;
    extract_field(pyObj, "pipeline_cap", pipeline_cap);
    if (pipeline_cap.has_value()) {
      cppObj.pipeline_cap(pipeline_cap.value());
    }

    std::optional<std::string> client_context_id;
    extract_field(pyObj, "client_context_id", client_context_id);
    if (client_context_id.has_value()) {
      cppObj.client_context_id(client_context_id.value());
    }

    std::optional<couchbase::query_scan_consistency> scan_consistency;
    extract_field(pyObj, "scan_consistency", scan_consistency);
    if (scan_consistency.has_value()) {
      cppObj.scan_consistency(scan_consistency.value());
    }

    std::optional<couchbase::query_profile> profile;
    extract_field(pyObj, "profile", profile);
    if (profile.has_value()) {
      cppObj.profile(profile.value());
    }

    // NOTE: Binary fields expect bytes (changed from string in original implementation)
    std::optional<std::map<std::string, std::vector<std::byte>, std::less<>>> raw;
    extract_field(pyObj, "raw", raw);
    if (raw.has_value() && raw.value().size() > 0) {
      cppObj.encoded_raw_options(raw.value());
    }

    std::optional<std::vector<std::vector<std::byte>>> positional_parameters;
    extract_field(pyObj, "positional_parameters", positional_parameters);
    if (positional_parameters.has_value() && positional_parameters.value().size() > 0) {
      cppObj.encoded_positional_parameters(positional_parameters.value());
    }

    std::optional<std::map<std::string, std::vector<std::byte>, std::less<>>> named_parameters;
    extract_field(pyObj, "named_parameters", named_parameters);
    if (named_parameters.has_value() && named_parameters.value().size() > 0) {
      cppObj.encoded_named_parameters(named_parameters.value());
    }

    return cppObj;
  }

  static inline PyObject* to_py(const cbtxns::transaction_query_options& cppObj)
  {
    PyObject* dict = PyDict_New();
    if (dict == nullptr) {
      return nullptr;
    }

    auto query_opts = cppObj.get_query_options().build();

    add_field(dict, "adhoc", query_opts.adhoc);
    add_field(dict, "metrics", query_opts.metrics);
    // read_only on the Python side (backwards compat from legacy txn implementation)
    add_field(dict, "read_only", query_opts.readonly);
    add_field(dict, "flex_index", query_opts.flex_index);
    add_field(dict, "preserve_expiry", query_opts.preserve_expiry);

    if (query_opts.max_parallelism.has_value()) {
      add_field(dict, "max_parallelism", query_opts.max_parallelism.value());
    }
    if (query_opts.scan_cap.has_value()) {
      add_field(dict, "scan_cap", query_opts.scan_cap.value());
    }
    if (query_opts.scan_wait.has_value()) {
      add_field(dict, "scan_wait", query_opts.scan_wait.value());
    }
    if (query_opts.pipeline_batch.has_value()) {
      add_field(dict, "pipeline_batch", query_opts.pipeline_batch.value());
    }
    if (query_opts.pipeline_cap.has_value()) {
      add_field(dict, "pipeline_cap", query_opts.pipeline_cap.value());
    }
    if (query_opts.client_context_id.has_value()) {
      add_field(dict, "client_context_id", query_opts.client_context_id.value());
    }

    if (query_opts.scan_consistency.has_value()) {
      add_field(dict, "scan_consistency", query_opts.scan_consistency.value());
    }
    if (query_opts.profile.has_value()) {
      add_field(dict, "profile", query_opts.profile.value());
    }

    // NOTE: Binary fields return bytes (changed from string in original implementation)
    if (!query_opts.raw.empty()) {
      add_field(dict, "raw", query_opts.raw);
    }
    if (!query_opts.positional_parameters.empty()) {
      add_field(dict, "positional_parameters", query_opts.positional_parameters);
    }
    if (!query_opts.named_parameters.empty()) {
      add_field(dict, "named_parameters", query_opts.named_parameters);
    }

    return dict;
  }
};

} // namespace pycbc
