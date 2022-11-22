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

#pragma once

#include "Python.h" // NOLINT
#include <core/utils/binary.hxx>
#include <core/utils/json.hxx>
#include <core/utils/join_strings.hxx>
#include <couchbase/persist_to.hxx>
#include <couchbase/replicate_to.hxx>
#include <couchbase/durability_level.hxx>
#include <couchbase/mutation_token.hxx>
#include <core/operations/document_query.hxx>
#include "tracing.hxx"
#include <stdexcept>
#include <string>
#include <chrono>

constexpr std::chrono::seconds FIFTY_YEARS{ 50 * 365 * 24 * 60 * 60 };

couchbase::core::utils::binary
PyObject_to_binary(PyObject*);
PyObject*
binary_to_PyObject(couchbase::core::utils::binary value);

PyObject*
binary_to_PyObject_unicode(couchbase::core::utils::binary value);

std::string
binary_to_string(couchbase::core::utils::binary value);

std::size_t py_ssize_t_to_size_t(Py_ssize_t);
Py_ssize_t size_t_to_py_ssize_t(std::size_t);

couchbase::persist_to
PyObject_to_persist_to(PyObject* pyObj_persist_to);
couchbase::replicate_to
PyObject_to_replicate_to(PyObject* pyObj_replicate_to);
std::pair<couchbase::persist_to, couchbase::replicate_to>
PyObject_to_durability(PyObject*);
couchbase::durability_level
PyObject_to_durability_level(PyObject*);

std::vector<couchbase::mutation_token>
get_mutation_state(PyObject* pyObj_mutation_state);

std::string
profile_mode_to_str(couchbase::query_profile profile_mode);

template<typename scan_consistency_type>
scan_consistency_type
str_to_scan_consistency_type(std::string consistency)
{
    if (consistency.compare("not_bounded") == 0) {
        return scan_consistency_type::not_bounded;
    }
    if (consistency.compare("request_plus") == 0) {
        return scan_consistency_type::request_plus;
    }

    // TODO: better exception
    PyErr_SetString(PyExc_ValueError, fmt::format("Invalid Scan Consistency type {}", consistency).c_str());
    return {};
}

// TODO: consolidate these types of methods to another file that handles other requests as well
couchbase::core::operations::query_request
build_query_request(PyObject* pyObj_query_args);

std::vector<couchbase::mutation_token>
get_mutation_state(PyObject* pyObj_mutation_state);
