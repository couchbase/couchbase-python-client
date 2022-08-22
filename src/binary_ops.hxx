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

#include "client.hxx"
#include <couchbase/append_options.hxx>
#include <couchbase/decrement_options.hxx>
#include <couchbase/increment_options.hxx>
#include <couchbase/prepend_options.hxx>
#include <couchbase/persist_to.hxx>
#include <couchbase/replicate_to.hxx>

struct counter_options {
    // required
    connection* conn;
    couchbase::core::document_id id;
    Operations::OperationType op_type;
    uint64_t delta;

    // optional
    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::key_value_timeout;
    std::chrono::seconds expiry;
    couchbase::durability_level* durability_level;
    couchbase::replicate_to* replicate_to;
    couchbase::persist_to* persist_to;
    uint64_t initial_value;
    PyObject* pyObj_span;
};

struct binary_mutation_options {
    // required
    connection* conn;
    couchbase::core::document_id id;
    Operations::OperationType op_type;
    PyObject* pyObj_value;

    // optional
    std::chrono::milliseconds timeout_ms = couchbase::core::timeout_defaults::key_value_timeout;
    couchbase::durability_level* durability_level;
    couchbase::replicate_to* replicate_to;
    couchbase::persist_to* persist_to;
    couchbase::cas cas;
    PyObject* pyObj_span;
};

PyObject*
handle_binary_op(PyObject* self, PyObject* args, PyObject* kwargs);

PyObject*
handle_binary_multi_op(PyObject* self, PyObject* args, PyObject* kwargs);
