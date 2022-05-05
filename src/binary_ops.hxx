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

struct counter_options {
    // required
    connection* conn;
    couchbase::document_id id;
    Operations::OperationType op_type;
    uint64_t delta;

    // optional
    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::key_value_timeout;
    uint32_t expiry;
    uint8_t replicate_to;
    uint8_t persist_to;
    couchbase::protocol::durability_level durability;
    uint64_t initial_value;
    PyObject* pyObj_span;
};

struct binary_mutation_options {
    // required
    connection* conn;
    couchbase::document_id id;
    Operations::OperationType op_type;
    PyObject* pyObj_value;

    // optional
    std::chrono::milliseconds timeout_ms = couchbase::timeout_defaults::key_value_timeout;
    uint8_t replicate_to;
    uint8_t persist_to;
    couchbase::protocol::durability_level durability;
    couchbase::cas cas;
    PyObject* pyObj_span;
};

PyObject*
handle_binary_op(PyObject* self, PyObject* args, PyObject* kwargs);

PyObject*
handle_binary_multi_op(PyObject* self, PyObject* args, PyObject* kwargs);
